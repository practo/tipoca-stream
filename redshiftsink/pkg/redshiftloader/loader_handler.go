package redshiftloader

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/practo/klog/v2"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/kafka"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/redshift"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/serializer"
	"github.com/spf13/viper"
	"time"
)

var (
	DefaultMaxWaitSeconds    int   = 60
	DefaultMaxProcessingTime int32 = 600000
)

type LoaderConfig struct {
	// Maximum size of a batch, on exceeding this batch is pushed
	// regarless of the wait time.
	// Deprecated: in favour of MaxBytesPerBatch
	MaxSize int `yaml:"maxSize,omitempty"`

	// MaxBytesPerBatch is the maximum bytes per batch. It is not the size
	// of kafka message but the size of all the messages that would be
	// loaded in the batch. Default is there
	// if the user has not specified a default will be applied.
	// If this is specified, maxSize specification is not considered.
	// Default would be specified after MaxSize is gone
	MaxBytesPerBatch *int64 `yaml:"maxBytesPerBatch,omitempty"`

	// MaxWaitSeconds after which the bash would be pushed regardless of its size.
	MaxWaitSeconds *int `yaml:"maxWaitSeconds,omitempty"`
}

// loaderHandler is the sarama consumer handler
// loaderHandler.ConsumeClaim() is called for every topic partition
type loaderHandler struct {
	ready chan bool
	ctx   context.Context

	consumerGroupID string

	maxSize int // Deprecated

	maxWaitSeconds   *int
	maxBytesPerBatch *int64

	saramaConfig kafka.SaramaConfig
	redshifter   *redshift.Redshift
	serializer   serializer.Serializer
}

func NewHandler(
	ctx context.Context,
	ready chan bool,
	consumerGroupID string,
	loaderConfig LoaderConfig,
	saramaConfig kafka.SaramaConfig,
	redshifter *redshift.Redshift,
) *loaderHandler {
	// apply defaults
	if loaderConfig.MaxWaitSeconds == nil {
		loaderConfig.MaxWaitSeconds = &DefaultMaxWaitSeconds
	}

	return &loaderHandler{
		ready: ready,
		ctx:   ctx,

		consumerGroupID: consumerGroupID,

		maxSize: loaderConfig.MaxSize, // Deprecated

		maxWaitSeconds:   loaderConfig.MaxWaitSeconds,
		maxBytesPerBatch: loaderConfig.MaxBytesPerBatch,

		saramaConfig: saramaConfig,
		redshifter:   redshifter,
		serializer:   serializer.NewSerializer(viper.GetString("schemaRegistryURL")),
	}
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (h *loaderHandler) Setup(sarama.ConsumerGroupSession) error {
	klog.V(1).Info("setting up handler")

	// Mark the consumer as ready
	select {
	case <-h.ready:
		return nil
	default:
	}
	close(h.ready)

	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (h *loaderHandler) Cleanup(sarama.ConsumerGroupSession) error {
	klog.V(1).Info("cleaning up handler")
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// ConsumeClaim is managed by the consumer.manager routine
func (h *loaderHandler) ConsumeClaim(session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim) error {

	klog.V(1).Infof(
		"%s: consumeClaim started, initalOffset:%d\n",
		claim.Topic(),
		claim.InitialOffset(),
	)

	var lastSchemaId *int
	var err error
	processor := newLoadProcessor(
		h.consumerGroupID,
		claim.Topic(),
		claim.Partition(),
		h.saramaConfig,
		h.redshifter,
	)
	maxBufSize := h.maxSize
	if h.maxBytesPerBatch != nil {
		maxBufSize = serializer.DefaultMessageBufferSize
	}
	msgBatch := serializer.NewMessageSyncBatch(
		claim.Topic(),
		claim.Partition(),
		h.maxSize, // Deprecated
		maxBufSize,
		h.maxBytesPerBatch,
		processor,
	)
	maxWaitTicker := time.NewTicker(
		time.Duration(*h.maxWaitSeconds) * time.Second,
	)

	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	claimMsgChan := claim.Messages()

	for {
		select {
		case <-h.ctx.Done():
			klog.V(2).Infof(
				"%s: consumeClaim returning, main ctx done",
				claim.Topic(),
			)
			return nil
		case <-session.Context().Done():
			klog.V(2).Infof(
				"%s: consumeClaim returning. session ctx done, ctxErr: %v",
				claim.Topic(),
				session.Context().Err(),
			)
			return fmt.Errorf("session ctx done, err: %v", session.Context().Err())
		case message, ok := <-claimMsgChan:
			maxWaitTicker.Stop()
			if !ok {
				klog.V(2).Infof(
					"%s: consumeClaim returning. read msg channel closed",
					claim.Topic(),
				)
				return nil
			}

			select {
			default:
			case <-h.ctx.Done():
				klog.V(2).Infof(
					"%s: consumeClaim returning, main ctx done",
					claim.Topic(),
				)
				return nil
			case <-session.Context().Done():
				klog.V(2).Infof(
					"%s: consumeClaim returning. session ctx done, ctxErr: %v",
					claim.Topic(),
					session.Context().Err(),
				)
				return fmt.Errorf("session ctx done, err: %v", session.Context().Err())
			}

			// Deserialize the message
			msg, err := h.serializer.Deserialize(message)
			if err != nil {
				klog.Fatalf("Error deserializing binary, err: %s\n", err)
			}
			if msg == nil || msg.Value == nil {
				klog.Fatalf("Got message as nil, message: %+v\n", msg)
			}

			// Process the batch by schemaID
			job := StringMapToJob(msg.Value.(map[string]interface{}))
			upstreamJobSchemaId := job.SchemaId

			if lastSchemaId == nil {
				lastSchemaId = new(int)
			} else if *lastSchemaId != upstreamJobSchemaId {
				klog.V(2).Infof(
					"%s: schema changed, %d => %d (batch flush)\n",
					claim.Topic(),
					*lastSchemaId,
					upstreamJobSchemaId,
				)
				err = msgBatch.Process(session)
				if err != nil {
					return err
				}
			}
			// Process the batch by size or insert in batch
			err = msgBatch.Insert(session, msg, job.BatchBytes)
			if err != nil {
				return err
			}
			*lastSchemaId = upstreamJobSchemaId
			maxWaitTicker.Reset(time.Duration(*h.maxWaitSeconds) * time.Second)
		case <-maxWaitTicker.C:
			// Process the batch by time
			klog.V(2).Infof(
				"%s: maxWaitSeconds hit",
				claim.Topic(),
			)
			err = msgBatch.Process(session)
			if err != nil {
				return err
			}
		}
	}
}
