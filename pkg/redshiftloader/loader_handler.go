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

type LoaderConfig struct {
	// Maximum size of a batch, on exceeding this batch is pushed
	// regarless of the wait time.
	MaxSize int `yaml:"maxSize,omitempty"`

	// MaxWaitSeconds after which the bash would be pushed regardless of its size.
	MaxWaitSeconds int `yaml:"maxWaitSeconds,omitempty"`
}

// loaderHandler is the sarama consumer handler
// loaderHandler.ConsumeClaim() is called for every topic partition
type loaderHandler struct {
	ready chan bool
	ctx   context.Context

	consumerGroupID string

	maxSize        int
	maxWaitSeconds int

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

	return &loaderHandler{
		ready: ready,
		ctx:   ctx,

		consumerGroupID: consumerGroupID,

		maxSize:        loaderConfig.MaxSize,
		maxWaitSeconds: loaderConfig.MaxWaitSeconds,

		saramaConfig: saramaConfig,
		redshifter:   redshifter,
		serializer:   serializer.NewSerializer(viper.GetString("schemaRegistryURL")),
	}
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (h *loaderHandler) Setup(sarama.ConsumerGroupSession) error {
	klog.V(1).Info("Setting up handler")

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
	klog.V(1).Info("Cleaning up handler")
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// ConsumeClaim is managed by the consumer.manager routine
func (h *loaderHandler) ConsumeClaim(session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim) error {

	klog.V(1).Infof(
		"ConsumeClaim started for topic:%s, partition:%d, initalOffset:%d\n",
		claim.Topic(),
		claim.Partition(),
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
	msgBatch := serializer.NewMessageBatch(
		claim.Topic(),
		claim.Partition(),
		h.maxSize,
		processor,
	)
	maxWaitTicker := time.NewTicker(
		time.Duration(h.maxWaitSeconds) * time.Second,
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
				"ConsumeClaim returning for topic: %s (main ctx done)",
				claim.Topic(),
			)
			return nil
		case <-session.Context().Done():
			klog.V(2).Infof(
				"ConsumeClaim returning for topic: %s (session ctx done)",
				claim.Topic(),
			)
			return fmt.Errorf("session ctx done, err: %v", session.Context().Err())
		case message, ok := <-claimMsgChan:
			if !ok {
				klog.V(2).Infof(
					"ConsumeClaim returning for topic: %s (read msg channel closed)",
					claim.Topic(),
				)
				return nil
			}

			select {
			default:
			case <-h.ctx.Done():
				klog.V(2).Infof(
					"ConsumeClaim returning for topic: %s (main ctx done)",
					claim.Topic(),
				)
				return nil
			case <-session.Context().Done():
				klog.V(2).Infof(
					"ConsumeClaim returning for topic: %s (session ctx done)",
					claim.Topic(),
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
					"topic:%s: schema changed, %d => %d (batch flush)\n",
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
			err = msgBatch.Insert(session, msg)
			if err != nil {
				return err
			}
			*lastSchemaId = upstreamJobSchemaId
		case <-maxWaitTicker.C:
			// Process the batch by time
			klog.V(2).Infof(
				"topic:%s: maxWaitSeconds hit",
				claim.Topic(),
			)
			err = msgBatch.Process(session)
			if err != nil {
				return err
			}
		}
	}
}
