package redshiftloader

import (
	"context"
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

	maxSize       int
	maxWaitTicker *time.Ticker

	saramaConfig kafka.SaramaConfig
	redshifter   *redshift.Redshift
	serializer   serializer.Serializer
}

func NewHandler(
	ctx context.Context,
	ready chan bool,
	loaderConfig LoaderConfig,
	saramaConfig kafka.SaramaConfig,
	redshifter *redshift.Redshift,
) *loaderHandler {

	return &loaderHandler{
		ready: ready,
		ctx:   ctx,

		maxSize:       loaderConfig.MaxSize,
		maxWaitTicker: time.NewTicker(time.Duration(loaderConfig.MaxWaitSeconds) * time.Second),

		saramaConfig: saramaConfig,
		redshifter:   redshifter,
		serializer:   serializer.NewSerializer(viper.GetString("schemaRegistryURL")),
	}
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (h *loaderHandler) Setup(sarama.ConsumerGroupSession) error {
	klog.V(3).Info("Setting up consumer")

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
	klog.V(3).Info("Cleaning up consumer")
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// ConsumeClaim is managed by the consumer.manager routine
func (h *loaderHandler) ConsumeClaim(session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim) error {

	klog.V(2).Infof(
		"ConsumeClaim for topic:%s, partition:%d, initalOffset:%d\n",
		claim.Topic(),
		claim.Partition(),
		claim.InitialOffset(),
	)

	var lastSchemaId *int
	processor := newLoadProcessor(
		session,
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

	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	claimMsgChan := claim.Messages()

	for {
		select {
		case <-h.ctx.Done():
			klog.V(2).Infof(
				"ConsumeClaim gracefully shutdown for topic: %s (above)",
				claim.Topic(),
			)
			return nil
		case message, ok := <-claimMsgChan:
			if !ok {
				klog.V(2).Infof(
					"ConsumeClaim ended for topic: %s, partition: %d (would rerun by manager)\n",
					claim.Topic(),
					claim.Partition(),
				)
				return nil
			}

			select {
			default:
			case <-h.ctx.Done():
				klog.V(2).Infof(
					"ConsumeClaim gracefully shutdown for topic: %s",
					claim.Topic(),
				)
				return nil
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
					"topic:%s: schema changed, %d => %d\n",
					claim.Topic(),
					*lastSchemaId,
					upstreamJobSchemaId,
				)
				msgBatch.Process(h.ctx)
			}
			// Process the batch by size or insert in batch
			msgBatch.Insert(h.ctx, msg)
			*lastSchemaId = upstreamJobSchemaId
		case <-h.maxWaitTicker.C:
			// Process the batch by time
			klog.V(2).Infof(
				"topic:%s: maxWaitSeconds hit",
				claim.Topic(),
			)
			msgBatch.Process(h.ctx)
		}
	}
}
