package redshiftloader

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/practo/klog/v2"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/kafka"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/redshift"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/serializer"
	"github.com/spf13/viper"
	"sync"
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
	ready   chan bool
	context context.Context

	maxSize       int
	maxWaitTicker *time.Ticker

	msgBuf []*serializer.Message

	saramaConfig kafka.SaramaConfig
	redshifter   *redshift.Redshift
	serializer   serializer.Serializer

	// lock to protect buffer operation
	mu sync.RWMutex
}

func NewHandler(
	context context.Context,
	ready chan bool,
	loaderConfig LoaderConfig,
	saramaConfig kafka.SaramaConfig,
	redshifter *redshift.Redshift,
) loaderHandler {

	return loaderHandler{
		ready:   ready,
		context: context,

		maxSize:       loaderConfig.MaxSize,
		maxWaitTicker: time.NewTicker(time.Duration(loaderConfig.MaxWaitSeconds) * time.Second),

		msgBuf: make([]*serializer.Message, 0, loaderConfig.MaxSize),

		saramaConfig: saramaConfig,
		redshifter:   redshifter,
		serializer:   serializer.NewSerializer(viper.GetString("schemaRegistryURL")),
	}
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (c loaderHandler) Setup(sarama.ConsumerGroupSession) error {
	klog.V(3).Info("Setting up consumer")

	// Mark the consumer as ready
	select {
	case <-c.ready:
		return nil
	default:
	}
	close(c.ready)

	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (c loaderHandler) Cleanup(sarama.ConsumerGroupSession) error {
	klog.V(3).Info("Cleaning up consumer")
	return nil
}

// process calls the load processor to process the batch
func (c loaderHandler) process(processor *loadProcessor) {
	if len(c.msgBuf) > 0 {
		processor.process(c.context, c.msgBuf)
		c.msgBuf = make([]*serializer.Message, 0, c.maxSize)
	}
}

// insert makes the batch and also calls the processor if batchSize >= maxSize
func (c loaderHandler) insert(
	msg *serializer.Message,
	processor *loadProcessor,
) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.msgBuf = append(c.msgBuf, msg)
	if len(c.msgBuf) >= c.maxSize {
		c.process(processor)
	}
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// ConsumeClaim is managed by the consumer.manager routine
func (c loaderHandler) ConsumeClaim(session sarama.ConsumerGroupSession,
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
		c.saramaConfig,
		c.redshifter,
	)

	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	claimMsgChan := claim.Messages()

	for {
		select {
		case <-c.context.Done():
			klog.Infof(
				"ConsumeClaim gracefully shutdown for topic: %s",
				claim.Topic(),
			)
			return nil
		case message, ok := <-claimMsgChan:
			if !ok {
				return nil
			}
			// Deserialize the message
			msg, err := c.serializer.Deserialize(message)
			if err != nil {
				klog.Fatalf("Error deserializing binary, err: %s\n", err)
			}
			if msg == nil || msg.Value == nil {
				klog.Fatalf("Got message as nil, message: %+v\n", msg)
			}

			// Process the batch by schemaID
			job := StringMapToJob(msg.Value.(map[string]interface{}))
			schemaID := job.SchemaId
			if lastSchemaId == nil {
				lastSchemaId = new(int)
			} else if *lastSchemaId != schemaID {
				klog.V(2).Infof(
					"topic:%s: Got new schema (new batch): %d => %d\n",
					claim.Topic(),
					*lastSchemaId,
					schemaID,
				)
				c.process(processor)
			} else {
			}
			// Process the batch by size or insert in batch
			c.insert(msg, processor)
		case <-c.maxWaitTicker.C:
			c.mu.Lock()
			// Process the batch by time
			c.process(processor)
			c.mu.Unlock()
		}
	}

	klog.V(2).Infof(
		"ConsumeClaim ended for topic: %s, partition: %d (would rerun)\n",
		claim.Topic(),
		claim.Partition(),
	)
	return nil
}
