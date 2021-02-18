package redshiftloader

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/practo/klog/v2"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/kafka"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/redshift"
)

func NewConsumer(ready chan bool, mainContext context.Context, saramaConfig kafka.SaramaConfig, redshifter *redshift.Redshift) consumer {
	return consumer{
		ready:        ready,
		mainContext:  mainContext,
		saramaConfig: saramaConfig,
		redshifter:   redshifter,

		// loader is initliazed in ConsumeClaim based on the topic it gets
		loader: nil,
	}
}

// consumer represents a Sarama consumer group consumer
// it is actually consumerGroupHandler, kept the name consumer to hide details
type consumer struct {
	// Ready is used to signal the main thread about the readiness
	ready        chan bool
	mainContext  context.Context
	loader       *loader
	saramaConfig kafka.SaramaConfig
	redshifter   *redshift.Redshift
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (c consumer) Setup(sarama.ConsumerGroupSession) error {
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

// Cleanup is run at the end of a session,
// once all ConsumeClaim goroutines have exited
func (c consumer) Cleanup(sarama.ConsumerGroupSession) error {
	klog.V(3).Info("Cleaning up consumer")
	return nil
}

func (c consumer) processMessage(
	session sarama.ConsumerGroupSession,
	message *sarama.ConsumerMessage) error {

	if c.loader.processor == nil {
		c.loader.processor = newLoadProcessor(
			message.Topic, message.Partition,
			session, c.mainContext, c.saramaConfig, c.redshifter,
		)
	}
	// TODO: not sure added below for safety, it may not be required
	c.loader.processor.session = session

	select {
	case <-c.mainContext.Done():
		klog.Info("Graceful shutdown requested, not inserting in batch")
		return nil
	default:
		c.loader.Insert(message)
	}

	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (c consumer) ConsumeClaim(session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim) error {

	klog.V(2).Infof(
		"ConsumeClaim for topic:%s, partition:%d, initalOffset:%d\n",
		claim.Topic(),
		claim.Partition(),
		claim.InitialOffset(),
	)

	c.loader = newLoader(claim.Topic())

	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for message := range claim.Messages() {
		select {
		case <-c.mainContext.Done():
			klog.Infof(
				"%s: Gracefully shutdown. Stopped taking new messages.",
				claim.Topic(),
			)
			return nil
		default:
			err := c.processMessage(session, message)
			if err != nil {
				klog.Errorf(
					"Error processing: value:%s, timestamp:%v, topic:%s",
					string(message.Value), message.Timestamp, message.Topic)
				continue
			}
		}
	}

	klog.V(4).Infof(
		"ConsumeClaim ended for topic: %s, partition: %d\n",
		claim.Topic(),
		claim.Partition(),
	)
	return nil
}
