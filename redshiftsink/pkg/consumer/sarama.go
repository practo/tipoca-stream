package consumer

import (
	"github.com/Shopify/sarama"
	"github.com/practo/klog/v2"
)

func NewSaramaConsumer() saramaConsumer {
	return saramaConsumer{
		ready: make(chan bool),
	}
}

// saramaConsumer represents a Sarama consumer group consumer
type saramaConsumer struct {
	// ready is used to signal the main thread about the readiness
	ready chan bool
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (c saramaConsumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	klog.V(4).Info("Setting up saramaConsumer")
	close(c.ready)
	return nil
}

// Cleanup is run at the end of a session,
// once all ConsumeClaim goroutines have exited
func (c saramaConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	klog.V(4).Info("Cleaning up saramaConsumer")
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (c saramaConsumer) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim) error {

	klog.V(4).Info("Starting to consume messages...")
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for message := range claim.Messages() {
		klog.Infof("Message claimed: value = %s, timestamp = %v, topic = %s",
			string(message.Value), message.Timestamp, message.Topic)
		session.MarkMessage(message, "")
	}
	klog.V(4).Info("All messages were consumed, exiting consumerClaim.")

	return nil
}
