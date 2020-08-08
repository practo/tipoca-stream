package consumer

import (
    "log"
    "github.com/Shopify/sarama"
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
	close(c.ready)
	return nil
}

// Cleanup is run at the end of a session,
// once all ConsumeClaim goroutines have exited
func (c saramaConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (c saramaConsumer) ConsumeClaim(
    session sarama.ConsumerGroupSession,
    claim sarama.ConsumerGroupClaim) error {

	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for message := range claim.Messages() {
		log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s", string(message.Value), message.Timestamp, message.Topic)
		session.MarkMessage(message, "")
	}

	return nil
}
