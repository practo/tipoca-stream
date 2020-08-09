package consumer

import (
	"github.com/Shopify/sarama"
	"github.com/practo/klog/v2"
)

func NewConsumer() consumer {
	return consumer{
		ready:    make(chan bool),
		batchers: new(batchers),
	}
}

// consumer represents a Sarama consumer group consumer
type consumer struct {
	// ready is used to signal the main thread about the readiness
	ready chan bool

	batchers *batchers
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (c consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	klog.V(4).Info("Setting up consumer")
	close(c.ready)
	return nil
}

// Cleanup is run at the end of a session,
// once all ConsumeClaim goroutines have exited
func (c consumer) Cleanup(sarama.ConsumerGroupSession) error {
	klog.V(4).Info("Cleaning up consumer")
	return nil
}

func (c consumer) processMessage(
	session sarama.ConsumerGroupSession,
	message *sarama.ConsumerMessage) error {

	ub, _ := c.batchers.Load(message.Topic)
	if ub == nil {
		klog.Fatalf("Error loading batcher for topic=%s\n", message.Topic)
	}
	b := ub.(*batcher)

	b.batch(message.Topic, message.Partition, message.Value)
	if !b.release() {
		return nil
	}

	// upload should be idempotent
	err := b.upload()
	if err != nil {
		klog.Fatalf("Error uploading to s3, err: %v\n", err)
	}

	err = b.signalLoader()
	if err != nil {
		klog.Fatalf("Error signaling to the loader, err: %v\n", err)
	}

	b.markMessageCommit()

	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (c consumer) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim) error {

	klog.V(4).Info("Starting to consume messages...")
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for message := range claim.Messages() {
		err := c.processMessage(session, message)
		if err != nil {
			klog.Errorf("Error processing: value=%s, timestamp=%v, topic=%s",
				string(message.Value), message.Timestamp, message.Topic)
			continue
		}
		klog.Infof("Message claimed: value=%s, timestamp=%v, topic=%s",
			string(message.Value), message.Timestamp, message.Topic)
	}

	klog.V(4).Info("Shutting down ConsumerClaim.")

	return nil
}
