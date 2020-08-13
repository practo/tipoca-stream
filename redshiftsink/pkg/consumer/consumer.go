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

	// load the batcher for the topic
	ub, _ := c.batchers.Load(message.Topic)
	if ub == nil {
		klog.Fatalf("Error loading batcher for topic:%s\n", message.Topic)
	}
	b := ub.(*batcher)

	if b.processor == nil {
		b.processor = newBatchProcessor(
			message.Topic, message.Partition, session)
	}
	// TODO: not sure added below for safety, it may not be required
	b.processor.session = session

	if b.mbatch == nil {
		b.mbatch = newMBatch(
			b.config.MaxSize,
			b.config.MaxWaitSeconds,
			b.processor.process,
			1,
		)
	}

	select {
	case <-b.processor.session.Context().Done():
		klog.Info("Graceful shutdown requested, not inserting in batch")
		return nil
	default:
		b.mbatch.Insert(message)
	}

	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (c consumer) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim) error {

	klog.V(2).Infof(
		"ConsumeClaim for topic:%s, partition:%d, initalOffset:%d\n",
		claim.Topic(),
		claim.Partition(),
		claim.InitialOffset(),
	)
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for message := range claim.Messages() {
		select {
		case <-session.Context().Done():
			klog.Infof(
				"Gracefully shutdown. Stopped taking new messages.")
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
		"ConsumeClaim shut down for topic: %s, partition: %d\n",
		claim.Topic(),
		claim.Partition(),
	)
	return nil
}
