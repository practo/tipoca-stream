package serializer

import (
	"github.com/Shopify/sarama"
	"github.com/practo/klog/v2"
)

type Message struct {
	SchemaId  int
	Topic     string
	Partition int32
	Offset    int64
	Key       string
	Value     interface{}

	Operation  string
	MaskSchema map[string]MaskInfo
}

type MessageBatchProcessor interface {
	Process(session sarama.ConsumerGroupSession, msgBuf []*Message) error
}

type MessageBatch struct {
	topic     string
	partition int32
	maxSize   int
	msgBuf    []*Message
	processor MessageBatchProcessor
}

func NewMessageBatch(topic string, partition int32, maxSize int, processor MessageBatchProcessor) *MessageBatch {
	return &MessageBatch{
		topic:     topic,
		partition: partition,
		maxSize:   maxSize,
		msgBuf:    make([]*Message, 0, maxSize),
		processor: processor,
	}
}

// process calls the processor to process the batch
func (b *MessageBatch) Process(session sarama.ConsumerGroupSession) error {
	if len(b.msgBuf) > 0 {
		klog.V(2).Infof(
			"topic:%s: calling processor...",
			b.topic,
		)
		err := b.processor.Process(session, b.msgBuf)
		if err != nil {
			return err
		}
		b.msgBuf = make([]*Message, 0, b.maxSize)
	} else {
		klog.V(2).Infof(
			"topic:%s: no msgs",
			b.topic,
		)
	}

	return nil
}

// insert makes the batch and also calls the processor if batchSize >= maxSize
func (b *MessageBatch) Insert(
	session sarama.ConsumerGroupSession,
	msg *Message,
) error {
	b.msgBuf = append(b.msgBuf, msg)
	if len(b.msgBuf) >= b.maxSize {
		klog.V(2).Infof(
			"topic:%s: maxSize hit",
			msg.Topic,
		)
		return b.Process(session)
	}

	return nil
}
