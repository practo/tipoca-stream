package serializer

import (
	"context"
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
	Process(ctx context.Context, msgBuf []*Message)
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
func (b *MessageBatch) Process(ctx context.Context) {
	if len(b.msgBuf) > 0 {
		klog.V(2).Infof(
			"topic:%s: calling processor...",
			b.topic,
		)
		b.processor.Process(ctx, b.msgBuf)
		b.msgBuf = make([]*Message, 0, b.maxSize)
	} else {
		klog.V(2).Infof(
			"topic:%s: no msgs",
			b.topic,
		)
	}
}

// insert makes the batch and also calls the processor if batchSize >= maxSize
func (b *MessageBatch) Insert(
	ctx context.Context,
	msg *Message,
) {
	b.msgBuf = append(b.msgBuf, msg)
	if len(b.msgBuf) >= b.maxSize {
		klog.V(2).Infof(
			"topic:%s: maxSize hit",
			msg.Topic,
		)
		b.Process(ctx)
	}
}
