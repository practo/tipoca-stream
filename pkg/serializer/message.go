package serializer

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/practo/klog/v2"
	"sync"
)

type MessageBatchSyncProcessor interface {
	Process(session sarama.ConsumerGroupSession, msgBuf []*Message) error
}

type MessageBatchAsyncProcessor interface {
	Process(
		wg *sync.WaitGroup,
		session sarama.ConsumerGroupSession,
		processChan <-chan []*Message,
		errChan chan<- error,
	)
}

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

type MessageAsyncBatch struct {
	topic       string
	partition   int32
	maxSize     int
	msgBuf      []*Message
	processChan chan []*Message
}

func NewMessageAsyncBatch(
	topic string,
	partition int32,
	maxSize int,
	processChan chan []*Message,
) *MessageAsyncBatch {
	return &MessageAsyncBatch{
		topic:       topic,
		partition:   partition,
		maxSize:     maxSize,
		msgBuf:      make([]*Message, 0, maxSize),
		processChan: processChan,
	}
}

func (b *MessageAsyncBatch) Flush(ctx context.Context) {
	size := len(b.msgBuf)
	if size > 0 {
		// write to channel with context check, fixes #170
		select {
		case <-ctx.Done():
			klog.V(2).Infof("%s: flush cancelled, ctx done, return", b.topic)
			return
		case b.processChan <- b.msgBuf:
		}
		b.msgBuf = make([]*Message, 0, b.maxSize)
		klog.V(4).Infof(
			"%s: flushed:%d, processChan:%v",
			b.topic,
			size,
			len(b.processChan),
		)
	} else {
		klog.V(2).Infof(
			"%s: no msgs",
			b.topic,
		)
	}
}

// insert makes the batch and also and flushes to the processor
// if batchSize >= maxSize
func (b *MessageAsyncBatch) Insert(ctx context.Context, msg *Message) {
	b.msgBuf = append(b.msgBuf, msg)
	if len(b.msgBuf) >= b.maxSize {
		klog.V(2).Infof(
			"%s: maxSize hit",
			msg.Topic,
		)
		b.Flush(ctx)
	}
}

type MessageSyncBatch struct {
	topic     string
	partition int32
	maxSize   int
	msgBuf    []*Message
	processor MessageBatchSyncProcessor
}

func NewMessageSyncBatch(topic string, partition int32, maxSize int, processor MessageBatchSyncProcessor) *MessageSyncBatch {
	return &MessageSyncBatch{
		topic:     topic,
		partition: partition,
		maxSize:   maxSize,
		msgBuf:    make([]*Message, 0, maxSize),
		processor: processor,
	}
}

// process calls the processor to process the batch
func (b *MessageSyncBatch) Process(session sarama.ConsumerGroupSession) error {
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
func (b *MessageSyncBatch) Insert(
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
