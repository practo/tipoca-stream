package consumer

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"

	gobatch "github.com/practo/gobatch"
	"github.com/practo/klog/v2"
)

type batchers = sync.Map

type batcher struct {
	topic string

	maxSize int
	maxWait time.Duration

	mbatch    *gobatch.Batch
	processor *batchProcessor
}

func newBatcher(
	topic string,
	maxSize int,
	maxWaitSeconds int) *batcher {

	return &batcher{
		topic:     topic,
		maxSize:   maxSize,
		maxWait:   time.Second * time.Duration(maxWaitSeconds),
		processor: nil,
		mbatch:    nil,
	}
}

func newMBatch(
	maxSize int,
	maxWait time.Duration,
	process gobatch.BatchFn,
	workers int) *gobatch.Batch {

	return gobatch.NewMemoryBatch(
		maxSize, maxWait, process, workers,
	)
}

type batchProcessor struct {
	topic     string
	partition int32

	// session is required to commit the offsets on succesfull processing
	session sarama.ConsumerGroupSession
}

func newBatchProcessor(
	topic string, partition int32,
	session sarama.ConsumerGroupSession) *batchProcessor {

	return &batchProcessor{
		topic:     topic,
		partition: partition,
		session:   session,
	}
}

func (b batchProcessor) process(workerID int, datas []interface{}) {
	// TODO: process datas
	klog.Infof("topic=%s, batch-size=%d: Processing...\n", b.topic, len(datas))

	// TODO: add a job to load the batch to redshift
	time.Sleep(time.Second * 3)

	// commit processed
	for _, data := range datas {
		// TODO: this should not be required, fix the gobatch code
		if data == nil {
			continue
		}
		message := data.(*sarama.ConsumerMessage)

		b.session.MarkMessage(message, "")
		// TODO: not sure how to avoid any failure when the process restarts
		// while doing this But if the system is idempotent we do not
		// need to worry about this. Since the write to s3 again will
		// just overwrite the same data.
		b.session.Commit()
		klog.Infof(
			"topic=%s, message=%s: Processed\n",
			message.Topic, string(message.Value),
		)
	}
}
