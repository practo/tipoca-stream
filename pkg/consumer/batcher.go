package consumer

import (
	"bytes"
	"path/filepath"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/spf13/viper"

	"github.com/practo/gobatch"
	"github.com/practo/klog/v2"
	"github.com/practo/tipoca-stream/s3sink"
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

	// s3Sink
	s3sink *s3sink.S3Sink

	// s3Key is the key at which the batch data is stored in s3
	s3Key string

	// bodyBuf stores the batch data in buffer before upload
	bodyBuf *bytes.Buffer
}

func newBatchProcessor(
	topic string, partition int32,
	session sarama.ConsumerGroupSession) *batchProcessor {

	sink, err := s3sink.NewS3Sink(
		viper.GetString("s3.access_key_id"),
		viper.GetString("s3.secret_access_key"),
		viper.GetString("s3.region"),
		viper.GetString("s3.bucket"),
		viper.GetString("s3.bucket_dir"),
	)
	if err != nil {
		klog.Fatalf("Error creating s3 client: %v\n", err)
	}

	return &batchProcessor{
		topic:     topic,
		partition: partition,
		session:   session,
		s3sink:    sink,
		bodyBuf:   bytes.NewBuffer(make([]byte, 0, 4096)),
	}
}

func (b batchProcessor) commitOffset(datas []interface{}) {
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

func (b batchProcessor) signalLoad(bucket string, key string) {

}

func (b batchProcessor) transform(data *sarama.ConsumerMessage) []byte {
	return []byte{}
}

func (b batchProcessor) setS3key(topic string, partition int32, offset int64) {
	b.s3Key = filepath.Join(topic, string(partition), string(offset))
}

func (b batchProcessor) transformedBuffer(datas []interface{}) {
	b.s3Key = ""

	for _, data := range datas {
		// TODO: this should not be required, fix the gobatch code
		if data == nil {
			continue
		}
		message := data.(*sarama.ConsumerMessage)

		// key is always made based on the first not nil message in the batch
		if b.s3Key == "" {
			b.setS3key(message.Topic, message.Partition, message.Offset)
		}

		b.bodyBuf.Write(message.Value)
		b.bodyBuf.Write([]byte{'\n'})
	}
}

func (b batchProcessor) process(workerID int, datas []interface{}) {
	klog.Infof("topic=%s, batch-size=%d: Processing...\n", b.topic, len(datas))

	// TODO: transform the debezium event
	// b.transformedBuffer(datas)

	// err := b.s3sink.upload(b.s3Key, b.bodyBuf)
	// if err != nil {
	// klog.Fatalf("Error writing to s3, err=%v\n", err)
	// }

	// TODO: add a job to load the batch to redshift
	time.Sleep(time.Second * 3)

	b.commitOffset(datas)

}
