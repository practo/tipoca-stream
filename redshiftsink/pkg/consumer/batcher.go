package consumer

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/spf13/viper"

	"github.com/practo/gobatch"
	"github.com/practo/klog/v2"
	"github.com/practo/tipoca-stream/s3sink"
)

const (
	maxBatchId = 99
)

type batchers = sync.Map

type batcher struct {
	topic     string
	config    *BatcherConfig
	mbatch    *gobatch.Batch
	processor *batchProcessor
}

type BatcherConfig struct {
	// Maximum size of a batch, on exceeding this batch is pushed
	// regarless of the wait time.
	MaxSize int `yaml:maxSize,omitempty`

	// MaxWaitSeconds after which the bash would be pushed regardless of its size.
	MaxWaitSeconds int `yaml:maxWaitSeconds,omitempty`
}

func newBatcher(topic string) *batcher {
	c := &BatcherConfig{
		MaxSize:        viper.GetInt("batcher.maxSize"),
		MaxWaitSeconds: viper.GetInt("batcher.maxWaitSeconds"),
	}

	return &batcher{
		topic:     topic,
		config:    c,
		processor: nil,
		mbatch:    nil,
	}
}

func newMBatch(
	maxSize int,
	maxWaitSeconds int,
	process gobatch.BatchFn,
	workers int) *gobatch.Batch {

	return gobatch.NewMemoryBatch(
		maxSize,
		time.Second*time.Duration(maxWaitSeconds),
		process,
		workers,
	)
}

type batchProcessor struct {
	topic     string
	partition int32

	// session is required to commit the offsets on succesfull processing
	session sarama.ConsumerGroupSession

	// s3Sink
	s3sink *s3sink.S3Sink

	// s3BucketDir
	s3BucketDir string

	// s3Key is the key at which the batch data is stored in s3
	s3Key string

	// bodyBuf stores the batch data in buffer before upload
	bodyBuf *bytes.Buffer

	// batchId is a forever increasing number which resets after maxBatchId
	batchId int
}

func newBatchProcessor(
	topic string, partition int32,
	session sarama.ConsumerGroupSession) *batchProcessor {

	sink, err := s3sink.NewS3Sink(
		viper.GetString("s3sink.accessKeyId"),
		viper.GetString("s3sink.secretAccessKey"),
		viper.GetString("s3sink.region"),
		viper.GetString("s3sink.bucket"),
	)
	if err != nil {
		klog.Fatalf("Error creating s3 client: %v\n", err)
	}

	return &batchProcessor{
		topic:       topic,
		partition:   partition,
		session:     session,
		s3sink:      sink,
		s3BucketDir: viper.GetString("s3sink.bucketDir"),
		bodyBuf:     bytes.NewBuffer(make([]byte, 0, 4096)),
	}
}

func (b *batchProcessor) commitOffset(datas []interface{}) {
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

func (b *batchProcessor) signalLoad(bucket string, key string) {

}

func (b *batchProcessor) transform(data *sarama.ConsumerMessage) []byte {
	return []byte{}
}

func (b *batchProcessor) setS3key(topic string, partition int32, offset int64) {
	b.s3Key = filepath.Join(
		b.s3BucketDir,
		topic,
		fmt.Sprintf("partition-%d_offset-%d.csv", partition, offset),
	)
}

func (b *batchProcessor) newtransformedBuffer(datas []interface{}) {
	b.s3Key = ""

	for id, data := range datas {
		// TODO: this should not be required, fix the gobatch code
		if data == nil {
			continue
		}
		message := data.(*sarama.ConsumerMessage)
		klog.V(5).Infof(
			"topic=%s, batchId=%d id=%d: transforming\n",
			b.topic, b.batchId, id)
		klog.V(5).Infof(
			"topic=%s, batchId=%d id=%d: message=%+v\n",
			b.topic, b.batchId, id, message)

		// key is always made based on the first not nil message in the batch
		if b.s3Key == "" {
			b.setS3key(message.Topic, message.Partition, message.Offset)
			klog.V(5).Infof("topic=%s, batchId=%d id=%d: s3Key=%s\n",
				b.topic, b.batchId, id, b.s3Key)
		}

		b.bodyBuf.Write(message.Value)
		b.bodyBuf.Write([]byte{'\n'})
		klog.V(5).Infof(
			"topic=%s, batchId=%d id=%d: transformed\n",
			b.topic, b.batchId, id)
	}
}

func (b *batchProcessor) setBatchId() {
	if b.batchId == maxBatchId {
		klog.V(5).Infof("topic=%s: Resetting batchId to zero.")
		b.batchId = 0
	}

	b.batchId += 1
}

func (b *batchProcessor) process(workerID int, datas []interface{}) {
	b.setBatchId()
	klog.Infof("topic=%s, batchId=%d, size=%d: Processing...\n",
		b.topic, b.batchId, len(datas))

	// TODO: transform the debezium event into redshift commands
	// and create a new buffer
	b.newtransformedBuffer(datas)

	err := b.s3sink.Upload(b.s3Key, b.bodyBuf)
	if err != nil {
		klog.Fatalf("Error writing to s3, err=%v\n", err)
	}
	klog.V(5).Infof(
		"topic=%s, batchId=%d: Uploaded batch to S3 at: %s",
		b.topic, b.batchId, b.s3Key,
	)
	b.bodyBuf.Truncate(0)

	// // TODO: add a job to load the batch to redshift
	// time.Sleep(time.Second * 3)

	b.commitOffset(datas)
}
