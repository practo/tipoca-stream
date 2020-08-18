package consumer

import (
    "bytes"
    "fmt"
    "context"
    "path/filepath"
    "github.com/spf13/viper"
    "github.com/Shopify/sarama"
    "github.com/practo/klog/v2"
    "github.com/practo/tipoca-stream/consumer-go/pkg/s3sink"
    serializr "github.com/practo/tipoca-stream/consumer-go/pkg/serializer"
    transformr "github.com/practo/tipoca-stream/consumer-go/pkg/transformer"
)

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
	// this is useful only for logging and debugging purpose
	batchId int

	// batchStartOffset is the starting offset of the batch
	// this is useful only for logging and debugging purpose
	batchStartOffset int64

	// batchEndOffset is the ending offset of the batch
	// this is useful only for logging and debugging purpose
	batchEndOffset int64

	// lastCommitedOffset tells the last commitedOffset
	// this is helpful to log at the time of shutdown and can help in debugging
	lastCommittedOffset int64

	// serializer is used to Deserialize the message stored in Kafka
	serializer serializr.Serializer

	// transformer is used to transform debezium events into
	// redshift COPY commands with some annotations
	transformer transformr.Transformer
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
		serializer:  serializr.NewSerializer(
			viper.GetString("schemaRegistryURL")),
        transformer: transformr.NewTransformer(),
	}
}

// TODO: get rid of this https://github.com/herryg91/gobatch/issues/2
func (b *batchProcessor) ctxCancelled() bool {
	select {
	case <-b.session.Context().Done():
		klog.Infof(
			"topic:%s, batchId:%d, lastCommittedOffset:%d: Cancelled.\n",
			b.topic, b.batchId, b.lastCommittedOffset,
		)
		return true
	default:
		return false
	}

	return false
}

func (b *batchProcessor) setBatchId() {
	if b.batchId == maxBatchId {
		klog.V(5).Infof("topic:%s: Resetting batchId to zero.")
		b.batchId = 0
	}

	b.batchId += 1
}

func (b *batchProcessor) setS3key(topic string, partition int32, offset int64) {
	b.s3Key = filepath.Join(
		b.s3BucketDir,
		topic,
		fmt.Sprintf(
			"%d_offset_%d_partition.csv",
			offset,
			partition),
	)
}

func (b *batchProcessor) commitOffset(datas []interface{}) {
	for i, data := range datas {
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
		// 99 is an exception
		b.lastCommittedOffset = message.Offset
		var verbosity klog.Level = 5
		if len(datas)-1 == i {
			verbosity = 3
		}
		klog.V(verbosity).Infof(
			"topic:%s, lastCommittedOffset:%d: Processed\n",
			message.Topic, b.lastCommittedOffset,
		)
	}
}

func (b *batchProcessor) shutdownInfo() {
	klog.Infof(
		"topic:%s, batchId:%d: Batch processing gracefully shutdown.\n",
		b.topic,
		b.batchId,
	)
	if b.lastCommittedOffset == 0 {
		klog.Infof("topic:%s: Nothing new was committed.\n", b.topic)
	} else {
		klog.Infof(
			"topic:%s: lastCommittedOffset: %d. Shut down.\n",
			b.topic,
			b.lastCommittedOffset,
		)
	}
}

func (b *batchProcessor) signalLoad(bucket string, key string) {

}

func (b *batchProcessor) processMessage(message *serializr.Message, id int) {
	klog.V(5).Infof(
		"topic:%s, batchId:%d id:%d: transforming\n",
		b.topic, b.batchId, id,
	)
	// V(99) is an expception but required (never try this in prod)
	klog.V(99).Infof(
		"topic:%s, batchId:%d id:%d: message:%+v\n",
		b.topic, b.batchId, id, message,
	)

	// key is always made based on the first not nil message in the batch
	if b.s3Key == "" {
		b.setS3key(message.Topic, message.Partition, message.Offset)

		klog.V(5).Infof("topic:%s, batchId:%d id:%d: s3Key:%s\n",
			b.topic, b.batchId, id, b.s3Key,
		)

		b.batchStartOffset = message.Offset
	}

    err := b.transformer.Transform(message)
    if err != nil {
        klog.Fatalf("Error transforming message:%+v, err:%v\n", message, err)
    }

	b.bodyBuf.Write(message.Value.([]byte))
	b.bodyBuf.Write([]byte{'\n'})

	klog.V(5).Infof(
		"topic:%s, batchId:%d id:%d: transformed\n",
		b.topic, b.batchId, id,
	)

	b.batchEndOffset = message.Offset
}

// processBatch handles the batch procesing and return true if all completes
// otherwise return false in case of gracefull shutdown signals being captured,
// this helps in cleanly shutting down the batch processing.
func (b *batchProcessor) processBatch(
	ctx context.Context, datas []interface{}) bool {

	b.s3Key = ""
	for id, data := range datas {
		select {
		case <-ctx.Done():
			return false
		default:
			// TODO: fix gobatch for this
			if data == nil {
				continue
			}
			message, err := b.serializer.Deserialize(
				data.(*sarama.ConsumerMessage),
			)
			if err != nil {
				klog.Fatalf("Error deserializing binary, err: %s\n", err)
			}
			b.processMessage(message, id)
		}
	}

	return true
}

func (b *batchProcessor) process(workerID int, datas []interface{}) {
	b.setBatchId()
	if b.ctxCancelled() {
		return
	}

	klog.Infof("topic:%s, batchId:%d, size:%d: Processing...\n",
		b.topic, b.batchId, len(datas),
	)

	// TODO: transform the debezium event into redshift commands
	// and create a new buffer
	done := b.processBatch(b.session.Context(), datas)
	if !done {
		b.shutdownInfo()
		return
	}

	err := b.s3sink.Upload(b.s3Key, b.bodyBuf)
	if err != nil {
		klog.Fatalf("Error writing to s3, err=%v\n", err)
	}

	klog.V(3).Infof(
		"topic:%s, batchId:%d, startOffset:%d, endOffset:%d: Uploaded",
		b.topic, b.batchId, b.batchStartOffset, b.batchEndOffset,
	)
	klog.V(5).Infof(
		"topic:%s, batchId:%d: Uploaded: %s",
		b.topic, b.batchId, b.s3Key,
	)

	b.bodyBuf.Truncate(0)

	b.commitOffset(datas)

	klog.Infof(
		"topic:%s, batchId:%d, startOffset:%d, endOffset:%d: Processed",
		b.topic, b.batchId, b.batchStartOffset, b.batchEndOffset,
	)
}
