package redshiftbatcher

import (
	"bytes"
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/practo/klog/v2"
	"github.com/practo/tipoca-stream/kafka-go/pkg/producer"
	"github.com/practo/tipoca-stream/kafka-go/pkg/redshift"
	loader "github.com/practo/tipoca-stream/kafka-go/pkg/redshiftloader"
	"github.com/practo/tipoca-stream/kafka-go/pkg/s3sink"
	"github.com/practo/tipoca-stream/kafka-go/pkg/serializer"
	"github.com/practo/tipoca-stream/kafka-go/pkg/transformer"
	"github.com/practo/tipoca-stream/kafka-go/pkg/transformer/debezium"
	"github.com/practo/tipoca-stream/kafka-go/pkg/transformer/masker"
	"github.com/spf13/viper"
	"path/filepath"
	"strings"
	"time"
)

type batchProcessor struct {
	topic     string
	partition int32

	// autoCommit to Kafka
	autoCommit bool

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

	// batchSchemaId contains the schema id the batch has
	// all messages in the batch should have same schema id
	batchSchemaId int

	// batchSchemaTable contains the redshift table made from the schema
	batchSchemaTable redshift.Table

	// batchStartOffset is the starting offset of the batch
	// this is useful only for logging and debugging purpose
	batchStartOffset int64

	// batchEndOffset is the ending offset of the batch
	// this is useful only for logging and debugging purpose
	batchEndOffset int64

	// lastCommitedOffset tells the last commitedOffset
	// this is helpful to log at the time of shutdown and can help in debugging
	lastCommittedOffset int64

	// messageTransformer is used to transform debezium events into
	// redshift COPY commands with some annotations
	messageTransformer transformer.MessageTransformer

	// schemaTransfomer is used to transform debezium schema
	// to redshift table
	schemaTransformer transformer.SchemaTransformer

	// msgMasker is used to mask the message based on the configuration
	// provided. Default is always to mask everything.
	// this gets activated only when batcher.mask is set to true
	// and batcher.maskConfigDir is defined.
	msgMasker transformer.MessageTransformer

	// maskMessages stores if the masking is enabled
	maskMessages bool

	// signaler is a kafka producer signaling the load the batch uploaded data
	// TODO: make the producer have interface
	signaler *producer.AvroProducer
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

	signaler, err := producer.NewAvroProducer(
		strings.Split(viper.GetString("kafka.brokers"), ","),
		viper.GetString("kafka.version"),
		viper.GetString("schemaRegistryURL"),
	)
	if err != nil {
		klog.Fatalf("unable to make signaler client, err:%v\n", err)
	}

	var msgMasker transformer.MessageTransformer
	maskMessages := viper.GetBool("batcher.mask")
	if maskMessages {
		msgMasker, err = masker.NewMsgMasker(
			viper.GetString("batcher.maskConfigDir"), topic)
		if err != nil && maskMessages {
			klog.Fatalf("unable to create the masker, err:%v", err)
		}
	}

	return &batchProcessor{
		topic:              topic,
		partition:          partition,
		autoCommit:         viper.GetBool("sarama.autoCommit"),
		session:            session,
		s3sink:             sink,
		s3BucketDir:        viper.GetString("s3sink.bucketDir"),
		bodyBuf:            bytes.NewBuffer(make([]byte, 0, 4096)),
		messageTransformer: debezium.NewMessageTransformer(),
		schemaTransformer: debezium.NewSchemaTransformer(
			viper.GetString("schemaRegistryURL")),
		msgMasker:    msgMasker,
		maskMessages: maskMessages,
		signaler:     signaler,
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
}

func (b *batchProcessor) setBatchId() {
	if b.batchId == maxBatchId {
		klog.V(5).Infof("topic:%s: Resetting batchId to zero.", b.topic)
		b.batchId = 0
	}

	b.batchId += 1
}

func (b *batchProcessor) setS3key(topic string, partition int32, offset int64) {
	b.s3Key = filepath.Join(
		b.s3BucketDir,
		topic,
		fmt.Sprintf(
			"%d_offset_%d_partition.json",
			offset,
			partition),
	)
}

func (b *batchProcessor) commitOffset(datas []interface{}) {
	for i, data := range datas {
		message := data.(*serializer.Message)

		b.session.MarkOffset(
			message.Topic,
			message.Partition,
			message.Offset+1,
			"",
		)
		// TODO: not sure how to avoid any failure when the process restarts
		// while doing this But if the system is idempotent we do not
		// need to worry about this. Since the write to s3 again will
		// just overwrite the same data.
		b.session.Commit()
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

func (b *batchProcessor) handleShutdown() {
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

	b.signaler.Close()
}

func (b *batchProcessor) signalLoad() {
	downstreamTopic := "loader-" + b.topic
	job := loader.NewJob(
		b.topic,
		b.batchStartOffset,
		b.batchEndOffset,
		",",
		b.s3sink.GetKeyURI(b.s3Key),
		b.batchSchemaId, // schema of upstream topic
	)

	err := b.signaler.Add(
		downstreamTopic,
		loader.JobAvroSchema,
		[]byte(time.Now().String()),
		job.ToStringMap(),
	)
	if err != nil {
		klog.Fatalf("Error sending the signal to the loader, err:%v\n", err)
	}
	klog.V(3).Infof(
		"topic:%s, batchId:%d: Signalled the loader.\n",
		b.topic, b.batchId,
	)
}

func (b *batchProcessor) processMessage(message *serializer.Message, id int) {
	now := time.Now()

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
	// also the batchSchemaId is set only at the start of the batch
	if b.s3Key == "" {
		b.batchSchemaId = message.SchemaId
		resp, err := b.schemaTransformer.TransformValue(
			b.topic,
			b.batchSchemaId,
		)
		if err != nil {
			klog.Fatalf(
				"Transforming schema:%d => inputTable failed: %v\n",
				b.batchSchemaId,
				err)
		}
		b.batchSchemaTable = resp.(redshift.Table)

		b.setS3key(message.Topic, message.Partition, message.Offset)

		klog.V(5).Infof("topic:%s, batchId:%d id:%d: s3Key:%s\n",
			b.topic, b.batchId, id, b.s3Key,
		)

		b.batchStartOffset = message.Offset
	}

	if b.batchSchemaId != message.SchemaId {
		klog.Fatalf("topic:%s, schema id mismatch in the batch, %d != %d\n",
			b.topic,
			b.batchSchemaId,
			message.SchemaId,
		)
	}

	err := b.messageTransformer.Transform(message, b.batchSchemaTable)
	if err != nil {
		klog.Fatalf("Error transforming message:%+v, err:%v\n", message, err)
	}

	if b.maskMessages {
		err := b.msgMasker.Transform(message, b.batchSchemaTable)
		if err != nil {
			klog.Fatalf("Error masking message:%+v, err:%v\n", message, err)
		}
	}

	b.bodyBuf.Write(message.Value.([]byte))
	b.bodyBuf.Write([]byte{'\n'})

	klog.V(5).Infof(
		"topic:%s, batchId:%d id:%d: transformed\n",
		b.topic, b.batchId, id,
	)

	b.batchEndOffset = message.Offset
	setBatchMessageProcessingSeconds(now, b.topic)
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
			b.processMessage(data.(*serializer.Message), id)
		}
	}

	return true
}

func (b *batchProcessor) process(workerID int, datas []interface{}) {
	now := time.Now()

	b.setBatchId()
	b.batchSchemaId = -1

	if b.ctxCancelled() {
		return
	}

	klog.Infof("topic:%s, batchId:%d, size:%d: Processing...\n",
		b.topic, b.batchId, len(datas),
	)

	done := b.processBatch(b.session.Context(), datas)
	if !done {
		b.handleShutdown()
		return
	}

	klog.Infof("topic:%s, batchId:%d, size:%d: Uploading...\n",
		b.topic, b.batchId, len(datas),
	)
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

	b.signalLoad()

	if b.autoCommit == false {
		b.commitOffset(datas)
	}

	klog.Infof(
		"topic:%s, batchId:%d, startOffset:%d, endOffset:%d: Processed",
		b.topic, b.batchId, b.batchStartOffset, b.batchEndOffset,
	)

	setBatchProcessingSeconds(now, b.topic)
}
