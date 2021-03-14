package redshiftbatcher

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/practo/klog/v2"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/kafka"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/redshift"
	loader "github.com/practo/tipoca-stream/redshiftsink/pkg/redshiftloader"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/s3sink"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/serializer"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/transformer"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/transformer/debezium"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/transformer/masker"
	"github.com/spf13/viper"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	maxBatchId = 99
)

type batchProcessor struct {
	topic             string
	partition         int32
	loaderTopicPrefix string

	consumerGroupID string

	// autoCommit to Kafka
	autoCommit bool

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

	// maskMessages stores if the masking is enabled
	maskSchema map[string]serializer.MaskInfo

	// skipMerge if only create operations are present in batch
	skipMerge bool

	// signaler is a kafka producer signaling the load the batch uploaded data
	// TODO: make the producer have interface
	signaler *kafka.AvroProducer
}

func newBatchProcessor(
	consumerGroupID string,
	topic string,
	partition int32,
	processChan chan []*serializer.Message,
	kafkaConfig kafka.KafkaConfig,
	saramaConfig kafka.SaramaConfig,
	maskConfig masker.MaskConfig,
	kafkaLoaderTopicPrefix string,
) serializer.MessageBatchAsyncProcessor {
	sink, err := s3sink.NewS3Sink(
		viper.GetString("s3sink.accessKeyId"),
		viper.GetString("s3sink.secretAccessKey"),
		viper.GetString("s3sink.region"),
		viper.GetString("s3sink.bucket"),
	)
	if err != nil {
		klog.Fatalf("Error creating s3 client: %v\n", err)
	}

	signaler, err := kafka.NewAvroProducer(
		strings.Split(kafkaConfig.Brokers, ","),
		kafkaConfig.Version,
		viper.GetString("schemaRegistryURL"),
		kafkaConfig.TLSConfig,
	)
	if err != nil {
		klog.Fatalf("unable to make signaler client, err:%v\n", err)
	}

	var msgMasker transformer.MessageTransformer
	maskMessages := viper.GetBool("batcher.mask")
	if maskMessages {
		msgMasker = masker.NewMsgMasker(
			viper.GetString("batcher.maskSalt"),
			topic,
			maskConfig,
		)
	}

	klog.Infof("topic: %v, autoCommit: %v", topic, saramaConfig.AutoCommit)

	return &batchProcessor{
		topic:              topic,
		partition:          partition,
		loaderTopicPrefix:  kafkaLoaderTopicPrefix,
		consumerGroupID:    consumerGroupID,
		autoCommit:         saramaConfig.AutoCommit,
		s3sink:             sink,
		s3BucketDir:        viper.GetString("s3sink.bucketDir"),
		bodyBuf:            bytes.NewBuffer(make([]byte, 0, 4096)),
		messageTransformer: debezium.NewMessageTransformer(),
		schemaTransformer: debezium.NewSchemaTransformer(
			viper.GetString("schemaRegistryURL")),
		msgMasker:    msgMasker,
		maskMessages: maskMessages,
		skipMerge:    true,
		maskSchema:   make(map[string]serializer.MaskInfo),
		signaler:     signaler,
	}
}

func removeEmptyNullValues(value map[string]*string) map[string]*string {
	for cName, cVal := range value {
		if cVal == nil {
			delete(value, cName)
			continue
		}

		if strings.TrimSpace(*cVal) == "" {
			delete(value, cName)
			continue
		}
	}

	return value
}

func (b *batchProcessor) ctxCancelled(ctx context.Context) error {
	select {
	case <-ctx.Done():
		klog.Warningf(
			"%s, batchId:%d, lastCommitted:%d: session ctx done. Cancelled.\n",
			b.topic, b.batchId, b.lastCommittedOffset,
		)
		return kafka.ErrSaramaSessionContextDone
	default:
		return nil
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
	s3FileName := fmt.Sprintf(
		"%d_offset_%d_partition.json",
		offset,
		partition,
	)

	maskFileVersion := viper.GetString("batcher.maskFileVersion")
	if maskFileVersion != "" {
		b.s3Key = filepath.Join(
			b.s3BucketDir,
			topic,
			maskFileVersion,
			s3FileName,
		)
	} else {
		b.s3Key = filepath.Join(
			b.s3BucketDir,
			topic,
			s3FileName,
		)
	}
}

func (b *batchProcessor) markOffset(session sarama.ConsumerGroupSession, msgBuf []*serializer.Message) {
	for i, message := range msgBuf {
		session.MarkOffset(
			message.Topic,
			message.Partition,
			message.Offset+1,
			"",
		)

		// TODO: not sure how to avoid any failure when the process restarts
		// while doing this But if the system is idempotent we do not
		// need to worry about this. Since the write to s3 again will
		// just overwrite the same data.
		// commit only when autoCommit is Off
		if b.autoCommit == false {
			klog.V(2).Infof("topic:%s, Committing (autoCommit=false)", message.Topic)
			session.Commit()
		}
		b.lastCommittedOffset = message.Offset
		var verbosity klog.Level = 5
		if len(msgBuf)-1 == i {
			verbosity = 3
		}
		klog.V(verbosity).Infof(
			"topic:%s, lastCommittedOffset:%d: Processed\n",
			message.Topic, b.lastCommittedOffset,
		)
	}
}

func (b *batchProcessor) handleShutdown() {
	klog.V(2).Infof(
		"topic:%s, batchId:%d: batch processing gracefully shutingdown.\n",
		b.topic,
		b.batchId,
	)
	if b.lastCommittedOffset == 0 {
		klog.Infof("topic:%s: nothing new was committed.\n", b.topic)
	} else {
		klog.V(2).Infof(
			"topic:%s: lastCommittedOffset: %d. shut down.\n",
			b.topic,
			b.lastCommittedOffset,
		)
	}

	b.signaler.Close()
}

func (b *batchProcessor) signalLoad() error {
	downstreamTopic := b.loaderTopicPrefix + b.topic
	klog.V(2).Infof("topic:%s, batchId:%d, skipMerge:%v\n",
		b.topic, b.batchId, b.skipMerge)
	job := loader.NewJob(
		b.topic,
		b.batchStartOffset,
		b.batchEndOffset,
		",",
		b.s3sink.GetKeyURI(b.s3Key),
		b.batchSchemaId, // schema of upstream topic
		b.maskSchema,
		b.skipMerge,
	)

	err := b.signaler.Add(
		downstreamTopic,
		loader.JobAvroSchema,
		[]byte(time.Now().String()),
		job.ToStringMap(),
	)
	if err != nil {
		return fmt.Errorf("Error signalling the loader, err:%v\n", err)
	}
	klog.V(2).Infof(
		"topic:%s, batchId:%d: Signalled loader.\n",
		b.topic, b.batchId,
	)

	return nil
}

func (b *batchProcessor) processMessage(ctx context.Context, message *serializer.Message, id int) error {
	klog.V(5).Infof(
		"topic:%s, batchId:%d id:%d: transforming\n",
		b.topic, b.batchId, id,
	)

	// key is always made based on the first not nil message in the batch
	// also the batchSchemaId is set only at the start of the batch
	if b.s3Key == "" {
		b.batchSchemaId = message.SchemaId
		resp, err := b.schemaTransformer.TransformValue(
			b.topic,
			b.batchSchemaId,
			b.maskSchema,
		)
		if err != nil {
			return fmt.Errorf(
				"transforming schema:%d => inputTable failed: %v\n",
				b.batchSchemaId,
				err,
			)
		}
		b.batchSchemaTable = resp.(redshift.Table)

		b.setS3key(message.Topic, message.Partition, message.Offset)

		b.batchStartOffset = message.Offset
	}

	if b.batchSchemaId != message.SchemaId {
		return fmt.Errorf("topic:%s, schema id mismatch in the batch, %d != %d\n",
			b.topic,
			b.batchSchemaId,
			message.SchemaId,
		)
	}

	err := b.messageTransformer.Transform(message, b.batchSchemaTable)
	if err != nil {
		return fmt.Errorf(
			"Error transforming message:%+v, err:%v\n", message, err,
		)
	}

	if b.maskMessages {
		err := b.msgMasker.Transform(message, b.batchSchemaTable)
		if err != nil {
			return fmt.Errorf("Error masking message:%+v, err:%v\n", message, err)
		}
	}

	message.Value = removeEmptyNullValues(message.Value.(map[string]*string))

	messageValueBytes, err := json.Marshal(message.Value)
	if err != nil {
		return fmt.Errorf("Error marshalling message.Value, message: %+v\n", message)
	}

	b.bodyBuf.Write(messageValueBytes)
	b.bodyBuf.Write([]byte{'\n'})
	if b.maskMessages && len(b.maskSchema) == 0 {
		b.maskSchema = message.MaskSchema
	}

	if message.Operation != serializer.OperationCreate {
		b.skipMerge = false
	}

	klog.V(5).Infof(
		"topic:%s, batchId:%d id:%d: transformed\n",
		b.topic, b.batchId, id,
	)

	b.batchEndOffset = message.Offset

	return nil
}

// processBatch handles the batch procesing and return true if all completes
// otherwise return false in case of gracefull shutdown signals being captured,
// this helps in cleanly shutting down the batch processing.
func (b *batchProcessor) processBatch(
	ctx context.Context,
	msgBuf []*serializer.Message,
) error {

	b.s3Key = ""
	for id, message := range msgBuf {
		select {
		case <-ctx.Done():
			return kafka.ErrSaramaSessionContextDone
		default:
			err := b.processMessage(ctx, message, id)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (b *batchProcessor) ProcessMsgBuf(
	session sarama.ConsumerGroupSession,
	msgBuf []*serializer.Message,
) error {

	now := time.Now()
	b.setBatchId()
	b.batchSchemaId = -1
	b.skipMerge = true
	ctx := session.Context()

	err := b.ctxCancelled(ctx)
	if err != nil {
		return err
	}
	klog.V(2).Infof("topic:%s, batchId:%d, size:%d: Processing...\n",
		b.topic, b.batchId, len(msgBuf),
	)
	err = b.processBatch(ctx, msgBuf)
	if err != nil {
		b.handleShutdown()
		return fmt.Errorf("Error processing batch, err:%v", err)
	}

	klog.V(2).Infof("topic:%s, batchId:%d, size:%d: Uploading...\n",
		b.topic, b.batchId, len(msgBuf),
	)
	err = b.s3sink.Upload(b.s3Key, b.bodyBuf)
	if err != nil {
		return fmt.Errorf("Error writing to s3, err=%v\n", err)
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

	err = b.signalLoad()
	if err != nil {
		return err
	}

	b.markOffset(session, msgBuf)

	klog.V(2).Infof(
		"topic:%s, batchId:%d, startOffset:%d, endOffset:%d: Processed",
		b.topic, b.batchId, b.batchStartOffset, b.batchEndOffset,
	)
	setMsgsProcessedPerSecond(
		b.consumerGroupID,
		b.topic,
		float64(len(msgBuf))/time.Since(now).Seconds(),
	)

	return nil
}

// Process implements serializer.MessageBatchAsyncProcessor
func (b *batchProcessor) Process(
	wg *sync.WaitGroup,
	session sarama.ConsumerGroupSession,
	processChan <-chan []*serializer.Message,
	errChan chan<- error,
) {
	defer wg.Done()

	for {
		select {
		case <-session.Context().Done():
			errChan <- kafka.ErrSaramaSessionContextDone
			return
		case msgBuf := <-processChan:
			err := b.ProcessMsgBuf(session, msgBuf)
			if err != nil {
				errChan <- err
				klog.Errorf(
					"%s, error occured: %v, processor shutdown.",
					b.topic,
					err,
				)
				return
			}
		}
	}
}
