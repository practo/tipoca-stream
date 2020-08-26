package redshiftloader

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/practo/klog/v2"
	"github.com/practo/tipoca-stream/kafka-go/pkg/redshift"
	"github.com/practo/tipoca-stream/kafka-go/pkg/serializer"
	"github.com/practo/tipoca-stream/kafka-go/pkg/transformer"
	"github.com/spf13/viper"
)

type loadProcessor struct {
	topic     string
	partition int32

	// session is required to commit the offsets on succesfull processing
	session sarama.ConsumerGroupSession

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

	// msgTransformer is used to transform debezium events into
	// redshift COPY commands with some annotations
	msgTransformer transformer.MsgTransformer

	// schemaTransfomer is used to transform debezium schema
	// to redshift table
	schemaTransformer transformer.SchemaTransformer

	// redshifter is the redshift client to perform redshift
	// operations
	redshifter *redshift.Redshift
}

func newLoadProcessor(
	topic string, partition int32,
	session sarama.ConsumerGroupSession) *loadProcessor {

	// TODO: check if session context is not a problem here.
	redshifter, err := redshift.NewRedshift(
		session.Context(), redshift.RedshiftConfig{
			Host:     viper.GetString("redshift.host"),
			Port:     viper.GetString("redshift.port"),
			Database: viper.GetString("redshift.database"),
			User:     viper.GetString("redshift.user"),
			Password: viper.GetString("redshift.password"),
			Timeout:  viper.GetInt("redshift.timeout"),
		},
	)
	if err != nil {
		klog.Fatalf("Error creating redshifter: %v\n", err)
	}

	return &loadProcessor{
		topic:          topic,
		partition:      partition,
		session:        session,
		msgTransformer: transformer.NewMsgTransformer(),
		schemaTransformer: transformer.NewSchemaTransformer(
			viper.GetString("schemaRegistryURL"),
		),
		redshifter: redshifter,
	}
}

// TODO: get rid of this https://github.com/herryg91/gobatch/issues/2
func (b *loadProcessor) ctxCancelled() bool {
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

func (b *loadProcessor) setBatchId() {
	if b.batchId == maxBatchId {
		klog.V(5).Infof("topic:%s: Resetting batchId to zero.")
		b.batchId = 0
	}

	b.batchId += 1
}

func (b *loadProcessor) commitOffset(datas []interface{}) {
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

func (b *loadProcessor) handleShutdown() {
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

func (b *loadProcessor) processMessage(
	message *serializer.Message, id int) error {

	return nil
}

func (b *loadProcessor) migrateSchema(message *serializer.Message) {
	klog.V(3).Infof("message=%+v\n", message)
	job := StringMapToJob(message.Value.(map[string]interface{}))
	klog.V(3).Infof("job=%+v\n", job)
	schemaId := job.SchemaId()
	// TODO: mem cache it later to prevent below computation on every new batch

	resp, err := b.schemaTransformer.Transform(schemaId)
	if err != nil {
		klog.Fatalf("Error fetching schemaId:%d, err: %v\n", err)
	}
	targetTable := resp.(redshift.Table)

	tx, err := b.redshifter.Begin()
	if err != nil {
		klog.Fatalf("Error redshifter tx begin, err: %v\n", err)
	}

	exist, err := b.redshifter.SchemaExist(targetTable.Meta.Schema)
	if err != nil {
		klog.Fatalf("Error checking schema exist, err: %v\n", err)
	}
	if !exist {
		err = b.redshifter.CreateSchema(tx, targetTable.Meta.Schema)
		if err != nil {
			klog.Fatalf("Error creating schema, err: %v\n", err)
		}
	}

	exist, err = b.redshifter.TableExist(
		targetTable.Meta.Schema, targetTable.Name,
	)
	if err != nil {
		klog.Fatalf("Error checking table exist, err: %v\n", err)
	}
	if !exist {
		err = b.redshifter.CreateTable(tx, targetTable)
		if err != nil {
			klog.Fatalf("Error creating table, err: %v\n", err)
		}
		klog.Info(
			"topic:%s, schema_id:%d: Table %s created",
			b.topic,
			schemaId,
			targetTable.Name,
		)
		return
	}

	currentTable, err := b.redshifter.GetTableMetadata(
		targetTable.Meta.Schema, targetTable.Name,
	)
	if err != nil {
		klog.Fatalf("Error getting table schema, err: %v\n", err)
	}

	alterCommands, err := redshift.CheckSchemas(targetTable, *currentTable)
	if err == nil {
		klog.V(5).Info("topic:%s, schema_id:%d: Schema not changed.",
			b.topic,
			schemaId,
			alterCommands,
			err.Error(),
		)
		return
	}

	klog.Info(
		"topic:%s, schema_id:%d: Schema different, alter:%v, msg:%s",
		b.topic,
		schemaId,
		alterCommands,
		err.Error(),
	)
	// TODO:
	// handle schema migration
	return
}

// processBatch handles the batch procesing and return true if all completes
// otherwise return false in case of gracefull shutdown signals being captured,
// this helps in cleanly shutting down the batch processing.
func (b *loadProcessor) processBatch(
	ctx context.Context, datas []interface{}) bool {
	klog.V(3).Infof("Processing batch: %v\n", datas)

	for id, data := range datas {
		klog.V(3).Infof("Processing batch/id: %v\n", id)
		select {
		case <-ctx.Done():
			return false
		default:
			message := data.(*serializer.Message)
			// this assumes all batches have same schema id
			if id == 0 {
				klog.V(3).Infof("Processing schema: %+v\n", message)
				b.migrateSchema(message)
			}
			b.processMessage(message, id)
		}
	}

	return true
}

func (b *loadProcessor) process(workerID int, datas []interface{}) {
	b.setBatchId()
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

	b.commitOffset(datas)

	klog.Infof(
		"topic:%s, batchId:%d, startOffset:%d, endOffset:%d: Processed",
		b.topic, b.batchId, b.batchStartOffset, b.batchEndOffset,
	)
}
