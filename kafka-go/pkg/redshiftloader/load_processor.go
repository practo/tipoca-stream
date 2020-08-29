package redshiftloader

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/practo/klog/v2"
	"github.com/practo/tipoca-stream/kafka-go/pkg/redshift"
	"github.com/practo/tipoca-stream/kafka-go/pkg/s3sink"
	"github.com/practo/tipoca-stream/kafka-go/pkg/serializer"
	"github.com/practo/tipoca-stream/kafka-go/pkg/transformer"
	"github.com/spf13/viper"
	"path/filepath"
)

type loadProcessor struct {
	topic     string
	partition int32

	// session is required to commit the offsets on succesfull processing
	session sarama.ConsumerGroupSession

	// s3Sink
	s3sink *s3sink.S3Sink

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

	sink, err := s3sink.NewS3Sink(
		viper.GetString("s3sink.accessKeyId"),
		viper.GetString("s3sink.secretAccessKey"),
		viper.GetString("s3sink.region"),
		viper.GetString("s3sink.bucket"),
	)
	if err != nil {
		klog.Fatalf("Error creating s3 client: %v\n", err)
	}

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
		s3sink:         sink,
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

func (b *loadProcessor) createStagingTable(
	schemaId int, stagingTable redshift.Table) {

	stagingTable.Name = "staged-" + stagingTable.Name
	tx, err := b.redshifter.Begin()
	if err != nil {
		klog.Fatalf("Error creating database tx, err: %v\n", err)
	}

	tableExist, err := b.redshifter.TableExist(
		stagingTable.Meta.Schema, stagingTable.Name,
	)
	if err != nil {
		klog.Fatalf("Error querying table exist, err: %v\n", err)
	}
	if tableExist {
		klog.Fatalf("Staging table: %s already present, did cleanup fail?",
			stagingTable.Name)
	}

	// add columns: kafkaOffset and operation in the staging table
	extraColumns := []redshift.ColInfo{
		redshift.ColInfo{
			Name:       "kafkaOffset",
			Type:       "string",
			DefaultVal: "",
			NotNull:    true,
			PrimaryKey: false,
		},
		redshift.ColInfo{
			Name:       "operation",
			Type:       "string",
			DefaultVal: "",
			NotNull:    true,
			PrimaryKey: false,
		},
	}
	stagingTable.Columns = append(extraColumns, stagingTable.Columns...)

	err = b.redshifter.CreateTable(tx, stagingTable)
	if err != nil {
		klog.Fatalf("Error creating staging table, err: %v\n", err)
	}
	err = tx.Commit()
	if err != nil {
		klog.Fatalf("Error committing tx, err:%v\n", err)
	}
	klog.Infof(
		"topic:%s, schemaId:%d: Staging Table %s created\n",
		b.topic,
		schemaId,
		stagingTable.Name)
}

// migrateSchema construct the "inputTable" using schemaId in the message.
// If the schema and table does not exist it creates and returns.
// If not then it constructs the "targetTable" by querying the database.
// It compares the targetTable and inputTable schema.
// If the schema is same it does anything and returns.
// If the schema is different it migrate targetTable schema to be same as
// inputTable.
// Following migrations are supported:
// Supported: add columns
// Supported: delete columns
// Supported: alter columns
// TODO: NotSupported: row ordering changes and row renames
func (b *loadProcessor) migrateSchema(schemaId int, inputTable redshift.Table) {
	// TODO: add cache here based on schema id and return
	// save some database calls.

	tx, err := b.redshifter.Begin()
	if err != nil {
		klog.Fatalf("Error creating database tx, err: %v\n", err)
	}

	schemaExist, err := b.redshifter.SchemaExist(inputTable.Meta.Schema)
	if err != nil {
		klog.Fatalf("Error querying schema exists, err: %v\n", err)
	}
	if !schemaExist {
		err = b.redshifter.CreateSchema(tx, inputTable.Meta.Schema)
		if err != nil {
			klog.Fatalf("Error creating schema, err: %v\n", err)
		}
		klog.Infof(
			"topic:%s, schemaId:%d: Schema %s created",
			b.topic,
			schemaId,
			inputTable.Meta.Schema)
	}

	tableExist, err := b.redshifter.TableExist(
		inputTable.Meta.Schema, inputTable.Name,
	)
	if err != nil {
		klog.Fatalf("Error querying table exist, err: %v\n", err)
	}
	if !tableExist {
		err = b.redshifter.CreateTable(tx, inputTable)
		if err != nil {
			klog.Fatalf("Error creating table, err: %v\n", err)
		}
		err = tx.Commit()
		if err != nil {
			klog.Fatalf("Error committing tx, err:%v\n", err)
		}
		klog.Infof(
			"topic:%s, schemaId:%d: Table %s created",
			b.topic,
			schemaId,
			inputTable.Name)
		return
	}

	targetTable, err := b.redshifter.GetTableMetadata(
		inputTable.Meta.Schema, inputTable.Name,
	)
	if err != nil {
		klog.Fatalf("Error querying targetTable, err: %v\n", err)
	}

	// UpdateTable computes the schema migration commands and executes it
	// if required else does nothing.
	err = b.redshifter.UpdateTable(tx, inputTable, *targetTable)
	if err != nil {
		klog.Fatalf("Error running schema migration, err: %v\n", err)
	}

	err = tx.Commit()
	if err != nil {
		klog.Fatalf("Error committing tx, err:%v\n", err)
	}
}

// processBatch handles the batch procesing and return true if all completes
// otherwise return false in case of gracefull shutdown signals being captured,
// this helps in cleanly shutting down the batch processing.
func (b *loadProcessor) processBatch(
	ctx context.Context, datas []interface{}) bool {

	var inputTable redshift.Table
	var entries []s3sink.S3ManifestEntry

	for id, data := range datas {
		select {
		case <-ctx.Done():
			return false
		default:
			message := data.(*serializer.Message)
			job := StringMapToJob(message.Value.(map[string]interface{}))
			schemaId := job.SchemaId()

			// this assumes all messages in a batch have same schema id
			if id == 0 {
				klog.V(3).Infof("Processing schema: %+v\n", schemaId)
				resp, err := b.schemaTransformer.Transform(schemaId)
				if err != nil {
					klog.Fatalf(
						"Transforming schema:%d => inputTable:%d failed: %v\n",
						schemaId,
						err)
				}
				inputTable = resp.(redshift.Table)
				b.migrateSchema(schemaId, inputTable)
				b.createStagingTable(schemaId, inputTable)
			}
			entries = append(
				entries,
				s3sink.S3ManifestEntry{
					URL:       job.S3Path(),
					Mandatory: true,
				},
			)
		}
	}

	// upload s3 manifest file to bulk copy data to staging table
	// this is an overwrite operation
	s3ManifestKey := filepath.Join(
		viper.GetString("s3sink.bucketDir"),
		b.topic,
		"manifest.json",
	)
	err := b.s3sink.UploadS3Manifest(s3ManifestKey, entries)
	if err != nil {
		klog.Fatalf("Error uploading manifest: %s to s3, err:%v\n",
			s3ManifestKey)
	}

	// merge:
	// begin transaction
	// 1. keep highest offset per pk in staging table, keep only the
	//    recent representation of the row in staging table.
	// 2. delete all rows in target table by pk which are present in
	//    in staging table
	// 3. delete all the DELETE rows in staging table
	// 4. insert all the rows from staging table to target table
	// 5. drop the staging table
	// end transaction

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
