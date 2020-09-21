package redshiftloader

import (
	"context"
	"database/sql"
	"github.com/Shopify/sarama"
	"github.com/practo/klog/v2"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/redshift"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/s3sink"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/serializer"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/transformer"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/transformer/debezium"
	"github.com/spf13/viper"
	"path/filepath"
	"strings"
)

const (
	stagingTablePrimaryKey     = "kafkaoffset"
	stagingTablePrimaryKeyType = "character varying(max)"
)

type loadProcessor struct {
	topic         string
	upstreamTopic string
	partition     int32

	// autoCommit to Kafka
	autoCommit bool

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

	// messageTransformer is used to transform debezium events into
	// redshift COPY commands with some annotations
	messageTransformer transformer.MessageTransformer

	// schemaTransfomer is used to transform debezium schema
	// to redshift table
	schemaTransformer transformer.SchemaTransformer

	// redshifter is the redshift client to perform redshift
	// operations
	redshifter *redshift.Redshift

	// redshift schema to operate on
	redshiftSchema string

	// stagingTable is the temp table to merge data into the target table
	stagingTable *redshift.Table

	// targetTable is actual table in redshift
	targetTable *redshift.Table

	// primaryKey is the primary key column for the topic corresponding table
	primaryKey string
}

func newLoadProcessor(
	topic string, partition int32, session sarama.ConsumerGroupSession,
	redshifter *redshift.Redshift) *loadProcessor {

	sink, err := s3sink.NewS3Sink(
		viper.GetString("s3sink.accessKeyId"),
		viper.GetString("s3sink.secretAccessKey"),
		viper.GetString("s3sink.region"),
		viper.GetString("s3sink.bucket"),
	)
	if err != nil {
		klog.Fatalf("Error creating s3 client: %v\n", err)
	}

	return &loadProcessor{
		topic:              topic,
		partition:          partition,
		autoCommit:         viper.GetBool("sarama.autoCommit"),
		session:            session,
		s3sink:             sink,
		messageTransformer: debezium.NewMessageTransformer(),
		schemaTransformer: debezium.NewSchemaTransformer(
			viper.GetString("schemaRegistryURL")),
		redshifter:     redshifter,
		redshiftSchema: viper.GetString("redshift.schema"),
		stagingTable:   nil,
		targetTable:    nil,
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
}

// setBatchId is used for logging, helps in debugging.
func (b *loadProcessor) setBatchId() {
	if b.batchId == maxBatchId {
		klog.V(5).Infof("topic:%s: Resetting batchId to zero.", b.topic)
		b.batchId = 0
	}

	b.batchId += 1
}

func (b *loadProcessor) markOffset(datas []interface{}) {
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
		if b.autoCommit == false {
			b.session.Commit()
		}
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

// handleShutdown is mostly used to log the messages before going down
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

// loadStagingTable loads the batch to redhsift table using
// COPY command. Staging table is a temp table used to merge data and
// finally insert into target table.
func (b *loadProcessor) loadStagingTable(s3ManifestKey string) {
	schema := b.stagingTable.Meta.Schema
	table := b.stagingTable.Name

	tx, err := b.redshifter.Begin()
	if err != nil {
		klog.Fatalf("Error creating database tx, err: %v\n", err)
	}
	err = b.redshifter.Copy(
		tx, schema, table, b.s3sink.GetKeyURI(s3ManifestKey), true, false)
	if err != nil {
		klog.Fatalf("Error loading data in staging table, err:%v\n", err)
	}
	err = tx.Commit()
	if err != nil {
		klog.Fatalf("Error committing tx, err:%v\n", err)
	}

	klog.Infof(
		"topic:%s, batchId:%d: Loaded Staging Table: %s\n",
		b.topic,
		b.batchId,
		table)
}

// deDupeStagingTable keeps the highest offset per pk in the table, keeping
// only the recent representation of the row in staging table, deleting others.
// TODO: de duplication may need optimizations (also measure the time taken)
// https://stackoverflow.com/questions/63664935/redshift-delete-duplicate-records-but-keep-latest/63664982?noredirect=1#comment112581353_63664982
func (b *loadProcessor) deDupeStagingTable(tx *sql.Tx) {
	err := b.redshifter.DeDupe(tx,
		b.stagingTable.Meta.Schema,
		b.stagingTable.Name,
		b.primaryKey,
		stagingTablePrimaryKey,
	)
	if err != nil {
		klog.Fatalf("Deduplication failed, %v\n", err)
	}
}

// deleteCommonRowsInTargetTable removes all the rows from the target table
// that is to be modified in this batch. This is done because we can then
// easily perform inserts in the target table. Also DELETE gets taken care.
func (b *loadProcessor) deleteCommonRowsInTargetTable(tx *sql.Tx) {
	err := b.redshifter.DeleteCommon(tx,
		b.targetTable.Meta.Schema,
		b.stagingTable.Name,
		b.targetTable.Name,
		b.primaryKey,
	)
	if err != nil {
		klog.Fatalf("DeleteCommon failed, %v\n", err)
	}
}

// deleteRowsWithDeleteOpInStagingTable deletes the rows with operation
// DELETE in the staging table. so that the delete gets taken care and
// after this we can freely insert everything in staging table to target table.
func (b *loadProcessor) deleteRowsWithDeleteOpInStagingTable(tx *sql.Tx) {
	err := b.redshifter.DeleteColumn(tx,
		b.stagingTable.Meta.Schema,
		b.stagingTable.Name,
		debezium.OperationColumn,
		debezium.OperationDelete,
	)
	if err != nil {
		klog.Fatalf("DeleteRowsWithDeleteOp failed, %v\n", err)
	}
}

// insertIntoTargetTable uses unload and copy strategy to bulk insert into
// target table. This is the most efficient way to inserting in redshift
// when the source is redshift table.
func (b *loadProcessor) insertIntoTargetTable(tx *sql.Tx) {
	err := b.redshifter.DropColumn(
		tx,
		b.stagingTable.Meta.Schema,
		b.stagingTable.Name,
		stagingTablePrimaryKey,
	)
	if err != nil {
		klog.Fatal(err)
	}

	err = b.redshifter.DropColumn(
		tx,
		b.stagingTable.Meta.Schema,
		b.stagingTable.Name,
		debezium.OperationColumn,
	)
	if err != nil {
		klog.Fatal(err)
	}

	s3CopyDir := filepath.Join(
		viper.GetString("s3sink.bucketDir"), b.topic, "unload_")
	err = b.redshifter.Unload(tx,
		b.stagingTable.Meta.Schema,
		b.stagingTable.Name,
		b.s3sink.GetKeyURI(s3CopyDir),
	)
	if err != nil {
		klog.Fatalf("Unloading staging table to s3 failed, %v\n", err)
	}

	s3ManifestKey := s3CopyDir + "manifest"
	err = b.redshifter.Copy(tx,
		b.targetTable.Meta.Schema,
		b.targetTable.Name,
		b.s3sink.GetKeyURI(s3ManifestKey),
		false,
		true,
	)
	if err != nil {
		klog.Fatalf("Copying data to target table from s3 failed, %v\n", err)
	}
}

// dropTable removes the table only if it exists else returns and does nothing
func (b *loadProcessor) dropTable(schema string, table string) error {
	tableExist, err := b.redshifter.TableExist(
		b.stagingTable.Meta.Schema, b.stagingTable.Name,
	)
	if err != nil {
		return err
	}
	if !tableExist {
		return nil
	}
	tx, err := b.redshifter.Begin()
	if err != nil {
		return err
	}
	err = b.redshifter.DropTable(tx,
		schema,
		table,
	)
	if err != nil {
		return err
	}
	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

// merge:
// begin transaction
// 1. deDupe
// 2. delete all rows in target table by pk which are present in
//    in staging table
// 3. delete all the DELETE rows in staging table
// 4. insert all the rows from staging table to target table
// 5. drop the staging table
// end transaction
func (b *loadProcessor) merge() {
	tx, err := b.redshifter.Begin()
	if err != nil {
		klog.Fatalf("Error creating database tx, err: %v\n", err)
	}

	b.deDupeStagingTable(tx)
	b.deleteCommonRowsInTargetTable(tx)
	b.deleteRowsWithDeleteOpInStagingTable(tx)
	b.insertIntoTargetTable(tx)
	err = tx.Commit()
	if err != nil {
		klog.Fatalf("Error committing tx, err:%v\n", err)
	}
	// error is ignored in the below task since we clean on start
	b.dropTable(b.stagingTable.Meta.Schema, b.stagingTable.Name)
}

// createStagingTable creates a staging table based on the schema id of the
// batch messages.
// this also intializes b.stagingTable
func (b *loadProcessor) createStagingTable(
	schemaId int, inputTable redshift.Table) {

	b.stagingTable = redshift.NewTable(inputTable)
	b.stagingTable.Name = b.stagingTable.Name + "_staged"

	// remove existing primary key if any
	for idx, column := range b.stagingTable.Columns {
		if column.Name == stagingTablePrimaryKey {
			continue
		}
		if column.Name == debezium.OperationColumn {
			continue
		}

		if column.PrimaryKey == true {
			column.PrimaryKey = false
			b.stagingTable.Columns[idx] = column
		}
	}
	err := b.dropTable(b.stagingTable.Meta.Schema, b.stagingTable.Name)
	if err != nil {
		klog.Fatalf("Error dropping staging table: %v\n", err)
	}

	tx, err := b.redshifter.Begin()
	if err != nil {
		klog.Fatalf("Error creating database tx, err: %v\n", err)
	}

	primaryKey, _, err := b.schemaTransformer.TransformKey(
		b.upstreamTopic)
	if err != nil {
		klog.Fatalf("Error getting primarykey for: %s, err: %v\n", b.topic, err)
	}
	b.primaryKey = primaryKey

	// add columns: kafkaOffset and operation in the staging table
	extraColumns := []redshift.ColInfo{
		redshift.ColInfo{
			Name:       stagingTablePrimaryKey,
			Type:       stagingTablePrimaryKeyType,
			DefaultVal: "",
			NotNull:    true,
			PrimaryKey: true,
		},
		redshift.ColInfo{
			Name:       debezium.OperationColumn,
			Type:       debezium.OperationColumnType,
			DefaultVal: "",
			NotNull:    true,
			PrimaryKey: false,
		},
	}
	b.stagingTable.Columns = append(extraColumns, b.stagingTable.Columns...)

	tx, err = b.redshifter.Begin()
	if err != nil {
		klog.Fatalf("Error creating database tx, err: %v\n", err)
	}
	err = b.redshifter.CreateTable(tx, *b.stagingTable)
	if err != nil {
		klog.Fatalf("Error creating staging table, err: %v\n", err)
	}
	err = tx.Commit()
	if err != nil {
		klog.Fatalf("Error committing tx, err:%v\n", err)
	}
	klog.V(3).Infof(
		"topic:%s, schemaId:%d: Staging Table %s created\n",
		b.topic,
		schemaId,
		b.stagingTable.Name)
}

// migrateTable migrates the table since redshift does not support
// ALTER COLUMN for everything. MoreInfo: #40
func (b *loadProcessor) migrateTable(
	inputTable, targetTable redshift.Table) {

	tx, err := b.redshifter.Begin()
	if err != nil {
		klog.Fatalf("Error creating database tx, err: %v\n", err)
	}

	s3CopyDir := filepath.Join(
		viper.GetString("s3sink.bucketDir"),
		b.topic,
		"migrating_unload_",
	)
	unLoadS3Key := b.s3sink.GetKeyURI(s3CopyDir)
	copyS3ManifestKey := b.s3sink.GetKeyURI(s3CopyDir + "manifest")

	err = b.redshifter.ReplaceTable(
		tx, unLoadS3Key, copyS3ManifestKey,
		inputTable, targetTable,
	)
	if err != nil {
		klog.Fatalf("Error migrating table, err:%v\n", err)
	}

	err = tx.Commit()
	if err != nil {
		klog.Fatalf("Error committing tx, err:%v\n", err)
	}
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
// Supported: alter columns (supported via table migration)
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
			exist, err2 := b.redshifter.SchemaExist(inputTable.Meta.Schema)
			if err2 != nil {
				klog.Fatalf("Error checking schema exist, err: %v\n", err2)
			}
			if !exist {
				klog.Fatalf("Error creating schema, err: %v\n", err)
			}
		} else {
			klog.Infof(
				"topic:%s, schemaId:%d: Schema %s created",
				b.topic,
				schemaId,
				inputTable.Meta.Schema,
			)
		}
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
		b.targetTable = redshift.NewTable(inputTable)
		return
	}

	targetTable, err := b.redshifter.GetTableMetadata(
		inputTable.Meta.Schema, inputTable.Name,
	)
	b.targetTable = targetTable
	if err != nil {
		klog.Fatalf("Error querying targetTable, err: %v\n", err)
	}

	// UpdateTable computes the schema migration commands and executes it
	// if required else does nothing.
	migrateTable, err := b.redshifter.UpdateTable(tx, inputTable, *targetTable)
	if err != nil {
		klog.Fatalf("Error running schema migration, err: %v\n", err)
	}

	err = tx.Commit()
	if err != nil {
		klog.Fatalf("Error committing tx, err:%v\n", err)
	}

	if migrateTable == true {
		b.migrateTable(inputTable, *targetTable)
	}
}

// processBatch handles the batch procesing and return true if all completes
// otherwise return false in case of gracefull shutdown signals being captured,
// this helps in cleanly shutting down the batch processing.
func (b *loadProcessor) processBatch(
	ctx context.Context, datas []interface{}) bool {

	var inputTable redshift.Table
	b.stagingTable = nil
	b.targetTable = nil
	b.upstreamTopic = ""

	var entries []s3sink.S3ManifestEntry

	for id, data := range datas {
		select {
		case <-ctx.Done():
			return false
		default:
			message := data.(*serializer.Message)
			job := StringMapToJob(message.Value.(map[string]interface{}))
			schemaId := job.SchemaId
			b.batchEndOffset = message.Offset

			// this assumes all messages in a batch have same schema id
			if id == 0 {
				b.upstreamTopic = job.UpstreamTopic
				klog.V(3).Infof("Processing schema: %+v\n", schemaId)
				maskConfigDir := ""
				if viper.GetBool("loader.mask") {
					maskConfigDir = viper.GetString("loader.maskConfigDir")
				}
				resp, err := b.schemaTransformer.TransformValue(
					b.upstreamTopic,
					schemaId,
					maskConfigDir,
				)
				if err != nil {
					klog.Fatalf(
						"Transforming schema:%d => inputTable failed: %v\n",
						schemaId,
						err)
				}
				inputTable = resp.(redshift.Table)
				inputTable.Meta.Schema = b.redshiftSchema
				// postgres(redshift)
				inputTable.Name = strings.ToLower(inputTable.Name)
				b.migrateSchema(schemaId, inputTable)
				b.createStagingTable(schemaId, inputTable)
			}
			entries = append(
				entries,
				s3sink.S3ManifestEntry{
					URL:       job.S3Path,
					Mandatory: true,
				},
			)
		}
	}

	// upload s3 manifest file to bulk copy data to staging table
	// this is an overwrite operation
	s3ManifestKey := filepath.Join(
		viper.GetString("s3sink.bucketDir"), b.topic, "manifest.json")
	err := b.s3sink.UploadS3Manifest(s3ManifestKey, entries)
	if err != nil {
		klog.Fatalf("Error uploading manifest: %s to s3, err:%v\n",
			s3ManifestKey, err)
	}

	b.loadStagingTable(s3ManifestKey)
	b.merge()

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

	b.markOffset(datas)

	klog.Infof(
		"topic:%s, batchId:%d, startOffset:%d, endOffset:%d: Processed",
		b.topic, b.batchId, b.batchStartOffset, b.batchEndOffset,
	)
}
