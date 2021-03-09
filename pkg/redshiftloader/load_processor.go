package redshiftloader

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/practo/klog/v2"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/kafka"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/redshift"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/s3sink"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/serializer"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/transformer"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/transformer/debezium"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/util"
	"github.com/spf13/viper"
	"path/filepath"
	"strings"
	"time"
)

const (
	maxBatchId = 99
)

type loadProcessor struct {
	topic         string
	upstreamTopic string
	partition     int32

	consumerGroupID string

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

	// redshiftSchema schema to operate on
	redshiftSchema string

	// redshiftStats stats show the db stats info in logs when enabled
	redshiftStats bool

	// stagingTable is the temp table to merge data into the target table
	stagingTable *redshift.Table

	// targetTable is actual table in redshift
	targetTable *redshift.Table

	// tableSuffix is used to perform table updates without downtime
	// it will be used by the redshiftsink operator
	// it adds suffix to both staging and target table
	tableSuffix string

	// primaryKeys is the primary key columns for the topics corresponding table
	primaryKeys []string
}

func newLoadProcessor(
	session sarama.ConsumerGroupSession,
	consumerGroupID string,
	topic string,
	partition int32,
	saramaConfig kafka.SaramaConfig,
	redshifter *redshift.Redshift,
) serializer.MessageBatchProcessor {
	sink, err := s3sink.NewS3Sink(
		viper.GetString("s3sink.accessKeyId"),
		viper.GetString("s3sink.secretAccessKey"),
		viper.GetString("s3sink.region"),
		viper.GetString("s3sink.bucket"),
	)
	if err != nil {
		klog.Fatalf("Error creating s3 client: %v\n", err)
	}

	klog.V(3).Infof("%s: auto-commit: %v", topic, saramaConfig.AutoCommit)

	return &loadProcessor{
		session:            session,
		topic:              topic,
		partition:          partition,
		consumerGroupID:    consumerGroupID,
		autoCommit:         saramaConfig.AutoCommit,
		s3sink:             sink,
		messageTransformer: debezium.NewMessageTransformer(),
		schemaTransformer: debezium.NewSchemaTransformer(
			viper.GetString("schemaRegistryURL")),
		redshifter:     redshifter,
		redshiftSchema: viper.GetString("redshift.schema"),
		stagingTable:   nil,
		targetTable:    nil,
		tableSuffix:    viper.GetString("redshift.tableSuffix"),
		redshiftStats:  viper.GetBool("redshift.stats"),
	}
}

func (b *loadProcessor) ctxCancelled(ctx context.Context) error {
	select {
	case <-ctx.Done():
		klog.Warningf(
			"%s, batchId:%d, lastCommitted:%d: main ctx done. Cancelled.\n",
			b.topic, b.batchId, b.lastCommittedOffset,
		)
		return fmt.Errorf("Processing stopped! main ctx done (recreate), ctxErr: %v", ctx.Err())
	case <-b.session.Context().Done():
		klog.Warningf(
			"%s, batchId:%d, lastCommitted:%d: session ctx done. Cancelled.\n",
			b.topic, b.batchId, b.lastCommittedOffset,
		)
		return fmt.Errorf("Processing stopped! session ctx done (recreate), ctxErr: %v", b.session.Context().Err())
	default:
		return nil
	}
}

// setBatchId is used for logging, helps in debugging.
func (b *loadProcessor) setBatchId() {
	if b.batchId == maxBatchId {
		klog.V(5).Infof("%s: Resetting batchId to zero.", b.topic)
		b.batchId = 0
	}

	b.batchId += 1
}

func (b *loadProcessor) markOffset(msgBuf []*serializer.Message) {
	if len(msgBuf) > 0 {
		lastMessage := msgBuf[len(msgBuf)-1]
		klog.V(2).Infof("%s, offset: %v, marking", lastMessage.Topic, lastMessage.Offset+1)
		b.session.MarkOffset(
			lastMessage.Topic,
			lastMessage.Partition,
			lastMessage.Offset+1,
			"",
		)
		klog.V(2).Infof("%s, offset: %v, marked", lastMessage.Topic, lastMessage.Offset+1)

		if b.autoCommit == false {
			klog.V(2).Infof("%s, committing (autoCommit=false)", lastMessage.Topic)
			b.session.Commit()
			klog.V(2).Infof("%s, committed (autoCommit=false)", lastMessage.Topic)
		}

		b.lastCommittedOffset = lastMessage.Offset

	} else {
		klog.Warningf("%s, markOffset not possible for empty batch", b.topic)
	}
}

// printCurrentState is mostly used to log the messages before going down
func (b *loadProcessor) printCurrentState() {
	klog.Infof(
		"%s, batchId:%d: Batch processing gracefully shutdown.\n",
		b.topic,
		b.batchId,
	)
	if b.lastCommittedOffset == 0 {
		klog.Infof("%s: Nothing new was committed.\n", b.topic)
	} else {
		klog.Infof(
			"%s: lastCommittedOffset: %d. Shut down.\n",
			b.topic,
			b.lastCommittedOffset,
		)
	}
}

// loadTable loads the batch to redhsift table using
// COPY command.
func (b *loadProcessor) loadTable(schema, table, s3ManifestKey string) {
	tx, err := b.redshifter.Begin()
	if err != nil {
		klog.Fatalf("Error creating database tx, err: %v\n", err)
	}
	err = b.redshifter.Copy(
		tx, schema, table, b.s3sink.GetKeyURI(s3ManifestKey),
		true, false,
		true, true,
	)
	if err != nil {
		tx.Rollback()
		klog.Fatalf("Error loading data in staging table, err:%v\n", err)
	}
	err = tx.Commit()
	if err != nil {
		klog.Fatalf("Error committing tx, err:%v\n", err)
	}

	klog.V(2).Infof(
		"%s, copied staging\n",
		b.topic,
	)
}

// deDupeStagingTable keeps the highest offset per pk in the table, keeping
// only the recent representation of the row in staging table, deleting others.
// TODO: de duplication may need optimizations (also measure the time taken)
// https://stackoverflow.com/questions/63664935/redshift-delete-duplicate-records-but-keep-latest/63664982?noredirect=1#comment112581353_63664982
func (b *loadProcessor) deDupeStagingTable(tx *sql.Tx) {
	err := b.redshifter.DeDupe(tx,
		b.stagingTable.Meta.Schema,
		b.stagingTable.Name,
		b.primaryKeys,
		transformer.TempTablePrimary,
	)
	if err != nil {
		tx.Rollback()
		klog.Fatalf("Deduplication failed, %v\n", err)
	}
	klog.V(2).Infof("%s, deduped", b.topic)
}

// deleteCommonRowsInTargetTable removes all the rows from the target table
// that is to be modified in this batch. This is done because we can then
// easily perform inserts in the target table. Also DELETE gets taken care.
func (b *loadProcessor) deleteCommonRowsInTargetTable(tx *sql.Tx) {
	err := b.redshifter.DeleteCommon(tx,
		b.targetTable.Meta.Schema,
		b.stagingTable.Name,
		b.targetTable.Name,
		b.primaryKeys,
	)
	if err != nil {
		tx.Rollback()
		klog.Fatalf("DeleteCommon failed, %v\n", err)
	}
	klog.V(2).Infof("%s, deleted common", b.topic)
}

// deleteRowsWithDeleteOpInStagingTable deletes the rows with operation
// DELETE in the staging table. so that the delete gets taken care and
// after this we can freely insert everything in staging table to target table.
func (b *loadProcessor) deleteRowsWithDeleteOpInStagingTable(tx *sql.Tx) {
	err := b.redshifter.DeleteColumn(tx,
		b.stagingTable.Meta.Schema,
		b.stagingTable.Name,
		transformer.TempTableOp,
		serializer.OperationDelete,
	)
	if err != nil {
		tx.Rollback()
		klog.Fatalf("DeleteRowsWithDeleteOp failed, %v\n", err)
	}
	klog.V(2).Infof("%s, deleted delete-op", b.topic)
}

// insertIntoTargetTable uses unload and copy strategy to bulk insert into
// target table. This is the most efficient way to inserting in redshift
// when the source is redshift table.
func (b *loadProcessor) insertIntoTargetTable(tx *sql.Tx) {
	err := b.redshifter.DropColumn(
		tx,
		b.stagingTable.Meta.Schema,
		b.stagingTable.Name,
		transformer.TempTablePrimary,
	)
	if err != nil {
		tx.Rollback()
		klog.Fatal(err)
	}

	err = b.redshifter.DropColumn(
		tx,
		b.stagingTable.Meta.Schema,
		b.stagingTable.Name,
		transformer.TempTableOp,
	)
	if err != nil {
		tx.Rollback()
		klog.Fatal(err)
	}

	s3CopyDir := filepath.Join(
		viper.GetString("s3sink.bucketDir"),
		b.topic,
		util.NewUUIDString(),
		"unload_",
	)
	err = b.redshifter.Unload(tx,
		b.stagingTable.Meta.Schema,
		b.stagingTable.Name,
		b.s3sink.GetKeyURI(s3CopyDir),
		true,
	)
	if err != nil {
		tx.Rollback()
		klog.Fatalf("Unloading staging table to s3 failed, %v\n", err)
	}
	klog.V(2).Infof("%s, unloaded", b.topic)

	s3ManifestKey := s3CopyDir + "manifest"
	err = b.redshifter.Copy(tx,
		b.targetTable.Meta.Schema,
		b.targetTable.Name,
		b.s3sink.GetKeyURI(s3ManifestKey),
		false,
		true,
		true,
		true,
	)
	if err != nil {
		tx.Rollback()
		klog.Fatalf("Copying data to target table from s3 failed, %v\n", err)
	}
	klog.V(2).Infof("%s, copied", b.topic)
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
		tx.Rollback()
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
	// error is warning in the below task since we clean on start
	err = b.dropTable(b.stagingTable.Meta.Schema, b.stagingTable.Name)
	if err != nil {
		klog.Warningf("Dropping the table: %s failed!, err: %v\n",
			b.stagingTable.Name, err)
	}
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
		if column.Name == transformer.TempTablePrimary {
			continue
		}
		if column.Name == transformer.TempTableOp {
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

	primaryKeys, err := b.schemaTransformer.TransformKey(
		b.upstreamTopic)
	if err != nil {
		klog.Fatalf("Error getting primarykey for: %s, err: %v\n", b.topic, err)
	}
	b.primaryKeys = primaryKeys

	// add columns: kafkaOffset and operation in the staging table
	extraColumns := []redshift.ColInfo{
		redshift.ColInfo{
			Name:       transformer.TempTablePrimary,
			Type:       transformer.TempTablePrimaryType,
			DefaultVal: "",
			NotNull:    true,
			PrimaryKey: true,
		},
		redshift.ColInfo{
			Name:       transformer.TempTableOp,
			Type:       transformer.TempTableOpType,
			DefaultVal: "",
			NotNull:    true,
			PrimaryKey: false,
		},
	}
	b.stagingTable.Columns = append(extraColumns, b.stagingTable.Columns...)

	tx, err := b.redshifter.Begin()
	if err != nil {
		klog.Fatalf("Error creating database tx, err: %v\n", err)
	}
	err = b.redshifter.CreateTable(tx, *b.stagingTable)
	if err != nil {
		tx.Rollback()
		klog.Fatalf("Error creating staging table, err: %v\n", err)
	}
	err = tx.Commit()
	if err != nil {
		klog.Fatalf("Error committing tx, err:%v\n", err)
	}
	klog.V(3).Infof(
		"%s, schemaId:%d: Staging Table %s created\n",
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
		util.NewUUIDString(),
		"migrating_unload_",
	)
	unLoadS3Key := b.s3sink.GetKeyURI(s3CopyDir)
	copyS3ManifestKey := b.s3sink.GetKeyURI(s3CopyDir + "manifest")

	err = b.redshifter.ReplaceTable(
		tx, unLoadS3Key, copyS3ManifestKey,
		inputTable, targetTable,
	)
	if err != nil {
		tx.Rollback()
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
	tableExist, err := b.redshifter.TableExist(
		inputTable.Meta.Schema, inputTable.Name,
	)
	if err != nil {
		klog.Fatalf("Error querying table exist, err: %v\n", err)
	}
	if !tableExist {
		tx, err := b.redshifter.Begin()
		if err != nil {
			klog.Fatalf("Error creating database tx, err: %v\n", err)
		}
		err = b.redshifter.CreateTable(tx, inputTable)
		if err != nil {
			tx.Rollback()
			klog.Fatalf(
				"Error creating table: %+v, err: %v\n",
				inputTable, err)
		}
		err = tx.Commit()
		if err != nil {
			klog.Fatalf("Error committing tx, err:%v\n", err)
		}
		klog.V(2).Infof(
			"%s, schemaId:%d: Table %s created",
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
	// if required else does nothing. (it runs in transaction based on strategy)
	migrateTable, err := b.redshifter.UpdateTable(inputTable, *targetTable)
	if err != nil {
		klog.Fatalf("Schema migration failed, err: %v\n", err)
	}

	if migrateTable == true {
		b.migrateTable(inputTable, *targetTable)
	}
}

// processBatch handles the batch procesing and return true if all completes
// otherwise return false in case of gracefull shutdown signals being captured,
// this helps in cleanly shutting down the batch processing.
func (b *loadProcessor) processBatch(
	ctx context.Context,
	msgBuf []*serializer.Message,
) error {

	if b.redshiftStats {
		klog.V(2).Infof("dbstats: %+v\n", b.redshifter.Stats())
	}

	var inputTable redshift.Table
	var schemaId int
	b.stagingTable = nil
	b.targetTable = nil
	b.upstreamTopic = ""

	var entries []s3sink.S3ManifestEntry
	for id, message := range msgBuf {
		select {
		case <-ctx.Done():
			return fmt.Errorf("Main context done, recreate, err: %v", ctx.Err())
		case <-b.session.Context().Done():
			return fmt.Errorf("Session context done, recreate, err: %v", b.session.Context().Err())
		default:
			job := StringMapToJob(message.Value.(map[string]interface{}))
			schemaId = job.SchemaId
			b.batchEndOffset = message.Offset

			// this assumes all messages in a batch have same schema id
			if id == 0 {
				b.batchStartOffset = message.Offset
				klog.V(2).Infof("%s, batchId:%d, startOffset:%v\n",
					b.topic, b.batchId, b.batchStartOffset,
				)
				b.upstreamTopic = job.UpstreamTopic
				klog.V(3).Infof("Processing schema: %+v\n", schemaId)
				resp, err := b.schemaTransformer.TransformValue(
					b.upstreamTopic,
					schemaId,
					job.MaskSchema,
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
				inputTable.Name = strings.ToLower(
					inputTable.Name + b.tableSuffix)
				b.migrateSchema(schemaId, inputTable)
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
	s3ManifestKey := filepath.Join(
		viper.GetString("s3sink.bucketDir"),
		b.topic,
		util.NewUUIDString(),
		"manifest.json")
	err := b.s3sink.UploadS3Manifest(s3ManifestKey, entries)
	if err != nil {
		klog.Fatalf("Error uploading manifest: %s to s3, err:%v\n",
			s3ManifestKey, err)
	}

	klog.V(2).Infof("%s, load staging\n", b.topic)
	b.createStagingTable(schemaId, inputTable)
	b.loadTable(
		b.stagingTable.Meta.Schema,
		b.stagingTable.Name,
		s3ManifestKey,
	)
	b.merge()

	if b.redshiftStats {
		klog.V(2).Infof("endbatch dbstats: %+v\n", b.redshifter.Stats())
	}

	return nil
}

// Process implements serializer.MessageBatch
func (b *loadProcessor) Process(ctx context.Context, msgBuf []*serializer.Message) error {
	start := time.Now()
	b.setBatchId()
	err := b.ctxCancelled(ctx)
	if err != nil {
		return err
	}

	klog.Infof("%s, batchId:%d, size:%d: Processing...\n",
		b.topic, b.batchId, len(msgBuf),
	)

	err = b.processBatch(ctx, msgBuf)
	if err != nil {
		b.printCurrentState()
		return err
	}
	b.markOffset(msgBuf)

	var timeTaken string
	secondsTaken := time.Since(start).Seconds()
	if secondsTaken > 60 {
		timeTaken = fmt.Sprintf("%.0fm", secondsTaken/60)
	} else {
		timeTaken = fmt.Sprintf("%.0fs", secondsTaken)
	}

	klog.Infof(
		"%s, batchId:%d, size:%d, end:%d:, Processed in %s",
		b.topic, b.batchId, len(msgBuf), b.batchEndOffset, timeTaken,
	)

	setMsgsProcessedPerSecond(
		b.consumerGroupID,
		b.topic,
		float64(len(msgBuf))/secondsTaken,
	)

	return nil
}
