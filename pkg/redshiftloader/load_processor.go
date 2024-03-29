package redshiftloader

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/practo/klog/v2"
	"github.com/practo/tipoca-stream/pkg/kafka"
	"github.com/practo/tipoca-stream/pkg/redshift"
	"github.com/practo/tipoca-stream/pkg/s3sink"
	"github.com/practo/tipoca-stream/pkg/serializer"
	"github.com/practo/tipoca-stream/pkg/transformer"
	"github.com/practo/tipoca-stream/pkg/transformer/debezium"
	"github.com/practo/tipoca-stream/pkg/util"
	"github.com/spf13/viper"
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

	// redshiftGroup to gives access to
	redshiftGroup *string

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

	// metricSetter sets the load metrics
	metric metricSetter

	// schemaTargetTable is the cache used to get the targetTable from
	// schema ID without doing recomputation for the schema id
	schemaTargetTable map[int]redshift.Table
}

func newLoadProcessor(
	consumerGroupID string,
	topic string,
	partition int32,
	saramaConfig kafka.SaramaConfig,
	redshifter *redshift.Redshift,
	redshiftGroup *string,
	metric metricSetter,
) (serializer.MessageBatchSyncProcessor, error) {
	sink, err := s3sink.NewS3Sink(
		viper.GetString("s3sink.accessKeyId"),
		viper.GetString("s3sink.secretAccessKey"),
		viper.GetString("s3sink.region"),
		viper.GetString("s3sink.bucket"),
	)
	if err != nil {
		return nil, fmt.Errorf("Error creating s3 client: %v\n", err)
	}

	klog.V(3).Infof("%s: auto-commit: %v", topic, saramaConfig.AutoCommit)

	return &loadProcessor{
		topic:              topic,
		partition:          partition,
		consumerGroupID:    consumerGroupID,
		autoCommit:         saramaConfig.AutoCommit,
		s3sink:             sink,
		messageTransformer: debezium.NewMessageTransformer(),
		schemaTransformer: debezium.NewSchemaTransformer(
			viper.GetString("schemaRegistryURL")),
		redshifter:        redshifter,
		redshiftSchema:    viper.GetString("redshift.schema"),
		redshiftGroup:     redshiftGroup,
		stagingTable:      nil,
		targetTable:       nil,
		tableSuffix:       viper.GetString("redshift.tableSuffix"),
		redshiftStats:     viper.GetBool("redshift.stats"),
		metric:            metric,
		schemaTargetTable: make(map[int]redshift.Table),
	}, nil
}

func (b *loadProcessor) ctxCancelled(ctx context.Context) error {
	select {
	case <-ctx.Done():
		klog.Warningf(
			"%s, batchId:%d, lastCommitted:%d: session ctx done. Cancelled.\n",
			b.topic, b.batchId, b.lastCommittedOffset,
		)
		return ctx.Err()
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

func (b *loadProcessor) markOffset(session sarama.ConsumerGroupSession, msgBuf []*serializer.Message) {
	if len(msgBuf) > 0 {
		lastMessage := msgBuf[len(msgBuf)-1]
		klog.V(2).Infof("%s, offset: %v, marking", lastMessage.Topic, lastMessage.Offset+1)
		session.MarkOffset(
			lastMessage.Topic,
			lastMessage.Partition,
			lastMessage.Offset+1,
			"",
		)
		klog.V(2).Infof("%s, offset: %v, marked", lastMessage.Topic, lastMessage.Offset+1)

		if b.autoCommit == false {
			klog.V(2).Infof("%s, committing (autoCommit=false)", lastMessage.Topic)
			session.Commit()
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
func (b *loadProcessor) loadTable(
	ctx context.Context,
	tx *sql.Tx,
	schema, table, s3ManifestKey string,
) error {
	err := b.redshifter.Copy(
		ctx, tx, schema, table, b.s3sink.GetKeyURI(s3ManifestKey),
		true, false,
		true, true,
	)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("Error loading data in staging table, err:%v\n", err)
	}
	klog.V(2).Infof(
		"%s, copied staging\n",
		b.topic,
	)

	return nil
}

// deDupeStagingTable keeps the highest offset per pk in the table, keeping
// only the recent representation of the row in staging table, deleting others.
// TODO: de duplication may need optimizations (also measure the time taken)
// https://stackoverflow.com/questions/63664935/redshift-delete-duplicate-records-but-keep-latest/63664982?noredirect=1#comment112581353_63664982
func (b *loadProcessor) deDupeStagingTable(ctx context.Context, tx *sql.Tx) error {
	err := b.redshifter.DeDupe(ctx, tx,
		b.stagingTable.Meta.Schema,
		b.stagingTable.Name,
		b.primaryKeys,
		transformer.TempTablePrimary,
	)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("Deduplication failed, %v\n", err)
	}
	klog.V(2).Infof("%s, deduped", b.topic)

	return nil
}

// deleteCommonRowsInTargetTable removes all the rows from the target table
// that is to be modified in this batch. This is done because we can then
// easily perform inserts in the target table. Also DELETE gets taken care.
func (b *loadProcessor) deleteCommonRowsInTargetTable(ctx context.Context, tx *sql.Tx) error {
	err := b.redshifter.DeleteCommon(ctx, tx,
		b.targetTable.Meta.Schema,
		b.stagingTable.Name,
		b.targetTable.Name,
		b.primaryKeys,
	)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("DeleteCommon failed, %v\n", err)
	}
	klog.V(2).Infof("%s, deleted common", b.topic)

	return nil
}

// deleteRowsWithDeleteOpInStagingTable deletes the rows with operation
// DELETE in the staging table. so that the delete gets taken care and
// after this we can freely insert everything in staging table to target table.
func (b *loadProcessor) deleteRowsWithDeleteOpInStagingTable(ctx context.Context, tx *sql.Tx) error {
	err := b.redshifter.DeleteColumn(ctx, tx,
		b.stagingTable.Meta.Schema,
		b.stagingTable.Name,
		transformer.TempTableOp,
		serializer.OperationDelete,
	)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("DeleteRowsWithDeleteOp failed, %v\n", err)
	}
	klog.V(2).Infof("%s, deleted delete-op", b.topic)

	return nil
}

// insertIntoTargetTable uses unload and copy strategy to bulk insert into
// target table. This is the most efficient way to inserting in redshift
// when the source is redshift table.
func (b *loadProcessor) insertIntoTargetTable(ctx context.Context, tx *sql.Tx) error {
	err := b.redshifter.DropColumn(
		ctx,
		tx,
		b.stagingTable.Meta.Schema,
		b.stagingTable.Name,
		transformer.TempTablePrimary,
	)
	if err != nil {
		tx.Rollback()
		return err
	}

	err = b.redshifter.DropColumn(
		ctx,
		tx,
		b.stagingTable.Meta.Schema,
		b.stagingTable.Name,
		transformer.TempTableOp,
	)
	if err != nil {
		tx.Rollback()
		return err
	}

	s3CopyDir := filepath.Join(
		viper.GetString("s3sink.bucketDir"),
		b.consumerGroupID,
		b.topic,
		util.NewUUIDString(),
		"unload_",
	)
	err = b.redshifter.Unload(ctx, tx,
		b.stagingTable.Meta.Schema,
		b.stagingTable.Name,
		b.s3sink.GetKeyURI(s3CopyDir),
		true,
	)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("Unloading staging table to s3 failed, %v\n", err)
	}
	klog.V(2).Infof("%s, unloaded", b.topic)

	s3ManifestKey := s3CopyDir + "manifest"
	err = b.redshifter.Copy(ctx, tx,
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
		return fmt.Errorf("Copying data to target table from s3 failed, %v\n", err)
	}
	klog.V(2).Infof("%s, copied", b.topic)

	return nil
}

// dropTable removes the table only if it exists else returns and does nothing
func (b *loadProcessor) dropTable(ctx context.Context, schema string, table string) error {
	tableExist, err := b.redshifter.TableExist(
		ctx, b.stagingTable.Meta.Schema, b.stagingTable.Name,
	)
	if err != nil {
		return err
	}
	if !tableExist {
		return nil
	}
	tx, err := b.redshifter.Begin(ctx)
	if err != nil {
		return err
	}
	err = b.redshifter.DropTable(ctx, tx,
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
func (b *loadProcessor) merge(ctx context.Context) error {
	start := time.Now()

	tx, err := b.redshifter.Begin(ctx)
	if err != nil {
		return fmt.Errorf("Error creating database tx, err: %v\n", err)
	}

	err = b.deDupeStagingTable(ctx, tx)
	if err != nil {
		return err
	}
	b.metric.setDedupeSeconds(time.Since(start).Seconds())

	start = time.Now()
	err = b.deleteCommonRowsInTargetTable(ctx, tx)
	if err != nil {
		return err
	}
	b.metric.setDeleteCommonSeconds(time.Since(start).Seconds())

	start = time.Now()
	err = b.deleteRowsWithDeleteOpInStagingTable(ctx, tx)
	if err != nil {
		return err
	}
	b.metric.setDeleteOpStageSeconds(time.Since(start).Seconds())

	start = time.Now()
	err = b.insertIntoTargetTable(ctx, tx)
	if err != nil {
		return err
	}
	b.metric.setCopyTargetSeconds(time.Since(start).Seconds())

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("Error committing tx, err:%v\n", err)
	}

	err = b.dropTable(ctx, b.stagingTable.Meta.Schema, b.stagingTable.Name)
	if err != nil {
		klog.Warningf("Dropping the table: %s failed!, err: %v\n",
			b.stagingTable.Name,
			err,
		)
	}

	return nil
}

// loadStagingTable creates a staging table based on the schema id of the
// batch messages and loads it
// this also intializes b.stagingTable
func (b *loadProcessor) loadStagingTable(
	ctx context.Context,
	schemaId int,
	schemaIdKey int,
	inputTable redshift.Table,
	s3ManifestKey string,
) error {
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

	var err error
	var primaryKeys []string
	if schemaIdKey == -1 || schemaIdKey == 0 { // Deprecated as below is expensive and does not use cache
		primaryKeys, err = b.schemaTransformer.TransformKey(b.upstreamTopic)
	} else { // below is the new faster way to get primary keys
		primaryKeys, err = b.schemaTransformer.PrimaryKeys(schemaIdKey)
	}
	if err != nil {
		return fmt.Errorf("Error getting primarykey for: %s, err: %v\n", b.topic, err)
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

	tx, err := b.redshifter.Begin(ctx)
	if err != nil {
		return fmt.Errorf("Error creating database tx, err: %v\n", err)
	}
	err = b.redshifter.CreateTable(ctx, tx, *b.stagingTable, true)
	if err != nil {
		tx.Rollback()
		orgiErr := fmt.Errorf("Error creating staging table, err: %v\n", err)
		err = b.dropTable(ctx, b.stagingTable.Meta.Schema, b.stagingTable.Name)
		if err != nil {
			klog.Errorf("Error trying to drop staging table: %v\n", err)
		}
		return orgiErr
	}
	klog.V(2).Infof("%s, created staging table", b.topic)
	err = b.loadTable(
		ctx,
		tx,
		b.stagingTable.Meta.Schema,
		b.stagingTable.Name,
		s3ManifestKey,
	)
	if err != nil {
		return err
	}
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("Error committing tx, err:%v\n", err)
	}
	klog.V(3).Infof(
		"%s, schemaId:%d: loaded staging %s \n",
		b.topic,
		schemaId,
		b.stagingTable.Name,
	)

	return nil
}

// migrateTable migrates the table since redshift does not support
// ALTER COLUMN for everything. MoreInfo: #40
func (b *loadProcessor) migrateTable(
	ctx context.Context, inputTable, targetTable redshift.Table) error {

	tx, err := b.redshifter.Begin(ctx)
	if err != nil {
		return fmt.Errorf("Error creating database tx, err: %v\n", err)
	}

	s3CopyDir := filepath.Join(
		viper.GetString("s3sink.bucketDir"),
		b.consumerGroupID,
		b.topic,
		util.NewUUIDString(),
		"migrating_unload_",
	)
	unLoadS3Key := b.s3sink.GetKeyURI(s3CopyDir)
	copyS3ManifestKey := b.s3sink.GetKeyURI(s3CopyDir + "manifest")

	err = b.redshifter.ReplaceTable(
		ctx,
		tx, unLoadS3Key, copyS3ManifestKey,
		inputTable, targetTable,
	)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("Error migrating table, err:%v\n", err)
	}

	if b.redshiftGroup != nil {
		klog.V(2).Infof("%s, granting schema access for table: %v to group: %v", b.topic, inputTable.Name, *b.redshiftGroup)
		err = b.redshifter.GrantSchemaAccess(ctx, tx, inputTable.Meta.Schema, inputTable.Name, *b.redshiftGroup)
		if err != nil {
			return err
		}
		return err
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("Error committing tx, err:%v\n", err)
	}

	return nil
}

// migrateSchema construct the "inputTable" using schemaId in the message.
// If it has processed the schemaID before it returns, this is done for reducing queries to redshift.
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
func (b *loadProcessor) migrateSchema(ctx context.Context, schemaId int, inputTable redshift.Table) error {
	targetTableCache, ok := b.schemaTargetTable[schemaId]
	if ok {
		klog.V(2).Infof("%s, using cache for targetTable", b.topic)
		b.targetTable = &targetTableCache
		return nil
	}

	tableExist, err := b.redshifter.TableExist(
		ctx, inputTable.Meta.Schema, inputTable.Name,
	)
	if err != nil {
		return fmt.Errorf("Error querying table exist, err: %v\n", err)
	}
	if !tableExist {
		tx, err := b.redshifter.Begin(ctx)
		if err != nil {
			return fmt.Errorf("Error creating database tx, err: %v\n", err)
		}
		err = b.redshifter.CreateTable(ctx, tx, inputTable, false)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf(
				"Error creating table: %+v, err: %v\n",
				inputTable, err,
			)
		}
		err = tx.Commit()
		if err != nil {
			return fmt.Errorf("Error committing tx, err:%v\n", err)
		}
		klog.V(2).Infof(
			"%s, schemaId:%d: created table %s",
			b.topic,
			schemaId,
			inputTable.Name,
		)
		b.targetTable = redshift.NewTable(inputTable)
		b.schemaTargetTable[schemaId] = *b.targetTable
		return nil
	}

	targetTable, err := b.redshifter.GetTableMetadata(
		ctx, inputTable.Meta.Schema, inputTable.Name,
	)
	b.targetTable = targetTable
	b.schemaTargetTable[schemaId] = *b.targetTable
	if err != nil {
		return fmt.Errorf("Error querying targetTable, err: %v\n", err)
	}

	// UpdateTable computes the schema migration commands and executes it
	// if required else does nothing. (it runs in transaction based on strategy)
	migrateTable, err := b.redshifter.UpdateTable(ctx, inputTable, *targetTable)
	if err != nil {
		return fmt.Errorf("Schema migration failed, err: %v\n", err)
	}

	if migrateTable == true {
		return b.migrateTable(ctx, inputTable, *targetTable)
	}

	return nil
}

// processBatch handles the batch procesing and return true if all completes
// otherwise return false in case of gracefull shutdown signals being captured,
// this helps in cleanly shutting down the batch processing.
func (b *loadProcessor) processBatch(
	ctx context.Context,
	msgBuf []*serializer.Message,
) (
	int64,
	error,
) {
	var bytesProcessed int64
	if b.redshiftStats {
		klog.V(2).Infof("dbstats: %+v\n", b.redshifter.Stats())
	}

	var inputTable redshift.Table
	var err error
	var schemaId, schemaIdKey int
	b.stagingTable = nil
	b.targetTable = nil
	b.upstreamTopic = ""

	var eventsInfoMissing bool
	var entries []s3sink.S3ManifestEntry
	var totalCreateEvents, totalUpdateEvents, totalDeleteEvents int64
	for id, message := range msgBuf {
		select {
		case <-ctx.Done():
			return bytesProcessed, fmt.Errorf(
				"session ctx done, err: %v", ctx.Err())
		default:
			job := StringMapToJob(message.Value.(map[string]interface{}))
			// backward comaptibility
			if job.CreateEvents <= 0 && job.UpdateEvents <= 0 && job.DeleteEvents <= 0 {
				klog.V(2).Infof("%s, events info missing", b.topic)
				eventsInfoMissing = true
			}
			totalCreateEvents += job.CreateEvents
			totalUpdateEvents += job.UpdateEvents
			totalDeleteEvents += job.DeleteEvents

			schemaId = job.SchemaId
			schemaIdKey = job.SchemaIdKey
			b.batchEndOffset = message.Offset
			bytesProcessed += job.BatchBytes

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
					schemaIdKey,
					job.MaskSchema,
					job.ExtraMaskSchema,
				)
				if err != nil {
					return bytesProcessed, fmt.Errorf(
						"Transforming schema:%d => inputTable failed: %v\n",
						schemaId,
						err,
					)
				}
				inputTable = resp.(redshift.Table)
				inputTable.Meta.Schema = b.redshiftSchema
				// postgres(redshift)
				inputTable.Name = strings.ToLower(
					inputTable.Name + b.tableSuffix)
				err = b.migrateSchema(ctx, schemaId, inputTable)
				if err != nil {
					return bytesProcessed, err
				}
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
		b.consumerGroupID,
		b.topic,
		util.NewUUIDString(),
		"manifest.json",
	)
	err = b.s3sink.UploadS3Manifest(s3ManifestKey, entries)
	if err != nil {
		return bytesProcessed, fmt.Errorf(
			"Error uploading manifest: %s to s3, err:%v\n",
			s3ManifestKey,
			err,
		)
	}

	allowMerge := true
	if !eventsInfoMissing {
		if totalCreateEvents > 0 && totalUpdateEvents == 0 && totalDeleteEvents == 0 {
			allowMerge = false
		}
	}

	klog.V(2).Infof("%s, create:%v, update:%v, delete:%v events", b.topic, totalCreateEvents, totalUpdateEvents, totalDeleteEvents)

	if allowMerge {
		// load data in target using staging table merge
		start := time.Now()
		klog.V(2).Infof("%s, load staging (using merge)", b.topic)
		err = b.loadStagingTable(
			ctx,
			schemaId,
			schemaIdKey,
			inputTable,
			s3ManifestKey,
		)
		if err != nil {
			return bytesProcessed, err
		}
		b.metric.setCopyStageSeconds(time.Since(start).Seconds())

		// merge and load in target
		err = b.merge(ctx)
		if err != nil {
			return bytesProcessed, err
		}
	} else {
		// directy load data in target table as there is no update or delete
		klog.V(2).Infof("%s, load target (skipping merge)", b.topic)
		tx, err := b.redshifter.Begin(ctx)
		if err != nil {
			return bytesProcessed, fmt.Errorf("Error creating database tx, err: %v\n", err)
		}
		err = b.loadTable(
			ctx,
			tx,
			b.targetTable.Meta.Schema,
			b.targetTable.Name,
			s3ManifestKey,
		)
		if err != nil {
			return bytesProcessed, err
		}
		err = tx.Commit()
		if err != nil {
			return bytesProcessed, fmt.Errorf("Error committing tx, err:%v\n", err)
		}
	}

	if b.redshiftStats {
		klog.V(3).Infof("endbatch dbstats: %+v\n", b.redshifter.Stats())
	}

	return bytesProcessed, nil
}

// Process implements serializer.MessageBatchSyncProcessor
func (b *loadProcessor) Process(session sarama.ConsumerGroupSession, msgBuf []*serializer.Message) error {
	start := time.Now()
	b.metric.setStartRunning()
	defer b.metric.setStopRunning()

	b.setBatchId()
	ctx := session.Context()

	err := b.ctxCancelled(ctx)
	if err != nil {
		return err
	}
	klog.Infof("%s, batchId:%d, size:%d: processing...\n",
		b.topic, b.batchId, len(msgBuf),
	)
	bytesProcessed, err := b.processBatch(ctx, msgBuf)
	if err != nil {
		b.printCurrentState()
		return err
	}
	err = b.ctxCancelled(ctx)
	if err != nil {
		return err
	}
	b.markOffset(session, msgBuf)

	var timeTaken string
	secondsTaken := time.Since(start).Seconds()
	if secondsTaken > 60 {
		timeTaken = fmt.Sprintf("%.0fm", secondsTaken/60)
	} else {
		timeTaken = fmt.Sprintf("%.0fs", secondsTaken)
	}

	// set cumulative metrics
	b.metric.setBytesLoaded(bytesProcessed)
	b.metric.setMsgsLoaded(len(msgBuf))
	b.metric.setLoadSeconds(secondsTaken)

	klog.Infof(
		"%s, batchId:%d, size:%d, end:%d:, processed in %s",
		b.topic, b.batchId, len(msgBuf), b.batchEndOffset, timeTaken,
	)

	return nil
}
