package redshift

import (
	"context"
	"database/sql"
	"fmt"
	multierror "github.com/hashicorp/go-multierror"
	"github.com/practo/klog/v2"
	"strconv"

	// TODO:
	// Use our own version of the postgres library so we get keep-alive support.
	// See https://github.com/Clever/pq/pull/1
	// TODO: https://github.com/Clever/s3-to-redshift/issues/163
	// TCP keep alive support pending: https://github.com/lib/pq/issues/360
	_ "github.com/practo/pq"
	"strings"
)

const (
	RedshiftString         = "character varying"
	RedshiftStringMax      = "character varying(65535)"
	RedshiftMaskedDataType = "character varying(50)"
	RedshiftDate           = "date"
	RedshiftInteger        = "integer"
	RedshiftTimeStamp      = "timestamp without time zone"

	// required to support utf8 characters
	// https://docs.aws.amazon.com/redshift/latest/dg/r_Character_types.html#r_Character_types-varchar-or-character-varying
	RedshiftToMysqlCharacterRatio = 4.0

	schemaExist = `select schema_name
from information_schema.schemata where schema_name='%s';`
	schemaCreate = `create schema "%s";`
	tableExist   = `select table_name from information_schema.tables where
table_schema='%s' and table_name='%s';`
	tableCreate = `CREATE TABLE "%s"."%s" (%s) %s %s;`
	deDupe      = `delete from %s where %s in (
select t1.%s from %s t1 join %s t2 on t1.%s=t2.%s where t1.%s < t2.%s);`
	deleteCommon = `delete from %s where %s in (
select t1.%s from %s t1 join %s t2 on t1.%s=t2.%s);`
	deleteColumn    = `delete from %s where %s.%s='%s';`
	dropColumn      = `ALTER TABLE "%s"."%s" DROP COLUMN %s;`
	alterSortColumn = `ALTER TABLE "%s"."%s" ALTER SORTKEY(%s);`
	dropTable       = `DROP TABLE %s;`
	// returns one row per column with the attributes:
	// name, type, default_val, not_null, primary_key,
	// need to pass a schema and table name as the parameters
	tableSchema = `SELECT
  f.attname AS name,
  pg_catalog.format_type(f.atttypid,f.atttypmod) AS col_type,
  CASE
      WHEN f.atthasdef = 't' THEN d.adsrc
      ELSE ''
  END AS default_val,
  f.attnotnull AS not_null,
  p.contype IS NOT NULL AND p.contype = 'p' AS primary_key,
  f.attisdistkey AS dist_key,
  f.attsortkeyord AS sort_ord
FROM pg_attribute f
  JOIN pg_class c ON c.oid = f.attrelid
  LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = f.attnum
  LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
  LEFT JOIN pg_constraint p ON p.conrelid = c.oid AND f.attnum = ANY (p.conkey)
WHERE c.relkind = 'r'::char
    AND n.nspname = '%s'  -- Replace with schema name
    AND c.relname = '%s'  -- Replace with table name
     AND f.attnum > 0 ORDER BY f.attnum;`
)

type dbExecCloser interface {
	Close() error

	// stats
	Stats() sql.DBStats
	SetMaxIdleConns(n int)
	SetMaxOpenConns(n int)

	// without tx
	PrepareContext(c context.Context, query string) (*sql.Stmt, error)

	// tx
	BeginTx(
		c context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	QueryContext(
		c context.Context, q string, a ...interface{}) (*sql.Rows, error)
	QueryRowContext(c context.Context, q string, a ...interface{}) *sql.Row
}

// Redshift wraps a dbExecCloser and can be used to perform
// operations on a redshift database.
// Give it a context for the duration of the job
type Redshift struct {
	dbExecCloser
	ctx  context.Context
	conf RedshiftConfig
}

type RedshiftConfig struct {
	Host              string `yaml:"host"`
	Port              string `yaml:"port"`
	Database          string `yaml:"database"`
	User              string `yaml:"user"`
	Password          string `yaml:"password"`
	Timeout           int    `yaml:"timeout"`
	S3AccessKeyId     string `yaml:"s3AccessKeyId"`
	S3SecretAccessKey string `yaml:"s3SecretAccessKey"`
	Schema            string `yaml:"schema"`
	Stats             bool   `yaml:"stats"`
	MaxOpenConns      int    `yaml:"maxOpenConns"`
	MaxIdleConns      int    `yaml:"maxIdleConns"`
}

// Table is representation of Redshift table
type Table struct {
	Name    string    `json:"dest"`
	Columns []ColInfo `json:"columns"`
	Meta    Meta      `json:"meta"`
}

// Meta holds information that might be not in Redshift or annoying to access
// in this case, schema a table is part of
type Meta struct {
	Schema string `json:"schema"`
}

func NewTable(t Table) *Table {
	return &Table{
		Name:    t.Name,
		Columns: t.Columns,
		Meta:    t.Meta,
	}
}

// ColInfo is a struct that contains information
// about a column in a Redshift database.
// SortOrdinal and DistKey only make sense for Redshift
type ColInfo struct {
	Name         string `json:"name"`
	Type         string `json:"type"`
	DebeziumType string `json:"debeziumtype"`
	DefaultVal   string `json:"defaultval"`
	NotNull      bool   `json:"notnull"`
	PrimaryKey   bool   `json:"primarykey"`
	SortOrdinal  int    `json:"sortord"`
	DistKey      bool   `json:"distkey"`
}

func NewRedshift(ctx context.Context, conf RedshiftConfig) (*Redshift, error) {
	source := fmt.Sprintf(
		"host=%s port=%s dbname=%s keepalive=1 connect_timeout=%d",
		conf.Host, conf.Port, conf.Database, conf.Timeout,
	)
	// TODO: make ssl configurable
	source += fmt.Sprintf(
		" user=%s password=%s sslmode=disable", conf.User, conf.Password)
	sqldb, err := sql.Open("postgres", source)
	if err != nil {
		return nil, err
	}
	if err := sqldb.Ping(); err != nil {
		return nil, err
	}
	r := &Redshift{sqldb, ctx, conf}

	r.SetMaxIdleConns(conf.MaxIdleConns)
	r.SetMaxOpenConns(conf.MaxOpenConns)
	klog.V(1).Infof("dbstats: %+v\n", r.Stats())
	// TODO: not using this
	// klog.Info("Setting Redshift ConnMaxLifetime=-1 (keep alive)")
	// r.SetConnMaxLifetime(1200 * time.Second)

	return r, nil
}

// Begin wraps a new transaction in the databases context
func (r *Redshift) Begin() (*sql.Tx, error) {
	return r.dbExecCloser.BeginTx(r.ctx, nil)
}

func (r *Redshift) SchemaExist(schema string) (bool, error) {
	q := fmt.Sprintf(schemaExist, schema)
	var placeholder string
	err := r.QueryRowContext(r.ctx, q).Scan(&placeholder)
	if err != nil {
		if err == sql.ErrNoRows {
			klog.V(5).Infof(
				"schema: %s does not exist", schema,
			)
			return false, nil
		}
		return false, fmt.Errorf("error querying schema exist: %v\n", err)
	}

	return true, nil
}

func (r *Redshift) CreateSchema(schema string) error {
	tx, err := r.Begin()
	if err != nil {
		return err
	}

	createSQL := fmt.Sprintf(schemaCreate, schema)
	createStmt, err := tx.PrepareContext(r.ctx, createSQL)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer createStmt.Close()

	_, err = createStmt.ExecContext(r.ctx)
	if err != nil {
		tx.Rollback()
	}

	return err
}

func (r *Redshift) TableExist(schema string, table string) (bool, error) {
	q := fmt.Sprintf(tableExist, schema, table)
	var placeholder string
	err := r.QueryRowContext(r.ctx, q).Scan(&placeholder)
	if err != nil {
		if err == sql.ErrNoRows {
			klog.V(5).Infof(
				"schema: %s, table: %s does not exist", schema, table,
			)
			return false, nil
		}
		return false, fmt.Errorf("failed sql:%s, err:%v\n", q, err)
	}

	return true, nil
}

func checkColumnsExactlySame(iCols, tCols []string) bool {
	if len(iCols) != len(tCols) {
		return false
	}

	for i := range iCols {
		if iCols[i] != tCols[i] {
			return false
		}
	}

	return true
}

func getSortColumns(table Table) []string {
	var sortColumns []string
	for _, column := range table.Columns {
		if column.SortOrdinal == 1 {
			sortColumns = append(sortColumns, column.Name)
		}
	}

	return sortColumns
}

func getSortColumnsSQL(columns []ColInfo) string {
	k := []string{}
	for _, column := range columns {
		if column.SortOrdinal == 1 {
			k = append(k, column.Name)
		}
	}
	if len(k) == 0 {
		return ""
	}

	return fmt.Sprintf(
		"compound sortkey(%s)",
		strings.Join(k, ","),
	)
}

func getDistColumns(table Table) []string {
	var distColumns []string
	for _, column := range table.Columns {
		if column.DistKey {
			distColumns = append(distColumns, column.Name)
		}
	}

	return distColumns
}

func getDistColumnSQL(columns []ColInfo) (string, error) {
	k := []string{}
	for _, column := range columns {
		if column.DistKey {
			k = append(k, column.Name)
		}
	}
	if len(k) == 0 {
		return "", nil
	}
	if len(k) > 1 {
		return "", fmt.Errorf(
			"dist key cant be > 1, config is list for backward comaptibility!")
	}

	return fmt.Sprintf("distkey(%s)", k[0]), nil
}

func getColumnSQL(c ColInfo) string {
	// note that we are relying on redshift
	// to fail if we have created multiple sort keys
	// currently we don't support that
	defaultVal := ""
	if c.DefaultVal != "" {
		if strings.Contains(c.Type, RedshiftString) {
			defaultVal = fmt.Sprintf("DEFAULT '%s'", c.DefaultVal)
		}
	}
	notNull := ""
	if c.NotNull {
		notNull = "NOT NULL"
	}
	primaryKey := ""
	if c.PrimaryKey {
		primaryKey = "PRIMARY KEY"
	}

	return fmt.Sprintf(
		" \"%s\" %s %s %s %s",
		c.Name,
		c.Type,
		defaultVal,
		notNull,
		primaryKey,
	)
}

func (r *Redshift) CreateTable(tx *sql.Tx, table Table) error {
	var columnSQL []string
	for _, c := range table.Columns {
		columnSQL = append(columnSQL, getColumnSQL(c))
	}

	sortColumnsSQL := getSortColumnsSQL(table.Columns)
	distColumnSQL, err := getDistColumnSQL(table.Columns)
	if err != nil {
		return err
	}

	args := []interface{}{strings.Join(columnSQL, ",")}
	createSQL := fmt.Sprintf(
		tableCreate,
		table.Meta.Schema,
		table.Name,
		strings.Join(columnSQL, ","),
		distColumnSQL,
		sortColumnsSQL,
	)

	klog.V(5).Infof("Preparing: %s with args: %v\n", createSQL, args)
	createStmt, err := tx.PrepareContext(r.ctx, createSQL)
	if err != nil {
		return fmt.Errorf("error preparing statement: %v\n", err)
	}
	defer createStmt.Close()

	klog.V(5).Infof("Running: %s with args: %v\n", createSQL, args)
	_, err = createStmt.ExecContext(r.ctx)

	return err
}

// UpdateTable migrates the table schema using one of the below 2 strategy:
// 1. Strategy1: inplace-migration using ALTER COMMANDS
//               Support: AddCol, DropCol, Change Type of Varchar Col
//               Executed by this function
// 2. Strategy2: table-migration using UNLOAD and COPY and a temp table
// 				 Support: all the other migration scearios
//               Exectued by ReplaceTable(), triggered by this function
// It returns a boolean specifying table migration is required or not.
func (r *Redshift) UpdateTable(
	tx *sql.Tx, inputTable, targetTable Table) (bool, error) {
	klog.V(5).Infof("inputt Table: \n%+v\n", inputTable)
	klog.V(5).Infof("target Table: \n%+v\n", targetTable)

	transactcolumnOps, columnOps, varCharColumnOps, err := CheckSchemas(
		inputTable, targetTable)
	if err != nil {
		return false, err
	}

	if len(transactcolumnOps)+len(columnOps)+len(varCharColumnOps) == 0 {
		klog.Infof(
			"Schema migration is not needed for table: %v\n",
			inputTable.Name)
		return false, nil
	}
	klog.Infof("Schema migration is required for: %v\n", inputTable.Name)

	if len(varCharColumnOps) > 0 {
		klog.Infof("Running migration (ALTER VACHAR): %v\n", inputTable.Name)
	}
	for _, op := range varCharColumnOps {
		klog.V(4).Infof("Preparing (!tx): %s", op)
		alterStmt, err := r.PrepareContext(r.ctx, op)
		if err != nil {
			return false, err
		}

		klog.Infof("Running (!tx): %s", op)
		_, err = alterStmt.ExecContext(r.ctx)
		if err != nil {
			return false, fmt.Errorf("cmd failed, cmd:%s, err: %s\n", op, err)
		}
		alterStmt.Close()
	}

	if len(transactcolumnOps) > 0 {
		klog.Infof(
			"Strategy1: starting inplace-migration, table:%v\n",
			inputTable.Name)
	}
	// run transcation block commands
	// postgres only allows adding one column at a time
	for _, op := range transactcolumnOps {
		klog.V(4).Infof("Preparing: %s", op)
		alterStmt, err := tx.PrepareContext(r.ctx, op)
		if err != nil {
			return false, err
		}
		klog.Infof("Running: %s", op)
		_, err = alterStmt.ExecContext(r.ctx)
		if err != nil {
			return false, fmt.Errorf("cmd failed, cmd:%s, err: %s\n", op, err)
		}
		alterStmt.Close()
	}

	// run non transaction block commands
	// redshift does not support alter columns #40
	performTableMigration := false
	if len(columnOps) > 0 {
		klog.Infof(
			"Strategy2: table-migration needed, table:%v\n",
			inputTable.Name)
		klog.V(5).Infof("columnOps=%v\n", columnOps)
		performTableMigration = true
	}

	return performTableMigration, nil
}

// Replace Table replaces the current table with a new schema table
// this is required in Redshift as ALTER COLUMNs are not supported
// for all column types
// 1. Rename the table t1_migrating
// 2. Create table with new schema t1
// 3. UNLOAD the renamed table data t1_migrating to s3
// 4. COPY the unloaded data from s3 to the new table t1
func (r *Redshift) ReplaceTable(
	tx *sql.Tx, unLoadS3Key string, copyS3ManifestKey string,
	inputTable, targetTable Table) error {

	klog.Infof("Strategy2: table-migration starting(slow), table: %s ...\n",
		inputTable.Name)
	targetTableName := fmt.Sprintf(
		`"%s"."%s"`, targetTable.Meta.Schema, targetTable.Name)
	migrationTableName := fmt.Sprintf(
		`%s_migrating`, targetTable.Name)

	exist, err := r.TableExist(targetTable.Meta.Schema, migrationTableName)
	if err != nil {
		return err
	}
	if exist {
		err := r.DropTable(tx, targetTable.Meta.Schema, migrationTableName)
		if err != nil {
			return err
		}
	}

	renameSQL := fmt.Sprintf(
		`ALTER TABLE %s RENAME TO "%s"`,
		targetTableName,
		migrationTableName,
	)
	klog.V(5).Infof("Running: %s", renameSQL)
	_, err = tx.ExecContext(r.ctx, renameSQL)
	if err != nil {
		return err
	}

	err = r.CreateTable(tx, inputTable)
	if err != nil {
		return err
	}

	err = r.Unload(tx,
		targetTable.Meta.Schema,
		migrationTableName,
		unLoadS3Key,
	)
	if err != nil {
		return err
	}

	err = r.Copy(tx,
		targetTable.Meta.Schema,
		targetTable.Name,
		copyS3ManifestKey,
		false,
		true,
	)
	if err != nil {
		return err
	}

	// Try dropping table and ignore the error if any
	// as this operation is always performed on start
	r.DropTable(tx, targetTable.Meta.Schema, migrationTableName)

	return nil
}

func (r *Redshift) prepareAndExecute(tx *sql.Tx, command string) error {
	klog.V(4).Infof("Preparing: %s\n", command)
	statement, err := tx.PrepareContext(r.ctx, command)
	if err != nil {
		return err
	}
	defer statement.Close()

	klog.Infof("Running: %s\n", command)
	_, err = statement.ExecContext(r.ctx)
	if err != nil {
		return fmt.Errorf("cmd failed, cmd:%s, err: %s\n", command, err)
	}

	return nil
}

// DeDupe deletes the duplicates in the redshift table and keeps only the
// latest, it accepts a transaction
// ex: targetTablePrimaryKey = some id, timestamp
// ex: stagingTablePrimaryKey = kafkaoffset
func (r *Redshift) DeDupe(tx *sql.Tx, schema string, table string,
	targetTablePrimaryKey string, stagingTablePrimaryKey string) error {

	sTable := fmt.Sprintf(`"%s"."%s"`, schema, table)
	command := fmt.Sprintf(
		deDupe,
		sTable,
		stagingTablePrimaryKey,
		stagingTablePrimaryKey,
		sTable,
		sTable,
		targetTablePrimaryKey,
		targetTablePrimaryKey,
		stagingTablePrimaryKey,
		stagingTablePrimaryKey,
	)

	return r.prepareAndExecute(tx, command)
}

// DeleteCommon deletes the common based on commonColumn from targetTable.
func (r *Redshift) DeleteCommon(tx *sql.Tx, schema string, stagingTable string,
	targetTable string, commonColumn string) error {

	sTable := fmt.Sprintf(`"%s"."%s"`, schema, stagingTable)
	tTable := fmt.Sprintf(`"%s"."%s"`, schema, targetTable)

	command := fmt.Sprintf(
		deleteCommon,
		tTable,
		commonColumn,
		commonColumn,
		sTable,
		tTable,
		commonColumn,
		commonColumn,
	)

	return r.prepareAndExecute(tx, command)
}

func (r *Redshift) DropTable(tx *sql.Tx, schema string, table string) error {
	return r.prepareAndExecute(
		tx,
		fmt.Sprintf(
			dropTable,
			fmt.Sprintf(`"%s"."%s"`, schema, table),
		),
	)
}

func (r *Redshift) DeleteColumn(tx *sql.Tx, schema string, table string,
	columnName string, columnValue string) error {

	sTable := fmt.Sprintf(`"%s"."%s"`, schema, table)
	command := fmt.Sprintf(
		deleteColumn,
		sTable,
		sTable,
		columnName,
		columnValue,
	)

	return r.prepareAndExecute(tx, command)
}

func (r *Redshift) DropColumn(tx *sql.Tx, schema string, table string,
	columnName string) error {

	command := fmt.Sprintf(
		dropColumn,
		schema,
		table,
		columnName,
	)

	return r.prepareAndExecute(tx, command)
}

// Unload copies data present in the table to s3
// this loads data to s3 and generates a manifest file at s3key + manifest path
func (r *Redshift) Unload(tx *sql.Tx,
	schema string, table string, s3Key string) error {

	credentials := fmt.Sprintf(
		`CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s'`,
		r.conf.S3AccessKeyId,
		r.conf.S3SecretAccessKey,
	)
	unLoadSQL := fmt.Sprintf(
		`UNLOAD ('select * from "%s"."%s"') TO '%s' %s manifest allowoverwrite addquotes escape delimiter ','`,
		schema,
		table,
		s3Key,
		credentials,
	)
	klog.V(5).Infof("Running: %s", unLoadSQL)
	_, err := tx.ExecContext(r.ctx, unLoadSQL)

	return err
}

// Copy using manifest file
// into redshift using manifest file.
// this is meant to be run in a transaction, so the first arg must be a sql.Tx
func (r *Redshift) Copy(tx *sql.Tx,
	schema string, table string, s3ManifestURI string,
	typeJson bool, typeCsv bool) error {

	json := ""
	if typeJson == true {
		json = "json 'auto'"
	}

	csv := ""
	if typeCsv == true {
		csv = `removequotes escape delimiter ','`
	}

	credentials := fmt.Sprintf(
		`CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s'`,
		r.conf.S3AccessKeyId,
		r.conf.S3SecretAccessKey,
	)
	copySQL := fmt.Sprintf(
		`COPY "%s"."%s" FROM '%s' %s manifest %s %s`,
		schema,
		table,
		s3ManifestURI,
		credentials,
		json,
		csv,
	)
	klog.V(5).Infof("Running: %s", copySQL)
	_, err := tx.ExecContext(r.ctx, copySQL)

	return err
}

// GetTableMetadata looks for a table and returns the Table representation
// if the table does not exist it returns an empty table but does not error
func (r *Redshift) GetTableMetadata(schema, tableName string) (*Table, error) {
	exist, err := r.TableExist(schema, tableName)
	if err != nil {
		return nil, err
	}
	if !exist {
		return nil, fmt.Errorf(
			"Table %s.%s does not exist\n", schema, tableName)
	}

	var cols []ColInfo
	rows, err := r.QueryContext(
		r.ctx, fmt.Sprintf(tableSchema, schema, tableName))
	if err != nil {
		return nil, fmt.Errorf(
			"error Running column query: %s, err: %s", tableSchema, err)
	}
	defer rows.Close()
	for rows.Next() {
		var c ColInfo
		if err := rows.Scan(&c.Name, &c.Type, &c.DefaultVal, &c.NotNull,
			&c.PrimaryKey, &c.DistKey, &c.SortOrdinal,
		); err != nil {
			return nil, fmt.Errorf("error scanning column, err: %s", err)
		}

		cols = append(cols, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating columns, err: %s", err)
	}

	retTable := Table{
		Name:    tableName,
		Columns: cols,
		Meta: Meta{
			Schema: schema,
		},
	}

	return &retTable, nil
}

// CheckSchemas takes in two tables and compares their column schemas
// to make sure they're compatible. If they have any mismatched columns
// they are returned in the errors array. Covers most of the schema migration
// scenarios, and returns the ALTER commands to do it.
func CheckSchemas(inputTable, targetTable Table) (
	[]string, []string, []string, error) {

	return checkColumnsAndOrdering(inputTable, targetTable)
}

func checkColumn(schemaName string, tableName string,
	inCol ColInfo, targetCol ColInfo) ([]string, []string, error) {
	// klog.V(5).Infof("inCol: %+v\n,taCol: %+v\n", inCol, targetCol)

	var errors error
	mismatchedTemplate := "mismatch col: %s, prop: %s, input: %v, target: %v"
	alterSQL := []string{}
	alterVarCharSQL := []string{}

	if inCol.Name != targetCol.Name {
		// TODO: add support for renaming columns
		// the only migration that is not supported at present
		errors = multierror.Append(
			errors, fmt.Errorf(mismatchedTemplate,
				inCol.Name, "Name", inCol.Name, targetCol.Name))
	}

	if inCol.PrimaryKey != targetCol.PrimaryKey {
		if inCol.PrimaryKey {
			alterSQL = append(alterSQL,
				fmt.Sprintf(
					`ALTER TABLE "%s"."%s" ADD PRIMARY KEY (%s)`,
					schemaName, tableName,
					inCol.Name,
				),
			)
		} else {
			alterSQL = append(alterSQL,
				fmt.Sprintf(
					`ALTER TABLE "%s"."%s" DROP CONSTRAINT %s_pkey`,
					schemaName,
					tableName,
					tableName,
				),
			)
		}
	}

	if inCol.Type != targetCol.Type {
		typeChangeSQL := fmt.Sprintf(
			`ALTER TABLE "%s"."%s" ALTER COLUMN %s %s %s`,
			schemaName,
			tableName,
			inCol.Name,
			"TYPE",
			inCol.Type,
		)

		if strings.Contains(targetCol.Type, RedshiftString) &&
			strings.Contains(inCol.Type, RedshiftString) {
			alterVarCharSQL = append(alterVarCharSQL, typeChangeSQL)
		} else {
			alterSQL = append(alterSQL, typeChangeSQL)
		}
	}

	// TODO: #40 #41 handle changes in null
	// if (inCol.NotNull != targetCol.NotNull) && !inCol.PrimaryKey {
	//
	// }

	// TODO: #40 #41 handle changes in default values
	// if ConvertDefaultValue(inCol.DefaultVal) != targetCol.DefaultVal {
	// }

	return alterVarCharSQL, alterSQL, errors
}

// checkColumnsAndOrdering constructs migration commands comparing the tables
// it returns the operations that can be performed using transaction
// and the operations which requires table migration, both handled
// differently. Also returns the command that does not needed a table migration
// but cannot not run as a transaction (varCharColumnOps)
func checkColumnsAndOrdering(
	inputTable, targetTable Table) ([]string, []string, []string, error) {

	var transactColumnOps []string
	var columnOps []string
	var varCharColumnOps []string
	var errors error

	inColMap := make(map[string]bool)

	for idx, inCol := range inputTable.Columns {
		inColMap[inCol.Name] = true

		// add column (runs in a single transcation, single ALTER COMMAND)
		if len(targetTable.Columns) <= idx {
			klog.V(5).Info("Missing column, alter table will run.\n")
			alterSQL := fmt.Sprintf(
				`ALTER TABLE "%s"."%s" ADD COLUMN %s`,
				targetTable.Meta.Schema,
				targetTable.Name,
				getColumnSQL(inCol),
			)
			transactColumnOps = append(transactColumnOps, alterSQL)
			continue
		}

		// alter column (requires table migration, can't run in transaction)
		// only varchar column operations for type change does not require
		// table migration
		targetCol := targetTable.Columns[idx]
		var alterColumnOps []string
		var err error
		varCharColumnOps, alterColumnOps, err = checkColumn(
			inputTable.Meta.Schema, inputTable.Name, inCol, targetCol)
		if err != nil {
			errors = multierror.Append(errors, err)
		}
		columnOps = append(columnOps, alterColumnOps...)
	}

	// drop column (runs in a single transcation, single ALTER COMMAND)
	for _, taCol := range targetTable.Columns {
		if _, ok := inColMap[taCol.Name]; !ok {
			klog.V(5).Infof(
				"Extra column: %s, alter table will run\n", taCol.Name,
			)
			alterSQL := fmt.Sprintf(
				dropColumn,
				targetTable.Meta.Schema,
				targetTable.Name,
				taCol.Name,
			)
			transactColumnOps = append(transactColumnOps, alterSQL)
			continue
		}
	}

	// alter sort keys (runs in a single transcation, single ALTER COMMAND)
	inputTableSortColumns := getSortColumns(inputTable)
	targetTableSortColumns := getSortColumns(targetTable)
	if !checkColumnsExactlySame(inputTableSortColumns, targetTableSortColumns) {
		klog.V(5).Infof(
			"SortKeys needs migration: before: %v, now: %v\n",
			inputTableSortColumns,
			targetTableSortColumns,
		)
		alterSQL := fmt.Sprintf(
			alterSortColumn,
			inputTable.Meta.Schema,
			inputTable.Name,
			strings.Join(inputTableSortColumns, ","),
		)
		transactColumnOps = append(transactColumnOps, alterSQL)
	}

	// alter dist (requires table migration, can't run in transaction)
	inputTableDistColumns := getDistColumns(inputTable)
	targetTableDistColumns := getDistColumns(targetTable)
	if !checkColumnsExactlySame(inputTableDistColumns, targetTableDistColumns) {
		columnOps = append(columnOps, "ALTER DISTKEY using table migration")
	}

	return transactColumnOps, columnOps, varCharColumnOps, errors
}

func ConvertDefaultValue(val string) string {
	if val != "" {
		return "'" + val + "'" + "::" + RedshiftString
	}

	return val
}

// https://debezium.io/documentation/reference/1.2/connectors/mysql.html
// https://docs.aws.amazon.com/redshift/latest/dg/c_Supported_data_types.html
var debeziumToRedshiftTypeMap = map[string]string{
	"boolean": "boolean",
	"float":   "real",
	"float32": "real",
	"float64": "double precision",
	"int":     "integer",
	"int16":   "smallint",
	"int32":   RedshiftInteger,
	"long":    "bigint",
	"bigint":  "bigint",
	"string":  RedshiftString,
}

var mysqlToRedshiftTypeMap = map[string]string{
	"bigint":                      "bigint",
	"integer unsigned":            "bigint",
	"bit":                         "bigint",
	"bool":                        "boolean",
	"boolean":                     "boolean",
	"date":                        RedshiftDate,
	"year":                        RedshiftDate,
	"binary":                      RedshiftString,
	"char":                        RedshiftString,
	"set":                         RedshiftString,
	"enum":                        RedshiftString,
	"longblob":                    RedshiftString,
	"mediumblob":                  RedshiftString,
	"tinyblob":                    RedshiftString,
	"varchar":                     RedshiftString,
	"blob":                        RedshiftStringMax,
	"longtext":                    RedshiftStringMax,
	"mediumtext":                  RedshiftStringMax,
	"text":                        RedshiftStringMax,
	"tinytext":                    RedshiftStringMax,
	"varbinary":                   RedshiftStringMax,
	"int":                         RedshiftInteger,
	"integer":                     RedshiftInteger,
	"mediumint":                   RedshiftInteger,
	"mediumint unsigned":          RedshiftInteger,
	"smallint unsigned":           RedshiftInteger,
	"double":                      "double precision",
	"double [precision]":          "double precision",
	"double [precision] unsigned": "double precision",
	"datetime":                    RedshiftTimeStamp,
	"time":                        RedshiftTimeStamp,
	"timestamp":                   RedshiftTimeStamp,
	"smallint":                    "smallint",
	"tinyint":                     "smallint",
	"tinyint unsigned":            "smallint",
	"dec":                         "numeric(18,0)",
	"decimal":                     "numeric(18,0)",
	"decimal unsigned":            "numeric(18,0)",
	"fixed":                       "numeric(18,0)",
	"numeric":                     "numeric(18,0)",
	"bigint unsigned":             "numeric(20, 0)",
	"float":                       "real",
}

// applyLength applies the length passed otherwise adds default
// TODO: only takes care of string length, numeric and other types pending
func applyLength(ratio float32, redshiftType, sourceColLength string) string {
	switch redshiftType {
	case RedshiftString:
		if sourceColLength == "" {
			sourceColLength = "256" //default
		} else {
			// if character length is defined take care of multi byte characters
			length, err := strconv.Atoi(sourceColLength)
			if err != nil {
				klog.Fatalf("Error converting to int, val: %v %v\n",
					sourceColLength, err)
			}
			multiByteSupportedLength := int(float32(length) * ratio)
			if multiByteSupportedLength > 65535 {
				return RedshiftStringMax
			}
			sourceColLength = strconv.Itoa(multiByteSupportedLength)
		}
		return fmt.Sprintf("%s(%s)", redshiftType, sourceColLength)
	default:
		return redshiftType
	}
}

// GetRedshiftDataType returns the mapped type for the sqlType's data type
func GetRedshiftDataType(sqlType, debeziumType, sourceColType,
	sourceColLength string, columnMasked bool) (string, error) {

	if columnMasked == true {
		return RedshiftMaskedDataType, nil
	}

	debeziumType = strings.ToLower(debeziumType)
	sourceColType = strings.ToLower(sourceColType)

	switch sqlType {
	case "mysql":
		redshiftType, ok := mysqlToRedshiftTypeMap[sourceColType]
		if ok {
			return applyLength(
				RedshiftToMysqlCharacterRatio,
				redshiftType,
				sourceColLength), nil
		}
		// default is the debeziumType
		redshiftType, ok = debeziumToRedshiftTypeMap[debeziumType]
		if ok {
			return applyLength(
				RedshiftToMysqlCharacterRatio,
				redshiftType,
				sourceColLength), nil
		}
		return "", fmt.Errorf(
			"Type: %s, SourceType: %s, not handled\n",
			debeziumType,
			sourceColType,
		)
	}

	return "", fmt.Errorf("Unsupported sqlType:%s\n", sqlType)
}
