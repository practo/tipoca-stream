package redshift

import (
	"context"
	"database/sql"
	"fmt"
	multierror "github.com/hashicorp/go-multierror"
	"github.com/practo/klog/v2"
	"math"
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
	RedshiftString              = "character varying"
	RedshiftStringMax           = "character varying(65535)"
	RedshiftStringMaxLength     = 65535
	RedshiftStringDefaultLength = 256

	RedshiftMaskedDataType       = "character varying(50)"
	RedshiftMobileColType        = "character varying(10)"
	RedshiftMaskedDataTypeLength = 50

	RedshiftNumeric             = "numeric"
	RedshiftNumericMaxLength    = 38
	RedshiftNumericDefautLength = 18
	RedshiftNumericMaxScale     = 37
	RedshiftNumericDefaultScale = 0

	RedshiftDate      = "date"
	RedshiftInteger   = "integer"
	RedshiftTime      = "character varying(32)"
	RedshiftTimeStamp = "timestamp without time zone"

	// required to support utf8 characters
	// https://docs.aws.amazon.com/redshift/latest/dg/r_Character_types.html#r_Character_types-varchar-or-character-varying
	RedshiftToMysqlCharacterRatio = 4.0

	schemaExist = `select schema_name
from information_schema.schemata where schema_name='%s';`
	schemaCreate = `create schema "%s";`
	tableExist   = `select table_name from information_schema.tables where
table_schema='%s' and table_name='%s';`
	dropColumn      = `ALTER TABLE "%s"."%s" DROP COLUMN %s;`
	alterSortColumn = `ALTER TABLE "%s"."%s" ALTER SORTKEY(%s);`
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
	Schema            string `yaml:"schema"`
	TableSuffix       string `yaml:"tableSuffix"`
	Host              string `yaml:"host"`
	Port              string `yaml:"port"`
	Database          string `yaml:"database"`
	User              string `yaml:"user"`
	Password          string `yaml:"password"`
	Timeout           int    `yaml:"timeout"`
	S3AccessKeyId     string `yaml:"s3AccessKeyId"`
	S3SecretAccessKey string `yaml:"s3SecretAccessKey"`
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
	Name         string     `json:"name"`
	Type         string     `json:"type"`
	DebeziumType string     `json:"debeziumtype"`
	SourceType   SourceType `yaml:"sourceType"`
	DefaultVal   string     `json:"defaultval"`
	NotNull      bool       `json:"notnull"`
	PrimaryKey   bool       `json:"primarykey"`
	SortOrdinal  int        `json:"sortord"`
	DistKey      bool       `json:"distkey"`
}

type SourceType struct {
	ColumnLength string `yaml:"columnLength"`
	ColumnType   string `yaml:"columnType"`
	ColumnScale  string `yaml:"columnScale"`
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
	klog.V(2).Infof("dbstats: %+v\n", r.Stats())
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

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("Error committing tx, err:%v\n", err)
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
	var primaryKeys []string
	for _, c := range table.Columns {
		if c.PrimaryKey {
			primaryKeys = append(primaryKeys, c.Name)
		}
	}

	var columnSQL []string
	for _, c := range table.Columns {
		sql := getColumnSQL(c)
		if len(primaryKeys) > 0 {
			sql = strings.ReplaceAll(sql, "PRIMARY KEY", "")
		}
		columnSQL = append(columnSQL, sql)
	}

	primaryKeySQL := ""
	if len(primaryKeys) > 0 {
		primaryKeySQL = fmt.Sprintf(
			`, primary key(%s)`, strings.Join(primaryKeys, ", "))
	}

	sortColumnsSQL := getSortColumnsSQL(table.Columns)
	distColumnSQL, err := getDistColumnSQL(table.Columns)
	if err != nil {
		return err
	}

	tableCreate := `CREATE TABLE "%s"."%s" (%s %s) %s %s;`

	createSQL := fmt.Sprintf(
		tableCreate,
		table.Meta.Schema,
		table.Name,
		strings.Join(columnSQL, ","),
		primaryKeySQL,
		distColumnSQL,
		sortColumnsSQL,
	)

	klog.V(5).Infof("Preparing: %s\n", createSQL)
	createStmt, err := tx.PrepareContext(r.ctx, createSQL)
	if err != nil {
		return fmt.Errorf("error preparing statement: %v\n", err)
	}
	defer createStmt.Close()

	klog.V(4).Infof("Running: %s\n", createSQL)
	_, err = createStmt.ExecContext(r.ctx)

	return err
}

// UpdateTable migrates the table schema using below 3 strategy:
// 1. Strategy1: inplace-migration-varchar-type Change length of VARCHAR col
//               Executed by this function
// 2. Strategy2: inplace-migration using ALTER COMMANDS
//               Supports: AddCol and DropCol
// 3. Strategy3: table-migration using UNLOAD and COPY and a temp table
// 				 Supports: all the other migration scenarios
//               Exectued by ReplaceTable(), triggered by this function
func (r *Redshift) UpdateTable(inputTable, targetTable Table) (bool, error) {
	klog.V(4).Infof("inputt Table: \n%+v\n", inputTable)
	klog.V(4).Infof("target Table: \n%+v\n", targetTable)
	transactcolumnOps, columnOps, varCharColumnOps, err := CheckSchemas(
		inputTable, targetTable)
	if err != nil {
		return false, err
	}

	if len(transactcolumnOps)+len(columnOps)+len(varCharColumnOps) == 0 {
		klog.V(4).Infof(
			"Schema migration is not needed for table: %v\n",
			inputTable.Name)
		return false, nil
	}
	klog.V(2).Infof("Schema migration is required for: %v\n", inputTable.Name)
	klog.V(2).Infof("tOps: %v, cOps: %v, vOps: %v\n",
		transactcolumnOps, columnOps, varCharColumnOps)

	// Strategy1: execute
	if len(varCharColumnOps) > 0 {
		klog.V(2).Infof("Strategy1: running migration (VARCHAR type): %v\n",
			inputTable.Name)
	}
	for _, op := range varCharColumnOps {
		klog.V(4).Infof("Preparing (!tx): %s", op)
		alterStmt, err := r.PrepareContext(r.ctx, op)
		if err != nil {
			return false, err
		}

		klog.V(2).Infof("Running (!tx): %s", op)
		_, err = alterStmt.ExecContext(r.ctx)
		if err != nil {
			return false, fmt.Errorf("cmd failed, cmd:%s, err: %s\n", op, err)
		}
		alterStmt.Close()
	}

	// Strategy2: execute
	if len(transactcolumnOps) > 0 {
		klog.V(2).Infof(
			"Strategy2: starting inplace-migration, table:%v\n",
			inputTable.Name)

		tx, err := r.Begin()
		if err != nil {
			return false, fmt.Errorf("Error creating tx, err: %v\n", err)
		}
		// run transcation block commands
		// postgres only allows adding one column at a time
		for _, op := range transactcolumnOps {
			klog.V(4).Infof("Preparing: %s", op)
			alterStmt, err := tx.PrepareContext(r.ctx, op)
			if err != nil {
				tx.Rollback()
				return false, err
			}
			klog.V(2).Infof("Running: %s", op)
			_, err = alterStmt.ExecContext(r.ctx)
			if err != nil {
				tx.Rollback()
				return false, fmt.Errorf(
					"cmd failed, cmd:%s, err: %s\n", op, err)
			}
			alterStmt.Close()
		}
		err = tx.Commit()
		if err != nil {
			tx.Rollback()
			return false, fmt.Errorf("Error committing tx, err:%v\n", err)
		}
	}

	// Strategy3: trigger the third one and then return
	// run non transaction block commands
	// redshift does not support alter columns #40
	if len(columnOps) > 0 {
		klog.V(2).Infof(
			"Strategy3: table-migration triggering, table:%v\n",
			inputTable.Name)
		klog.V(5).Infof("columnOps=%v\n", columnOps)
		// trigger table migration
		return true, nil
	}

	return false, nil
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

	klog.Infof("Strategy3: table-migration starting(slow), table: %s ...\n",
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
	klog.V(4).Infof("Running: %s", renameSQL)
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
		false,
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
		true,
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

func (r *Redshift) RenameTable(
	tx *sql.Tx,
	schema string,
	sourceTableName string,
	destTableName string,
) error {
	renameSQL := fmt.Sprintf(
		`ALTER TABLE %s.%s RENAME TO "%s"`,
		schema,
		sourceTableName,
		destTableName,
	)

	klog.V(4).Infof("Running: %s", renameSQL)
	_, err := tx.ExecContext(r.ctx, renameSQL)
	if err != nil {
		return fmt.Errorf("Error renaming table, err: %v", err)
	}

	return nil
}

func (r *Redshift) GrantSchemaAccess(
	tx *sql.Tx,
	schema string,
	table string,
	group string,
) error {
	grantSelectSQL := fmt.Sprintf(
		`GRANT SELECT ON TABLE %s.%s TO GROUP %s`,
		schema,
		table,
		group,
	)
	grantUsageSQL := fmt.Sprintf(
		`GRANT USAGE ON SCHEMA %s TO GROUP %s`,
		schema,
		group,
	)

	for _, sql := range []string{grantSelectSQL, grantUsageSQL} {
		klog.V(4).Infof("Running: %s", sql)
		_, err := tx.ExecContext(r.ctx, sql)
		if err != nil {
			return fmt.Errorf("Error granting schema access, err: %v", err)
		}
	}

	return nil
}

func (r *Redshift) prepareAndExecute(tx *sql.Tx, command string) error {
	klog.V(5).Infof("Preparing: %s\n", command)
	statement, err := tx.PrepareContext(r.ctx, command)
	if err != nil {
		return err
	}
	defer statement.Close()

	klog.V(4).Infof("Running: %s\n", command)
	_, err = statement.ExecContext(r.ctx)
	if err != nil {
		return fmt.Errorf("cmd failed, cmd:%s, err: %s\n", command, err)
	}

	return nil
}

// DeDupe deletes the duplicates in the redshift table and keeps only the
// latest, it accepts a transaction
func (r *Redshift) DeDupe(tx *sql.Tx, schema string, table string,
	targetTablePrimaryKeys []string, stagingTablePrimaryKey string) error {

	var joinOn string
	for i, targetPk := range targetTablePrimaryKeys {
		if i == 0 {
			joinOn = fmt.Sprintf(`t1.%s=t2.%s`, targetPk, targetPk)
		} else {
			joinOn = fmt.Sprintf(
				`%s AND t1.%s=t2.%s`, joinOn, targetPk, targetPk)
		}
	}

	deDupe := `delete from %s where %s in (
	select t1.%s from %s t1 join %s t2 on %s where t1.%s < t2.%s);`

	sTable := fmt.Sprintf(`"%s"."%s"`, schema, table)
	command := fmt.Sprintf(
		deDupe,
		sTable,
		stagingTablePrimaryKey,
		stagingTablePrimaryKey,
		sTable,
		sTable,
		joinOn,
		stagingTablePrimaryKey,
		stagingTablePrimaryKey,
	)

	return r.prepareAndExecute(tx, command)
}

// DeleteCommon deletes the common based on commonColumn from targetTable.
func (r *Redshift) DeleteCommon(tx *sql.Tx, schema string, stagingTable string,
	targetTable string, commonColumns []string) error {

	var whereColumn string
	for i, commonColumn := range commonColumns {
		if i == 0 {
			whereColumn = commonColumn
		} else {
			whereColumn = fmt.Sprintf(`%s, %s`, whereColumn, commonColumn)
		}
	}
	if len(commonColumns) > 1 {
		whereColumn = fmt.Sprintf(`(%s)`, whereColumn)
	}

	var joinOn string
	for i, commonColumn := range commonColumns {
		if i == 0 {
			joinOn = fmt.Sprintf(`t1.%s=t2.%s`, commonColumn, commonColumn)
		} else {
			joinOn = fmt.Sprintf(
				`%s AND t1.%s=t2.%s`, joinOn, commonColumn, commonColumn)
		}
	}

	var selectColumn string
	for i, commonColumn := range commonColumns {
		if i == 0 {
			selectColumn = fmt.Sprintf(`t1.%s`, commonColumn)
		} else {
			selectColumn = fmt.Sprintf(
				`%s, t1.%s`, selectColumn, commonColumn)
		}
	}

	sTable := fmt.Sprintf(`"%s"."%s"`, schema, stagingTable)
	tTable := fmt.Sprintf(`"%s"."%s"`, schema, targetTable)

	deleteCommon := `delete from %s where %s in (
select %s from %s t1 join %s t2 on %s);`

	command := fmt.Sprintf(
		deleteCommon,
		tTable,
		whereColumn,
		selectColumn,
		sTable,
		tTable,
		joinOn,
	)

	return r.prepareAndExecute(tx, command)
}

func (r *Redshift) DropTable(tx *sql.Tx, schema string, table string) error {
	dropTable := `DROP TABLE %s;`
	return r.prepareAndExecute(
		tx,
		fmt.Sprintf(
			dropTable,
			fmt.Sprintf(`"%s"."%s"`, schema, table),
		),
	)
}

func (r *Redshift) DropTableWithCascade(tx *sql.Tx, schema string, table string) error {
	dropTable := `DROP TABLE %s cascade;`
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

	deleteColumn := `delete from %s where %s.%s='%s';`

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
	schema string, table string, s3Key string, removeDuplicate bool) error {

	distinct := ""
	if removeDuplicate {
		distinct = "DISTINCT"
	}

	credentials := fmt.Sprintf(
		`CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s'`,
		r.conf.S3AccessKeyId,
		r.conf.S3SecretAccessKey,
	)
	unLoadSQL := fmt.Sprintf(
		`UNLOAD ('select %s * from "%s"."%s"') TO '%s' %s manifest allowoverwrite addquotes escape delimiter ','`,
		distinct,
		schema,
		table,
		s3Key,
		credentials,
	)
	klog.V(2).Infof("Running: UNLOAD from %s to s3\n", table)
	klog.V(5).Infof("Running: %s", unLoadSQL)
	_, err := tx.ExecContext(r.ctx, unLoadSQL)

	return err
}

// Copy using manifest file
// into redshift using manifest file.
// this is meant to be run in a transaction, so the first arg must be a sql.Tx
func (r *Redshift) Copy(tx *sql.Tx,
	schema string, table string, s3ManifestURI string,
	typeJson bool, typeCsv bool, comupdateOff bool, statupdateOff bool) error {

	json := ""
	if typeJson == true {
		json = "json 'auto'"
	}

	csv := ""
	if typeCsv == true {
		csv = `removequotes escape delimiter ',' EMPTYASNULL`
	}

	credentials := fmt.Sprintf(
		`CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s'`,
		r.conf.S3AccessKeyId,
		r.conf.S3SecretAccessKey,
	)

	comupdate := ""
	if comupdateOff {
		comupdate = "COMPUPDATE OFF"
	}

	statupdate := ""
	if statupdateOff {
		statupdate = "STATUPDATE OFF"
	}

	copySQL := fmt.Sprintf(
		`COPY "%s"."%s" FROM '%s' %s manifest %s %s %s %s`,
		schema,
		table,
		s3ManifestURI,
		credentials,
		json,
		csv,
		comupdate,
		statupdate,
	)
	klog.V(2).Infof("Running: COPY from s3 to: %s\n", table)
	klog.V(5).Infof("Running: %s\n", copySQL)
	_, err := tx.ExecContext(r.ctx, copySQL)
	if err != nil {
		return fmt.Errorf(
			"Error running copySQL: %v, err: %v\n",
			copySQL,
			err)
	}

	return nil
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
	alterSQL := []string{}
	alterVarCharSQL := []string{}

	if inCol.Name != targetCol.Name {
		// TODO: add support for renaming columns
		// the only migration that is not supported at present
		errors = multierror.Append(
			errors, fmt.Errorf(
				"table: %s mismatch col: %s, prop: %s, input: %v, target: %v",
				tableName,
				inCol.Name,
				"Name", inCol.Name,
				targetCol.Name,
			),
		)
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

	for _, inCol := range inputTable.Columns {
		inColMap[inCol.Name] = true
	}

	// drop column (runs in a single transcation, single ALTER COMMAND)
	// newTargetColumns is used to remove the columns which needs to be deleted
	// and then find the cols to add
	// and then the cols which has differences
	var newTargetColumns []ColInfo
	for _, taCol := range targetTable.Columns {
		_, ok := inColMap[taCol.Name]
		if !ok {
			klog.V(5).Infof(
				"Extra column: %s, delete column will run\n", taCol.Name,
			)
			alterSQL := fmt.Sprintf(
				dropColumn,
				targetTable.Meta.Schema,
				targetTable.Name,
				taCol.Name,
			)
			transactColumnOps = append(transactColumnOps, alterSQL)
		} else {
			newTargetColumns = append(newTargetColumns, taCol)
		}
	}
	targetTable.Columns = newTargetColumns

	for idx, inCol := range inputTable.Columns {
		// add column (runs in a single transcation, single ALTER COMMAND)
		if len(targetTable.Columns) <= idx {
			klog.V(5).Infof(
				"Missing column: %s, add column will run.\n", inCol.Name)
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
		alterVarCharSQL, alterColumnOps, err := checkColumn(
			inputTable.Meta.Schema, inputTable.Name, inCol, targetCol)
		if err != nil {
			errors = multierror.Append(errors, err)
		}
		columnOps = append(columnOps, alterColumnOps...)
		varCharColumnOps = append(varCharColumnOps, alterVarCharSQL...)
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
		if len(inputTableSortColumns) == 0 {
			klog.Warningf(
				"SortKey in table %s looks manually modified, skipped.",
				inputTable.Name,
			)
		} else {
			alterSQL := fmt.Sprintf(
				alterSortColumn,
				inputTable.Meta.Schema,
				inputTable.Name,
				strings.Join(inputTableSortColumns, ","),
			)
			transactColumnOps = append(transactColumnOps, alterSQL)
		}
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
	"time":                        RedshiftTime,
	"datetime":                    RedshiftTimeStamp,
	"timestamp":                   RedshiftTimeStamp,
	"smallint":                    "smallint",
	"tinyint":                     "smallint",
	"tinyint unsigned":            "smallint",
	"dec":                         RedshiftNumeric,
	"decimal":                     RedshiftNumeric,
	"decimal unsigned":            RedshiftNumeric,
	"fixed":                       RedshiftNumeric,
	"numeric":                     RedshiftNumeric,
	"bigint unsigned":             RedshiftNumeric,
	"float":                       "real",
}

func applyRange(masked bool, min, max, current int) int {
	if current > max {
		current = max
	} else if current < min && masked {
		current = min
	}

	return current
}

func computeScale(sourceColScale string) int {
	if sourceColScale == "" {
		return RedshiftNumericDefaultScale
	}

	scale, err := strconv.Atoi(sourceColScale)
	if err != nil {
		klog.Fatalf("Error converting to int, val: %v %v\n",
			sourceColScale, err)
	}

	if scale < 0 {
		return 0
	}

	if scale > RedshiftNumericMaxScale {
		return RedshiftNumericMaxScale
	}

	return scale
}

func computeLength(sourceColLength string, defaultLength int,
	columnMasked bool, ratio float64) int {

	var lengthCol int
	if sourceColLength == "" {
		if columnMasked {
			// default for masked column
			lengthCol = RedshiftMaskedDataTypeLength
		} else {
			// default for unmasked column
			lengthCol = defaultLength
		}
	} else {
		sourceLength, err := strconv.Atoi(sourceColLength)
		if err != nil {
			klog.Fatalf("Error converting to int, val: %v %v\n",
				sourceColLength, err)
		}
		// multi byte support for strings, numeric always get ratio as 1
		lengthCol = int(math.Ceil(float64(sourceLength) * ratio))
	}

	return lengthCol
}

// applyLength applies the length passed otherwise adds default
func applyLength(ratio float64, redshiftType,
	sourceColLength string, sourceColScale string,
	columnMasked bool) string {

	switch redshiftType {
	case RedshiftString:
		lengthCol := computeLength(
			sourceColLength,
			RedshiftStringDefaultLength,
			columnMasked,
			ratio)
		lengthCol = applyRange(
			columnMasked,
			RedshiftMaskedDataTypeLength,
			RedshiftStringMaxLength,
			lengthCol,
		)
		return fmt.Sprintf("%s(%d)", redshiftType, lengthCol)
	case RedshiftNumeric:
		lengthCol := computeLength(
			sourceColLength,
			RedshiftNumericDefautLength,
			columnMasked,
			1.0)

		if columnMasked {
			if lengthCol < RedshiftMaskedDataTypeLength {
				lengthCol = RedshiftMaskedDataTypeLength
			} else if lengthCol > RedshiftStringMaxLength {
				lengthCol = RedshiftStringMaxLength
			}
			return fmt.Sprintf("%s(%d)", RedshiftString, lengthCol)
		}
		if lengthCol > RedshiftNumericMaxLength {
			lengthCol = RedshiftNumericMaxLength
		}
		return fmt.Sprintf(
			"%s(%d,%d)", redshiftType, lengthCol, computeScale(sourceColScale))
	default:
		if columnMasked {
			return RedshiftMaskedDataType
		}
		return redshiftType
	}
}

// GetRedshiftDataType returns the mapped type for the sqlType's data type
func GetRedshiftDataType(sqlType, debeziumType, sourceColType,
	sourceColLength string, sourceColScale string,
	columnMasked bool) (string, error) {

	debeziumType = strings.ToLower(debeziumType)
	sourceColType = strings.ToLower(sourceColType)

	switch sqlType {
	case "mysql":
		redshiftType, ok := mysqlToRedshiftTypeMap[sourceColType]
		if !ok {
			// default is the debeziumType (fallback)
			redshiftType, ok = debeziumToRedshiftTypeMap[debeziumType]
			if !ok {
				// don't fail for masked types
				if columnMasked == true {
					return RedshiftMaskedDataType, nil
				}
				return "", fmt.Errorf(
					"DebeziumType: %s, SourceType: %s, not handled\n",
					debeziumType,
					sourceColType,
				)
			}
		}

		return applyLength(
			RedshiftToMysqlCharacterRatio,
			redshiftType,
			sourceColLength,
			sourceColScale,
			columnMasked,
		), nil
	}

	return "", fmt.Errorf("Unsupported sqlType:%s\n", sqlType)
}
