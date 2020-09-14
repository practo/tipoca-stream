package redshift

import (
	"context"
	"database/sql"
	"fmt"
	multierror "github.com/hashicorp/go-multierror"
	"github.com/practo/klog/v2"

	// TODO:
	// Use our own version of the postgres library so we get keep-alive support.
	// See https://github.com/Clever/pq/pull/1
	// TODO: https://github.com/Clever/s3-to-redshift/issues/163
	_ "github.com/lib/pq"
	"strings"
)

const (
	schemaExist = `select schema_name
from information_schema.schemata where schema_name='%s';`
	schemaCreate = `create schema "%s";`
	tableExist   = `select table_name from information_schema.tables where
table_schema='%s' and table_name='%s';`
	tableCreate = `CREATE TABLE "%s"."%s" (%s);`
	deDupe      = `delete from %s where %s in (
select t1.%s from %s t1 join %s t2 on t1.%s=t2.%s where t1.%s < t2.%s);`
	deleteCommon = `delete from %s where %s in (
select t1.%s from %s t1 join %s t2 on t1.%s=t2.%s);`
	deleteColumn = `delete from %s where %s.%s='%s';`
	dropColumn   = `ALTER TABLE "%s"."%s" DROP COLUMN %s;`
	dropTable    = `DROP TABLE %s;`
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
  p.contype IS NOT NULL AND p.contype = 'p' AS primary_key
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
	S3AcessKeyId      string `yaml:"s3AccessKeyId"`
	S3SecretAccessKey string `yaml:"s3SecretAccessKey"`
	Schema            string `yaml:"schema"`
}

func NewRedshift(ctx context.Context, conf RedshiftConfig) (*Redshift, error) {
	source := fmt.Sprintf(
		"host=%s port=%s dbname=%s connect_timeout=%d",
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
	return &Redshift{sqldb, ctx, conf}, nil
}

// Begin wraps a new transaction in the databases context
func (r *Redshift) Begin() (*sql.Tx, error) {
	return r.dbExecCloser.BeginTx(r.ctx, nil)
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
	Name       string `json:"dest"`
	Type       string `json:"type"`
	DefaultVal string `json:"defaultval"`
	NotNull    bool   `json:"notnull"`
	PrimaryKey bool   `json:"primarykey"`
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

func (r *Redshift) CreateSchema(tx *sql.Tx, schema string) error {
	createSQL := fmt.Sprintf(schemaCreate, schema)
	createStmt, err := tx.PrepareContext(r.ctx, createSQL)
	if err != nil {
		return err
	}

	_, err = createStmt.ExecContext(r.ctx)
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

// https://debezium.io/documentation/reference/1.2/connectors/mysql.html
// https://docs.aws.amazon.com/redshift/latest/dg/c_Supported_data_types.html
// debeziumToRedshift map
var typeMapping = map[string]string{
	"boolean": "boolean",
	"float":   "real",
	"float32": "real",
	"float64": "double precision",
	"int":     "integer",
	"int16":   "smallint",
	"int32":   "integer",
	"long":    "character varying(65535)",
	"bigint":  "bigint",
	"string":  "character varying(256)",
}

func getColumnSQL(c ColInfo) string {
	// note that we are relying on redshift
	// to fail if we have created multiple sort keys
	// currently we don't support that
	defaultVal := ""
	if c.DefaultVal != "" {
		switch c.Type {
		case "string":
			defaultVal = fmt.Sprintf("DEFAULT '%s'", c.DefaultVal)
		default:
			defaultVal = fmt.Sprintf("DEFAULT %s", c.DefaultVal)
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
		typeMapping[c.Type],
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

	args := []interface{}{strings.Join(columnSQL, ",")}
	createSQL := fmt.Sprintf(
		tableCreate,
		table.Meta.Schema,
		table.Name,
		strings.Join(columnSQL, ","),
	)

	klog.V(5).Infof("Preparing: %s with args: %v\n", createSQL, args)
	createStmt, err := tx.PrepareContext(r.ctx, createSQL)
	if err != nil {
		return fmt.Errorf("error preparing statement: %v\n", err)
	}

	klog.V(5).Infof("Running: %s with args: %v\n", createSQL, args)
	_, err = createStmt.ExecContext(r.ctx)

	return err
}

// UpdateTable figures out what columns we need to add to the target table
// based on the input table,
// and completes this action in the transaction provided
// Supported: add columns
// Supported: drop columns
// Supported: alter columns
// TODO:
// NotSupported: row ordering changes and row renames
func (r *Redshift) UpdateTable(
	tx *sql.Tx, inputTable, targetTable Table) error {
	klog.V(5).Infof("inputt Table: \n%+v\n", inputTable)
	klog.V(5).Infof("target Table: \n%+v\n", targetTable)

	columnOps, err := CheckSchemas(inputTable, targetTable)
	if err != nil {
		return err
	}

	if len(columnOps) == 0 {
		klog.V(4).Infof("Schema same for table: %s\n", inputTable.Name)
	} else {
		klog.Infof("Migrating schema for table: %s ...\n", inputTable.Name)
	}

	// postgres only allows adding one column at a time
	for _, op := range columnOps {
		klog.V(4).Infof("Preparing: %s", op)
		alterStmt, err := tx.PrepareContext(r.ctx, op)
		if err != nil {
			return err
		}
		klog.Infof("Running: %s", op)
		_, err = alterStmt.ExecContext(r.ctx)
		if err != nil {
			return fmt.Errorf("cmd failed, cmd:%s, err: %s\n", op, err)
		}
	}
	return nil
}

func (r *Redshift) prepareAndExecute(tx *sql.Tx, command string) error {
	klog.V(4).Infof("Preparing: %s\n", command)
	statement, err := tx.PrepareContext(r.ctx, command)
	if err != nil {
		return err
	}

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
		r.conf.S3AcessKeyId,
		r.conf.S3SecretAccessKey,
	)
	unLoadSQL := fmt.Sprintf(
		`UNLOAD ('select * from "%s"."%s"') TO '%s' %s manifest allowoverwrite`,
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
	schema string, table string, s3ManifestURI string, typeJson bool) error {

	json := ""
	if typeJson == true {
		json = "json 'auto'"
	}

	credentials := fmt.Sprintf(
		`CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s'`,
		r.conf.S3AcessKeyId,
		r.conf.S3SecretAccessKey,
	)
	copySQL := fmt.Sprintf(
		`COPY "%s"."%s" FROM '%s' %s manifest %s`,
		schema,
		table,
		s3ManifestURI,
		credentials,
		json,
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
			&c.PrimaryKey,
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
func CheckSchemas(inputTable, targetTable Table) ([]string, error) {
	return checkColumnsAndOrdering(inputTable, targetTable)
}

func checkColumn(schemaName string, tableName string,
	inCol ColInfo, targetCol ColInfo) ([]string, error) {
	// klog.V(5).Infof("inCol: %+v\n,taCol: %+v\n", inCol, targetCol)

	var errors error
	mismatchedTemplate := "mismatch col: %s, prop: %s, input: %v, target: %v"
	alterSQL := []string{}

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

	if typeMapping[inCol.Type] != targetCol.Type {
		alterSQL = append(alterSQL,
			fmt.Sprintf(
				`ALTER TABLE "%s"."%s" ALTER COLUMN %s %s %s`,
				schemaName,
				tableName,
				inCol.Name,
				"TYPE",
				typeMapping[inCol.Type],
			),
		)
	}

	// TODO: #40 #41 handle changes in null
	// if (inCol.NotNull != targetCol.NotNull) && !inCol.PrimaryKey {
	//
	// }

	// TODO: #40 #41 handle changes in default values
	// if ConvertDefaultValue(inCol.DefaultVal) != targetCol.DefaultVal {
	// }

	return alterSQL, errors
}

func checkColumnsAndOrdering(inputTable, targetTable Table) ([]string, error) {
	var columnOps []string
	var errors error

	inColMap := make(map[string]bool)

	for idx, inCol := range inputTable.Columns {
		inColMap[inCol.Name] = true

		// add column
		if len(targetTable.Columns) <= idx {
			klog.V(5).Info("Missing column, alter table will run.\n")
			alterSQL := fmt.Sprintf(
				`ALTER TABLE "%s"."%s" ADD COLUMN %s`,
				targetTable.Meta.Schema,
				targetTable.Name,
				getColumnSQL(inCol),
			)
			columnOps = append(columnOps, alterSQL)
			continue
		}

		// alter column
		targetCol := targetTable.Columns[idx]
		alterColumnOps, err := checkColumn(
			inputTable.Meta.Schema, inputTable.Name, inCol, targetCol)
		if err != nil {
			errors = multierror.Append(errors, err)
		}
		columnOps = append(columnOps, alterColumnOps...)
	}

	// drop column
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
			columnOps = append(columnOps, alterSQL)
			continue
		}
	}

	return columnOps, errors
}

func ConvertDefaultValue(val string) string {
	if val != "" {
		return "'" + val + "'" + "::character varying"
	}

	return val
}

var mysqlToRedshiftType = map[string]string{
	"BOOL":                        "INT2",
	"BOOLEAN":                     "INT2",
	"BIGINT":                      "INT8",
	"BIGINT UNSIGNED":             "NUMERIC(20, 0)",
	"BINARY":                      "VARCHAR",
	"BIT":                         "INT8",
	"BLOB":                        "VARCHAR(65535)",
	"CHAR":                        "VARCHAR",
	"DEC":                         "NUMERIC",
	"DECIMAL":                     "NUMERIC",
	"DECIMAL UNSIGNED":            "NUMERIC",
	"DOUBLE [PRECISION]":          "FLOAT8",
	"DOUBLE [PRECISION] UNSIGNED": "FLOAT8",
	"DATE":                        "DATE",
	"DATETIME":                    "TIMESTAMP",
	"ENUM":                        "VARCHAR",
	"FIXED":                       "NUMERIC",
	"FLOAT":                       "FLOAT4",
	"INT":                         "INT4",
	"INTEGER":                     "INT4",
	"INTEGER UNSIGNED":            "INT8",
	"LONGBLOB":                    "VARCHAR",
	"LONGTEXT":                    "VARCHAR(MAX)",
	"MEDIUMBLOB":                  "VARCHAR",
	"MEDIUMINT":                   "INT4",
	"MEDIUMINT UNSIGNED":          "INT4",
	"MEDIUMTEXT":                  "VARCHAR(MAX)",
	"NUMERIC":                     "NUMERIC",
	"SET":                         "VARCHAR",
	"SMALLINT":                    "INT2",
	"SMALLINT UNSIGNED":           "INT4",
	"TEXT":                        "VARCHAR(MAX)",
	"TIME":                        "TIMESTAMP",
	"TIMESTAMP":                   "TIMESTAMP",
	"TINYBLOB":                    "VARCHAR",
	"TINYINT":                     "INT2",
	"TINYINT UNSIGNED":            "INT2",
	"TINYTEXT":                    "VARCHAR(MAX)",
	"VARBINARY":                   "VARCHAR(MAX)",
	"VARCHAR":                     "VARCHAR",
	"YEAR":                        "DATE",
}

// GetRedshiftDataType returns the mapped type for the sqlType's data type
func GetRedshiftDataType(sqlType, defaultType,
	sourceColType string) (string, error) {

	switch sqlType {
	case "mysql":
		redshiftType, ok := mysqlToRedshiftType[sourceColType]
		if ok {
			return redshiftType, nil
		}
		// default is not cause error but use the default type
		return defaultType, nil
	}

	return "", fmt.Errorf("Unsupported sqlType:%s\n", sqlType)
}
