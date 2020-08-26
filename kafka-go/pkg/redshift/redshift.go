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
	// todo due to: https://github.com/Clever/s3-to-redshift/issues/163
	_ "github.com/lib/pq"
	"strings"
)

type dbExecCloser interface {
	Close() error
	BeginTx(c context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	QueryContext(c context.Context, q string, a ...interface{}) (*sql.Rows, error)
	QueryRowContext(c context.Context, q string, a ...interface{}) *sql.Row
}

// Redshift wraps a dbExecCloser and can be used to perform
// operations on a redshift database.
// Give it a context for the duration of the job
type Redshift struct {
	dbExecCloser
	ctx context.Context
}

type RedshiftConfig struct {
	Host     string `yaml:"host"`
	Port     string `yaml:"port"`
	Database string `yaml:"database"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Timeout  int    `yaml:"timeout"`
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
	return &Redshift{sqldb, ctx}, nil
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

const (
	schemaExist  = `SELECT schema_name FROM information_schema.schemata WHERE schema_name="%s";`
	schemaCreate = `CREATE SCHEMA "%s";`
	tableExist   = `SELECT table_name FROM information_schema.tables WHERE table_schema="%s" AND table_name="%s";`
	tableCreate  = `CREATE TABLE "%s"."%s" (%s);`
	// returns one row per column with the attributes:
	// name, type, default_val, not_null, primary_key, dist_key, and sort_ordinal
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
		// TODO: fix me
		// return false, fmt.Errorf("error querying schema exist: %v\n", err)
		return false, nil
	}

	return true, nil
}

func (r *Redshift) CreateSchema(tx *sql.Tx, schema string) error {
	createSQL := fmt.Sprintf(schemaCreate, schema)
	klog.V(5).Infof("Creating Schema: sql=%s\n", createSQL)
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
		// TODO: fix the induced bug
		// return false, fmt.Errorf("failed sql:%s, err:%v\n", q, err)
		return false, nil
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
	"bigint":  "bigint",
	"string":  "character varying(256)",
}

func getColumnSQL(c ColInfo) string {
	// note that we are relying on redshift
	// to fail if we have created multiple sort keys
	// currently we don't support that
	defaultVal := ""
	if c.DefaultVal != "" {
		defaultVal = fmt.Sprintf("DEFAULT %s", c.DefaultVal)
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
		table,
		strings.Join(columnSQL, ","),
	)
	klog.V(5).Infof("Creating Table: sql=%s\n", createSQL)

	createStmt, err := tx.PrepareContext(r.ctx, createSQL)
	if err != nil {
		return fmt.Errorf("error preparing statement: %v\n", err)
	}

	klog.V(5).Infof("Running command: %s with args: %v\n", createSQL, args)
	_, err = createStmt.ExecContext(r.ctx)

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
			"error running column query: %s, err: %s", tableSchema, err)
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

// CheckSchemas takes in two tables and compares their column schemas to make sure they're compatible.
// If they have any mismatched columns they are returned in the errors array. If the input table has
// columns at the end that the target table does not then the appropriate alter tables sql commands are
// returned.
func CheckSchemas(inputTable, targetTable Table) ([]string, error) {
	// If the schema is mongo_raw then we know the input files are json so ordering doesn't matter. At
	// some point we could handle this in a more general way by checking if the input files are json.
	// This wouldn't be too hard, but we would have to peak in the manifest file to check if all the
	// files references are json.
	if targetTable.Meta.Schema == "mongo_raw" {
		return checkColumnsWithoutOrdering(inputTable, targetTable)
	}
	return checkColumnsAndOrdering(inputTable, targetTable)
}

func checkColumn(inCol ColInfo, targetCol ColInfo) error {
	var errors error
	mismatchedTemplate := "mismatched column: %s property: %s, input: %v, target: %v"
	if inCol.Name != targetCol.Name {
		errors = multierror.Append(errors, fmt.Errorf(mismatchedTemplate, inCol.Name, "Name", inCol.Name, targetCol.Name))
	}
	if typeMapping[inCol.Type] != targetCol.Type {
		if strings.HasPrefix(typeMapping[inCol.Type], "character varying") && strings.HasPrefix(typeMapping[inCol.Type], "character varying") {
			// If they are both varchars but differing values, we will ignore this
		} else {
			errors = multierror.Append(errors, fmt.Errorf(mismatchedTemplate, inCol.Name, "Type", typeMapping[inCol.Type], targetCol.Type))
		}
	}
	if inCol.DefaultVal != targetCol.DefaultVal {
		errors = multierror.Append(errors, fmt.Errorf(mismatchedTemplate, inCol.Name, "DefaultVal", inCol.DefaultVal, targetCol.DefaultVal))
	}
	if inCol.NotNull != targetCol.NotNull {
		errors = multierror.Append(errors, fmt.Errorf(mismatchedTemplate, inCol.Name, "NotNull", inCol.NotNull, targetCol.NotNull))
	}
	if inCol.PrimaryKey != targetCol.PrimaryKey {
		errors = multierror.Append(errors, fmt.Errorf(mismatchedTemplate, inCol.Name, "PrimaryKey", inCol.PrimaryKey, targetCol.PrimaryKey))
	}
	return errors
}

func checkColumnsAndOrdering(inputTable, targetTable Table) ([]string, error) {
	var columnOps []string
	var errors error
	if len(inputTable.Columns) < len(targetTable.Columns) {
		errors = multierror.Append(errors, fmt.Errorf("target table has more columns than the input table"))
	}

	for idx, inCol := range inputTable.Columns {
		if len(targetTable.Columns) <= idx {
			klog.V(5).Info("Missing column -- running alter table\n")
			alterSQL := fmt.Sprintf(`ALTER TABLE "%s"."%s" ADD COLUMN %s`, targetTable.Meta.Schema, targetTable.Name, getColumnSQL(inCol))
			columnOps = append(columnOps, alterSQL)
			continue
		}

		targetCol := targetTable.Columns[idx]
		err := checkColumn(inCol, targetCol)
		if err != nil {
			errors = multierror.Append(errors, err)
		}

	}
	return columnOps, errors
}

func checkColumnsWithoutOrdering(inputTable, targetTable Table) ([]string, error) {
	var columnOps []string
	var errors error

	for _, inCol := range inputTable.Columns {
		foundMatching := false
		for _, targetCol := range targetTable.Columns {
			if inCol.Name == targetCol.Name {
				foundMatching = true
				if err := checkColumn(inCol, targetCol); err != nil {
					errors = multierror.Append(errors, err)
				}
			}
		}
		if !foundMatching {
			klog.V(5).Info("Missing column -- running alter table\n")
			alterSQL := fmt.Sprintf(`ALTER TABLE "%s"."%s" ADD COLUMN %s`,
				targetTable.Meta.Schema, targetTable.Name, getColumnSQL(inCol))
			columnOps = append(columnOps, alterSQL)
		}
	}
	return columnOps, errors
}
