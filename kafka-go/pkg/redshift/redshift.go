package redshift

import (
    "fmt"
    "context"
    "database/sql"
    yaml "gopkg.in/yaml.v2"
    "github.com/practo/klog/v2"
)

type dbExecCloser interface {
	Close() error
	BeginTx(ctx context.Context,
            opts *sql.TxOptions) (*sql.Tx, error)
	QueryContext(ctx context.Context,
                 query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context,
                    query string, args ...interface{}) *sql.Row
}

// Redshift wraps a dbExecCloser and can be used to perform
// operations on a redshift database.
// Give it a context for the duration of the job
type Redshift struct {
	dbExecCloser
	ctx context.Context
}

func NewRedshift(ctx context.Context, host, port, db, user, password string,
    timeout int) (*Redshift, error) {

	source := fmt.Sprintf(
        "host=%s port=%s dbname=%s keepalive=1 connect_timeout=%d",
        host, port, db, timeout,
    )
	source += fmt.Sprintf(" user=%s password=%s", user, password)
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
	Name    string    `yaml:"dest"`
	Columns []ColInfo `yaml:"columns"`
	Meta    Meta      `yaml:"meta"`
}

// Meta holds information that might be not in Redshift or annoying to access
// in this case, schema a table is part of and column which
// corresponds to the timestamp at which the data was gathered
type Meta struct {
	DataDateColumn string `yaml:"datadatecolumn"`
	Schema         string `yaml:"schema"`
}

// ColInfo is a struct that contains information
// about a column in a Redshift database.
// SortOrdinal and DistKey only make sense for Redshift
type ColInfo struct {
	Name        string `yaml:"dest"`
	Type        string `yaml:"type"`
	DefaultVal  string `yaml:"defaultval"`
	NotNull     bool   `yaml:"notnull"`
	PrimaryKey  bool   `yaml:"primarykey"`
	DistKey     bool   `yaml:"distkey"`
	SortOrdinal int    `yaml:"sortord"`
}

const (
    schemaExist  = `select schema_name from information_schema.schemata where schema_name='%s'`
    schemaCreate = `create schema '%s'`
    tableExist   = `select table_name from information_schema.tables where table_schema='%s' and table_name='%s'`
    tableCreate  = `create table "%s"."%s" (%s)`
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
    iferr != nil {
		if err == sql.ErrNoRows {
			klog.V(5).Infof(
                "schema: %s, table: %s does not exist", schema, table,
            )
			return false, nil
		}
		return false, fmt.Errorf("error querying table exists: %v\n", err)
	}

    return true, nil
}

func (r *Redshift) CreateTable(
    tx *sql.Tx, schema string, table string, columnSQL []string) error {

	args := []interface{}{strings.Join(columnSQL, ",")}
	createSQL := fmt.Sprintf(
        tableCreate,
        schema,
        table,
        strings.Join(columnSQL, ","),
    )

	if match, _ := regexp.MatchString("SORTKEY|DISTKEY", createSQL); !match {
        return fmt.Errorf("SORTKEY and DISTKEY are mandatory in create table")
	}

	createStmt, err := tx.PrepareContext(r.ctx, createSQL)
	if err != nil {
		return fmt.Errorf("error preparing statement: %v\n", err)
	}

	klog.V(5).Infof("Running command: %s with args: %v\n", createSQL, args)
	_, err = createStmt.ExecContext(r.ctx)

    return err
}
