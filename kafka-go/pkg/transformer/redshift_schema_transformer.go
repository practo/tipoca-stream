package transformer

import (
	"encoding/json"
	"fmt"
	"github.com/practo/tipoca-stream/kafka-go/pkg/redshift"
	"github.com/riferrei/srclient"
	"strings"
)

type DebeziumSchema struct {
	Type        string                `yaml:"type"`
	Name        string                `yaml:"name"`
	Namespace   string                `yaml:"namespace"`
	Fields      []DebeziumSchemaField `yaml:"fields"`
	ConnectName string                `yaml:"connect.name"`
}

type DebeziumSchemaField struct {
	Name    string      `yaml:"name"`
	Type    interface{} `yaml:"type"`
	Default interface{} `yaml:"default"`
}

type DebeziumColInfo struct {
	Name       string `yaml:"name"`
	Type       string `yaml:"type"`
	Default    string `yaml:"default"`
	NotNull    bool   `yaml:"notnull"`
	PrimaryKey bool   `yaml:"primarykey"`
}

type debeziumSchemaParser struct {
	tableDelim string

	schema  DebeziumSchema
	columns DebeziumColInfo
}

func (d *debeziumSchemaParser) schemaName() string {
	namespace := strings.Split(d.schema.Namespace, d.tableDelim)
	return strings.Join(namespace[0:len(namespace)-1], d.tableDelim)
}

func (d *debeziumSchemaParser) tableName() string {
	namespace := strings.Split(d.schema.Namespace, d.tableDelim)
	return namespace[len(namespace)-1]
}

func debeziumColumn(v map[string]interface{}) DebeziumColInfo {
	column := DebeziumColInfo{}

	// TODO: figure out not null and primarykey
	// https://stackoverflow.com/questions/63576770/
	// debezium-schema-not-null-and-primary-key-info
	// need to use customers-key and not customers-value
	// nullable fields look like, handle it
	//    "null",
	//    "string"
	//     ],
	for key, v2 := range v {
		switch key {
		case "name":
			column.Name = v["name"].(string)
		case "type":
			switch v2.(type) {
			case string:
				column.Type = v["type"].(string)
			case interface{}:
				listSlice, ok := v2.([]interface{})
				if !ok {
					fmt.Printf("error typ casting: %v, ignoring", v2)
					continue
				}
				for _, vx := range listSlice {
					switch vx.(type) {
					case map[string]interface{}:
						for k3, v3 := range vx.(map[string]interface{}) {
							if k3 != "type" {
								continue
							}
							column.Type = v3.(string)
						}
					}
				}
			default:
				column.Type = v["type"].(string)
			}
		case "default":
			column.Default = v["default"].(string)
		}
	}

	return column
}

// TOOD: make this better if possible
// https://stackoverflow.com/questions/63564543/
// decode-a-debeuzium-event-schema-into-a-meaningful-datastructure-in-golang
func (d *debeziumSchemaParser) columnsBefore() []DebeziumColInfo {
	columns := []DebeziumColInfo{}

	for _, field := range d.schema.Fields {
		if field.Name != "before" {
			continue
		}

		for _, v1 := range field.Type.([]interface{}) {
			switch v1.(type) {
			case map[string]interface{}:
				v2 := v1.(map[string]interface{})
				for k3, v3 := range v2 {
					if k3 != "fields" {
						continue
					}
					for _, v4 := range v3.([]interface{}) {
						v5 := v4.(map[string]interface{})
						columns = append(columns, debeziumColumn(v5))
					}
				}

			}
		}
	}

	return columns
}

func NewSchemaTransformer(schemaRegistryURL string) SchemaTransformer {
	return &redshiftSchemaTransformer{
		srclient: srclient.CreateSchemaRegistryClient(schemaRegistryURL),
	}
}

type redshiftSchemaTransformer struct {
	srclient *srclient.SchemaRegistryClient
}

func (c *redshiftSchemaTransformer) Transform(schemaId int) (
	interface{}, error) {

	j, err := c.srclient.GetSchema(schemaId)
	if err != nil {
		return nil, err
	}

	return c.transformSchema(j.Schema())
}

func (c *redshiftSchemaTransformer) transformSchema(jobSchema string) (
	interface{}, error) {

	// remove nulls
	schema := strings.ReplaceAll(jobSchema, `"null",`, "")

	var debeziumSchema DebeziumSchema
	err := json.Unmarshal([]byte(schema), &debeziumSchema)
	if err != nil {
		return nil, err
	}

	d := &debeziumSchemaParser{
		tableDelim: ".",
		schema:     debeziumSchema,
	}

	columns := d.columnsBefore()
	var redshiftColumns []redshift.ColInfo
	for _, column := range columns {
		redshiftColumns = append(redshiftColumns, redshift.ColInfo{
			Name:       column.Name,
			Type:       column.Type,
			DefaultVal: column.Default,
			NotNull:    column.NotNull,
			PrimaryKey: column.PrimaryKey,
		})
	}

	table := redshift.Table{
		Name:    d.tableName(),
		Columns: redshiftColumns,
		Meta: redshift.Meta{
			Schema: d.schemaName(),
		},
	}

	return table, nil
}
