package transformer

import (
	"encoding/json"
	"fmt"
	"github.com/practo/tipoca-stream/kafka-go/pkg/redshift"
	"github.com/riferrei/srclient"
	"os"
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

	// TODO: Have figured out not null and primary key, TODO is open
	// because not null is set only when the default==nil
	// https://stackoverflow.com/questions/63576770/
	for key, v2 := range v {
		switch key {
		case "name":
			column.Name = v["name"].(string)
		case "type":
			switch v2.(type) {
			case string:
				column.Type = v["type"].(string)
			case interface{}:
				// handles
				// [map[connect.default:singh type:string], null]
				listSlice, ok := v2.([]interface{})
				if ok {
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
					continue
				}

				// handles
				// map[connect.default:singh type:string]
				listMap, ok := v2.(map[string]interface{})
				if !ok {
					fmt.Printf("Error type casting for v2=%v\n", v2)
					os.Exit(1)
				}
				for k4, v4 := range listMap {
					if k4 != "type" {
						continue
					}
					column.Type = v4.(string)
				}
			default:
				column.Type = v["type"].(string)
			}
		case "default":
			if v["default"] == nil {
				column.Default = ""
				column.NotNull = true
			} else {
				column.Default = v["default"].(string)
			}
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
	return &debeziumSchemaTransformer{
		srclient: srclient.CreateSchemaRegistryClient(schemaRegistryURL),
	}
}

type debeziumSchemaTransformer struct {
	srclient *srclient.SchemaRegistryClient
}

func (c *debeziumSchemaTransformer) TransformKey(topic string) (
	string, string, error) {

	s, err := c.srclient.GetLatestSchema(topic, true)
	if err != nil {
		return "", "", err
	}

	return c.transformSchemaKey(s.Schema())
}

func (c *debeziumSchemaTransformer) transformSchemaKey(
	schema string) (string, string, error) {

	debeziumSchema := make(map[string]interface{})
	err := json.Unmarshal([]byte(schema), &debeziumSchema)
	if err != nil {
		return "", "", err
	}

	fields, ok := debeziumSchema["fields"].([]interface{})
	if !ok {
		return "", "", fmt.Errorf("Error parsing schema: %s\n", schema)
	}

	for _, field := range fields {
		switch field.(type) {
		case map[string]interface{}:
			primaryKey := ""
			primaryKeyType := ""
			for fieldKey, fieldValue := range field.(map[string]interface{}) {
				switch fieldKey {
				case "name":
					primaryKey = fmt.Sprintf("%v", fieldValue)
				case "type":
					primaryKeyType = fmt.Sprintf("%v", fieldValue)
				}
			}
			if primaryKey != "" && primaryKeyType != "" {
				return primaryKey, primaryKeyType, nil
			}
		}
	}

	return "", "", fmt.Errorf("Primary key not found in schema: %s\n", schema)
}

func (c *debeziumSchemaTransformer) TransformValue(topic string, schemaId int) (
	interface{}, error) {

	s, err := c.srclient.GetSchema(schemaId)
	if err != nil {
		return nil, err
	}

	primaryKey, primaryKeyType, err := c.TransformKey(topic)
	if err != nil {
		return nil, err
	}

	if primaryKey == "" || primaryKeyType == "" {
		return nil, fmt.Errorf(
			"primary key not found for topic:%s, schemaId:%d\n",
			topic,
			schemaId,
		)
	}

	return c.transformSchemaValue(s.Schema(), primaryKey, primaryKeyType)
}

func (c *debeziumSchemaTransformer) transformSchemaValue(jobSchema string,
	primaryKey string, primaryKeyType string) (interface{}, error) {

	// remove nulls
	// TODO: this might be required, better if not
	// schema := strings.ReplaceAll(jobSchema, `"null",`, "")
	schema := jobSchema

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

	// set primary key
	for idx, column := range redshiftColumns {
		if column.Name == primaryKey && column.Type == primaryKeyType {
			column.PrimaryKey = true
			redshiftColumns[idx] = column
		}
	}

	table := redshift.Table{
		Name:    d.tableName(),
		Columns: redshiftColumns,
		Meta: redshift.Meta{
			Schema: d.schemaName(),
		},
	}

	fmt.Printf("%+v\n", table)

	return table, nil
}
