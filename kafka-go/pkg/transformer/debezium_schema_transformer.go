package transformer

import (
	"encoding/json"
	// "reflect"
	"fmt"
	"github.com/practo/klog/v2"
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
	Name       string             `yaml:"name"`
	Type       string             `yaml:"type"`
	SourceType DebeziumSourceType `yaml:"debeziumSourceType"`
	Default    string             `yaml:"default"`
	NotNull    bool               `yaml:"notnull"`
	PrimaryKey bool               `yaml:"primarykey"`
}

type DebeziumSourceType struct {
	ColumnLength string `yaml:"columnLength"`
	ColumnType   string `yaml:"columnType"`
}

type DebeziumDataType struct {
	Type               string `yaml:"type"`
	SourceColumnType   string `yaml:"sourceColumnType"`
	SourceColumnLength string `yaml:"sourceColumnLength"`
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

func (d *debeziumSchemaParser) sqlType() string {
	// TODO: parse to send mysql and postgres dependening on the connector
	// do this when postgres comes
	return "mysql"
}

func getSourceType(v interface{}) DebeziumSourceType {
	valueMap := v.(map[string]interface{})

	var columnType string
	var columnLength string
	fieldsFound := 0

	for key, value := range valueMap {
		if key == "__debezium.source.column.length" {
			columnLength = fmt.Sprintf("%s", value)
			fieldsFound = fieldsFound + 1
		}

		if key == "__debezium.source.column.type" {
			columnType = fmt.Sprintf("%s", value)
			fieldsFound = fieldsFound + 1
		}
	}

	if fieldsFound == 0 {
		klog.Warningf("Source info missing in %+v\n", v)
	}

	return DebeziumSourceType{
		ColumnType:   columnType,
		ColumnLength: columnLength,
	}
}

func debeziumColumn(v map[string]interface{}) DebeziumColInfo {
	column := DebeziumColInfo{}

	// TODO: Have figured out not null and primary key, TODO is open
	// because not null is set only when the default==nil
	// https://stackoverflow.com/questions/63576770/debezium-schema-not-null-and-primary-key-info/

	for key, v2 := range v {
		switch key {
		case "name":
			column.Name = v["name"].(string)
		case "type":
			switch v2.(type) {
			case interface{}:
				// handles slice
				// [
				//   null,
				//   map[
				//       connect.parameters:map[
				//			__debezium.source.column.length:255
				//          __debezium.source.column.type:VARCHAR
				//       ]
				//       type:string
				//   ],
				// ]
				listSlice, ok := v2.([]interface{})
				if ok {
					for _, vx := range listSlice {
						switch vx.(type) {
						// handles if value is map
						case map[string]interface{}:
							for k3, v3 := range vx.(map[string]interface{}) {
								if k3 == "type" {
									column.Type = v3.(string)
								}
								if k3 == "connect.parameters" {
									column.SourceType = getSourceType(v3)
								}
							}
						}
					}
					// handled the case continue case
					continue
				}

				// handles map
				// map[
				// 		connect.parameters:map[
				//          __debezium.source.column.length:255
				//          __debezium.source.column.type:VARCHAR
				//      ]
				//      type:string
				// ]
				listMap, ok := v2.(map[string]interface{})
				if !ok {
					fmt.Printf("Error type casting, value=%v\n", v2)
					os.Exit(1)
				}
				for k4, v4 := range listMap {
					if k4 == "type" {
						column.Type = v4.(string)
					}
					if k4 == "connect.parameters" {
						column.SourceType = getSourceType(v4)
					}
				}
			default:
				fmt.Printf("Unhandled type for v2=%v\n", v2)
				os.Exit(1)
			}
		case "default":
			if v["default"] == nil {
				column.Default = ""
				column.NotNull = false
			} else {
				column.Default = fmt.Sprintf("%v", v["default"])
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
		redshiftDataType, err := redshift.GetRedshiftDataType(
			d.sqlType(),
			column.Type,
			column.SourceType.ColumnLength,
		)
		if err != nil {
			return nil, err
		}

		redshiftColumns = append(redshiftColumns, redshift.ColInfo{
			Name:       strings.ToLower(column.Name),
			Type:       redshiftDataType,
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

	return table, nil
}
