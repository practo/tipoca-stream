package debezium

import (
	"encoding/json"
	"fmt"
	"github.com/practo/klog/v2"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/redshift"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/transformer"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/transformer/masker"
	"github.com/riferrei/srclient"
	"strings"
)

type Schema struct {
	Type        string        `yaml:"type"`
	Name        string        `yaml:"name"`
	Namespace   string        `yaml:"namespace"`
	Fields      []SchemaField `yaml:"fields"`
	ConnectName string        `yaml:"connect.name"`
}

type SchemaField struct {
	Name    string      `yaml:"name"`
	Type    interface{} `yaml:"type"`
	Default interface{} `yaml:"default"`
}

type ColInfo struct {
	Name       string     `yaml:"name"`
	Type       string     `yaml:"type"`
	SourceType SourceType `yaml:"debeziumSourceType"`
	Default    string     `yaml:"default"`
	NotNull    bool       `yaml:"notnull"`
	PrimaryKey bool       `yaml:"primarykey"`
}

type SourceType struct {
	ColumnLength string `yaml:"columnLength"`
	ColumnType   string `yaml:"columnType"`
}

func NewSchemaTransformer(url string) transformer.SchemaTransformer {
	return &schemaTransformer{
		maskConfig: make(map[int]masker.MaskConfig),
		srclient:   srclient.CreateSchemaRegistryClient(url),
	}
}

type schemaParser struct {
	schema  Schema
	columns ColInfo

	tableDelim string
}

func (d *schemaParser) schemaName() string {
	namespace := strings.Split(d.schema.Namespace, d.tableDelim)
	return strings.Join(namespace[0:len(namespace)-1], d.tableDelim)
}

func (d *schemaParser) tableName() string {
	namespace := strings.Split(d.schema.Namespace, d.tableDelim)
	return namespace[len(namespace)-1]
}

func (d *schemaParser) sqlType() string {
	// TODO: parse to send mysql and postgres dependening on the connector
	// do this when postgres comes
	return "mysql"
}

func getSourceType(v interface{}) SourceType {
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

	return SourceType{
		ColumnType:   columnType,
		ColumnLength: columnLength,
	}
}

// column extracts the column information from the schema fields
func column(v map[string]interface{}) ColInfo {
	//fmt.Printf("v=%+v\n", v)
	column := ColInfo{}
	// TODO: Have figured out not null and primary key, TODO is open
	// because not null is set only when the default==nil
	// https://stackoverflow.com/questions/63576770/debezium-schema-not-null-and-primary-key-info/
	for key, v2 := range v {
		switch key {
		case "name":
			column.Name = v["name"].(string)
		case "type":
			//fmt.Printf("name=%v v2=%v\n",v["name"], v2)
			switch v2.(type) {
			case string:
				column.Type = v["type"].(string)
			case int:
				column.Type = v["type"].(string)
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
						// handles ["null", "string"]
						case string:
							if vx != "null" {
								column.Type = vx.(string)
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
					klog.Fatalf("Error type casting, value=%+v\n", v2)
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
				klog.Fatalf("Unhandled type for v2=%v\n", v2)
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

// TOOD: make this better and faster if possible
// https://stackoverflow.com/questions/63564543/
// decode-a-debeuzium-event-schema-into-a-meaningful-datastructure-in-golang
func (d *schemaParser) columnsBefore() []ColInfo {
	columns := []ColInfo{}

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
						columns = append(columns, column(v5))
					}
				}

			}
		}
	}

	return columns
}

type schemaTransformer struct {
	mask       bool
	maskConfig map[int]masker.MaskConfig
	srclient   *srclient.SchemaRegistryClient
}

func (c *schemaTransformer) TransformKey(topic string) (string, string, error) {
	s, err := c.srclient.GetLatestSchema(topic, true)
	if err != nil {
		return "", "", err
	}

	return c.transformSchemaKey(s.Schema())
}

func (c *schemaTransformer) transformSchemaKey(
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

func (c *schemaTransformer) TransformValue(
	topic string, schemaId int, maskConfDir string) (interface{}, error) {

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

	if maskConfDir != "" {
		c.mask = true
		_, ok := c.maskConfig[schemaId]
		if !ok {
			maskConfig, err := masker.NewMaskConfig(maskConfDir, topic)
			if err != nil {
				klog.Fatalf("Error making masking config: %v\n", err)
			}
			c.maskConfig[schemaId] = maskConfig
		}
	}

	return c.transformSchemaValue(
		schemaId,
		s.Schema(),
		primaryKey,
		c.mask,
	)
}

func (c *schemaTransformer) transformSchemaValue(schemaId int, jobSchema string,
	primaryKey string, mask bool) (interface{}, error) {

	// remove nulls
	// TODO: this might be required, better if not
	// schema := strings.ReplaceAll(jobSchema, `"null",`, "")
	schema := jobSchema

	var debeziumSchema Schema
	err := json.Unmarshal([]byte(schema), &debeziumSchema)
	if err != nil {
		return nil, err
	}

	d := &schemaParser{
		tableDelim: ".",
		schema:     debeziumSchema,
	}

	columns := d.columnsBefore()

	var redshiftColumns []redshift.ColInfo
	for _, column := range columns {
		columnMasked := false
		sortKey := false
		distKey := false
		if mask {
			maskConfig := c.maskConfig[schemaId]
			columnMasked = maskConfig.Masked(d.tableName(), column.Name)
			sortKey = maskConfig.SortKey(d.tableName(), column.Name)
			distKey = maskConfig.DistKey(d.tableName(), column.Name)
		}
		redshiftDataType, err := redshift.GetRedshiftDataType(
			d.sqlType(),
			column.Type,
			column.SourceType.ColumnType,
			columnMasked,
		)
		if err != nil {
			return nil, err
		}

		sortOrdinal := 0
		if sortKey {
			sortOrdinal = 1
		}

		redshiftColumns = append(redshiftColumns, redshift.ColInfo{
			Name:         strings.ToLower(column.Name),
			Type:         redshiftDataType,
			DebeziumType: column.Type,
			DefaultVal:   column.Default,
			NotNull:      column.NotNull,
			PrimaryKey:   column.PrimaryKey,
			SortOrdinal:  sortOrdinal,
			DistKey:      distKey,
		})
	}

	// set primary key
	for idx, column := range redshiftColumns {
		if column.Name == primaryKey {
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
