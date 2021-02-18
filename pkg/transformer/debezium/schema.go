package debezium

import (
	"encoding/json"
	"fmt"
	"github.com/practo/klog/v2"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/redshift"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/schemaregistry"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/serializer"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/transformer"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/transformer/masker"
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
	SourceType SourceType `yaml:"sourceType"`
	Default    string     `yaml:"default"`
	NotNull    bool       `yaml:"notnull"`
	PrimaryKey bool       `yaml:"primarykey"`
}

type SourceType struct {
	ColumnLength string `yaml:"columnLength"`
	ColumnType   string `yaml:"columnType"`
	ColumnScale  string `yaml:"columnScale"`
}

func NewSchemaTransformer(url string) transformer.SchemaTransformer {
	return &schemaTransformer{
		maskConfig: make(map[int]masker.MaskConfig),
		registry:   schemaregistry.NewRegistry(url),
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
	var columnScale string
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

		if key == "__debezium.source.column.scale" {
			columnScale = fmt.Sprintf("%s", value)
			fieldsFound = fieldsFound + 1
		}
	}
	if fieldsFound == 0 {
		klog.Warningf("Source info missing in %+v\n", v)
	}

	return SourceType{
		ColumnType:   columnType,
		ColumnLength: columnLength,
		ColumnScale:  columnScale,
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
	registry   schemaregistry.SchemaRegistry
}

func (c *schemaTransformer) TransformKey(topic string) ([]string, error) {
	s, err := schemaregistry.GetLatestSchemaWithRetry(c.registry, topic, true, 3)
	if err != nil {
		return []string{}, err
	}

	return c.transformSchemaKey(s.Schema())
}

func (c *schemaTransformer) transformSchemaKey(
	schema string) ([]string, error) {

	var primaryKeys []string
	debeziumSchema := make(map[string]interface{})
	err := json.Unmarshal([]byte(schema), &debeziumSchema)
	if err != nil {
		return primaryKeys, err
	}

	fields, ok := debeziumSchema["fields"].([]interface{})
	if !ok {
		return primaryKeys, fmt.Errorf("Error parsing schema: %s\n", schema)
	}

	for _, field := range fields {
		switch field.(type) {
		case map[string]interface{}:
			for fieldKey, fieldValue := range field.(map[string]interface{}) {
				switch fieldKey {
				case "name":
					primaryKeys = append(
						primaryKeys, fmt.Sprintf("%v", fieldValue),
					)
				}
			}
		}
	}

	if len(primaryKeys) != 0 {
		return primaryKeys, nil
	}

	return primaryKeys, fmt.Errorf("Primarykey not found, schema: %s\n", schema)
}

func isPrimaryKey(columnName string, primaryKeys []string) bool {
	for _, pk := range primaryKeys {
		if pk == columnName {
			return true
		}
	}

	return false
}

func (c *schemaTransformer) TransformValue(topic string, schemaId int,
	maskSchema map[string]serializer.MaskInfo) (interface{}, error) {

	s, err := schemaregistry.GetSchemaWithRetry(c.registry, schemaId, 10)
	if err != nil {
		return nil, err
	}

	primaryKeys, err := c.TransformKey(topic)
	if err != nil {
		return nil, err
	}

	return c.transformSchemaValue(
		s.Schema(),
		primaryKeys,
		maskSchema,
	)
}

func (c *schemaTransformer) transformSchemaValue(jobSchema string,
	primaryKeys []string,
	maskSchema map[string]serializer.MaskInfo) (interface{}, error) {

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
	var extraColumns []redshift.ColInfo
	for _, column := range columns {
		sortKey := false
		distKey := false
		columnMasked := false
		if len(maskSchema) != 0 {
			mschema, ok := maskSchema[column.Name]
			if ok {
				sortKey = mschema.SortCol
				distKey = mschema.DistCol
				columnMasked = mschema.Masked
				if mschema.LengthCol {
					newColName := strings.ToLower(
						column.Name) + transformer.LengthColumnSuffix
					extraColumns = append(extraColumns, redshift.ColInfo{
						Name:         newColName,
						Type:         redshift.RedshiftInteger,
						DebeziumType: "", // not required
						DefaultVal:   "0",
						NotNull:      false,
						PrimaryKey:   false,
						SortOrdinal:  0,
						DistKey:      false,
						SourceType:   redshift.SourceType{}, // not required
					})
				}
				if mschema.MobileCol {
					newColName := strings.ToLower(
						column.Name) + transformer.MobileCoulmnSuffix
					extraColumns = append(extraColumns, redshift.ColInfo{
						Name:         newColName,
						Type:         redshift.RedshiftMobileColType,
						DebeziumType: "", // not required
						DefaultVal:   "",
						NotNull:      false,
						PrimaryKey:   false,
						SortOrdinal:  0,
						DistKey:      false,
						SourceType:   redshift.SourceType{}, // not required
					})
				}
				if mschema.MappingPIICol {
					newColName := strings.ToLower(
						transformer.MappingPIIColumnPrefix + column.Name,
					)
					extraColumns = append(extraColumns, redshift.ColInfo{
						Name:         newColName,
						Type:         redshift.RedshiftMaskedDataType,
						DebeziumType: "", // not required
						DefaultVal:   "",
						NotNull:      false,
						PrimaryKey:   false,
						SortOrdinal:  0,
						DistKey:      false,
						SourceType:   redshift.SourceType{}, // not required
					})
				}
			}
		}
		redshiftDataType, err := redshift.GetRedshiftDataType(
			d.sqlType(),
			column.Type,
			column.SourceType.ColumnType,
			column.SourceType.ColumnLength,
			column.SourceType.ColumnScale,
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
			SourceType: redshift.SourceType{
				ColumnLength: column.SourceType.ColumnLength,
				ColumnType:   column.SourceType.ColumnType,
				ColumnScale:  column.SourceType.ColumnScale,
			},
		})
	}

	// set primary key
	for idx, column := range redshiftColumns {
		if isPrimaryKey(column.Name, primaryKeys) {
			column.PrimaryKey = true
			redshiftColumns[idx] = column
		}
	}

	// add extra columns (length columns)
	for _, extraColumn := range extraColumns {
		redshiftColumns = append(redshiftColumns, extraColumn)
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
