package transformer

import (
	"encoding/json"
)

type DebeziumSchema struct {
	Type        string        `json:"type"`
	Name        string        `json:"name"`
	Namespace   string        `json:"namespace"`
	Fields      []SchemaField `json:"fields"`
	ConnectName string        `json:"connect.name"`
}

type SchemaField struct {
	Name    string      `json:"name"`
	Type    interface{} `json:"type"`
	Default interface{} `json:"default,omitempty"`
}

type debeziumSchemaParser struct {
	schemaStr map[string]interface{}
	schema    DebeziumSchema
}

func (d *debeziumSchemaParser) Decode() error {
	err := json.Unmarshal([]byte(d.schemaStr), &d.schema)
	if err != nil {
		return err
	}

	return nil
}

// TOOD: make this better
// https://stackoverflow.com/questions/63564543/decode-a-debeuzium-event-schema-into-a-meaningful-datastructure-in-golang
func (d *debeziumSchemaParser) ColumnsBefore() map[string]string {
	columns := make(map[string]string)

	for _, field := range d.Fields {
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
						columns[v5["name"].(string)] = v5["type"].(string)
					}
				}

			}
		}
	}

	return columns
}
