package debezium

import (
	"github.com/practo/tipoca-stream/redshiftsink/pkg/redshift"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/serializer"
	"testing"
)

func TestSchemaMysqlDataType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		jobSchema  string
		maskSchema map[string]serializer.MaskInfo
		cName      string
		cType      string
	}{
		{
			name:       "test1: int type test",
			jobSchema:  `{"type":"record","name":"Envelope","namespace":"inventory.inventory.customers","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"id","type":"int"},{"name":"first_name","type":"string"},{"name":"last_name","type":["null","string"],"default":null},{"name":"email","type":"string"}],"connect.name":"inventory.inventory.customers.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"inventory.inventory.customers.Envelope"}`,
			maskSchema: map[string]serializer.MaskInfo{},
		},
		{
			name:       "test2: conversion test",
			jobSchema:  `{"type":"record","name":"Envelope","namespace":"inventory.inventory.customers","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"id","type":"int"},{"name":"first_name","type":"string"},{"name":"last_name","type":["null","string"],"default":null},{"name":"email","type":"string"}],"connect.name":"inventory.inventory.customers.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"inventory.inventory.customers.Envelope"}`,
			maskSchema: map[string]serializer.MaskInfo{},
		},
		{
			name:      "test3: length key test",
			jobSchema: `{"type":"record","name":"Envelope","namespace":"dbserver.database.customers","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"id","type":"int"},{"name":"first_name","type":"string"},{"name":"last_name","type":["null","string"],"default":null},{"name":"email","type":"string"}],"connect.name":"inventory.inventory.customers.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"inventory.inventory.customers.Envelope"}`,
			maskSchema: map[string]serializer.MaskInfo{
				"email": serializer.MaskInfo{
					Masked:    true,
					LengthCol: true,
				},
				"email_length": serializer.MaskInfo{
					Masked:    false,
					LengthCol: false,
				},
			},
			cName: "email_length",
			cType: "integer",
		},
		{
			name:       "test4: source col length test",
			jobSchema:  `{"type":"record","name":"Envelope","namespace":"ts.inventory.customers","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"id","type":{"type":"int","connect.parameters":{"__debezium.source.column.type":"INT","__debezium.source.column.length":"11"}}},{"name":"first_name","type":["null",{"type":"string","connect.parameters":{"__debezium.source.column.type":"VARCHAR","__debezium.source.column.length":"1100"}}],"default":null},{"name":"last_name","type":["null",{"type":"string","connect.parameters":{"__debezium.source.column.type":"VARCHAR","__debezium.source.column.length":"255"}}],"default":null},{"name":"email","type":{"type":"string","connect.parameters":{"__debezium.source.column.type":"VARCHAR","__debezium.source.column.length":"255"}}},{"name":"dob","type":["null",{"type":"int","connect.version":1,"connect.parameters":{"__debezium.source.column.type":"DATE"},"connect.name":"io.debezium.time.Date"}],"default":null},{"name":"score","type":["null",{"type":"string","connect.parameters":{"__debezium.source.column.type":"DECIMAL","__debezium.source.column.length":"10","__debezium.source.column.scale":"4"}}],"default":null}],"connect.name":"ts.inventory.customers.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"ts.inventory.customers.Envelope"}`,
			maskSchema: map[string]serializer.MaskInfo{},
			cName:      "first_name",
			cType:      "character varying(4400)",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			c := &schemaTransformer{srclient: nil}

			resp, err := c.transformSchemaValue(
				tc.jobSchema, []string{"id"}, tc.maskSchema)
			table := resp.(redshift.Table)
			if err != nil {
				t.Error(err)
			}

			if tc.cName != "" && tc.cType != "" {
				for _, column := range table.Columns {
					if column.Name != tc.cName {
						continue
					}

					if column.Type != tc.cType {
						t.Errorf(
							"expected column type: %v, got:%v\n",
							column.Type,
							tc.cType,
						)
					}
					return
				}

				t.Errorf("column missing: %v\n", tc.cName)
			}
		})
	}
}
