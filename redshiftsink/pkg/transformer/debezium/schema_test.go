package debezium

import (
	"github.com/practo/tipoca-stream/redshiftsink/pkg/redshift"
	"testing"
)

func TestSchemaMysqlDataType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name		string
		jobSchema 	string
		mask        bool
		cName       string
		cType       string
	}{
		{
			name: "test1: int type test",
			jobSchema: `{"type":"record","name":"Envelope","namespace":"inventory.inventory.customers","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"id","type":"int"},{"name":"first_name","type":"string"},{"name":"last_name","type":["null","string"],"default":null},{"name":"email","type":"string"}],"connect.name":"inventory.inventory.customers.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"inventory.inventory.customers.Envelope"}`,
			mask: false,
		},
		{
			name: "test2: conversion test",
			jobSchema: `{"type":"record","name":"Envelope","namespace":"inventory.inventory.customers","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"id","type":"int"},{"name":"first_name","type":"string"},{"name":"last_name","type":["null","string"],"default":null},{"name":"email","type":"string"}],"connect.name":"inventory.inventory.customers.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"inventory.inventory.customers.Envelope"}`,
			mask: false,
		},
		{
			name: "test3: length key test",
			jobSchema: `{"type":"record","name":"Envelope","namespace":"dbserver.database.customers","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"id","type":"int"},{"name":"first_name","type":"string"},{"name":"last_name","type":["null","string"],"default":null},{"name":"email","type":"string"}],"connect.name":"inventory.inventory.customers.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"inventory.inventory.customers.Envelope"}`,
			mask: true,
			cName: "email_test",
			cType: "integer",
		},
	}

	for _, tc := range tests {
		tc := tc
		c := &schemaTransformer{srclient: nil}
		resp, err := c.transformSchemaValue(1, tc.jobSchema, "id", tc.mask)
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
	}
}
