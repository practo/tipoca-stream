package debezium

import (
	"github.com/practo/tipoca-stream/redshiftsink/pkg/redshift"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/serializer"
	"reflect"
	"testing"
)

func TestExtraColumnSort(t *testing.T) {
	var ec, rec []redshift.ColInfo
	ec = append(
		ec,
		redshift.ColInfo{
			Name:       "email_length",
			Type:       "email",
			DefaultVal: "",
		},
		redshift.ColInfo{
			Name:       "has_covid",
			Type:       "string",
			DefaultVal: "",
		},
		redshift.ColInfo{
			Name:       "a1988born",
			Type:       "string",
			DefaultVal: "",
		},
		redshift.ColInfo{
			Name:       "b1986born",
			Type:       "string",
			DefaultVal: "",
		},
	)

	sortExtraColumns(ec)

	rec = append(
		rec,
		redshift.ColInfo{
			Name:       "a1988born",
			Type:       "string",
			DefaultVal: "",
		},
		redshift.ColInfo{
			Name:       "b1986born",
			Type:       "string",
			DefaultVal: "",
		},
		redshift.ColInfo{
			Name:       "email_length",
			Type:       "email",
			DefaultVal: "",
		},
		redshift.ColInfo{
			Name:       "has_covid",
			Type:       "string",
			DefaultVal: "",
		},
	)

	if !reflect.DeepEqual(rec, ec) {
		t.Errorf("rec!=ec, got=%+v\n, expected=%+v", rec, ec)
	}

}

func TestSchemaMysqlDataType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		jobSchema       string
		maskSchema      map[string]serializer.MaskInfo
		extraMaskSchema map[string]serializer.ExtraMaskInfo
		cName           string
		cType           string
	}{
		{
			name:            "test1: int type test",
			jobSchema:       `{"type":"record","name":"Envelope","namespace":"inventory.inventory.customers","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"id","type":"int"},{"name":"first_name","type":"string"},{"name":"last_name","type":["null","string"],"default":null},{"name":"email","type":"string"}],"connect.name":"inventory.inventory.customers.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"inventory.inventory.customers.Envelope"}`,
			maskSchema:      map[string]serializer.MaskInfo{},
			extraMaskSchema: map[string]serializer.ExtraMaskInfo{},
		},
		{
			name:            "test2: conversion test",
			jobSchema:       `{"type":"record","name":"Envelope","namespace":"inventory.inventory.customers","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"id","type":"int"},{"name":"first_name","type":"string"},{"name":"last_name","type":["null","string"],"default":null},{"name":"email","type":"string"}],"connect.name":"inventory.inventory.customers.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"inventory.inventory.customers.Envelope"}`,
			maskSchema:      map[string]serializer.MaskInfo{},
			extraMaskSchema: map[string]serializer.ExtraMaskInfo{},
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
			extraMaskSchema: map[string]serializer.ExtraMaskInfo{},
			cName:           "email_length",
			cType:           "integer",
		},
		{
			name:            "test4: source col length test",
			jobSchema:       `{"type":"record","name":"Envelope","namespace":"ts.inventory.customers","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"id","type":{"type":"int","connect.parameters":{"__debezium.source.column.type":"INT","__debezium.source.column.length":"11"}}},{"name":"first_name","type":["null",{"type":"string","connect.parameters":{"__debezium.source.column.type":"VARCHAR","__debezium.source.column.length":"1100"}}],"default":null},{"name":"last_name","type":["null",{"type":"string","connect.parameters":{"__debezium.source.column.type":"VARCHAR","__debezium.source.column.length":"255"}}],"default":null},{"name":"email","type":{"type":"string","connect.parameters":{"__debezium.source.column.type":"VARCHAR","__debezium.source.column.length":"255"}}},{"name":"dob","type":["null",{"type":"int","connect.version":1,"connect.parameters":{"__debezium.source.column.type":"DATE"},"connect.name":"io.debezium.time.Date"}],"default":null},{"name":"score","type":["null",{"type":"string","connect.parameters":{"__debezium.source.column.type":"DECIMAL","__debezium.source.column.length":"10","__debezium.source.column.scale":"4"}}],"default":null}],"connect.name":"ts.inventory.customers.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"ts.inventory.customers.Envelope"}`,
			maskSchema:      map[string]serializer.MaskInfo{},
			extraMaskSchema: map[string]serializer.ExtraMaskInfo{},
			cName:           "first_name",
			cType:           "character varying(4400)",
		},
		{
			name:            "test4: enum test",
			jobSchema:       `{"type":"record","name":"Envelope","namespace":"ts.inventory.subscription_heroes","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"id","type":{"type":"string","connect.parameters":{"__debezium.source.column.type":"VARCHAR","__debezium.source.column.length":"36"}}},{"name":"subscription_id","type":{"type":"string","connect.parameters":{"__debezium.source.column.type":"VARCHAR","__debezium.source.column.length":"36"}}},{"name":"uhid","type":{"type":"string","connect.parameters":{"__debezium.source.column.type":"VARCHAR","__debezium.source.column.length":"36"}}},{"name":"account_id","type":["null",{"type":"int","connect.parameters":{"__debezium.source.column.type":"INT","__debezium.source.column.length":"11"}}],"default":null},{"name":"name","type":["null",{"type":"string","connect.parameters":{"__debezium.source.column.type":"VARCHAR","__debezium.source.column.length":"120"}}],"default":null},{"name":"relation","type":{"type":"string","connect.version":1,"connect.parameters":{"allowed":"SELF,FATHER,MOTHER,SPOUSE,SON,DAUGHTER,OTHER","__debezium.source.column.type":"ENUM","__debezium.source.column.length":"1"},"connect.name":"io.debezium.data.Enum"}},{"name":"mobile","type":["null",{"type":"string","connect.parameters":{"__debezium.source.column.type":"VARCHAR","__debezium.source.column.length":"15"}}],"default":null},{"name":"email","type":["null",{"type":"string","connect.parameters":{"__debezium.source.column.type":"VARCHAR","__debezium.source.column.length":"60"}}],"default":null},{"name":"adult","type":{"type":"int","connect.parameters":{"__debezium.source.column.type":"TINYINT","__debezium.source.column.length":"1"},"connect.type":"int16"}},{"name":"age","type":["null",{"type":"int","connect.parameters":{"__debezium.source.column.type":"INT","__debezium.source.column.length":"11"}}],"default":null},{"name":"gender","type":["null",{"type":"string","connect.version":1,"connect.parameters":{"allowed":"MALE,FEMALE,OTHER","__debezium.source.column.type":"ENUM","__debezium.source.column.length":"1"},"connect.name":"io.debezium.data.Enum"}],"default":null},{"name":"primary","type":["null",{"type":"int","connect.parameters":{"__debezium.source.column.type":"TINYINT","__debezium.source.column.length":"1"},"connect.type":"int16"}],"default":null},{"name":"soft_deleted","type":["null",{"type":"int","connect.parameters":{"__debezium.source.column.type":"TINYINT","__debezium.source.column.length":"1"},"connect.type":"int16"}],"default":null},{"name":"created_at","type":{"type":"long","connect.version":1,"connect.parameters":{"__debezium.source.column.type":"DATETIME"},"connect.name":"io.debezium.time.Timestamp"}},{"name":"modified_at","type":{"type":"long","connect.version":1,"connect.parameters":{"__debezium.source.column.type":"DATETIME"},"connect.name":"io.debezium.time.Timestamp"}},{"name":"created_by","type":["null",{"type":"string","connect.parameters":{"__debezium.source.column.type":"VARCHAR","__debezium.source.column.length":"36"}}],"default":null},{"name":"modified_by","type":["null",{"type":"string","connect.parameters":{"__debezium.source.column.type":"VARCHAR","__debezium.source.column.length":"36"}}],"default":null}],"connect.name":"ts.inventory.subscription_heroes.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"ts.inventory.subscription_heroes.Envelope"}`,
			maskSchema:      map[string]serializer.MaskInfo{},
			extraMaskSchema: map[string]serializer.ExtraMaskInfo{},
			cName:           "gender",
			cType:           "character varying(65535)",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			c := &schemaTransformer{registry: nil}

			resp, err := c.transformSchemaValue(
				tc.jobSchema, []string{"id"}, tc.maskSchema, tc.extraMaskSchema)
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
