package debezium

import (
	"fmt"
	"github.com/practo/tipoca-stream/kafka-go/pkg/redshift"
	"testing"
)

func TestSchemaMysqlDataTypeInt(t *testing.T) {
	jobSchema := `{"type":"record","name":"Envelope","namespace":"inventory.inventory.customers","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"id","type":"int"},{"name":"first_name","type":"string"},{"name":"last_name","type":["null","string"],"default":null},{"name":"email","type":"string"}],"connect.name":"inventory.inventory.customers.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"inventory.inventory.customers.Envelope"}`
	c := &schemaTransformer{srclient: nil}
	resp, err := c.transformSchemaValue(1, jobSchema, "id", false)
	table := resp.(redshift.Table)
	if err != nil {
		t.Error(err)
	}

	fmt.Printf("table:%+v\n", table)
}

func TestSchemaMysqlDataTypeConversions(t *testing.T) {
	jobSchema := `{"type":"record","name":"Envelope","namespace":"inventory.inventory.customers","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"id","type":{"type":"int","connect.parameters":{"__debezium.source.column.type":"INT","__debezium.source.column.length":"11"}}},{"name":"first_name","type":{"type":"string","connect.parameters":{"__debezium.source.column.type":"VARCHAR","__debezium.source.column.length":"255"}}},{"name":"last_name","type":["null",{"type":"string","connect.parameters":{"__debezium.source.column.type":"VARCHAR","__debezium.source.column.length":"255"}}],"default":null},{"name":"email","type":{"type":"string","connect.parameters":{"__debezium.source.column.type":"VARCHAR","__debezium.source.column.length":"255"}}}],"connect.name":"inventory.inventory.customers.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"inventory.inventory.customers.Envelope"}`

	c := &schemaTransformer{srclient: nil}
	_, err := c.transformSchemaValue(1, jobSchema, "id", false)
	if err != nil {
		t.Error(err)
	}
}
