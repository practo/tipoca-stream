package transformer

import (
	"testing"
)

func TestSchemaTransformListWithDefaults(t *testing.T) {
	jobSchema := `{"type":"record","name":"Envelope","namespace":"datapipe.inventory.customers","fields":[{"name":"before","type":[{"type":"record","name":"Value","fields":[{"name":"id","type":"int"},{"name":"first_name","type":[{"type":"string","connect.default":"alok"},"null"],"default":"alok"},{"name":"last_name","type":"string"},{"name":"email","type":"string"}],"connect.name":"datapipe.inventory.customers.Value"}],"default":null},{"name":"after","type":["Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"table","type":["string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["long"],"default":null},{"name":"query","type":["string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["long"],"default":null},{"name":"transaction","type":[{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"datapipe.inventory.customers.Envelope"}`

	c := &redshiftSchemaTransformer{srclient: nil}
	_, err := c.transformSchema(jobSchema)
	if err != nil {
		t.Error(err)
	}
}

func TestSchemaTransformMapWithDefaults(t *testing.T) {
	jobSchema := `{"type":"record","name":"Envelope","namespace":"datapipe.inventory.customers","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"id","type":"int"},{"name":"first_name","type":[{"type":"string","connect.default":"alok"},"null"],"default":"alok"},{"name":"last_name","type":{"type":"string","connect.default":"singh"},"default":"singh"},{"name":"email","type":"string"}],"connect.name":"datapipe.inventory.customers.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"datapipe.inventory.customers.Envelope"}`

	c := &redshiftSchemaTransformer{srclient: nil}
	_, err := c.transformSchema(jobSchema)
	if err != nil {
		t.Error(err)
	}
}
