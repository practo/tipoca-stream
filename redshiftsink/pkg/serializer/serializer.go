package serializer

import (
	"encoding/binary"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/riferrei/srclient"
)

const (
	OperationCreate = "CREATE"
	OperationUpdate = "UPDATE"
	OperationDelete = "DELETE"
)

type Message struct {
	SchemaId  int
	Topic     string
	Partition int32
	Offset    int64
	Key       string
	Value     interface{}

	Operation  string
	MaskSchema map[string]MaskInfo
}

type MaskInfo struct {
	Masked        bool
	SortCol       bool
	DistCol       bool
	LengthCol     bool
	MobileCol     bool
	MappingPIICol bool
}

type Serializer interface {
	Deserialize(message *sarama.ConsumerMessage) (*Message, error)
}

func NewSerializer(schemaRegistryURL string) Serializer {
	return &avroSerializer{
		srclient: srclient.CreateSchemaRegistryClient(schemaRegistryURL),
	}
}

type avroSerializer struct {
	srclient *srclient.SchemaRegistryClient
}

func (c *avroSerializer) Deserialize(
	message *sarama.ConsumerMessage) (*Message, error) {

	schemaId := binary.BigEndian.Uint32(message.Value[1:5])
	schema, err := GetSchemaWithRetry(c.srclient, int(schemaId), 10)
	if err != nil {
		return nil, err
	}
	if schema == nil {
		return nil, fmt.Errorf("Got nil schema for message:%+v\n", message)
	}

	// Convert binary Avro data back to native Go form
	native, _, err := schema.Codec().NativeFromBinary(message.Value[5:])
	if err != nil {
		return nil, err
	}

	return &Message{
		SchemaId:   int(schemaId),
		Topic:      message.Topic,
		Partition:  message.Partition,
		Offset:     message.Offset,
		Key:        string(message.Key),
		Value:      native,
		MaskSchema: make(map[string]MaskInfo),
	}, nil
}
