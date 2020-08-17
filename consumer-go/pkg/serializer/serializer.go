package serializer

import (
    "fmt"
    "encoding/binary"
    "github.com/Shopify/sarama"
    "github.com/riferrei/srclient"
    // "github.com/linkedin/goavro/v2"
)

// type Message struct {
// 	SchemaId  int
// 	Topic     string
// 	Partition int32
// 	Offset    int64
// 	Key       string
// 	Value     string
// }

type Serializer interface {
    Deserialize(message *sarama.ConsumerMessage) (string, error)
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
    message *sarama.ConsumerMessage) (string, error) {

    schemaId := binary.BigEndian.Uint32(message.Value[1:5])
    schema, err := c.srclient.GetSchema(int(schemaId))
	if err != nil {
		return "Message{}", err
	}

    // Convert binary Avro data back to native Go form
	native, _, err := schema.Codec().NativeFromBinary(message.Value[5:])
	if err != nil {
		return "", err
	}

	// Convert native Go form to textual Avro data
	textual, err := schema.Codec().TextualFromNative(nil, native)
	if err != nil {
		return "", err
	}

	return string(textual), nil
}
