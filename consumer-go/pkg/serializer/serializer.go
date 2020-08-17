package serializer

import (
    "fmt"
    "encoding/binary"
    "github.com/Shopify/sarama"
    "github.com/riferrei/srclient"
)

type Serializer interface {
    Deserialize(message *sarama.ConsumerMessage) (Message, error)
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
    message *sarama.ConsumerMessage) (Message, error) {

    schemaId := binary.BigEndian.Uint32(message.Value[1:5])
    schema, err := c.srclient.GetSchema(int(schemaId))
	if err != nil {
		return Message{}, err
	}
    if schema == nil {
        return Message{}, fmt.Errorf(
            "Got nil schema for message:%+v\n", message)
    }

    // Convert binary Avro data back to native Go form
	native, _, err := schema.Codec().NativeFromBinary(message.Value[5:])
	if err != nil {
		return Message{}, err
	}

	// // Convert native Go form to textual Avro data
	// textual, err := schema.Codec().TextualFromNative(nil, native)
	// if err != nil {
	// 	return Message{}, err
	// }

    return Message{
        SchemaId:   int(schemaId),
        Topic:      message.Topic,
        Partition:  message.Partition,
        Offset:     message.Offset,
        Key:        string(message.Key),
        // Value:      string(textual),
        Value:      native,
    }, nil
}
