package serializer

import (
    "fmt"
    "encoding/binary"
    "github.com/Shopify/sarama"
    "github.com/linkedin/goavro/v2"
    "github.com/riferrei/srclient"
)

type Message struct {
	SchemaId  int
	Topic     string
	Partition int32
	Offset    int64
	Key       string
	Value     string
}

type Serializer interface {
    Deserialize(message *sarama.ConsumerMessage) (Message, error)
}

func NewSerializer(schemaRegistryURL string) *Serializer {
    return &avroSerializer{
        srclient: srclient.CreateSchemaRegistryClient(schemaRegistryURL),
    }
}

type avroSerializer struct {
    srclient *srclient.SchemaRegistryClient
}

func (c *avroSerializer) Deserialize(
    message *sarama.ConsumerMessage) (Message, error) {
    fmt.Printf("saramaMessage=%+v\n", message)

    schemaId := binary.BigEndian.Uint32(message.Value[1:5])
    codec, err := c.GetSchema(int(schemaId))
	if err != nil {
		return Message{}, err
	}
    fmt.Printf("schemaID=%+v\n", schemaID)

    // Convert binary Avro data back to native Go form
	native, _, err := codec.NativeFromBinary(message.Value[5:])
    fmt.Printf("native=%+v\n", native)
	if err != nil {
		return Message{}, err
	}

	// Convert native Go form to textual Avro data
	textual, err := codec.TextualFromNative(nil, native)
	if err != nil {
		return Message{}, err
	}
    fmt.Printf("textual=%+v\n", textual)

	msg := Message{
        int(schemaId),
        m.Topic,
        m.Partition,
        m.Offset,
        string(m.Key),
        string(textual),
    }

	return msg, nil
}
