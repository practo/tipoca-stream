package serializer

import (
	"encoding/binary"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/practo/tipoca-stream/pkg/schemaregistry"
)

const (
	OperationCreate = "CREATE"
	OperationUpdate = "UPDATE"
	OperationDelete = "DELETE"
)

type MaskInfo struct {
	Masked bool

	SortCol bool
	DistCol bool

	LengthCol              bool
	MobileCol              bool
	MappingPIICol          bool
	ConditionalNonPIICol   bool
	DependentNonPIICol     bool
	RegexPatternBooleanCol bool
}

type ExtraMaskInfo struct {
	Masked     bool
	ColumnType string
	DefaultVal string
}

type Serializer interface {
	Deserialize(message *sarama.ConsumerMessage) (*Message, error)
}

func NewSerializer(schemaRegistryURL string) Serializer {
	return &avroSerializer{
		registry: schemaregistry.NewRegistry(schemaRegistryURL),
	}
}

type avroSerializer struct {
	registry schemaregistry.SchemaRegistry
}

func (c *avroSerializer) Deserialize(
	message *sarama.ConsumerMessage) (*Message, error) {

	schemaId := binary.BigEndian.Uint32(message.Value[1:5])
	schema, err := schemaregistry.GetSchemaWithRetry(
		c.registry,
		int(schemaId),
		10,
	)
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
		SchemaId:        int(schemaId),
		Topic:           message.Topic,
		Partition:       message.Partition,
		Offset:          message.Offset,
		Key:             string(message.Key),
		Value:           native,
		Bytes:           int64(len(message.Value)),
		MaskSchema:      make(map[string]MaskInfo),
		ExtraMaskSchema: make(map[string]ExtraMaskInfo),
	}, nil
}
