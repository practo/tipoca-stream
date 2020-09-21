package producer

import (
	"encoding/binary"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/linkedin/goavro/v2"
	"github.com/riferrei/srclient"
	"time"
)

type AvroProducer struct {
	producer sarama.SyncProducer
	srclient *srclient.SchemaRegistryClient
}

func NewAvroProducer(brokers []string,
	kafkaVersion string, schemaRegistryURL string) (*AvroProducer, error) {

	version, err := sarama.ParseKafkaVersion(kafkaVersion)
	if err != nil {
		return nil, fmt.Errorf("Error parsing Kafka version: %v\n", err)
	}

	config := sarama.NewConfig()
	config.Version = version
	config.Producer.Partitioner = sarama.NewHashPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Producer.Compression = sarama.CompressionNone
	config.Producer.MaxMessageBytes = 10000000
	config.Producer.Retry.Max = 10
	config.Producer.Retry.Backoff = 1000 * time.Millisecond

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}

	return &AvroProducer{
		producer: producer,
		srclient: srclient.CreateSchemaRegistryClient(schemaRegistryURL),
	}, nil
}

// CreateSchema creates schema if it does not exist
func (c *AvroProducer) CreateSchema(
	topic string, scheme string) (int, bool, error) {

	created := false

	schema, err := c.srclient.GetLatestSchema(topic, false)
	if schema == nil {
		schema, err = c.srclient.CreateSchema(
			topic, scheme, srclient.Avro, false,
		)
		if err != nil {
			return 0, false, err
		}
		created = true
	}

	return schema.ID(), created, nil
}

func (c *AvroProducer) Add(topic string, schema string,
	key []byte, value map[string]interface{}) error {

	schemaId, _, err := c.CreateSchema(topic, schema)
	if err != nil {
		return err
	}

	avroCodec, err := goavro.NewCodec(schema)
	if err != nil {
		return err
	}

	binaryValue, err := avroCodec.BinaryFromNative(nil, value)
	if err != nil {
		return err
	}

	binaryMsg := &AvroEncoder{
		SchemaID: schemaId,
		Content:  binaryValue,
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: binaryMsg,
	}
	_, _, err = c.producer.SendMessage(msg)
	return err
}

func (c *AvroProducer) Close() {
	c.producer.Close()
}

// TODO: move to serializer
// AvroEncoder encodes schemaId and Avro message.
type AvroEncoder struct {
	SchemaID int
	Content  []byte
}

// Confluent schema registry has special requirements for the
// Avro serialization rules, not only need to serialize the specific content,
// but also attach the Schema ID and Magic Byte.
// https://docs.confluent.io/current/schema-registry/serializer-formatter.html#wire-format
func (a *AvroEncoder) Encode() ([]byte, error) {
	var binaryMsg []byte

	// Confluent serialization format version number; currently always 0.
	binaryMsg = append(binaryMsg, byte(0))

	// 4-byte schema ID as returned by Schema Registry
	binarySchemaId := make([]byte, 4)
	binary.BigEndian.PutUint32(binarySchemaId, uint32(a.SchemaID))
	binaryMsg = append(binaryMsg, binarySchemaId...)

	// Avro serialized data in Avro's binary encoding
	binaryMsg = append(binaryMsg, a.Content...)
	return binaryMsg, nil
}

// Length of schemaId and Content.
func (a *AvroEncoder) Length() int {
	return 5 + len(a.Content)
}
