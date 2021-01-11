package kafka

import (
	"encoding/binary"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/linkedin/goavro/v2"
	"github.com/practo/klog/v2"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/schemaregistry"
	"strings"
	"time"
)

type AvroProducer struct {
	producer sarama.SyncProducer
	registry schemaregistry.SchemaRegistry
}

func NewAvroProducer(
	brokers []string,
	kafkaVersion string,
	schemaRegistryURL string,
	configTLS TLSConfig,
) (*AvroProducer, error) {
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
	if configTLS.Enable {
		config.Net.TLS.Enable = true
		tlsConfig, err := NewTLSConfig(configTLS)
		if err != nil {
			return nil, fmt.Errorf("TLS init failed, err: %v", err)
		}
		config.Net.TLS.Config = tlsConfig
	}

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}

	return &AvroProducer{
		producer: producer,
		registry: schemaregistry.NewRegistry(schemaRegistryURL),
	}, nil
}

// CreateSchema creates schema if it does not exist
func (c *AvroProducer) CreateSchema(
	topic string, scheme string) (int, bool, error) {

	created := false

	schemeStr := strings.ReplaceAll(scheme, "\n", "")
	schemeStr = strings.ReplaceAll(schemeStr, " ", "")

	schema, err := schemaregistry.GetLatestSchemaWithRetry(
		c.registry, topic, false, 10,
	)
	if schema == nil || schema.Schema() != schemeStr {
		klog.V(2).Infof("Creating schema version. topic: %s", topic)
		schema, err = c.registry.CreateSchema(
			topic, scheme, schemaregistry.Avro, false,
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

func (a *AvroEncoder) Encode() ([]byte, error) {
	var binaryMsg []byte

	// Serialization format version number; currently always 0.
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
