package redshiftloader

import (
	"time"

	"github.com/Shopify/sarama"
	"github.com/practo/gobatch"
	"github.com/practo/klog/v2"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/serializer"
	"github.com/spf13/viper"
)

const (
	maxBatchId = 99
)

type loader struct {
	topic        string
	lastSchemaId *int
	config       *LoaderConfig
	mbatch       *gobatch.Batch

	// serializer is used to Deserialize the message stored in Kafka
	serializer serializer.Serializer

	// processeor processes the desrialized message
	processor *loadProcessor
}

func newLoader(topic string) *loader {
	c := &LoaderConfig{
		MaxSize:        viper.GetInt("loader.maxSize"),
		MaxWaitSeconds: viper.GetInt("loader.maxWaitSeconds"),
	}

	return &loader{
		topic:        topic,
		lastSchemaId: nil,
		config:       c,
		mbatch:       nil,

		serializer: serializer.NewSerializer(
			viper.GetString("schemaRegistryURL")),

		processor: nil,
	}
}

func (b *loader) Insert(saramaMessage *sarama.ConsumerMessage) {
	if b.mbatch == nil {
		b.mbatch = newMBatch(
			b.config.MaxSize,
			b.config.MaxWaitSeconds,
			b.processor.process,
			1,
		)
	}

	// TOOD: there could give best performance since desirailizing is being done
	// at the time of insert. Could not think of better way
	// as needed to extract schema id from the message
	// to batch by schema id. Revisit this later.
	message, err := b.serializer.Deserialize(saramaMessage)
	if err != nil {
		klog.Fatalf("Error deserializing binary, err: %s\n", err)
	}

	if message == nil || message.Value == nil {
		klog.Fatalf("Got message as nil, message: %+v\n", message)
	}

	klog.V(99).Infof("message.Value=%v\n", message.Value)

	job := StringMapToJob(message.Value.(map[string]interface{}))
	upstreamJobSchemaId := job.SchemaId

	//  batch by schema id of upstream topic
	if b.lastSchemaId == nil {
		b.mbatch.Insert(message)
		b.lastSchemaId = new(int)
	} else if *b.lastSchemaId != upstreamJobSchemaId {
		klog.V(3).Infof("topic:%s: Got new schema (new batch): %d => %d\n",
			b.topic, *b.lastSchemaId, upstreamJobSchemaId)
		b.mbatch.FlushInsert(message)
	} else {
		b.mbatch.Insert(message)
	}

	*b.lastSchemaId = upstreamJobSchemaId
	klog.V(6).Infof("topic:%s, schemaId: %d\n", b.topic, *b.lastSchemaId)
}

type LoaderConfig struct {
	// Mask should be turned on or off
	Mask string `yaml:"mask"`

	// Mask config dir is the directory where the database.yaml is
	// is to be searched to get the masked columns info, this is required
	// to add a chatacter masked type as the datatype for such columns
	MaskConfigDir string `yaml:"maskConfigDir"`

	// Maximum size of a batch, on exceeding this batch is pushed
	// regarless of the wait time.
	MaxSize int `yaml:"maxSize,omitempty"`

	// MaxWaitSeconds after which the bash would be pushed regardless of its size.
	MaxWaitSeconds int `yaml:"maxWaitSeconds,omitempty"`
}

func newMBatch(maxSize int,
	maxWaitSeconds int, process gobatch.BatchFn,
	workers int) *gobatch.Batch {

	return gobatch.NewMemoryBatch(
		maxSize,
		time.Second*time.Duration(maxWaitSeconds),
		process,
		workers,
	)
}
