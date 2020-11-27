package redshiftbatcher

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

type batcher struct {
	topic        string
	lastSchemaId *int
	config       *BatcherConfig
	mbatch       *gobatch.Batch

	// serializer is used to Deserialize the message stored in Kafka
	serializer serializer.Serializer

	// processeor processes the desrialized message
	processor *batchProcessor
}

func newBatcher(topic string) *batcher {
	c := &BatcherConfig{
		MaxSize:        viper.GetInt("batcher.maxSize"),
		MaxWaitSeconds: viper.GetInt("batcher.maxWaitSeconds"),
	}

	return &batcher{
		topic:        topic,
		lastSchemaId: nil,
		config:       c,
		mbatch:       nil,

		serializer: serializer.NewSerializer(
			viper.GetString("schemaRegistryURL")),

		processor: nil,
	}
}

func (b *batcher) Insert(saramaMessage *sarama.ConsumerMessage) {
	if b.mbatch == nil {
		b.mbatch = newMBatch(
			b.config.MaxSize,
			b.config.MaxWaitSeconds,
			b.processor.process,
			1,
		)
	}

	if len(saramaMessage.Value) == 0 {
		klog.Infof(
			"Skipping message, received a tombstone event, message: %+v\n",
			saramaMessage)
		return
	}

	// TODO: performance optimization
	// to batch byschema id needed to extract schema id
	message, err := b.serializer.Deserialize(saramaMessage)
	if err != nil {
		klog.Fatalf("Error deserializing binary, err: %s\n", err)
	}

	// batch by schema id
	if b.lastSchemaId == nil {
		b.mbatch.Insert(message)
		b.lastSchemaId = new(int)
	} else if *b.lastSchemaId != message.SchemaId {
		klog.V(3).Infof("topic:%s: Got new schema (new batch): %d => %d\n",
			b.topic, *b.lastSchemaId, message.SchemaId)
		b.mbatch.FlushInsert(message)
	} else {
		b.mbatch.Insert(message)
	}

	*b.lastSchemaId = message.SchemaId
	klog.V(5).Infof("topic:%s, schemaId: %d\n", b.topic, *b.lastSchemaId)
}

type BatcherConfig struct {
	// Mask should be turned on or off
	Mask string `yaml:"mask,omitempty"`
	// MaskSalt specifies the salt to be used for masking
	MaskSalt string `yaml:"maskSalt,omitempty"`
	// MaskFile can be the either of the two:
	// 1. Absolute path of the mask configuration file. This file needs to be
	// be mounted as config map when the batcher starts.
	// 2. Git File or Folder. Examples:
	// https://github.com/practo/tipoca-stream/pkg/database.yaml
	// then this file or repo be cloned and kept at / when batcher starts.
	MaskFile string `yaml:"maskFile,omitempty"`
	// MaskFileVersion is the git version of the MaskFile
	// It is useful when the MaskFile is specified is a Git File.
	// otherwise when MaskFile is an abosolute mounted file, it is not used.
	MaskFileVersion string `yaml:"maskFileVersion,omitempty"`

	// MaxSize is the maximum size of a batch, on exceeding this batch is pushed
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
