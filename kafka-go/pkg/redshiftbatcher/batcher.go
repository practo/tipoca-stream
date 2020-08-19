package redshiftbatcher

import (
	"time"

	"github.com/practo/gobatch"
	"github.com/spf13/viper"
)

const (
	maxBatchId = 99
)

type batcher struct {
	topic     string
	config    *BatcherConfig
	mbatch    *gobatch.Batch
	processor *batchProcessor
}

type BatcherConfig struct {
	// Maximum size of a batch, on exceeding this batch is pushed
	// regarless of the wait time.
	MaxSize int `yaml:maxSize,omitempty`

	// MaxWaitSeconds after which the bash would be pushed regardless of its size.
	MaxWaitSeconds int `yaml:maxWaitSeconds,omitempty`
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

func newBatcher(topic string) *batcher {
	c := &BatcherConfig{
		MaxSize:        viper.GetInt("batcher.maxSize"),
		MaxWaitSeconds: viper.GetInt("batcher.maxWaitSeconds"),
	}

	return &batcher{
		topic:     topic,
		config:    c,
		processor: nil,
		mbatch:    nil,
	}
}
