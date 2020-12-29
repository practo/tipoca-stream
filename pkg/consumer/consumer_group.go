package consumer

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
)

type ConsumerGroupInterface interface {
	Topics() ([]string, error)

	// RefreshMetadata takes a list of topics and
	// queries the cluster to refresh the
	// available metadata for those topics.
	// If no topics are provided, it will refresh
	// metadata for all topics.
	RefreshMetadata(topics ...string) error

	LastOffset(topic string, partition int32) (int64, error)
	Consume(ctx context.Context, topics []string) error
	Close() error
}

type ConsumerGroupConfig struct {
	GroupID           string       `yaml:"groupID"`
	TopicRegexes      string       `yaml:"topicRegexes"`
	// TODO: LoaderTopicPrefix is "" for loader consumer groups
	// it should be an optional field, a pointer.
	LoaderTopicPrefix string       `yaml:"loaderTopicPrefix"` // default is there
	Kafka             KafkaConfig  `yaml:"kafka"`
	Sarama            SaramaConfig `yaml:"sarama"`
}

type KafkaConfig struct {
	Brokers     string `yaml:"brokers"`
	Version     string `yaml:"version"`     // default is there
	KafkaClient string `yaml:"kafkaClient"` // default is there
}

type SaramaConfig struct {
	Assignor   string `yaml:"assignor"` // default is there
	Oldest     bool   `yaml:"oldest"`
	Log        bool   `yaml:"log"`
	AutoCommit bool   `yaml:"autoCommit"`
}

func NewConsumerGroup(
	config ConsumerGroupConfig,
	consumerGroupHandler sarama.ConsumerGroupHandler,
) (
	ConsumerGroupInterface,
	error,
) {
	// set defaults
	if config.Kafka.Version == "" {
		config.Kafka.Version = "2.5.0"
	}
	if config.Kafka.KafkaClient == "" {
		config.Kafka.KafkaClient = "sarama"
	}
	if config.LoaderTopicPrefix == "" {
		config.LoaderTopicPrefix = "loader-"
	}
	if config.Sarama.Assignor == "" {
		config.Sarama.Assignor = "range"
	}

	switch config.Kafka.KafkaClient {
	case "sarama":
		return NewSaramaConsumerGroup(config, consumerGroupHandler)
	default:
		return nil, fmt.Errorf(
			"client not supported: %v\n", config.Kafka.KafkaClient)
	}
}
