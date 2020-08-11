package consumer

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/practo/klog/v2"
)

type Client interface {
	Topics() ([]string, error)
	Consume(ctx context.Context, topics []string, ready chan bool) error
	Close() error
}

type KafkaConfig struct {
	Brokers       string `yaml: brokers`
	Group         string `yaml: group`
	Version       string `yaml: version`
	TopicPrefixes string `yaml: topicPrefixes`
	KafkaClient   string `yaml: kafkaClient`
}

type SaramaConfig struct {
	Assignor string `yaml: assignor`
	Oldest   bool   `yaml: oldest`
	Log      bool   `yaml: log`
}

func NewClient(k KafkaConfig, s SaramaConfig) (Client, error) {
	switch k.KafkaClient {
	case "sarama":
		return NewSaramaClient(k, s)
	default:
		return nil, fmt.Errorf("kafkaClient not supported: %v\n", k.KafkaClient)
	}
}

func NewSaramaClient(k KafkaConfig, s SaramaConfig) (Client, error) {
	if s.Log {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	version, err := sarama.ParseKafkaVersion(k.Version)
	if err != nil {
		return nil, fmt.Errorf("Error parsing Kafka version: %v\n", err)
	}

	c := sarama.NewConfig()
	c.Version = version

	switch s.Assignor {
	case "sticky":
		c.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
	case "roundrobin":
		c.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	case "range":
		c.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	default:
		return nil, fmt.Errorf(
			"Unknown group partition saramaAssignor: %s", s.Assignor)
	}

	if s.Oldest {
		c.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	// disable auto commits of offsets
	// https://github.com/Shopify/sarama/issues/1570#issuecomment-574908417
	c.Consumer.Offsets.AutoCommit.Enable = false

	// TODO: find the correct values and make it confiurable
	// c.Consumer.Fetch.Min = 3
	// c.Consumer.Fetch.Max = 10

	brokers := strings.Split(k.Brokers, ",")
	consumerGroup, err := sarama.NewConsumerGroup(brokers, k.Group, c)
	if err != nil {
		return nil, fmt.Errorf("Error creating consumer group: %v\n", err)
	}

	cluster, err := sarama.NewConsumer(brokers, c)
	if err != nil {
		return nil, fmt.Errorf("Error creating consumer: %v\n", err)
	}

	return &saramaClient{
		cluster:       cluster,
		consumerGroup: consumerGroup,
		consumer:      NewConsumer(),
	}, nil
}

type saramaClient struct {
	// cluster is required to get Kafka cluster related info like Topics
	cluster sarama.Consumer

	// consumerGroup uses consumer to consume records in kaafka topics
	consumerGroup sarama.ConsumerGroup

	// consumer is the implementation that is called by the sarama
	// to perform consumption
	consumer consumer
}

func (c *saramaClient) Topics() ([]string, error) {
	return c.cluster.Topics()
}

func (c *saramaClient) Consume(
	ctx context.Context, topics []string, ready chan bool) error {

	// create batchers
	b := new(batchers)
	for _, topic := range topics {
		b.Store(topic, newBatcher(topic, 10, 4))
	}
	c.consumer.batchers = b
	klog.V(5).Infof("Created batchers: %+v\n", b)

	c.consumer.ready = ready

	return c.consumerGroup.Consume(ctx, topics, c.consumer)
}

func (c *saramaClient) Close() error {
	if err := c.consumerGroup.Close(); err != nil {
		return err
	}

	if err := c.cluster.Close(); err != nil {
		return err
	}

	return nil
}
