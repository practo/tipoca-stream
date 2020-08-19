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

type ConsumerGroup interface {
	Topics() ([]string, error)
	LastOffset(topic string, partition int32) (int64, error)
	Consume(ctx context.Context, topics []string) error
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

func NewConsumerGroup(k KafkaConfig, s SaramaConfig,
	consumer sarama.ConsumerGroupHandler) (ConsumerGroup, error) {

	switch k.KafkaClient {
	case "sarama":
		return NewSaramaConsumerGroup(k, s, consumer)
	default:
		return nil, fmt.Errorf("kafkaClient not supported: %v\n", k.KafkaClient)
	}
}

func NewSaramaConsumerGroup(k KafkaConfig, s SaramaConfig,
	consumer sarama.ConsumerGroupHandler) (ConsumerGroup, error) {

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

	client, err := sarama.NewClient(brokers, c)
	if err != nil {
		return nil, fmt.Errorf("Error creating client: %v\n", err)
	}

	return &saramaConsumerGroup{
		client:        client,
		consumerGroup: consumerGroup,
		consumer:      consumer,
	}, nil
}

type saramaConsumerGroup struct {
	// client is required to get Kafka cluster related info like Topics
	client sarama.Client

	// consumerGroup uses consumer to consume records in kaafka topics
	consumerGroup sarama.ConsumerGroup

	// consumer is the implementation that is called by the sarama
	// to perform consumption
	consumer sarama.ConsumerGroupHandler
}

func (c *saramaConsumerGroup) Topics() ([]string, error) {
	return c.client.Topics()
}

func (c *saramaConsumerGroup) LastOffset(
	topic string, partition int32) (int64, error) {
	return c.client.GetOffset(topic, partition, sarama.OffsetNewest)
}

func (c *saramaConsumerGroup) Close() error {
	klog.V(4).Infof("Closing consumerGroup.")
	if err := c.consumerGroup.Close(); err != nil {
		return err
	}

	klog.V(4).Info("Closing client.")
	if err := c.client.Close(); err != nil {
		return err
	}

	klog.Info("Closed open connections.")
	return nil
}

func (c *saramaConsumerGroup) Consume(
	ctx context.Context, topics []string) error {

	return c.consumerGroup.Consume(ctx, topics, c.consumer)
}
