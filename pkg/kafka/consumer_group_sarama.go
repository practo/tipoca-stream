package kafka

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/practo/klog/v2"
	"log"
	"os"
	"strings"
)

type saramaConsumerGroup struct {
	// client is required to get Kafka cluster related info like Topics
	client sarama.Client

	// consumerGroup uses consumer to consume records in kafka topics
	consumerGroup sarama.ConsumerGroup

	// consumerGroupID is saramas consumer group ID
	consumerGroupID string

	// consumerGroupHandler is the implementation that is called by the sarama
	// to perform consumption
	consumerGroupHandler sarama.ConsumerGroupHandler
}

func NewSaramaConsumerGroup(
	config ConsumerGroupConfig,
	consumerGroupHandler sarama.ConsumerGroupHandler,
) (
	ConsumerGroupInterface,
	error,
) {
	if config.Sarama.Log {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	version, err := sarama.ParseKafkaVersion(config.Kafka.Version)
	if err != nil {
		return nil, fmt.Errorf("Error parsing Kafka version: %v\n", err)
	}

	c := sarama.NewConfig()
	c.Version = version

	switch config.Sarama.Assignor {
	case "sticky":
		c.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
	case "roundrobin":
		c.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	case "range":
		c.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	default:
		return nil, fmt.Errorf(
			"Unknown group-partition assignor: %s", config.Sarama.Assignor)
	}

	if config.Kafka.TLSConfig.Enable {
		c.Net.TLS.Enable = true
		tlsConfig, err := NewTLSConfig(config.Kafka.TLSConfig)
		if err != nil {
			return nil, fmt.Errorf("TLS init failed, err: %v", err)
		}
		c.Net.TLS.Config = tlsConfig
	}

	if config.Sarama.Oldest {
		c.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	// disable auto commits of offsets
	// https://github.com/Shopify/sarama/issues/1570#issuecomment-574908417
	c.Consumer.Offsets.AutoCommit.Enable = config.Sarama.AutoCommit

	// TODO: find the correct values and make it confiurable
	// c.Consumer.Fetch.Min = 3
	// c.Consumer.Fetch.Max = 10
	brokers := strings.Split(config.Kafka.Brokers, ",")

	klog.V(2).Infof("ConsumerGroup config: %v", c)
	consumerGroup, err := sarama.NewConsumerGroup(
		brokers, config.GroupID, c)
	if err != nil {
		return nil, fmt.Errorf("Error creating consumer group: %v\n", err)
	}

	client, err := sarama.NewClient(brokers, c)
	if err != nil {
		return nil, fmt.Errorf("Error creating client: %v\n", err)
	}

	return &saramaConsumerGroup{
		client:               client,
		consumerGroup:        consumerGroup,
		consumerGroupID:      config.GroupID,
		consumerGroupHandler: consumerGroupHandler,
	}, nil
}

func (c *saramaConsumerGroup) Topics() ([]string, error) {
	return c.client.Topics()
}

func (c *saramaConsumerGroup) RefreshMetadata(topics ...string) error {
	return c.client.RefreshMetadata(topics...)
}

func (c *saramaConsumerGroup) LastOffset(
	topic string, partition int32) (int64, error) {
	return c.client.GetOffset(topic, partition, sarama.OffsetNewest)
}

func (c *saramaConsumerGroup) Close() error {
	klog.V(4).Infof("Closing consumerGroup: %v", c.consumerGroupID)
	if err := c.consumerGroup.Close(); err != nil {
		return err
	}

	klog.V(4).Infof("Closing client for consumerGroup: %v", c.consumerGroupID)
	if err := c.client.Close(); err != nil {
		return err
	}

	klog.V(4).Infof(
		"Closed open connections for consumerGroup: %v",
		c.consumerGroupID,
	)
	return nil
}

func (c *saramaConsumerGroup) Consume(
	ctx context.Context, topics []string) error {

	return c.consumerGroup.Consume(ctx, topics, c.consumerGroupHandler)
}
