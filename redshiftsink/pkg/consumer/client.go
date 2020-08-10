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

const (
	KafkaGo = "kafka-go"
	Sarama  = "sarama"
)

type Client interface {
	Topics() ([]string, error)
	Consume(ctx context.Context, topics []string, ready chan bool) error
	Close() error
}

func NewClient(
	kafkaClient string,
	brokerURLs string,
	group string,
	ver string,
	saramaLog bool,
	saramaAssignor string,
	saramaOldest bool) (Client, error) {

	switch kafkaClient {
	case Sarama:
		return NewSaramaClient(
			brokerURLs, group, ver, saramaLog, saramaAssignor, saramaOldest,
		)
	case KafkaGo:
		return nil, fmt.Errorf(
			"not yet supported, waiting for: %s",
			"https://github.com/segmentio/kafka-go/issues/131",
		)
	default:
		return nil, fmt.Errorf("kafkaClient not supported: %v\n", kafkaClient)
	}
}

func NewSaramaClient(
	brokerURLs string,
	group string,
	ver string,
	saramaLog bool,
	saramaAssignor string,
	saramaOldest bool) (Client, error) {

	if saramaLog {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	version, err := sarama.ParseKafkaVersion(ver)
	if err != nil {
		return nil, fmt.Errorf("Error parsing Kafka version: %v\n", err)
	}

	c := sarama.NewConfig()
	c.Version = version

	switch saramaAssignor {
	case "sticky":
		c.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
	case "roundrobin":
		c.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	case "range":
		c.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	default:
		return nil, fmt.Errorf(
			"Unknown group partition saramaAssignor: %s", saramaAssignor)
	}

	if saramaOldest {
		c.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	// disable auto commits of offsets
	// https://github.com/Shopify/sarama/issues/1570#issuecomment-574908417
	c.Consumer.Offsets.AutoCommit.Enable = false
	c.Consumer.Fetch.Min = 3
	c.Consumer.Fetch.Max = 10

	brokers := strings.Split(brokerURLs, ",")

	consumerGroup, err := sarama.NewConsumerGroup(brokers, group, c)
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
	cluster       sarama.Consumer
	consumerGroup sarama.ConsumerGroup
	consumer      consumer
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
