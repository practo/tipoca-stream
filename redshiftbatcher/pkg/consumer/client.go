package consumer

import (
    "fmt"
    "os"
    "log"
    "context"

    "github.com/Shopify/sarama"
)

type Client interface {
    Topics() ([]string, error)
    Consume(ctx context.Context, topics []string)
}

type saramaClient struct {
    client   sarama.Consumer
    consumer Consumer
}

func (c *saramaClient) Topics() ([]string, error) {
    return c.client.Topics()
}

func (c *saramaClient) Consume(ctx context.Context, topics []string) error {
    return c.client.Consume(ctx, topics, &c.consumer)
}

func NewClient(
    brokers []string,
    group string,
    clientlog bool,
    version string,
    assignor string,
    oldest bool) (*Client, error) {

    if verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	version, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		return nil, fmt.Errorf("Error parsing Kafka version: %v", err)
	}

	config := sarama.NewConfig()
	config.Version = version

	switch assignor {
	case "sticky":
		config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
	case "roundrobin":
		config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	case "range":
		config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	default:
		return nil, fmt.Errorf(
            "Unknown group partition assignor: %s", assignor)
	}

	if oldest {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

    client, err := sarama.NewConsumerGroup(
        strings.Split(brokers, ","), group, config)
	if err != nil {
		return nil, fmt.Errorf("Error creating consumer group client: %v", err)
	}

    // TODO
    // init Batcher

    return &saramaClient{
        client: client,
        batcher: b,
    }, nil
}
