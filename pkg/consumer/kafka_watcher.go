package consumer

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/practo/klog/v2"
	"time"
)

type KafkaWatcher interface {
	Topics() ([]string, error)
}

type kafkaWatch struct {
	client               sarama.Client
	cacheValidity        time.Duration
	lastTopicRefreshTime *int64

	topics []string
}

func NewKafkaWatcher(brokers []string, version string) (KafkaWatcher, error) {
	v, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		return nil, fmt.Errorf("Error parsing Kafka version: %v\n", err)
	}

	c := sarama.NewConfig()
	c.Version = v

	client, err := sarama.NewClient(brokers, c)
	if err != nil {
		return nil, fmt.Errorf("Error creating client: %v\n", err)
	}

	return &kafkaWatch{
		client:               client,
		cacheValidity:        time.Second * time.Duration(30),
		lastTopicRefreshTime: nil,
	}, nil
}

// Topics get the latest topics after refreshing the client with the latest
// it caches it for t.cacheValidity
func (t *kafkaWatch) Topics() ([]string, error) {
	if cacheValid(t.cacheValidity, t.lastTopicRefreshTime) {
		return t.topics, nil
	}

	klog.V(3).Info("Refreshing kafka topic cache")
	// empty so that it refresh all topics
	emptyTopics := []string{}
	err := t.client.RefreshMetadata(emptyTopics...)
	if err != nil {
		return []string{}, err
	}

	return t.client.Topics()
}

func cacheValid(validity time.Duration, lastCachedTime *int64) bool {
	if lastCachedTime == nil {
		return false
	}

	if (*lastCachedTime + validity.Nanoseconds()) > time.Now().UnixNano() {
		return true
	}

	return false
}
