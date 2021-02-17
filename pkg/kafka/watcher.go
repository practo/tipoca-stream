package kafka

import (
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/practo/klog/v2"
)

type Watcher interface {
	Topics() ([]string, error)
	ConsumerGroupLag(id string, topic string, partition int32) (int64, error)
}

type kafkaWatch struct {
	client               sarama.Client
	cacheValidity        time.Duration
	lastTopicRefreshTime *int64
	brokers              []string

	// mutex protects the following the mutable state
	mutex sync.Mutex

	topics []string
}

func NewWatcher(
	brokers []string,
	version string,
	configTLS TLSConfig,
) (
	Watcher, error,
) {
	v, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		return nil, fmt.Errorf("Error parsing Kafka version: %v\n", err)
	}

	c := sarama.NewConfig()
	c.Version = v
	if configTLS.Enable {
		c.Net.TLS.Enable = true
		tlsConfig, err := NewTLSConfig(configTLS)
		if err != nil {
			return nil, fmt.Errorf("TLS init failed, err: %v", err)
		}
		c.Net.TLS.Config = tlsConfig
	}

	client, err := sarama.NewClient(brokers, c)
	if err != nil {
		return nil, fmt.Errorf("Error creating client: %v\n", err)
	}

	return &kafkaWatch{
		client:               client,
		cacheValidity:        time.Second * time.Duration(30),
		lastTopicRefreshTime: nil,
		brokers:              brokers,
	}, nil
}

// Topics get the latest topics after refreshing the client with the latest
// it caches it for t.cacheValidity
func (t *kafkaWatch) Topics() ([]string, error) {
	if cacheValid(t.cacheValidity, t.lastTopicRefreshTime) {
		return t.topics, nil
	}

	klog.V(4).Info("Refreshing kafka topic cache")
	// empty so that it refresh all topics
	emptyTopics := []string{}
	err := t.client.RefreshMetadata(emptyTopics...)
	if err != nil {
		return []string{}, err
	}

	topics, err := t.client.Topics()
	if err != nil {
		return []string{}, err
	}

	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.topics = topics
	now := time.Now().UnixNano()
	t.lastTopicRefreshTime = &now

	return t.topics, nil
}

func (t *kafkaWatch) ConsumerGroupLag(
	id string,
	topic string,
	partition int32,
) (
	int64,
	error,
) {
	lag := int64(-1)

	err := t.client.RefreshBrokers(t.brokers)
	if err != nil {
		return lag, err
	}
	err = t.client.RefreshMetadata(topic)
	if err != nil {
		return lag, err
	}

	for _, broker := range t.client.Brokers() {
		err = broker.Open(t.client.Config())
		if err != nil && err != sarama.ErrAlreadyConnected {
			return lag, err
		}

		lag, err = t.consumerGroupLag(id, topic, 0, broker)
		if err != nil {
			return lag, err
		}

		if lag != -1 {
			return lag, nil
		}
	}

	return lag, nil
}

func (t *kafkaWatch) consumerGroupLag(
	id string,
	topic string,
	partition int32,
	broker *sarama.Broker,
) (
	int64,
	error,
) {
	defaultLag := int64(-1)

	lastOffset, err := t.client.GetOffset(topic, partition, sarama.OffsetNewest)
	if err != nil {
		return defaultLag, err
	}

	offsetFetchRequest := sarama.OffsetFetchRequest{
		ConsumerGroup: id,
		Version:       1,
	}
	offsetFetchRequest.AddPartition(topic, partition)

	offsetFetchResponse, err := broker.FetchOffset(&offsetFetchRequest)
	if err != nil {
		return defaultLag, err
	}
	if offsetFetchResponse == nil {
		return defaultLag, fmt.Errorf(
			"OffsetFetch request got no response for request: %+v",
			offsetFetchRequest)
	}

	for topicInResponse, partitions := range offsetFetchResponse.Blocks {
		if topicInResponse != topic {
			continue
		}

		for partitionInResponse, offsetFetchResponseBlock := range partitions {
			if partition != partitionInResponse {
				continue
			}
			// Kafka will return -1 if there is no offset associated
			// with a topic-partition under that consumer group
			if offsetFetchResponseBlock.Offset == -1 {
				klog.V(4).Infof("%s not consumed by group: %v", topic, id)
				return defaultLag, nil
			}
			if offsetFetchResponseBlock.Err != sarama.ErrNoError {
				return defaultLag, err
			}
			return lastOffset - offsetFetchResponseBlock.Offset, nil
		}
	}

	klog.Warningf("%s for group is not active or present in Kafka", topic)
	return defaultLag, nil
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
