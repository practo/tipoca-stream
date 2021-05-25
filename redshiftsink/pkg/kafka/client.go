package kafka

import (
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/practo/klog/v2"
)

type Client interface {
	// Topics return all the topics present in kafka, it keeps a cache
	// which is refreshed every cacheValidity seconds
	Topics() ([]string, error)

	// LastOffset returns the current offset for the topic partition
	LastOffset(topic string, partition int32) (int64, error)

	// CurrentOffset talks to kafka and finds the current offset for the
	// consumer group. It makes call to all brokers to determine
	// the current offset. If group is not found it returns -1
	CurrentOffset(id string, topic string, partition int32) (int64, error)
}

type kafkaClient struct {
	client               sarama.Client
	cacheValidity        time.Duration
	lastTopicRefreshTime *int64
	brokers              []string

	// mutex protects the following the mutable state
	mutex sync.Mutex

	topics []string
}

func NewClient(
	brokers []string,
	version string,
	configTLS TLSConfig,
) (
	Client, error,
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

	return &kafkaClient{
		client:               client,
		cacheValidity:        time.Second * time.Duration(30),
		lastTopicRefreshTime: nil,
		brokers:              brokers,
	}, nil
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

// Topics get the latest topics after refreshing the client with the latest
// it caches it for t.cacheValidity
func (t *kafkaClient) Topics() ([]string, error) {
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

func (t *kafkaClient) LastOffset(topic string, partition int32) (int64, error) {
	return t.client.GetOffset(topic, partition, sarama.OffsetNewest)
}

func (t *kafkaClient) fetchCurrentOffset(
	id string,
	topic string,
	partition int32,
	broker *sarama.Broker,
) (
	int64,
	error,
) {
	defaultCurrentOffset := int64(-1)

	offsetFetchRequest := sarama.OffsetFetchRequest{
		ConsumerGroup: id,
		Version:       1,
	}
	offsetFetchRequest.AddPartition(topic, partition)

	err := broker.Open(t.client.Config())
	if err != nil && err != sarama.ErrAlreadyConnected {
		return defaultCurrentOffset, fmt.Errorf("Error opening broker connection again, err: %v", err)
	}

	offsetFetchResponse, err := broker.FetchOffset(&offsetFetchRequest)
	if err != nil {
		return defaultCurrentOffset, fmt.Errorf(
			"Error fetching offset for offsetFetchRequest: %s %v, err: %v",
			topic, offsetFetchRequest, err)
	}
	if offsetFetchResponse == nil {
		return defaultCurrentOffset, fmt.Errorf(
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
				return defaultCurrentOffset, nil
			}
			if offsetFetchResponseBlock.Err != sarama.ErrNoError {
				return defaultCurrentOffset, fmt.Errorf(
					"Error since offsetFetchResponseBlock.Err != sarama.ErrNoError for offsetFetchResponseBlock.Err: %+v",
					offsetFetchResponseBlock.Err)
			}
			return offsetFetchResponseBlock.Offset, nil
		}
	}

	klog.Warningf("%s for group is not active or present in Kafka", topic)
	return defaultCurrentOffset, nil
}

func (t *kafkaClient) CurrentOffset(
	id string,
	topic string,
	partition int32,
) (
	int64,
	error,
) {
	currentOffset := int64(-1)

	err := t.client.RefreshBrokers(t.brokers)
	if err != nil {
		return currentOffset, fmt.Errorf("Error refreshing kafka brokers, err: %v", err)
	}
	err = t.client.RefreshMetadata(topic)
	if err != nil {
		return currentOffset, fmt.Errorf("Error refreshing kafka metadata, err: %v", err)
	}

	for _, broker := range t.client.Brokers() {
		defer broker.Close()

		err = broker.Open(t.client.Config())
		if err != nil && err != sarama.ErrAlreadyConnected {
			return currentOffset, fmt.Errorf("Error opening broker connection, err: %v", err)
		}

		connected, err := broker.Connected()
		if err != nil {
			return currentOffset, fmt.Errorf("Error checking broker connection, err:%v", err)
		}
		if !connected {
			return currentOffset, fmt.Errorf("Could not connect broker: %+v", broker)
		}

		currentOffset, err = t.fetchCurrentOffset(id, topic, 0, broker)
		if err != nil {
			return currentOffset, fmt.Errorf("Error calculating currentOffset, err: %v", err)
		}

		if currentOffset != -1 {
			return currentOffset, nil
		}
	}

	return currentOffset, nil
}
