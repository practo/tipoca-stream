package consumer

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/practo/klog/v2"
	"sync"
	"time"
)

type KafkaWatcher interface {
	Topics() ([]string, error)
	ConsumerGroupLag(id string, topic string, partition int32) (int64, error)
}

type kafkaWatch struct {
	client               sarama.Client
	broker               *sarama.Broker
	cacheValidity        time.Duration
	lastTopicRefreshTime *int64

	// mutex protects the following the mutable state
	mutex sync.Mutex

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

	broker := sarama.NewBroker(brokers[0])
	err = broker.Open(nil)
	if err != nil {
		return nil, fmt.Errorf(
			"Cannot connect to broker: %s, err: %v", brokers[0], err)
	}

	return &kafkaWatch{
		client:               client,
		broker:               broker,
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

	offsetFetchResponse, err := t.broker.FetchOffset(&offsetFetchRequest)
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
				klog.Warningf(
					"Topic:%s, Parition:%v not consumed by group: %s yet!",
					topic, partition, id)
				return defaultLag, nil
			}
			if offsetFetchResponseBlock.Err != sarama.ErrNoError {
				return defaultLag, err
			}
			return lastOffset - offsetFetchResponseBlock.Offset, nil
		}
	}

	klog.Warningf(
		"Topic:%s, Parition:%v not found in Kafka for group: %s",
		topic, partition, id)
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
