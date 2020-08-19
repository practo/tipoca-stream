package consumer

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/practo/klog/v2"
)

type Manager struct {
	// consumer client, this is sarama now, can be kafka-go later
	consumerGroup ConsumerGroup

	// topicPrefixes is the list of topics to monitor
	topicPrefixes []string

	// ready is used to signal the main thread about the readiness of
	// the manager
	Ready chan bool

	// mutex protects the following mutable state
	mutex sync.Mutex

	// topics is computed based on the topicPrefixes specified
	topics []string
}

func NewManager(
	consumerGroup ConsumerGroup, topicPrefixes string) *Manager {

	return &Manager{
		consumerGroup: consumerGroup,
		topicPrefixes: strings.Split(topicPrefixes, ","),
		Ready:         make(chan bool),
	}
}

func (c *Manager) updatetopics(allTopics []string) {
	topics := []string{}

	for _, topic := range allTopics {
		for _, prefix := range c.topicPrefixes {
			if strings.HasPrefix(topic, prefix) {
				topics = append(topics, topic)
			}
		}
	}
	// TODO: remove duplicates in topics

	c.mutex.Lock()
	defer c.mutex.Unlock()

	klog.V(5).Infof(
		"%d topic(s) with prefixes: %v\n",
		len(topics),
		c.topicPrefixes,
	)
	c.topics = topics
}

func (c *Manager) deepCopyTopics() []string {
	return append(make([]string, 0, len(c.topics)), c.topics...)
}

func (c *Manager) refreshTopics() {
	topics, err := c.consumerGroup.Topics()
	if err != nil {
		klog.Fatalf("Error getting topics, err=%v\n", err)
	}
	klog.V(5).Infof("%d topic(s) in the cluster\n", len(topics))
	klog.V(5).Infof("Topics in the cluster=%v\n", topics)
	c.updatetopics(topics)
}

func (c *Manager) SyncTopics(
	ctx context.Context, seconds int, wg *sync.WaitGroup) {

	defer wg.Done()
	ticker := time.NewTicker(time.Second * time.Duration(seconds))
	for {
		c.refreshTopics()

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			continue
		}
	}
}

// TODO: prints the last offset in a topic, can help in making the metric
// for the lag, not being used at present (can call it dead)
func (c *Manager) printLastOffsets() {
	for _, topic := range c.topics {
		lastOffset, err := c.consumerGroup.LastOffset(topic, 0)
		if err != nil {
			klog.Errorf("Unable to get offset, err:%v\n", err)
			continue
		}
		klog.Infof(
			"topic:%s, partition:0, lastOffset:%d (kafka lastoffset)\n",
			topic,
			lastOffset,
		)
	}
}

func (c *Manager) Consume(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		// `Consume` should be called inside an infinite loop, when a
		// server-side rebalance happens, the consumer session will need to be
		// recreated to get the new claims
		topics := c.deepCopyTopics()
		if len(topics) == 0 {
			klog.Error("No topics found, waiting")
			time.Sleep(time.Second * 5)
			continue
		}

		c.printLastOffsets()

		klog.V(2).Infof("Manager.Consume for %d topic(s)\n", len(topics))
		err := c.consumerGroup.Consume(ctx, topics, c.Ready)
		if err != nil {
			klog.Fatalf("Error from consumer: %v", err)
		}
		// check if context was cancelled, the consumer should stop
		if ctx.Err() != nil {
			klog.V(2).Info("Manager.Context cancelled")
			return
		}
		klog.V(2).Info("Manager.Consume completed loop, will re run")
	}
}
