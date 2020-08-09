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
	client Client

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
	client Client, topicPrefixes string) *Manager {

	return &Manager{
		client:        client,
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

	klog.V(4).Infof(
		"%d topic(s) with prefixes: %v\n",
		len(topics),
		c.topicPrefixes,
	)
	c.topics = topics
}

func (c *Manager) getDeepCopyTopics() []string {
	return append(make([]string, 0, len(c.topics)), c.topics...)
}

func (c *Manager) refreshTopics() {
	topics, err := c.client.Topics()
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
		case <-ticker.C:
			continue
		case <-ctx.Done():
			return
		}
	}
}

func (c *Manager) Consume(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		// `Consume` should be called inside an infinite loop, when a
		// server-side rebalance happens, the consumer session will need to be
		// recreated to get the new claims
		topics := c.getDeepCopyTopics()
		if len(topics) == 0 {
			klog.Error("No topics found, waiting")
			time.Sleep(time.Second * 5)
			continue
		}

		klog.V(4).Infof("Calling consume for %d topic(s)\n", len(topics))
		err := c.client.Consume(ctx, topics, c.Ready)
		if err != nil {
			klog.Fatalf("Error from consumer: %v", err)
		}
		// check if context was cancelled, signaling that the consumer should stop
		if ctx.Err() != nil {
			return
		}
		klog.V(5).Info("Done with Consume. It will be rerun.")
	}
}
