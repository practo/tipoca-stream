package consumer

import (
    "sync"
    "time"
    "context"
    "strings"

    "github.com/practo/klog/v2"
)

type Manager struct {
    // consumer client
    client *consumer.Consumer

    // topicPrefixes is the list of topics to monitor
    topicPrefixes []string

    // mutex protects the following mutable state
	mutex sync.Mutex

    // topics is computed based on the topicPrefixes specified
    topics []string
}

func NewManager(client *consumer.Consumer, topicPrefixes []string) *Manager {
    return &Manager{
        client: client,
        topicPrefixes: topicPrefixes,
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

    c.topics = topics
}

func (c *Manager) RefreshTopics(
    ctx context.Context, seconds int, wg &sync.WaitGroup) {

    defer wg.Done()
    ticker := time.NewTicker(time.Second * seconds)
    for {
        select {
        case <-ticker.C:
            topics, err := c.client.Topics()
            if err != nil {
                klog.Fatalf("Error getting topics, err=%v\n", err)
            }
            c.updatetopics(topics)
        case <-ctx.Done():
            return
        }
    }
}

func(c *Manager) Consume(ctx context.Context, wg *sync.WaitGroup) error {
	defer wg.Done()
    for {
		// `Consume` should be called inside an infinite loop, when a
		// server-side rebalance happens, the consumer session will need to be
		// recreated to get the new claims

        // TODO deep copy topics
        topics

		err := c.client.Consume(ctx, topics)
        if err != nil {
			klog.Error("Error from consumer: %v", err)
		}
		// check if context was cancelled, signaling that the consumer should stop
		if ctx.Err() != nil {
			return
		}
		consumer.ready = make(chan bool)
	}
}
