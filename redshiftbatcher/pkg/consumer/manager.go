package consumer

import (
    "sync"
    "time"
    "context"
    "strings"

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
        client: client,
        topicPrefixes: strings.Split(topicPrefixes, ","),
        Ready: make(chan bool),
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

func (c *Manager) getDeepCopyTopics() []string {
    return append(make([]string, 0, len(c.topics)), c.topics...)
}

func (c *Manager) RefreshTopics(
    ctx context.Context, seconds int, wg *sync.WaitGroup) {

    defer wg.Done()
    ticker := time.NewTicker(time.Second * time.Duration(seconds))
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

func(c *Manager) Consume(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
    for {
		// `Consume` should be called inside an infinite loop, when a
		// server-side rebalance happens, the consumer session will need to be
		// recreated to get the new claims
        topics := c.getDeepCopyTopics()

		err := c.client.Consume(ctx, topics, c.Ready)
        if err != nil {
			klog.Fatalf("Error from consumer: %v", err)
		}
		// check if context was cancelled, signaling that the consumer should stop
		if ctx.Err() != nil {
			return
		}
	}
}
