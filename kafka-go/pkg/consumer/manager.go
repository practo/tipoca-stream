package consumer

import (
	"context"
	"os"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/practo/klog/v2"
)

type Manager struct {
	// consumer client, this is sarama now, can be kafka-go later
	consumerGroup ConsumerGroup

	// topicRegexes is the list of topics to monitor
	topicRegexes []*regexp.Regexp

	// sigterm is for cancellation
	sigterm chan os.Signal

	// ready is used to signal the main thread about the readiness of
	// the manager
	Ready chan bool

	// mutex protects the following mutable state
	mutex sync.Mutex

	// topics is computed based on the topicRegexes specified
	topics []string

	// activeTopics keep track of topics whose consumer loop has stared
	activeTopics map[string]bool
}

func NewManager(consumerGroup ConsumerGroup,
	regexes string, sigterm chan os.Signal) *Manager {

	var topicRegexes []*regexp.Regexp
	expressions := strings.Split(regexes, ",")
	for _, expression := range expressions {
		rgx, err := regexp.Compile(strings.TrimSpace(expression))
		if err != nil {
			klog.Fatalf("Compling regex: %s failed, err:%v\n", expression, err)
		}
		topicRegexes = append(topicRegexes, rgx)
	}

	return &Manager{
		consumerGroup: consumerGroup,
		topicRegexes:  topicRegexes,
		Ready:         make(chan bool),
		activeTopics:  make(map[string]bool),
		sigterm:       sigterm,
	}
}

func (c *Manager) updatetopics(allTopics []string) {
	topics := []string{}
	topicsAppended := make(map[string]bool)

	for _, topic := range allTopics {
		for _, regex := range c.topicRegexes {
			if !regex.MatchString(topic) {
				continue
			}
			_, ok := topicsAppended[topic]
			if ok {
				continue
			}
			topics = append(topics, topic)
			topicsAppended[topic] = true
		}
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	klog.V(6).Infof(
		"%d topic(s) with regexes: %v\n",
		len(topics),
		c.topicRegexes,
	)
	c.topics = topics
}

func (c *Manager) deepCopyTopics() []string {
	return append(make([]string, 0, len(c.topics)), c.topics...)
}

func (c *Manager) refreshTopics() {
	allTopics, err := c.consumerGroup.Topics()
	if err != nil {
		klog.Fatalf("Error getting topics, err=%v\n", err)
	}
	klog.V(6).Infof("%d topic(s) in the cluster\n", len(allTopics))
	klog.V(6).Infof("Topics in the cluster=%v\n", allTopics)
	c.updatetopics(allTopics)
}

func (c *Manager) topicInActive(topics []string) []string {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	inActiveTopics := []string{}

	for _, topic := range topics {
		_, ok := c.activeTopics[topic]
		if !ok {
			inActiveTopics = append(inActiveTopics, topic)
		}
	}

	return inActiveTopics
}

func (c *Manager) setActiveTopics(topics []string) {
	activeTopics := make(map[string]bool)
	for _, topic := range topics {
		activeTopics[topic] = true
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.activeTopics = activeTopics
}

func (c *Manager) SyncTopics(
	ctx context.Context, seconds int, wg *sync.WaitGroup) {

	defer wg.Done()
	ticker := time.NewTicker(time.Second * time.Duration(seconds))
	for {
		c.refreshTopics()
		topics := c.deepCopyTopics()

		inactiveTopics := c.topicInActive(topics)
		if len(inactiveTopics) > 0 {
			klog.Info("New topics are inactive: %v\n", inactiveTopics)
			// TODO: assumes there is a proecss above that restarts
			// when this shutsdown, it is using Kubernetes Deployment like
			// functionality to reload
			klog.Info("Reloading.... Gracefully shutdown, container restarts!")
			c.sigterm <- syscall.SIGINT
		}

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
			klog.Info("No topics found. Waiting, correct topicRegexes?")
			time.Sleep(time.Second * 5)
			continue
		}

		c.printLastOffsets()

		c.setActiveTopics(topics)
		klog.V(2).Infof("Manager.Consume for %d topic(s)\n", len(topics))
		err := c.consumerGroup.Consume(ctx, topics)
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
