package consumer

import (
	"sync"
	"time"
)

type batchers = sync.Map

type batcher struct {
	topic string

	minItems uint64 `json:"minItems"`
	maxItems uint64 `json:"maxItems"`

	minWait time.Duration `json:"minTime"`
	maxWait time.Duration `json:"maxTime"`
}

func newBatcher(topic string) *batcher {
	return &batcher{
		minItems: 10,
		maxItems: 20,
		minWait:  time.Second * time.Duration(1),
		maxWait:  time.Second * time.Duration(60),
	}
}

func (c *batcher) batch(topic string, partition int32, value []byte) error {
	return nil
}

func (c *batcher) release() bool {
	return false
}

func (c *batcher) upload() error {
	return nil
}

func (c *batcher) signalLoader() error {
	return nil
}

func (c *batcher) markMessageCommit() {
	// session.MarkMessage(message, "")
	// session.Commit()
}
