package gobatch

import (
	"sync"
	"time"
)

func NewMemoryBatch(flushMaxSize int, flushMaxWait time.Duration, callback BatchFn, workerSize int) *Batch {
	instance := &Batch{
		maxSize: flushMaxSize,
		maxWait: flushMaxWait,

		items: make([]interface{}, 0),
		doFn:  callback,
		mutex: &sync.RWMutex{},

		flushChan: make(chan []interface{}, workerSize),
	}
	instance.setFlushWorker(workerSize)
	go instance.runFlushByTime()
	return instance
}

func (b *Batch) Insert(data interface{}) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.items = append(b.items, data)
	if len(b.items) >= b.maxSize {
		b.Flush()
	}
}

func (b *Batch) FlushInsert(data interface{}) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	b.Flush()

	b.items = append(b.items, data)
	if len(b.items) >= b.maxSize {
		b.Flush()
	}
}

func (b *Batch) runFlushByTime() {
	for {
		select {
		case <-time.Tick(b.maxWait):
			b.mutex.Lock()
			b.Flush()
			b.mutex.Unlock()
		}
	}
}

func (b *Batch) Flush() {
	if len(b.items) <= 0 {
		return
	}

	copiedItems := make([]interface{}, len(b.items))
	for idx, i := range b.items {
		copiedItems[idx] = i
	}
	b.items = b.items[:0]
	b.flushChan <- copiedItems
}
