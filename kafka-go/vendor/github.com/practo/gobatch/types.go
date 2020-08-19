package gobatch

import (
	"sync"
	"time"
)

type BatchFn func(workerID int, datas []interface{})
type Batch struct {
	maxSize int
	maxWait time.Duration

	items []interface{}
	doFn  BatchFn
	mutex *sync.RWMutex

	/*notifier channel*/
	flushChan chan []interface{}
}
