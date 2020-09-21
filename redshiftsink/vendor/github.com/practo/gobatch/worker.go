package gobatch

func (b *Batch) flushWorker(workerID int, datas []interface{}) {
	b.doFn(workerID, datas)
}

func (b *Batch) setFlushWorker(workerSize int) {
	if workerSize < 1 {
		workerSize = 1
	}
	for id := 1; id <= workerSize; id++ {
		go func(workerID int, flushJobs <-chan []interface{}) {
			for j := range flushJobs {
				b.flushWorker(workerID, j)
			}
		}(id, b.flushChan)
	}
}
