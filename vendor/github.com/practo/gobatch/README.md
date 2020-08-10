# gobatch

Simple batch library for Golang.

## How to Use

Install:

```
go get github.com/herryg91/gobatch
```

Example:

```
func fn1(workerID int, datas []interface{}) (err error) {
    //do something
    return
}

// every 100 datas or 15 second no activity, batch will be processed (fn1 will be run) with 2 worker
mBatch := gobatch.NewMemoryBatch(fn1, 100, time.Second*15, 2)

mBatch.Insert(interface{}{})
mBatch.Insert(interface{}{})
mBatch.Insert(interface{}{})
```

Need additional param to DoFn? You can do something like this
```
type additionalParam struct {
	RedisPool *redis.Pool
}

func (p additionalParam) fn1(workerID int, datas []interface{}) (err error) {
	// do something log.Println(p)
	return
}

mBatch := gobatch.NewMemoryBatch(additionalParam{pool}.fn1, 100, time.Second*15, 2)

```

## Future Update:
- Batch using Redis
- Batch using File
- Add / Decrease Worker on the fly
