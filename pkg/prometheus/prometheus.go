package prometheus

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	klog "github.com/practo/klog/v2"
	prometheus "github.com/prometheus/client_golang/api"
	prometheusv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	model "github.com/prometheus/common/model"
)

type Client interface {
	Address() string
	Query(queryString string) (float64, error)
	QueryVector(queryString string) (*model.Vector, error)
	FilterVector(v model.Vector, filterLabelName, filterLabelValue string) (*float64, error)
}

type promClient struct {
	address string
	client  prometheusv1.API

	mutex sync.Mutex

	// cache
	queryCache         map[string]float64
	queryLastRun       map[string]*int64
	queryCacheValidity time.Duration
}

func NewClient(address string) (Client, error) {
	config := prometheus.Config{
		Address: address,
	}
	client, err := prometheus.NewClient(config)
	if err != nil {
		return nil, fmt.Errorf("Error making prometheus api client, err: %v", err)
	}

	return &promClient{
		address:            address,
		client:             prometheusv1.NewAPI(client),
		queryCache:         make(map[string]float64),
		queryLastRun:       make(map[string]*int64),
		queryCacheValidity: time.Second * time.Duration(15),
	}, nil
}

func cacheValid(validity time.Duration, lastCachedTime *int64) bool {
	if lastCachedTime == nil {
		return false
	}

	if (*lastCachedTime + validity.Nanoseconds()) > time.Now().UnixNano() {
		return true
	}

	return false
}

func convertValueToFloat(val model.Value) float64 {
	switch {
	case val.Type() == model.ValScalar:
		scalarVal := val.(*model.Scalar)
		return float64(scalarVal.Value)
	case val.Type() == model.ValVector:
		vectorVal := val.(model.Vector)
		total := float64(0)
		for _, elem := range vectorVal {
			total += float64(elem.Value)
		}
		return total
	case val.Type() == model.ValMatrix:
		matrixVal := val.(model.Matrix)
		total := float64(0)
		for _, elem := range matrixVal {
			total += float64(elem.Values[len(elem.Values)-1].Value)
		}
		return total
	default:
		klog.Warningf(
			"return value type of prometheus query was unrecognized, type: %v",
			val.Type(),
		)
		return 0
	}
}

func (p *promClient) Address() string {
	return p.address
}

func (p *promClient) queryWithRetry(
	queryString string, maxRetry int,
) (model.Value, prometheusv1.Warnings, error) {
	var err error
	for retry := 0; retry < maxRetry; retry++ {
		value, warning, err := p.client.Query(
			context.Background(),
			queryString,
			time.Now(),
		)
		if err == nil {
			return value, warning, nil
		}
		klog.Errorf("retrying querying prometheus, err:%v", err)
		sleepFor := rand.Intn(5000) + 1000
		time.Sleep(time.Duration(sleepFor) * time.Millisecond)
	}

	return nil, prometheusv1.Warnings{}, err
}

func (p *promClient) Query(queryString string) (float64, error) {
	// return from cache if cache hit
	p.mutex.Lock()
	lastRun, ok := p.queryLastRun[queryString]
	p.mutex.Unlock()
	if ok {
		if cacheValid(p.queryCacheValidity, lastRun) {
			cache, ok := p.queryCache[queryString]
			if ok {
				return cache, nil
			}
			klog.Warningf("cache empty for query: %s, unexpected!", queryString)
		}
	}

	// query
	now := time.Now().UnixNano()
	value, warning, err := p.queryWithRetry(queryString, 5)
	if err != nil {
		return 0, fmt.Errorf("Error querying prometheus, err: %v", err)
	}
	if warning != nil {
		klog.Warningf("%v", warning)
	}
	result := convertValueToFloat(value)

	// update cache
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.queryLastRun[queryString] = &now
	p.queryCache[queryString] = result

	return result, nil
}

func (p *promClient) QueryVector(queryString string) (*model.Vector, error) {
	// query
	value, warning, err := p.queryWithRetry(queryString, 5)
	if err != nil {
		return &model.Vector{}, fmt.Errorf(
			"Error querying prometheus, err: %v", err,
		)
	}
	if warning != nil {
		klog.Warningf("%v", warning)
	}

	if value.Type() != model.ValVector {
		return &model.Vector{}, fmt.Errorf(
			"query did not return vector, it returned: %v", value.Type(),
		)
	}
	vector := value.(model.Vector)

	return &vector, nil
}

// FilterVector returns the first found match, user needs to take
// care of keeping queries such that there are not multiple values
func (p *promClient) FilterVector(
	vector model.Vector,
	filterName string,
	filterValue string,
) (*float64, error) {
	for _, v := range vector {
		for labelName, labelValue := range v.Metric {
			if string(labelName) == filterName && string(labelValue) == filterValue {
				sampleValue := float64(v.Value)
				return &sampleValue, nil
			}
		}
	}

	return nil, nil
}
