package controllers

import (
	"fmt"
	klog "github.com/practo/klog/v2"
	tipocav1 "github.com/practo/tipoca-stream/redshiftsink/api/v1"
	kafka "github.com/practo/tipoca-stream/redshiftsink/pkg/kafka"
	"math/rand"
	"sync"
	"time"
)

var (
	DefaultMaxBatcherTopics int = 30
	DefaultMaxLoaderTopics  int = 300
)

type realtimeCalculatorInterface interface {
	calculate(reloading []string, currentRealtime []string) []string
}

type offsetPosition struct {
	last    *int64
	current *int64
}

type topicRealtimeInfo struct {
	lastUpdate      *int64
	batcher         *offsetPosition
	loader          *offsetPosition
	batcherRealtime bool
	loaderRealtime  bool
}

type realtimeCalculator struct {
	rsk         *tipocav1.RedshiftSink
	watcher     kafka.Watcher
	topicGroups map[string]tipocav1.Group
	cache       *sync.Map

	batchersRealtime []string
	loadersRealtime  []string
}

func newRealtimeCalculator(
	rsk *tipocav1.RedshiftSink,
	watcher kafka.Watcher,
	topicGroups map[string]tipocav1.Group,
	cache *sync.Map,
) realtimeCalculatorInterface {

	return &realtimeCalculator{
		rsk:         rsk,
		watcher:     watcher,
		topicGroups: topicGroups,
		cache:       cache,
	}
}

func (r *realtimeCalculator) maxLag(topic string) (int64, int64) {
	var maxBatcherLag, maxLoaderLag int64
	if r.rsk.Spec.ReleaseCondition == nil {
		maxBatcherLag = DefaultMaxBatcherLag
		maxLoaderLag = DefautMaxLoaderLag
	} else {
		if r.rsk.Spec.ReleaseCondition.MaxBatcherLag != nil {
			maxBatcherLag = *r.rsk.Spec.ReleaseCondition.MaxBatcherLag
		}
		if r.rsk.Spec.ReleaseCondition.MaxLoaderLag != nil {
			maxLoaderLag = *r.rsk.Spec.ReleaseCondition.MaxLoaderLag
		}
		if r.rsk.Spec.TopicReleaseCondition != nil {
			d, ok := r.rsk.Spec.TopicReleaseCondition[topic]
			if ok {
				if d.MaxBatcherLag != nil {
					maxBatcherLag = *d.MaxBatcherLag
				}
				if d.MaxLoaderLag != nil {
					maxLoaderLag = *d.MaxLoaderLag
				}
			}
		}
	}

	return maxBatcherLag, maxLoaderLag
}

// fetchRealtimeCache tires to get the topicRealtimeInfo from cache
// if found in cache and cache is valid it returns true and the info
// else it returns no info and false
func (r *realtimeCalculator) fetchRealtimeCache(
	topic string,
) (
	topicRealtimeInfo, bool,
) {
	loadedInfo, ok := r.cache.Load(topic)
	if !ok {
		return topicRealtimeInfo{}, false
	}

	// 600 to 840 seconds, randomness to prevent multiple parallel calls
	validSec := rand.Intn(240) + 600
	klog.V(5).Infof(
		"rsk/%s validSec: %v topic: %s",
		r.rsk.Name,
		validSec,
		topic,
	)

	info := loadedInfo.(topicRealtimeInfo)
	if cacheValid(time.Second*time.Duration(validSec), info.lastUpdate) {
		klog.V(4).Infof(
			"rsk/%s (realtime cache hit) topic: %s",
			r.rsk.Name,
			topic,
		)
		return info, true
	}

	return topicRealtimeInfo{}, false
}

// fetchRealtimeInfo fetches the offset info for the topic
func (r *realtimeCalculator) fetchRealtimeInfo(
	topic string,
	loaderTopic *string,
	group tipocav1.Group,
) (
	topicRealtimeInfo, error,
) {
	klog.V(2).Infof("rsk/%s (fetching realtime) topic: %s", r.rsk.Name, topic)

	now := time.Now().UnixNano()
	info := topicRealtimeInfo{
		batcher:         &offsetPosition{},
		loader:          &offsetPosition{},
		batcherRealtime: false,
		loaderRealtime:  false,
		lastUpdate:      &now,
	}
	// batcher's lag analysis: a) get last
	last, err := r.watcher.LastOffset(topic, 0)
	if err != nil {
		return info, fmt.Errorf("Error getting last offset for %s", topic)
	}
	info.batcher.last = &last
	klog.V(4).Infof("%s, lastOffset=%v", topic, last)

	// batcher's lag analysis: b) get current
	current, err := r.watcher.CurrentOffset(
		consumerGroupID(r.rsk.Name, r.rsk.Namespace, group.ID, "-batcher"),
		topic,
		0,
	)
	if err != nil {
		return info, err
	}
	klog.V(4).Infof("%s, currentOffset=%v", topic, current)
	if current == -1 {
		info.batcher.current = nil
		klog.V(2).Infof("%s, batcher cg 404, not realtime", topic)
		return info, nil
	} else {
		info.batcher.current = &current
	}

	if loaderTopic == nil {
		return info, nil
	}

	// loader's lag analysis: a) get last
	last, err = r.watcher.LastOffset(*loaderTopic, 0)
	if err != nil {
		return info, fmt.Errorf("Error getting last offset for %s", *loaderTopic)
	}
	info.loader.last = &last
	klog.V(4).Infof("%s, lastOffset=%v", *loaderTopic, last)

	// loader's lag analysis: b) get current
	current, err = r.watcher.CurrentOffset(
		consumerGroupID(r.rsk.Name, r.rsk.Namespace, group.ID, "-loader"),
		*loaderTopic,
		0,
	)
	if err != nil {
		return info, err
	}
	klog.V(4).Infof("%s, currentOffset=%v (queried)", *loaderTopic, current)
	if current == -1 {
		// CurrentOffset can be -1 in two cases (this may be required in batcher also)
		// 1. When the Consumer Group was never created in that case we return and consider the topic not realtime
		// 2. When the Consumer Group had processed before but now is showing -1 currentOffset as it is inactive due to less throughput.
		//    On such a scenario, we consider it realtime. We find this case by saving the currentOffset for the loader topcics in RedshiftSinkStatus.TopicGroup
		if group.LoaderCurrentOffset == nil {
			klog.V(2).Infof("%s, loader cg 404, not realtime", *loaderTopic)
			return info, nil
		}
		klog.V(2).Infof("%s, currentOffset=%v (old), cg 404, try realtime", *loaderTopic, *group.LoaderCurrentOffset)
		// give the topic the opportunity to release based on its last found currentOffset
		info.loader.current = group.LoaderCurrentOffset
	} else {
		group.LoaderCurrentOffset = &current
		// updates the new queried loader offset
		klog.V(4).Infof("%s, cg found", *loaderTopic)
		updateTopicGroup(r.rsk, topic, group)
		info.loader.current = &current
	}

	return info, nil
}

// calculate computes the realtime topics and updates its realtime info
func (r *realtimeCalculator) calculate(reloading []string, currentRealtime []string) []string {
	if len(reloading) == 0 {
		return currentRealtime
	}

	realtimeTopics := []string{}
	allTopics, err := r.watcher.Topics()
	if err != nil {
		klog.Errorf(
			"Ignoring realtime update. Error fetching all topics, err:%v",
			err,
		)
		return currentRealtime
	}
	allTopicsMap := toMap(allTopics)

	current := toMap(currentRealtime)
	for _, topic := range reloading {

		group, ok := r.topicGroups[topic]
		if !ok {
			klog.Errorf("topicGroup 404 in status for: %s", topic)
			continue
		}

		var loaderTopic *string
		ltopic := r.rsk.Spec.KafkaLoaderTopicPrefix + group.ID + "-" + topic
		_, ok = allTopicsMap[ltopic]
		if !ok {
			klog.V(2).Infof("%s topic 404, not realtime.", loaderTopic)
		} else {
			loaderTopic = &ltopic
		}

		now := time.Now().UnixNano()

		info, hit := r.fetchRealtimeCache(topic)
		if !hit { // fetch again, cache miss
			info, err = r.fetchRealtimeInfo(topic, loaderTopic, group)
			if err != nil {
				klog.Errorf(
					"rsk/%s Error fetching realtime info for topic: %s, err: %v",
					r.rsk.Name,
					topic,
					err,
				)
				// if there is an error in finding lag
				// and the topic was already in realtime consider it realtime
				// consumer groups disappear due to inactivity, hence this
				_, ok := current[topic]
				if ok {
					r.cache.Store(
						topic,
						topicRealtimeInfo{
							batcherRealtime: true,
							loaderRealtime:  true,
							lastUpdate:      &now,
						},
					)
					realtimeTopics = append(realtimeTopics, topic)
					r.batchersRealtime = append(r.batchersRealtime, topic)
					r.loadersRealtime = append(r.loadersRealtime, ltopic)
					continue
				}
			}
		}

		// compute realtime
		maxBatcherLag, maxLoaderLag := r.maxLag(topic)
		if info.batcher != nil && info.batcher.last != nil && info.batcher.current != nil {
			if *info.batcher.last-*info.batcher.current <= maxBatcherLag {
				klog.V(3).Infof("rsk/s: %s, batcher realtime", r.rsk.Name, topic)
				info.batcherRealtime = true
				r.batchersRealtime = append(r.batchersRealtime, topic)
			}
		}
		if info.loader != nil && info.loader.last != nil && info.loader.current != nil {
			if *info.loader.last-*info.loader.current <= maxLoaderLag {
				klog.V(3).Infof("rsk/s: %s, loader realtime", r.rsk.Name, ltopic)
				info.loaderRealtime = true
				r.loadersRealtime = append(r.loadersRealtime, ltopic)
			}
		}
		if info.batcherRealtime && info.loaderRealtime {
			klog.V(2).Infof("rsk/s: %s, realtime", r.rsk.Name, topic)
			realtimeTopics = append(realtimeTopics, topic)
		} else {
			klog.V(2).Infof("%v: waiting to reach realtime", topic)
		}

		info.lastUpdate = &now
		r.cache.Store(topic, info)
	}

	return realtimeTopics
}
