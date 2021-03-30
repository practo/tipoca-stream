package controllers

import (
	"github.com/practo/klog/v2"
	tipocav1 "github.com/practo/tipoca-stream/redshiftsink/api/v1"
	"sort"
)

type unitAllocator struct {
	topics   []string
	realtime []string

	topicsLag              []topicLag
	maxReloadingUnits      int
	currentReloadingTopics []string
	mainSinkGroupSpec      *tipocav1.SinkGroupSpec
	reloadSinkGroupSpec    *tipocav1.SinkGroupSpec

	units []deploymentUnit
}

func newUnitAllocator(
	topics,
	realtime []string,
	topicsLag []topicLag,
	maxReloadingUnits int32,
	currentReloadingTopics []string,
	main *tipocav1.SinkGroupSpec,
	reload *tipocav1.SinkGroupSpec,
) *unitAllocator {
	return &unitAllocator{
		topics:                 topics,
		realtime:               realtime,
		topicsLag:              topicsLag,
		maxReloadingUnits:      int(maxReloadingUnits),
		currentReloadingTopics: currentReloadingTopics,
		units:                  []deploymentUnit{},
		mainSinkGroupSpec:      main,
		reloadSinkGroupSpec:    reload,
	}
}

type deploymentUnit struct {
	id            string
	sinkGroupSpec *tipocav1.SinkGroupSpec
	topics        []string
}

func sortTopicsByLag(topicsLag []topicLag) []string {
	sort.SliceStable(topicsLag, func(i, j int) bool {
		return topicsLag[i].lag < topicsLag[j].lag
	})

	topics := []string{}
	for _, tl := range topicsLag {
		topics = append(topics, tl.topic)
	}

	return topics
}

// for the reloading sinkGroup
func (u *unitAllocator) allocateReloadingUnits() {
	realtime := toMap(u.realtime)
	realtimeUnit := deploymentUnit{
		id:            "realtime",
		sinkGroupSpec: u.mainSinkGroupSpec,
		topics:        u.realtime,
	}

	// don't shuffle the already reloading topics unless realtime
	reloadingUnits := []deploymentUnit{}
	for _, topic := range u.currentReloadingTopics {
		_, ok := realtime[topic]
		if ok {
			continue
		}
		reloadingUnits = append(reloadingUnits, deploymentUnit{
			id:            topic,
			sinkGroupSpec: u.reloadSinkGroupSpec,
			topics:        []string{topic},
		})
	}

	if len(reloadingUnits) >= u.maxReloadingUnits {
		u.units = reloadingUnits
		if len(realtimeUnit.topics) > 0 {
			u.units = append(u.units, realtimeUnit)
		}
		return
	}

	topicsByLagAsc := sortTopicsByLag(u.topicsLag)
	if len(topicsByLagAsc) == 0 && len(u.topics) != 0 {
		klog.Infof("empty topicsLag, using %+v", u.topics)
		topicsByLagAsc = u.topics
	}
	for _, topic := range topicsByLagAsc {
		_, ok := realtime[topic]
		if ok {
			continue
		}
		if len(reloadingUnits) >= u.maxReloadingUnits {
			break
		}
		reloadingUnits = append(reloadingUnits, deploymentUnit{
			id:            topic,
			sinkGroupSpec: u.reloadSinkGroupSpec,
			topics:        []string{topic},
		})
	}

	u.units = reloadingUnits
	if len(realtimeUnit.topics) > 0 {
		u.units = append(u.units, realtimeUnit)
	}
}
