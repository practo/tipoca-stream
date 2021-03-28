package controllers

import (
	"sort"
)

type unitAllocator struct {
	topics   []string
	realtime []string

	topicsLag              []topicLag
	maxReloadingUnits      int
	currentReloadingTopics []string

	units []deploymentUnit
}

func newUnitAllocator(
	topics,
	realtime []string,
	topicsLag []topicLag,
	maxReloadingUnits int32,
	currentReloadingTopics []string,
) *unitAllocator {
	return &unitAllocator{
		topics:                 topics,
		realtime:               realtime,
		topicsLag:              topicsLag,
		maxReloadingUnits:      int(maxReloadingUnits),
		currentReloadingTopics: currentReloadingTopics,
		units:                  []deploymentUnit{},
	}
}

type deploymentUnit struct {
	id     string
	topics []string
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
		id:     "realtime",
		topics: u.realtime,
	}

	// don't shuffle the already reloading topics unless realtime
	reloadingUnits := []deploymentUnit{}
	for _, topic := range u.currentReloadingTopics {
		_, ok := realtime[topic]
		if ok {
			continue
		}
		reloadingUnits = append(reloadingUnits, deploymentUnit{
			id:     topic,
			topics: []string{topic},
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
	for _, topic := range topicsByLagAsc {
		_, ok := realtime[topic]
		if ok {
			continue
		}
		if len(reloadingUnits) >= u.maxReloadingUnits {
			break
		}
		reloadingUnits = append(reloadingUnits, deploymentUnit{
			id:     topic,
			topics: []string{topic},
		})
	}

	u.units = reloadingUnits
	if len(realtimeUnit.topics) > 0 {
		u.units = append(u.units, realtimeUnit)
	}
}
