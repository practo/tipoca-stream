package controllers

import (
	"github.com/practo/klog/v2"
	tipocav1 "github.com/practo/tipoca-stream/redshiftsink/api/v1"
	transformer "github.com/practo/tipoca-stream/redshiftsink/pkg/transformer"
	"sort"
)

type unitAllocator struct {
	topics   []string
	realtime []string

	topicsLast             []topicLast
	maxReloadingUnits      int
	currentReloadingTopics []string
	mainSinkGroupSpec      *tipocav1.SinkGroupSpec
	reloadSinkGroupSpec    *tipocav1.SinkGroupSpec

	units []deploymentUnit
}

func newUnitAllocator(
	topics,
	realtime []string,
	topicsLast []topicLast,
	maxReloadingUnits int32,
	currentReloadingTopics []string,
	main *tipocav1.SinkGroupSpec,
	reload *tipocav1.SinkGroupSpec,
) *unitAllocator {
	return &unitAllocator{
		topics:                 topics,
		realtime:               realtime,
		topicsLast:             topicsLast,
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

func sortTopicsByLastOffset(topicsLast []topicLast) []string {
	sort.SliceStable(topicsLast, func(i, j int) bool {
		return topicsLast[i].last < topicsLast[j].last
	})

	topics := []string{}
	for _, tl := range topicsLast {
		topics = append(topics, tl.topic)
	}

	return topics
}

func (u *unitAllocator) unitID(topic string) string {
	_, _, table := transformer.ParseTopic(topic)
	if len(table) > 10 {
		return table[:10]
	}

	return table
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
			id:            u.unitID(topic),
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

	topicsByLastAsc := sortTopicsByLastOffset(u.topicsLast)
	if len(topicsByLastAsc) == 0 && len(u.topics) != 0 {
		klog.Infof("empty topicsLast, using %+v", u.topics)
		topicsByLastAsc = u.topics
	}
	for _, topic := range topicsByLastAsc {
		_, ok := realtime[topic]
		if ok {
			continue
		}
		if len(reloadingUnits) >= u.maxReloadingUnits {
			break
		}
		reloadingUnits = append(reloadingUnits, deploymentUnit{
			id:            u.unitID(topic),
			sinkGroupSpec: u.reloadSinkGroupSpec,
			topics:        []string{topic},
		})
	}

	u.units = reloadingUnits
	if len(realtimeUnit.topics) > 0 {
		u.units = append(u.units, realtimeUnit)
	}
}
