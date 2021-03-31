package controllers

import (
	"github.com/practo/klog/v2"
	tipocav1 "github.com/practo/tipoca-stream/redshiftsink/api/v1"
	transformer "github.com/practo/tipoca-stream/redshiftsink/pkg/transformer"
	"sort"
	"strings"
)

type unitAllocator struct {
	rskName  string
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
	rskName string,
	topics,
	realtime []string,
	topicsLast []topicLast,
	maxReloadingUnits int32,
	currentReloadingTopics []string,
	main *tipocav1.SinkGroupSpec,
	reload *tipocav1.SinkGroupSpec,
) *unitAllocator {
	return &unitAllocator{
		rskName:                rskName,
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

func k8sCompatibleName(name string) string {
	// satisfy k8s name regex
	// '[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*'
	return strings.ToLower(strings.ReplaceAll(name, "_", "-"))
}

func (u *unitAllocator) unitID(topic string) string {
	_, _, table := transformer.ParseTopic(topic)

	table = k8sCompatibleName(table)

	if len(table) > 20 {
		return table[:20]
	}

	return table
}

// for the reloading sinkGroup
func (u *unitAllocator) allocateReloadingUnits() {
	realtime := toMap(u.realtime)
	klog.V(3).Infof(
		"rsk/%s realtime: %v, max: %v",
		u.rskName,
		u.realtime,
		u.maxReloadingUnits,
	)

	klog.V(3).Infof(
		"rsk/%s currentUnits: %v %v",
		u.rskName,
		len(u.currentReloadingTopics),
		u.currentReloadingTopics,
	)

	reloadingTopics := make(map[string]bool)
	reloadingUnits := []deploymentUnit{}

	// don't shuffle the already reloading topics unless realtime
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
		reloadingTopics[topic] = true
		if len(reloadingUnits) >= u.maxReloadingUnits {
			break
		}
	}
	klog.V(3).Infof(
		"rsk/%s reloadingUnits(based on current): %v %v",
		u.rskName,
		len(reloadingUnits),
		reloadingUnits,
	)

	realtimeUnit := deploymentUnit{
		id:            "realtime",
		sinkGroupSpec: u.mainSinkGroupSpec,
		topics:        u.realtime,
	}

	if len(reloadingUnits) >= u.maxReloadingUnits {
		u.units = reloadingUnits
		if len(realtimeUnit.topics) > 0 {
			u.units = append(u.units, realtimeUnit)
		}
		klog.V(2).Infof("rsk/%s units: %v", u.rskName, len(u.units))
		return
	}

	topicsByLastAsc := sortTopicsByLastOffset(u.topicsLast)
	klog.V(3).Infof("rsk/%s sortByLast: %v", u.rskName, topicsByLastAsc)
	for _, topic := range topicsByLastAsc {
		_, ok := realtime[topic]
		if ok {
			continue
		}
		_, ok = reloadingTopics[topic]
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
		reloadingTopics[topic] = true
	}

	u.units = reloadingUnits
	if len(realtimeUnit.topics) > 0 {
		u.units = append(u.units, realtimeUnit)
	}
	klog.V(2).Infof("rsk/%s units: %v", u.rskName, len(u.units))
}
