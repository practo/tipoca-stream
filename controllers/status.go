package controllers

import (
	"fmt"
	klog "github.com/practo/klog/v2"
	tipocav1 "github.com/practo/tipoca-stream/redshiftsink/api/v1"
)

type status struct {
	rsk            *tipocav1.RedshiftSink
	currentVersion string
	desiredVersion string

	allTopics     []string
	diffTopics    []string
	released      []string
	realtime      []string
	reloading     []string
	reloadingDupe []string
}

type statusBuilder interface {
	setRedshiftSink(rsk *tipocav1.RedshiftSink) statusBuilder
	setCurrentVersion(version string) statusBuilder
	setDesiredVersion(version string) statusBuilder
	setAllTopics(topics []string) statusBuilder
	setDiffTopics(topics []string) statusBuilder
	computeReleased() statusBuilder
	setRealtime() statusBuilder
	computeReloading() statusBuilder
	computeReloadingDupe() statusBuilder
	build() *status
}

func newStatusBuilder() statusBuilder {
	return &buildStatus{}
}

type buildStatus struct {
	currentVersion string
	desiredVersion string
	rsk            *tipocav1.RedshiftSink
	allTopics      []string
	diffTopics     []string
	released       []string
	realtime       []string
	reloading      []string
	reloadingDupe  []string
}

func (sb *buildStatus) setRedshiftSink(rsk *tipocav1.RedshiftSink) statusBuilder {
	sb.rsk = rsk
	return sb
}

func (sb *buildStatus) setCurrentVersion(version string) statusBuilder {
	sb.currentVersion = version
	return sb
}

func (sb *buildStatus) setDesiredVersion(version string) statusBuilder {
	sb.desiredVersion = version
	return sb
}

func (sb *buildStatus) setAllTopics(topics []string) statusBuilder {
	sb.allTopics = topics
	return sb
}

func (sb *buildStatus) computeReleased() statusBuilder {
	released := currentTopicsByMaskStatus(
		sb.rsk, tipocav1.MaskActive, sb.desiredVersion,
	)

	// directly move topics not having mask diff to released
	if sb.rsk.Status.MaskStatus != nil &&
		sb.rsk.Status.MaskStatus.CurrentMaskStatus != nil {

		releasedMap := toMap(released)
		diffMap := toMap(sb.diffTopics)

		for topic, status := range sb.rsk.Status.MaskStatus.CurrentMaskStatus {
			_, ok := releasedMap[topic]
			if ok {
				continue
			}

			_, ok = diffMap[topic]
			if ok {
				continue
			}

			if status.Phase == tipocav1.MaskActive && status.Version != sb.desiredVersion {
				released = appendIfMissing(released, topic)
			}
		}
	}
	sb.released = released

	return sb
}

func (sb *buildStatus) setDiffTopics(topics []string) statusBuilder {
	sb.diffTopics = topics
	return sb
}

func (sb *buildStatus) setRealtime() statusBuilder {
	sb.realtime = currentTopicsByMaskStatus(
		sb.rsk, tipocav1.MaskRealtime, sb.desiredVersion,
	)
	return sb
}

func (sb *buildStatus) computeReloading() statusBuilder {
	if sb.rsk.Status.MaskStatus == nil ||
		sb.rsk.Status.MaskStatus.CurrentMaskStatus == nil {
		sb.reloading = sb.diffTopics
		return sb
	}

	reConstructingReloading := []string{}
	releasedMap := toMap(sb.released)
	realtimeMap := toMap(sb.realtime)
	for _, topic := range sb.diffTopics {
		_, ok := releasedMap[topic]
		if ok {
			continue
		}
		_, ok = realtimeMap[topic]
		if ok {
			continue
		}
		reConstructingReloading = append(reConstructingReloading, topic)
	}

	// this is required to add newly created topics as reloading always
	for _, topic := range sb.allTopics {
		if sb.rsk.Status.MaskStatus != nil &&
			sb.rsk.Status.MaskStatus.CurrentMaskStatus != nil {
			curr, ok := sb.rsk.Status.MaskStatus.CurrentMaskStatus[topic]
			if !ok {
				reConstructingReloading = appendIfMissing(reConstructingReloading, topic)
			} else {
				if curr.Phase == tipocav1.MaskReloading {
					reConstructingReloading = appendIfMissing(reConstructingReloading, topic)
				}
			}
		}
	}

	sb.reloading = reConstructingReloading
	return sb
}

func (sb *buildStatus) computeReloadingDupe() statusBuilder {
	reloadDupeTopics := []string{}

	for _, reloadingTopic := range sb.reloading {
		topicStatus := topicGroup(sb.rsk, reloadingTopic)
		// never dupe a topic which is releasing for the first time
		if topicStatus == nil {
			klog.V(3).Infof(
				"topic: %s is a new topic, it was never released before",
				reloadingTopic,
			)
			continue
		}
		reloadDupeTopics = append(reloadDupeTopics, reloadingTopic)
	}

	sb.reloadingDupe = reloadDupeTopics
	return sb
}

func (sb *buildStatus) build() *status {
	s := &status{
		rsk:            sb.rsk,
		currentVersion: sb.currentVersion,
		desiredVersion: sb.desiredVersion,
		allTopics:      sb.allTopics,
		diffTopics:     sb.diffTopics,
		released:       sb.released,
		realtime:       sb.realtime,
		reloading:      sb.reloading,
		reloadingDupe:  sb.reloadingDupe,
	}

	s.updateMaskStatus()
	return s
}

func currentTopicsByMaskStatus(rsk *tipocav1.RedshiftSink, phase tipocav1.MaskPhase, version string) []string {
	if rsk.Status.MaskStatus == nil ||
		rsk.Status.MaskStatus.CurrentMaskStatus == nil {
		return []string{}
	}
	topics := []string{}
	for topic, status := range rsk.Status.MaskStatus.CurrentMaskStatus {
		if status.Phase == phase && status.Version == version {
			topics = append(topics, topic)
		}
	}

	return topics
}

func currentTopicStatus(rsk *tipocav1.RedshiftSink, topic string) *tipocav1.TopicMaskStatus {
	if rsk.Status.MaskStatus != nil &&
		rsk.Status.MaskStatus.CurrentMaskStatus != nil {
		topicStatus, ok := rsk.Status.MaskStatus.CurrentMaskStatus[topic]
		if ok {
			return &topicStatus
		}
	}

	return nil
}

func (s *status) info() {
	rskName := fmt.Sprintf("rsk/%s", s.rsk.Name)
	klog.V(2).Infof("%s allTopics:  %d", rskName, len(s.allTopics))
	klog.V(2).Infof("%s diffTopics: %d", rskName, len(s.diffTopics))
	klog.V(2).Infof("%s released:   %d", rskName, len(s.released))
	klog.V(2).Infof("%s reloading:  %d %v", rskName, len(s.reloading), s.reloading)
	klog.V(2).Infof("%s rDupe:      %d %v", rskName, len(s.reloadingDupe), s.reloadingDupe)
	klog.V(2).Infof("%s realtime:   %d %v", rskName, len(s.realtime), s.realtime)
}

func (s *status) updateTopicsOnRelease(releasedTopic string) {
	s.released = appendIfMissing(s.released, releasedTopic)
	s.reloading = removeFromSlice(s.reloading, releasedTopic)
	s.reloadingDupe = removeFromSlice(s.reloadingDupe, releasedTopic)
	s.realtime = removeFromSlice(s.realtime, releasedTopic)
}

func (s *status) computerCurrentMaskStatus() map[string]tipocav1.TopicMaskStatus {
	topicsReleased := toMap(s.released)
	topicsRealtime := toMap(s.realtime)
	topicsReloading := toMap(s.reloading)
	status := make(map[string]tipocav1.TopicMaskStatus)

	for _, topic := range s.allTopics {
		// topic is released and the desired version is active now
		// and the redshift schema operations for it is also done properly
		_, ok := topicsReleased[topic]
		if ok {
			status[topic] = tipocav1.TopicMaskStatus{
				Version: s.desiredVersion,
				Phase:   tipocav1.MaskActive,
			}
			continue
		}

		// the topic is waiting to get released, it has reached realtime
		// release can happen any time soon, since it is one operation
		// per reconcile, the topics might be there in this state
		_, ok = topicsRealtime[topic]
		if ok {
			status[topic] = tipocav1.TopicMaskStatus{
				Version: s.desiredVersion,
				Phase:   tipocav1.MaskRealtime,
			}
			continue
		}

		// if the topic has not reached realtime and is still reloading
		_, ok = topicsReloading[topic]
		if ok {
			status[topic] = tipocav1.TopicMaskStatus{
				Version: s.desiredVersion,
				Phase:   tipocav1.MaskReloading,
			}
			continue
		}

		topicStatus := currentTopicStatus(s.rsk, topic)
		if topicStatus != nil {
			klog.V(5).Infof("%s status unchanged", topic)
			status[topic] = *topicStatus
			continue
		}

		// else for all the other topics it is considered they are active
		status[topic] = tipocav1.TopicMaskStatus{
			Version: s.desiredVersion,
			Phase:   tipocav1.MaskActive,
		}
	}

	return status
}

func (s *status) computeDesiredMaskStatus() map[string]tipocav1.TopicMaskStatus {
	status := make(map[string]tipocav1.TopicMaskStatus)
	for _, topic := range s.allTopics {
		status[topic] = tipocav1.TopicMaskStatus{
			Version: s.desiredVersion,
			Phase:   tipocav1.MaskActive,
		}
	}

	return status
}

func (s *status) updateMaskStatus() {
	currentVersion := &s.currentVersion
	if len(s.allTopics) == len(s.released) &&
		len(s.reloading) == 0 && len(s.realtime) == 0 {
		currentVersion = &s.desiredVersion
	}

	maskStatus := tipocav1.MaskStatus{
		CurrentMaskStatus:  s.computerCurrentMaskStatus(),
		DesiredMaskStatus:  s.computeDesiredMaskStatus(),
		CurrentMaskVersion: currentVersion,
		DesiredMaskVersion: &s.desiredVersion,
	}
	s.rsk.Status.MaskStatus = &maskStatus
}

func (s *status) updateTopicGroup(topic string) {
	klog.Infof("updating topic group: %s %+v", topic, s.rsk.Status)
	if s.rsk.Status.TopicGroup == nil {
		s.rsk.Status.TopicGroup = make(map[string]tipocav1.Group)
	}

	groupID := groupIDFromVersion(s.desiredVersion)
	prefix := loaderPrefixFromGroupID(
		s.rsk.Spec.KafkaLoaderTopicPrefix,
		groupID,
	)

	s.rsk.Status.TopicGroup[topic] = tipocav1.Group{
		LoaderTopicPrefix: prefix,
		ID:                groupID,
	}
}
