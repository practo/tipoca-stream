package controllers

import (
	"fmt"

	klog "github.com/practo/klog/v2"
	tipocav1 "github.com/practo/tipoca-stream/redshiftsink/api/v1"
)

type statusHandler struct {
	allTopics      []string
	diff           []string
	currentVersion string
	desiredVersion string
	rsk            *tipocav1.RedshiftSink
}

func newStatusHandler(
	allTopics, diff []string,
	c string, d string, rsk *tipocav1.RedshiftSink) *statusHandler {

	return &statusHandler{
		allTopics:      allTopics,
		diff:           diff,
		currentVersion: c,
		desiredVersion: d,
		rsk:            rsk,
	}

}

func (r *statusHandler) reloading() []string {
	if r.rsk.Status.MaskStatus == nil ||
		r.rsk.Status.MaskStatus.CurrentMaskStatus == nil {
		return r.diff
	}
	return r.getTopics(tipocav1.MaskReloading, r.desiredVersion)
}

func (r *statusHandler) realtime() []string {
	return r.getTopics(tipocav1.MaskRealtime, r.desiredVersion)
}

func (r *statusHandler) released() []string {
	return r.getTopics(tipocav1.MaskActive, r.desiredVersion)
}

func (r *statusHandler) currentTopicStatus(
	topic string) *tipocav1.TopicMaskStatus {

	if r.rsk.Status.MaskStatus != nil &&
		r.rsk.Status.MaskStatus.CurrentMaskStatus != nil {
		topicStatus, ok := r.rsk.Status.MaskStatus.CurrentMaskStatus[topic]
		if ok {
			return &topicStatus
		}
	}

	return nil
}

func (r *statusHandler) reloadingDupe() []string {
	reloadDupeTopics := []string{}
	reloading := r.reloading()

	for _, reloadingTopic := range reloading {
		topicStatus := r.currentTopicStatus(reloadingTopic)
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

	return reloadDupeTopics
}

func removeReleased(from []string, released map[string]bool) []string {
	topics := []string{}
	for _, topic := range from {
		_, ok := released[topic]
		if !ok {
			topics = append(topics, topic)
		}
	}

	return topics
}

func (r *statusHandler) getTopics(
	phase tipocav1.MaskPhase, version string) []string {
	if r.rsk.Status.MaskStatus == nil ||
		r.rsk.Status.MaskStatus.CurrentMaskStatus == nil {
		return []string{}
	}

	topics := []string{}

	for topic, status := range r.rsk.Status.MaskStatus.CurrentMaskStatus {
		if status.Phase == phase && status.Version == version {
			topics = append(topics, topic)
		}
	}

	return topics
}

// computerCurrentMaskStatus updates the status for topics
// reload -> releasing -> released(active)
func (r *statusHandler) computerCurrentMaskStatus(
	topicsReleased map[string]bool,
	topicsRealtime map[string]bool,
	topicsReloading map[string]bool,
) map[string]tipocav1.TopicMaskStatus {
	status := make(map[string]tipocav1.TopicMaskStatus)
	for _, topic := range r.allTopics {

		// topic is released and the desired version is active now
		// and the redshift schema operations for it is also done properly
		_, ok := topicsReleased[topic]
		if ok {
			klog.V(5).Infof("%s/%s marked active", topic, r.desiredVersion)
			status[topic] = tipocav1.TopicMaskStatus{
				Version: r.desiredVersion,
				Phase:   tipocav1.MaskActive,
			}
			continue
		}

		// the topic is waiting to get released, it has reached realtime
		// release can happen any time soon, since it is one operation
		// per reconcile, the topics might be there in this state
		_, ok = topicsRealtime[topic]
		if ok {
			klog.V(5).Infof("%s/%s marked realtime", topic, r.desiredVersion)
			status[topic] = tipocav1.TopicMaskStatus{
				Version: r.desiredVersion,
				Phase:   tipocav1.MaskRealtime,
			}
			continue
		}

		// if the topic has not reached realtime and is still reloading
		_, ok = topicsReloading[topic]
		if ok {
			klog.V(5).Infof("%s/%s marked reloading", topic, r.desiredVersion)
			status[topic] = tipocav1.TopicMaskStatus{
				Version: r.desiredVersion,
				Phase:   tipocav1.MaskReloading,
			}
			continue
		}

		topicStatus := r.currentTopicStatus(topic)
		if topicStatus != nil {
			klog.V(5).Infof("%s status unchanged", topic)
			status[topic] = *topicStatus
			continue
		}

		// else for all the other topics it is considered they are active
		status[topic] = tipocav1.TopicMaskStatus{
			Version: r.desiredVersion,
			Phase:   tipocav1.MaskActive,
		}
	}

	return status
}

func (r *statusHandler) computeDesiredMaskStatus() map[string]tipocav1.TopicMaskStatus {
	status := make(map[string]tipocav1.TopicMaskStatus)
	for _, topic := range r.allTopics {
		status[topic] = tipocav1.TopicMaskStatus{
			Version: r.desiredVersion,
			Phase:   tipocav1.MaskActive,
		}
	}

	return status
}

func (r *statusHandler) updateMaskStatus(
	topicsReleased []string,
	topicsRealtime []string,
	topicsReloading []string,
) {
	currentVersion := &r.currentVersion
	if len(r.allTopics) == len(topicsReleased) &&
		len(topicsReloading) == 0 && len(topicsRealtime) == 0 {

		currentVersion = &r.desiredVersion
	}

	maskStatus := tipocav1.MaskStatus{
		CurrentMaskStatus: r.computerCurrentMaskStatus(
			toMap(topicsReleased),
			toMap(topicsRealtime),
			toMap(topicsReloading),
		),
		DesiredMaskStatus:  r.computeDesiredMaskStatus(),
		CurrentMaskVersion: currentVersion,
		DesiredMaskVersion: &r.desiredVersion,
	}
	r.rsk.Status.MaskStatus = &maskStatus
}

func groupIDFromVersion(version string) string {
	groupID := version
	if len(version) >= 6 {
		groupID = groupID[:6]
	}

	return groupID
}

func loaderPrefixFromGroupID(prefix string, version string) string {
	return prefix + version + "-"
}

func (r *statusHandler) initTopicGroup() {
	if r.rsk.Status.TopicGroup == nil {
		r.rsk.Status.TopicGroup = make(map[string]tipocav1.Group)
	}
	for _, topic := range r.allTopics {
		_, ok := r.rsk.Status.TopicGroup[topic]
		if ok {
			continue
		}

		groupID := groupIDFromVersion(r.desiredVersion)
		prefix := loaderPrefixFromGroupID(
			r.rsk.Spec.KafkaLoaderTopicPrefix,
			groupID,
		)

		r.rsk.Status.TopicGroup[topic] = tipocav1.Group{
			LoaderTopicPrefix: prefix,
			ID:                groupID,
		}
	}
}

func (r *statusHandler) updateTopicGroup(topic string) {
	if r.rsk.Status.TopicGroup == nil {
		r.rsk.Status.TopicGroup = make(map[string]tipocav1.Group)
	}

	groupID := groupIDFromVersion(r.desiredVersion)
	prefix := loaderPrefixFromGroupID(
		r.rsk.Spec.KafkaLoaderTopicPrefix,
		groupID,
	)

	r.rsk.Status.TopicGroup[topic] = tipocav1.Group{
		LoaderTopicPrefix: prefix,
		ID:                groupID,
	}
}

type consumerGroup struct {
	topics            []string
	loaderTopicPrefix string
}

func computeConsumerGroups(
	topicGroups map[string]tipocav1.Group,
	topics []string,
) (
	map[string]consumerGroup,
	error,
) {
	consumerGroups := make(map[string]consumerGroup)
	for _, topic := range topics {
		topicGroup, ok := topicGroups[topic]
		if !ok {
			return nil, fmt.Errorf(
				"Group info missing for topic: %s in Status", topic)
		}

		existingGroup, ok := consumerGroups[topicGroup.ID]
		if !ok {
			consumerGroups[topicGroup.ID] = consumerGroup{
				topics:            []string{topic},
				loaderTopicPrefix: topicGroup.LoaderTopicPrefix,
			}
		} else {
			if existingGroup.loaderTopicPrefix != topicGroup.LoaderTopicPrefix {
				return nil, fmt.Errorf(
					"Mismatch in loaderTopicPrefix in status: %v for topic: %v",
					existingGroup,
					topic,
				)
			}
			existingGroup.topics = append(existingGroup.topics, topic)
			consumerGroups[topicGroup.ID] = existingGroup
		}
	}

	return consumerGroups, nil
}
