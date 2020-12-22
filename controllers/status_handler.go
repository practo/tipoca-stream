package controllers

import (
	tipocav1 "github.com/practo/tipoca-stream/redshiftsink/api/v1"
)

type statusHandler struct {
	all            []string
	diff           []string
	currentVersion string
	desiredVersion string
	rsk            *tipocav1.RedshiftSink
}

func newStatusHandler(
	all, diff []string,
	c string, d string, rsk *tipocav1.RedshiftSink) *statusHandler {

	return &statusHandler{
		all:            all,
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

func (r *statusHandler) verify() bool {
	if r.currentVersion == "" {
		return true
	}

	total := len(r.reloading()) + len(r.realtime()) + len(r.released())
	if total != len(r.all) {
		return false
	}

	return true
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
	for _, topic := range r.all {

		// topic is released and the desired version is active now
		// and the redshift schema operations for it is also done properly
		_, ok := topicsReleased[topic]
		if ok {
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
			status[topic] = tipocav1.TopicMaskStatus{
				Version: r.desiredVersion,
				Phase:   tipocav1.MaskRealtime,
			}
			continue
		}

		// if the topic has not reached realtime and is still reloading
		_, ok = topicsReloading[topic]
		if ok {
			status[topic] = tipocav1.TopicMaskStatus{
				Version: r.desiredVersion,
				Phase:   tipocav1.MaskReloading,
			}
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
	for _, topic := range r.all {
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
	maskStatus := tipocav1.MaskStatus{
		CurrentMaskStatus: r.computerCurrentMaskStatus(
			toMap(topicsReleased),
			toMap(topicsRealtime),
			toMap(topicsReloading),
		),
		DesiredMaskStatus:  r.computeDesiredMaskStatus(),
		CurrentMaskVersion: &r.currentVersion,
		DesiredMaskVersion: &r.desiredVersion,
	}
	r.rsk.Status.MaskStatus = &maskStatus
}

func consumerGroupsBySinkGroup(
	rsk *tipocav1.RedshiftSink, sgName string) map[string]tipocav1.ConsumerGroup {
	empty := map[string]tipocav1.ConsumerGroup{}

	if rsk.Status.SinkGroupStatus == nil {
		return empty
	}

	groups, ok := rsk.Status.SinkGroupStatus[sgName]
	if ok {
		return groups
	}

	return empty
}

func (r *statusHandler) updateSinkGroupStatus(
	sgName,
	cgName,
	topic string,
	loaderPrefix string,
) {
	consumerGroups := consumerGroupsBySinkGroup(r.rsk, sgName)
	if len(consumerGroups) == 0 {
		r.rsk.Status.SinkGroupStatus[sgName] = map[string]tipocav1.ConsumerGroup{
			cgName: tipocav1.ConsumerGroup{
				KafkaLoaderTopicPrefix: loaderPrefix,
				KafkaTopicRegexes:      expandTopicsToRegex([]string{topic}),
			},
		}
		return
	}

	consumerGroup, ok := consumerGroups[cgName]
	if !ok {
		consumerGroups[cgName] = tipocav1.ConsumerGroup{
			KafkaLoaderTopicPrefix: loaderPrefix,
			KafkaTopicRegexes:      expandTopicsToRegex([]string{topic}),
		}
		return
	}

	consumerGroup.KafkaTopicRegexes += "," + fullMatchRegexForTopic(topic)
	consumerGroup.KafkaLoaderTopicPrefix = loaderPrefix
}
