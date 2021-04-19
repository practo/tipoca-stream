package controllers

import (
	"context"
	"fmt"
	klog "github.com/practo/klog/v2"
	tipocav1 "github.com/practo/tipoca-stream/redshiftsink/api/v1"
	"reflect"
	client "sigs.k8s.io/controller-runtime/pkg/client"
	"strings"
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
	sortStringSlice(sb.allTopics)
	return sb
}

func (sb *buildStatus) setDiffTopics(topics []string) statusBuilder {
	sb.diffTopics = topics
	sortStringSlice(sb.diffTopics)
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

			// directly move topics not having mask diff to released
			if status.Phase == tipocav1.MaskActive && status.Version != sb.desiredVersion {
				released = appendIfMissing(released, topic)
			}
		}
	} else {
		klog.V(2).Infof("rsk/%s, Status empty, released=0 ", sb.rsk.Name)
	}
	sb.released = released
	sortStringSlice(sb.released)

	return sb
}

func (sb *buildStatus) setRealtime() statusBuilder {
	sb.realtime = currentTopicsByMaskStatus(
		sb.rsk, tipocav1.MaskRealtime, sb.desiredVersion,
	)
	sortStringSlice(sb.realtime)

	return sb
}

func (sb *buildStatus) computeReloading() statusBuilder {
	if sb.rsk.Status.MaskStatus == nil ||
		sb.rsk.Status.MaskStatus.CurrentMaskStatus == nil {
		klog.V(2).Infof("rsk/%s, Status empty, reloading=diffTopics ", sb.rsk.Name)
		sb.reloading = sb.diffTopics
		sortStringSlice(sb.reloading)
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
		} else {
			klog.V(2).Infof("rsk/%s, Status empty, newly created topics left", sb.rsk.Name)
		}
	}

	sb.reloading = reConstructingReloading
	sortStringSlice(sb.reloading)
	return sb
}

func (sb *buildStatus) computeReloadingDupe() statusBuilder {
	reloadDupeTopics := []string{}
	alreadyReleased := alreadyReleasedTopics(sb.rsk)

	for _, reloadingTopic := range sb.reloading {
		yes, ok := alreadyReleased[reloadingTopic]
		if ok && yes {
			reloadDupeTopics = append(reloadDupeTopics, reloadingTopic)
		} else {
			klog.V(4).Infof(
				"%s is a new topic (reload-dupe not required)",
				reloadingTopic,
			)
		}
	}

	sb.reloadingDupe = reloadDupeTopics
	sortStringSlice(sb.reloadingDupe)
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

// alreadyReleasedTopics finds if the topic was relaesed before or not, if it
// was released before the reloadingDupe is configured for it
func alreadyReleasedTopics(rsk *tipocav1.RedshiftSink) map[string]bool {
	topics := make(map[string]bool)
	if rsk.Status.MaskStatus == nil ||
		rsk.Status.MaskStatus.CurrentMaskStatus == nil {
		return topics
	}

	if rsk.Status.MaskStatus.CurrentMaskVersion == nil {
		return topics
	}

	for topic, status := range rsk.Status.MaskStatus.CurrentMaskStatus {
		if status.ReleasedVersion == nil {
			topics[topic] = false
		} else {
			topics[topic] = true
		}
	}

	return topics
}

func currentReleasedVersion(rsk *tipocav1.RedshiftSink, topic string) *string {
	if rsk.Status.MaskStatus == nil ||
		rsk.Status.MaskStatus.CurrentMaskStatus == nil {
		return nil
	}

	status, ok := rsk.Status.MaskStatus.CurrentMaskStatus[topic]
	if !ok {
		return nil
	}

	return status.ReleasedVersion
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

func (s *status) overwrite(copyStatus *status) {
	s.rsk = copyStatus.rsk
	s.currentVersion = copyStatus.currentVersion
	s.desiredVersion = copyStatus.desiredVersion
	s.allTopics = copyStatus.allTopics
	s.diffTopics = copyStatus.diffTopics
	s.released = copyStatus.released
	s.realtime = copyStatus.realtime
	s.reloading = copyStatus.reloading
	s.reloadingDupe = copyStatus.reloadingDupe
}

func (s *status) deepCopy() *status {
	copy := &status{
		rsk:            s.rsk.DeepCopy(),
		currentVersion: s.currentVersion,
		desiredVersion: s.desiredVersion,
		allTopics:      s.allTopics,
		diffTopics:     s.diffTopics,
		released:       s.released,
		realtime:       s.realtime,
		reloading:      s.reloading,
		reloadingDupe:  s.reloadingDupe,
	}

	return copy
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

// manyReloading checks the percentage of reloading topics of the total topics
func (s *status) reloadingRatio() float32 {
	reloading := len(s.reloading)
	if reloading == 0 {
		return 0
	}

	allTopics := len(s.allTopics)
	if allTopics == 0 {
		s.info()
		klog.Fatalf("All topics should not have been zero, exiting")
	}

	return float32(reloading) / float32(allTopics)
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
				Version:         s.desiredVersion,
				Phase:           tipocav1.MaskActive,
				ReleasedVersion: &s.desiredVersion,
			}
			continue
		}

		releasedVersion := currentReleasedVersion(s.rsk, topic)

		// the topic is waiting to get released, it has reached realtime
		// release can happen any time soon, since it is one operation
		// per reconcile, the topics might be there in this state
		_, ok = topicsRealtime[topic]
		if ok {
			status[topic] = tipocav1.TopicMaskStatus{
				Version:         s.desiredVersion,
				Phase:           tipocav1.MaskRealtime,
				ReleasedVersion: releasedVersion,
			}
			continue
		}

		// if the topic has not reached realtime and is still reloading
		_, ok = topicsReloading[topic]
		if ok {
			status[topic] = tipocav1.TopicMaskStatus{
				Version:         s.desiredVersion,
				Phase:           tipocav1.MaskReloading,
				ReleasedVersion: releasedVersion,
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
			Version:         s.desiredVersion,
			Phase:           tipocav1.MaskActive,
			ReleasedVersion: &s.desiredVersion,
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

func (s *status) notifyRelease(
	secret map[string]string,
	repo string,
	filePath string,
) {
	if len(s.allTopics) == len(s.released) &&
		len(s.reloading) == 0 && len(s.realtime) == 0 {

		if s.desiredVersion == "" {
			return
		}
		if s.currentVersion == s.desiredVersion {
			return
		}

		sha := s.desiredVersion
		if len(s.desiredVersion) >= 6 {
			sha = s.desiredVersion[:6]
		}
		message := fmt.Sprintf(
			"%s has %d tables live",
			s.rsk.Name,
			len(s.released),
		)
		klog.V(2).Infof("rsk/%s with %s", message, sha)
		notifier := makeNotifier(secret)
		if notifier == nil {
			return
		}
		releaseMessage := fmt.Sprintf(
			"%s with mask-version: <https://github.com/%s/blob/%s/%s | %s>",
			message,
			repo,
			s.desiredVersion,
			filePath,
			sha,
		)
		err := notifier.Notify(releaseMessage)
		if err != nil {
			klog.Errorf("release notification failed, err: %v", err)
		}
	}
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
	klog.V(5).Infof("updating topic group: %s %+v", topic, s.rsk.Status)

	groupID := groupIDFromTopicVersion(topic, s.desiredVersion)
	prefix := loaderPrefixFromVersion(s.rsk.Spec.KafkaLoaderTopicPrefix, s.desiredVersion)

	group := tipocav1.Group{
		LoaderCurrentOffset: nil, // Deprecated
		LoaderTopicPrefix:   prefix,
		ID:                  groupID,
	}
	updateTopicGroup(s.rsk, topic, group)
}

func (s *status) updateBatcherReloadingTopics(topics []string, batcherRealtime []string) {
	reloadingTopics := []string{}
	allRealtime := toMap(s.realtime)
	realtimeBatcher := toMap(batcherRealtime)

	for _, t := range topics {
		// remove topics which have become realtime (both batcher and loader)
		_, ok := allRealtime[t]
		if ok {
			continue

		}
		// remove topics which have become batcher realtime
		_, ok = realtimeBatcher[t]
		if ok {
			continue
		}

		reloadingTopics = append(reloadingTopics, t)
	}

	klog.V(2).Infof("rsk/%s currentReloading(batcher): %d %v", s.rsk.Name, len(reloadingTopics), reloadingTopics)
	s.rsk.Status.BatcherReloadingTopics = reloadingTopics
}

func (s *status) updateLoaderReloadingTopics(topics []string, loadersRealtime []string) {
	reloadingTopics := []string{}
	allRealtime := make(map[string]bool)
	realtimeLoader := toMap(loadersRealtime)

	// TODO: improve me :( realtime does not have loader topic info
	// topicGroup has info on it in status
	for _, t := range s.realtime {
		for _, reloadingTopic := range topics {
			if strings.HasSuffix(reloadingTopic, t) {
				allRealtime[reloadingTopic] = true
			}
		}
	}

	for _, t := range topics {
		// remove topics which have become realtime (both batcher and loader)
		_, ok := allRealtime[t]
		if ok {
			continue

		}
		// remove topics which have become loader realtime
		_, ok = realtimeLoader[t]
		if ok {
			continue
		}

		reloadingTopics = append(reloadingTopics, t)
	}

	klog.V(2).Infof("rsk/%s currentReloading(loader): %d %v", s.rsk.Name, len(reloadingTopics), reloadingTopics)
	s.rsk.Status.LoaderReloadingTopics = reloadingTopics
}

// fixMaskStatus fixes the status due to not proper updates to the mask status
func (s *status) fixMaskStatus() {
	klog.V(2).Infof("rsk/%s fixing mask status", s.rsk.Name)
	if len(s.diffTopics) == 0 && len(s.reloading) > 0 {
		if s.desiredVersion == s.currentVersion {
			klog.V(2).Infof("rsk/%s fixing mask status (desired=current)", s.rsk.Name)
			// fix
			s.released = s.allTopics
			s.realtime = []string{}
			s.reloading = []string{}

			maskStatus := tipocav1.MaskStatus{
				CurrentMaskStatus:  s.computerCurrentMaskStatus(),
				DesiredMaskStatus:  s.computeDesiredMaskStatus(),
				CurrentMaskVersion: &s.currentVersion,
				DesiredMaskVersion: &s.desiredVersion,
			}
			s.rsk.Status.MaskStatus = &maskStatus
			klog.V(2).Infof("rsk/%s fixed maskStatus: %+v", s.rsk.Name, maskStatus)
		}
	}
}

func (s *status) deleteLoaderTopicGroupCurrentOffset(topic string) {
	if s.rsk.Status.LoaderTopicGroupCurrentOffset == nil {
		return
	}

	groupID := groupIDFromTopicVersion(topic, s.desiredVersion)
	tg := fmt.Sprintf("%s-%s", topic, groupID)

	delete(s.rsk.Status.LoaderTopicGroupCurrentOffset, tg)
}

func updateTopicGroup(rsk *tipocav1.RedshiftSink, topic string, group tipocav1.Group) {
	if rsk.Status.TopicGroup == nil {
		rsk.Status.TopicGroup = make(map[string]tipocav1.Group)
	}

	rsk.Status.TopicGroup[topic] = group
}

func updateLoaderTopicGroupCurrentOffset(rsk *tipocav1.RedshiftSink, topic, groupID string, offset int64) {
	if rsk.Status.LoaderTopicGroupCurrentOffset == nil {
		rsk.Status.LoaderTopicGroupCurrentOffset = make(map[string]int64)
	}
	tg := fmt.Sprintf("%s-%s", topic, groupID)
	rsk.Status.LoaderTopicGroupCurrentOffset[tg] = offset
}

func loaderTopicGroupCurrentOffset(rsk *tipocav1.RedshiftSink, topic, groupID string) *int64 {
	if rsk.Status.LoaderTopicGroupCurrentOffset == nil {
		rsk.Status.LoaderTopicGroupCurrentOffset = make(map[string]int64)
		return nil
	}

	tg := fmt.Sprintf("%s-%s", topic, groupID)
	offset, ok := rsk.Status.LoaderTopicGroupCurrentOffset[tg]
	if ok {
		return &offset
	}

	return nil
}

func addDeadConsumerGroups(rsk *tipocav1.RedshiftSink, consumerGroupID string) {
	rsk.Status.DeadConsumerGroups = append(rsk.Status.DeadConsumerGroups, consumerGroupID)
}

// statusPatcher is used to update the status of rsk
type statusPatcher struct {
	client client.Client
	// allowMain determines if the main Reconcile defer func is allowed
	// to update status or not. Whenever a release happens, we expect
	// the status to be updated only by the patcher calls in the release func
	// this is to prevent overwriting of status from an old object
	allowMain bool
}

func (s *statusPatcher) Patch(ctx context.Context, original *tipocav1.RedshiftSink, new *tipocav1.RedshiftSink, caller string) error {
	if reflect.DeepEqual(original.Status, new.Status) {
		klog.V(4).Infof("rsk/%s caller:%s no patch", new.Name, caller)
		return nil
	}

	klog.V(4).Infof("rsk/%s patching status...", new.Name)
	if new.Status.MaskStatus != nil {
		klog.V(4).Infof("rsk/%s caller:%s patching, currentMaskStatus: %+v", new.Name, caller, new.Status.MaskStatus.CurrentMaskStatus)
	}
	return s.client.Status().Patch(
		ctx,
		new,
		client.MergeFrom(original),
	)
}
