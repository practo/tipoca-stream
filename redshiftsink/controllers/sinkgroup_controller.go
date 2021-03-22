package controllers

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	klog "github.com/practo/klog/v2"
	tipocav1 "github.com/practo/tipoca-stream/redshiftsink/api/v1"
	kafka "github.com/practo/tipoca-stream/redshiftsink/pkg/kafka"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	client "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	AllSinkGroup        = "all"
	MainSinkGroup       = "main"
	ReloadSinkGroup     = "reload"
	ReloadDupeSinkGroup = "reload-dupe"

	DefaultMaxBatcherLag = int64(100)
	DefautMaxLoaderLag   = int64(10)

	ReloadTableSuffix = "_ts_adx_reload"
)

var DefaultMaxBatcherTopics int = 30

type sinkGroupInterface interface {
	reconcile(ctx context.Context) (ctrl.Result, ReconcilerEvent, error)
	realtimeTopics(currentRealtime []string, watcher kafka.Watcher, cache *sync.Map) []string
}

type Deployment interface {
	Name() string
	Namespace() string
	Config() *corev1.ConfigMap
	Deployment() *appsv1.Deployment
	UpdateConfig(current *corev1.ConfigMap) bool
	UpdateDeployment(current *appsv1.Deployment) bool
}

type sinkGroup struct {
	rsk         *tipocav1.RedshiftSink
	client      client.Client
	scheme      *runtime.Scheme
	sgType      string
	topics      []string
	topicGroups map[string]tipocav1.Group

	batchers []Deployment
	loaders  []Deployment
}

type sinkGroupBuilder interface {
	setRedshiftSink(rsk *tipocav1.RedshiftSink) sinkGroupBuilder
	setClient(client client.Client) sinkGroupBuilder
	setScheme(scheme *runtime.Scheme) sinkGroupBuilder
	setType(sgType string) sinkGroupBuilder
	setTopics(topics []string) sinkGroupBuilder
	setMaskVersion(version string) sinkGroupBuilder
	setTopicGroups() sinkGroupBuilder

	buildBatchers(secret map[string]string, defaultImage, defaultKafkaVersion string, tlsConfig *kafka.TLSConfig) sinkGroupBuilder
	buildLoaders(secret map[string]string, defaultImage, tableSuffix string, defaultKafkaVersion string, tlsConfig *kafka.TLSConfig, defaultMaxOpenConns int, defaultMaxIdleConns int) sinkGroupBuilder

	build() *sinkGroup
}

func newSinkGroupBuilder() sinkGroupBuilder {
	return &buildSinkGroup{}
}

type buildSinkGroup struct {
	rsk         *tipocav1.RedshiftSink
	client      client.Client
	scheme      *runtime.Scheme
	sgType      string
	topics      []string
	topicGroups map[string]tipocav1.Group
	maskVersion string

	batchers []Deployment
	loaders  []Deployment
}

func (sb *buildSinkGroup) setRedshiftSink(rsk *tipocav1.RedshiftSink) sinkGroupBuilder {
	sb.rsk = rsk
	return sb
}

func (sb *buildSinkGroup) setClient(client client.Client) sinkGroupBuilder {
	sb.client = client
	return sb
}

func (sb *buildSinkGroup) setScheme(scheme *runtime.Scheme) sinkGroupBuilder {
	sb.scheme = scheme
	return sb
}

func (sb *buildSinkGroup) setType(sgType string) sinkGroupBuilder {
	sb.sgType = sgType
	return sb
}

func (sb *buildSinkGroup) setTopics(topics []string) sinkGroupBuilder {
	sb.topics = topics
	return sb
}

func (sb *buildSinkGroup) setMaskVersion(maskVersion string) sinkGroupBuilder {
	sb.maskVersion = maskVersion
	return sb
}

func (sb *buildSinkGroup) setTopicGroups() sinkGroupBuilder {
	sb.topicGroups = topicGroupBySinkGroup(
		sb.rsk,
		sb.sgType,
		sb.topics,
		sb.maskVersion,
		sb.rsk.Spec.KafkaLoaderTopicPrefix,
	)
	return sb
}

type deploymentUnit struct {
	id     string
	topics []string
}

// assignDeploymentUnits allocates the total topics into groups of deployments
// based on the specification. groups are called as deploymentUnit
func assignDeploymentUnits(allTopics []string, maxTopics int) []deploymentUnit {
	if len(allTopics) <= maxTopics {
		return []deploymentUnit{
			deploymentUnit{
				id:     "0",
				topics: allTopics,
			},
		}
	}

	units := []deploymentUnit{}
	totalUnits := int(math.Ceil(float64(len(allTopics)) / float64(maxTopics)))
	startIndex := 0
	lastIndex := maxTopics
	for id := 0; id < totalUnits; id++ {
		topics := allTopics[startIndex:lastIndex]
		startIndex = lastIndex
		if lastIndex+maxTopics >= len(allTopics) {
			lastIndex = len(allTopics)
		} else {
			lastIndex = lastIndex + maxTopics
		}
		units = append(units, deploymentUnit{
			id:     fmt.Sprintf("%d", id),
			topics: topics,
		})
	}

	return units
}

func (sb *buildSinkGroup) buildBatchers(
	secret map[string]string,
	defaultImage string,
	defaultKafkaVersion string,
	tlsConfig *kafka.TLSConfig,
) sinkGroupBuilder {
	batchers := []Deployment{}
	if sb.rsk.Spec.Batcher.SinkGroup != nil {
		sinkGroupSpec := applyBatcherSinkGroupDefaults(
			sb.rsk,
			sb.sgType,
			defaultImage,
		)
		units := assignDeploymentUnits(
			sb.topics,
			*sinkGroupSpec.DeploymentUnit.MaxTopics,
		)
		for _, unit := range units {
			consumerGroups, err := computeConsumerGroups(
				sb.topicGroups, unit.topics)
			if err != nil {
				klog.Fatalf(
					"Error computing consumer group from status, err: %v", err)
			}
			batcher, err := NewBatcher(
				batcherName(sb.rsk.Name, sb.sgType, unit.id),
				sb.rsk,
				sb.maskVersion,
				secret,
				sb.sgType,
				sinkGroupSpec,
				consumerGroups,
				defaultImage,
				defaultKafkaVersion,
				tlsConfig,
			)
			if err != nil {
				klog.Fatalf("Error making batcher: %v", err)
			}
			batchers = append(batchers, batcher)
		}
	} else { // Deprecated
		consumerGroups, err := computeConsumerGroups(sb.topicGroups, sb.topics)
		if err != nil {
			klog.Fatalf(
				"Error computing consumer group from status, err: %v", err)
		}
		batcher, err := NewBatcher(
			batcherName(sb.rsk.Name, sb.sgType, ""),
			sb.rsk,
			sb.maskVersion,
			secret,
			sb.sgType,
			nil,
			consumerGroups,
			defaultImage,
			defaultKafkaVersion,
			tlsConfig,
		)
		if err != nil {
			klog.Fatalf("Error making batcher: %v", err)
		}
		batchers = append(batchers, batcher)
	}

	sb.batchers = batchers
	return sb
}

func (sb *buildSinkGroup) buildLoaders(
	secret map[string]string,
	defaultImage string,
	tableSuffix string,
	defaultKafkaVersion string,
	tlsConfig *kafka.TLSConfig,
	defaultMaxOpenConns int,
	defaultMaxIdleConns int,
) sinkGroupBuilder {
	consumerGroups, err := computeConsumerGroups(sb.topicGroups, sb.topics)
	if err != nil {
		klog.Fatalf("Error computing consumer group from status, err: %v", err)
	}
	loader, err := NewLoader(
		loaderName(sb.rsk.Name, sb.sgType),
		sb.rsk,
		tableSuffix,
		secret,
		sb.sgType,
		consumerGroups,
		defaultImage,
		defaultKafkaVersion,
		tlsConfig,
		defaultMaxOpenConns,
		defaultMaxIdleConns,
	)
	if err != nil {
		klog.Fatalf("Error making loader: %v", err)
	}
	sb.loaders = []Deployment{loader}
	return sb
}

func (sb *buildSinkGroup) build() *sinkGroup {
	return &sinkGroup{
		rsk:         sb.rsk,
		client:      sb.client,
		scheme:      sb.scheme,
		sgType:      sb.sgType,
		topics:      sb.topics,
		topicGroups: sb.topicGroups,

		batchers: sb.batchers,
		loaders:  sb.loaders,
	}
}

func topicGroup(rsk *tipocav1.RedshiftSink, topic string) *tipocav1.Group {
	if rsk.Status.TopicGroup == nil {
		rsk.Status.TopicGroup = make(map[string]tipocav1.Group)
		return nil
	}
	group, ok := rsk.Status.TopicGroup[topic]
	if ok {
		return &group
	}

	return nil
}

func topicGroupBySinkGroup(
	rsk *tipocav1.RedshiftSink,
	sinkGroupName string,
	topics []string,
	version string,
	prefix string,
) map[string]tipocav1.Group {

	groups := make(map[string]tipocav1.Group)
	groupID := groupIDFromVersion(version)

	switch sinkGroupName {
	case ReloadSinkGroup:
		for _, topic := range topics {
			groups[topic] = tipocav1.Group{
				ID: groupID,
				LoaderTopicPrefix: loaderPrefixFromGroupID(
					prefix,
					groupID,
				),
			}
		}
		return groups
	case ReloadDupeSinkGroup:
		for _, topic := range topics {
			group := topicGroup(rsk, topic)
			if group == nil {
				// do not sink topics which sinking for first time
				continue
			} else {
				groups[topic] = *group
			}
		}
		return groups
	case MainSinkGroup:
		for _, topic := range topics {
			group := topicGroup(rsk, topic)
			if group == nil {
				groups[topic] = tipocav1.Group{
					ID: groupID,
					LoaderTopicPrefix: loaderPrefixFromGroupID(
						prefix,
						groupID,
					),
				}
			} else {
				groups[topic] = *group
			}
		}
		return groups
	}
	return groups
}

func consumerGroupID(rskName, rskNamespace, groupID string, suffix string) string {
	return rskNamespace + "-" + rskName + "-" + groupID + suffix
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
			klog.Warningf(
				"Assuming first time sink for:%s, ignoring topic", topic,
			)
			continue
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

func (s *sinkGroup) reconcileConfigMap(
	ctx context.Context,
	d Deployment,
) (
	ReconcilerEvent,
	error,
) {
	config := d.Config()
	_, exists, err := getConfigMap(
		ctx,
		s.client,
		config.Name,
		d.Namespace(),
	)
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, nil
	}

	err = ctrlutil.SetOwnerReference(s.rsk, config, s.scheme)
	if err != nil {
		return nil, err
	}

	klog.V(2).Infof("Creating configMap: %v", config.Name)
	event, err := createConfigMap(ctx, s.client, config, s.rsk)
	if err != nil {
		return nil, err
	}

	return event, nil
}

func (s *sinkGroup) reconcileDeployment(
	ctx context.Context,
	d Deployment,
) (
	ReconcilerEvent,
	error,
) {
	deployment := d.Deployment()
	current, exists, err := getDeployment(
		ctx,
		s.client,
		deployment.Name,
		d.Namespace(),
	)
	if err != nil {
		return nil, err
	}
	if exists {
		if !d.UpdateDeployment(current) {
			return nil, nil
		}

		err = ctrlutil.SetOwnerReference(s.rsk, deployment, s.scheme)
		if err != nil {
			return nil, err
		}

		klog.V(2).Infof("Updating deployment: %v", deployment.Name)
		event, err := updateDeployment(ctx, s.client, deployment, s.rsk)
		if err != nil {
			return nil, err
		}
		if event != nil {
			return event, nil
		}
	}

	err = ctrlutil.SetOwnerReference(s.rsk, deployment, s.scheme)
	if err != nil {
		return nil, err
	}

	// create new deployment pointing to new config map
	klog.V(2).Infof("Creating deployment: %v", deployment.Name)
	event, err := createDeployment(ctx, s.client, deployment, s.rsk)
	if err != nil {
		return nil, err
	}

	return event, nil
}

func (s *sinkGroup) cleanup(
	ctx context.Context,
	labelInstance string,
	neededDeployments map[string]bool,
	neededConfigMaps map[string]bool,
) (
	ReconcilerEvent,
	error,
) {
	klog.V(3).Infof("Current active deployments, needed: %+v", neededDeployments)
	// query all deployment for the sinkgroup
	deploymentList, err := listDeployments(
		ctx,
		s.client,
		labelInstance,
		s.sgType,
		s.rsk.Namespace,
		s.rsk.Name,
	)
	if err != nil {
		return nil, err
	}
	for _, deploy := range deploymentList.Items {
		klog.V(3).Infof("Cleanup suspect deployment: %v", deploy.Name)
		labelValue, ok := deploy.Labels[InstanceName]
		if !ok {
			continue
		}
		_, ok = neededDeployments[labelValue]
		if !ok {
			klog.V(3).Infof("Cleanup deployment: %v", labelValue)
			event, err := deleteDeployment(ctx, s.client, &deploy, s.rsk)
			if err != nil {
				return nil, err
			}
			if event != nil {
				return event, nil
			}
		}
	}

	klog.V(3).Infof("Current active configMaps, needed: %+v", neededConfigMaps)
	// query all configmaps for the sinkgroup
	configMapList, err := listConfigMaps(
		ctx,
		s.client,
		labelInstance,
		s.sgType,
		s.rsk.Namespace,
		s.rsk.Name,
	)
	if err != nil {
		return nil, err
	}

	for _, config := range configMapList.Items {
		klog.V(3).Infof("Cleanup configmap suspect cm: %v", config.Name)
		labelValue, ok := config.Labels[InstanceName]
		if !ok {
			continue
		}
		_, ok = neededConfigMaps[labelValue]
		if !ok {
			klog.V(2).Infof("Cleanup configmap: %s", labelValue)
			event, err := deleteConfigMap(ctx, s.client, &config, s.rsk)
			if err != nil {
				return nil, err
			}
			if event != nil {
				return event, nil
			}
		}
	}

	return nil, nil
}

func (s *sinkGroup) reconcileBatcher(
	ctx context.Context,
	d Deployment,
) (
	ReconcilerEvent,
	error,
) {
	// reconcile batcher configMap
	event, err := s.reconcileConfigMap(ctx, d)
	if err != nil {
		return nil, fmt.Errorf("Error reconciling batcher configMap, %v", err)
	}
	if event != nil {
		return event, nil
	}

	// reconcile batcher deployment
	event, err = s.reconcileDeployment(ctx, d)
	if err != nil {
		return nil, fmt.Errorf("Error reconciling batcher deployment, %v", err)
	}
	if event != nil {
		return event, nil
	}

	return nil, nil
}

func (s *sinkGroup) reconcileBatchers(
	ctx context.Context,
	deployments []Deployment,
) (
	ReconcilerEvent,
	error,
) {
	// cleanup the ones which should be dead before creating new
	var neededDeployments, neededConfigMaps []string
	for _, d := range deployments {
		neededDeployments = append(neededDeployments, d.Name())
		neededConfigMaps = append(neededConfigMaps, d.Name())
	}
	event, err := s.cleanup(
		ctx,
		BatcherLabelInstance,
		toMap(neededDeployments),
		toMap(neededConfigMaps),
	)
	if err != nil {
		return nil, err
	}
	if event != nil {
		return event, nil
	}

	// create or update
	for _, d := range deployments {
		event, err := s.reconcileBatcher(ctx, d)
		if err != nil {
			return nil, err
		}
		if event != nil {
			return event, nil
		}
	}

	return nil, nil
}

func (s *sinkGroup) reconcileLoader(
	ctx context.Context,
	d Deployment,
) (
	ReconcilerEvent,
	error,
) {
	event, err := s.reconcileConfigMap(ctx, d)
	if err != nil {
		return nil, fmt.Errorf("Error reconciling loader configMap, %v", err)
	}
	if event != nil {
		return event, nil
	}

	// reconcile loader deployment
	event, err = s.reconcileDeployment(ctx, d)
	if err != nil {
		return nil, fmt.Errorf("Error reconciling loader deployment, %v", err)
	}
	if event != nil {
		return event, nil
	}

	return nil, nil
}

func (s *sinkGroup) reconcileLoaders(
	ctx context.Context,
	deployments []Deployment,
) (
	ReconcilerEvent,
	error,
) {
	// cleanup the ones which should be dead before creating new
	var neededDeployments, neededConfigMaps []string
	for _, d := range deployments {
		neededDeployments = append(neededDeployments, d.Name())
		neededConfigMaps = append(neededConfigMaps, d.Name())
	}
	event, err := s.cleanup(
		ctx,
		LoaderLabelInstance,
		toMap(neededDeployments),
		toMap(neededConfigMaps),
	)
	if err != nil {
		return nil, err
	}
	if event != nil {
		return event, nil
	}

	// create or update
	for _, d := range deployments {
		event, err := s.reconcileLoader(ctx, d)
		if err != nil {
			return nil, err
		}
		if event != nil {
			return event, nil
		}
	}

	return nil, nil
}

func maxLag(rsk *tipocav1.RedshiftSink, topic string) (int64, int64) {
	var maxBatcherLag, maxLoaderLag int64
	if rsk.Spec.ReleaseCondition == nil {
		maxBatcherLag = DefaultMaxBatcherLag
		maxLoaderLag = DefautMaxLoaderLag
	} else {
		if rsk.Spec.ReleaseCondition.MaxBatcherLag != nil {
			maxBatcherLag = *rsk.Spec.ReleaseCondition.MaxBatcherLag
		}
		if rsk.Spec.ReleaseCondition.MaxLoaderLag != nil {
			maxLoaderLag = *rsk.Spec.ReleaseCondition.MaxLoaderLag
		}
		if rsk.Spec.TopicReleaseCondition != nil {
			d, ok := rsk.Spec.TopicReleaseCondition[topic]
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

func (s *sinkGroup) lagBelowThreshold(
	topic string,
	batcherLag,
	loaderLag,
	maxBatcherLag,
	maxLoaderLag int64,
) bool {
	// if batcherLag <= maxBatcherLag && loaderLag == -1 {
	// 	// TODO: this might lead to false positives, solve it
	// 	// but without it some very low throughput topics wont go live.
	// 	// may need to plugin prometheus time series data for analysis later
	// 	// to solve it
	// 	klog.Warningf("topic: %s assumed to have reached realtime as batcherLag<=threshold and loaderLag=-1 (consumer group not active)", topic)
	// 	return true
	// }

	klog.V(4).Infof("topic: %s lag=%v", topic, batcherLag)
	klog.V(4).Infof("topic: %s lag=%v", topic, loaderLag)

	if batcherLag <= maxBatcherLag &&
		loaderLag <= maxLoaderLag {

		return true
	}

	return false
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

type kafkaRealtimeCache struct {
	lastCacheRefresh *int64
	realtime         bool
}

func (s *sinkGroup) topicRealtime(
	watcher kafka.Watcher,
	topic string,
	cache *sync.Map,
	allTopics map[string]bool,
) (
	bool, *int64, error,
) {
	// use cache to prevent calls to kafka
	var realtimeCache kafkaRealtimeCache
	cacheLoaded, ok := cache.Load(topic)
	if ok {
		realtimeCache = cacheLoaded.(kafkaRealtimeCache)
		// 600 to 840 seconds
		validitySeconds := rand.Intn(240) + 300
		klog.V(5).Infof("rsk/%s validity seconds: %v topic: %s", s.rsk.Name, validitySeconds, topic)
		if cacheValid(time.Second*time.Duration(validitySeconds), realtimeCache.lastCacheRefresh) {
			klog.V(4).Infof("rsk/%s (realtime cache hit) topic: %s", s.rsk.Name, topic)
			if realtimeCache.realtime {
				return true, realtimeCache.lastCacheRefresh, nil
			}
			return false, realtimeCache.lastCacheRefresh, nil
		}
	}

	// new cache refresh time so that topics are only checked after an interval
	// reduces the request to Kafka by big factor
	now := time.Now().UnixNano()
	maxBatcherLag, maxLoaderLag := maxLag(s.rsk, topic)

	klog.V(2).Infof("rsk/%s (fetching realtime stats) topic: %s", s.rsk.Name, topic)
	group, ok := s.topicGroups[topic]
	if !ok {
		return false, &now, fmt.Errorf("consumerGroupID not found for %s", topic)
	}

	// batcher's lag analysis
	batcherLastOffset, err := watcher.LastOffset(topic, 0)
	if err != nil {
		return false, &now, fmt.Errorf("Error getting current offset for %s", topic)
	}
	klog.V(4).Infof("%s, lastOffset=%v", topic, batcherLastOffset)

	// This won't work for topics which have lastOffset less than lag
	// klog.V(2).Infof("%s, lastOffset=%v", topic, batcherLastOffset)
	// if batcherLastOffset < maxBatcherLag {
	// 	klog.V(2).Infof("%s, lastOffset < %v, not realtime", topic, maxBatcherLag)
	// 	return false, &now, nil
	// }

	batcherCGID := consumerGroupID(s.rsk.Name, s.rsk.Namespace, group.ID, "-batcher")
	batcherCurrentOffset, err := watcher.CurrentOffset(
		batcherCGID,
		topic,
		0,
	)
	if err != nil {
		return false, &now, err
	}
	klog.V(4).Infof("%s, currentOffset=%v", topic, batcherCurrentOffset)
	if batcherCurrentOffset == -1 {
		klog.V(2).Infof("%s, batcher cg 404, not realtime", topic)
		return false, &now, nil
	}

	// loader's lag analysis
	loaderTopic := s.rsk.Spec.KafkaLoaderTopicPrefix + group.ID + "-" + topic
	_, ok = allTopics[loaderTopic]
	if !ok {
		klog.V(2).Infof("%s topic 404, not realtime.", loaderTopic)
		return false, &now, nil
	}
	loaderLastOffset, err := watcher.LastOffset(loaderTopic, 0)
	if err != nil {
		return false, &now, fmt.Errorf("Error getting last offset for %s", loaderTopic)
	}
	klog.V(4).Infof("%s, lastOffset=%v", loaderTopic, loaderLastOffset)

	// This won't work for topics which have lastOffset less than lag
	// if loaderLastOffset < maxLoaderLag {
	// 	klog.V(2).Infof("%s, lastOffset < %v, not realtime", loaderTopic, maxLoaderLag)
	// 	return false, &now, nil
	// }
	loaderCGID := consumerGroupID(s.rsk.Name, s.rsk.Namespace, group.ID, "-loader")
	loaderCurrentOffset, err := watcher.CurrentOffset(
		loaderCGID,
		loaderTopic,
		0,
	)
	if err != nil {
		return false, &now, err
	}
	klog.V(4).Infof("%s, currentOffset=%v (queried)", loaderTopic, loaderCurrentOffset)
	if loaderCurrentOffset == -1 {
		// CurrentOffset can be -1 in two cases
		// 1. When the Consumer Group was never created in that case we return and consider the topic not realtime
		// 2. When the Consumer Group had processed before but now is showing -1 currentOffset as it is inactive due to less throughput,
		//    On such a scenario, we consider it realtime.
		//    we find this case by saving the currentOffset for the loader topcics in RedshiftSink Topic Group Status
		if group.LoaderCurrentOffset == nil {
			klog.V(2).Infof("%s, loader cg 404, not realtime", loaderTopic)
			return false, &now, nil
		}
		klog.V(2).Infof("%s, currentOffset=%v (old), cg 404, try realtime", loaderTopic, *group.LoaderCurrentOffset)
		// give the topic the opportunity to release based on its last found currentOffset
		loaderCurrentOffset = *group.LoaderCurrentOffset
	} else {
		group.LoaderCurrentOffset = &loaderCurrentOffset
		// updates the new queried lodaer offset
		klog.V(4).Infof("%s, cg found", loaderTopic)
		updateTopicGroup(s.rsk, topic, group)
	}
	klog.V(2).Infof("%s, currentOffset=%v, checking realtime", loaderTopic, *group.LoaderCurrentOffset)

	// check realtime
	if s.lagBelowThreshold(
		topic,
		batcherLastOffset-batcherCurrentOffset, // batcher lag
		loaderLastOffset-loaderCurrentOffset,   // loader lag
		maxBatcherLag,
		maxLoaderLag,
	) {
		klog.V(2).Infof("%s, realtime", topic)
		return true, &now, nil
	} else {
		klog.V(2).Infof("%v: waiting to reach realtime", topic)
		return false, &now, nil
	}
}

// realtimeTopics gives back the list of topics whose consumer lags are
// less than or equal to the specified thresholds to be considered realtime
func (s *sinkGroup) realtimeTopics(
	currentRealtime []string,
	watcher kafka.Watcher,
	cache *sync.Map,
) []string {
	current := toMap(currentRealtime)
	realtimeTopics := []string{}

	allTopics, err := watcher.Topics()
	if err != nil {
		klog.Errorf(
			"Ignoring realtime update. Error fetching all topics, err:%v",
			err,
		)
		return currentRealtime
	}

	for _, topic := range s.topics {
		realtime, lastRefresh, err := s.topicRealtime(
			watcher, topic, cache, toMap(allTopics),
		)
		if err != nil {
			klog.Errorf(
				"rsk/%s Error getting realtime for topic: %s, err: %v",
				s.rsk.Name,
				topic,
				err,
			)
			_, ok := current[topic]
			// if there is an error in finding lag
			// and the topic was already in realtime consider it realtime
			if ok {
				cache.Store(topic, kafkaRealtimeCache{lastCacheRefresh: lastRefresh, realtime: true})
				realtimeTopics = append(realtimeTopics, topic)
				continue
			}
		}
		if realtime {
			realtimeTopics = append(realtimeTopics, topic)
		}
		cache.Store(topic, kafkaRealtimeCache{lastCacheRefresh: lastRefresh, realtime: realtime})
	}

	return realtimeTopics
}

func (s *sinkGroup) reconcile(
	ctx context.Context,
) (
	ctrl.Result, ReconcilerEvent, error,
) {
	result := ctrl.Result{RequeueAfter: time.Second * 30}

	event, err := s.reconcileBatchers(ctx, s.batchers)
	if err != nil {
		return result, nil, err
	}
	if event != nil {
		return result, event, nil
	}

	event, err = s.reconcileLoaders(ctx, s.loaders)
	if err != nil {
		return result, nil, err
	}
	if event != nil {
		return result, event, nil
	}

	return result, nil, nil
}
