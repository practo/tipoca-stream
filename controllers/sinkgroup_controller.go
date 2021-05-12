package controllers

import (
	"context"
	"fmt"
	"time"

	klog "github.com/practo/klog/v2"
	tipocav1 "github.com/practo/tipoca-stream/redshiftsink/api/v1"
	kafka "github.com/practo/tipoca-stream/redshiftsink/pkg/kafka"
	transformer "github.com/practo/tipoca-stream/redshiftsink/pkg/transformer"
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

type sinkGroupInterface interface {
	batcherDeploymentTopics() []string
	loaderDeploymentTopics() []string
	reconcile(ctx context.Context) (ctrl.Result, ReconcilerEvent, error)
}

type Deployment interface {
	Name() string
	Namespace() string
	Config() *corev1.ConfigMap
	Deployment() *appsv1.Deployment
	UpdateConfig(current *corev1.ConfigMap) bool
	UpdateDeployment(current *appsv1.Deployment) bool
	Topics() []string
}

type sinkGroup struct {
	rsk         *tipocav1.RedshiftSink
	client      client.Client
	scheme      *runtime.Scheme
	sgType      string
	topics      []string
	topicGroups map[string]tipocav1.Group
	calc        *realtimeCalculator

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
	setRealtimeCalculator(calc *realtimeCalculator) sinkGroupBuilder

	buildBatchers(secret map[string]string, defaultImage, defaultKafkaVersion string, tlsConfig *kafka.TLSConfig) sinkGroupBuilder
	buildLoaders(secret map[string]string, defaultImage, tableSuffix string, defaultKafkaVersion string, tlsConfig *kafka.TLSConfig, defaultMaxOpenConns int, defaultMaxIdleConns int, prometheusURL string) sinkGroupBuilder

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
	calc        *realtimeCalculator

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

func (sb *buildSinkGroup) setRealtimeCalculator(calc *realtimeCalculator) sinkGroupBuilder {
	sb.calc = calc

	return sb
}

func (sb *buildSinkGroup) buildBatchers(
	secret map[string]string,
	defaultImage string,
	defaultKafkaVersion string,
	tlsConfig *kafka.TLSConfig,
) sinkGroupBuilder {
	batchers := []Deployment{}
	if sb.rsk.Spec.Batcher.SinkGroup != nil {
		var sinkGroupSpec, mainSinkGroupSpec *tipocav1.SinkGroupSpec
		sinkGroupSpec = applyBatcherSinkGroupDefaults(
			sb.rsk,
			sb.sgType,
			defaultImage,
		)
		units := []deploymentUnit{}
		// ReloadingSinkGroup
		if len(sb.topics) > 0 && sb.calc != nil { // overwrite units if currently reloading and calculation is available
			if len(sb.calc.batchersRealtime) > 0 {
				mainSinkGroupSpec = applyBatcherSinkGroupDefaults(
					sb.rsk,
					MainSinkGroup,
					defaultImage,
				)
			}
			allocator := newUnitAllocator(
				sb.rsk.Name+" batcher",
				sb.topics,
				sb.calc.batchersRealtime,
				sb.calc.batchersLast,
				*sinkGroupSpec.MaxReloadingUnits,
				sb.rsk.Status.BatcherReloadingTopics,
				mainSinkGroupSpec,
				sinkGroupSpec,
			)
			allocator.allocateReloadingUnits()
			units = allocator.units
		} else { // MainSinkGroup or ReloadDupeSinkGroup
			units = allocateUnitWithChunks(sb.topics, sinkGroupSpec, 100)
		}

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
				unit.sinkGroupSpec,
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
	prometheusURL string,
) sinkGroupBuilder {
	loaders := []Deployment{}
	if sb.rsk.Spec.Loader.SinkGroup != nil {
		var sinkGroupSpec, mainSinkGroupSpec *tipocav1.SinkGroupSpec
		sinkGroupSpec = applyLoaderSinkGroupDefaults(
			sb.rsk,
			sb.sgType,
			defaultImage,
		)
		units := []deploymentUnit{}
		// ReloadingSinkGroup
		if len(sb.topics) > 0 && sb.calc != nil { // overwrite units if currently reloading and calculation is available
			if len(sb.calc.loadersRealtime) > 0 {
				mainSinkGroupSpec = applyLoaderSinkGroupDefaults(
					sb.rsk,
					MainSinkGroup,
					defaultImage,
				)
			}
			allocator := newUnitAllocator(
				sb.rsk.Name+" loader",
				sb.topics,
				sb.calc.loadersRealtime,
				sb.calc.loadersLast,
				*sinkGroupSpec.MaxReloadingUnits,
				sb.rsk.Status.LoaderReloadingTopics,
				mainSinkGroupSpec,
				sinkGroupSpec,
			)
			allocator.allocateReloadingUnits()
			units = []deploymentUnit{}
			for _, unit := range allocator.units {
				units = append(units, deploymentUnit{
					id:            unit.id,
					sinkGroupSpec: unit.sinkGroupSpec,
					topics:        makeBatcherTopics(unit.topics),
				})
			}
		} else { // MainSinkGroup or ReloadDupeSinkGroup
			units = allocateUnitWithChunks(sb.topics, sinkGroupSpec, 100)
		}

		for _, unit := range units {
			consumerGroups, err := computeConsumerGroups(
				sb.topicGroups, unit.topics)
			if err != nil {
				klog.Fatalf(
					"Error computing consumer group from status, err: %v", err)
			}
			loader, err := NewLoader(
				loaderName(sb.rsk.Name, sb.sgType, unit.id),
				sb.rsk,
				tableSuffix,
				secret,
				sb.sgType,
				unit.sinkGroupSpec,
				consumerGroups,
				defaultImage,
				defaultKafkaVersion,
				tlsConfig,
				defaultMaxOpenConns,
				defaultMaxIdleConns,
				prometheusURL,
			)
			if err != nil {
				klog.Fatalf("Error making loader: %v", err)
			}
			loaders = append(loaders, loader)
		}
	} else { // Deprecated
		consumerGroups, err := computeConsumerGroups(sb.topicGroups, sb.topics)
		if err != nil {
			klog.Fatalf(
				"Error computing consumer group from status, err: %v", err)
		}
		loader, err := NewLoader(
			loaderName(sb.rsk.Name, sb.sgType, ""),
			sb.rsk,
			tableSuffix,
			secret,
			sb.sgType,
			nil,
			consumerGroups,
			defaultImage,
			defaultKafkaVersion,
			tlsConfig,
			defaultMaxOpenConns,
			defaultMaxIdleConns,
			prometheusURL,
		)
		if err != nil {
			klog.Fatalf("Error making loader: %v", err)
		}
		loaders = append(loaders, loader)
	}

	sb.loaders = loaders
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

	switch sinkGroupName {
	case ReloadSinkGroup:
		for _, topic := range topics {
			groupID := groupIDFromTopicVersion(topic, version)
			groups[topic] = tipocav1.Group{
				ID: groupID,
				LoaderTopicPrefix: loaderPrefixFromVersion(
					prefix,
					version,
				),
			}
		}
		return groups
	case ReloadDupeSinkGroup:
		for _, topic := range topics {
			group := topicGroup(rsk, topic)
			if group == nil {
				// do not sink topics which are sinking for the first time
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
				groupID := groupIDFromTopicVersion(topic, version)
				groups[topic] = tipocav1.Group{
					ID: groupID,
					LoaderTopicPrefix: loaderPrefixFromVersion(
						prefix,
						version,
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

func groupIDFromTopicVersion(topic string, version string) string {
	_, _, table := transformer.ParseTopic(topic)

	groupID := version
	if len(version) >= 6 {
		groupID = groupID[:6]
	}

	return fmt.Sprintf("%s-%s", table, groupID)
}

func loaderPrefixFromVersion(prefix string, version string) string {
	sVersion := version
	if len(sVersion) >= 6 {
		sVersion = sVersion[:6]
	}

	return prefix + sVersion + "-"
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
		} else { // Deprecated, every topic now runs in a separate consumer group
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

	klog.V(2).Infof("rsk/%s Creating configMap: %v", s.rsk.Name, config.Name)
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

		klog.V(2).Infof("rsk/%s Updating deployment: %v", s.rsk.Name, deployment.Name)
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
	klog.V(2).Infof("rsk/%s Creating deployment: %v", s.rsk.Name, deployment.Name)
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
	[]ReconcilerEvent,
	error,
) {
	var events []ReconcilerEvent

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
		return events, err
	}
	for _, deploy := range deploymentList.Items {
		klog.V(4).Infof("Cleanup suspect deployment: %v", deploy.Name)
		labelValue, ok := deploy.Labels[InstanceName]
		if !ok {
			continue
		}
		_, ok = neededDeployments[labelValue]
		if !ok {
			klog.V(2).Infof("rsk/%s Deleting deployment: %v", s.rsk.Name, labelValue)
			event, err := deleteDeployment(ctx, s.client, &deploy, s.rsk)
			if err != nil {
				return events, err
			}
			if event != nil {
				events = append(events, event)
			}
		}
	}

	klog.V(4).Infof("Current active configMaps, needed: %+v", neededConfigMaps)
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
		return events, err
	}

	for _, config := range configMapList.Items {
		klog.V(4).Infof("Cleanup configmap suspect cm: %v", config.Name)
		labelValue, ok := config.Labels[InstanceName]
		if !ok {
			continue
		}
		_, ok = neededConfigMaps[labelValue]
		if !ok {
			klog.V(2).Infof("rsk/%s Deleting configmap: %s", s.rsk.Name, labelValue)
			event, err := deleteConfigMap(ctx, s.client, &config, s.rsk)
			if err != nil {
				return events, err
			}
			if event != nil {
				events = append(events, event)
			}
		}
	}

	return events, nil
}

func (s *sinkGroup) reconcileBatcher(
	ctx context.Context,
	d Deployment,
) (
	[]ReconcilerEvent,
	error,
) {
	var events []ReconcilerEvent

	// reconcile batcher configMap
	event, err := s.reconcileConfigMap(ctx, d)
	if err != nil {
		return events, fmt.Errorf(
			"Error reconciling batcher configMap, %v", err)
	}
	if event != nil {
		events = append(events, event)
	}

	// reconcile batcher deployment
	event, err = s.reconcileDeployment(ctx, d)
	if err != nil {
		return events, fmt.Errorf(
			"Error reconciling batcher deployment, %v", err)
	}
	if event != nil {
		events = append(events, event)
	}

	return events, nil
}

func (s *sinkGroup) reconcileBatchers(
	ctx context.Context,
	deployments []Deployment,
) (
	[]ReconcilerEvent,
	error,
) {
	var events []ReconcilerEvent

	// cleanup the ones which should be dead before creating new
	var neededDeployments, neededConfigMaps []string
	for _, d := range deployments {
		neededDeployments = append(neededDeployments, d.Name())
		neededConfigMaps = append(neededConfigMaps, d.Name())
	}
	cleanupEvents, err := s.cleanup(
		ctx,
		BatcherLabelInstance,
		toMap(neededDeployments),
		toMap(neededConfigMaps),
	)
	if err != nil {
		return events, err
	}
	if len(cleanupEvents) > 0 {
		events = append(events, cleanupEvents...)
	}

	// create or update
	for _, d := range deployments {
		batcherEvents, err := s.reconcileBatcher(ctx, d)
		if err != nil {
			return events, err
		}
		if len(batcherEvents) > 0 {
			events = append(events, batcherEvents...)
		}
	}

	return events, nil
}

func (s *sinkGroup) reconcileLoader(
	ctx context.Context,
	d Deployment,
) (
	[]ReconcilerEvent,
	error,
) {
	var events []ReconcilerEvent

	event, err := s.reconcileConfigMap(ctx, d)
	if err != nil {
		return events, fmt.Errorf("Error reconciling loader configMap, %v", err)
	}
	if event != nil {
		events = append(events, event)
	}

	// reconcile loader deployment
	event, err = s.reconcileDeployment(ctx, d)
	if err != nil {
		return events, fmt.Errorf("Error reconciling loader deployment, %v", err)
	}
	if event != nil {
		events = append(events, event)
	}

	return events, nil
}

func (s *sinkGroup) reconcileLoaders(
	ctx context.Context,
	deployments []Deployment,
) (
	[]ReconcilerEvent,
	error,
) {
	var events []ReconcilerEvent

	// cleanup the ones which should be dead before creating new
	var neededDeployments, neededConfigMaps []string
	for _, d := range deployments {
		neededDeployments = append(neededDeployments, d.Name())
		neededConfigMaps = append(neededConfigMaps, d.Name())
	}
	cleanupEvents, err := s.cleanup(
		ctx,
		LoaderLabelInstance,
		toMap(neededDeployments),
		toMap(neededConfigMaps),
	)
	if err != nil {
		return events, err
	}
	if len(cleanupEvents) > 0 {
		events = append(events, cleanupEvents...)
	}

	// create or update
	for _, d := range deployments {
		loaderEvents, err := s.reconcileLoader(ctx, d)
		if err != nil {
			return events, err
		}
		if len(loaderEvents) > 0 {
			events = append(events, loaderEvents...)
		}
	}

	return events, nil
}

func (s *sinkGroup) reconcile(
	ctx context.Context,
) (
	ctrl.Result,
	[]ReconcilerEvent,
	error,
) {
	result := ctrl.Result{RequeueAfter: time.Second * 30}
	events := []ReconcilerEvent{}

	batcherEvents, err := s.reconcileBatchers(ctx, s.batchers)
	if err != nil {
		return result, events, err
	}
	if len(batcherEvents) > 0 {
		events = append(events, batcherEvents...)
	}

	loaderEvents, err := s.reconcileLoaders(ctx, s.loaders)
	if err != nil {
		return result, events, err
	}
	if len(loaderEvents) > 0 {
		events = append(events, loaderEvents...)
	}

	return result, events, nil
}

func (s *sinkGroup) batcherDeploymentTopics() []string {
	t := []string{}
	for _, d := range s.batchers {
		t = append(t, d.Topics()...)
	}

	return t
}

func (s *sinkGroup) loaderDeploymentTopics() []string {
	t := []string{}
	for _, d := range s.loaders {
		t = append(t, d.Topics()...)
	}

	return t
}
