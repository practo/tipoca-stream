package controllers

import (
	"context"
	"fmt"
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
	MainSinkGroup                = "main"
	ReloadSinkGroup              = "reload"
	ReloadDupeSinkGroup          = "reload-dupe"
	DefaultmaxBatcherRealtimeLag = int64(100)
	DefautmaxLoaderRealtimeLag   = int64(10)
	ReloadTableSuffix            = "_ts_adx_reload"
)

type sinkGroupInterface interface {
	Reconcile(ctx context.Context) (ctrl.Result, ReconcilerEvent, error)
	RealtimeTopics(watcher kafka.Watcher) ([]string, error)
}

type Deployment interface {
	Name() string
	Namespace() string
	Deployment() *appsv1.Deployment
	Config() *corev1.ConfigMap
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
	batcher     Deployment
	loader      Deployment
}

type sinkGroupBuilder interface {
	setRedshiftSink(rsk *tipocav1.RedshiftSink) sinkGroupBuilder
	setClient(client client.Client) sinkGroupBuilder
	setScheme(scheme *runtime.Scheme) sinkGroupBuilder
	setType(sgType string) sinkGroupBuilder
	setTopics(topics []string) sinkGroupBuilder
	setMaskVersion(version string) sinkGroupBuilder
	setTopicGroups() sinkGroupBuilder
	buildBatcher(secret map[string]string, defaultImage, defaultKafkaVersion string, tlsConfig *kafka.TLSConfig) sinkGroupBuilder
	buildLoader(secret map[string]string, defaultImage, tableSuffix string, defaultKafkaVersion string, tlsConfig *kafka.TLSConfig) sinkGroupBuilder
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
	batcher     Deployment
	loader      Deployment
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

func (sb *buildSinkGroup) buildBatcher(
	secret map[string]string,
	defaultImage string,
	defaultKafkaVersion string,
	tlsConfig *kafka.TLSConfig,
) sinkGroupBuilder {
	consumerGroups, err := computeConsumerGroups(sb.topicGroups, sb.topics)
	if err != nil {
		klog.Fatalf("Error computing consumer group from status, err: %v", err)
	}
	batcher, err := NewBatcher(
		batcherName(sb.rsk.Name, sb.sgType),
		sb.rsk,
		sb.maskVersion,
		secret,
		sb.sgType,
		consumerGroups,
		defaultImage,
		defaultKafkaVersion,
		tlsConfig,
	)
	if err != nil {
		klog.Fatalf("Error making batcher: %v", err)
	}
	sb.batcher = batcher
	return sb
}

func (sb *buildSinkGroup) buildLoader(
	secret map[string]string,
	defaultImage string,
	tableSuffix string,
	defaultKafkaVersion string,
	tlsConfig *kafka.TLSConfig,
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
	)
	if err != nil {
		klog.Fatalf("Error making loader: %v", err)
	}
	sb.loader = loader
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
		batcher:     sb.batcher,
		loader:      sb.loader,
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

func consumerGroupID(rskName, rskNamespace, groupID string) string {
	return rskNamespace + "-" + rskName + "-" + groupID
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
	labelInstance string,
	d Deployment,
) (
	ReconcilerEvent,
	error,
) {
	deployment := d.Deployment()
	configMap := d.Config()

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
		return nil, nil
	}

	// find and cleanup dead deployments
	deploymentList, err := listDeployments(
		ctx,
		s.client,
		labelInstance,
		s.sgType,
		d.Namespace(),
	)
	if err != nil {
		return nil, err
	}
	for _, deploy := range deploymentList.Items {
		labelValue, ok := deploy.Labels[InstanceName]
		if !ok {
			continue
		}
		if labelValue != deployment.Name {
			klog.V(2).Infof("[Cleanup] Deleting deployment %s", labelValue)
			event, err := deleteDeployment(ctx, s.client, &deploy, s.rsk)
			if err != nil {
				return nil, err
			}
			if event != nil {
				return event, nil
			}
		}
	}

	// find and cleanup dead config maps
	configMapList, err := listConfigMaps(
		ctx,
		s.client,
		labelInstance,
		s.sgType,
		d.Namespace(),
	)
	if err != nil {
		return nil, err
	}
	klog.V(5).Infof("[Cleanup] ConfigMapList: %+v", len(configMapList.Items))
	for _, config := range configMapList.Items {
		labelValue, ok := config.Labels[InstanceName]
		if !ok {
			continue
		}
		if labelValue != configMap.Name {
			klog.V(2).Infof("[Cleanup] Deleting configMap %s", labelValue)
			event, err := deleteConfigMap(ctx, s.client, &config, s.rsk)
			if err != nil {
				return nil, err
			}
			if event != nil {
				return event, nil
			}
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

func (s *sinkGroup) reconcileBatcher(
	ctx context.Context,
	d Deployment,
) (
	ReconcilerEvent,
	error,
) {
	// reconcile batcher configMap
	event, err := s.reconcileConfigMap(ctx, s.batcher)
	if err != nil {
		return nil, fmt.Errorf("Error reconciling batcher configMap, %v", err)
	}
	if event != nil {
		return event, nil
	}

	// reconcile batcher deployment
	event, err = s.reconcileDeployment(ctx, BatcherLabelInstance, s.batcher)
	if err != nil {
		return nil, fmt.Errorf("Error reconciling batcher deployment, %v", err)
	}
	if event != nil {
		return event, nil
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
	event, err := s.reconcileConfigMap(ctx, s.loader)
	if err != nil {
		return nil, fmt.Errorf("Error reconciling loader configMap, %v", err)
	}
	if event != nil {
		return event, nil
	}

	// reconcile loader deployment
	event, err = s.reconcileDeployment(ctx, LoaderLabelInstance, s.loader)
	if err != nil {
		return nil, fmt.Errorf("Error reconciling loader deployment, %v", err)
	}
	if event != nil {
		return event, nil
	}

	return nil, nil
}

func (s *sinkGroup) lagBelowThreshold(
	topic string,
	batcherLag,
	loaderLag int64,
) bool {
	var maxBatcherRealtimeLag, maxLoaderRealtimeLag int64
	if s.rsk.Spec.ReleaseCondition == nil {
		maxBatcherRealtimeLag = DefaultmaxBatcherRealtimeLag
		maxLoaderRealtimeLag = DefautmaxLoaderRealtimeLag
	} else {
		if s.rsk.Spec.ReleaseCondition.MaxBatcherLag != nil {
			maxBatcherRealtimeLag = *s.rsk.Spec.ReleaseCondition.MaxBatcherLag
		}
		if s.rsk.Spec.ReleaseCondition.MaxLoaderLag != nil {
			maxLoaderRealtimeLag = *s.rsk.Spec.ReleaseCondition.MaxLoaderLag
		}
		if s.rsk.Spec.TopicReleaseCondition != nil {
			d, ok := s.rsk.Spec.TopicReleaseCondition[topic]
			if ok {
				if d.MaxBatcherLag != nil {
					maxBatcherRealtimeLag = *d.MaxBatcherLag
				}
				if d.MaxLoaderLag != nil {
					maxLoaderRealtimeLag = *d.MaxLoaderLag
				}
			}
		}
	}

	if batcherLag <= maxBatcherRealtimeLag &&
		loaderLag <= maxLoaderRealtimeLag {

		return true
	}

	return false
}

// realtimeTopics gives back the list of topics whose consumer lags are
// less than or equal to the specified thresholds to be considered realtime
func (s *sinkGroup) realtimeTopics(
	watcher kafka.Watcher,
) (
	[]string, error,
) {
	realtimeTopics := []string{}
	for _, topic := range s.topics {
		group, ok := s.topicGroups[topic]
		if !ok {
			return realtimeTopics, fmt.Errorf("groupID not found for %s", topic)
		}

		batcherCGID := consumerGroupID(s.rsk.Name, s.rsk.Namespace, group.ID)
		batcherLag, err := watcher.ConsumerGroupLag(
			batcherCGID,
			topic,
			0,
		)
		if err != nil {
			return realtimeTopics, err
		}
		if batcherLag == -1 {
			klog.V(3).Infof("%v: lag=-1, condition unmet", batcherCGID)
			continue
		}

		loaderCGID := consumerGroupID(s.rsk.Name, s.rsk.Namespace, group.ID)
		loaderLag, err := watcher.ConsumerGroupLag(
			loaderCGID,
			s.rsk.Spec.KafkaLoaderTopicPrefix+group.ID+"-"+topic,
			0,
		)
		if err != nil {
			return realtimeTopics, err
		}
		if loaderLag == -1 {
			klog.V(3).Infof("%v: lag=-1, condition unmet", loaderCGID)
			continue
		}

		klog.V(3).Infof("%v: lag=%v", batcherCGID, batcherLag)
		klog.V(3).Infof("%v: lag=%v", loaderCGID, loaderLag)

		if s.lagBelowThreshold(topic, batcherLag, loaderLag) {
			realtimeTopics = append(realtimeTopics, topic)
		} else {
			klog.V(2).Infof("%v: waiting to reach realtime", topic)
		}
	}

	return realtimeTopics, nil
}

func (s *sinkGroup) reconcile(
	ctx context.Context,
) (
	ctrl.Result, ReconcilerEvent, error,
) {
	result := ctrl.Result{RequeueAfter: time.Second * 30}

	event, err := s.reconcileBatcher(ctx, s.batcher)
	if err != nil {
		return result, nil, err
	}
	if event != nil {
		return result, event, nil
	}

	event, err = s.reconcileLoader(ctx, s.loader)
	if err != nil {
		return result, nil, err
	}
	if event != nil {
		return result, event, nil
	}

	return result, nil, nil
}
