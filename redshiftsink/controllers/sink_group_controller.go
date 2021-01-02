package controllers

import (
	"context"
	"fmt"
	"time"

	klog "github.com/practo/klog/v2"
	tipocav1 "github.com/practo/tipoca-stream/redshiftsink/api/v1"
	consumer "github.com/practo/tipoca-stream/redshiftsink/pkg/consumer"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	client "sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	MainSinkGroup                = "main"
	ReloadSinkGroup              = "reload"
	ReloadDupeSinkGroup          = "reload-dupe"
	DefaultmaxBatcherRealtimeLag = int64(100)
	DefautmaxLoaderRealtimeLag   = int64(10)
	ReloadTableSuffix            = "_ts_adx_reload"
)

type SinkGroupInterface interface {
	Reconcile(ctx context.Context) (ctrl.Result, ReconcilerEvent, error)
	RealtimeTopics(watcher consumer.KafkaWatcher) ([]string, error)
}

type sinkGroup struct {
	name    string
	batcher Deployment
	loader  Deployment

	topics            []string
	loaderTopicPrefix string

	topicGroups map[string]tipocav1.Group

	client client.Client
	scheme *runtime.Scheme
	rsk    *tipocav1.RedshiftSink
}

type Deployment interface {
	Name() string
	Namespace() string
	Deployment() *appsv1.Deployment
	Config() *corev1.ConfigMap
	UpdateConfig(current *corev1.ConfigMap) bool
	UpdateDeployment(current *appsv1.Deployment) bool
}

// TODO: use builder pattern to construct the sink group
// refactor here plz
func newSinkGroup(
	name string,
	client client.Client,
	scheme *runtime.Scheme,
	rsk *tipocav1.RedshiftSink,
	kafkaTopics []string,
	maskFileVersion string,
	secret map[string]string,
	tableSuffix string) *sinkGroup {

	// TODO: use builder pattern to construct the sink group
	// refactor here plz
	topicGroups, err := topicGroupBySinkGroup(
		name,
		kafkaTopics,
		rsk.Status.TopicGroup,
		maskFileVersion,
		rsk.Spec.KafkaLoaderTopicPrefix,
	)
	if err != nil {
		klog.Fatalf("Error creating topic groups, err: %v", err)
	}

	consumerGroups, err := computeConsumerGroups(topicGroups, kafkaTopics)
	if err != nil {
		klog.Fatalf("Error computing consumer group from status, err: %v", err)
	}

	batcher, err := NewBatcher(
		batcherName(rsk.Name, name),
		rsk,
		maskFileVersion,
		secret,
		name,
		consumerGroups,
	)
	if err != nil {
		klog.Fatalf("Error making batcher: %v", err)
	}

	loader, err := NewLoader(
		loaderName(rsk.Name, name),
		rsk,
		tableSuffix,
		secret,
		name,
		consumerGroups,
	)
	if err != nil {
		klog.Fatalf("Error making loader: %v", err)
	}

	return &sinkGroup{
		name:    name,
		batcher: batcher,
		loader:  loader,

		topics:            kafkaTopics,
		loaderTopicPrefix: rsk.Spec.KafkaLoaderTopicPrefix,

		topicGroups: topicGroups,

		client: client,
		scheme: scheme,
		rsk:    rsk,
	}
}

func topicGroupBySinkGroup(
	sinkGroupName string,
	topics []string,
	topicGroups map[string]tipocav1.Group,
	desiredVersion string,
	prefix string,
) (
	map[string]tipocav1.Group,
	error,
) {
	switch sinkGroupName {
	case MainSinkGroup:
		return topicGroups, nil
	case ReloadDupeSinkGroup:
		return topicGroups, nil
	case ReloadSinkGroup:
		groupID := groupIDFromVersion(desiredVersion)
		reloadTopicGroup := make(map[string]tipocav1.Group)
		for _, topic := range topics {
			reloadTopicGroup[topic] = tipocav1.Group{
				ID: groupID,
				LoaderTopicPrefix: loaderPrefixFromGroupID(
					prefix,
					groupID,
				),
			}
		}
		return reloadTopicGroup, nil
	default:
		return nil, fmt.Errorf("Invalid sink group: %s", sinkGroupName)
	}
}

func consumerGroupID(sinkPodName string, groupID string) string {
	return sinkPodName + "-" + groupID
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

	klog.V(2).Infof("Creating configMap: %v", config.Name)
	event, err := createConfigMap(ctx, s.client, config, s.rsk)
	if err != nil {
		return nil, err
	}
	ctrl.SetControllerReference(s.rsk, config, s.scheme)
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
		s.name,
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
		s.name,
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

	// create new deployment pointing to new config map
	klog.V(2).Infof("Creating deployment: %v", deployment.Name)
	event, err := createDeployment(ctx, s.client, deployment, s.rsk)
	if err != nil {
		return nil, err
	}
	ctrl.SetControllerReference(s.rsk, deployment, s.scheme)
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

func (s *sinkGroup) reconcile(
	ctx context.Context,
) (
	ctrl.Result, ReconcilerEvent, error,
) {
	result := ctrl.Result{RequeueAfter: time.Second * 1}

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

// realtimeTopics gives back the list of topics whose consumer lags are
// less than or equal to the specified thresholds to be considered realtime
func (s *sinkGroup) realtimeTopics(
	watcher consumer.KafkaWatcher,
) (
	[]string, error,
) {
	realtimeTopics := []string{}
	for _, topic := range s.topics {
		group, ok := s.topicGroups[topic]
		if !ok {
			return realtimeTopics, fmt.Errorf("groupID not found for %s", topic)
		}

		batcherCGID := consumerGroupID(s.batcher.Name(), group.ID)
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

		loaderCGID := consumerGroupID(s.loader.Name(), group.ID)
		loaderLag, err := watcher.ConsumerGroupLag(
			loaderCGID,
			s.loaderTopicPrefix+topic,
			0,
		)
		if err != nil {
			return realtimeTopics, err
		}
		if loaderLag == -1 {
			klog.V(3).Infof("%v: lag=-1, condition unmet", loaderCGID)
			continue
		}

		klog.V(3).Infof("%v: lag=%v", batcherCGID)
		klog.V(3).Infof("%v: lag=%v", loaderCGID)

		if s.lagBelowThreshold(topic, batcherLag, loaderLag) {
			realtimeTopics = append(realtimeTopics, topic)
		} else {
			klog.V(2).Infof("%v: waiting to reach realtime", topic)
		}
	}

	return realtimeTopics, nil
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
		if s.rsk.Spec.ReleaseCondition.MaxBatcherLag == nil {
			maxBatcherRealtimeLag = *s.rsk.Spec.ReleaseCondition.MaxBatcherLag
		}
		if s.rsk.Spec.ReleaseCondition.MaxLoaderLag == nil {
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
