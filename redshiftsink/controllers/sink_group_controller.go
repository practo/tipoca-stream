package controllers

import (
	"context"
	"fmt"
	klog "github.com/practo/klog/v2"
	tipocav1 "github.com/practo/tipoca-stream/redshiftsink/api/v1"
	consumer "github.com/practo/tipoca-stream/redshiftsink/pkg/consumer"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	client "sigs.k8s.io/controller-runtime/pkg/client"
	"time"
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
		rsk.Name+"-"+name+BatcherSuffix,
		rsk,
		maskFileVersion,
		secret,
		consumerGroups,
	)
	if err != nil {
		klog.Fatalf("Error making batcher: %v", err)
	}

	loader, err := NewLoader(
		rsk.Name+"-"+name+LoaderSuffix,
		rsk,
		tableSuffix,
		secret,
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
	current, exists, err := getConfigMap(
		ctx,
		s.client,
		d.Name(),
		d.Namespace(),
	)
	if err != nil {
		return nil, err
	}
	if !exists {
		klog.Infof("%v: Creating configMap", d.Name())
		event, err := createConfigMap(ctx, s.client, d.Config(), s.rsk)
		if err != nil {
			return nil, err
		}
		return event, nil
	}

	if d.UpdateConfig(current) {
		klog.Infof("%v: Updating configMap", d.Name())
		return updateConfigMap(ctx, s.client, d.Config(), s.rsk)
	}

	ctrl.SetControllerReference(s.rsk, d.Config(), s.scheme)

	return nil, nil
}

func (s *sinkGroup) reconcileDeployment(
	ctx context.Context,
	d Deployment,
) (
	ReconcilerEvent,
	error,
) {
	current, exists, err := getDeployment(
		ctx,
		s.client,
		d.Name(),
		d.Namespace(),
	)
	if err != nil {
		return nil, err
	}
	if !exists {
		klog.Infof("%v: Creating deployment", d.Name())
		event, err := createDeployment(ctx, s.client, d.Deployment(), s.rsk)
		if err != nil {
			return nil, err
		}
		return event, nil
	}

	if d.UpdateDeployment(current) {
		klog.Infof("%v: Updating deployment", d.Name())
		return updateDeployment(ctx, s.client, d.Deployment(), s.rsk)
	}

	ctrl.SetControllerReference(s.rsk, d.Deployment(), s.scheme)

	return nil, nil
}

func (s *sinkGroup) reconcile(
	ctx context.Context,
) (
	ctrl.Result, ReconcilerEvent, error,
) {
	result := ctrl.Result{RequeueAfter: time.Second * 10}

	// reconcile batcher configMap
	event, err := s.reconcileConfigMap(ctx, s.batcher)
	if err != nil {
		return result, nil, fmt.Errorf("Error reconciling batcher, %v", err)
	}
	if event != nil {
		return result, event, nil
	}

	// reconcile batcher deployment
	event, err = s.reconcileDeployment(ctx, s.batcher)
	if err != nil {
		return result, nil, fmt.Errorf("Error reconciling batcher, %v", err)
	}
	if event != nil {
		return result, event, nil
	}

	// reconcile loader configMap
	event, err = s.reconcileConfigMap(ctx, s.loader)
	if err != nil {
		return result, nil, fmt.Errorf("Error reconciling loader, %v", err)
	}
	if event != nil {
		return result, event, nil
	}

	// reconcile loader deployment
	event, err = s.reconcileDeployment(ctx, s.loader)
	if err != nil {
		return result, nil, fmt.Errorf("Error reconciling loader, %v", err)
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
		batcherLag, err := watcher.ConsumerGroupLag(
			s.batcher.Name(),
			topic,
			0,
		)
		if err != nil {
			return realtimeTopics, err
		}
		if batcherLag == -1 {
			klog.V(5).Infof("%v: consumer lag is -1, condition unmet", topic)
			continue
		}

		loaderLag, err := watcher.ConsumerGroupLag(
			s.loader.Name(),
			s.loaderTopicPrefix+topic,
			0,
		)
		if err != nil {
			return realtimeTopics, err
		}
		if loaderLag == -1 {
			klog.V(5).Infof(
				"%v%v: consumer lag is -1, condition unmet",
				s.loaderTopicPrefix,
				topic)
			continue
		}

		klog.V(5).Infof("%v: batcher consumer group lag: %v", topic, batcherLag)
		klog.V(5).Infof(
			"%v%v: loader consumer group lag: %v",
			s.loaderTopicPrefix, topic, loaderLag)

		if s.lagBelowThreshold(topic, batcherLag, loaderLag) {
			realtimeTopics = append(realtimeTopics, topic)
		} else {
			klog.V(2).Infof(
				"%v: waiting for release condition to be met.", topic)
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
