package controllers

import (
	"context"
	"fmt"
	klog "github.com/practo/klog/v2"
	tipocav1 "github.com/practo/tipoca-stream/redshiftsink/api/v1"
	consumer "github.com/practo/tipoca-stream/redshiftsink/pkg/consumer"
	appsv1 "k8s.io/api/apps/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	client "sigs.k8s.io/controller-runtime/pkg/client"
	"time"
)

const (
	MainSinkGroup             = "main"
	ReloadSinkGroup           = "reload"
	ReloadDupeSinkGroup       = "reload-dupe"
	DefaultBatcherRealtimeLag = int64(100)
	DefautLoaderRealtimeLag   = int64(10)
)

type SinkGroupInterface interface {
	Reconcile(ctx context.Context) (ctrl.Result, ReconcilerEvent, error)
	RealtimeTopics(watcher consumer.KafkaWatcher) ([]string, error)
}

type SinkGroup struct {
	name    string
	batcher Deployment
	loader  Deployment

	topics            []string
	loaderTopicPrefix string

	// realtimeLag is the metric that tells if the SinkGroup has reached
	// near real time. i.e. batcher and loader's
	// consumer group lags <= realtimeLag
	batcherRealtimeLag int64
	loaderRealtimeLag  int64

	scheme *runtime.Scheme
	rsk    *tipocav1.RedshiftSink
}

type Deployment interface {
	Name() string
	Namespace() string
	Client() client.Client
	Deployment() *appsv1.Deployment
	UpdateRequired(current *appsv1.Deployment) bool
}

func NewSinkGroup(
	name string,
	client client.Client,
	scheme *runtime.Scheme,
	rsk *tipocav1.RedshiftSink,
	kafkaTopics []string,
	maskFileVersion string) *SinkGroup {

	tableSuffix := tableSuffixBySinkGroup(name, maskFileVersion)
	consumerGroups := consumerGroupsBySinkGroup(rsk, name)
	// TODO: pass on to batcher and loader to handle multiple consumer groups
	_ = consumerGroups

	batcherTopics := expandTopicsToRegex(kafkaTopics)
	loaderTopics := expandTopicsToRegex(
		makeLoaderTopics(
			rsk.Spec.KafkaLoaderTopicPrefix,
			kafkaTopics),
	)

	batcherName := rsk.Name + "-" + name + BatcherSuffix
	loaderName := rsk.Name + "-" + name + LoaderSuffix

	return &SinkGroup{
		batcher: NewBatcher(
			batcherName, client, rsk, batcherTopics, maskFileVersion),
		loader: NewLoader(
			loaderName, client, rsk, loaderTopics, tableSuffix),
		topics: kafkaTopics,

		loaderTopicPrefix:  rsk.Spec.KafkaLoaderTopicPrefix,
		batcherRealtimeLag: DefaultBatcherRealtimeLag,
		loaderRealtimeLag:  DefautLoaderRealtimeLag,

		scheme: scheme,
		rsk:    rsk,
	}
}

// used to set consumer group name, sink group name batcher name
func getBatcherUniqueName() {

}

func tableSuffixBySinkGroup(sg string, desireVersion string) string {
	switch sg {
	case ReloadSinkGroup:
		return "-" + desireVersion
	case MainSinkGroup:
		return ""
	case ReloadDupeSinkGroup:
		return ""
	}

	return ""
}

func (s *SinkGroup) reconcile(
	ctx context.Context,
	d Deployment,
) (
	ReconcilerEvent,
	error,
) {
	currentDeployment, exists, err := getDeployment(
		ctx,
		d.Client(),
		d.Name(),
		d.Namespace(),
	)
	if err != nil {
		return nil, err
	}
	if !exists {
		klog.Infof("%v: Creating", d.Name())
		event, err := createDeployment(ctx, d.Client(), d.Deployment(), s.rsk)
		if err != nil {
			return nil, err
		}
		return event, nil
	}

	if d.UpdateRequired(currentDeployment) {
		klog.Infof("%v: Updating", d.Name())
		event, err := updateDeployment(ctx, d.Client(), d.Deployment(), s.rsk)
		return event, err
	}

	ctrl.SetControllerReference(s.rsk, d.Deployment(), s.scheme)

	return nil, nil
}

func (s *SinkGroup) Reconcile(
	ctx context.Context,
) (
	ctrl.Result, ReconcilerEvent, error,
) {
	result := ctrl.Result{RequeueAfter: time.Second * 10}

	// reconcile batcher
	event, err := s.reconcile(ctx, s.batcher)
	if err != nil {
		return result, event, fmt.Errorf("Error reconciling batcher, %v", err)
	}
	if event != nil {
		return result, event, nil
	}

	// reconcile loader
	event, err = s.reconcile(ctx, s.loader)
	if err != nil {
		return result, event, fmt.Errorf("Error reconciling loader, %v", err)
	}
	if event != nil {
		return result, event, nil
	}

	return result, nil, nil
}

// RealtimeTopics gives back the list of topics whose consumer lags are
// less than or equal to the specified thresholds to be considered realtime
func (s *SinkGroup) RealtimeTopics(
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
			klog.Infof("%v: consumer lag is -1, condition unmet", topic)
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
			klog.Infof(
				"%v%v: consumer lag is -1, condition unmet",
				s.loaderTopicPrefix,
				topic)
			continue
		}

		klog.Infof("%v: batcher consumer group lag: %v", topic, batcherLag)
		klog.Infof(
			"%v%v: loader consumer group lag: %v",
			s.loaderTopicPrefix, topic, loaderLag)

		if batcherLag > s.batcherRealtimeLag &&
			loaderLag > s.loaderRealtimeLag {
			klog.Infof("%v: waiting for realtime condition to be met.", topic)
		}
		realtimeTopics = append(realtimeTopics, topic)
	}

	return realtimeTopics, nil
}
