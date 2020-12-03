package controllers

import (
	"context"
	klog "github.com/practo/klog/v2"
	tipocav1 "github.com/practo/tipoca-stream/redshiftsink/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	client "sigs.k8s.io/controller-runtime/pkg/client"
	"time"
)

type SinkGroup struct {
	name    string
	batcher Deployment
	loader  Deployment
	rsk     *tipocav1.RedshiftSink
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
	rsk *tipocav1.RedshiftSink,
	kakfaTopics []string, tableSuffix string) *SinkGroup {

	batcherTopics := expandTopicsToRegex(kakfaTopics)
	loaderTopics := expandTopicsToRegex(
		makeLoaderTopics(
			rsk.Spec.KafkaLoaderTopicPrefix,
			kakfaTopics),
	)

	return &SinkGroup{
		batcher: NewBatcher(name, client, rsk, batcherTopics),
		loader:  NewLoader(name, client, rsk, loaderTopics, tableSuffix),
		rsk:     rsk,
	}
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
		serviceName(
			d.Name(),
			d.Namespace(),
		),
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

	return nil, nil
}

func (s *SinkGroup) Reconcile(
	ctx context.Context) (ctrl.Result, ReconcilerEvent, error) {

	result := ctrl.Result{RequeueAfter: time.Second * 10}

	// reconcile batcher
	event, err := s.reconcile(ctx, s.batcher)
	if err != nil {
		return result, event, err
	}
	if event != nil {
		return result, event, nil
	}

	// reconcile loader
	event, err = s.reconcile(ctx, s.loader)
	if err != nil {
		return result, event, err
	}
	if event != nil {
		return result, event, nil
	}

	klog.Info("Nothing done.")
	return result, nil, nil
}
