/*


Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/practo/klog/v2"
	tipocav1 "github.com/practo/tipoca-stream/redshiftsink/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	controller "sigs.k8s.io/controller-runtime/pkg/controller"
)

const (
	BatcherSuffix       = "-batcher"
	BatcherEnvPrefix    = "BATCHER_"
	BatcherDefaultImage = "practodev/redshiftbatcher:latest"

	LoaderSuffix       = "-loader"
	LoaderEnvPrefix    = "LOADER_"
	LoaderDefaultImage = "practodev/redshiftloader:latest"
)

// RedshiftSinkReconciler reconciles a RedshiftSink object
type RedshiftSinkReconciler struct {
	client.Client

	Log      logr.Logger
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=tipoca.k8s.practo.dev,resources=redshiftsinks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=tipoca.k8s.practo.dev,resources=redshiftsinks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch;create;update;patch;delete

func serviceName(name, namespace string) types.NamespacedName {
	return types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}
}

// getDeployment gives back a deployment object for a deploySpec
// deploySpec is constructed using redshiftsink crd
func deploymentForRedshiftSink(deploySpec deploymentSpec) *appsv1.Deployment {
	d := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deploySpec.deploymentName,
			Namespace: deploySpec.namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: deploySpec.replicas,
			Strategy: appsv1.DeploymentStrategy{
				Type: appsv1.RecreateDeploymentStrategyType,
			},
			Selector: &metav1.LabelSelector{
				MatchLabels: deploySpec.labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Name:      deploySpec.name,
					Namespace: deploySpec.namespace,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						corev1.Container{
							Name:  deploySpec.name,
							Image: deploySpec.image,
							Env:   deploySpec.envs,
						},
					},
				},
			},
		},
	}

	if deploySpec.resources != nil {
		d.Spec.Template.Spec.Containers[0].Resources = *deploySpec.resources
	}

	if deploySpec.tolerations != nil {
		d.Spec.Template.Spec.Tolerations = *deploySpec.tolerations
	}

	return d
}

// getImage gets the image based on the image passed otherwise default
func getImage(image *string, batcher bool) string {
	if image != nil {
		return *image
	}

	if batcher {
		return BatcherDefaultImage
	} else {
		return LoaderDefaultImage
	}
}

// getDefaultLabels gives back the default labels for the crd resources
func getDefaultLabels(app string) map[string]string {
	return map[string]string{
		"app":                          "redshiftsink",
		"app.kubernetes.io/instance":   app,
		"app.kubernetes.io/managed-by": "redshiftsink-operator",
		"practo.dev/kind":              "RedshiftSink",
		"practo.dev/name":              app,
	}
}

// replicas for the crd resources batcher and loader are boolean, either 1 or 0
func getReplicas(suspend bool) *int32 {
	var replicas int32
	if suspend {
		replicas = 0
	} else {
		replicas = 1
	}

	return &replicas
}

type deploymentSpec struct {
	name           string
	namespace      string
	labels         map[string]string
	replicas       *int32
	deploymentName string
	envs           []corev1.EnvVar
	resources      *corev1.ResourceRequirements
	tolerations    *[]corev1.Toleration
	image          string
}

// loaderDeploymentForRedshiftSink constructs the deployment
// spec based on the crd spec
func (r *RedshiftSinkReconciler) loaderDeploymentForRedshiftSink(
	redshiftsink *tipocav1.RedshiftSink,
	envs []corev1.EnvVar) *appsv1.Deployment {

	deploySpec := deploymentSpec{
		name:           redshiftsink.Name + LoaderSuffix,
		namespace:      redshiftsink.Namespace,
		labels:         getDefaultLabels("redshiftloader"),
		replicas:       getReplicas(redshiftsink.Spec.Loader.Suspend),
		deploymentName: redshiftsink.Name + LoaderSuffix,
		envs:           envs,
		resources:      redshiftsink.Spec.Loader.PodTemplate.Resources,
		tolerations:    redshiftsink.Spec.Loader.PodTemplate.Tolerations,
		image:          getImage(redshiftsink.Spec.Loader.PodTemplate.Image, false),
	}
	deployment := deploymentForRedshiftSink(deploySpec)
	ctrl.SetControllerReference(redshiftsink, deployment, r.Scheme)

	return deployment
}

// batcherDeploymentForRedshiftSink constructs the deployment
// spec based on the crd spec
func (r *RedshiftSinkReconciler) batcherDeploymentForRedshiftSink(
	redshiftsink *tipocav1.RedshiftSink,
	envs []corev1.EnvVar) *appsv1.Deployment {

	deploySpec := deploymentSpec{
		name:           redshiftsink.Name + BatcherSuffix,
		namespace:      redshiftsink.Namespace,
		labels:         getDefaultLabels("redshiftbatcher"),
		replicas:       getReplicas(redshiftsink.Spec.Batcher.Suspend),
		deploymentName: redshiftsink.Name + BatcherSuffix,
		envs:           envs,
		resources:      redshiftsink.Spec.Batcher.PodTemplate.Resources,
		tolerations:    redshiftsink.Spec.Batcher.PodTemplate.Tolerations,
		image:          getImage(redshiftsink.Spec.Batcher.PodTemplate.Image, true),
	}
	deployment := deploymentForRedshiftSink(deploySpec)
	ctrl.SetControllerReference(redshiftsink, deployment, r.Scheme)

	return deployment
}

// addBatcherConfigToEnv adds the batcher envs to the list
func addBatcherConfigToEnv(
	envVars []corev1.EnvVar,
	redshiftsink *tipocav1.RedshiftSink) []corev1.EnvVar {

	// TODO: any better way to do this?
	envs := []corev1.EnvVar{
		corev1.EnvVar{
			Name:  BatcherEnvPrefix + "BATCHER_MASK",
			Value: fmt.Sprintf("%v", redshiftsink.Spec.Batcher.Mask),
		},
		corev1.EnvVar{
			Name:  BatcherEnvPrefix + "BATCHER_MASKCONFIGDIR",
			Value: fmt.Sprintf("%v", redshiftsink.Spec.Batcher.MaskConfigDir),
		},
		corev1.EnvVar{
			Name: BatcherEnvPrefix + "BATCHER_MASKCONFIGFILENAME",
			Value: fmt.Sprintf(
				"%v", redshiftsink.Spec.Batcher.MaskConfigFileName),
		},
		corev1.EnvVar{
			Name:  BatcherEnvPrefix + "BATCHER_MAXSIZE",
			Value: fmt.Sprintf("%v", redshiftsink.Spec.Batcher.MaxSize),
		},
		corev1.EnvVar{
			Name:  BatcherEnvPrefix + "BATCHER_MAXWAITSECONDS",
			Value: fmt.Sprintf("%v", redshiftsink.Spec.Batcher.MaxWaitSeconds),
		},
		corev1.EnvVar{
			Name:  BatcherEnvPrefix + "KAFKA_BROKERS",
			Value: fmt.Sprintf("%v", redshiftsink.Spec.Batcher.KafkaBrokers),
		},
		corev1.EnvVar{
			Name:  BatcherEnvPrefix + "KAFKA_GROUP",
			Value: fmt.Sprintf("%v", redshiftsink.Spec.Batcher.KafkaGroup),
		},
		corev1.EnvVar{
			Name: BatcherEnvPrefix + "KAFKA_TOPICREGEXES",
			Value: fmt.Sprintf(
				"%v", redshiftsink.Spec.Batcher.KafkaTopicRegexes),
		},
		corev1.EnvVar{
			Name: BatcherEnvPrefix + "KAFKA_LOADERTOPICPREFIX",
			Value: fmt.Sprintf(
				"%v", redshiftsink.Spec.Batcher.KafkaLoaderTopicPrefix),
		},
		corev1.EnvVar{
			Name:  BatcherEnvPrefix + "SARAMA_OLDEST",
			Value: "true",
		},
		corev1.EnvVar{
			Name:  BatcherEnvPrefix + "SARAMA_LOG",
			Value: "false",
		},
		corev1.EnvVar{
			Name:  BatcherEnvPrefix + "SARAMA_AUTOCOMMIT",
			Value: "true",
		},
	}
	envVars = append(envVars, envs...)

	return envVars
}

// addLoaderConfigToEnv adds the loader envs to the list
func addLoaderConfigToEnv(
	envVars []corev1.EnvVar,
	redshiftsink *tipocav1.RedshiftSink) []corev1.EnvVar {

	// TODO: any better way to do this?
	envs := []corev1.EnvVar{
		corev1.EnvVar{
			Name:  BatcherEnvPrefix + "LOADER_MAXSIZE",
			Value: fmt.Sprintf("%v", redshiftsink.Spec.Loader.MaxSize),
		},
		corev1.EnvVar{
			Name:  BatcherEnvPrefix + "LOADER_MAXWAITSECONDS",
			Value: fmt.Sprintf("%v", redshiftsink.Spec.Loader.MaxWaitSeconds),
		},
		corev1.EnvVar{
			Name:  BatcherEnvPrefix + "KAFKA_BROKERS",
			Value: fmt.Sprintf("%v", redshiftsink.Spec.Loader.KafkaBrokers),
		},
		corev1.EnvVar{
			Name:  BatcherEnvPrefix + "KAFKA_GROUP",
			Value: fmt.Sprintf("%v", redshiftsink.Spec.Loader.KafkaGroup),
		},
		corev1.EnvVar{
			Name: BatcherEnvPrefix + "KAFKA_TOPICREGEXES",
			Value: fmt.Sprintf(
				"%v", redshiftsink.Spec.Loader.KafkaTopicRegexes),
		},
		corev1.EnvVar{
			Name:  BatcherEnvPrefix + "SARAMA_OLDEST",
			Value: "true",
		},
		corev1.EnvVar{
			Name:  BatcherEnvPrefix + "SARAMA_LOG",
			Value: "false",
		},
		corev1.EnvVar{
			Name:  BatcherEnvPrefix + "SARAMA_AUTOCOMMIT",
			Value: "false",
		},
		corev1.EnvVar{
			Name:  BatcherEnvPrefix + "REDSHIFT_SCHEMA",
			Value: redshiftsink.Spec.Loader.RedshiftSchema,
		},
	}
	envVars = append(envVars, envs...)

	return envVars
}

// secretEnvVar constructs the secret envvar
func secretEnvVar(name, secretKey, secretRefName string) corev1.EnvVar {
	optional := false

	return corev1.EnvVar{
		Name: strings.ToUpper(name),
		ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: secretRefName,
				},
				Key:      secretKey,
				Optional: &optional,
			},
		},
	}
}

// addBatcherSecretsToEnv to the environment
func addBatcherSecretsToEnv(
	envVars []corev1.EnvVar,
	secretRefName string) []corev1.EnvVar {

	envVars = append(envVars,
		secretEnvVar(
			BatcherEnvPrefix+"BATCHER_MASKSALT", "maskSalt", secretRefName),
		secretEnvVar(
			BatcherEnvPrefix+"S3SINK_REGION", "s3Region", secretRefName),
		secretEnvVar(
			BatcherEnvPrefix+"S3SINK_BUCKET", "s3Bucket", secretRefName),
		secretEnvVar(
			BatcherEnvPrefix+"S3SINK_BUCKETDIR",
			"s3BatcherBucketDir", secretRefName),
		secretEnvVar(
			BatcherEnvPrefix+"S3SINK_ACCESSKEYID",
			"s3AccessKeyId", secretRefName),
		secretEnvVar(
			BatcherEnvPrefix+"S3SINK_SECRETACCESSKEY",
			"s3SecretAccessKey", secretRefName),
		secretEnvVar(
			BatcherEnvPrefix+"SCHEMAREGISTRYURL",
			"schemaRegistryURL", secretRefName),
	)

	return envVars
}

// addLoaderSecretsToEnv to the environment
func addLoaderSecretsToEnv(
	envVars []corev1.EnvVar,
	secretRefName string) []corev1.EnvVar {

	envVars = append(envVars,
		secretEnvVar(
			BatcherEnvPrefix+"S3SINK_REGION", "s3Region", secretRefName),
		secretEnvVar(
			BatcherEnvPrefix+"S3SINK_BUCKET", "s3Bucket", secretRefName),
		secretEnvVar(
			BatcherEnvPrefix+"S3SINK_BUCKETDIR",
			"s3LoaderBucketDir", secretRefName),
		secretEnvVar(
			BatcherEnvPrefix+"S3SINK_ACCESSKEYID",
			"s3AccessKeyId", secretRefName),
		secretEnvVar(
			BatcherEnvPrefix+"S3SINK_SECRETACCESSKEY",
			"s3SecretAccessKey", secretRefName),
		secretEnvVar(
			BatcherEnvPrefix+"SCHEMAREGISTRYURL",
			"schemaRegistryURL", secretRefName),
		secretEnvVar(
			BatcherEnvPrefix+"REDSHIFT_HOST",
			"redshiftHost", secretRefName),
		secretEnvVar(
			BatcherEnvPrefix+"REDSHIFT_PORT",
			"redshiftPort", secretRefName),
		secretEnvVar(
			BatcherEnvPrefix+"REDSHIFT_DATABASE",
			"redshiftDatabase", secretRefName),
		secretEnvVar(
			BatcherEnvPrefix+"REDSHIFT_USER",
			"redshiftUser", secretRefName),
		secretEnvVar(
			BatcherEnvPrefix+"REDSHIFT_PASSWORD",
			"redshiftPassword", secretRefName),
	)

	return envVars
}

func (r *RedshiftSinkReconciler) updateDeployment(
	ctx context.Context,
	deployment *appsv1.Deployment,
	redshiftsink *tipocav1.RedshiftSink) error {

	err := r.Update(ctx, deployment)
	if err != nil {
		klog.Errorf("Failed to update Deployment: %s/%s, err: %v\n",
			deployment.Namespace, deployment.Name, err)
		return nil, err
	}

	event := &DeploymentUpdateEvent{
		Object: redshiftsink,
		Name:   deployment.Name,
	}
	event.Record(r.Recorder)

	return nil
}

func (r *RedshiftSinkReconciler) createDeployment(
	ctx context.Context,
	deployment *appsv1.Deployment,
	redshiftsink *tipocav1.RedshiftSink) (*DeploymentCreatedEvent, error) {

	err := r.Create(ctx, deployment)
	if err != nil {
		klog.Errorf("Failed to create Deployment: %s/%s, err: %v\n",
			deployment.Namespace, deployment.Name, err)
		return nil, err
	}

	return &DeploymentCreatedEvent{
		Object: redshiftsink,
		Name:   deployment.Name,
	}, nil

}

func (r *RedshiftSinkReconciler) createLoader(
	ctx context.Context,
	redshiftsink *tipocav1.RedshiftSink) (*DeploymentCreatedEvent, error) {

	envs := []corev1.EnvVar{}
	envs = addLoaderConfigToEnv(envs, redshiftsink)
	envs = addLoaderSecretsToEnv(envs, redshiftsink.Spec.SecretRefName)
	deployment := r.batcherDeploymentForRedshiftSink(redshiftsink, envs)

	return r.createDeployment(ctx, deployment, redshiftsink)
}

func (r *RedshiftSinkReconciler) createBatcher(
	ctx context.Context,
	redshiftsink *tipocav1.RedshiftSink) (*DeploymentCreatedEvent, error) {

	envs := []corev1.EnvVar{}
	envs = addBatcherConfigToEnv(envs, redshiftsink)
	envs = addBatcherSecretsToEnv(envs, redshiftsink.Spec.SecretRefName)
	deployment := r.loaderDeploymentForRedshiftSink(redshiftsink, envs)

	return r.createDeployment(ctx, deployment, redshiftsink)
}

func (r *RedshiftSinkReconciler) getDeployment(
	ctx context.Context,
	nameNamespace types.NamespacedName) (*appsv1.Deployment, bool, error) {

	deployment := &appsv1.Deployment{}
	err := r.Get(ctx, nameNamespace, deployment)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return deployment, true, nil
}

// loader gets the deplyoment if it already exists or create and returns that
func (r *RedshiftSinkReconciler) loader(
	ctx context.Context,
	redshiftsink *tipocav1.RedshiftSink) (*appsv1.Deployment, error) {

	deployment, found, err := r.getDeployment(
		ctx,
		serviceName(
			redshiftsink.Name+LoaderSuffix,
			redshiftsink.Namespace,
		),
	)
	if err != nil {
		return nil, err
	}
	if found {
		return deployment, nil
	}

	// create
	event, err := r.createLoader(ctx, redshiftsink)
	if err != nil {
		return nil, fmt.Errorf("unable to create loader: %w", err)
	}
	klog.V(1).Info("Created Loader: ", event.Name)
	event.Record(r.Recorder)

	// get
	deployment, found, err = r.getDeployment(
		ctx,
		serviceName(
			redshiftsink.Name+LoaderSuffix,
			redshiftsink.Namespace,
		),
	)
	if err != nil {
		return nil, err
	}
	if found {
		return deployment, nil
	}

	return nil, fmt.Errorf("Could not get loader deployment")
}

// batcher gets the deplyoment if it already exists or create and returns that
func (r *RedshiftSinkReconciler) batcher(
	ctx context.Context,
	redshiftsink *tipocav1.RedshiftSink) (*appsv1.Deployment, error) {

	// get
	deployment, found, err := r.getDeployment(
		ctx,
		serviceName(
			redshiftsink.Name+BatcherSuffix,
			redshiftsink.Namespace,
		),
	)
	if err != nil {
		return nil, err
	}
	if found {
		return deployment, nil
	}

	// create
	event, err := r.createBatcher(ctx, redshiftsink)
	if err != nil {
		return nil, fmt.Errorf("unable to create batcher: %w", err)
	}
	klog.V(1).Info("Created Batcher: ", event.Name)
	event.Record(r.Recorder)

	// get
	deployment, found, err = r.getDeployment(
		ctx,
		serviceName(
			redshiftsink.Name+BatcherSuffix,
			redshiftsink.Namespace,
		),
	)
	if err != nil {
		return nil, err
	}
	if found {
		return deployment, nil
	}

	return nil, fmt.Errorf("Could not get batcher deployment")
}

func (r *RedshiftSinkReconciler) reconcile(
	ctx context.Context,
	redshiftsink *tipocav1.RedshiftSink) (ctrl.Result, error) {

	batcher, err := r.batcher(ctx, redshiftsink)
	if err != nil {
		return ctrl.Result{Requeue: true}, err
	}

	loader, err := r.loader(ctx, redshiftsink)
	if err != nil {
		return ctrl.Result{Requeue: true}, err
	}

	err = r.updateDeployment(ctx, batcher, redshiftsink)
	if err != nil {
		return ctrl.Result{Requeue: true}, err
	}

	err = r.updateDeployment(ctx, loader, redshiftsink)
	if err != nil {
		return ctrl.Result{Requeue: true}, err
	}

	return ctrl.Result{}, nil
}

func (r *RedshiftSinkReconciler) Reconcile(
	req ctrl.Request) (_ ctrl.Result, reterr error) {

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var redshiftsink tipocav1.RedshiftSink
	if err := r.Get(ctx, req.NamespacedName, &redshiftsink); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	original := redshiftsink.DeepCopy()

	// Always attempt to patch the status after each reconciliation.
	defer func() {
		if reflect.DeepEqual(original.Status, redshiftsink.Status) {
			return
		}
		err := r.Client.Status().Patch(
			ctx,
			&redshiftsink,
			client.MergeFrom(original),
		)
		if err != nil {
			reterr = kerrors.NewAggregate(
				[]error{
					reterr,
					fmt.Errorf(
						"error while patching EtcdCluster.Status: %s ", err),
				},
			)
		}
	}()

	// Perform a reconcile, getting back the desired result, any utilerrors
	result, err := r.reconcile(ctx, &redshiftsink)
	if err != nil {
		err = fmt.Errorf("Failed to reconcile: %s", err)
	}

	return result, err
}

// SetupWithManager sets up the controller and applies all controller configs
func (r *RedshiftSinkReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&tipocav1.RedshiftSink{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: 1}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Secret{}).
		Complete(r)
}
