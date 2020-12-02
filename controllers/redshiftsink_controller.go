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
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/practo/klog/v2"
	tipocav1 "github.com/practo/tipoca-stream/redshiftsink/api/v1"
	consumer "github.com/practo/tipoca-stream/redshiftsink/pkg/consumer"
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

	KafkaTopicRegexes *sync.Map
	KafkaWatcher      consumer.KafkaWatcher

	GitCache *sync.Map
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
					Labels:    deploySpec.labels,
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
	// replicas = 0
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

func expandTopicsToRegex(topics []string) string {
	sort.Strings(topics)

	fullMatchRegex := ""
	for _, topic := range topics {
		fullMatchRegex = fullMatchRegex + "^" + topic + "$,"
	}

	return strings.TrimSuffix(fullMatchRegex, ",")
}

// addBatcherConfigToEnv adds the batcher envs to the list
func (r *RedshiftSinkReconciler) addBatcherConfigToEnv(
	envVars []corev1.EnvVar,
	redshiftsink *tipocav1.RedshiftSink) ([]corev1.EnvVar, error) {

	topics, err := r.topics(redshiftsink.Spec.Batcher.KafkaTopicRegexes)
	if err != nil {
		return []corev1.EnvVar{}, err
	}

	// TODO: any better way to do this?
	envs := []corev1.EnvVar{
		corev1.EnvVar{
			Name:  BatcherEnvPrefix + "BATCHER_MASK",
			Value: fmt.Sprintf("%v", redshiftsink.Spec.Batcher.Mask),
		},
		corev1.EnvVar{
			Name:  BatcherEnvPrefix + "BATCHER_MASKFILE",
			Value: fmt.Sprintf("%v", redshiftsink.Spec.Batcher.MaskFile),
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
			Name:  BatcherEnvPrefix + "KAFKA_TOPICREGEXES",
			Value: expandTopicsToRegex(topics),
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
		corev1.EnvVar{
			Name:  BatcherEnvPrefix + "RELOAD",
			Value: "false",
		},
	}
	envVars = append(envVars, envs...)

	return envVars, nil
}

// addLoaderConfigToEnv adds the loader envs to the list
func (r *RedshiftSinkReconciler) addLoaderConfigToEnv(
	envVars []corev1.EnvVar,
	redshiftsink *tipocav1.RedshiftSink) ([]corev1.EnvVar, error) {

	topics, err := r.topics(redshiftsink.Spec.Loader.KafkaTopicRegexes)
	if err != nil {
		return []corev1.EnvVar{}, err
	}

	// TODO: any better way to do this?
	envs := []corev1.EnvVar{
		corev1.EnvVar{
			Name:  LoaderEnvPrefix + "LOADER_MAXSIZE",
			Value: fmt.Sprintf("%v", redshiftsink.Spec.Loader.MaxSize),
		},
		corev1.EnvVar{
			Name:  LoaderEnvPrefix + "LOADER_MAXWAITSECONDS",
			Value: fmt.Sprintf("%v", redshiftsink.Spec.Loader.MaxWaitSeconds),
		},
		corev1.EnvVar{
			Name:  LoaderEnvPrefix + "KAFKA_BROKERS",
			Value: fmt.Sprintf("%v", redshiftsink.Spec.Loader.KafkaBrokers),
		},
		corev1.EnvVar{
			Name:  LoaderEnvPrefix + "KAFKA_GROUP",
			Value: fmt.Sprintf("%v", redshiftsink.Spec.Loader.KafkaGroup),
		},
		corev1.EnvVar{
			Name:  LoaderEnvPrefix + "KAFKA_TOPICREGEXES",
			Value: expandTopicsToRegex(topics),
		},
		corev1.EnvVar{
			Name:  LoaderEnvPrefix + "SARAMA_OLDEST",
			Value: "true",
		},
		corev1.EnvVar{
			Name:  LoaderEnvPrefix + "SARAMA_LOG",
			Value: "false",
		},
		corev1.EnvVar{
			Name:  LoaderEnvPrefix + "SARAMA_AUTOCOMMIT",
			Value: "false",
		},
		corev1.EnvVar{
			Name:  LoaderEnvPrefix + "REDSHIFT_SCHEMA",
			Value: redshiftsink.Spec.Loader.RedshiftSchema,
		},
		corev1.EnvVar{
			Name:  LoaderEnvPrefix + "RELOAD",
			Value: "false",
		},
	}
	envVars = append(envVars, envs...)

	return envVars, nil
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
		secretEnvVar(
			BatcherEnvPrefix+"GITACCESSTOKEN",
			"gitAccessToken", secretRefName),
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

// isSpecSubsetEqual compares only subset of specs
func isSpecSubsetEqual(
	deployment *appsv1.Deployment,
	suspend bool,
	image *string,
	resources *corev1.ResourceRequirements,
	tolerations *[]corev1.Toleration) (bool, string) {

	if deployment == nil {
		return false, "deployment was nil"
	}

	specReplicas := getReplicas(suspend)
	if *specReplicas != *deployment.Spec.Replicas {
		return false, "replicas mismatch"
	}

	if image != nil {
		if *image != deployment.Spec.Template.Spec.Containers[0].Image {
			return false, "image mismatch"
		}
	}

	if resources != nil {
		if !reflect.DeepEqual(
			*resources, deployment.Spec.Template.Spec.Containers[0].Resources) {

			return false, "resources mismatch"
		}
	}

	if tolerations != nil {
		if !reflect.DeepEqual(
			*tolerations, deployment.Spec.Template.Spec.Tolerations) {

			return false, "tolerations mismatch"
		}
	}

	return true, ""
}

// batcherUpdateRequired compares the state and return accordingly
func (r *RedshiftSinkReconciler) batcherUpdateRequired(
	batcher *appsv1.Deployment,
	redshiftsink *tipocav1.RedshiftSink) (bool, error) {

	var specEnvs []corev1.EnvVar
	specEnvs, err := r.addBatcherConfigToEnv(specEnvs, redshiftsink)
	if err != nil {
		return false, err
	}
	specEnvs = addBatcherSecretsToEnv(specEnvs, redshiftsink.Spec.SecretRefName)
	currentEnvs := batcher.Spec.Template.Spec.Containers[0].Env
	if !reflect.DeepEqual(specEnvs, currentEnvs) {
		klog.Info(specEnvs)
		klog.Info(currentEnvs)
		// klog.Infof("Envs of batcher: %v requires update.", batcher.Name)
		return true, nil
	}

	equal, reason := isSpecSubsetEqual(
		batcher,
		redshiftsink.Spec.Batcher.Suspend,
		redshiftsink.Spec.Batcher.PodTemplate.Image,
		redshiftsink.Spec.Batcher.PodTemplate.Resources,
		redshiftsink.Spec.Batcher.PodTemplate.Tolerations,
	)

	if !equal {
		klog.Infof(
			"Spec mismatch for %v, reason: %v, deployment would be updated",
			batcher.Name, reason)
		return true, nil
	}

	return false, nil
}

// loaderUpdateRequired compares the state and returns accordingly
func (r *RedshiftSinkReconciler) loaderUpdateRequired(
	batcher *appsv1.Deployment,
	redshiftsink *tipocav1.RedshiftSink) (bool, error) {

	var specEnvs []corev1.EnvVar
	specEnvs, err := r.addLoaderConfigToEnv(specEnvs, redshiftsink)
	if err != nil {
		return false, err
	}
	specEnvs = addLoaderSecretsToEnv(specEnvs, redshiftsink.Spec.SecretRefName)
	currentEnvs := batcher.Spec.Template.Spec.Containers[0].Env
	if !reflect.DeepEqual(specEnvs, currentEnvs) {
		// klog.Infof("Envs of loader: %v requires update.", batcher.Name)
		return true, nil
	}

	equal, reason := isSpecSubsetEqual(
		batcher,
		redshiftsink.Spec.Loader.Suspend,
		redshiftsink.Spec.Loader.PodTemplate.Image,
		redshiftsink.Spec.Loader.PodTemplate.Resources,
		redshiftsink.Spec.Loader.PodTemplate.Tolerations,
	)

	if !equal {
		klog.Infof(
			"Spec mismatch for %v, reason: %v, deployment would be updated",
			batcher.Name, reason)
		return true, nil
	}

	return false, nil
}

func (r *RedshiftSinkReconciler) updateDeployment(
	ctx context.Context,
	deployment *appsv1.Deployment,
	redshiftsink *tipocav1.RedshiftSink) (*DeploymentUpdatedEvent, error) {

	err := r.Update(ctx, deployment)
	if err != nil {
		klog.Errorf("Failed to update Deployment: %s/%s, err: %v\n",
			deployment.Namespace, deployment.Name, err)
		return nil, err
	}

	return &DeploymentUpdatedEvent{
		Object: redshiftsink,
		Name:   deployment.Name,
	}, nil
}

func (r *RedshiftSinkReconciler) topics(regexes string) ([]string, error) {
	var topics []string
	var err error
	var rgx *regexp.Regexp
	topicsAppended := make(map[string]bool)
	expressions := strings.Split(regexes, ",")

	allTopics, err := r.KafkaWatcher.Topics()
	if err != nil {
		return topics, err
	}

	for _, expression := range expressions {
		rgxLoaded, ok := r.KafkaTopicRegexes.Load(expression)
		if !ok {
			rgx, err = regexp.Compile(strings.TrimSpace(expression))
			if err != nil {
				return topics, fmt.Errorf(
					"Compling regex: %s failed, err:%v\n", expression, err)
			}
			r.KafkaTopicRegexes.Store(expression, rgx)
		} else {
			rgx = rgxLoaded.(*regexp.Regexp)
		}

		for _, topic := range allTopics {
			if !rgx.MatchString(topic) {
				continue
			}
			_, ok := topicsAppended[topic]
			if ok {
				continue
			}
			topics = append(topics, topic)
			topicsAppended[topic] = true
		}
	}

	return topics, nil
}

func (r *RedshiftSinkReconciler) batcherDeploymentFromRedshiftSink(
	redshiftsink *tipocav1.RedshiftSink) (*appsv1.Deployment, error) {

	var envs []corev1.EnvVar
	envs, err := r.addBatcherConfigToEnv(envs, redshiftsink)
	if err != nil {
		return nil, err
	}
	envs = addBatcherSecretsToEnv(envs, redshiftsink.Spec.SecretRefName)
	dep := r.batcherDeploymentForRedshiftSink(redshiftsink, envs)
	return dep, nil
}

func (r *RedshiftSinkReconciler) loaderDeploymentFromRedshiftSink(
	redshiftsink *tipocav1.RedshiftSink) (*appsv1.Deployment, error) {

	var envs []corev1.EnvVar
	envs, err := r.addLoaderConfigToEnv(envs, redshiftsink)
	if err != nil {
		return nil, err
	}
	envs = addLoaderSecretsToEnv(envs, redshiftsink.Spec.SecretRefName)
	dep := r.loaderDeploymentForRedshiftSink(redshiftsink, envs)
	return dep, nil
}

func (r *RedshiftSinkReconciler) updateBatcher(
	ctx context.Context,
	redshiftsink *tipocav1.RedshiftSink) (*DeploymentUpdatedEvent, error) {

	deployment, err := r.batcherDeploymentFromRedshiftSink(redshiftsink)
	if err != nil {
		return nil, err
	}

	return r.updateDeployment(ctx, deployment, redshiftsink)
}

func (r *RedshiftSinkReconciler) updateLoader(
	ctx context.Context,
	redshiftsink *tipocav1.RedshiftSink) (*DeploymentUpdatedEvent, error) {

	deployment, err := r.loaderDeploymentFromRedshiftSink(redshiftsink)
	if err != nil {
		return nil, err
	}

	return r.updateDeployment(ctx, deployment, redshiftsink)
}

func (r *RedshiftSinkReconciler) createDeployment(
	ctx context.Context,
	deployment *appsv1.Deployment,
	redshiftsink *tipocav1.RedshiftSink) (*DeploymentCreatedEvent, error) {

	err := r.Create(ctx, deployment)
	if err != nil {
		klog.Errorf(
			"Failed to create Deployment: %s/%s, err: %v\n",
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

	deployment, err := r.loaderDeploymentFromRedshiftSink(redshiftsink)
	if err != nil {
		return nil, err
	}

	return r.createDeployment(ctx, deployment, redshiftsink)
}

func (r *RedshiftSinkReconciler) createBatcher(
	ctx context.Context,
	redshiftsink *tipocav1.RedshiftSink) (*DeploymentCreatedEvent, error) {

	deployment, err := r.batcherDeploymentFromRedshiftSink(redshiftsink)
	if err != nil {
		return nil, err
	}

	return r.createDeployment(ctx, deployment, redshiftsink)
}

func (r *RedshiftSinkReconciler) getDeployment(
	ctx context.Context,
	nameNamespace types.NamespacedName) (*appsv1.Deployment, bool, error) {

	deployment := &appsv1.Deployment{}
	err := r.Get(ctx, nameNamespace, deployment)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// got the expected err of not found
			return nil, false, nil
		}
		// unexpected err, should return err
		return nil, false, err
	}
	return deployment, true, nil
}

func (r *RedshiftSinkReconciler) hasLoader(
	ctx context.Context,
	redshiftsink *tipocav1.RedshiftSink) (*appsv1.Deployment, bool, error) {

	return r.getDeployment(
		ctx,
		serviceName(
			redshiftsink.Name+LoaderSuffix,
			redshiftsink.Namespace,
		),
	)
}

func (r *RedshiftSinkReconciler) hasBatcher(
	ctx context.Context,
	redshiftsink *tipocav1.RedshiftSink) (*appsv1.Deployment, bool, error) {

	return r.getDeployment(
		ctx,
		serviceName(
			redshiftsink.Name+BatcherSuffix,
			redshiftsink.Namespace,
		),
	)
}

func (r *RedshiftSinkReconciler) reconcile(
	ctx context.Context,
	redshiftsink *tipocav1.RedshiftSink,
) (
	ctrl.Result,
	ReconcilerEvent,
	error,
) {
	// always requeue after 30 seconds as we are going with the approach
	// of doing only one operation on every reconcile
	result := ctrl.Result{RequeueAfter: time.Second * 30}

	// ensure batcher deployment exists
	batcher, exists, err := r.hasBatcher(ctx, redshiftsink)
	if err != nil {
		return result, nil, err
	}
	if !exists {
		klog.Infof("%v: Creating batcher", redshiftsink.Name)
		event, err := r.createBatcher(ctx, redshiftsink)
		if err != nil {
			return result, nil, err
		}
		// klog.Info("1 returning batcher create")
		// one operation at a time
		return result, event, nil
	}

	// ensure loader deployment exists
	loader, exists, err := r.hasLoader(ctx, redshiftsink)
	if err != nil {
		return result, nil, err
	}
	if !exists {
		klog.Infof("%v: Creating loader", redshiftsink.Name)
		event, err := r.createLoader(ctx, redshiftsink)
		if err != nil {
			return result, nil, err
		}
		// klog.Info("2 returning loader create")
		// one operation at a time
		return result, event, nil
	}

	// compare spec and update batcher if required
	required, err := r.batcherUpdateRequired(batcher, redshiftsink)
	if err != nil {
		return result, nil, err
	}
	if required {
		klog.Infof("%v: Batcher update required", redshiftsink.Name)
		event, err := r.updateBatcher(ctx, redshiftsink)
		return result, event, err
	}

	// compare spec and update loader if required
	required, err = r.loaderUpdateRequired(loader, redshiftsink)
	if err != nil {
		return result, nil, err
	}
	if required {
		klog.Infof("%v: Loader update required", redshiftsink.Name)
		event, err := r.updateLoader(ctx, redshiftsink)
		return result, event, err
	}
	klog.Infof("%v: Nothing done.", redshiftsink.Name)

	return result, nil, nil
}

func (r *RedshiftSinkReconciler) Reconcile(
	req ctrl.Request) (_ ctrl.Result, reterr error) {

	klog.Infof("Reconciling %+v", req)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var redshiftsink tipocav1.RedshiftSink
	err := r.Get(ctx, req.NamespacedName, &redshiftsink)
	if err != nil {
		return ctrl.Result{
			RequeueAfter: time.Second * 30}, client.IgnoreNotFound(err)
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
	result, event, err := r.reconcile(ctx, &redshiftsink)
	if err != nil {
		err = fmt.Errorf("Failed to reconcile: %s", err)
	}

	// Finally, the event is used to generate a Kubernetes event by
	// calling `Record` and passing in the recorder.
	if event != nil {
		event.Record(r.Recorder)
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
