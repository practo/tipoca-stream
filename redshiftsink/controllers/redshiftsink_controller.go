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
	"os"
	"strings"

	"github.com/go-logr/logr"
	"github.com/practo/klog/v2"
	tipocav1 "github.com/practo/tipoca-stream/redshiftsink/api/v1"
	k8s "github.com/practo/tipoca-stream/redshiftsink/pkg/k8s"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	controller "sigs.k8s.io/controller-runtime/pkg/controller"
)

const (
	BatcherSuffix    = "-batcher"
	BatcherEnvPrefix = "BATCHER_"
)

// RedshiftSinkReconciler reconciles a RedshiftSink object
type RedshiftSinkReconciler struct {
	client.Client

	Log             logr.Logger
	Scheme          *runtime.Scheme
	ResourceManager *k8s.ResourceManager
}

// +kubebuilder:rbac:groups=tipoca.k8s.practo.dev,resources=redshiftsinks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=tipoca.k8s.practo.dev,resources=redshiftsinks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch;create;update;patch;delete

// addSpecifiedEnvVars gets the specified envs and adds the existing envs
// over it. Passed labels should override the default labels.
func addSpecifiedEnvVars(s []corev1.EnvVar, e []corev1.EnvVar) []corev1.EnvVar {
	if len(s) == 0 {
		return e
	}
	for _, v := range s {
		e = append(e, v)
	}
	return e
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

// addDefaultLabels gets the default labels and apply the existing labels
// over it. Passed labels should override the default labels.
func addDefaultLabels(app string, labels map[string]string) map[string]string {
	if labels == nil {
		labels = map[string]string{}
	}
	outLabels := getDefaultLabels(app)
	for k, v := range labels {
		outLabels[k] = v
	}
	return outLabels
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

// getDeployment gives back a deployment object for a redshiftink crd object
func getDeployment(
	redshiftsink *tipocav1.RedshiftSink,
	podTemplate corev1.PodTemplateSpec,
	defaultSpec defaultContainerSpec) *appsv1.Deployment {

	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      defaultSpec.deploymentName,
			Namespace: redshiftsink.Namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: defaultSpec.replicas,
			Strategy: appsv1.DeploymentStrategy{
				Type: appsv1.RecreateDeploymentStrategyType,
			},
			Selector: &metav1.LabelSelector{
				MatchLabels: defaultSpec.labels,
			},
			Template: podTemplate,
		},
	}
}

// applyDefaultsToPodTemplateSpec takes the current pod template spec and applies
// the default field to the specified pod template spec
func applyDefaultsToPodTemplateSpec(
	podTemplateSpec corev1.PodTemplateSpec,
	defaultSpec defaultContainerSpec,
	envVars []corev1.EnvVar) corev1.PodTemplateSpec {

	podTemplateSpec.Labels = addDefaultLabels(
		defaultSpec.podName,
		podTemplateSpec.Labels,
	)

	if len(podTemplateSpec.Spec.Containers) == 0 {
		podTemplateSpec.Spec.Containers = []corev1.Container{
			corev1.Container{
				Image: defaultSpec.image,
				Name:  defaultSpec.podName,
				Env:   envVars,
			},
		}
	} else {
		podTemplateSpec.Spec.Containers[0].Env = addSpecifiedEnvVars(
			podTemplateSpec.Spec.Containers[0].Env,
			envVars,
		)
	}

	return podTemplateSpec
}

type defaultContainerSpec struct {
	podName        string
	deploymentName string
	image          string
	replicas       *int32
	labels         map[string]string
}

// getBatcherDeployment constructs the deployment spec based on the crd spec
func (r *RedshiftSinkReconciler) getBatcherDeployment(
	redshiftsink *tipocav1.RedshiftSink,
	envVars []corev1.EnvVar) *appsv1.Deployment {

	defaultSpec := defaultContainerSpec{
		podName:        redshiftsink.Name + BatcherSuffix,
		deploymentName: redshiftsink.Name + BatcherSuffix,
		image:          "practodev/redshiftbatcher:latest",
		replicas:       getReplicas(redshiftsink.Spec.Batcher.Suspend),
		labels:         redshiftsink.Spec.Batcher.PodTemplate.Labels,
	}

	applyDefaultsToPodTemplateSpec(
		redshiftsink.Spec.Batcher.PodTemplate,
		defaultSpec,
		envVars,
	)

	dep := getDeployment(
		redshiftsink,
		redshiftsink.Spec.Batcher.PodTemplate,
		defaultSpec,
	)

	ctrl.SetControllerReference(redshiftsink, dep, r.Scheme)
	return dep
}

// getEnvVars makes EnvVars by picking environment variables by prefix
// and also adds secret referrences to the EnvVars
func getEnvVars(prefix string, secretRefName string,
	secretKeys []string) []corev1.EnvVar {

	var envVars []corev1.EnvVar

	for _, keyValue := range os.Environ() {
		if !strings.HasPrefix(prefix, keyValue) {
			continue
		}

		v := strings.Split(keyValue, "=")
		if len(v) < 2 {
			klog.Warningf("Expected both key value for %v\n", keyValue)
			continue
		}
		envVars = append(envVars, corev1.EnvVar{
			Name:  v[0],
			Value: v[1],
		})
	}

	for _, secretKey := range secretKeys {
		optional := false
		envVars = append(envVars, corev1.EnvVar{
			Name: prefix + strings.ToUpper(secretKey),
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: secretRefName,
					},
					Key:      secretKey,
					Optional: &optional,
				},
			},
		})
	}

	return envVars
}

// getSecretKeys gets the of all secret keys
func (r *RedshiftSinkReconciler) getSecretKeys(
	seretRefName, secretRefNamespace string) ([]string, error) {

	secret, err := r.ResourceManager.Secret(
		seretRefName,
		secretRefNamespace,
		nil,
		corev1.SecretTypeOpaque,
		nil,
	).Get()
	if err != nil {
		return []string{}, err
	}

	secretKeys := make([]string, 0, len(secret.Data))
	for k, _ := range secret.Data {
		secretKeys = append(secretKeys, k)
	}

	return secretKeys, nil
}

// Reconcile is the main reconciliation logic perform on every crd object
func (r *RedshiftSinkReconciler) Reconcile(
	req ctrl.Request) (ctrl.Result, error) {

	ctx := context.Background()
	_ = r.Log.WithValues("redshiftsink", req.NamespacedName)

	// Fetch the RedshiftSink instance
	redshiftsink := &tipocav1.RedshiftSink{}
	err := r.Get(ctx, req.NamespacedName, redshiftsink)
	if err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile
			// request. Owned objects are automatically garbage collected.
			// For additional cleanup logic use finalizers.
			// Return and don't requeue
			klog.Infof("Redshiftsink: %s not found\n", req)
			klog.Infof("Ignoring since object: %s must be deleted\n", req)
			return ctrl.Result{}, nil
		}
		// Error reading the object - requeue the request.
		klog.Errorf("Failed to get Redshiftsink, err: %v\n", err)
		return ctrl.Result{}, err
	}

	// Fetch the secret keys from the secret
	secretKeys, err := r.getSecretKeys(
		redshiftsink.Spec.SecretRefName,
		redshiftsink.Spec.SecretRefNamespace,
	)
	if err != nil {
		klog.Error(err)
		return ctrl.Result{}, err
	}

	batcherEnvVars := getEnvVars(
		BatcherEnvPrefix,
		redshiftsink.Spec.SecretRefName,
		secretKeys,
	)

	// Create the Batcher Deployment if it doesn’t exist.
	batcherDeployment := &appsv1.Deployment{}
	err = r.Get(ctx,
		types.NamespacedName{
			Name:      redshiftsink.Name + BatcherSuffix,
			Namespace: redshiftsink.Namespace,
		}, batcherDeployment,
	)
	if err != nil && errors.IsNotFound(err) {
		dep := r.getBatcherDeployment(redshiftsink, batcherEnvVars)
		klog.Infof("Creating Deployment: %s/%s\n", dep.Namespace, dep.Name)
		err = r.Create(ctx, dep)
		if err != nil {
			klog.Errorf("Failed to create Deployment: %s/%s, err: %v\n",
				dep.Namespace, dep.Name, err)
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	} else if err != nil {
		klog.Errorf("Failed to get Deployment, err: %v\n", err)
		return ctrl.Result{}, err
	}

	_ = batcherDeployment

	// TODO:
	// Ensure that the Deployment spec.replica=1 or 0 based on Suspend spec.
	// Update the CRD status using status writer.

	return ctrl.Result{}, nil
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
