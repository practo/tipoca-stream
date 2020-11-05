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

	"github.com/go-logr/logr"
	"github.com/practo/klog/v2"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	controller "sigs.k8s.io/controller-runtime/pkg/controller"

	tipocav1 "github.com/practo/tipoca-stream/redshiftsink/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	BatcherSuffix = "-batcher"
)

// RedshiftSinkReconciler reconciles a RedshiftSink object
type RedshiftSinkReconciler struct {
	client.Client
	Log    logr.Logger
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=tipoca.k8s.practo.dev,resources=redshiftsinks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=tipoca.k8s.practo.dev,resources=redshiftsinks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch;create;update;patch;delete

func (r *RedshiftSinkReconciler) Reconcile(req ctrl.Request) (ctrl.Result, error) {
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

	klog.Infof("Reconciling: %+v", req)

	// 2. Create the Batcher Deployment if it doesn’t exist.
	batcherDeployment := &appsv1.Deployment{}
	err = r.Get(
		ctx,
		types.NamespacedName{
			Name:      redshiftsink.Name + BatcherSuffix,
			Namespace: redshiftsink.Namespace,
		},
		batcherDeployment,
	)
	if err != nil && errors.IsNotFound(err) {
		dep := r.deploymentForBatcher(redshiftsink)
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

	// 3. Ensure that the Deployment spec.replica=1 or 0 based on Suspend spec.
	// 4. Update the CRD status using status writer.

	return ctrl.Result{}, nil
}

func (r *RedshiftSinkReconciler) getSecret(
	name, namespace string, labels map[string]string) *corev1.Secret {

	return &corev1.Secret{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Secret",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    labels,
		},
	}
}

func (r *RedshiftSinkReconciler) deploymentForBatcher(
	redshiftsink *tipocav1.RedshiftSink) *appsv1.Deployment {

	var replicas int32
	if redshiftsink.Spec.Batcher.Suspend {
		replicas = 0
	} else {
		replicas = 1
	}

	labels := map[string]string{
		"app":                          "redshiftsink-batcher",
		"app.kubernetes.io/instance":   redshiftsink.Name,
		"app.kubernetes.io/managed-by": "redshiftsink-operator",
		"practo.dev/kind":              "RedshiftSink",
		"practo.dev/name":              redshiftsink.Name,
	}

	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      redshiftsink.Name + BatcherSuffix,
			Namespace: redshiftsink.Namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Strategy: appsv1.DeploymentStrategy{
				Type: appsv1.RecreateDeploymentStrategyType,
			},
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Image: "practodev/redshiftbatcher:latest",
						Name:  redshiftsink.Name + BatcherSuffix,
					}},
				},
			},
		},
	}

	ctrl.SetControllerReference(redshiftsink, dep, r.Scheme)
	return dep
}

func (r *RedshiftSinkReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&tipocav1.RedshiftSink{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: 1}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Secret{}).
		Complete(r)
}
