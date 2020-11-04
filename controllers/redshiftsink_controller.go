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
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tipocav1 "github.com/practo/tipoca-stream/redshiftsink/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
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
	_ = context.Background()
	_ = r.Log.WithValues("redshiftsink", req.NamespacedName)

	// 1. Create the ConfigMap for the batcher.
	// 2. Create the Deployment Batcher if it doesn’t exist.
	// 3. Ensure that the Deployment spec.replica=1 or 0 based on Suspend spec.
	// 4. Update the CRD status using status writer.

	return ctrl.Result{}, nil
}

func (r *RedshiftSinkReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&tipocav1.RedshiftSink{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Secret{}).
		Complete(r)
}
