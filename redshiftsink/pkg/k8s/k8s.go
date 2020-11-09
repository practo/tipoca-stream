package k8s

import (
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	resource "github.com/practo/tipoca-stream/redshiftsink/pkg/k8s/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Resource is an interface for k8s resources. All the k8s resources supported
// by this package must implement this interface.
type Resource interface {
	// Get tries to get an existing resource if any, else returns an error.
	Get() (interface{}, error)

	// Create creates the resource.
	Create() error

	// Delete deletes the resource.
	Delete() error
}

// ResourceManager is k8s resource manager. It provides methods to easily manage
// k8s resources.
type ResourceManager struct {
	client client.Client
	labels map[string]string
}

// TODO: Maybe add a Namespaced ResourceManager to make the namespace of the
// resources implicit.

// NewResourceManager returns an initialized k8s ResourceManager.
func NewResourceManager(client client.Client) *ResourceManager {
	return &ResourceManager{client: client}
}

// SetLabels sets a label for the resources created by the resource manager.
func (r *ResourceManager) SetLabels(labels map[string]string) *ResourceManager {
	if labels == nil {
		labels = map[string]string{}
	}
	r.labels = labels
	return r
}

// GetLabels returns labels of the resource manager.
func (r *ResourceManager) GetLabels() map[string]string {
	return r.labels
}

// Deployment returns a Deployment object.
func (r ResourceManager) Deployment(
	name, namespace string,
	labels map[string]string,
	spec *appsv1.DeploymentSpec) *resource.Deployment {

	return resource.NewDeployment(
		r.client, name, namespace, r.combineLabels(labels), spec)
}

// Secret returns a Secret object.
func (r ResourceManager) Secret(
	name, namespace string,
	labels map[string]string,
	secType corev1.SecretType,
	data map[string][]byte) *resource.Secret {

	return resource.NewSecret(
		r.client, name, namespace, r.combineLabels(labels), secType, data)
}

func (r ResourceManager) combineLabels(
	labels map[string]string) map[string]string {

	// Combine the common labels and resource specific labels.
	if labels == nil {
		labels = map[string]string{}
	}
	for k, v := range r.labels {
		labels[k] = v
	}
	return labels
}
