package controllers

import (
	"context"
	klog "github.com/practo/klog/v2"
	tipocav1 "github.com/practo/tipoca-stream/redshiftsink/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	types "k8s.io/apimachinery/pkg/types"
	"reflect"
	client "sigs.k8s.io/controller-runtime/pkg/client"
	"sort"
	"strings"
)

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

// replicas for the crd resources batcher and loader are boolean, either 1 or 0
func getReplicas(suspend bool) int32 {
	if suspend {
		return 0
	} else {
		return 1
	}
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

func makeLoaderTopics(prefix string, topics []string) []string {
	var prefixedTopics []string
	for _, topic := range topics {
		prefixedTopics = append(prefixedTopics, prefix+topic)
	}

	return prefixedTopics
}

func expandTopicsToRegex(topics []string) string {
	if len(topics) == 0 {
		return ""
	}
	sort.Strings(topics)

	fullMatchRegex := ""
	for _, topic := range topics {
		fullMatchRegex = fullMatchRegex + "^" + topic + "$,"
	}

	return strings.TrimSuffix(fullMatchRegex, ",")
}

func serviceName(name, namespace string) types.NamespacedName {
	return types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}
}

func deploymentSpecEqual(
	current *appsv1.Deployment,
	desired *appsv1.Deployment) bool {

	currentEnvs := current.Spec.Template.Spec.Containers[0].Env
	desiredEnvs := desired.Spec.Template.Spec.Containers[0].Env
	if !reflect.DeepEqual(currentEnvs, desiredEnvs) {
		klog.Infof("%s environments variables required update!", desired.Name)
		return false
	}

	if *current.Spec.Replicas != *desired.Spec.Replicas {
		klog.Infof("%s replicas require update!", desired.Name)
		return false
	}

	currentImage := current.Spec.Template.Spec.Containers[0].Image
	desiredImage := desired.Spec.Template.Spec.Containers[0].Image
	if currentImage != desiredImage {
		klog.Infof("%s image require update!", desired.Name)
		return false
	}

	currentResources := current.Spec.Template.Spec.Containers[0].Resources
	desiredResources := desired.Spec.Template.Spec.Containers[0].Resources
	if !reflect.DeepEqual(currentResources, desiredResources) {
		klog.Infof("%s resources require update!", desired.Name)
		return false
	}

	currentTolerations := current.Spec.Template.Spec.Tolerations
	desiredTolerations := desired.Spec.Template.Spec.Tolerations
	if !reflect.DeepEqual(currentTolerations, desiredTolerations) {
		klog.Infof("%s tolerations require update!", desired.Name)
		return false
	}

	return true
}

func getDeployment(
	ctx context.Context,
	client client.Client,
	name string,
	namespace string) (*appsv1.Deployment, bool, error) {

	deployment := &appsv1.Deployment{}
	err := client.Get(ctx, serviceName(name, namespace), deployment)
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

func createDeployment(
	ctx context.Context,
	client client.Client,
	deployment *appsv1.Deployment,
	redshiftsink *tipocav1.RedshiftSink) (*DeploymentCreatedEvent, error) {

	err := client.Create(ctx, deployment)
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

func updateDeployment(
	ctx context.Context,
	client client.Client,
	deployment *appsv1.Deployment,
	redshiftsink *tipocav1.RedshiftSink) (*DeploymentUpdatedEvent, error) {

	err := client.Update(ctx, deployment)
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

func getSecret(
	ctx context.Context,
	client client.Client,
	name string,
	namespace string) (*corev1.Secret, error) {

	secret := &corev1.Secret{}
	err := client.Get(ctx, serviceName(name, namespace), secret)
	return secret, err
}

func toMap(s []string) map[string]bool {
	m := make(map[string]bool)
	for _, r := range s {
		m[r] = true
	}

	return m
}
