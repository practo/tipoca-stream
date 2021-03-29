package controllers

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	hashstructure "github.com/mitchellh/hashstructure/v2"
	klog "github.com/practo/klog/v2"
	tipocav1 "github.com/practo/tipoca-stream/redshiftsink/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	resource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	types "k8s.io/apimachinery/pkg/types"
	client "sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	InstanceLabel  = "app.kubernetes.io/instance"
	InstanceName   = "practo.dev/name"
	SinkGroupLabel = "practo.dev/sinkgroup"
	RskResource    = "practo.dev/resource"
)

type deploymentSpec struct {
	name        string
	namespace   string
	labels      map[string]string
	replicas    *int32
	resources   *corev1.ResourceRequirements
	tolerations *[]corev1.Toleration
	image       string
	args        []string
}

type configMapSpec struct {
	name       string
	namespace  string
	volumeName string
	mountPath  string
	subPath    string
	data       map[string]string
	labels     map[string]string
}

func configFromSpec(configSpec configMapSpec) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configSpec.name,
			Namespace: configSpec.namespace,
			Labels:    configSpec.labels,
		},
		Data: configSpec.data,
	}
}

func deploymentFromSpec(
	deploySpec deploymentSpec,
	configSpec configMapSpec,
) *appsv1.Deployment {
	d := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deploySpec.name,
			Namespace: deploySpec.namespace,
			Labels:    deploySpec.labels,
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
							Name:            deploySpec.name,
							Image:           deploySpec.image,
							ImagePullPolicy: corev1.PullAlways, // PullIfNotPresent
							Args:            deploySpec.args,
							VolumeMounts: []corev1.VolumeMount{
								corev1.VolumeMount{
									MountPath: configSpec.mountPath,
									SubPath:   configSpec.subPath,
									Name:      configSpec.volumeName,
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						corev1.Volume{
							Name: configSpec.volumeName,
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: configSpec.volumeName,
									},
								},
							},
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

func getHashStructure(v interface{}) (string, error) {
	h, err := hashstructure.Hash(
		v,
		hashstructure.FormatV2,
		&hashstructure.HashOptions{SlicesAsSets: true},
	)
	if err != nil {
		return "", err
	}
	hash := fmt.Sprintf("%d", h)

	return hash[:6], nil
}

func toIntPtr(i int) *int {
	return &i
}

func toInt32Ptr(i int32) *int32 {
	return &i
}

func toQuantityPtr(r resource.Quantity) *resource.Quantity {
	return &r
}

func sortStringSlice(t []string) {
	sort.Sort(sort.StringSlice(t))
}

// getDefaultLabels gives back the default labels for the crd resources
func getDefaultLabels(
	instance, sinkGroup, objectName string,
	rskResource string) map[string]string {

	return map[string]string{
		"app":                          "redshiftsink",
		InstanceLabel:                  instance,
		InstanceName:                   objectName,
		SinkGroupLabel:                 sinkGroup,
		RskResource:                    rskResource,
		"app.kubernetes.io/managed-by": "redshiftsink-operator",
		"practo.dev/kind":              "RedshiftSink",
	}
}

// replicas for the crd resources batcher and loader are boolean, either 1 or 0
func getReplicas(suspend bool, totalGroups, totalTopics int) int32 {
	if suspend {
		return 0
	}

	if totalGroups == 0 {
		return 0
	}

	if totalTopics == 0 {
		return 0
	}

	return 1
}

func cacheValid(validity time.Duration, lastCachedTime *int64) bool {
	if lastCachedTime == nil {
		return false
	}

	if (*lastCachedTime + validity.Nanoseconds()) > time.Now().UnixNano() {
		return true
	}

	return false
}

func makeLoaderTopics(prefix string, topics []string) []string {
	var prefixedTopics []string
	for _, topic := range topics {
		prefixedTopics = append(prefixedTopics, prefix+topic)
	}

	return prefixedTopics
}

func fullMatchRegexForTopic(topic string) string {
	return "^" + topic + "$"
}

func expandTopicsToRegex(topics []string) string {
	if len(topics) == 0 {
		return ""
	}
	sortStringSlice(topics)

	fullMatchRegex := ""
	for _, topic := range topics {
		fullMatchRegex = fullMatchRegex + fullMatchRegexForTopic(topic) + ","
	}

	return strings.TrimSuffix(fullMatchRegex, ",")
}

func serviceName(name, namespace string) types.NamespacedName {
	return types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}
}

func toMap(s []string) map[string]bool {
	m := make(map[string]bool)
	for _, r := range s {
		m[r] = true
	}

	return m
}

func appendIfMissing(slice []string, elementToAdd string) []string {
	for _, element := range slice {
		if element == elementToAdd {
			return slice
		}
	}
	return append(slice, elementToAdd)
}

func removeFromSlice(slice []string, elementToRemove string) []string {
	new := []string{}
	for _, element := range slice {
		if element != elementToRemove {
			new = append(new, element)
		}
	}
	return new
}

func subSetSlice(small, big []string) bool {
	m := toMap(big)
	for _, e := range small {
		_, ok := m[e]
		if !ok {
			return false
		}
	}
	return true
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

func configSpecEqual(
	current *corev1.ConfigMap,
	desired *corev1.ConfigMap,
) bool {
	if !reflect.DeepEqual(current.Data, desired.Data) {
		return false
	}

	return true
}

func deploymentSpecEqual(
	current *appsv1.Deployment,
	desired *appsv1.Deployment,
) bool {
	if !reflect.DeepEqual(
		current.Spec.Template.Labels,
		desired.Spec.Template.Labels) {
		klog.V(5).Infof(
			"currentLabels: %v, desiredLabels: %v\n",
			current.Spec.Template.Labels,
			desired.Spec.Template.Labels,
		)
		klog.Infof("%v labels require update!", desired.Name)
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

func listDeployments(
	ctx context.Context,
	clientCrudder client.Client,
	instance string,
	sinkGroup string,
	namespace string,
	rskName string,
) (
	*appsv1.DeploymentList,
	error,
) {
	list := &appsv1.DeploymentList{}
	options := []client.ListOption{
		client.InNamespace(namespace),
		client.MatchingLabels{
			InstanceLabel:  instance,
			SinkGroupLabel: sinkGroup,
			RskResource:    rskName,
		},
	}
	err := clientCrudder.List(ctx, list, options...)
	if err != nil {
		return nil, err
	}

	return list, nil
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
		klog.Errorf(
			"Failed to update Deployment: %s/%s, err: %v\n",
			deployment.Namespace, deployment.Name, err)
		return nil, err
	}

	return &DeploymentUpdatedEvent{
		Object: redshiftsink,
		Name:   deployment.Name,
	}, nil

}

func deleteDeployment(
	ctx context.Context,
	clientCrudder client.Client,
	deployment *appsv1.Deployment,
	redshiftsink *tipocav1.RedshiftSink) (*DeploymentDeletedEvent, error) {

	err := clientCrudder.Delete(
		ctx,
		deployment,
		client.PropagationPolicy(metav1.DeletePropagationForeground),
	)
	if err != nil {
		klog.Errorf(
			"Failed to delete Deployment: %s/%s, err: %v\n",
			deployment.Namespace, deployment.Name, err)
		return nil, err
	}

	return &DeploymentDeletedEvent{
		Object: redshiftsink,
		Name:   deployment.Name,
	}, nil
}

func getConfigMap(
	ctx context.Context,
	client client.Client,
	name string,
	namespace string) (*corev1.ConfigMap, bool, error) {

	configMap := &corev1.ConfigMap{}
	err := client.Get(ctx, serviceName(name, namespace), configMap)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// got the expected err of not found
			return nil, false, nil
		}
		// unexpected err, should return err
		return nil, false, err
	}
	return configMap, true, nil
}

func listConfigMaps(
	ctx context.Context,
	clientCrudder client.Client,
	instance string,
	sinkGroup string,
	namespace string,
	rskName string,
) (
	*corev1.ConfigMapList,
	error,
) {
	list := &corev1.ConfigMapList{}
	options := []client.ListOption{
		client.InNamespace(namespace),
		client.MatchingLabels{
			InstanceLabel:  instance,
			SinkGroupLabel: sinkGroup,
			RskResource:    rskName,
		},
	}
	err := clientCrudder.List(ctx, list, options...)
	if err != nil {
		return nil, err
	}

	return list, nil
}

func createConfigMap(
	ctx context.Context,
	client client.Client,
	configMap *corev1.ConfigMap,
	redshiftsink *tipocav1.RedshiftSink) (*ConfigMapCreatedEvent, error) {

	err := client.Create(ctx, configMap)
	if err != nil {
		klog.Errorf("Failed to create configMap: %s/%s, err: %v\n",
			configMap.Namespace, configMap.Name, err)
		return nil, err
	}

	return &ConfigMapCreatedEvent{
		Object: redshiftsink,
		Name:   configMap.Name,
	}, nil
}

func deleteConfigMap(
	ctx context.Context,
	client client.Client,
	configMap *corev1.ConfigMap,
	redshiftsink *tipocav1.RedshiftSink) (*ConfigMapDeletedEvent, error) {

	err := client.Delete(ctx, configMap)
	if err != nil {
		klog.Errorf(
			"Failed to delete ConfigMap: %s/%s, err: %v\n",
			configMap.Namespace, configMap.Name, err)
		return nil, err
	}

	return &ConfigMapDeletedEvent{
		Object: redshiftsink,
		Name:   configMap.Name,
	}, nil
}
