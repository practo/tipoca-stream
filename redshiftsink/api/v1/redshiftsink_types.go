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

package v1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.
// Any new fields you add must have json tags for the fields to be serialized.

// RedshiftPodTemplateSpec supports a subset of `v1/PodTemplateSpec`
// that the operator explicitly permits. We don't
// want to allow a user to set arbitrary features on our underlying pods.
type RedshiftPodTemplateSpec struct {
	// Image for the underlying pod
	// +optional
	Image *string `json:"image,omitempty"`

	// Resources is for configuring the compute resources required
	// +optional
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`

	// Toleartions the underlying pods should have
	// +optional
	Tolerations *[]corev1.Toleration `json:"tolerations,omitempty"`
}

// RedshiftBatcherSpec defines the desired state of RedshiftBatcher
type RedshiftBatcherSpec struct {
	// Supsend when turned on makes sure no batcher pods
	// are running for this CRD object. Default: false
	Suspend bool `json:"suspend,omitempty"`

	// Max configurations for the batcher to batch
	MaxSize        int `json:"maxSize"`
	MaxWaitSeconds int `json:"maxWaitSeconds"`

	// Mask when turned on enables masking of the data
	// Default: false
	// +optional
	Mask bool `json:"mask"`
	// +optional
	MaskFile string `json:"maskFile"`
	// +optional

	// Template describes the pods that will be created.
	// if this is not specifed, a default pod template is created
	// +optional
	PodTemplate *RedshiftPodTemplateSpec `json:"podTemplate,omitempty"`
}

// RedshiftLoaderSpec defines the desired state of RedshifLoader
type RedshiftLoaderSpec struct {
	// Supsend when turned on makes sure no batcher pods
	// are running for this CRD object. Default: false
	Suspend bool `json:"suspend,omitempty"`

	// Max configurations for the loader to batch and load
	MaxSize        int `json:"maxSize"`
	MaxWaitSeconds int `json:"maxWaitSeconds"`

	// RedshiftSchema to sink the data in
	RedshiftSchema string `json:"redshiftSchema"`

	// Template describes the pods that will be created.
	// if this is not specifed, a default pod template is created
	// +optional
	PodTemplate *RedshiftPodTemplateSpec `json:"podTemplate,omitempty"`
}

// RedshiftSinkSpec defines the desired state of RedshiftSink
type RedshiftSinkSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// Secrets to be used by the batcher pod
	// Default: the secret name and namespace provided in the controller flags
	SecretRefName      string `json:"secretRefName"`
	SecretRefNamespace string `json:"secretRefNamespace"`

	// Kafka configurations like consumer group and topics to watch
	KafkaBrokers      string `json:"kafkaBrokers"`
	KafkaTopicRegexes string `json:"kafkaTopicRegexes"`
	// +optional
	KafkaLoaderTopicPrefix string `json:"kafkaLoaderTopicPrefix,omitempty"`

	Batcher RedshiftBatcherSpec `json:"batcher"`
	Loader  RedshiftLoaderSpec  `json:"loader"`
}

// MaskPhase is a label for the condition of a masking at the current time.
type MaskPhase string

// These are the valid statuses of masking.
const (
	// MaskActive tells the current MaskFileVersion
	MaskActive MaskPhase = "Active"

	// MaskReloading tells the current MaskFileVersion is not same as the
	// MaskFileVersion specified(i.e. the latest mask config) and is in the
	// phase of transition to the new mask configuration. At the end of
	// transition the MaskFileVersion in the status is updated to the spec.
	MaskReloading MaskPhase = "Reloading"
)

// TopicMaskStatus store the mask status of a single topic
type TopicMaskStatus struct {
	// MaskFileVersion is the current mask configuration being used
	// +optional
	MaskFileVersion string `json:"maskFileVersion,omitempty"`
	// Phase determines the
	// +optional
	Phase MaskPhase `json:"phase,omitempty"`
}

type MaskStatus struct {
	// CurrentMaskStatus stores the current status of mask status of topics
	// +optional
	CurrentMaskStatus map[string]TopicMaskStatus `json:"currentMaskStatus,omitempty"`

	// DesiredMaskStatus stores the current status of mask status of topics
	// +optional
	DesiredMaskStatus map[string]TopicMaskStatus `json:"desiredMaskStatus,omitempty"`

	// CurrentMaskVersion stores the mask version which was completely rolled
	// out in all the topics.
	// +optional
	CurrenMaskVersion *string `json:"currentMaskVersion,omitempty"`

	// DesiredMaskVersion stores the latest mask version which should be
	// completely rolled out in all the topics.
	// +optional
	DesiredMaskVersion *string `json:"desiredMaskedVersion,omitempty"`
}

// RedshiftSinkStatus defines the observed state of RedshiftSink
type RedshiftSinkStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// MaskStatus stores the status of masking for topics if masking is enabled
	// +optional
	MaskStatus *MaskStatus `json:"maskStatus"`
}

// +kubebuilder:resource:path=redshiftsinks,shortName=rsk;rsks
// +kubebuilder:object:root=true

// +kubebuilder:subresource:status
// RedshiftSink is the Schema for the redshiftsinks API
type RedshiftSink struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RedshiftSinkSpec   `json:"spec,omitempty"`
	Status RedshiftSinkStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// RedshiftSinkList contains a list of RedshiftSink
type RedshiftSinkList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RedshiftSink `json:"items"`
}

func init() {
	SchemeBuilder.Register(&RedshiftSink{}, &RedshiftSinkList{})
}
