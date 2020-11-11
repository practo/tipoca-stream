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
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// RedshiftBatcherSpec defines the desired state of RedshiftBatcher
type RedshiftBatcherSpec struct {
	// Supsend when turned makes sure no batcher pods
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
	MaskConfigDir string `json:"maskConfigDir"`
	// +optional
	MaskConfigFileName string `json:"maskConfigFileName"`

	// Kafka configurations like consumer group and topics to watch
	KafkaBrokers      string `json:"kafkaBrokers"`
	KafkaGroup        string `json:"kafkaGroup"`
	KafkaTopicRegexes string `json:"kafkaTopicRegexes"`
	// +optional
	KafkaLoaderTopicPrefix string `json:"kafkaLoaderTopicPrefix,omitempty"`

	// Template describes the pods that will be created.
	// if this is not specifed, a default pod template is created
	// +optional
	PodTemplate corev1.PodTemplateSpec `json:"podTemplate,omitempty"`
}

// RedshiftLoaderSpec defines the desired state of RedshifLoader
type RedshiftLoaderSpec struct {
	// Supsend when turned makes sure no batcher pods
	// are running for this CRD object. Default: false
	Suspend bool `json:"suspend,omitempty"`

	// Max configurations for the loader to batch and load
	MaxSize        int `json:"maxSize"`
	MaxWaitSeconds int `json:"maxWaitSeconds"`

	// Kafka configurations like consumer group and topics to watch
	KafkaBrokers      string `json:"kafkaBrokers"`
	KafkaGroup        string `json:"kafkaGroup"`
	KafkaTopicRegexes string `json:"kafkaTopicRegexes"`

	// RedshiftSchema to sink the data in
	RedshiftSchema string `json:"redshiftSchema"`

	// Template describes the pods that will be created.
	// if this is not specifed, a default pod template is created
	// +optional
	PodTemplate corev1.PodTemplateSpec `json:"podTemplate,omitempty"`
}

// RedshiftSinkSpec defines the desired state of RedshiftSink
type RedshiftSinkSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// Secrets to be used by the batcher pod
	// Default: the secret name and namespace provided in the controller flags
	SecretRefName      string `json:"secretRefName"`
	SecretRefNamespace string `json:"secretRefNamespace"`

	Batcher RedshiftBatcherSpec `json:"batcher"`
	Loader  RedshiftLoaderSpec  `json:"loader"`
}

// RedshiftSinkStatus defines the observed state of RedshiftSink
type RedshiftSinkStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file
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
