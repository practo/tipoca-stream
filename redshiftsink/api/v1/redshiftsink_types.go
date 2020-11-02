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
	consumer "github.com/practo/tipoca-stream/redshiftsink/pkg/consumer"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// RedshiftBatcher defines the desired state of RedshiftBatcher
type RedshiftBatcherSpec struct {
	// Supsend when turned makes sure no batcher pods
	// are running for this CRD object. Default: false
	Suspend bool `json:"suspend,omitempty"`

	// Secrets to be used by the batcher pod
	// Default: the secret name and namespace provided in the controller flags
	SecretRefName      string `json:"secretRefName"`
	SecretRefNamespace string `json:"secretRefNamespace"`

	// Max configurations for the batcher to batch
	MaxSize        int `json:"maxSize"`
	MaxWaitSeconds int `json:"maxWaitSeconds"`

	// Mask when turned on enables masking of the data
	// Default: false
	Mask               string `json:"mask"`
	MaskConfigDir      string `json:"maskConfigDir"`
	MaskConfigFileName string `json:"maskConfigFileName"`

	// Kafka configurations like consumer group and topics to watch
	Kafka consumer.KafkaConfig `json:"kafka"`
}

// RedshiftSinkSpec defines the desired state of RedshiftSink
type RedshiftSinkSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	Batcher RedshiftBatcherSpec `json:"batcher"`
}

// RedshiftSinkStatus defines the observed state of RedshiftSink
type RedshiftSinkStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file
}

// +kubebuilder:object:root=true

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
