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

// Deployment is used to specify how many topics will run together in a unit
// and how much resources needs to be given to them.
type DeploymentUnit struct {
	// MaxTopics specify the maximum number of topics that
	// can be part of this unit of deployment.
	MaxTopics *int `json:"maxTopics,omitempty"`

	// PodTemplate describes the specification for the unit.
	// +optional
	PodTemplate *RedshiftPodTemplateSpec `json:"podTemplate,omitempty"`
}

type SinkGroupSpec struct {
	// MaxBytesPerBatch is the maximum bytes per batch.
	MaxBytesPerBatch *int `json:"maxBytesPerBatch,omitempty"`
	// MaxWaitSeconds is the maximum time to wait before making a batch,
	// make a batch if MaxBytesPerBatch is not hit during MaxWaitSeconds.
	MaxWaitSeconds *int `json:"maxWaitSeconds"`
	// MaxConcurrency is the maximum no, of batch processors to run concurrently.
	MaxConcurrency *int `json:"maxConcurrency,omitempty"`
	// MaxProcessingTime is the max time in ms required to consume one message.
	// Defaults to 1000ms
	MaxProcessingTime *int32 `json:"maxProcessingTime,omitempty"`

	// DeploymentUnit is to specify the configuration of the unit of deployment
	// This helps the user to specify how many topics with what resources
	// can run in one unit of Deployment. Based on this the operator decides
	// how many deployment units would be launched. This is useful in the first
	// time sink of redshiftsink resources having huge number of topics.
	// Check #167 to understand the need of a unit specification.
	DeploymentUnit *DeploymentUnit `json:"deploymentUnit,omitempty"`
}

// SinkGroup is the group of batcher and loader pods based on the
// mask version, target table and the topic release status. These grouped
// pods can require different configuration to sink the resources. Pods of batcher
// and loader can specify their sink group configuration using SinkGroupSpec.
// For example:
// The first time sink of a table requires different values for MaxBytesPerBatch
// and different pod resources than the realtime differential sink ones.
// If All is specified and none of the others are specified, all is used
// for Main, Reload and ReloadDupe SinkGroup. If others are specified then
// they take precedence over all. For example if you have specified All and
// Main, then for the MainSinkGroup Main is used and not All.
type SinkGroup struct {
	All        *SinkGroupSpec `json:"all,omitempty"`
	Main       *SinkGroupSpec `json:"main,omitempty"`
	Reload     *SinkGroupSpec `json:"reload,omitempty"`
	ReloadDupe *SinkGroupSpec `json:"reloadDupe,omitempty"`
}

// RedshiftBatcherSpec defines the desired state of RedshiftBatcher
type RedshiftBatcherSpec struct {
	// Supsend is used to suspend batcher pods. Defaults to false.
	Suspend bool `json:"suspend,omitempty"`

	// Mask when turned on enables masking of the data. Defaults to false
	// +optional
	Mask bool `json:"mask"`
	// MaskFile to use to apply mask configurations
	// +optional
	MaskFile string `json:"maskFile"`
	// +optional

	// SinkGroup contains the specification for main, reload and reloadDupe
	// sinkgroups. Operator uses 3 groups to perform Redshiftsink. The topics
	// which have never been released is part of Reload SinkGroup, the topics
	// which gets released moves to the Main SinkGroup. ReloadDupe SinkGroup
	// is used to give realtime upadates to the topics which are reloading.
	// Defaults are there for all sinkGroups if none is specifed.
	SinkGroup *SinkGroup `json:"sinkGroup,omitempty"`

	// Deprecated all of the below spec in favour of SinkGroup #167
	MaxSize        int  `json:"maxSize"`
	MaxWaitSeconds int  `json:"maxWaitSeconds"`
	MaxConcurrency *int `json:"maxConcurrency,omitempty"`
	// MaxProcessingTime is the sarama configuration MaxProcessingTime
	// It is the max time in milliseconds required to consume one message.
	// Defaults to 1000ms
	MaxProcessingTime *int32 `json:"maxProcessingTime,omitempty"`
	// PodTemplate describes the pods that will be created.
	// if this is not specifed, a default pod template is created
	// +optional
	PodTemplate *RedshiftPodTemplateSpec `json:"podTemplate,omitempty"`
}

// RedshiftLoaderSpec defines the desired state of RedshifLoader
type RedshiftLoaderSpec struct {
	// Supsend when turned on makes sure no batcher pods
	// are running for this CRD object. Default: false
	Suspend bool `json:"suspend,omitempty"`

	// Max configurations for the loader to batch the load
	MaxSize        int `json:"maxSize"`
	MaxWaitSeconds int `json:"maxWaitSeconds"`

	// MaxProcessingTime is the sarama configuration MaxProcessingTime
	// It is the max time in milliseconds required to consume one message.
	// Defaults to 600000ms (10mins)
	MaxProcessingTime *int32 `json:"maxProcessingTime,omitempty"`

	// RedshiftSchema to sink the data in
	RedshiftSchema string `json:"redshiftSchema"`
	// RedshiftMaxOpenConns is the maximum open connections allowed
	RedshiftMaxOpenConns *int `json:"redshiftMaxOpenConns,omitempty"`
	// RedshiftMaxIdleConns is the maximum idle connections allowed
	RedshiftMaxIdleConns *int `json:"redshiftMaxIdleConns,omitempty"`
	// RedshiftGroup to give the access to when new topics gets released
	RedshiftGroup *string `json:"redshiftGroup"`

	// PodTemplate describes the pods that will be created.
	// if this is not specifed, a default pod template is created
	// +optional
	PodTemplate *RedshiftPodTemplateSpec `json:"podTemplate,omitempty"`
}

// RedshiftSinkSpec defines the desired state of RedshiftSink
type RedshiftSinkSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// Secrets to be used
	// Default: the secret name and namespace provided in the controller flags
	// +optional
	SecretRefName *string `json:"secretRefName"`
	// +optional
	// Secrets namespace to be used
	// Default: the secret name and namespace provided in the controller flags
	SecretRefNamespace *string `json:"secretRefNamespace"`

	// Kafka configurations like consumer group and topics to watch
	KafkaBrokers string `json:"kafkaBrokers"`
	// +optional
	KafkaVersion      string `json:"kafkaVersion"`
	KafkaTopicRegexes string `json:"kafkaTopicRegexes"`
	// +optional
	KafkaLoaderTopicPrefix string `json:"kafkaLoaderTopicPrefix"`

	Batcher RedshiftBatcherSpec `json:"batcher"`
	Loader  RedshiftLoaderSpec  `json:"loader"`

	// ReleaseCondition specifies the release condition to consider a topic
	// realtime and to consider the topci to be moved from reloading to released
	// This is relevant only if masking is turned on in mask configuration.
	// It is used for live mask reloading.
	// +optional
	ReleaseCondition *ReleaseCondition `json:"releaseCondition"`

	// TopicReleaseCondition is considered instead of ReleaseCondition
	// if it is defined for a topic. This is used for topics which
	// does not work well with central ReleaseCondition for all topics
	// +optional
	TopicReleaseCondition map[string]ReleaseCondition `json:"topicReleaseCondition"`
}

type ReleaseCondition struct {
	// MaxBatcherLag is the maximum lag the batcher consumer group
	// shoud have to be be considered to be operating in realtime and
	// to be considered for release.
	MaxBatcherLag *int64 `json:"maxBatcherLag"`

	// MaxLoaderLag is the maximum lag the loader consumer group
	// shoud have to be be considered to be operating in realtime and
	// to be considered for release.
	MaxLoaderLag *int64 `json:"maxLoaderLag"`
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

	// MaskRealtime tells the SinkGroup has been reloaded with new mask
	// version and is realtime and it is waiting to be released
	MaskRealtime MaskPhase = "Realtime"
)

// TopicMaskStatus store the mask status of a single topic
type TopicMaskStatus struct {
	// MaskFileVersion is the current mask configuration being worked on
	// +optional
	Version string `json:"version,omitempty"`
	// Phase determines the
	// +optional
	Phase MaskPhase `json:"phase,omitempty"`

	// ReleasedVersion is the last released version for the topic
	// +optional
	ReleasedVersion *string `json:"releasedVersion,omitempty"`
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
	CurrentMaskVersion *string `json:"currentMaskVersion,omitempty"`

	// DesiredMaskVersion stores the latest mask version which should be
	// completely rolled out in all the topics.
	// +optional
	DesiredMaskVersion *string `json:"desiredMaskedVersion,omitempty"`
}

type Group struct {
	// LoaderTopicPrefix stores the name of the loader topic prefix
	LoaderTopicPrefix string `json:"loaderTopicPrefix"`

	// LoaderCurrentOffset stores the last read current offset of the consumer group
	// This is required to determine if the consumer group has performed any
	// processing in the past. As for low throughput topics,
	// the consumer group disappears and distinguishing between never created
	// and inactive consumer groups become difficult. Which leads to low
	// throughput consumer groups not getting moved to realtime from reloading.
	// TODO: This is not dead field once a group moves to released and
	// should be cleaned after that(status needs to be updated)
	LoaderCurrentOffset *int64 `json:"currentOffset"`

	// ID stores the name of the consumer group for the topic
	// based on this batcher and loader consumer groups are made
	ID string `json:"id"`
}

// RedshiftSinkStatus defines the observed state of RedshiftSink
type RedshiftSinkStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// MaskStatus stores the status of masking for topics if masking is enabled
	// +optional
	MaskStatus *MaskStatus `json:"maskStatus"`

	// TopicGroup stores the group info for the topic
	// +optional
	TopicGroup map[string]Group `json:"topicGroups"`
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
