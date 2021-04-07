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
	resource "k8s.io/apimachinery/pkg/api/resource"
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

// DeploymentUnit is used to specify how many topics will run together in a unit
// and how much resources it needs.
type DeploymentUnit struct {
	// PodTemplate describes the pod specification for the unit.
	// +optional
	PodTemplate *RedshiftPodTemplateSpec `json:"podTemplate,omitempty"`
}

// SinkGroupSpec defines the specification for one of the three sinkgroups:
// 1. MainSinkGroup 2. ReloadSinkGroup 3. ReloadDupeSinkGroup
type SinkGroupSpec struct {
	// MaxSizePerBatch is the maximum size of the batch in bytes, Ki, Mi, Gi
	// Example values: 1000, 1Ki, 100Mi, 1Gi
	// 1000 is 1000 bytes, 1Ki is 1 Killo byte,
	// 100Mi is 100 mega bytes, 1Gi is 1 Giga bytes
	// +optional
	MaxSizePerBatch *resource.Quantity `json:"maxSizePerBatch,omitempty"`
	// MaxWaitSeconds is the maximum time to wait before making a batch,
	// make a batch if MaxSizePerBatch is not hit during MaxWaitSeconds.
	// +optional
	MaxWaitSeconds *int `json:"maxWaitSeconds,omitempty"`
	// MaxConcurrency is the maximum no, of batch processors to run concurrently.
	// This spec is useful when the sink group pod operates in asynchronous mode.
	// Loader pods does not needed this as they are synchronous.
	// +optional
	MaxConcurrency *int `json:"maxConcurrency,omitempty"`
	// MaxProcessingTime is the max time in ms required to consume one message.
	// Defaults for the batcher is 180000ms and loader is 600000ms.
	// +optional
	MaxProcessingTime *int32 `json:"maxProcessingTime,omitempty"`

	// MaxReloadingUnits is the maximum number of units(pods) that can be launched
	// based on the DeploymentUnit specification. Only valid for Reloading SinkGroup.
	// +optional
	MaxReloadingUnits *int32 `json:"maxReloadingUnits,omitempty"`
	// DeploymentUnit(pod) is the unit of deployment for the batcher or the loader.
	// Using this user can specify the amount of resources
	// needed to run them as one unit. Operator calculates the total units
	// based on the total number of topics and this unit spec. This majorly
	// solves the scaling issues described in #167.
	// +optional
	DeploymentUnit *DeploymentUnit `json:"deploymentUnit,omitempty"`
}

// SinkGroup is the group of batcher and loader pods based on the
// mask version, target table and the topic release status. This is the specification
// to allow to have different set of SinkGroupSpec for each type of SinkGroups.
// Explaining the precedence:
// The configuration required for full sink and the realtime sink can be different.
// SinkGroupSpec for each of the type of sink groups helps us provide different
// configurations for each of them. Following are the precedence:
// a) If All is specified and none of the others are specified, All is used for all SinkGroups.
// b) If All and Main both are specified then Main gets used for MainSinkGroup
// c) If All and Reload are specified then Reload gets used for ReloadSinkGroup
// d) If All and ReloadDupe are specified then ReloadDupe gets used for ReloadDupeSinkGroup
// d) If None gets specified then Defaults are used for all of them..
type SinkGroup struct {
	// All specifies a common specification for all SinkGroups
	// +optional
	All *SinkGroupSpec `json:"all,omitempty"`
	// Main specifies the MainSinkGroup specification, overwrites All
	// +optional
	Main *SinkGroupSpec `json:"main,omitempty"`
	// Reload specifies the ReloadSinkGroup specification, overwrites All
	// +optional
	Reload *SinkGroupSpec `json:"reload,omitempty"`
	// ReloadDupe specifies the ReloadDupeSinkGroup specification, overwrites All
	// +optional
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
	MaskFile string `json:"maskFile,omitempty"`
	// +optional

	// SinkGroup contains the specification for main, reload and reloadDupe
	// sinkgroups. Operator uses 3 groups to perform Redshiftsink. The topics
	// which have never been released is part of Reload SinkGroup, the topics
	// which gets released moves to the Main SinkGroup. ReloadDupe SinkGroup
	// is used to give realtime upaates to the topics which are reloading.
	// Defaults are there for all sinkGroups if none is specifed.
	// +optional
	SinkGroup *SinkGroup `json:"sinkGroup,omitempty"`

	// Deprecated all of the below spec in favour of SinkGroup #167
	MaxSize        int  `json:"maxSize,omitempty"`
	MaxWaitSeconds int  `json:"maxWaitSeconds,omitempty"`
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

	// SinkGroup contains the specification for main, reload and reloadDupe
	// sinkgroups. Operator uses 3 groups to perform Redshiftsink. The topics
	// which have never been released is part of Reload SinkGroup, the topics
	// which gets released moves to the Main SinkGroup. ReloadDupe SinkGroup
	// is used to give realtime upaates to the topics which are reloading.
	// Defaults are there for all sinkGroups if none is specifed.
	// +optional
	SinkGroup *SinkGroup `json:"sinkGroup,omitempty"`

	// RedshiftSchema to sink the data in
	RedshiftSchema string `json:"redshiftSchema"`
	// RedshiftMaxOpenConns is the maximum open connections allowed
	// +optional
	RedshiftMaxOpenConns *int `json:"redshiftMaxOpenConns,omitempty"`
	// RedshiftMaxIdleConns is the maximum idle connections allowed
	// +optional
	RedshiftMaxIdleConns *int `json:"redshiftMaxIdleConns,omitempty"`
	// RedshiftGroup to give the access to when new topics gets released
	RedshiftGroup *string `json:"redshiftGroup"`

	// Deprecated all of the below spec in favour of SinkGroup #167
	// Max configurations for the loader to batch the load
	// +optional
	MaxSize int `json:"maxSize,omitempty"`
	// +optional
	MaxWaitSeconds int `json:"maxWaitSeconds,omitempty"`
	// MaxProcessingTime is the sarama configuration MaxProcessingTime
	// It is the max time in milliseconds required to consume one message.
	// Defaults to 600000ms (10mins)
	// +optional
	MaxProcessingTime *int32 `json:"maxProcessingTime,omitempty"`
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
	// KafkaLoaderTopicPrefix is the prefix to use for loader topic
	// loader topic is this prefix "-" + batcher topic
	// batcherTopic: ts.inventory.customers and prefix is loader-
	// then the loaderTopic: loader-ts.inventory.customers
	// the prefix can contain at max 1 hyphen "-"
	// +optional
	KafkaLoaderTopicPrefix string `json:"kafkaLoaderTopicPrefix,omitempty"`

	Batcher RedshiftBatcherSpec `json:"batcher"`
	Loader  RedshiftLoaderSpec  `json:"loader"`

	// ReleaseCondition specifies the release condition to consider a topic
	// realtime and to consider the topci to be moved from reloading to released
	// This is relevant only if masking is turned on in mask configuration.
	// It is used for live mask reloading.
	// +optional
	ReleaseCondition *ReleaseCondition `json:"releaseCondition,omitempty"`

	// TopicReleaseCondition is considered instead of ReleaseCondition
	// if it is defined for a topic. This is used for topics which
	// does not work well with central ReleaseCondition for all topics
	// +optional
	TopicReleaseCondition map[string]ReleaseCondition `json:"topicReleaseCondition,omitempty"`
}

type ReleaseCondition struct {
	// MaxBatcherLag is the maximum lag the batcher consumer group
	// shoud have to be be considered to be operating in realtime and
	// to be considered for release.
	MaxBatcherLag *int64 `json:"maxBatcherLag,omitempty"`

	// MaxLoaderLag is the maximum lag the loader consumer group
	// shoud have to be be considered to be operating in realtime and
	// to be considered for release.
	MaxLoaderLag *int64 `json:"maxLoaderLag,omitempty"`
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
	LoaderTopicPrefix string `json:"loaderTopicPrefix,omitempty"`

	// LoaderCurrentOffset stores the last read current offset of the consumer group
	// This is required to determine if the consumer group has performed any
	// processing in the past. As for low throughput topics,
	// the consumer group disappears and distinguishing between never created
	// and inactive consumer groups become difficult. Which leads to low
	// throughput consumer groups not getting moved to realtime from reloading.
	// TODO: This is not dead field once a group moves to released and
	// should be cleaned after that(status needs to be updated)
	LoaderCurrentOffset *int64 `json:"currentOffset,omitempty"`

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
	MaskStatus *MaskStatus `json:"maskStatus,omitempty"`

	// TopicGroup stores the group info for the topic
	// +optional
	TopicGroup map[string]Group `json:"topicGroups,omitempty"`

	// BatcherReloadingTopics stores the list of topics which are currently reloading
	// for the batcher deployments in the reload sink group.
	// There is a limit to maximum topics that can be reloaded at a time. (MaxReloadingUnits)
	// +optional
	BatcherReloadingTopics []string `json:"batcherReloadingTopics,omitempty"`

	// LoaderReloadingTopics stores the list of topics which are currently reloading
	// for the loader deployments in the reload sink group.
	// There is a limit to maximum topics that can be reloaded at a time. (MaxReloadingUnits)
	// +optional
	LoaderReloadingTopics []string `json:"loaderReloadingTopics,omitempty"`
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
