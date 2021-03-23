package controllers

import (
	"fmt"

	tipocav1 "github.com/practo/tipoca-stream/redshiftsink/api/v1"
	"github.com/practo/tipoca-stream/redshiftsink/cmd/redshiftbatcher/config"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/kafka"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/redshiftbatcher"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/s3sink"
	yaml "gopkg.in/yaml.v2"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

const (
	BatcherSuffix        = "-batcher"
	BatcherLabelInstance = "redshiftbatcher"
)

type Batcher struct {
	name       string
	namespace  string
	deployment *appsv1.Deployment
	config     *corev1.ConfigMap
}

// applyBatcherSinkGroupDefaults applies the defaults for the batcher
// deployments of the sink group. User does not need to specify big lengthy
// configurations everytime. Defaults are optimized for maximum performance
// and is recommended to use.
func applyBatcherSinkGroupDefaults(
	rsk *tipocav1.RedshiftSink,
	sgType string,
	defaultImage string,
) *tipocav1.SinkGroupSpec {
	// defaults
	maxBytesPerBatch := &redshiftbatcher.DefaultMaxBytesPerBatch
	maxWaitSeconds := &redshiftbatcher.DefaultMaxWaitSeconds
	maxConcurrency := &redshiftbatcher.DefaultMaxConcurrency
	maxProcessingTime := &redshiftbatcher.DefaultMaxProcessingTime
	maxTopics := &DefaultMaxBatcherTopics
	image := &defaultImage
	var resources *corev1.ResourceRequirements
	var tolerations *[]corev1.Toleration

	// apply the sinkGroup spec rules
	var specifiedSpec *tipocav1.SinkGroupSpec
	if rsk.Spec.Batcher.SinkGroup.All != nil {
		specifiedSpec = rsk.Spec.Batcher.SinkGroup.All
	}
	switch sgType {
	case MainSinkGroup:
		if rsk.Spec.Batcher.SinkGroup.Main != nil {
			specifiedSpec = rsk.Spec.Batcher.SinkGroup.Main
		}
	case ReloadSinkGroup:
		if rsk.Spec.Batcher.SinkGroup.Reload != nil {
			specifiedSpec = rsk.Spec.Batcher.SinkGroup.Reload
		}
	case ReloadDupeSinkGroup:
		if rsk.Spec.Batcher.SinkGroup.ReloadDupe != nil {
			specifiedSpec = rsk.Spec.Batcher.SinkGroup.ReloadDupe
		}
	}

	// overwrite with the defaults with the specified values
	if specifiedSpec != nil {
		if specifiedSpec.MaxBytesPerBatch != nil {
			maxBytesPerBatch = specifiedSpec.MaxBytesPerBatch
		}
		if specifiedSpec.MaxWaitSeconds != nil {
			maxWaitSeconds = specifiedSpec.MaxWaitSeconds
		}
		if specifiedSpec.MaxConcurrency != nil {
			maxConcurrency = specifiedSpec.MaxConcurrency
		}
		if specifiedSpec.MaxProcessingTime != nil {
			maxProcessingTime = specifiedSpec.MaxProcessingTime
		}
		if specifiedSpec.DeploymentUnit.PodTemplate.Image != nil {
			image = specifiedSpec.DeploymentUnit.PodTemplate.Image
		}
		if specifiedSpec.DeploymentUnit.PodTemplate.Resources != nil {
			resources = specifiedSpec.DeploymentUnit.PodTemplate.Resources
		}
		if specifiedSpec.DeploymentUnit.PodTemplate.Tolerations != nil {
			tolerations = specifiedSpec.DeploymentUnit.PodTemplate.Tolerations
		}
	}

	return &tipocav1.SinkGroupSpec{
		MaxBytesPerBatch:  maxBytesPerBatch,
		MaxWaitSeconds:    maxWaitSeconds,
		MaxConcurrency:    maxConcurrency,
		MaxProcessingTime: maxProcessingTime,
		DeploymentUnit: &tipocav1.DeploymentUnit{
			MaxTopics: maxTopics,
			PodTemplate: &tipocav1.RedshiftPodTemplateSpec{
				Image:       image,
				Resources:   resources,
				Tolerations: tolerations,
			},
		},
	}
}

func batcherSecret(secret map[string]string) (map[string]string, error) {
	s := make(map[string]string)
	secretKeys := []string{
		"maskSalt",
		"s3Region",
		"s3Bucket",
		"s3BatcherBucketDir",
		"s3AccessKeyId",
		"s3SecretAccessKey",
		"schemaRegistryURL",
		"gitAccessToken",
	}

	for _, key := range secretKeys {
		value, err := secretByKey(secret, key)
		if err != nil {
			return nil, fmt.Errorf("batcher secret: %s not found, %v", key, err)
		}
		s[key] = value
	}

	return s, nil
}

func batcherName(rskName, sinkGroup string, id string) string {
	return fmt.Sprintf(
		"%s-%s%s%s",
		rskName,
		sinkGroup,
		id,
		BatcherSuffix,
	)
}

func NewBatcher(
	name string,
	rsk *tipocav1.RedshiftSink,
	maskFileVersion string,
	secret map[string]string,
	sinkGroup string,
	sinkGroupSpec *tipocav1.SinkGroupSpec,
	consumerGroups map[string]consumerGroup,
	defaultImage string,
	defaultKafkaVersion string,
	tlsConfig *kafka.TLSConfig,
) (
	Deployment,
	error,
) {
	secret, err := batcherSecret(secret)
	if err != nil {
		return nil, err
	}

	// defaults
	kafkaVersion := rsk.Spec.KafkaVersion
	if kafkaVersion == "" {
		kafkaVersion = defaultKafkaVersion
	}
	var maxSize int // Deprecated
	var maxBytesPerBatch, maxWaitSeconds, maxConcurrency *int
	var maxProcessingTime int32 = redshiftbatcher.DefaultMaxProcessingTime
	if sinkGroupSpec != nil {
		maxBytesPerBatch = sinkGroupSpec.MaxBytesPerBatch
		maxWaitSeconds = sinkGroupSpec.MaxWaitSeconds
		maxConcurrency = sinkGroupSpec.MaxConcurrency
		maxProcessingTime = *sinkGroupSpec.MaxProcessingTime
	} else { // Deprecated
		maxSize = rsk.Spec.Batcher.MaxSize
		maxWaitSeconds = &rsk.Spec.Batcher.MaxWaitSeconds
		maxConcurrency = &redshiftbatcher.DefaultMaxConcurrency
		if rsk.Spec.Batcher.MaxConcurrency != nil {
			maxConcurrency = rsk.Spec.Batcher.MaxConcurrency
		}
		if rsk.Spec.Batcher.MaxProcessingTime != nil {
			maxProcessingTime = *rsk.Spec.Batcher.MaxProcessingTime
		}
	}
	// defaults which are not configurable for the user
	var sessionTimeoutSeconds int = 10
	var hearbeatIntervalSeconds int = 2

	totalTopics := 0
	var groupConfigs []kafka.ConsumerGroupConfig
	for groupID, group := range consumerGroups {
		totalTopics += len(group.topics)
		groupConfigs = append(groupConfigs, kafka.ConsumerGroupConfig{
			GroupID:           consumerGroupID(rsk.Name, rsk.Namespace, groupID, "-batcher"),
			TopicRegexes:      expandTopicsToRegex(group.topics),
			LoaderTopicPrefix: group.loaderTopicPrefix,
			Kafka: kafka.KafkaConfig{
				Brokers:   rsk.Spec.KafkaBrokers,
				Version:   kafkaVersion,
				TLSConfig: *tlsConfig,
			},
			Sarama: kafka.SaramaConfig{
				Assignor:                "range",
				Oldest:                  true,
				Log:                     true,
				AutoCommit:              true,
				SessionTimeoutSeconds:   &sessionTimeoutSeconds,
				HearbeatIntervalSeconds: &hearbeatIntervalSeconds,
				MaxProcessingTime:       &maxProcessingTime,
			},
		})
	}

	conf := config.Config{
		Batcher: redshiftbatcher.BatcherConfig{
			Mask:             rsk.Spec.Batcher.Mask,
			MaskSalt:         secret["maskSalt"],
			MaskFile:         rsk.Spec.Batcher.MaskFile,
			MaskFileVersion:  maskFileVersion,
			MaxSize:          maxSize, // Deprecated
			MaxWaitSeconds:   maxWaitSeconds,
			MaxConcurrency:   maxConcurrency,
			MaxBytesPerBatch: maxBytesPerBatch,
		},
		ConsumerGroups: groupConfigs,
		S3Sink: s3sink.Config{
			Region:          secret["s3Region"],
			AccessKeyId:     secret["s3AccessKeyId"],
			SecretAccessKey: secret["s3SecretAccessKey"],
			Bucket:          secret["s3Bucket"],
			BucketDir:       secret["s3BatcherBucketDir"],
		},
		SchemaRegistryURL: secret["schemaRegistryURL"],
		GitAccessToken:    secret["gitAccessToken"],
	}
	confBytes, err := yaml.Marshal(conf)
	if err != nil {
		return nil, err
	}

	replicas := getReplicas(
		rsk.Spec.Batcher.Suspend,
		len(consumerGroups),
		totalTopics,
	)

	var image string
	if rsk.Spec.Batcher.PodTemplate.Image != nil {
		image = *rsk.Spec.Batcher.PodTemplate.Image
	} else {
		image = defaultImage
	}

	confString := string(confBytes)
	hash, err := getHashStructure(conf)
	if err != nil {
		return nil, err
	}
	objectName := fmt.Sprintf("%s-%s", name, hash)
	labels := getDefaultLabels(
		BatcherLabelInstance, sinkGroup, objectName, rsk.Name,
	)

	configSpec := configMapSpec{
		name:       objectName,
		namespace:  rsk.Namespace,
		labels:     labels,
		volumeName: objectName,
		mountPath:  "/config.yaml",
		subPath:    "config.yaml",
		data:       map[string]string{"config.yaml": confString},
	}

	deploySpec := deploymentSpec{
		name:        objectName,
		namespace:   rsk.Namespace,
		labels:      labels,
		replicas:    &replicas,
		resources:   rsk.Spec.Batcher.PodTemplate.Resources,
		tolerations: rsk.Spec.Batcher.PodTemplate.Tolerations,
		image:       image,
		args:        []string{"-v=4", "--config=/config.yaml"},
	}

	return &Batcher{
		name:       name,
		namespace:  rsk.Namespace,
		deployment: deploymentFromSpec(deploySpec, configSpec),
		config:     configFromSpec(configSpec),
	}, nil
}

func (b Batcher) Name() string {
	return b.name
}

func (b Batcher) Namespace() string {
	return b.namespace
}

func (b Batcher) Deployment() *appsv1.Deployment {
	return b.deployment
}

func (b Batcher) Config() *corev1.ConfigMap {
	return b.config
}

func (b Batcher) UpdateDeployment(current *appsv1.Deployment) bool {
	return !deploymentSpecEqual(current, b.Deployment())
}

func (b Batcher) UpdateConfig(current *corev1.ConfigMap) bool {
	return !configSpecEqual(current, b.Config())
}
