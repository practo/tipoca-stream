package controllers

import (
	"fmt"
	tipocav1 "github.com/practo/tipoca-stream/redshiftsink/api/v1"
	"github.com/practo/tipoca-stream/redshiftsink/cmd/redshiftbatcher/config"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/consumer"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/redshiftbatcher"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/s3sink"
	yaml "gopkg.in/yaml.v2"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

const (
	BatcherSuffix       = "-batcher"
	BatcherEnvPrefix    = "BATCHER_"
	BatcherDefaultImage = "practodev/redshiftbatcher:latest"
)

type Batcher struct {
	name       string
	namespace  string
	deployment *appsv1.Deployment
	config     *corev1.ConfigMap
}

func batcherSecret(secret map[string]string) (map[string]string, error) {
	var s map[string]string
	secretKeys := []string{
		"maskSalt",
		"s3Region",
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

func NewBatcher(
	name string,
	rsk *tipocav1.RedshiftSink,
	maskFileVersion string,
	secret map[string]string,
	consumerGroups map[string]consumerGroup,
) (
	Deployment,
	error,
) {
	secret, err := batcherSecret(secret)
	if err != nil {
		return nil, err
	}

	totalTopics := 0
	var groupConfigs []consumer.ConsumerGroupConfig
	for groupID, group := range consumerGroups {
		totalTopics += len(group.topics)
		groupConfigs = append(groupConfigs, consumer.ConsumerGroupConfig{
			GroupID:           consumerGroupID(name, groupID),
			TopicRegexes:      expandTopicsToRegex(group.topics),
			LoaderTopicPrefix: group.loaderTopicPrefix,
			Kafka: consumer.KafkaConfig{
				Brokers: rsk.Spec.KafkaBrokers,
			},
			Sarama: consumer.SaramaConfig{
				Assignor:   "range",
				Oldest:     true,
				Log:        false,
				AutoCommit: true,
			},
		})
	}

	conf := config.Config{
		Batcher: redshiftbatcher.BatcherConfig{
			Mask:            rsk.Spec.Batcher.Mask,
			MaskSalt:        secret["maskSalt"],
			MaskFile:        rsk.Spec.Batcher.MaskFile,
			MaskFileVersion: maskFileVersion,
			MaxSize:         rsk.Spec.Batcher.MaxSize,
			MaxWaitSeconds:  rsk.Spec.Batcher.MaxWaitSeconds,
		},
		ConsumerGroups: groupConfigs,
		S3Sink: s3sink.Config{
			Region:          secret["s3Region"],
			AccessKeyId:     secret["s3AccessKeyId"],
			SecretAccessKey: secret["s3SecretAccessKey"],
			Bucket:          secret["s3BatcherBucketDir"],
		},
		SchemaRegistryURL: secret["schemaRegistryURL"],
		GitAccessToken:    secret["gitAccessToken"],
	}
	confBytes, err := yaml.Marshal(conf)
	if err != nil {
		return nil, err
	}

	var replicas int32
	if totalTopics > 0 {
		replicas = getReplicas(rsk.Spec.Batcher.Suspend)
	} else {
		replicas = 0
	}

	deploySpec := deploymentSpec{
		name:           name,
		namespace:      rsk.Namespace,
		labels:         getDefaultLabels("redshiftbatcher"),
		replicas:       &replicas,
		deploymentName: name,
		resources:      rsk.Spec.Batcher.PodTemplate.Resources,
		tolerations:    rsk.Spec.Batcher.PodTemplate.Tolerations,
		image:          getImage(rsk.Spec.Batcher.PodTemplate.Image, true),
	}

	configSpec := configMapSpec{
		volumeName: name,
		mountPath:  "/config.yaml",
		subPath:    "config.yaml",
		data: map[string]string{
			"config.yaml": string(confBytes),
		},
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

func (b Batcher) UpdateRequired(current *appsv1.Deployment) bool {
	return !deploymentSpecEqual(current, b.Deployment())
}
