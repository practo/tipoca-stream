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

func batcherSecret(secret map[string]string) (map[string]string, error) {
	s := make(map[string]string)
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

func batcherName(rskName, sinkGroup string) string {
	return fmt.Sprintf("%s-%s%s", rskName, sinkGroup, BatcherSuffix)
}

func NewBatcher(
	name string,
	rsk *tipocav1.RedshiftSink,
	maskFileVersion string,
	secret map[string]string,
	sinkGroup string,
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

	totalTopics := 0
	kafkaVersion := rsk.Spec.KafkaVersion
	if kafkaVersion == "" {
		kafkaVersion = defaultKafkaVersion
	}
	var groupConfigs []kafka.ConsumerGroupConfig
	for groupID, group := range consumerGroups {
		totalTopics += len(group.topics)
		groupConfigs = append(groupConfigs, kafka.ConsumerGroupConfig{
			GroupID:           consumerGroupID(name, groupID),
			TopicRegexes:      expandTopicsToRegex(group.topics),
			LoaderTopicPrefix: group.loaderTopicPrefix,
			Kafka: kafka.KafkaConfig{
				Brokers:   rsk.Spec.KafkaBrokers,
				Version:   kafkaVersion,
				TLSConfig: *tlsConfig,
			},
			Sarama: kafka.SaramaConfig{
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
		rsk.Spec.Loader.Suspend,
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
	objectName := getObjectName(name, confString)
	labels := getDefaultLabels(BatcherLabelInstance, sinkGroup, objectName)

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
		args:        []string{"-v=2", "--config=/config.yaml"},
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
