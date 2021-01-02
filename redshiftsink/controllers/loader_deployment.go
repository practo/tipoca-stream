package controllers

import (
	"fmt"

	tipocav1 "github.com/practo/tipoca-stream/redshiftsink/api/v1"
	"github.com/practo/tipoca-stream/redshiftsink/cmd/redshiftloader/config"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/consumer"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/redshift"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/redshiftloader"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/s3sink"
	yaml "gopkg.in/yaml.v2"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

const (
	LoaderSuffix        = "-loader"
	LoaderLabelInstance = "redshiftloader"
	LoaderDefaultImage  = "practodev/redshiftloader:latest"
)

type Loader struct {
	name       string
	namespace  string
	deployment *appsv1.Deployment
	config     *corev1.ConfigMap
}

func loaderSecret(secret map[string]string) (map[string]string, error) {
	s := make(map[string]string)
	secretKeys := []string{
		"s3Region",
		"s3Bucket",
		"s3BatcherBucketDir",
		"s3AccessKeyId",
		"s3SecretAccessKey",
		"schemaRegistryURL",
		"redshiftHost",
		"redshiftPort",
		"redshiftDatabase",
		"redshiftUser",
		"redshiftPassword",
	}

	for _, key := range secretKeys {
		value, err := secretByKey(secret, key)
		if err != nil {
			return nil, fmt.Errorf("loader secret: %s not found, %v", key, err)
		}
		s[key] = value
	}

	return s, nil
}

func loaderName(rskName, sinkGroup string) string {
	return fmt.Sprintf("%s-%s%s", rskName, sinkGroup, LoaderSuffix)
}

func NewLoader(
	name string,
	rsk *tipocav1.RedshiftSink,
	tableSuffix string,
	secret map[string]string,
	sinkGroup string,
	consumerGroups map[string]consumerGroup,
) (
	Deployment,
	error,
) {
	secret, err := loaderSecret(secret)
	if err != nil {
		return nil, err
	}

	totalTopics := 0
	var groupConfigs []consumer.ConsumerGroupConfig
	for groupID, group := range consumerGroups {
		totalTopics += len(group.topics)
		groupConfigs = append(groupConfigs, consumer.ConsumerGroupConfig{
			GroupID: consumerGroupID(name, groupID),
			TopicRegexes: expandTopicsToRegex(
				makeLoaderTopics(
					group.loaderTopicPrefix,
					group.topics,
				),
			),
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
		Loader: redshiftloader.LoaderConfig{
			MaxSize:        rsk.Spec.Loader.MaxSize,
			MaxWaitSeconds: rsk.Spec.Loader.MaxWaitSeconds,
		},
		ConsumerGroups: groupConfigs,
		S3Sink: s3sink.Config{
			Region:          secret["s3Region"],
			AccessKeyId:     secret["s3AccessKeyId"],
			SecretAccessKey: secret["s3SecretAccessKey"],
			Bucket:          secret["s3BatcherBucketDir"],
		},
		SchemaRegistryURL: secret["schemaRegistryURL"],
		Redshift: redshift.RedshiftConfig{
			Schema:       rsk.Spec.Loader.RedshiftSchema,
			TableSuffix:  tableSuffix,
			Host:         secret["redshiftHost"],
			Port:         secret["redshiftPort"],
			Database:     secret["redshiftDatabase"],
			User:         secret["redshiftUser"],
			Password:     secret["redshiftPassword"],
			Timeout:      10,
			Stats:        true,
			MaxOpenConns: 3,
			MaxIdleConns: 3,
		},
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

	confString := string(confBytes)
	objectName := getObjectName(name, confString)
	labels := getDefaultLabels(LoaderLabelInstance, sinkGroup, objectName)

	configSpec := configMapSpec{
		name:       objectName,
		namespace:  rsk.Namespace,
		labels:     labels,
		volumeName: name,
		mountPath:  "/config.yaml",
		subPath:    "config.yaml",
		data:       map[string]string{"config.yaml": confString},
	}

	deploySpec := deploymentSpec{
		name:        objectName,
		namespace:   rsk.Namespace,
		labels:      labels,
		replicas:    &replicas,
		resources:   rsk.Spec.Loader.PodTemplate.Resources,
		tolerations: rsk.Spec.Loader.PodTemplate.Tolerations,
		image:       getImage(rsk.Spec.Loader.PodTemplate.Image, false),
	}

	return &Loader{
		name:       name,
		namespace:  rsk.Namespace,
		deployment: deploymentFromSpec(deploySpec, configSpec),
		config:     configFromSpec(configSpec),
	}, nil
}

func (l Loader) Name() string {
	return l.name
}

func (l Loader) Namespace() string {
	return l.namespace
}

func (l Loader) Deployment() *appsv1.Deployment {
	return l.deployment
}

func (l Loader) Config() *corev1.ConfigMap {
	return l.config
}

func (l Loader) UpdateDeployment(current *appsv1.Deployment) bool {
	return !deploymentSpecEqual(current, l.Deployment())
}

func (l Loader) UpdateConfig(current *corev1.ConfigMap) bool {
	return !configSpecEqual(current, l.Config())
}
