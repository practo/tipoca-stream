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
	LoaderSuffix       = "-loader"
	LoaderEnvPrefix    = "LOADER_"
	LoaderDefaultImage = "practodev/redshiftloader:latest"
)

type Loader struct {
	name       string
	namespace  string
	deployment *appsv1.Deployment
	config     *corev1.ConfigMap
}

func loaderSecret(secret map[string]string) (map[string]string, error) {
	var s map[string]string
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

func NewLoader(
	name string,
	rsk *tipocav1.RedshiftSink,
	tableSuffix string,
	secret map[string]string,
	consumerGroups tipocav1.ConsumerGroups,
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
		totalTopics += len(group.Topics)
		groupConfigs = append(groupConfigs, consumer.ConsumerGroupConfig{
			GroupID: consumerGroupID(name, groupID),
			TopicRegexes: expandTopicsToRegex(
				makeLoaderTopics(
					group.LoaderTopicPrefix,
					group.Topics,
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

	var replicas int32
	if totalTopics > 0 {
		replicas = getReplicas(rsk.Spec.Batcher.Suspend)
	} else {
		replicas = 0
	}

	deploySpec := deploymentSpec{
		name:           name,
		namespace:      rsk.Namespace,
		labels:         getDefaultLabels("redshiftloader"),
		replicas:       &replicas,
		deploymentName: name,
		resources:      rsk.Spec.Loader.PodTemplate.Resources,
		tolerations:    rsk.Spec.Loader.PodTemplate.Tolerations,
		image:          getImage(rsk.Spec.Loader.PodTemplate.Image, false),
	}

	configSpec := configMapSpec{
		name:       name,
		namespace:  rsk.Namespace,
		volumeName: name,
		mountPath:  "/config.yaml",
		subPath:    "config.yaml",
		data: map[string]string{
			"config.yaml": string(confBytes),
		},
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

func (l Loader) UpdateRequired(current *appsv1.Deployment) bool {
	return !deploymentSpecEqual(current, l.Deployment())
}
