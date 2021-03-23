package controllers

import (
	"fmt"

	tipocav1 "github.com/practo/tipoca-stream/redshiftsink/api/v1"
	"github.com/practo/tipoca-stream/redshiftsink/cmd/redshiftloader/config"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/kafka"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/redshift"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/redshiftloader"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/s3sink"
	yaml "gopkg.in/yaml.v2"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	resource "k8s.io/apimachinery/pkg/api/resource"
)

const (
	LoaderSuffix        = "-loader"
	LoaderLabelInstance = "redshiftloader"
)

type Loader struct {
	name       string
	namespace  string
	deployment *appsv1.Deployment
	config     *corev1.ConfigMap
}

// applyLoaderSinkGroupDefaults applies the defaults for the loader
// deployments of the sink group. User does not need to specify big lengthy
// configurations everytime. Defaults are optimized for maximum performance
// and are recommended to use.
func applyLoaderSinkGroupDefaults(
	rsk *tipocav1.RedshiftSink,
	sgType string,
	defaultImage string,
) *tipocav1.SinkGroupSpec {
	// defaults
	defaultMaxBytesPerBatch := resource.MustParse(
		redshiftloader.DefaultMaxBytesPerBatch,
	)
	maxSizePerBatch := &defaultMaxBytesPerBatch
	maxWaitSeconds := &redshiftloader.DefaultMaxWaitSeconds
	maxProcessingTime := &redshiftloader.DefaultMaxProcessingTime
	maxTopics := &DefaultMaxBatcherTopics
	image := &defaultImage
	var resources *corev1.ResourceRequirements
	var tolerations *[]corev1.Toleration

	// apply the sinkGroup spec rules
	var specifiedSpec *tipocav1.SinkGroupSpec
	if rsk.Spec.Loader.SinkGroup.All != nil {
		specifiedSpec = rsk.Spec.Loader.SinkGroup.All
	}
	switch sgType {
	case MainSinkGroup:
		if rsk.Spec.Loader.SinkGroup.Main != nil {
			specifiedSpec = rsk.Spec.Loader.SinkGroup.Main
		}
	case ReloadSinkGroup:
		if rsk.Spec.Loader.SinkGroup.Reload != nil {
			specifiedSpec = rsk.Spec.Loader.SinkGroup.Reload
		}
	case ReloadDupeSinkGroup:
		if rsk.Spec.Loader.SinkGroup.ReloadDupe != nil {
			specifiedSpec = rsk.Spec.Loader.SinkGroup.ReloadDupe
		}
	}

	// overwrite with the defaults with the specified values
	if specifiedSpec != nil {
		if specifiedSpec.MaxSizePerBatch != nil {
			maxSizePerBatch = specifiedSpec.MaxSizePerBatch
		}
		if specifiedSpec.MaxWaitSeconds != nil {
			maxWaitSeconds = specifiedSpec.MaxWaitSeconds
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
		MaxSizePerBatch:   maxSizePerBatch,
		MaxWaitSeconds:    maxWaitSeconds,
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

func loaderSecret(secret map[string]string) (map[string]string, error) {
	s := make(map[string]string)
	secretKeys := []string{
		"s3Region",
		"s3Bucket",
		"s3LoaderBucketDir",
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

func loaderName(rskName, sinkGroup string, id string) string {
	return fmt.Sprintf(
		"%s-%s%s%s",
		rskName,
		sinkGroup,
		id,
		LoaderSuffix,
	)
}

func redshiftConnections(rsk *tipocav1.RedshiftSink, defaultMaxOpenConns, defaultMaxIdleConns int) (int, int) {
	maxOpenConns := defaultMaxOpenConns
	maxIdleConns := defaultMaxIdleConns
	if rsk.Spec.Loader.RedshiftMaxOpenConns != nil {
		maxOpenConns = *rsk.Spec.Loader.RedshiftMaxOpenConns
	}
	if rsk.Spec.Loader.RedshiftMaxIdleConns != nil {
		maxIdleConns = *rsk.Spec.Loader.RedshiftMaxIdleConns
	}

	return maxOpenConns, maxIdleConns
}

func NewLoader(
	name string,
	rsk *tipocav1.RedshiftSink,
	tableSuffix string,
	secret map[string]string,
	sinkGroup string,
	sinkGroupSpec *tipocav1.SinkGroupSpec,
	consumerGroups map[string]consumerGroup,
	defaultImage string,
	defaultKafkaVersion string,
	tlsConfig *kafka.TLSConfig,
	defaultMaxOpenConns int,
	defaultMaxIdleConns int,
) (
	Deployment,
	error,
) {
	secret, err := loaderSecret(secret)
	if err != nil {
		return nil, err
	}

	// defaults
	kafkaVersion := rsk.Spec.KafkaVersion
	if kafkaVersion == "" {
		kafkaVersion = defaultKafkaVersion
	}
	var maxSize int // Deprecated
	var maxBytesPerBatch *int64
	var maxWaitSeconds *int
	var maxProcessingTime int32 = redshiftloader.DefaultMaxProcessingTime
	if sinkGroupSpec != nil {
		m := sinkGroupSpec.MaxSizePerBatch.Value()
		maxBytesPerBatch = &m
		maxWaitSeconds = sinkGroupSpec.MaxWaitSeconds
		maxProcessingTime = *sinkGroupSpec.MaxProcessingTime
	} else { // Deprecated
		maxSize = rsk.Spec.Loader.MaxSize
		maxWaitSeconds = &rsk.Spec.Loader.MaxWaitSeconds
		if rsk.Spec.Loader.MaxProcessingTime != nil {
			maxProcessingTime = *rsk.Spec.Loader.MaxProcessingTime
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
			GroupID: consumerGroupID(rsk.Name, rsk.Namespace, groupID, "-loader"),
			TopicRegexes: expandTopicsToRegex(
				makeLoaderTopics(
					group.loaderTopicPrefix,
					group.topics,
				),
			),
			Kafka: kafka.KafkaConfig{
				Brokers:   rsk.Spec.KafkaBrokers,
				Version:   kafkaVersion,
				TLSConfig: *tlsConfig,
			},
			Sarama: kafka.SaramaConfig{
				Assignor:                "range",
				Oldest:                  true,
				Log:                     true,
				AutoCommit:              false,
				SessionTimeoutSeconds:   &sessionTimeoutSeconds,
				HearbeatIntervalSeconds: &hearbeatIntervalSeconds,
				MaxProcessingTime:       &maxProcessingTime,
			},
		})
	}

	maxOpenConns, maxIdleConns := redshiftConnections(rsk, defaultMaxOpenConns, defaultMaxIdleConns)

	conf := config.Config{
		Loader: redshiftloader.LoaderConfig{
			MaxSize:          maxSize, // Deprecated
			MaxWaitSeconds:   maxWaitSeconds,
			MaxBytesPerBatch: maxBytesPerBatch,
		},
		ConsumerGroups: groupConfigs,
		S3Sink: s3sink.Config{
			Region:          secret["s3Region"],
			AccessKeyId:     secret["s3AccessKeyId"],
			SecretAccessKey: secret["s3SecretAccessKey"],
			Bucket:          secret["s3Bucket"],
			BucketDir:       secret["s3LoaderBucketDir"],
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
			MaxOpenConns: maxOpenConns,
			MaxIdleConns: maxIdleConns,
		},
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
	if rsk.Spec.Loader.PodTemplate.Image != nil {
		image = *rsk.Spec.Loader.PodTemplate.Image
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
		LoaderLabelInstance, sinkGroup, objectName, rsk.Name,
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
		resources:   rsk.Spec.Loader.PodTemplate.Resources,
		tolerations: rsk.Spec.Loader.PodTemplate.Tolerations,
		image:       image,
		args:        []string{"-v=2", "--config=/config.yaml"},
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
