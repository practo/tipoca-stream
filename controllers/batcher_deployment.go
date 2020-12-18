package controllers

import (
	"fmt"
	tipocav1 "github.com/practo/tipoca-stream/redshiftsink/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	client "sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	BatcherSuffix       = "-batcher"
	BatcherEnvPrefix    = "BATCHER_"
	BatcherDefaultImage = "practodev/redshiftbatcher:latest"
)

type Batcher struct {
	name       string
	namespace  string
	client     client.Client
	deployment *appsv1.Deployment
}

func NewBatcher(
	name string,
	client client.Client,
	rsk *tipocav1.RedshiftSink,
	topics string) Deployment {

	secretRefName := rsk.Spec.SecretRefName
	envs := []corev1.EnvVar{
		corev1.EnvVar{
			Name:  BatcherEnvPrefix + "BATCHER_MASK",
			Value: fmt.Sprintf("%v", rsk.Spec.Batcher.Mask),
		},
		corev1.EnvVar{
			Name:  BatcherEnvPrefix + "BATCHER_MASKFILE",
			Value: rsk.Spec.Batcher.MaskFile,
		},
		corev1.EnvVar{
			Name:  BatcherEnvPrefix + "BATCHER_MAXSIZE",
			Value: fmt.Sprintf("%v", rsk.Spec.Batcher.MaxSize),
		},
		corev1.EnvVar{
			Name:  BatcherEnvPrefix + "BATCHER_MAXWAITSECONDS",
			Value: fmt.Sprintf("%v", rsk.Spec.Batcher.MaxWaitSeconds),
		},
		corev1.EnvVar{
			Name:  BatcherEnvPrefix + "KAFKA_BROKERS",
			Value: rsk.Spec.KafkaBrokers,
		},
		corev1.EnvVar{
			Name:  BatcherEnvPrefix + "KAFKA_GROUP",
			Value: name,
		},
		corev1.EnvVar{
			Name:  BatcherEnvPrefix + "KAFKA_TOPICREGEXES",
			Value: topics,
		},
		corev1.EnvVar{
			Name:  BatcherEnvPrefix + "KAFKA_LOADERTOPICPREFIX",
			Value: rsk.Spec.KafkaLoaderTopicPrefix,
		},
		corev1.EnvVar{
			Name:  BatcherEnvPrefix + "SARAMA_OLDEST",
			Value: "true",
		},
		corev1.EnvVar{
			Name:  BatcherEnvPrefix + "SARAMA_LOG",
			Value: "false",
		},
		corev1.EnvVar{
			Name:  BatcherEnvPrefix + "SARAMA_AUTOCOMMIT",
			Value: "true",
		},
		corev1.EnvVar{
			Name:  BatcherEnvPrefix + "RELOAD",
			Value: "false",
		},
		secretEnvVar(
			BatcherEnvPrefix+"BATCHER_MASKSALT", "maskSalt", secretRefName),
		secretEnvVar(
			BatcherEnvPrefix+"S3SINK_REGION", "s3Region", secretRefName),
		secretEnvVar(
			BatcherEnvPrefix+"S3SINK_BUCKET", "s3Bucket", secretRefName),
		secretEnvVar(
			BatcherEnvPrefix+"S3SINK_BUCKETDIR",
			"s3BatcherBucketDir", secretRefName),
		secretEnvVar(
			BatcherEnvPrefix+"S3SINK_ACCESSKEYID",
			"s3AccessKeyId", secretRefName),
		secretEnvVar(
			BatcherEnvPrefix+"S3SINK_SECRETACCESSKEY",
			"s3SecretAccessKey", secretRefName),
		secretEnvVar(
			BatcherEnvPrefix+"SCHEMAREGISTRYURL",
			"schemaRegistryURL", secretRefName),
		secretEnvVar(
			BatcherEnvPrefix+"GITACCESSTOKEN",
			"gitAccessToken", secretRefName),
	}

	var replicas int32
	if topics != "" {
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
		envs:           envs,
		resources:      rsk.Spec.Batcher.PodTemplate.Resources,
		tolerations:    rsk.Spec.Batcher.PodTemplate.Tolerations,
		image:          getImage(rsk.Spec.Batcher.PodTemplate.Image, true),
	}

	return &Batcher{
		name:       name,
		client:     client,
		namespace:  rsk.Namespace,
		deployment: deploymentForRedshiftSink(deploySpec),
	}
}

func (b Batcher) Name() string {
	return b.name
}

func (b Batcher) Namespace() string {
	return b.namespace
}

func (b Batcher) Client() client.Client {
	return b.client
}

func (b Batcher) Deployment() *appsv1.Deployment {
	return b.deployment
}

func (b Batcher) UpdateRequired(current *appsv1.Deployment) bool {
	return !deploymentSpecEqual(current, b.Deployment())
}
