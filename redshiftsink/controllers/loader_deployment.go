package controllers

//ctrl.SetControllerReference(redshiftsink, deployment, r.Scheme)

import (
	"fmt"
	tipocav1 "github.com/practo/tipoca-stream/redshiftsink/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	client "sigs.k8s.io/controller-runtime/pkg/client"
	// ctrl "sigs.k8s.io/controller-runtime"
)

const (
	LoaderSuffix       = "-loader"
	LoaderEnvPrefix    = "LOADER_"
	LoaderDefaultImage = "practodev/redshiftloader:latest"
)

type Loader struct {
	name       string
	namespace  string
	client     client.Client
	deployment *appsv1.Deployment
}

func NewLoader(
	name string,
	client client.Client,
	rsk *tipocav1.RedshiftSink,
	loaderTopics string,
	tableSuffix string) Deployment {

	uniqueName := rsk.Name + "-" + name + LoaderSuffix
	secretRefName := rsk.Spec.SecretRefName
	envs := []corev1.EnvVar{
		corev1.EnvVar{
			Name:  LoaderEnvPrefix + "LOADER_MAXSIZE",
			Value: fmt.Sprintf("%v", rsk.Spec.Loader.MaxSize),
		},
		corev1.EnvVar{
			Name:  LoaderEnvPrefix + "LOADER_MAXWAITSECONDS",
			Value: fmt.Sprintf("%v", rsk.Spec.Loader.MaxWaitSeconds),
		},
		corev1.EnvVar{
			Name:  LoaderEnvPrefix + "KAFKA_BROKERS",
			Value: rsk.Spec.KafkaBrokers,
		},
		corev1.EnvVar{
			Name:  LoaderEnvPrefix + "KAFKA_GROUP",
			Value: uniqueName,
		},
		corev1.EnvVar{
			Name:  LoaderEnvPrefix + "KAFKA_TOPICREGEXES",
			Value: loaderTopics,
		},
		corev1.EnvVar{
			Name:  LoaderEnvPrefix + "SARAMA_OLDEST",
			Value: "true",
		},
		corev1.EnvVar{
			Name:  LoaderEnvPrefix + "SARAMA_LOG",
			Value: "false",
		},
		corev1.EnvVar{
			Name:  LoaderEnvPrefix + "SARAMA_AUTOCOMMIT",
			Value: "false",
		},
		corev1.EnvVar{
			Name:  LoaderEnvPrefix + "REDSHIFT_SCHEMA",
			Value: rsk.Spec.Loader.RedshiftSchema,
		},
		corev1.EnvVar{
			Name:  LoaderEnvPrefix + "RELOAD",
			Value: "false",
		},
		corev1.EnvVar{
			Name:  LoaderEnvPrefix + "REDSHIFT_TARGETTABLESUFFIX",
			Value: "", // to be passed later.
		},
		secretEnvVar(
			BatcherEnvPrefix+"S3SINK_REGION", "s3Region", secretRefName),
		secretEnvVar(
			BatcherEnvPrefix+"S3SINK_BUCKET", "s3Bucket", secretRefName),
		secretEnvVar(
			BatcherEnvPrefix+"S3SINK_BUCKETDIR",
			"s3LoaderBucketDir", secretRefName),
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
			BatcherEnvPrefix+"REDSHIFT_HOST",
			"redshiftHost", secretRefName),
		secretEnvVar(
			BatcherEnvPrefix+"REDSHIFT_PORT",
			"redshiftPort", secretRefName),
		secretEnvVar(
			BatcherEnvPrefix+"REDSHIFT_DATABASE",
			"redshiftDatabase", secretRefName),
		secretEnvVar(
			BatcherEnvPrefix+"REDSHIFT_USER",
			"redshiftUser", secretRefName),
		secretEnvVar(
			BatcherEnvPrefix+"REDSHIFT_PASSWORD",
			"redshiftPassword", secretRefName),
	}

	deploySpec := deploymentSpec{
		name:           uniqueName,
		namespace:      rsk.Namespace,
		labels:         getDefaultLabels("redshiftloader"),
		replicas:       getReplicas(rsk.Spec.Loader.Suspend),
		deploymentName: uniqueName,
		envs:           envs,
		resources:      rsk.Spec.Loader.PodTemplate.Resources,
		tolerations:    rsk.Spec.Loader.PodTemplate.Tolerations,
		image:          getImage(rsk.Spec.Loader.PodTemplate.Image, false),
	}

	return &Loader{
		name:       uniqueName,
		client:     client,
		namespace:  rsk.Namespace,
		deployment: deploymentForRedshiftSink(deploySpec),
	}
}

func (l Loader) Name() string {
	return l.name
}

func (l Loader) Namespace() string {
	return l.namespace
}

func (l Loader) Client() client.Client {
	return l.client
}

func (l Loader) Deployment() *appsv1.Deployment {
	return l.deployment
}

func (l Loader) UpdateRequired(current *appsv1.Deployment) bool {
	return !deploymentSpecEqual(current, l.Deployment())
}
