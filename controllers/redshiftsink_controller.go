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

package controllers

import (
	"context"
	"crypto/sha1"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	logr "github.com/go-logr/logr"
	klog "github.com/practo/klog/v2"
	tipocav1 "github.com/practo/tipoca-stream/redshiftsink/api/v1"
	git "github.com/practo/tipoca-stream/redshiftsink/pkg/git"
	kafka "github.com/practo/tipoca-stream/redshiftsink/pkg/kafka"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
	kerrors "k8s.io/apimachinery/pkg/util/errors"
	record "k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	client "sigs.k8s.io/controller-runtime/pkg/client"
	controller "sigs.k8s.io/controller-runtime/pkg/controller"
)

// RedshiftSinkReconciler reconciles a RedshiftSink object
type RedshiftSinkReconciler struct {
	client.Client

	Log      logr.Logger
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder

	KafkaTopicRegexes *sync.Map
	KafkaWatchers     *sync.Map
	KafkaTopicsCache  *sync.Map

	DefaultBatcherImage       string
	DefaultLoaderImage        string
	DefaultSecretRefName      string
	DefaultSecretRefNamespace string
	DefaultKafkaVersion       string

	GitCache *sync.Map
}

// +kubebuilder:rbac:groups=tipoca.k8s.practo.dev,resources=redshiftsinks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=tipoca.k8s.practo.dev,resources=redshiftsinks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch;create;update;patch;delete

// fetchSecretMap fetchs the k8s secret and returns it as a the map
// also it expects the secret to be of type as created from
// ../config/operator/kustomization_sample.yaml
func (r *RedshiftSinkReconciler) fetchSecretMap(
	ctx context.Context,
	name *string,
	namespace *string,
) (
	map[string]string,
	error,
) {
	secret := make(map[string]string)

	secretRefName := r.DefaultSecretRefName
	if name != nil {
		secretRefName = *name
	}
	secretRefNamespace := r.DefaultSecretRefNamespace
	if namespace != nil {
		secretRefNamespace = *namespace
	}

	k8sSecret, err := getSecret(ctx, r.Client, secretRefName, secretRefNamespace)
	if err != nil {
		return secret, fmt.Errorf("Error getting secret, %v", err)
	}
	for key, value := range k8sSecret.Data {
		secret[key] = string(value)
	}

	return secret, nil
}

func secretByKey(secret map[string]string, key string) (string, error) {
	value, ok := secret[key]
	if !ok {
		return "", fmt.Errorf("%s not found in secret", key)
	}

	return value, nil
}

// fetchLatestMaskFileVersion gets the latest mask file from remote
// git repository. It the git hash of the maskFile. Supports only github at
// present as maskFile parsing is not handled for all the types of url formats
// but with very few line of changes it can support all the other git repository.
func (r *RedshiftSinkReconciler) fetchLatestMaskFileVersion(
	maskFile string,
	gitToken string,
) (
	string,
	string,
	string,
	error,
) {
	url, err := git.ParseURL(maskFile)
	if err != nil {
		return "", "", "", err
	}

	if url.Scheme != "https" {
		return "", "", "", fmt.Errorf("scheme: %s not supported.\n", url.Scheme)
	}

	var repo, filePath string

	switch url.Host {
	case "github.com":
		repo, filePath = git.ParseGithubURL(url.Path)
	default:
		return "", "", "", fmt.Errorf("parsing not supported for: %s\n", url.Host)
	}

	var cache git.GitCacheInterface

	cacheLoaded, ok := r.GitCache.Load(repo)
	if ok {
		cache = cacheLoaded.(git.GitCacheInterface)
	} else {
		repoURL := strings.ReplaceAll(maskFile, filePath, "")
		cache, err = git.NewGitCache(repoURL, gitToken)
		if err != nil {
			return "", "", "", err
		}
		r.GitCache.Store(repo, cache)
	}

	latestVersion, err := cache.GetFileVersion(filePath)
	if err != nil {
		return "", "", "", err
	}

	return latestVersion, repo, filePath, nil
}

func resultRequeueSeconds(seconds int) ctrl.Result {
	return ctrl.Result{RequeueAfter: time.Second * time.Duration(seconds)}
}

func (r *RedshiftSinkReconciler) fetchLatestTopics(
	kafkaWatcher kafka.Watcher,
	regexes string,
) (
	[]string,
	error,
) {
	var topics []string
	var err error
	var rgx *regexp.Regexp
	topicsAppended := make(map[string]bool)
	expressions := strings.Split(regexes, ",")

	allTopics, err := kafkaWatcher.Topics()
	if err != nil {
		return topics, err
	}

	for _, expression := range expressions {
		rgxLoaded, ok := r.KafkaTopicRegexes.Load(expression)
		if !ok {
			rgx, err = regexp.Compile(strings.TrimSpace(expression))
			if err != nil {
				return topics, fmt.Errorf(
					"Compling regex: %s failed, err:%v\n", expression, err)
			}
			r.KafkaTopicRegexes.Store(expression, rgx)
		} else {
			rgx = rgxLoaded.(*regexp.Regexp)
		}

		for _, topic := range allTopics {
			if !rgx.MatchString(topic) {
				continue
			}
			_, ok := topicsAppended[topic]
			if ok {
				continue
			}
			topics = append(topics, topic)
			topicsAppended[topic] = true
		}
	}

	return topics, nil
}

func (r *RedshiftSinkReconciler) makeTLSConfig(secret map[string]string) (*kafka.TLSConfig, error) {
	enabled, err := secretByKey(secret, "tlsEnable")
	if err != nil {
		return nil, fmt.Errorf("Could not find secret: tlsEnable")
	}
	tlsEnabled, err := strconv.ParseBool(enabled)
	if err != nil {
		return nil, err
	}
	configTLS := kafka.TLSConfig{Enable: tlsEnabled}
	if tlsEnabled {
		tlsSecrets := make(map[string]string)
		tlsSecretsKeys := []string{
			"tlsUserCert",
			"tlsUserKey",
			"tlsCaCert",
		}
		for _, key := range tlsSecretsKeys {
			value, err := secretByKey(secret, key)
			if err != nil {
				return nil, fmt.Errorf("Could not find secret: %s", key)
			}
			tlsSecrets[key] = value
		}
		configTLS.UserCert = tlsSecrets[tlsSecretsKeys[0]]
		configTLS.UserKey = tlsSecrets[tlsSecretsKeys[1]]
		configTLS.CACert = tlsSecrets[tlsSecretsKeys[2]]
	}

	return &configTLS, nil
}

func (r *RedshiftSinkReconciler) loadKafkaWatcher(
	rsk *tipocav1.RedshiftSink,
	tlsConfig *kafka.TLSConfig,
) (
	kafka.Watcher,
	error,
) {
	values := ""
	brokers := strings.Split(rsk.Spec.KafkaBrokers, ",")
	for _, broker := range brokers {
		values += broker
	}
	kafkaVersion := rsk.Spec.KafkaVersion
	if kafkaVersion == "" {
		kafkaVersion = r.DefaultKafkaVersion
	}
	values += kafkaVersion
	hash := fmt.Sprintf("%x", sha1.Sum([]byte(values)))

	watcher, ok := r.KafkaWatchers.Load(hash)
	if ok {
		return watcher.(kafka.Watcher), nil
	} else {
		watcher, err := kafka.NewWatcher(
			brokers, kafkaVersion, *tlsConfig,
		)
		if err != nil {
			return nil, err
		}
		r.KafkaWatchers.Store(hash, watcher)

		return watcher, nil
	}
}

func (r *RedshiftSinkReconciler) reconcile(
	ctx context.Context,
	rsk *tipocav1.RedshiftSink,
) (
	ctrl.Result,
	ReconcilerEvent,
	error,
) {
	result := ctrl.Result{RequeueAfter: time.Second * 30}

	secret, err := r.fetchSecretMap(
		ctx,
		rsk.Spec.SecretRefName,
		rsk.Spec.SecretRefNamespace,
	)
	if err != nil {
		return result, nil, err
	}

	tlsConfig, err := r.makeTLSConfig(secret)
	if err != nil {
		return result, nil, err
	}

	kafkaWatcher, err := r.loadKafkaWatcher(rsk, tlsConfig)
	if err != nil {
		return result, nil, fmt.Errorf("Error fetching kafka watcher, %v", err)
	}

	kafkaTopics, err := r.fetchLatestTopics(
		kafkaWatcher, rsk.Spec.KafkaTopicRegexes,
	)
	if err != nil {
		return result, nil, fmt.Errorf("Error fetching topics, err: %v", err)
	}
	if len(kafkaTopics) == 0 {
		klog.Warningf(
			"Kafka topics not found for regex: %s", rsk.Spec.KafkaTopicRegexes)
	}

	sgBuilder := newSinkGroupBuilder()
	if rsk.Spec.Batcher.Mask == false {
		maskLessSinkGroup := sgBuilder.
			setRedshiftSink(rsk).setClient(r.Client).setScheme(r.Scheme).
			setType(MainSinkGroup).
			setTopics(kafkaTopics).
			setMaskVersion("").
			buildBatcher(secret, r.DefaultBatcherImage, r.DefaultKafkaVersion, tlsConfig).
			buildLoader(secret, r.DefaultLoaderImage, "", r.DefaultKafkaVersion, tlsConfig).
			build()
		result, event, err := maskLessSinkGroup.reconcile(ctx)
		return result, event, err
	}

	gitToken, err := secretByKey(secret, "gitAccessToken")
	if err != nil {
		return result, nil, err
	}

	desiredMaskVersion, repo, filePath, err := r.fetchLatestMaskFileVersion(
		rsk.Spec.Batcher.MaskFile,
		gitToken,
	)
	if err != nil {
		return result, nil, fmt.Errorf(
			"Error fetching latest mask file version, err: %v\n", err)
	}

	var currentMaskVersion string
	if rsk.Status.MaskStatus != nil &&
		rsk.Status.MaskStatus.CurrentMaskVersion != nil {
		currentMaskVersion = *rsk.Status.MaskStatus.CurrentMaskVersion
	} else {
		currentMaskVersion = ""
	}

	diffTopics, kafkaTopics, err := MaskDiff(
		kafkaTopics,
		rsk.Spec.Batcher.MaskFile,
		desiredMaskVersion,
		currentMaskVersion,
		gitToken,
		r.KafkaTopicsCache,
	)
	if err != nil {
		return result, nil, fmt.Errorf("Error doing mask diff, err: %v", err)
	}

	sBuilder := newStatusBuilder()
	status := sBuilder.
		setRedshiftSink(rsk).
		setCurrentVersion(currentMaskVersion).
		setDesiredVersion(desiredMaskVersion).
		setAllTopics(kafkaTopics).
		setDiffTopics(diffTopics).
		computeReleased().
		setRealtime().
		computeReloading().
		computeReloadingDupe().
		build()
	status.info()
	defer status.updateMaskStatus()

	// SinkGroup are of following types:
	// 1. main: sink group which has desiredMaskVersion
	//      and has topics which have been released
	//      consumer group: main
	//      tableSuffix: ""
	// 2. reload: sink group which has the desiredMaskVersion and is
	//      is undergoing reload with new mask configurations
	//      consumer group: desiredMaskVersion
	//      tableSuffix: "_reload_desiredMaskVersion"
	// 3. reloadDupe: sink group which has the currentMaskVersion
	//      and will be stopped when reload ones moves to realtime
	//      and when they are released.
	//      consumer group: currentMaskVersion
	//      tableSuffix: ""
	var reload, reloadDupe, main *sinkGroup

	reload = sgBuilder.
		setRedshiftSink(rsk).setClient(r.Client).setScheme(r.Scheme).
		setType(ReloadSinkGroup).
		setTopics(status.reloading).
		setMaskVersion(status.desiredVersion).
		setTopicGroups().
		buildBatcher(secret, r.DefaultBatcherImage, r.DefaultKafkaVersion, tlsConfig).
		buildLoader(secret, r.DefaultLoaderImage, ReloadTableSuffix, r.DefaultKafkaVersion, tlsConfig).
		build()

	currentRealtime, err := reload.realtimeTopics(kafkaWatcher)
	if err != nil {
		return result, nil, err
	}
	if !subSetSlice(currentRealtime, status.realtime) {
		for _, moreRealtime := range currentRealtime {
			status.realtime = appendIfMissing(status.realtime, moreRealtime)
		}
		klog.V(2).Infof(
			"Reconcile needed, realtime topics updated: %v", status.realtime)
		return resultRequeueSeconds(3), nil, nil
	}

	reloadDupe = sgBuilder.
		setRedshiftSink(rsk).setClient(r.Client).setScheme(r.Scheme).
		setType(ReloadDupeSinkGroup).
		setTopics(status.reloadingDupe).
		setMaskVersion(status.currentVersion).
		setTopicGroups().
		buildBatcher(secret, r.DefaultBatcherImage, r.DefaultKafkaVersion, tlsConfig).
		buildLoader(secret, r.DefaultLoaderImage, "", r.DefaultKafkaVersion, tlsConfig).
		build()

	main = sgBuilder.
		setRedshiftSink(rsk).setClient(r.Client).setScheme(r.Scheme).
		setType(MainSinkGroup).
		setTopics(status.released).
		setMaskVersion(status.desiredVersion).
		setTopicGroups().
		buildBatcher(secret, r.DefaultBatcherImage, r.DefaultKafkaVersion, tlsConfig).
		buildLoader(secret, r.DefaultLoaderImage, "", r.DefaultKafkaVersion, tlsConfig).
		build()

	sinkGroups := []*sinkGroup{reloadDupe, reload, main}
	if len(status.reloadingDupe) > 0 {
		sinkGroups = []*sinkGroup{main, reloadDupe, reload}
	}

	for _, sinkGroup := range sinkGroups {
		result, event, err := sinkGroup.reconcile(ctx)
		if err != nil {
			return result, nil, err
		}
		if event != nil {
			return resultRequeueSeconds(3), event, nil
		}
	}

	klog.V(4).Info("finding release candidates...")
	var topicReleaseEvent *TopicReleasedEvent
	var releaseError error
	var releaser *releaser
	if len(status.realtime) > 0 {
		klog.V(2).Infof("release candidates: %v", status.realtime)
		releasingTopic := status.realtime[0]

		releaser, releaseError = newReleaser(
			ctx,
			rsk.Spec.Loader.RedshiftSchema,
			repo,
			filePath,
			currentMaskVersion,
			desiredMaskVersion,
			secret,
		)
		if releaseError != nil {
			return result, nil, releaseError
		}
		releaseError = releaser.release(
			rsk.Spec.Loader.RedshiftSchema,
			status.realtime[0],
			ReloadTableSuffix,
			rsk.Spec.Loader.RedshiftGroup,
		)

		if releaseError != nil {
			klog.Errorf(
				"Error releasing topic: %s, err: %v",
				releasingTopic,
				releaseError,
			)
		} else {
			topicReleaseEvent = &TopicReleasedEvent{
				Object:  rsk,
				Topic:   releasingTopic,
				Version: status.desiredVersion,
			}
			klog.V(2).Infof(
				"released topic: %v, version: %v",
				releasingTopic, status.desiredVersion,
			)
			status.updateTopicsOnRelease(releasingTopic)
			status.updateTopicGroup(releasingTopic)
		}
	}

	if releaseError != nil {
		return result, nil, releaseError
	}
	if topicReleaseEvent != nil {
		return result, topicReleaseEvent, nil
	}

	klog.V(5).Info("Nothing done in reconcile.")

	return result, nil, nil
}

func (r *RedshiftSinkReconciler) Reconcile(
	ctx context.Context,
	req ctrl.Request,
) (
	_ ctrl.Result,
	reterr error,
) {
	klog.V(2).Infof("Reconciling %+v ...", req)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	var redshiftsink tipocav1.RedshiftSink
	err := r.Get(ctx, req.NamespacedName, &redshiftsink)
	if err != nil {
		return ctrl.Result{
			RequeueAfter: time.Second * 30}, client.IgnoreNotFound(err)
	}

	original := redshiftsink.DeepCopy()
	// Always attempt to patch the status after each reconciliation.
	defer func() {
		if reflect.DeepEqual(original.Status, redshiftsink.Status) {
			return
		}
		err := r.Client.Status().Patch(
			ctx,
			&redshiftsink,
			client.MergeFrom(original),
		)
		if err != nil {
			reterr = kerrors.NewAggregate(
				[]error{
					reterr,
					fmt.Errorf(
						"error while patching EtcdCluster.Status: %s ", err),
				},
			)
		}
	}()

	// Perform a reconcile, getting back the desired result, any utilerrors
	result, event, err := r.reconcile(ctx, &redshiftsink)
	if err != nil {
		err = fmt.Errorf("Failed to reconcile: %s", err)
	}

	// Finally, the event is used to generate a Kubernetes event by
	// calling `Record` and passing in the recorder.
	if event != nil {
		event.Record(r.Recorder)
	}

	klog.V(2).Infof("Reconciled %+v", req)
	return result, err
}

// SetupWithManager sets up the controller and applies all controller configs
func (r *RedshiftSinkReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&tipocav1.RedshiftSink{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: 1}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Secret{}).
		Complete(r)
}
