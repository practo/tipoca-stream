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

	KafkaTopicRegexes  *sync.Map
	KafkaWatchers      *sync.Map
	KafkaTopicsCache   *sync.Map
	KafkaRealtimeCache *sync.Map
	ReleaseCache       *sync.Map
	GitCache           *sync.Map

	DefaultBatcherImage         string
	DefaultLoaderImage          string
	DefaultSecretRefName        string
	DefaultSecretRefNamespace   string
	DefaultKafkaVersion         string
	DefaultRedshiftMaxIdleConns int
	DefaultRedshiftMaxOpenConns int
}

const (
	MaxTopicRelease = 50
)

// +kubebuilder:rbac:groups=tipoca.k8s.practo.dev,resources=redshiftsinks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=tipoca.k8s.practo.dev,resources=redshiftsinks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=*,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=*,resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=*,resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=*,resources=events,verbs=get;list;watch;create;update;patch;delete

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

func resultRequeueMilliSeconds(ms int) ctrl.Result {
	return ctrl.Result{RequeueAfter: time.Millisecond * time.Duration(ms)}
}

func (r *RedshiftSinkReconciler) fetchLatestTopics(
	kafkaWatcher kafka.Watcher,
	regexes string,
) (
	[]string,
	error,
) {
	var err error
	var rgx *regexp.Regexp
	topicsAppended := make(map[string]bool)
	expressions := strings.Split(regexes, ",")

	allTopics, err := kafkaWatcher.Topics()
	if err != nil {
		return []string{}, err
	}

	var topics []string
	for _, expression := range expressions {
		rgxLoaded, ok := r.KafkaTopicRegexes.Load(expression)
		if !ok {
			rgx, err = regexp.Compile(strings.TrimSpace(expression))
			if err != nil {
				return []string{}, fmt.Errorf(
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

	sortStringSlice(topics)

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
	patcher *statusPatcher,
) (
	ctrl.Result,
	[]ReconcilerEvent,
	error,
) {
	var events []ReconcilerEvent
	result := ctrl.Result{RequeueAfter: time.Second * 30}

	secret, err := r.fetchSecretMap(
		ctx,
		rsk.Spec.SecretRefName,
		rsk.Spec.SecretRefNamespace,
	)
	if err != nil {
		return result, events, err
	}

	tlsConfig, err := r.makeTLSConfig(secret)
	if err != nil {
		return result, events, err
	}

	kafkaWatcher, err := r.loadKafkaWatcher(rsk, tlsConfig)
	if err != nil {
		return result, events, fmt.Errorf("Error fetching kafka watcher, %v", err)
	}

	kafkaTopics, err := r.fetchLatestTopics(
		kafkaWatcher, rsk.Spec.KafkaTopicRegexes,
	)
	if err != nil {
		return result, events, fmt.Errorf("Error fetching topics, err: %v", err)
	}
	if len(kafkaTopics) == 0 {
		klog.Warningf(
			"Kafka topics not found for regex: %s", rsk.Spec.KafkaTopicRegexes)
		return result, events, nil
	}

	sgBuilder := newSinkGroupBuilder()
	if rsk.Spec.Batcher.Mask == false {
		maskLessSinkGroup := sgBuilder.
			setRedshiftSink(rsk).setClient(r.Client).setScheme(r.Scheme).
			setType(MainSinkGroup).
			setTopics(kafkaTopics).
			setMaskVersion("").
			buildBatchers(secret, r.DefaultBatcherImage, r.DefaultKafkaVersion, tlsConfig).
			buildLoaders(secret, r.DefaultLoaderImage, "", r.DefaultKafkaVersion, tlsConfig, r.DefaultRedshiftMaxOpenConns, r.DefaultRedshiftMaxIdleConns).
			build()
		result, maskLessSinkGroupEvent, err := maskLessSinkGroup.reconcile(ctx)
		if len(maskLessSinkGroupEvent) > 0 {
			events = append(events, maskLessSinkGroupEvent...)
		}
		return result, events, err
	}

	gitToken, err := secretByKey(secret, "gitAccessToken")
	if err != nil {
		return result, events, err
	}

	desiredMaskVersion, repo, filePath, err := r.fetchLatestMaskFileVersion(
		rsk.Spec.Batcher.MaskFile,
		gitToken,
	)
	if err != nil {
		return result, events, fmt.Errorf(
			"Error fetching latest mask file version, err: %v\n", err)
	}
	klog.V(2).Infof("rsk/%s desiredMaskVersion=%v", rsk.Name, desiredMaskVersion)

	var currentMaskVersion string
	if rsk.Status.MaskStatus != nil &&
		rsk.Status.MaskStatus.CurrentMaskVersion != nil {
		currentMaskVersion = *rsk.Status.MaskStatus.CurrentMaskVersion
	} else {
		klog.V(2).Infof("rsk/%s, Status empty, currentVersion=''", rsk.Name)
		currentMaskVersion = ""
	}
	klog.V(2).Infof("rsk/%s currentMaskVersion=%v", rsk.Name, currentMaskVersion)

	diffTopics, kafkaTopics, err := MaskDiff(
		kafkaTopics,
		rsk.Spec.Batcher.MaskFile,
		desiredMaskVersion,
		currentMaskVersion,
		gitToken,
		r.KafkaTopicsCache,
	)
	if err != nil {
		return result, events, fmt.Errorf("Error doing mask diff, err: %v", err)
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

	// Safety checks
	if currentMaskVersion == desiredMaskVersion {
		if len(status.reloading) > 0 && len(status.diffTopics) == 0 {
			klog.Errorf(
				"rsk/%s unexpected status, no diff but reloading", rsk.Name)
			status.fixMaskStatus()
		}

		if len(status.released) == 0 {
			klog.Fatalf("rsk/%s unexpected status, released=0", rsk.Name)
		}
	}
	if len(status.diffTopics) == 0 && len(status.reloading) > 0 {
		klog.Errorf("rsk/%s unexpected status, no diff but reloading", rsk.Name)
		status.fixMaskStatus()
		return result, events, nil
	}

	// Realtime status is always calculated to keep the CurrentOffset
	// info updated in the rsk status. This is required so that low throughput
	// release do not get blocked due to missing consumer group currentOffset.
	calc := newRealtimeCalculator(rsk, kafkaWatcher, r.KafkaRealtimeCache, desiredMaskVersion)
	currentRealtime := calc.calculate(status.reloading, status.realtime)
	if len(status.reloading) > 0 {
		klog.V(2).Infof("rsk/%v batchersRealtime: %d / %d (current=%d)", rsk.Name, len(calc.batchersRealtime), len(status.reloading), len(rsk.Status.BatcherReloadingTopics))
		klog.V(2).Infof("rsk/%v loadersRealtime:  %d / %d", rsk.Name, len(calc.loadersRealtime), len(status.reloading))
	}

	if !subSetSlice(currentRealtime, status.realtime) {
		for _, moreRealtime := range currentRealtime {
			status.realtime = appendIfMissing(status.realtime, moreRealtime)
		}
		klog.V(2).Infof(
			"rsk/%s reconcile needed, realtime topics updated: %v",
			rsk.Name,
			status.realtime,
		)
		status.updateBatcherReloadingTopics(rsk.Status.BatcherReloadingTopics, calc.batchersRealtime)
		status.updateLoaderReloadingTopics(rsk.Status.LoaderReloadingTopics, calc.loadersRealtime)
		return resultRequeueMilliSeconds(1500), events, nil
	}
	klog.V(2).Infof("rsk/%v reconciling all sinkGroups", rsk.Name)

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
		setRealtimeCalculator(calc).
		buildBatchers(secret, r.DefaultBatcherImage, r.DefaultKafkaVersion, tlsConfig).
		buildLoaders(secret, r.DefaultLoaderImage, ReloadTableSuffix, r.DefaultKafkaVersion, tlsConfig, r.DefaultRedshiftMaxOpenConns, r.DefaultRedshiftMaxIdleConns).
		build()
	status.updateBatcherReloadingTopics(reload.batcherDeploymentTopics(), calc.batchersRealtime)
	status.updateLoaderReloadingTopics(reload.loaderDeploymentTopics(), calc.loadersRealtime)

	reloadDupe = sgBuilder.
		setRedshiftSink(rsk).setClient(r.Client).setScheme(r.Scheme).
		setType(ReloadDupeSinkGroup).
		setTopics(status.reloadingDupe).
		setMaskVersion(status.currentVersion).
		setTopicGroups().
		setRealtimeCalculator(nil).
		buildBatchers(secret, r.DefaultBatcherImage, r.DefaultKafkaVersion, tlsConfig).
		buildLoaders(secret, r.DefaultLoaderImage, "", r.DefaultKafkaVersion, tlsConfig, r.DefaultRedshiftMaxOpenConns, r.DefaultRedshiftMaxIdleConns).
		build()

	main = sgBuilder.
		setRedshiftSink(rsk).setClient(r.Client).setScheme(r.Scheme).
		setType(MainSinkGroup).
		setTopics(status.released).
		setMaskVersion(status.desiredVersion).
		setTopicGroups().
		setRealtimeCalculator(nil).
		buildBatchers(secret, r.DefaultBatcherImage, r.DefaultKafkaVersion, tlsConfig).
		buildLoaders(secret, r.DefaultLoaderImage, "", r.DefaultKafkaVersion, tlsConfig, r.DefaultRedshiftMaxOpenConns, r.DefaultRedshiftMaxIdleConns).
		build()

	sinkGroups := []*sinkGroup{reloadDupe, reload, main}
	if len(status.reloadingDupe) > 0 {
		sinkGroups = []*sinkGroup{main, reloadDupe, reload}
	}

	for _, sinkGroup := range sinkGroups {
		result, sinkGroupEvents, err := sinkGroup.reconcile(ctx)
		if err != nil {
			return result, nil, err
		}
		if len(sinkGroupEvents) > 0 {
			events = append(events, sinkGroupEvents...)
		}
	}

	if len(events) > 0 {
		return resultRequeueMilliSeconds(3000), events, nil
	}

	if len(status.realtime) == 0 {
		klog.V(2).Infof("rsk/%s nothing done in reconcile", rsk.Name)
		return result, events, nil
	}

	// release the realtime topics, topics in realtime (MaxTopicRelease) are
	// taken as a group and is tried to release in single reconcile
	// to reduce the time spent on rebalance of sink groups (optimization)
	// #141
	releaseCandidates := status.realtime
	if len(status.realtime) >= MaxTopicRelease {
		releaseCandidates = status.realtime[:MaxTopicRelease]
	}
	klog.V(2).Infof("rsk/%s releaseCandidates: %v", rsk.Name, releaseCandidates)

	var releaser *releaser
	if len(releaseCandidates) > 0 {
		releaser, err = newReleaser(
			rsk.Spec.Loader.RedshiftSchema,
			repo,
			filePath,
			currentMaskVersion,
			desiredMaskVersion,
			secret,
		)
		if err != nil {
			return result, events, fmt.Errorf(
				"Error making releaser, err: %v", err)
		}
	}

	releasedTopics := []string{}
	var releaseError error
	for id, releasingTopic := range releaseCandidates {
		klog.V(2).Infof("rsk/%v releasing #%d topic: %v", rsk.Name, id+1, releasingTopic)
		releaseError = releaser.release(
			ctx,
			rsk.Spec.Loader.RedshiftSchema,
			releasingTopic,
			ReloadTableSuffix,
			rsk.Spec.Loader.RedshiftGroup,
			status,
			patcher,
		)
		if releaseError != nil {
			releaseError = fmt.Errorf(
				"Error releasing topic: %s, err: %v",
				releasingTopic,
				releaseError,
			)
			klog.Error(releaseError)
			break
		} else {
			releasedTopics = append(releasedTopics, releasingTopic)
			klog.V(2).Infof("rsk/%v released #%d topic: %v", rsk.Name, id+1, releasingTopic)
			releaser.notifyTopicRelease(
				rsk.Spec.Loader.RedshiftSchema,
				releasingTopic,
				ReloadTableSuffix,
			)
		}
	}
	if len(releasedTopics) > 0 {
		now := time.Now().UnixNano()
		r.ReleaseCache.Store(
			rsk.Namespace+rsk.Name,
			releaseCache{lastCacheRefresh: &now},
		)
		status.notifyRelease(secret, repo, filePath)
	}
	if releaseError != nil {
		return result, events, releaseError
	}
	if len(releasedTopics) > 0 {
		klog.V(2).Infof("rsk/%v: all topics were released succesfully!", rsk.Name)
		return result, events, nil
	}

	// not possible to reach here
	return result, events, nil
}

func (r *RedshiftSinkReconciler) Reconcile(
	ctx context.Context,
	req ctrl.Request,
) (
	_ ctrl.Result,
	reterr error,
) {
	klog.V(2).Infof("Reconciling %+v ...", req)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1800)
	defer cancel()

	var redshiftsink tipocav1.RedshiftSink
	err := r.Get(ctx, req.NamespacedName, &redshiftsink)
	if err != nil {
		return ctrl.Result{
			RequeueAfter: time.Second * 30}, client.IgnoreNotFound(err)
	}

	original := redshiftsink.DeepCopy()
	patcher := &statusPatcher{
		client:    r.Client,
		allowMain: true,
	}

	// Always attempt to patch the status after each reconciliation.
	defer func() {
		if !patcher.allowMain {
			klog.V(4).Infof(
				"rsk/%s patching is not allowed for main", redshiftsink.Name)
			return
		}

		err := patcher.Patch(ctx, original, &redshiftsink, "main")
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
	result, events, err := r.reconcile(ctx, &redshiftsink, patcher)
	if err != nil {
		err = fmt.Errorf("Failed to reconcile: %s", err)
	}

	// Finally, the event is used to generate a Kubernetes event by
	// calling `Record` and passing in the recorder.
	for _, event := range events {
		event.Record(r.Recorder)
	}

	klog.V(2).Infof("Reconciled %+v", req)
	return result, err
}

// SetupWithManager sets up the controller and applies all controller configs
func (r *RedshiftSinkReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&tipocav1.RedshiftSink{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: 10}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Secret{}).
		Complete(r)
}
