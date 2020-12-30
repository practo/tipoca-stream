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
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"

	logr "github.com/go-logr/logr"
	klog "github.com/practo/klog/v2"
	tipocav1 "github.com/practo/tipoca-stream/redshiftsink/api/v1"
	consumer "github.com/practo/tipoca-stream/redshiftsink/pkg/consumer"
	git "github.com/practo/tipoca-stream/redshiftsink/pkg/git"
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
	KafkaWatcher      consumer.KafkaWatcher
	HomeDir           string

	GitCache *sync.Map
}

// +kubebuilder:rbac:groups=tipoca.k8s.practo.dev,resources=redshiftsinks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=tipoca.k8s.practo.dev,resources=redshiftsinks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch;create;update;patch;delete

// fetchSecretMap fetchs the k8s secret and returns it as a the map
// also it expects the secret to be of
// type as created from ../config/manager/kustomization_sample.yaml
func (r *RedshiftSinkReconciler) fetchSecretMap(
	ctx context.Context, name, namespace string) (map[string]string, error) {

	secret := make(map[string]string)

	k8sSecret, err := getSecret(ctx, r.Client, name, namespace)
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

// fetchLatestMaskFileVersion gets the latest mask file from remote git repository.
// It the git hash of the maskFile.
// Supports only github at present as maskFile parsing is not handled
// for all the types of url formats but with very few line of changes
// it can support all the other git repository.
func (r *RedshiftSinkReconciler) fetchLatestMaskFileVersion(
	maskFile string, gitToken string) (string, error) {
	url, err := git.ParseURL(maskFile)
	if err != nil {
		return "", err
	}

	if url.Scheme != "https" {
		return "", fmt.Errorf("scheme: %s not supported.\n", url.Scheme)
	}

	var repo, filePath string

	switch url.Host {
	case "github.com":
		repo, filePath = git.ParseGithubURL(url.Path)
	default:
		return "", fmt.Errorf("parsing not supported for: %s\n", url.Host)
	}

	var cache git.GitCacheInterface

	cacheLoaded, ok := r.GitCache.Load(repo)
	if ok {
		cache = cacheLoaded.(git.GitCacheInterface)
	} else {
		repoURL := strings.ReplaceAll(maskFile, filePath, "")
		cache, err = git.NewGitCache(repoURL, gitToken)
		if err != nil {
			return "", err
		}
		r.GitCache.Store(repo, cache)
	}

	return cache.GetFileVersion(filePath)
}

func (r *RedshiftSinkReconciler) fetchLatestTopics(
	regexes string) ([]string, error) {

	var topics []string
	var err error
	var rgx *regexp.Regexp
	topicsAppended := make(map[string]bool)
	expressions := strings.Split(regexes, ",")

	allTopics, err := r.KafkaWatcher.Topics()
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

func (r *RedshiftSinkReconciler) reconcile(
	ctx context.Context,
	rsk *tipocav1.RedshiftSink,
) (
	ctrl.Result,
	ReconcilerEvent,
	error,
) {
	result := ctrl.Result{RequeueAfter: time.Second * 15}

	kafkaTopics, err := r.fetchLatestTopics(rsk.Spec.KafkaTopicRegexes)
	if err != nil {
		return result, nil, fmt.Errorf("Error fetching topics, err: %v", err)
	}
	if len(kafkaTopics) == 0 {
		klog.Warningf(
			"Kafka topics not found for regex: %s", rsk.Spec.KafkaTopicRegexes)
	}

	secret, err := r.fetchSecretMap(
		ctx,
		rsk.Spec.SecretRefName,
		rsk.Spec.SecretRefNamespace,
	)
	if err != nil {
		return result, nil, err
	}

	if rsk.Spec.Batcher.Mask == false {
		maskLessSinkGroup := newSinkGroup(
			MainSinkGroup, r.Client, r.Scheme, rsk,
			kafkaTopics,
			"",
			secret,
			"",
		)
		result, event, err := maskLessSinkGroup.reconcile(ctx)
		return result, event, err
	}

	gitToken, err := secretByKey(secret, "gitAccessToken")
	if err != nil {
		return result, nil, err
	}

	desiredMaskVersion, err := r.fetchLatestMaskFileVersion(
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

	diff, err := MaskDiff(
		kafkaTopics,
		rsk.Spec.Batcher.MaskFile,
		desiredMaskVersion,
		currentMaskVersion,
		gitToken,
		r.HomeDir,
	)
	if err != nil {
		return result, nil, fmt.Errorf("Error doing mask diff, err: %v", err)
	}

	klog.V(2).Infof("diff: %v", diff)

	status := newStatusHandler(
		kafkaTopics,
		diff,
		currentMaskVersion,
		desiredMaskVersion,
		rsk)
	// TODO: add some verifications
	// if !status.verify() {
	// 	return result, nil, fmt.Errorf(
	// 		"topics status does not add up as expected")
	// }
	status.initTopicGroup()

	topicsReleased := status.released()
	topicsRealtime := status.realtime()
	topicsReloading := status.reloading()

	klog.V(4).Infof("released: %v", topicsReleased)
	klog.V(4).Infof("realtime: %v", topicsRealtime)
	klog.V(4).Infof("reloading: %v", topicsReloading)

	reloadSinkGroup := newSinkGroup(
		ReloadSinkGroup, r.Client, r.Scheme, rsk,
		topicsReloading,
		desiredMaskVersion,
		secret,
		ReloadTableSuffix,
	)
	topicsRealtime, err = reloadSinkGroup.realtimeTopics(r.KafkaWatcher)
	if err != nil {
		return result, nil, err
	}

	klog.V(4).Infof("realtime (latest): %v", topicsRealtime)

	status.updateMaskStatus(
		topicsReleased,
		topicsRealtime,
		topicsReloading,
	)
	topicsReleased = status.released()
	topicsRealtime = status.realtime()
	topicsReloading = status.reloading()
	topicsReloadingDupe := status.reloadingDupe()

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
	var main, reload, reloadDupe *sinkGroup
	main = newSinkGroup(
		MainSinkGroup, r.Client, r.Scheme, rsk,
		topicsReleased,
		desiredMaskVersion,
		secret,
		"",
	)
	reload = newSinkGroup(
		ReloadSinkGroup, r.Client, r.Scheme, rsk,
		topicsReloading,
		desiredMaskVersion,
		secret,
		ReloadTableSuffix,
	)
	reloadDupe = newSinkGroup(
		ReloadDupeSinkGroup, r.Client, r.Scheme, rsk,
		topicsReloadingDupe,
		currentMaskVersion,
		secret,
		"",
	)
	for _, sinkGroup := range []*sinkGroup{main, reload, reloadDupe} {
		result, event, err := sinkGroup.reconcile(ctx)
		if err != nil {
			return result, nil, err
		}
		if event != nil {
			return result, event, nil
		}
	}

	klog.V(4).Info("checking if release is possible...")

	var topicReleaseEvent *TopicReleasedEvent
	var releaseError error
	var releaser *releaser
	if len(topicsRealtime) > 0 {
		klog.V(2).Infof("release candidates: %v", topicsRealtime)
		releaser, releaseError = newReleaser(
			ctx, rsk.Spec.Loader.RedshiftSchema, secret)
		if releaseError != nil {
			return result, nil, releaseError
		}
		releaseError = releaser.release(
			rsk.Spec.Loader.RedshiftSchema,
			topicsRealtime[0],
			ReloadTableSuffix,
			rsk.Spec.Loader.RedshiftGroup,
		)
		if releaseError != nil {
			klog.Errorf(
				"Error performing release for topic: %s, err: %v",
				topicsRealtime[0],
				releaseError,
			)
		} else {
			topicsReleased = append(topicsReleased, topicsRealtime[0])
			topicReleaseEvent = &TopicReleasedEvent{
				Topic:   topicsRealtime[0],
				Version: desiredMaskVersion,
			}
			klog.V(2).Infof(
				"released topic: %v, version: %v",
				topicsRealtime[0], desiredMaskVersion,
			)
			status.updateTopicGroup(topicsRealtime[0])
		}
	}

	// remove the released topics
	topicsReloading = removeReleased(topicsReloading, toMap(topicsReleased))
	topicsRealtime = removeReleased(topicsRealtime, toMap(topicsReleased))
	status.updateMaskStatus(
		topicsReleased,
		topicsRealtime,
		topicsReloading,
	)

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
	req ctrl.Request) (_ ctrl.Result, reterr error) {

	klog.Infof("Reconciling %+v {-_-}", req)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
