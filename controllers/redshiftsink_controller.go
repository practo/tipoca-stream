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

func getGitToken(secret map[string]string) (string, error) {
	gitToken, ok := secret["githubAccessToken"]
	if !ok {
		return "", fmt.Errorf("githubAccessToken not found in secret")
	}

	return gitToken, nil
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

func getCurrentMaskStatus(
	topics []string, reloadTopic map[string]bool,
	currentVersion string, desiredVersion string) map[string]tipocav1.TopicMaskStatus {

	status := make(map[string]tipocav1.TopicMaskStatus)
	for _, topic := range topics {
		_, ok := reloadTopic[topic]
		if ok {
			status[topic] = tipocav1.TopicMaskStatus{
				Version: desiredVersion,
				Phase:   tipocav1.MaskReloading,
			}
		} else {
			status[topic] = tipocav1.TopicMaskStatus{
				Version: currentVersion,
				Phase:   tipocav1.MaskActive,
			}
		}
	}

	return status
}

func getDesiredMaskStatus(
	topics []string, version string) map[string]tipocav1.TopicMaskStatus {

	status := make(map[string]tipocav1.TopicMaskStatus)
	for _, topic := range topics {
		status[topic] = tipocav1.TopicMaskStatus{
			Version: version,
			Phase:   tipocav1.MaskActive,
		}
	}

	return status
}

func (r *RedshiftSinkReconciler) updateStatus(
	rsk *tipocav1.RedshiftSink,
	topics []string,
	reloadTopic map[string]bool,
	currentMaskVersion string,
	desiredMaskVersion string,
) {
	maskStatus := tipocav1.MaskStatus{
		CurrentMaskStatus: getCurrentMaskStatus(
			topics, reloadTopic, currentMaskVersion, desiredMaskVersion,
		),
		DesiredMaskStatus:  getDesiredMaskStatus(topics, desiredMaskVersion),
		CurrentMaskVersion: &currentMaskVersion,
		DesiredMaskVersion: &desiredMaskVersion,
	}
	rsk.Status.MaskStatus = &maskStatus
}

func (r *RedshiftSinkReconciler) reconcile(
	ctx context.Context,
	rsk *tipocav1.RedshiftSink,
) (
	ctrl.Result,
	ReconcilerEvent,
	error,
) {
	result := ctrl.Result{RequeueAfter: time.Second * 5}

	kafkaTopics, err := r.fetchLatestTopics(rsk.Spec.KafkaTopicRegexes)
	if err != nil {
		return result, nil, fmt.Errorf(
			"Error fetching topics, err: %v", err)
	}
	if len(kafkaTopics) == 0 {
		klog.Warningf(
			"Kafka topics not found for regex: %s", rsk.Spec.KafkaTopicRegexes)
	}

	masterSinkGroup := NewSinkGroup(
		MasterSinkGroup, r.Client, r.Scheme, rsk, kafkaTopics, "")

	if rsk.Spec.Batcher.Mask == false {
		result, event, err := masterSinkGroup.Reconcile(ctx)
		return result, event, err
	} else {
		// reconcile master sink group
		result, event, err := masterSinkGroup.Reconcile(ctx)
		if err != nil {
			return result, event, err
		}

		if event != nil {
			return result, event, nil
		}
	}

	secret, err := r.fetchSecretMap(
		ctx, rsk.Spec.SecretRefName, rsk.Spec.SecretRefNamespace)
	if err != nil {
		return result, nil, err
	}
	gitToken, err := getGitToken(secret)
	if err != nil {
		return result, nil, err
	}

	desiredMaskVersion, err := r.fetchLatestMaskFileVersion(
		rsk.Spec.Batcher.MaskFile, gitToken)
	if err != nil {
		return result, nil, fmt.Errorf(
			"Error fetching latest mask file version, err: %v\n", err)
	}

	var currentMaskVersion string
	if rsk.Status.MaskStatus != nil &&
		rsk.Status.MaskStatus.CurrentMaskVersion != nil {
		currentMaskVersion = *rsk.Status.MaskStatus.CurrentMaskVersion
	} else {
		currentMaskVersion = desiredMaskVersion
	}

	reloadTopics, err := MaskDiff(
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

	// update mask status
	r.updateStatus(
		rsk, kafkaTopics, toMap(reloadTopics),
		currentMaskVersion, desiredMaskVersion,
	)

	reloadSinkGroup := NewSinkGroup(
		ReloadSinkGroup, r.Client, r.Scheme, rsk,
		reloadTopics, "-reload",
	)

	// reconcile reload sink group
	result, event, err := reloadSinkGroup.Reconcile(ctx)
	if err != nil {
		return result, event, err
	}
	if event != nil {
		return result, event, nil
	}

	klog.Info("Nothing done.")

	return result, nil, nil
}

func (r *RedshiftSinkReconciler) Reconcile(
	req ctrl.Request) (_ ctrl.Result, reterr error) {

	klog.Infof("Reconciling %+v", req)
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
