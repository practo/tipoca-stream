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

package main

import (
	"context"
	"flag"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/practo/klog/v2"
	prometheus "github.com/practo/tipoca-stream/pkg/prometheus"
	redshift "github.com/practo/tipoca-stream/pkg/redshift"
	pflag "github.com/spf13/pflag"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	"k8s.io/klog/klogr"
	ctrl "sigs.k8s.io/controller-runtime"
	client "sigs.k8s.io/controller-runtime/pkg/client"
	metrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	tipocav1 "github.com/practo/tipoca-stream/api/v1"
	"github.com/practo/tipoca-stream/controllers"
	// +kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	klog.InitFlags(nil)
	pflag.CommandLine.AddGoFlag(flag.CommandLine.Lookup("v"))
	_ = clientgoscheme.AddToScheme(scheme)

	_ = tipocav1.AddToScheme(scheme)
	// +kubebuilder:scaffold:scheme
}

func parseDatabase(databases string) []*string {
	var dbs []*string

	if databases != "" {
		supplied := strings.Split(databases, ",")
		for i, _ := range supplied {
			dbs = append(dbs, &supplied[i]) // use supplied dbs
		}
	} else {
		dbs = append(dbs, nil) // use default db from config
	}

	return dbs
}

func main() {
	rand.Seed(time.Now().UnixNano())

	var enableLeaderElection, collectRedshiftMetrics bool
	var batcherImage, loaderImage, secretRefName, secretRefNamespace string
	var kafkaVersion, metricsAddr, allowedRsks, prometheusURL, databases string
	var redshiftMaxOpenConns, redshiftMaxIdleConns int
	flag.StringVar(&batcherImage, "default-batcher-image", "practodev/redshiftbatcher:v1.0.0-beta.1", "image to use for the redshiftbatcher")
	flag.StringVar(&loaderImage, "default-loader-image", "practodev/redshiftloader:v1.0.0-beta.1", "image to use for the redshiftloader")
	flag.StringVar(&secretRefName, "default-secret-ref-name", "redshiftsink-secret", "default secret name for all redshiftsink secret")
	flag.StringVar(&secretRefNamespace, "default-secret-ref-namespace", "ts-redshiftsink-latest", "default namespace where redshiftsink secret is there")
	flag.BoolVar(&collectRedshiftMetrics, "collect-redshift-metrics", false, "collectRedshiftMetrics when enabled collects redshift metrics for better calculations, used for calculating throttling seconds value at present for each table")
	flag.StringVar(&kafkaVersion, "default-kafka-version", "2.6.0", "default kafka version")
	flag.StringVar(&metricsAddr, "metrics-addr", ":8443", "The address the metric endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "enable-leader-election", false, "Enable leader election for controller manager. Enabling this will ensure there is only one active controller manager.")
	flag.IntVar(&redshiftMaxOpenConns, "default-redshift-max-open-conns", 10, "the maximum number of open connections allowed to redshift per redshiftsink resource")
	flag.IntVar(&redshiftMaxIdleConns, "default-redshift-max-idle-conns", 2, "the maximum number of idle connections allowed to redshift per redshiftsink resource")
	flag.StringVar(&allowedRsks, "allowed-rsks", "", "comma separated list of names of rsk resources to allow, if empty all rsk resources are allowed")
	flag.StringVar(&prometheusURL, "prometheus-url", "", "optional, giving prometheus makes the operator enable new features using time series data. Features: loader throttling, resetting offsets of 0 throughput topics.")
	flag.StringVar(&databases, "databases", "", "comma separated list of all redshift databases to query for redshiftsink_operator.scan_query_total view. This is required for throttling support. Please note: the view should be manually created beforehand for all the specified databases.")
	flag.Parse()

	ctrl.SetLogger(klogr.New())

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:             scheme,
		MetricsBindAddress: metricsAddr,
		Port:               9443,
		LeaderElection:     enableLeaderElection,
		LeaderElectionID:   "854ae6e3.",
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	uncachedClient, err := client.New(
		mgr.GetConfig(),
		client.Options{Scheme: mgr.GetScheme()},
	)
	if err != nil {
		setupLog.Error(err, "unable to make uncached client")
		os.Exit(1)
	}
	var prometheusClient prometheus.Client
	if prometheusURL != "" {
		prometheusClient, err = prometheus.NewClient(prometheusURL)
		if err != nil {
			setupLog.Error(err, "unable to init prometheus")
			os.Exit(1)
		}
	}
	var allowedResources []string
	if allowedRsks != "" {
		allowedResources = strings.Split(allowedRsks, ",")
	}

	if err = (&controllers.RedshiftSinkReconciler{
		Client:                      uncachedClient,
		Log:                         ctrl.Log.WithName("controllers").WithName("RedshiftSink"),
		Scheme:                      mgr.GetScheme(),
		Recorder:                    mgr.GetEventRecorderFor("redshiftsink-reconciler"),
		KafkaClients:                new(sync.Map),
		KafkaTopicRegexes:           new(sync.Map),
		KafkaTopicsCache:            new(sync.Map),
		KafkaRealtimeCache:          new(sync.Map),
		ReleaseCache:                new(sync.Map),
		GitCache:                    new(sync.Map),
		IncludeTablesCache:          new(sync.Map),
		DefaultBatcherImage:         batcherImage,
		DefaultLoaderImage:          loaderImage,
		DefaultSecretRefName:        secretRefName,
		DefaultSecretRefNamespace:   secretRefNamespace,
		DefaultKafkaVersion:         kafkaVersion,
		DefaultRedshiftMaxOpenConns: redshiftMaxOpenConns,
		DefaultRedshiftMaxIdleConns: redshiftMaxIdleConns,
		AllowedResources:            allowedResources,
		PrometheusClient:            prometheusClient,
		RedshiftMetrics:             collectRedshiftMetrics,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "RedshiftSink")
		os.Exit(1)
	}
	// +kubebuilder:scaffold:builder

	if !collectRedshiftMetrics {
		setupLog.Info("Starting Operator... (redshift metrics feature is disabled)")
		if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
			setupLog.Error(err, "problem running manager")
			os.Exit(1)
		}
		return
	}

	ctx, cancel := context.WithCancel(ctrl.SetupSignalHandler())
	defer cancel()
	setupLog.Info("Configuring Redshift exporter...")

	// collect redshift metrics for all databases
	wg := &sync.WaitGroup{}
	dbs := parseDatabase(databases)
	redshiftClients := []*redshift.Redshift{}
	for _, database := range dbs {
		client, err := controllers.NewRedshiftConn(uncachedClient,
			secretRefName,
			secretRefNamespace,
			database,
		)
		if err != nil {
			setupLog.Error(err, "problem initializing redshift connection")
			os.Exit(1)
		}
		redshiftClients = append(redshiftClients, client)
	}

	redshiftCollector := redshift.NewRedshiftCollector(redshiftClients)
	wg.Add(1)
	go redshiftCollector.Fetch(ctx, wg)

	metrics.Registry.MustRegister(redshiftCollector)

	setupLog.Info("Starting Operator...")
	if err := mgr.Start(ctx); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}

	wg.Wait()
}
