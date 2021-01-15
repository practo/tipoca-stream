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
	"flag"
	"os"
	"sync"

	"github.com/practo/klog/v2"
	pflag "github.com/spf13/pflag"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	"k8s.io/klog/klogr"
	ctrl "sigs.k8s.io/controller-runtime"

	tipocav1 "github.com/practo/tipoca-stream/redshiftsink/api/v1"
	"github.com/practo/tipoca-stream/redshiftsink/controllers"
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

func main() {
	var enableLeaderElection bool
	var batcherImage, loaderImage, secretRefName, secretRefNamespace, kafkaVersion, metricsAddr string
	flag.StringVar(&batcherImage, "default-batcher-image", "practodev/redshiftbatcher:latest", "image to use for the redshiftbatcher")
	flag.StringVar(&loaderImage, "default-loader-image", "practodev/redshiftloader:latest", "image to use for the redshiftloader")
	flag.StringVar(&loaderImage, "default-secret-ref-name", "redshiftsink-secret", "default secret name for all redshiftsink secret")
	flag.StringVar(&loaderImage, "default-secret-ref-namespace", "ts-redshiftsink", "default namespace where redshiftsink secret is there")
	flag.StringVar(&kafkaVersion, "default-kafka-version", "2.6.0", "default kafka version")
	flag.StringVar(&metricsAddr, "metrics-addr", ":8080", "The address the metric endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "enable-leader-election", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
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

	if err = (&controllers.RedshiftSinkReconciler{
		Client:                    mgr.GetClient(),
		Log:                       ctrl.Log.WithName("controllers").WithName("RedshiftSink"),
		Scheme:                    mgr.GetScheme(),
		Recorder:                  mgr.GetEventRecorderFor("redshiftsink-reconciler"),
		KafkaWatchers:             new(sync.Map),
		KafkaTopicRegexes:         new(sync.Map),
		GitCache:                  new(sync.Map),
		DefaultBatcherImage:       batcherImage,
		DefaultLoaderImage:        loaderImage,
		DefaultSecretRefName:      secretRefName,
		DefaultSecretRefNamespace: secretRefNamespace,
		DefaultKafkaVersion:       kafkaVersion,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "RedshiftSink")
		os.Exit(1)
	}
	// +kubebuilder:scaffold:builder

	setupLog.Info("Starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}
