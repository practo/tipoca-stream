module github.com/practo/tipoca-stream/redshiftsink

go 1.15

require (
	github.com/Shopify/sarama v1.27.2
	github.com/aws/aws-sdk-go v1.36.21
	github.com/go-git/go-git/v5 v5.2.0
	github.com/go-logr/logr v0.3.0
	github.com/google/uuid v1.1.4
	github.com/hashicorp/go-multierror v1.1.0
	github.com/linkedin/goavro/v2 v2.10.0
	github.com/mitchellh/hashstructure/v2 v2.0.1
	github.com/onsi/ginkgo v1.14.2
	github.com/onsi/gomega v1.10.4
	github.com/practo/klog/v2 v2.2.1
	github.com/practo/pq v0.0.0-20200930024154-af3ceb106a20
	github.com/prometheus/client_golang v1.10.0
	github.com/riferrei/srclient v0.2.1
	github.com/slack-go/slack v0.8.0
	github.com/spf13/cobra v1.1.1
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.7.1
	github.com/whilp/git-urls v1.0.0
	gopkg.in/yaml.v2 v2.4.0
	k8s.io/api v0.19.2
	k8s.io/apimachinery v0.19.2
	k8s.io/client-go v0.19.2
	k8s.io/klog v1.0.0
	sigs.k8s.io/controller-runtime v0.7.0
)

replace github.com/Shopify/sarama => github.com/alok87/sarama v1.27.2-fix1897
