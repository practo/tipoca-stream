module github.com/practo/tipoca-stream/redshiftsink

go 1.15

require (
	github.com/Shopify/sarama v1.27.2
	github.com/aws/aws-sdk-go v1.35.17
	github.com/go-git/go-git/v5 v5.2.0
	github.com/go-logr/logr v0.2.2-0.20201024050955-4fa77cb7175c
	github.com/hashicorp/go-multierror v1.1.0
	github.com/linkedin/goavro/v2 v2.9.8
	github.com/onsi/ginkgo v1.11.0
	github.com/onsi/gomega v1.8.1
	github.com/practo/gobatch v0.0.0-20200822085922-4905d08d9f40
	github.com/practo/klog/v2 v2.2.1
	github.com/practo/pq v0.0.0-20200930024154-af3ceb106a20
	github.com/prometheus/client_golang v1.8.0
	github.com/riferrei/srclient v0.0.0-20201014145833-7e71e060b551
	github.com/spf13/cobra v1.1.1
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.7.1
	github.com/whilp/git-urls v1.0.0
	gopkg.in/yaml.v2 v2.3.0
	k8s.io/api v0.17.2
	k8s.io/apimachinery v0.17.2
	k8s.io/client-go v0.17.2
	sigs.k8s.io/controller-runtime v0.5.0
)

replace (
	github.com/go-logr/zapr => github.com/go-logr/zapr v0.2.1-0.20201012235832-146009e52d52 // indirect
	go.uber.org/multierr => go.uber.org/multierr v1.6.0 // indirect
	go.uber.org/zap => go.uber.org/zap v1.16.0 // indirect
	golang.org/x/lint => golang.org/x/lint v0.0.0-20200302205851-738671d3881b // indirect
	golang.org/x/tools => golang.org/x/tools v0.0.0-20201028224754-2c115999a7f0 // indirect
	honnef.co/go/tools => honnef.co/go/tools v0.0.1-2020.1.6 // indirect
)
