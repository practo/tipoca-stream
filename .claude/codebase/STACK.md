# STACK

## Language & Runtime
- **Go 1.17** (`go.mod` `go 1.17`). Build toolchain image: `public.ecr.aws/practo/golang:1.17.1-alpine` (`Makefile` `BUILD_IMAGE`).
- Module path: `github.com/practo/tipoca-stream` (`go.mod`).
- `CGO_ENABLED=0`, static binaries, vendored deps (`GOFLAGS=-mod=vendor`) — see `build/build.sh`, `build/test.sh`.

## Binaries (3)
Built from `cmd/` (`Makefile` `BINS := redshiftbatcher redshiftloader redshiftsink`):
- `cmd/redshiftsink/main.go` — the Kubernetes operator / controller manager.
- `cmd/redshiftbatcher/main.go` — Kafka consumer that transforms/masks records and uploads batches to S3.
- `cmd/redshiftloader/main.go` — Kafka consumer that loads S3 batches into Redshift.

## Frameworks & Major Libraries
- **Operator framework:** Kubebuilder v2 (`PROJECT` `version: "2"`) on `sigs.k8s.io/controller-runtime` (vendored). CRD group/kind: `tipoca.k8s.practo.dev` / `RedshiftSink` v1 (`PROJECT`, `api/v1/`).
- **Kubernetes client:** `k8s.io/api`, `k8s.io/apimachinery`, `k8s.io/client-go` all `v0.19.2`.
- **Kafka client:** `github.com/Shopify/sarama v1.30.1` (consumer groups, admin) — wrapped in `pkg/kafka/`.
- **CLI:** `github.com/spf13/cobra v1.1.3` + `spf13/pflag` + `spf13/viper v1.7.1` (config) — used by batcher/loader `main.go`.
- **AWS:** `github.com/aws/aws-sdk-go v1.38.51` (S3 upload in `pkg/s3sink/`).
- **Avro / Schema Registry:** `github.com/linkedin/goavro/v2 v2.10.0`, `github.com/riferrei/srclient v0.2.1` (`pkg/schemaregistry/`, `pkg/serializer/`).
- **Redshift driver:** `github.com/practo/pq` (Postgres-wire driver fork) (`pkg/redshift/`).
- **Git access:** `github.com/go-git/go-git/v5 v5.4.1`, `github.com/whilp/git-urls` (`pkg/git/` — reads mask config files from GitHub).
- **Logging:** `github.com/practo/klog/v2 v2.2.1` (klog fork) — the standard logger across the repo.
- **Metrics:** `github.com/prometheus/client_golang v1.10.0`.
- **Slack:** `github.com/slack-go/slack v0.9.1` (`pkg/notify/`).
- **Testing:** `github.com/onsi/ginkgo v1.14.2` + `github.com/onsi/gomega v1.10.4` (controller suite); standard `testing` elsewhere.

## Build Tools
- **Make** is the entry point (`Makefile`), based on thockin/go-build-template. Compiles inside Docker (`make build` runs `go install` in `BUILD_IMAGE`).
- `git describe --tags --always --dirty` sets `VERSION`, injected via ldflags into `pkg/version.Version` (`build/build.sh`).
- `controller-gen v0.2.5` (auto-downloaded by `make manifests` / `make generate`) regenerates CRDs (`config/crd/bases`) and deepcopy code (`api/v1/zz_generated.deepcopy.go`).

## Database / Storage Technologies
- **Amazon Redshift** — the sink target. Accessed over the Postgres wire protocol via `pkg/redshift/redshift.go`.
- **Amazon S3** — staging area for batched data before Redshift `COPY` (`pkg/s3sink/`).
- **Apache Kafka** — source of change events (Debezium CDC). No local DB; the operator stores state in the `RedshiftSink` CR `.status` (`api/v1/redshiftsink_types.go`).

## Infrastructure
- **Container images:** `gcr.io/distroless/static` base (`Makefile` `BASEIMAGE`); pushed to `public.ecr.aws/practo`. Built from `Dockerfile.in` (templated per-binary by Make).
- **Multi-arch:** `linux/amd64 linux/arm linux/arm64 linux/ppc64le linux/s390x` (`ALL_PLATFORMS`); `manifest-list` target builds a multi-arch manifest.
- **Kubernetes deploy:** Kustomize overlays in `config/` (`config/default`, `config/operator`, `config/crd`, `config/rbac`, `config/webhook`, `config/certmanager`, `config/prometheus`).
- Pipeline depends on external operators: **Strimzi** (Kafka CRDs) and **Debezium** (CDC source connectors) — see `README.md`. These live outside this repo.
