# CLAUDE.md — tipoca-stream

> Detailed docs live in `.claude/codebase/*.md`. This file is the dense summary; follow the pointers for depth.

## Service Overview
**RedshiftSink** — a near-realtime CDC data pipeline that streams database changes (Debezium → Kafka) into Amazon Redshift, with built-in column masking driven by a GitHub-hosted config. Open-sourced by Practo (Apache 2.0).

This repo ships **three Go binaries**:
- **redshiftsink** (`cmd/redshiftsink`) — Kubernetes **operator** managing `RedshiftSink` CRs (control plane).
- **redshiftbatcher** (`cmd/redshiftbatcher`) — Kafka worker: consume → transform/mask → batch → upload to S3 → signal load (data plane).
- **redshiftloader** (`cmd/redshiftloader`) — Kafka worker: consume signal → `COPY` from S3 → merge/upsert into Redshift (data plane).

<!-- TODO: Verify with team --> Owning team, on-call rotation, and the production deployment topology (namespaces, cluster) are not inferable from code.

## Tech Stack
- **Go 1.17**, static binaries (`CGO_ENABLED=0`), **vendored** deps (`-mod=vendor`). Build toolchain runs in Docker (`public.ecr.aws/practo/golang:1.17.1-alpine`).
- **Operator:** Kubebuilder v2 + `sigs.k8s.io/controller-runtime`; CRD `tipoca.k8s.practo.dev/RedshiftSink` v1 (`api/v1/`). k8s libs `v0.19.2`.
- **Kafka:** `Shopify/sarama` (`pkg/kafka`). **AWS S3:** `aws-sdk-go` (`pkg/s3sink`). **Redshift:** `practo/pq` Postgres driver (`pkg/redshift`).
- **Avro/Schema Registry:** `goavro` + `riferrei/srclient` (`pkg/serializer`, `pkg/schemaregistry`). **Git:** `go-git` (`pkg/git`, GitHub only). **Slack:** `slack-go` (`pkg/notify`). **Metrics:** `prometheus/client_golang`.
- **CLI/config (workers):** cobra + viper. **Logging (everywhere):** `practo/klog/v2` (aliased `klog`).
- See `.claude/codebase/STACK.md` and `INTEGRATIONS.md`.

## Architecture
Operator = control plane; batcher/loader = data plane pods the operator creates. Full detail in `.claude/codebase/ARCHITECTURE.md`.

- **Reconcile loop** (`controllers/redshiftsink_controller.go:653` `Reconcile` → `:329` `reconcile`): level-triggered, requeues instead of blocking; `MaxConcurrentReconciles: 10`; `Owns` Deployments/ConfigMaps/Secrets. Desired state = CR spec + live Kafka topics (regex-matched) + latest mask-file git hash.
- **The central concept — mask-reload via three sink groups:** when the GitHub mask YAML's git hash changes, affected tables must be reloaded into Redshift with new masking *without downtime*. Modeled as:
  - **main** — released topics at desired mask version (live tables).
  - **reload** — topics reloading at desired version into a shadow table (`_ts_adx_reload` suffix), separate consumer group.
  - **reloadDupe** — same topics at the *old* version keeping live tables fresh until reload catches up.
  When the reload group reaches realtime (lag ≤ `ReleaseCondition`), the operator **releases** the topic (`controllers/release.go`): swaps the shadow table in, moves topic to main, tears down reloadDupe, Slack-notifies. If `Batcher.Mask == false`, all this is skipped (single main group).
- **Data flow (async, decoupled):** batcher (`pkg/redshiftbatcher/batch_processor.go`) deserializes Avro, decodes Debezium, masks, batches, uploads to S3, publishes a loader-topic signal — committing Kafka offsets only after success. Loader (`pkg/redshiftloader/load_processor.go`) `COPY`s from the S3 manifest into a staging table, migrates schema if changed, then merges (dedupe → delete-common → delete-deletes → insert) into the target.

## Build & Run
- `make build` — compile the 3 binaries to `bin/<os>_<arch>/` (in Docker).
- `make test` — `go test ./cmd/... ./pkg/...` + `gofmt` check + `go vet` (in Docker). **Note: this does NOT run `controllers/` tests** (see Anti-Patterns).
- `make fmt` — gofmt the code (required before commit; CI fails on unformatted files).
- `make all-container` / `make all-push` — build/push multi-arch images to `public.ecr.aws/practo`.
- `make generate manifests` — regenerate deepcopy + CRDs after editing `api/v1/redshiftsink_types.go` (uses controller-gen v0.2.5).
- `make run` — run the operator against your `~/.kube/config` (`go run ./cmd/redshiftsink/main.go`).
- `make install` / `make deploy` — Kustomize-apply CRDs / operator from `config/`.
- Operator config: **flags only** (`cmd/redshiftsink/main.go`). Worker config: viper YAML (`cmd/*/config/`, real `config.yaml` git-ignored).

## Coding Standards (`.claude/codebase/CONVENTIONS.md`)
- **gofmt + `go vet` are CI-enforced.** Keep both clean.
- Files `snake_case.go`; packages short lowercase; exported `PascalCase`, unexported `camelCase`; interfaces often `...Interface`.
- **Alias imports explicitly** for provenance: `klog`, `tipocav1`, `ctrl`, `corev1`, `appsv1`, `kerrors`.
- **Errors:** wrap with `fmt.Errorf("...: %v", err)`; error strings are **capitalized** here (house style — match it). Aggregate with `kerrors.NewAggregate`.
- **Logging:** `klog` only; verbosity `klog.V(2).Infof(...)`; **prefix controller logs with `rsk/%s`** (resource name).
- **Builders:** `newXBuilder().setX(...).build()` (see `sinkgroup_controller.go`, `status.go`).
- Reference GitHub issue numbers inline for rationale (e.g. `#167`). Preserve `// +kubebuilder:scaffold:*` markers in `main.go`.
- CRD fields need JSON tags + kubebuilder markers; pointers = optional. Never hand-edit `api/v1/zz_generated.deepcopy.go` or `config/crd/bases`.

## Testing (`.claude/codebase/TESTING.md`)
- `pkg/*`: standard `testing`, table-driven (these are what CI runs).
- `controllers/`: **Ginkgo + Gomega** with `envtest` (`suite_test.go`) — needs kube binaries; **not run by `make test`/CI**.
- No coverage gate, no mocking framework (hand-rolled fakes via interfaces).

## API / CRD Conventions
- Single CRD: `RedshiftSink` (short names `rsk`/`rsks`), spec + status subresource (`api/v1/redshiftsink_types.go`).
- Operator stores all pipeline state in the CR `.status` (`MaskStatus`, `TopicGroup`, reloading-topic lists, offsets) — there is no separate DB.
- Topics are bound to a CR by **regex** (`Spec.KafkaTopicRegexes`), re-evaluated every reconcile.

## Common Patterns (`.claude/memory/patterns-learned.md`)
- Add a worker data-transform: implement in `pkg/transformer/...` behind `MessageTransformer`; wire into the batch processor.
- Add a CRD field: edit `api/v1/redshiftsink_types.go` (tags + markers) → `make generate manifests` → consume in `controllers/`.
- Add an external client: new leaf package under `pkg/`, behind an interface, constructed from the k8s secret in `controllers/`.

## Anti-Patterns / Known Concerns (`.claude/codebase/CONCERNS.md`)
- **CI does not run `controllers/` tests** — the most complex package can regress green. Verify control-plane changes locally with `go test ./controllers/...` + envtest.
- **`klog.Fatalf` in the reconcile loop** crashes the operator for ALL resources on one bad CR (`redshiftsink_controller.go:450,:460`). Prefer returning an error (requeue) for per-resource conditions.
- **Leader election is OFF by default** with a hardcoded `LeaderElectionID` — don't run multiple replicas without `--enable-leader-election`.
- `pkg/redshift/redshift.go` is 1405 lines; several files >500 lines (split when touching heavily).
- Mask config is **GitHub-only**; `KafkaLoaderTopicPrefix` is fragile with hyphens (`util.go:226`).
- Deps are old (Go 1.17, k8s 0.19.2); upgrades are non-trivial due to vendoring + multi-arch.

## Related Services (external to this repo)
- **Upstream:** Debezium (CDC source connectors) + Strimzi (Kafka CRDs) produce the Kafka topics this pipeline consumes. Schema Registry serves Avro schemas.
- **Downstream:** Amazon Redshift (sink) and S3 (staging). Slack receives release notifications. Optional Prometheus feeds throttling/offset-reset decisions back into the operator.
- <!-- TODO: Verify with team --> Exact Practo services/teams that own the source DBs and consume the Redshift tables are not inferable from code.

## CI/CD
- GitHub Actions `.github/workflows/tipoca-stream.yml`: single `build` job on `ubuntu-latest` → `make build` → `make all-container` → `make test`. Triggers: push to `master`, all PRs. Recently migrated from Travis; no deploy stage in CI (images/deploy are manual).
