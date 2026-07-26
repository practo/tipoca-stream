# CONCERNS

Technical-debt and risk inventory from static analysis. Prioritized roughly by impact.

## High Impact

### 1. `controllers/` tests are not run by CI
`Makefile` `SRC_DIRS := cmd pkg` and `build/test.sh` only run `go test ./cmd/... ./pkg/...`. The `controllers/` package — which contains the most complex logic (reconcile loop, sink groups, status, release) **and the most test files** (`unit_allocator_test.go`, `status_test.go`, `batcher_deployment_test.go`, plus the `suite_test.go` envtest harness) — is **never exercised by `make test` or CI**. Regressions in the control plane can merge green. The likely reason is that the Ginkgo suite needs `envtest` kube binaries; CI doesn't provision them. Fix: add envtest setup to CI and include `controllers` in the test target (or split the lightweight unit tests so they run without envtest).

### 2. `klog.Fatalf` inside the reconcile loop crashes the whole operator
There are 51 `klog.Fatalf` calls repo-wide, several on the hot reconcile path in `controllers/redshiftsink_controller.go` `reconcile`:
- `:450` `klog.Fatalf("rsk/%s includeTables cannot be zero", ...)`
- `:460` `klog.Fatalf("rsk/%s unexpected status, released=0", ...)`
These are treated as "impossible" invariants, but a `Fatalf` on one CR's bad/edge state **takes down the manager for ALL `RedshiftSink` resources** (kubelet restarts the pod, but it's a global blast radius from a single bad resource). Prefer returning an error (requeue) over `Fatalf` for per-resource conditions.

### 3. Single hardcoded LeaderElectionID
`cmd/redshiftsink/main.go:101` `LeaderElectionID: "854ae6e3."` — opaque, magic, and unchangeable via flag. Combined with leader election being **off by default** (`--enable-leader-election` defaults false), running >1 replica without enabling it risks duplicate reconciliation.

## Medium Impact

### 4. Large files concentrating complexity
Files over 500 lines (non-vendor, non-generated) that may warrant splitting:
- `pkg/redshift/redshift.go` — **1405 lines** (the entire Redshift SQL/loader library in one file).
- `pkg/redshiftloader/load_processor.go` — 880.
- `controllers/sinkgroup_controller.go` — 852.
- `controllers/redshiftsink_controller.go` — 732.
- `pkg/redshiftbatcher/batch_processor.go` — 668.
- `controllers/status.go` — 656.
- `controllers/util.go` — 569.
- `pkg/transformer/masker/mask_config.go` — 516; `pkg/transformer/debezium/schema.go` — 506.

### 5. GitHub-only mask-config support
`controllers/redshiftsink_controller.go` `fetchLatestMaskFileVersion` hard-switches on `url.Host == "github.com"` and errors otherwise. `controllers/release.go:186` carries `// TODO: make it generic for all git repos`. Any non-GitHub mask source is unsupported.

### 6. Loader topic prefix cannot contain "-"
`controllers/util.go:226` `// TODO: fix me please, prefix cannot contain "-" because of this`. The `KafkaLoaderTopicPrefix` parsing is fragile (the spec comment says "at max 1 hyphen"). A footgun for operators configuring CRs.

### 7. Worker config files are git-ignored with no `.env.example` equivalent at root
Real `config.yaml` for batcher/loader is git-ignored (`cmd/*/config/.gitignore`); only `config_sample.yaml` is committed. Required secret keys are implicit (read ad-hoc in `controllers/redshiftsink_controller.go` and `redshift_connection.go`) — there is no single documented list of required secret keys. Onboarding a new deployment requires reading code to know every key.

## Low Impact / Style

### 8. Outstanding TODOs (non-vendor, non-generated): 5
- `api/v1/redshiftsink_types.go:321` — `Group.LoaderCurrentOffset` is "not a dead field once a group moves to released and should be cleaned after that".
- `controllers/release.go:186` — generalize git repos (see #5).
- `controllers/util.go:226` — loader prefix hyphen bug (see #6).
- `controllers/status.go:532` — `// TODO: improve me :( realtime does not have loader topic info`.
- `controllers/unit_allocator_test.go:578` — missing test cases.
(The repo-wide grep for TODO/FIXME/HACK returns ~9,260 hits, but virtually all are inside `vendor/`.)

### 9. Pinned-old dependency baseline
Go 1.17 and `k8s.io/* v0.19.2` / controller-runtime are several major versions behind current. `Shopify/sarama` has since moved org (`IBM/sarama`). Upgrades are non-trivial (vendored, multi-arch) but worth tracking for security patches.

### 10. Error strings capitalized (lint deviation)
House style capitalizes error strings (`fmt.Errorf("Error ...")`), which `golint`/`staticcheck` flag (`ST1005`). There is no linter in CI beyond `go vet`, so this is consistent but non-idiomatic. Keep consistent within files; don't mix.

### 11. Typos in user-facing CRD field docs
`api/v1/redshiftsink_types.go` has several typos in godoc that surface in `kubectl explain` ("Supsend", "trasnform", "upaates", "topci", "Defaut" in `DefautMaxLoaderLag`). Cosmetic but user-visible.
