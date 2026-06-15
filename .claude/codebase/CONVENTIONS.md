# CONVENTIONS

House style observed across the codebase. Follow these when adding code.

## Formatting
- **gofmt is enforced** — `build/test.sh` fails the build on any non-gofmt'd `.go` file. Tabs for indentation (Go default). Run `make fmt` (`build/format.sh`) before committing.
- **`go vet` is enforced** in CI (`build/test.sh`) — keep vet-clean.

## Naming
- **Packages:** short, lowercase, single word — `kafka`, `redshift`, `s3sink`, `masker`, `serializer`, `notify`. One concept per package under `pkg/`.
- **Files:** `snake_case.go` — `batch_processor.go`, `load_processor.go`, `redshift_connection.go`, `realtime_calculator.go`, `mask_diff.go`. Tests are `<file>_test.go`.
- **Exported types:** PascalCase — `RedshiftSinkReconciler`, `S3Sink`, `MaskConfig`, `ConsumerGroupInterface`.
- **Unexported helpers/types:** camelCase — `batchProcessor`, `loadProcessor`, `sinkGroup`, `statusBuilder`, `releaser`.
- **Interfaces:** suffix `Interface` is common (`sinkGroupInterface`, `kafka.ConsumerGroupInterface`, `git.GitCacheInterface`) but not universal (`Notifier`, `Serializer`, `MessageTransformer` have no suffix). Prefer the existing convention of the package you edit.
- **Constants:** PascalCase exported (`MainSinkGroup`, `ReloadTableSuffix`, `MaxTopicRelease`, `MaskActive`). Grouped in `const (...)` blocks near the top of the file.
- **Builders:** `newXBuilder()` returning a builder with fluent `.setX(...)` methods and a terminal `.build()` (see `sinkgroup_controller.go`, `status.go`).

## Imports
- **Explicit aliasing is heavily used**, even when redundant, to make provenance obvious:
  `klog "github.com/practo/klog/v2"`, `tipocav1 "github.com/practo/tipoca-stream/api/v1"`, `ctrl "sigs.k8s.io/controller-runtime"`, `corev1 "k8s.io/api/core/v1"`, `appsv1 "k8s.io/api/apps/v1"`, `kerrors "k8s.io/apimachinery/pkg/util/errors"`.
- Standard library first, third-party/internal after (grouped by blank lines in most files).
- Kubebuilder scaffold markers (`// +kubebuilder:scaffold:imports`, `:scheme`, `:builder`) must be preserved in `cmd/redshiftsink/main.go`.
- Dot-imports (`.`) only in Ginkgo tests (`. "github.com/onsi/ginkgo"`, `. "github.com/onsi/gomega"`).

## Error Handling
- Errors are created and wrapped with `fmt.Errorf("...: %v", err)` — wrapping with context message + `%v` is the norm (not `%w`, since Go 1.17 here but the codebase predates wide `%w` adoption).
- **Error message strings are capitalized** in this repo (e.g. `fmt.Errorf("Error fetching topics, err: %v", err)`) — this is contrary to standard Go lint advice but is the established style; match it for consistency within a file.
- Multi-error aggregation uses `k8s.io/apimachinery/pkg/util/errors` `NewAggregate` (see `Reconcile` deferred patch) and `github.com/hashicorp/go-multierror` (vendored).
- Functions commonly return multiple named-or-positional values ending in `error`; some return 3-4 strings + error (e.g. `fetchLatestMaskFileVersion`).
- **`klog.Fatalf` is used for invariant violations** that "should never happen" inside reconcile — this terminates the process. Use sparingly and only for true impossibilities (see CONCERNS for the risk).

## Logging
- **Single logger: `github.com/practo/klog/v2`** (aliased `klog`). Do not introduce another logger.
- Verbosity levels via `klog.V(n).Infof(...)` — `V(2)` is the common debug level, `V(4)` for very verbose. Plain `klog.Info/Infof/Warningf/Errorf/Fatalf` for default-visibility logs.
- **Log correlation prefix:** controller logs are prefixed with the resource, `rsk/%s` (e.g. `klog.V(2).Infof("rsk/%s desiredMaskVersion=%v", rsk.Name, ...)`). Keep this prefix when logging inside reconcile.
- The operator also uses controller-runtime's `logr.Logger` (`r.Log`) wired through `klogr` in `main.go`; klog is the primary in handlers/pkg.

## Comments & Docs
- Exported types/funcs have a Go doc comment starting with the identifier name (standard godoc form) — see the extensive comments on `api/v1/redshiftsink_types.go` and `SinkGroup`.
- Design rationale frequently references GitHub issue numbers (e.g. `#141`, `#167`) inline — keep this practice; it links code to the decision.
- Apache 2.0 license header block tops most operator/api files (`cmd/`, `controllers/`, `api/`). `pkg/` files generally omit it.

## API / CRD Conventions
- All `RedshiftSink` spec/status fields require **JSON tags** and kubebuilder markers (`// +optional`, `// +kubebuilder:...`). Pointers (`*string`, `*int`, `*int64`) denote optional/unset fields.
- After editing `api/v1/redshiftsink_types.go`, run `make generate manifests` to regen deepcopy + CRDs. Never hand-edit `zz_generated.deepcopy.go` or `config/crd/bases`.
- Deprecated fields are kept with a `// Deprecated ... #<issue>` comment rather than removed (backward compat) — see deprecated `MaxSize`/`MaxWaitSeconds` superseded by `SinkGroup` (#167).
