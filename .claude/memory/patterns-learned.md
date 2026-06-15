# Patterns Learned
Last updated: 2026-06-15 by Claude

<!-- Append new patterns below. Format:
## Pattern Name
**Where:** File or area where this pattern applies.
**Pattern:** Description of the pattern.
**Example:** Brief code example or file reference.
-->

## Level-triggered reconcile with requeue (never block)
**Where:** `controllers/redshiftsink_controller.go:653` `Reconcile`, `:329` `reconcile`.
**Pattern:** Compute desired vs actual, do one increment of work, return `ctrl.Result{RequeueAfter: ...}` instead of looping/waiting. Short requeues (1.5s `resultRequeueMilliSeconds(1500)`, 3s) while making progress; long ones (15m / 900s) when idle. Status is always patched via a deferred `statusPatcher.Patch`.
**Example:** `redshiftsink_controller.go:490` `return resultRequeueMilliSeconds(1500), events, nil` after updating realtime status.

## Builder pattern for composite objects
**Where:** `controllers/sinkgroup_controller.go` (`newSinkGroupBuilder`), `controllers/status.go` (`newStatusBuilder`).
**Pattern:** Fluent `newXBuilder().setA(...).setB(...).build()` to assemble configuration-heavy objects (sink groups, status) readably. Each setter returns the builder.
**Example:** `redshiftsink_controller.go:432-444` `sBuilder.setRedshiftSink(rsk).setCurrentVersion(...).computeReleased().setRealtime().computeReloading().build()`.

## Three sink groups for zero-downtime mask reload
**Where:** `controllers/redshiftsink_controller.go` reconcile (~lines 494-548), `controllers/release.go`.
**Pattern:** When mask config (git hash) changes, model the transition with `main` (released, desired version), `reload` (reloading into shadow table `_ts_adx_reload`, desired version, own consumer group), `reloadDupe` (same topics, old version, keeps live table fresh). On reload catch-up, "release" swaps the table and collapses back to main.
**Example:** `sinkgroup_controller.go` `const ReloadTableSuffix = "_ts_adx_reload"`; sink group types `MainSinkGroup`/`ReloadSinkGroup`/`ReloadDupeSinkGroup`.

## Reconciler-scoped caching with sync.Map
**Where:** `controllers/redshiftsink_controller.go:53-59` (reconciler fields), populated in `main.go:134-139`.
**Pattern:** Expensive/shared objects (Kafka clients, compiled regexes, topic lists, git caches) are cached on the reconciler in `*sync.Map`, keyed by a stable hash, to avoid rebuilding them every reconcile across concurrent goroutines (`MaxConcurrentReconciles: 10`).
**Example:** `loadKafkaClient` hashes brokers+version (`sha1.Sum`) and `r.KafkaClients.Load/Store` the client (`:283-296`).

## Consumer-group worker (manager + handler + processor)
**Where:** `cmd/redshiftbatcher/main.go:78-115`, `pkg/kafka/manager.go`, `pkg/redshiftbatcher/`, `pkg/redshiftloader/`.
**Pattern:** A worker builds a `kafka.ConsumerGroup` with a handler (`redshiftbatcher.NewHandler` / loader equivalent), then a `kafka.Manager` runs two goroutines: `SyncTopics` (regex topic discovery) and `Consume`. The handler delegates each batch to a processor (`batchProcessor.Process` / `loadProcessor.Process`). Offsets are committed only after successful processing (`markOffset`).
**Example:** `cmd/redshiftbatcher/main.go:104-114` `manager := kafka.NewManager(...); go manager.SyncTopics(...); go manager.Consume(...)`.

## Pluggable behavior behind small interfaces
**Where:** `pkg/notify` (`Notifier`), `pkg/serializer` (`Serializer`), `pkg/transformer` (`MessageTransformer`), `pkg/kafka` (`ConsumerGroupInterface`), `pkg/git` (`GitCacheInterface`), `pkg/schemaregistry` (`SchemaRegistry`).
**Pattern:** External integrations and swappable strategies are defined as narrow interfaces in their leaf package, with a `New...` constructor returning the interface. Enables substitution in tests/callers without a mocking framework.
**Example:** `pkg/notify/notify.go` `type Notifier interface { Notify(message string) error }` + `func New(token, channelID string) Notifier`.

## CRD field + regenerate workflow
**Where:** `api/v1/redshiftsink_types.go`, `Makefile` `generate`/`manifests`.
**Pattern:** Add/change a CR field with JSON tag + kubebuilder marker (`// +optional`, pointer for optional), then run `make generate manifests` to regenerate `zz_generated.deepcopy.go` and `config/crd/bases`. Deprecated fields are kept with a `// Deprecated ... #<issue>` comment, not removed.
**Example:** `redshiftsink_types.go:137` `SinkGroup *SinkGroup` (new) alongside `:140` deprecated `MaxSize int` (#167).

## klog with rsk/ correlation prefix
**Where:** throughout `controllers/`.
**Pattern:** All controller logs use `klog.V(n).Infof("rsk/%s ...", rsk.Name, ...)` so log lines are filterable by resource. `V(2)` is the standard debug level.
**Example:** `redshiftsink_controller.go:407` `klog.V(2).Infof("rsk/%s desiredMaskVersion=%v", rsk.Name, desiredMaskVersion)`.

## Table-driven tests, standard library testing
**Where:** `pkg/transformer/masker/masker_test.go`, `pkg/redshift/redshift_test.go`, etc.
**Pattern:** Plain `func TestX(t *testing.T)` with a slice of input/expected structs looped as subtests; no mocking framework. (Controllers use Ginkgo + envtest instead.)
**Example:** `pkg/transformer/masker/masker_test.go` (663 lines of table cases).
