# ARCHITECTURE

## System Type
A near-realtime CDC data pipeline shipped as **three cooperating Go services**, all in this one repo:

1. **redshiftsink operator** (`cmd/redshiftsink`) — a Kubernetes controller (Kubebuilder/controller-runtime) that reconciles `RedshiftSink` custom resources. It does no data movement itself; it provisions and manages batcher/loader Deployments and orchestrates mask reloads.
2. **redshiftbatcher** (`cmd/redshiftbatcher`) — a Kafka consumer-group worker: consume Debezium messages → transform/mask → batch → upload to S3 → signal load.
3. **redshiftloader** (`cmd/redshiftloader`) — a Kafka consumer-group worker: consume "load" signals → `COPY` S3 data into a Redshift staging table → merge/upsert into the target table.

The operator is the control plane; batcher and loader are the data plane (run as pods the operator creates).

## Core Patterns
- **Operator / reconcile pattern** (level-triggered): `controllers/redshiftsink_controller.go` `Reconcile` is the standard controller-runtime entry. Desired state = CR spec + latest Kafka topics + latest mask-file git hash; actual state = existing Deployments/ConfigMaps/Secrets + CR `.status`. The controller drives actual toward desired and **requeues** (`ctrl.Result{RequeueAfter}`) rather than blocking.
- **Builder pattern** for assembling sink groups and status: `newSinkGroupBuilder()` (`controllers/sinkgroup_controller.go`), `newStatusBuilder()` (`controllers/status.go`) — fluent `.setX(...).build()` chains.
- **Consumer-group worker** (batcher/loader): `pkg/kafka` manager runs `SyncTopics` + `Consume` goroutines; per-message work delegated to a handler → processor.
- **Pipeline / transform stages** (batcher): deserialize (Avro) → transform (Debezium decode) → mask → batch → upload.

## The Mask-Reload State Machine (the central concept)
Masking config lives in a GitHub YAML file referenced by the CR. When that file's git hash changes, every affected table must be re-loaded into Redshift with the new masking — **without downtime** for already-released tables. The operator models this with **three sink groups** (`controllers/redshiftsink_controller.go` `reconcile`, lines ~494-548):

- **main** — released topics, running at the *desired* mask version. Consumer group = `main`, table suffix = `""`.
- **reload** — topics that have never been released (or are reloading) at the *desired* mask version, loading into a shadow table. Consumer group = desiredMaskVersion, table suffix = `_reload_<version>`.
- **reloadDupe** — the same reloading topics at the *current* (old) mask version, keeping the live table fresh until the reload catches up. Consumer group = currentMaskVersion, suffix = `""`.

Flow: a topic starts in **reload + reloadDupe**, the reload sink group catches up to realtime (lag ≤ `ReleaseCondition.MaxBatcherLag`/`MaxLoaderLag`), then the operator **releases** it (`controllers/release.go`): the shadow table is swapped in, the topic moves to **main**, reloadDupe is torn down, and a Slack notification fires. Status (`api/v1/redshiftsink_types.go` `MaskStatus`, phases `Active`/`Reloading`/`Realtime`) tracks each topic. When `Batcher.Mask == false`, all of this is skipped and a single maskless main sink group is built.

Realtime/lag calculation: `controllers/realtime_calculator.go`. Reload throttling (max units): `controllers/unit_allocator.go` + `Spec.SinkGroup.MaxReloadingUnits`.

## Request / Reconcile Lifecycle (operator)
Entry: `controllers/redshiftsink_controller.go` `Reconcile` (line 653).
1. 30-min context timeout; `Get` the `RedshiftSink` CR; if not found, requeue (`client.IgnoreNotFound`).
2. `AllowedResources` gate (`--allowed-rsks`) — lets only named CRs reconcile (used to canary new code in prod).
3. `DeepCopy` the original for status diffing; set up a deferred `statusPatcher.Patch(..., "main")`.
4. Call private `reconcile` (line 329):
   a. `fetchSecretMap` → TLS config → `loadKafkaClient` (cached by broker+version hash).
   b. `fetchLatestTopics` (regex match against all Kafka topics).
   c. If `Mask == false`: build one maskless main sink group, reconcile, return.
   d. Else: `fetchLatestMaskFileVersion` (git hash) → `MaskDiff` → `newStatusBuilder()...build()` → safety checks (some are `klog.Fatalf` — see CONCERNS).
   e. `newRealtimeCalculator().calculate(...)`; if realtime set changed, update status and short-requeue (1.5s).
   f. Build the three sink groups; `reconcile` each (creates/updates Deployments). If any emitted events, requeue 3s.
   g. If nothing realtime: remove dead consumer groups, long requeue (15m if reloading present, else 900s).
   h. Else release up to `MaxTopicRelease` (5) realtime topics via `releaser.release(...)`, notify, cache.
5. Back in `Reconcile`: record k8s events, run the deferred status patch, return result/err.
Concurrency: `MaxConcurrentReconciles: 10` (`SetupWithManager`); `Owns` Deployments/ConfigMaps/Secrets so changes to those re-trigger reconcile.

## Data Flow (batcher → loader → Redshift)
Async, decoupled via Kafka topics and S3.
- **Batcher** (`pkg/redshiftbatcher/batch_processor.go`): `Process` → `processMessages` → `processMessage` (deserialize Avro via `pkg/serializer`, decode Debezium via `pkg/transformer/debezium`, mask via `pkg/transformer/masker`) → `processBatch` → upload to S3 (`pkg/s3sink`) → `signalLoad` publishes a message on the loader topic (`KafkaLoaderTopicPrefix` + batcher topic). Offsets committed only after successful upload+signal (`markOffset`).
- **Loader** (`pkg/redshiftloader/load_processor.go`): `Process` → `processBatch` → `loadStagingTable` (`COPY` from S3 manifest) → `migrateTable`/`migrateSchema` if schema changed → `merge` (`deDupeStagingTable` → `deleteCommonRowsInTargetTable` → `deleteRowsWithDeleteOpInStagingTable` → `insertIntoTargetTable`) → drop staging. Sync batch processing (one batch at a time).

## Key Abstractions
- `RedshiftSinkReconciler` (`controllers/redshiftsink_controller.go`) — the controller; holds caches (`sync.Map`) for Kafka clients, topic regexes, topics, realtime, releases, git, include-tables.
- `sinkGroup` / `sinkGroupBuilder` (`controllers/sinkgroup_controller.go`) — owns batcher+loader Deployments for one (type, maskVersion, topics) tuple.
- `status` / `statusBuilder` + `statusPatcher` (`controllers/status.go`) — computes released/reloading/realtime topic sets and patches CR `.status`.
- `releaser` (`controllers/release.go`) — performs the Redshift table swap + grants + Slack notify when a topic is released.
- `kafka.Manager` / `ConsumerGroup` (`pkg/kafka`) — topic discovery + consume loop for the worker binaries.
- `serializer.Message` / `MessageSyncBatch` / `MessageAsyncBatch` (`pkg/serializer`) — the in-flight record + batching abstractions.

## Error Handling Strategy
- **Operator:** errors bubble up from private `reconcile` to `Reconcile`, which wraps them (`fmt.Errorf("Failed to reconcile: %s", err)`) and returns them to controller-runtime, which requeues with backoff. Status is always patched via the deferred patcher (errors aggregated with `kerrors.NewAggregate`).
- **`klog.Fatalf` for "impossible" invariant violations** in reconcile (e.g. `includeTables == 0`, `released == 0`) — these crash the operator process intentionally (51 `klog.Fatalf` calls repo-wide). See CONCERNS.
- **Workers:** processors return errors that stop the batch; offsets are committed only on success, so failures are retried by re-consuming. SIGTERM/SIGINT cancels the root context and closes consumer groups gracefully (`cmd/redshiftbatcher/main.go`).
- Errors are created with `fmt.Errorf` (wrapped, capitalized leading word is the house style here) and logged with `klog`.
