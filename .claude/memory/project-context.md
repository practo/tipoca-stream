# Project Context
Last updated: 2026-06-15 by Claude

## What This Service Does
tipoca-stream is Practo's open-source (Apache 2.0) **RedshiftSink** — a near-realtime CDC pipeline that streams database changes captured by Debezium into Kafka and sinks them into Amazon Redshift, with first-class **column masking** so the same data can be made widely accessible internally while preserving customer privacy. The repo contains three Go binaries: a Kubernetes **operator** (`redshiftsink`) and two Kafka workers (`redshiftbatcher`, `redshiftloader`). See `README.md`, `REDSHIFTSINK.md`, `MASKING.md`.

<!-- TODO: Verify with team --> Business SLAs (acceptable end-to-end lag, freshness guarantees), the source databases/teams feeding it, and downstream consumers of the Redshift tables are not inferable from code.

## Key Architecture Decisions
- **Operator pattern over a standalone service.** Pipeline state is stored entirely in the `RedshiftSink` CR `.status` (no separate datastore); the controller reconciles desired (spec + live topics + mask git hash) toward actual (Deployments + status). (`controllers/redshiftsink_controller.go`)
- **Three-sink-group mask-reload state machine** (main / reload / reloadDupe) enables zero-downtime re-masking: reload builds a shadow Redshift table at the new mask version while reloadDupe keeps the live table fresh at the old version; on catch-up the topic is "released" (table swap + grants + Slack notify). (`controllers/redshiftsink_controller.go` reconcile lines ~494-548, `controllers/release.go`)
- **Batcher/loader split with S3 + a Kafka signal topic between them** decouples transform/mask (async, S3 upload) from load (sync Redshift COPY+merge), so each scales independently. (`pkg/redshiftbatcher`, `pkg/redshiftloader`)
- **Masking config lives in GitHub, versioned by commit hash**, polled every reconcile — a hash change is what triggers a reload. (`pkg/git`, `fetchLatestMaskFileVersion`)
- **Composable leaf libraries under `pkg/`** behind interfaces (kafka, redshift, s3sink, serializer, transformer, schemaregistry, git, notify) — README explicitly markets these as reusable for other pipelines.
- <!-- TODO: Verify with team --> The "why" behind specific tuning constants (`MaxTopicRelease=5`, default lags 100/10, 30-min reconcile timeout, `MaxConcurrentReconciles=10`) — code shows the values but not the operational reasoning.

## Important Patterns
- **Reconcile = level-triggered + requeue** (never block); short requeues (1.5-3s) when progressing, long ones (15m/900s) when idle.
- **Builder pattern** for sink groups and status (`new*Builder().set*(...).build()`).
- **Caching via `sync.Map`** on the reconciler for Kafka clients, compiled topic regexes, topics, realtime, releases, git, include-tables.
- **Consumer-group workers** (`pkg/kafka` manager runs `SyncTopics` + `Consume` goroutines); offsets committed only after successful processing.
- See `.claude/memory/patterns-learned.md` for file:line examples.

## Request / Reconcile Lifecycle (summary)
`Reconcile` (`controllers/redshiftsink_controller.go:653`): timeout ctx → Get CR → `AllowedResources` gate → deferred status patch → private `reconcile` (`:329`): fetch secret/TLS/Kafka client → regex topics → (maskless fast path) OR (mask diff → status build → realtime calc → build main/reload/reloadDupe → reconcile each → release realtime topics) → record events → patch status. Full trace in `.claude/codebase/ARCHITECTURE.md`.

## Team & Ownership
<!-- TODO: Verify with team --> Owning team, code owners, on-call rotation, and escalation path are unknown from the repo (no CODEOWNERS file, no OWNERS). Repo is public under `practo/tipoca-stream`.

## Known Gotchas
- **`make test` / CI does NOT run `controllers/` tests** (`SRC_DIRS := cmd pkg`). The most complex package can regress green. Run `go test ./controllers/...` (needs envtest) before merging control-plane changes.
- **`klog.Fatalf` in the reconcile path** (`redshiftsink_controller.go:450,:460`) crashes the operator for ALL `RedshiftSink` resources on one bad CR.
- **Leader election defaults OFF** with a hardcoded `LeaderElectionID` — multiple replicas need `--enable-leader-election`.
- Mask config supports **GitHub only**; `KafkaLoaderTopicPrefix` breaks with extra hyphens (`util.go:226`).
- Editing `api/v1/redshiftsink_types.go` requires `make generate manifests`; never hand-edit `zz_generated.deepcopy.go` or `config/crd/bases`.
- Error strings are **capitalized** here by convention (deviates from Go lint); match the existing style.
- Deps are old (Go 1.17, k8s 0.19.2, `Shopify/sarama` pre-IBM-rename); upgrades are non-trivial (vendored + multi-arch).
- Full debt inventory in `.claude/codebase/CONCERNS.md`.
