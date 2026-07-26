# INTEGRATIONS

External systems this codebase talks to, and the code that owns each integration.

## Kafka (source of change events)
- Client wrapper: `pkg/kafka/` — `client.go` (admin: list topics, list/delete consumer groups, watermarks), `consumer_group.go`, `consumer_group_handler.go`, `manager.go` (regex topic discovery + consume loop), `metrics.go`.
- Built on `Shopify/sarama`. TLS config assembled from the k8s secret keys `tlsEnable`, `tlsUserCert`, `tlsUserKey`, `tlsCaCert` (`controllers/redshiftsink_controller.go` `makeTLSConfig`).
- Brokers and topic regexes come from the CR spec: `Spec.KafkaBrokers`, `Spec.KafkaTopicRegexes`, `Spec.KafkaVersion` (`api/v1/redshiftsink_types.go`). Default kafka version `2.6.0` (`cmd/redshiftsink/main.go`).
- Topics are matched to a `RedshiftSink` by **regex**, evaluated each reconcile (`fetchLatestTopics`).
- Consumer groups encode the mask version (main / reload / reloadDupe sink groups) — see ARCHITECTURE.

## Amazon S3 (batch staging)
- `pkg/s3sink/s3sink.go` — `S3Sink` wraps `s3manager.Uploader`. Uploads batched, transformed records as files; also writes an S3 `manifest` (`S3Manifest` / `S3ManifestEntry`) used by Redshift `COPY ... FROM ... MANIFEST`.
- Credentials/region/bucket sourced from the k8s secret (see "Secrets" below).
- S3 keys constructed in `pkg/redshiftbatcher/batch_processor.go` `constructS3key`.

## Amazon Redshift (sink target)
- `pkg/redshift/redshift.go` (1405 lines) — the data-loader library: table create/migrate, `COPY` from S3, staging-table de-dup, merge/upsert, drop, group/grant management.
- `pkg/redshift/redshift_collector.go` — optional Prometheus collector that queries Redshift system tables for metrics (enabled by `--collect-redshift-metrics`).
- Connection built in `controllers/redshift_connection.go` (`NewRedshiftConn`) from secret values; driver is `practo/pq`.
- Conn pool sizing: `--default-redshift-max-open-conns` (10) / `--default-redshift-max-idle-conns` (2), overridable per-CR.

## Schema Registry (Confluent-compatible)
- `pkg/schemaregistry/schemaregistry.go` — adapter interface over `riferrei/srclient`; resolves Avro schemas by id/subject and can create schemas.
- `pkg/serializer/serializer.go` — `avroSerializer` deserializes Debezium Avro Kafka messages using `goavro`. Schema-registry URL is passed to `NewSerializer`.

## GitHub (mask configuration source)
- `pkg/git/` — `git.go` (`GitCache`, `GetFileVersion`), `git_url.go` (`ParseURL`, `ParseGithubURL`). Uses `go-git` to fetch the mask YAML and its commit hash from a GitHub repo.
- The operator polls the mask file's latest git hash each reconcile (`fetchLatestMaskFileVersion`); a hash change triggers the mask-reload state machine.
- Auth: `gitAccessToken` key in the k8s secret. **Only github.com is supported** (`controllers/release.go:186` TODO to generalize).

## Slack (release notifications)
- `pkg/notify/notify.go` — `slackNotifier` posts messages via `slack-go/slack`. Used to announce topic releases (`controllers/release.go`, `controllers/status.go` `notifyRelease`).
- Token + channel id from the k8s secret.

## Prometheus (observability + optional control input)
- Batcher/loader expose `/metrics` and `/status` on `:8787` (`cmd/redshiftbatcher/main.go` `serveMetrics`); per-subsystem metrics in `pkg/redshiftbatcher/metrics.go`, `pkg/redshiftloader/metrics.go`, `pkg/kafka/metrics.go`.
- The operator serves metrics on `:8443` (`--metrics-addr`).
- **Optional control feedback:** `--prometheus-url` lets the operator read time-series to enable loader throttling and reset offsets of zero-throughput topics (`pkg/prometheus/`, `cmd/redshiftsink/main.go`). ServiceMonitors in `build/*-servicemonitor.yaml` and `config/prometheus/`.

## Secrets (single k8s Secret per CR)
Fetched by `controllers/redshiftsink_controller.go` `fetchSecretMap`. Defaults: name `redshiftsink-secret`, namespace `ts-redshiftsink-latest` (operator flags). Expected keys include: `tlsEnable`, `tlsUserCert`, `tlsUserKey`, `tlsCaCert`, `gitAccessToken`, plus Redshift/S3/Slack credentials. See `config/operator/kustomization_sample.yaml`.

## CI/CD
- **GitHub Actions:** `.github/workflows/tipoca-stream.yml` (single `build` job, `runs-on: ubuntu-latest`). Steps: `make build` (asserts the 3 linux/amd64 binaries), `make all-container` (asserts the 3 images), `make test`. Triggers: push to `master` and all pull requests.
- Recently migrated from Travis CI (`.travis.yml` removed). Public repo, so it uses GitHub-hosted runners (not the org ARC self-hosted group, which excludes public repos) — rationale documented in the workflow header.
- No deploy stage in CI; image publish (`make all-push` / `manifest-list`) and Kustomize deploy are run manually/elsewhere.

## Docker
- `Dockerfile.in` — templated per binary (`{REGISTRY}`, `{ARG_BIN}`, `{ARG_OS}`, `{ARG_ARCH}` substituted by Make). Single `ADD` of the static binary onto distroless; `ENTRYPOINT` is the binary.
- `build/docker-compose.yml` + `build/producer/`, `build/inventory-mysql/` — local dev harness for producing test CDC data.
