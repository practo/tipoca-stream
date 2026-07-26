# TESTING

## How to Run
- **`make test`** — the canonical command. Runs `build/test.sh cmd pkg` inside the build Docker image. It does three things: `go test ./cmd/... ./pkg/...`, a `gofmt -l` check (fails on unformatted files), and `go vet`.
- **IMPORTANT: `make test` does NOT run the `controllers/` tests.** `Makefile` sets `SRC_DIRS := cmd pkg`, and `test.sh` only targets those. The controller Ginkgo suite is excluded — see CONCERNS. To run it directly: `go test ./controllers/...` (requires `envtest` kube binaries installed; see below).
- Run a single package locally (outside Docker): `go test ./pkg/transformer/masker/...`.

## Frameworks
- **Standard `testing`** for `pkg/*` — plain `func TestXxx(t *testing.T)` with table-driven cases. This is the dominant style.
- **Ginkgo v1.14.2 + Gomega v1.10.4** for the `controllers/` suite (BDD style: `Describe`/`Context`/`It`, `Expect(...).To(...)`). Entry: `controllers/suite_test.go` `TestAPIs` → `RunSpecsWithDefaultAndCustomReporters`.

## Test Locations (colocated)
Tests live next to the code they test, same package:
- `controllers/`: `redshiftsink_controller` has no direct test, but `unit_allocator_test.go`, `status_test.go`, `batcher_deployment_test.go`, and the `suite_test.go` harness exist.
- `pkg/kafka/manager_test.go`, `pkg/redshiftloader/job_test.go`, `pkg/git/git_test.go`, `pkg/util/random_test.go`, `pkg/redshift/redshift_test.go`.
- `pkg/transformer/masker/{masker,mask_diff,mask_config}_test.go`, `pkg/transformer/debezium/{schema,message}_test.go`.

## Patterns
- **Table-driven tests** are the norm in `pkg/` — a slice of structs with input + expected, looped with subtests. Largest example: `pkg/transformer/masker/masker_test.go` (663 lines).
- **Ginkgo envtest** for controllers: `suite_test.go` `BeforeSuite` boots `envtest.Environment{CRDDirectoryPaths: ["../config/crd/bases"]}` — a real kube API server + etcd from binaries. This is why the suite needs `KUBEBUILDER_ASSETS`/`setup-envtest` and is heavier than unit tests.
- Test data: inline literals and fixtures within the test files; the masking tests construct mask configs in-test. Local CDC fixtures for manual/integration runs live under `build/inventory-mysql/` and `build/producer/`.

## Coverage
- **No coverage threshold or gate** is configured (no coverage flags in `build/test.sh`, no codecov/coveralls config). CI passes purely on tests + gofmt + vet succeeding.

## Unit vs Integration
- **Unit:** everything under `pkg/` (pure-Go, no external services) — these are what CI actually runs.
- **Integration-ish:** the `controllers/` envtest suite (needs a kube control plane) — not wired into CI/`make test`.
- **End-to-end / manual:** `build/docker-compose.yml` + helper scripts (`build/produce.sh`, `build/insert.sh`, `build/query.sh`, `build/reset-kafka-offset.sh`) spin up Kafka/MySQL/producer locally to exercise the real pipeline by hand.

## Mocking
- No mocking framework (no gomock/testify-mock). Interfaces are hand-satisfied with small in-test fakes where needed, or real components are used (envtest provides a real API server rather than a mock client).
- Many seams are interfaces (`kafka.ConsumerGroupInterface`, `git.GitCacheInterface`, `Notifier`, `Serializer`, `MessageTransformer`) precisely so tests/callers can substitute implementations.

## When Adding Tests
- For `pkg/` code: add a colocated `_test.go`, table-driven, standard `testing` — it will be picked up by `make test`.
- For `controllers/` code: extend the Ginkgo suite, but be aware CI will not run it; verify locally with `go test ./controllers/...` + envtest.
