# Architecture and Tech Choices (Deep Guide With Alternatives)

This guide explains what this repository chose, why it likely chose it, and what alternatives exist at every level.

For exact file coverage, use:
- `docs/repo_file_catalog.md` (all tracked files)

## 1) System-Level Architecture Choices

### Choice: Event-Driven Pipeline (Kafka-Centric)
- Current approach:
  - Services communicate via Kafka topics (`ticks`, `bars.*`, `orders*`, `fills`).
  - Each stage is a separate process that can scale/fail independently.
- Why this is strong:
  - Loose coupling and replayability.
  - Natural buffering between fast and slow stages.
- Alternatives:
  - Direct RPC (gRPC/REST): simpler topology, weaker buffering/replay by default.
  - In-process monolith: easier debugging early, weaker independent scaling.
  - Managed stream platforms (Pulsar/Kinesis/Redpanda): different operational and cost profiles.

### Choice: Polyglot Runtime (Python + Go + C++)
- Current approach:
  - Python for orchestration/research-heavy logic.
  - Go path for high-throughput ingress/aggregation migration.
  - C++ matcher for low-latency paper execution realism.
- Why this is strong:
  - Places each language where it has advantages.
- Alternatives:
  - All Python: fastest iteration, lower peak performance for hot loops.
  - All Go: stronger uniformity/perf, slower research iteration.
  - All Rust/C++: strongest performance/safety control, highest onboarding cost.

### Choice: Postgres as Operational Source of Record
- Current approach:
  - Bars, orders, fills, OMS state, and recon artifacts in Postgres.
- Why this is strong:
  - ACID guarantees, mature tooling, simple local setup.
- Alternatives:
  - Time-series DB (Timescale/ClickHouse): better large-scale analytical patterns.
  - NoSQL stores: flexible schema, weaker transactional clarity for OMS workflows.

## 2) Stage-by-Stage Design Choices

### Ingestion (`ingestion/`, `go/ingestion/`)
- Current approach:
  - Websocket ticks converted to compact events, queued, then published to Kafka.
  - Token list from CSV and auth token from JSON file.
- Why this is strong:
  - Minimal payload and clear contract.
- Alternatives:
  - Binary payloads (Protobuf/Avro): smaller/faster but needs schema registry discipline.
  - Dynamic instrument discovery service vs static CSV.
  - Managed broker SDK wrappers vs direct websocket protocol handling.

### Compute (`compute/`, `go/compute/`)
- Current approach:
  - 1s bar builder then multi-timeframe aggregators.
  - Grace periods for late ticks; upsert semantics to handle updates.
- Why this is strong:
  - Deterministic and auditable transformation chain.
- Alternatives:
  - Stream processor frameworks (Flink/Spark Structured Streaming): stronger built-in state/checkpointing with higher complexity.
  - In-memory TSDB + periodic batch persistence.

### Strategy (`strategy/`, `indicators/`)
- Current approach:
  - YAML-driven strategy and indicator configuration.
  - Modular runner and ensemble engine.
- Why this is strong:
  - Fast experimentation without editing core runtime each time.
- Alternatives:
  - Hard-coded strategies for strict control and speed.
  - DSL-based strategy language.
  - Feature-store + model-serving architecture for stronger ML pathways.

### Risk (`risk/`)
- Current approach:
  - Two-stage controls: sizing first (`manager_v2`), budget guard second.
- Why this is strong:
  - Separation of responsibilities (quantity math vs aggregate throttling).
- Alternatives:
  - Single unified risk engine (simpler topology, broader blast radius on bugs).
  - External risk microservice with central policy store.

### Execution (`execution/`)
- Current approach:
  - OMS state machine + gateway + poller, with dry/paper/live modes.
- Why this is strong:
  - Idempotent lifecycle and clear broker reconciliation boundary.
- Alternatives:
  - Direct strategy-to-broker calls (lower latency, significantly higher risk).
  - Managed broker abstraction platform.

### Monitoring (`monitoring/`, `infra/prometheus`, `infra/grafana`)
- Current approach:
  - Prometheus exporters per service and pre-provisioned Grafana dashboards.
- Why this is strong:
  - Fast operational visibility and low custom UI burden.
- Alternatives:
  - OpenTelemetry + vendor APM stack.
  - Cloud-native monitoring suites.

### Research and Promotion (`backtest/`, `analytics/`, `tools/`)
- Current approach:
  - Offline backtest/scoring pipeline to generate next-day candidate sets.
- Why this is strong:
  - Clear separation of research from live control plane.
- Alternatives:
  - Online learning with continual deployment (higher model risk/ops complexity).

## 3) Artifact-Level Choices (Smallest Units)

### Python file (`*.py`)
- Current role:
  - Service logic, orchestration, analytics, utilities.
- Why chosen:
  - Fast development, rich ecosystem.
- Alternatives:
  - Go (throughput/service reliability), Rust (safe performance), Java/Kotlin (enterprise stream ops).

### Go file (`*.go`)
- Current role:
  - High-throughput and swappable service implementations.
- Why chosen:
  - Strong concurrency model and easy deployment as single binaries.
- Alternatives:
  - Rust (safer memory/perf), C++ (max control), Java (mature stream tooling).

### C++ file (`*.cpp`)
- Current role:
  - Matcher where latency realism matters.
- Why chosen:
  - Deterministic low-latency control.
- Alternatives:
  - Rust for safer native engine; Go/Java for easier maintenance.

### Dockerfile (`Dockerfile.app`, `infra/docker/go-builder.Dockerfile`)
- Current role:
  - Build/run reproducibility.
- Why chosen:
  - Portable runtime and build environment.
- Alternatives:
  - Nix/Bazel, VM images, serverless jobs, direct host package management.

### Compose file (`infra/docker-compose.yml`)
- Current role:
  - Local stack orchestration.
- Why chosen:
  - One-command dev/test infra startup.
- Alternatives:
  - Kubernetes (heavier but scalable), Nomad, local scripts.

### SQL migration (`infra/postgres/init/*.sql`)
- Current role:
  - Explicit schema initialization and evolution.
- Why chosen:
  - Full control and transparency.
- Alternatives:
  - Migration frameworks (Alembic/Flyway/Liquibase), ORM migrations.

### YAML config (`configs/*.yaml`, `strategy/*.yaml`)
- Current role:
  - Runtime policies and strategy/risk/runner behavior.
- Why chosen:
  - Editable and reviewable config-as-code.
- Alternatives:
  - JSON/TOML/HCL, DB-backed config service, feature-flag platforms.

### JSON schema/dashboard (`schemas/*.json`, `infra/grafana/dashboards/*.json`)
- Current role:
  - Contract validation and dashboard provisioning.
- Why chosen:
  - Tool-compatible structured representation.
- Alternatives:
  - Avro/Protobuf for contracts; UI-created dashboards with API exports.

### Shell script (`tools/*.sh`, `infra/scripts/*.sh`)
- Current role:
  - Operational glue commands.
- Why chosen:
  - Fast, transparent automation.
- Alternatives:
  - Python/Go CLIs, Ansible playbooks, task runners.

### CSV dataset (`configs/*.csv`, `data/*.csv`)
- Current role:
  - Instrument/universe/reference data.
- Why chosen:
  - Human-editable and source-control friendly.
- Alternatives:
  - Database tables, parquet, managed metadata service.

### Markdown docs (`docs/*.md`)
- Current role:
  - Human knowledge layer.
- Why chosen:
  - Low friction and git-reviewable.
- Alternatives:
  - Wiki, docs portal, ADR repository.

## 4) Go Migration Strategy (Why This Repo Uses Swappable Services)

### Choice: Keep Python Path + Add Go Path
- Current approach:
  - Env-overridable process supervisor commands (`SERVICE_CMD_*`, `INGEST_CMD`, `BARS1S_CMD`, `BARAGG_CMD`).
- Why this is strong:
  - Safe incremental migration with immediate rollback path.
- Alternatives:
  - Big-bang rewrite (high delivery risk).
  - Sidecar duplication without command-level swappability (harder ops story).

### Contract Compatibility Principle
- Must remain stable:
  - Topic names/payload contracts.
  - DB schemas/upsert semantics.
  - Metrics needed by dashboards/alerts.
- Can change:
  - Internal implementation language and concurrency model.

## 5) Why Some Choices Are Conservative (And Why That Is Good)

- Conservative defaults (dry run, explicit risk gates, idempotent OMS transitions) reduce catastrophic mistakes.
- High-frequency systems fail from edge cases, not happy paths.
- Conservative architecture often outperforms “clever” architecture in production reliability.

## 6) Interview Defense Template (Use This for Every Design Choice)

For any file/component, answer in this sequence:
1. `What` does it do?
2. `Why` this design was selected?
3. `Tradeoff` accepted?
4. `Alternative` and when it is better?
5. `Control` mechanism for failure/quality?

If you can answer this template for all entries in `docs/repo_file_catalog.md`, you have full architecture command of the repository.
