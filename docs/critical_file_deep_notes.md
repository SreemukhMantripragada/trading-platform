# Critical File Deep Notes (Top 40, Make-or-Break)

This document is intentionally strict and repetitive.
Each entry uses the same structure so you can study without ambiguity.

For complete file coverage, use:
- `docs/repo_file_catalog.md` (all tracked files)

How to read each entry:
- `Why this exists`: business and architecture purpose.
- `Consumes`: what it reads (topics/files/env/tables).
- `Produces`: what it writes/emits.
- `Breaks when`: common failure mode to watch.
- `Alternatives`: realistic implementation options and tradeoffs.

## 1) Platform Control and Infrastructure

### 01) `README.md`
- `Why this exists`: single source of truth for system map and startup flow.
- `Consumes`: human understanding and repo conventions.
- `Produces`: operator/developer onboarding path.
- `Breaks when`: README drifts from real commands/services.
- `Alternatives`: docs portal (MkDocs), ADR index, internal wiki.

### 02) `Makefile`
- `Why this exists`: stable command interface for multi-service operations.
- `Consumes`: shell, env vars, script paths.
- `Produces`: repeatable operational actions (`make up`, `make risk-v2`, etc.).
- `Breaks when`: target command paths or env names change silently.
- `Alternatives`: Taskfile, justfile, Python CLI, shell-only scripts.

### 03) `configs/process_supervisor.yaml`
- `Why this exists`: declarative stack composition for coordinated startup.
- `Consumes`: service command definitions and env defaults.
- `Produces`: supervisor runtime plan.
- `Breaks when`: service names/cmds mismatch actual binaries/scripts.
- `Alternatives`: systemd units, Docker Compose service-only orchestration, Nomad jobs.

### 04) `orchestrator/process_supervisor.py`
- `Why this exists`: process lifecycle manager with logs, signal fanout, graceful shutdown.
- `Consumes`: YAML config, `.env`, `infra/.env.docker` (plus legacy `infra/.env`), OS signals.
- `Produces`: child process tree, per-service logs, controlled shutdown behavior.
- `Breaks when`: invalid config or child crash policy misunderstood.
- `Alternatives`: supervisord, systemd, pm2, Kubernetes Deployments.

### 05) `infra/docker-compose.yml`
- `Why this exists`: local full-stack infra bootstrap (Kafka/Postgres/Prometheus/Grafana).
- `Consumes`: generated `infra/.env.docker` (source: `configs/docker_stack.json`), mounted configs/scripts.
- `Produces`: reproducible local infrastructure.
- `Breaks when`: port collisions, env mismatches, container health checks fail.
- `Alternatives`: Kubernetes kind/minikube, managed cloud services, local binaries.

### 06) `infra/postgres/init/01_trading_v1.sql`
- `Why this exists`: base trading schema initialization.
- `Consumes`: Postgres bootstrap init process.
- `Produces`: foundational tables/indexes.
- `Breaks when`: schema assumptions drift from code queries.
- `Alternatives`: migration frameworks (Alembic/Flyway/Liquibase).

### 07) `infra/postgres/init/07_oms_orders.sql`
- `Why this exists`: OMS core tables for order lifecycle + audit.
- `Consumes`: Postgres bootstrap sequence.
- `Produces`: durable order-state storage.
- `Breaks when`: state transition code expects fields/indexes not present.
- `Alternatives`: event-store DB, append-only ledger model, CQRS split stores.

### 08) `infra/postgres/init/09_oms_live.sql`
- `Why this exists`: live order and execution tracking schema.
- `Consumes`: OMS/gateway data model assumptions.
- `Produces`: `live_orders` and related execution persistence.
- `Breaks when`: gateway/poller writes diverge from table constraints.
- `Alternatives`: dedicated execution DB, managed broker-state mirror service.

### 09) `infra/prometheus/prometheus.yml`
- `Why this exists`: scrape topology for all exporters.
- `Consumes`: target host:port metrics endpoints.
- `Produces`: unified time-series monitoring dataset.
- `Breaks when`: exporter ports change and scrape config is stale.
- `Alternatives`: OpenTelemetry collector pipelines, cloud APM agents.

### 10) `infra/grafana/provisioning/datasources/datasource.yml`
- `Why this exists`: datasource provisioning as code.
- `Consumes`: datasource connection details.
- `Produces`: deterministic Grafana datasource setup.
- `Breaks when`: datasource auth/host changes without config update.
- `Alternatives`: manual Grafana setup, Terraform Grafana provider.

## 2) Go Runtime and Build Path (Special Focus)

### 11) `go.mod`
- `Why this exists`: dependency contract for all Go services.
- `Consumes`: module versions.
- `Produces`: reproducible Go builds.
- `Breaks when`: incompatible SDK upgrades (Kafka, Kite, pgx).
- `Alternatives`: vendoring, monorepo lock tooling, Bazel-managed deps.

### 12) `infra/docker/go-builder.Dockerfile`
- `Why this exists`: build Go binaries without local Go toolchain.
- `Consumes`: `go.mod`, `go.sum`, `go/` source.
- `Produces`: compiled binaries for swappable services.
- `Breaks when`: dependency resolution or package path changes.
- `Alternatives`: local Go build, CI artifacts, multi-arch build pipelines.

### 13) `go/pkg/shared/kafka.go`
- `Why this exists`: standardized Kafka producer/consumer abstraction.
- `Consumes`: broker/group/topic config, JSON payload structs.
- `Produces`: consistent poll/produce/commit behavior across Go services.
- `Breaks when`: commit discipline differs from Python semantics.
- `Alternatives`: Sarama, segmentio/kafka-go, Redpanda client APIs.

### 14) `go/pkg/shared/pg.go`
- `Why this exists`: centralized Postgres pool management for Go services.
- `Consumes`: `POSTGRES_*` env config.
- `Produces`: pooled DB execution path.
- `Breaks when`: DSN/env mismatch or pool sizing wrong for load.
- `Alternatives`: `database/sql` + pq, GORM, sqlc-generated repositories.

### 15) `go/ingestion/ws_bridge/main.go`
- `Why this exists`: Go websocket ingestion path for high tick throughput.
- `Consumes`: Kite tokens, instruments CSV, websocket ticks.
- `Produces`: Kafka `ticks` payloads and ingress metrics.
- `Breaks when`: reconnect logic or queue pressure handling is weak.
- `Alternatives`: Python ingress, Rust async websocket bridge, managed feed adapter.

### 16) `go/compute/bar_builder_1s/main.go`
- `Why this exists`: Go 1-second bar builder replacement candidate.
- `Consumes`: Kafka `ticks`, DB connection.
- `Produces`: Postgres `bars_1s`, Kafka `bars.1s`.
- `Breaks when`: flush/commit ordering causes duplicates/loss perception.
- `Alternatives`: Python `compute/bar_builder_1s.py`, Flink windows, ksqlDB transforms.

### 17) `go/compute/bar_aggregator_multi/main.go`
- `Why this exists`: Go multi-timeframe aggregator replacement candidate.
- `Consumes`: `bars.1s` or canonical bar stream.
- `Produces`: `bars.{tf}m` topics and `bars_{tf}m` table upserts.
- `Breaks when`: bucket boundaries or late-data flush logic diverges from Python.
- `Alternatives`: Python multi-agg, stream processor framework, DB materialized views.

## 3) Ingestion and Contracts

### 18) `configs/tokens.csv`
- `Why this exists`: instrument token-to-symbol source for subscriptions.
- `Consumes`: manual/automated instrument updates.
- `Produces`: subscription universe for ingestion services.
- `Breaks when`: stale tokens or wrong symbols cause data loss/misrouting.
- `Alternatives`: instrument master table, broker API pull at startup, config service.

### 19) `ingestion/zerodha_ws.py`
- `Why this exists`: production-proven Python websocket to Kafka bridge.
- `Consumes`: Kite auth token, token universe, WS callbacks.
- `Produces`: `ticks` topic events + ingress metrics.
- `Breaks when`: queue overflows, auth expires, reconnect storms.
- `Alternatives`: Go ws bridge, managed market-data feed connector.

### 20) `ingestion/smoke_producer.py`
- `Why this exists`: synthetic tick source for dry testing.
- `Consumes`: local test intent and config.
- `Produces`: predictable test traffic into Kafka.
- `Breaks when`: test assumptions differ from real feed behavior.
- `Alternatives`: replay from captured tick logs, deterministic simulation engine.

### 21) `ingestion/merge_hist_daily.py`
- `Why this exists`: historical merge/reconciliation pipeline for bar completeness.
- `Consumes`: historical vendor/broker inputs.
- `Produces`: updated historical bar records.
- `Breaks when`: time-zone/session alignment is wrong.
- `Alternatives`: dedicated batch ETL tool (Airflow/Dagster), lakehouse ingest jobs.

### 22) `schemas/order.schema.json`
- `Why this exists`: contract guard for order payload integrity.
- `Consumes`: order event schema expectations.
- `Produces`: validation boundary for producer/consumer compatibility.
- `Breaks when`: producers add/rename fields without controlled evolution.
- `Alternatives`: Avro/Protobuf with schema registry.

## 4) Compute Layer (Hot Path)

### 23) `compute/bar_builder_1s.py`
- `Why this exists`: canonical Python tick-to-1s bar transformation.
- `Consumes`: Kafka `ticks`.
- `Produces`: Postgres `bars_1s` + Kafka `bars.1s`.
- `Breaks when`: flush grace tuning is wrong for market latency profile.
- `Alternatives`: Go builder, streaming SQL engine, broker-side bar feed.

### 24) `compute/bar_aggregator_1m.py`
- `Why this exists`: 1m OHLCV construction from lower granularity bars.
- `Consumes`: lower timeframe bar stream.
- `Produces`: `bars.1m` topic/table writes.
- `Breaks when`: bucket boundary math or event ordering assumptions fail.
- `Alternatives`: multi-agg-only architecture without separate 1m service.

### 25) `compute/bar_aggregator_1m_to_multi.py`
- `Why this exists`: derive 3m/5m/15m/... bars from canonical stream.
- `Consumes`: canonical bar stream and optional vendor verification config.
- `Produces`: multi-timeframe tables/topics + verification correction metrics.
- `Breaks when`: override/correction logic masks upstream data defects.
- `Alternatives`: precomputed vendor candles, stream processor window joins.

### 26) `compute/sinks/s3_archiver.py`
- `Why this exists`: long-term retention offload from hot database.
- `Consumes`: bar history in Postgres or local retention window.
- `Produces`: parquet archives in S3/object storage.
- `Breaks when`: archive gaps appear (date ranges, credentials, retries).
- `Alternatives`: direct lakehouse ingest, CDC into warehouse.

## 5) Strategy Layer

### 27) `strategy/runner_modular.py`
- `Why this exists`: primary modular strategy execution engine.
- `Consumes`: timeframe bars, indicator configs, strategy registry.
- `Produces`: `orders` topic + orders table rows.
- `Breaks when`: indicator readiness or state handling is inconsistent.
- `Alternatives`: monolithic runner, per-strategy microservice model.

### 28) `strategy/ensemble_engine.py`
- `Why this exists`: combines individual strategy signals into final decision.
- `Consumes`: multiple signal outputs and ensemble policy.
- `Produces`: consolidated signal/action.
- `Breaks when`: weight/quorum misconfiguration hides useful signals.
- `Alternatives`: ML meta-model, rule graph engine, voting with confidence calibration.

### 29) `strategy/registry.yaml`
- `Why this exists`: declarative strategy catalog and parameters.
- `Consumes`: strategy enablement and param settings.
- `Produces`: runner instantiation plan.
- `Breaks when`: stale names/params mismatch actual strategy classes.
- `Alternatives`: DB-backed strategy registry, code-only registration.

### 30) `strategy/ensemble.yaml`
- `Why this exists`: policy config for ensemble decision logic.
- `Consumes`: mode/k/weights thresholds.
- `Produces`: runtime ensemble behavior.
- `Breaks when`: config changes without validation tests.
- `Alternatives`: feature flags with staged rollout, model-driven arbitration.

### 31) `configs/indicators.yaml`
- `Why this exists`: indicator parameterization per timeframe.
- `Consumes`: tuning choices from research.
- `Produces`: runtime indicator instantiation settings.
- `Breaks when`: bad windows create unstable or lagging signals.
- `Alternatives`: code constants, model feature store config.

## 6) Risk Layer

### 32) `risk/manager_v2.py`
- `Why this exists`: primary sizing engine and approval/rejection decision point.
- `Consumes`: `orders`, risk budget config, broker/equity context.
- `Produces`: `orders.sized`, updated order status/extra risk metadata.
- `Breaks when`: sizing fallback logic is misunderstood under broker API failures.
- `Alternatives`: external risk microservice, single-step hard cap engine.

### 33) `risk/order_budget_guard.py`
- `Why this exists`: post-sizing throttle for bucket and per-trade limits.
- `Consumes`: `orders.sized`, computed budgets.
- `Produces`: `orders.allowed` or reject counters.
- `Breaks when`: in-memory `used` state resets unexpectedly on restart.
- `Alternatives`: persistent budget state table, Redis-based budget allocator.

### 34) `configs/risk_budget.yaml`
- `Why this exists`: central risk policy source (bucket split, caps, rules).
- `Consumes`: risk policy decisions.
- `Produces`: deterministic risk runtime behavior.
- `Breaks when`: unreviewed config edits materially alter exposure.
- `Alternatives`: policy service with approval workflow and versioned rollouts.

## 7) Execution Layer

### 35) `execution/oms.py`
- `Why this exists`: order idempotency and lifecycle state-transition authority.
- `Consumes`: order upserts and transition requests.
- `Produces`: consistent order states and audit trail.
- `Breaks when`: transition matrix does not match real broker lifecycle events.
- `Alternatives`: event-sourced OMS, broker-native state-only mirror (less internal control).

### 36) `execution/zerodha_gateway.py`
- `Why this exists`: broker routing layer with dry/live branching.
- `Consumes`: `orders.allowed`, broker auth/rate config.
- `Produces`: broker order placement + `fills` events + `live_orders` status.
- `Breaks when`: rate limit, token expiry, or error handling suppresses true status.
- `Alternatives`: broker abstraction service, FIX gateway if supported.

### 37) `execution/zerodha_poller.py`
- `Why this exists`: reconciliation between broker-reported state and internal state.
- `Consumes`: broker orderbook/tradebook responses.
- `Produces`: status corrections and fill completion events.
- `Breaks when`: polling lag causes stale exposure perception.
- `Alternatives`: webhook-based broker updates, event bridge if broker supports push.

### 38) `execution/paper_gateway_matcher.py`
- `Why this exists`: realistic paper execution path integrated with matcher engine.
- `Consumes`: approved orders and market proxy prices.
- `Produces`: simulated fills reflecting price-time behavior.
- `Breaks when`: mismatch with live microstructure causes false confidence.
- `Alternatives`: simple immediate-fill paper gateway, full market simulator.

### 39) `execution/cpp/matcher/src/matcher.cpp`
- `Why this exists`: low-latency matching core for realistic queue behavior.
- `Consumes`: order stream + book state assumptions.
- `Produces`: deterministic simulated fill outcomes.
- `Breaks when`: edge-case matching rules diverge from intended market model.
- `Alternatives`: Go/Rust matcher, exchange simulator, coarser probabilistic slippage model.

## 8) Reliability and Research

### 40) `monitoring/daily_recon.py`
- `Why this exists`: end-of-day integrity check between expected and actual records.
- `Consumes`: internal bars/orders/fills and broker-reported data snapshots.
- `Produces`: discrepancy reports for operator action.
- `Breaks when`: reconciliation tolerances/time alignment are not calibrated.
- `Alternatives`: real-time reconciler only, managed observability with anomaly detection.

Companion research file to review immediately after these 40:
- `backtest/scorer.py` (ranking and promotion logic for next-day strategy selection)

## Study Method for This File

For each file above, write 6 lines in your own words:
1. One-line purpose.
2. Exact input source(s).
3. Exact output target(s).
4. One failure mode.
5. One monitoring signal to detect that failure.
6. One viable alternative and why/when it is better.

If you can do this without looking at the code, you are interview-ready for that file.
