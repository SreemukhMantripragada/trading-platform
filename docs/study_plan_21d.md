# 21-Day Study Plan (Zero Coding Background, Interview-Ready)

This plan is designed for someone who starts with no programming background and needs deep, defensible understanding of this repository.

Use this plan with:
- `docs/study_cheatsheet.md` (quick command + explanation revision before each session)
- `docs/study_playbook.md` (single place for full flow map + run commands + simple design reasons)
- `docs/repo_file_catalog.md` (every tracked file, purpose, and alternatives)
- `docs/runbook_dry_mode.md` (hands-on step-by-step runbook)
- `docs/architecture_tech_choices.md` (deep tradeoff guide across all tech choices)
- `docs/critical_file_deep_notes.md` (top 40 make-or-break files with strict study template)

## How to Use This Plan

Each day has exactly 5 blocks:
1. `Vocabulary (10 min)` - Learn terms before touching code.
2. `Read (25 min)` - Read specific files with clear goals.
3. `Run (25 min)` - Execute one or two commands only.
4. `Observe (20 min)` - Confirm what happened in Kafka/Postgres/metrics/logs.
5. `Explain (10 min)` - Speak or write a plain-English explanation.

Do not skip the `Explain` block. Interview clarity comes from explanation, not reading volume.

## Ground Rules (Non-Negotiable)

- Start only in dry mode/paper mode. Never begin with live broker mode.
- Follow one data item end-to-end before learning advanced features.
- Use `docs/repo_file_catalog.md` as a checklist to ensure nothing is skipped.
- Every session ends with: "What does this component consume, produce, store, and monitor?"

## Prerequisites

- Python + venv + requirements
- Docker Desktop
- Optional later: Go toolchain OR containerized Go build (`make go-build`)

Setup once:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
make up
make doctor
```

Before Day 1, do one fast orientation pass:
- Read `docs/study_playbook.md` completely.
- Run the command sequence in `docs/study_playbook.md` Section 2 (A to D).
- Keep one note page with: command, observed output, and one-line "why this exists".

## Master Mental Model (Memorize)

Think of this platform as a factory with conveyor belts:
- `Ingestion` receives market ticks and places them on Kafka topic `ticks`.
- `Compute` converts ticks into bars (`bars.1s`, `bars.1m`, `bars.3m/5m/15m...`).
- `Strategy` reads bars and creates order intents (`orders`).
- `Risk` sizes/filters orders (`orders.sized`, `orders.allowed`).
- `Execution` sends orders to paper/live gateways and emits `fills`.
- `Monitoring` measures health, drift, and reconciliation.
- `Backtest/Scoring` improves future strategy selection.

## Week 1: Build Core Map (Days 1-7)

### Day 1 - What Problem This Repo Solves
- Vocabulary: market tick, bar, strategy, execution, fill, reconciliation.
- Read:
  - `README.md`
  - `docs/ingestion.md`
  - `docs/compute.md`
- Run:
  - `make up`
  - `make ps`
- Observe:
  - Confirm containers `kafka`, `postgres`, `prometheus`, `grafana` are healthy.
- Explain:
  - "This system transforms market data into decisions and tracks every decision."

### Day 2 - Folder Geography
- Vocabulary: module, service, config, schema, migration.
- Read:
  - `docs/repo_file_catalog.md` (first 80 entries)
  - `Makefile`
- Run:
  - `rg --files | wc -l`
- Observe:
  - Confirm file count and major domains (`ingestion`, `compute`, `strategy`, `risk`, `execution`).
- Explain:
  - "Each folder maps to a stage in the trade lifecycle."

### Day 3 - Data Contracts
- Vocabulary: schema, contract, producer, consumer, compatibility.
- Read:
  - `schemas/order.schema.json`
  - `schemas/fill.schema.json`
  - `schemas/pair_signal.schema.json`
  - `libs/schema.py`
- Run:
  - `rg "schema" -n libs ingestion compute strategy execution`
- Observe:
  - Identify where schema violations would break downstream services.
- Explain:
  - "Schemas prevent silent payload drift between services."

### Day 4 - Ingestion Deep Dive
- Vocabulary: websocket, callback, queue depth, backpressure, reconnect.
- Read:
  - `ingestion/zerodha_ws.py`
  - `ingestion/smoke_producer.py`
  - `configs/tokens.csv`
- Run:
  - Dry test path with smoke producer (if no broker credentials): `python ingestion/smoke_producer.py`
- Observe:
  - Tick payload shape and queue/latency metrics behavior.
- Explain:
  - "Ingestion decouples broker events from Kafka writes to reduce drop risk."

### Day 5 - 1-Second Bars
- Vocabulary: OHLC, aggregation window, flush grace, upsert.
- Read:
  - `compute/bar_builder_1s.py`
  - `infra/postgres/init/17_bars_1m_golden.sql`
- Run:
  - `python compute/bar_builder_1s.py`
- Observe:
  - New rows in `bars_1s`; topic activity on `bars.1s`.
- Explain:
  - "A short grace window handles slightly late ticks before finalizing bars."

### Day 6 - Multi-Timeframe Aggregation
- Vocabulary: bucketization, timeframe fanout, late data, correction.
- Read:
  - `compute/bar_aggregator_1m_to_multi.py`
  - `docs/compute.md`
- Run:
  - `make agg-multi`
- Observe:
  - Output topics `bars.3m`, `bars.5m`, `bars.15m` and matching tables.
- Explain:
  - "One canonical stream is reused to derive all larger intervals."

### Day 7 - Checkpoint #1 (Explain End-to-End Data)
- Task:
  - Draw data flow from `ticks` to `bars.15m`.
  - Name each store/topic touched.
- Validation:
  - If you cannot explain this in 3 minutes, repeat Days 4-6.

## Week 2: Decision and Control Planes (Days 8-14)

### Day 8 - Strategy Runner and Indicators
- Vocabulary: indicator, signal, ensemble, confidence.
- Read:
  - `strategy/runner_modular.py`
  - `strategy/registry.yaml`
  - `configs/indicators.yaml`
  - `strategy/ensemble.yaml`
- Run:
  - `make strat-1m`
- Observe:
  - Orders emitted to topic/table with `extra.signal` metadata.
- Explain:
  - "Strategies convert bars into intents; they do not directly place broker orders."

### Day 9 - Risk Sizing Layer
- Vocabulary: risk-per-trade, bucket cap, per-symbol max, leverage.
- Read:
  - `risk/manager_v2.py`
  - `configs/risk_budget.yaml`
  - `risk/position_sizer.py`
- Run:
  - `make risk-v2`
- Observe:
  - Transition from `orders` to `orders.sized`; approval/rejection metrics.
- Explain:
  - "Risk manager computes quantity from exposure constraints, not strategy preference."

### Day 10 - Budget Guard Layer
- Vocabulary: throttle, max-per-trade, budget burn, guardrail.
- Read:
  - `risk/order_budget_guard.py`
  - `risk/budget_source.py`
- Run:
  - `make budget-guard`
- Observe:
  - Transition from `orders.sized` to `orders.allowed` and reject reasons.
- Explain:
  - "Second risk gate prevents burst overspend even after sizing."

### Day 11 - OMS and State Machine
- Vocabulary: idempotency, transition, audit trail, terminal state.
- Read:
  - `execution/oms.py`
  - `infra/postgres/init/07_oms_orders.sql`
  - `infra/postgres/init/09_oms_live.sql`
- Run:
  - Review with: `rg "VALID" -n execution/oms.py`
- Observe:
  - Allowed state transitions and terminal safeguards.
- Explain:
  - "OMS protects order lifecycle consistency under retries and duplicates."

### Day 12 - Execution Gateways
- Vocabulary: dry run, live route, token bucket rate limit, poller reconciliation.
- Read:
  - `execution/zerodha_gateway.py`
  - `execution/zerodha_poller.py`
  - `execution/paper_gateway_matcher.py`
- Run:
  - `make gateway-dry`
  - `make poller-dry`
- Observe:
  - `fills` events and `live_orders` updates.
- Explain:
  - "Gateway acknowledges intent; poller closes truth gap with broker state."

### Day 13 - Monitoring + Reconciliation
- Vocabulary: exporter, gauge, histogram, recon, auto-heal.
- Read:
  - `monitoring/gateway_oms_exporter.py`
  - `monitoring/risk_drawdown_exporter.py`
  - `monitoring/daily_recon.py`
  - `monitoring/recon_autoheal.py`
- Run:
  - `make doctor`
- Observe:
  - Prom metrics exposure and mismatch repair flow.
- Explain:
  - "Monitoring proves behavior; recon proves data integrity."

### Day 14 - Checkpoint #2 (Explain Order Lifecycle)
- Task:
  - Draw path: `bars -> orders -> orders.sized -> orders.allowed -> live_orders/fills`.
- Validation:
  - If you cannot name all gates and their purpose, revisit Days 8-12.

## Week 3: Operations, Research, and Go Track (Days 15-21)

### Day 15 - Orchestration and Runtime Control
- Vocabulary: supervisor, stop_on_exit, graceful shutdown, process fanout.
- Read:
  - `orchestrator/process_supervisor.py`
  - `configs/process_supervisor.yaml`
  - `orchestrator/eod_pipeline.py`
- Run:
  - `python orchestrator/process_supervisor.py --config configs/process_supervisor.yaml`
- Observe:
  - Per-service logs and shutdown behavior.
- Explain:
  - "Supervisor standardizes process lifecycle and crash behavior."

### Day 16 - Research and Promotion Pipeline
- Vocabulary: grid search, gating, ranking, promotion.
- Read:
  - `backtest/engine.py`
  - `backtest/scorer.py`
  - `tools/promote_topn.py`
  - `docs/scoring.md`
- Run:
  - `python backtest/peek_results.py` (if data exists)
- Observe:
  - How offline results become next-day configs.
- Explain:
  - "Backtest decides candidates; risk/execution still enforce runtime safety."

### Day 17 - Infra as Code + Data Model
- Vocabulary: migration, bootstrap, provisioning.
- Read:
  - `infra/docker-compose.yml`
  - `infra/postgres/init/*.sql` (all)
  - `infra/prometheus/prometheus.yml`
  - `infra/grafana/provisioning/*`
- Run:
  - `make logs`
- Observe:
  - DB init scripts and observability provisioning are deterministic.
- Explain:
  - "Platform state comes from versioned infra/config, not manual clicks."

### Day 18 - Go Path (High-Throughput Re-implementation)
- Vocabulary: goroutine, channel, interface, module, compile target.
- Read:
  - `go/pkg/shared/config.go`
  - `go/pkg/shared/kafka.go`
  - `go/pkg/shared/pg.go`
  - `go/ingestion/ws_bridge/main.go`
  - `go/compute/bar_builder_1s/main.go`
  - `go/compute/bar_aggregator_multi/main.go`
- Run:
  - `make go-build`
- Observe:
  - Go binaries generated in `go/bin/` and swappable via supervisor env overrides.
- Explain:
  - "Go path targets hot loops while preserving topic/table contracts for safe swap."

### Day 19 - Python vs Go Comparison
- Vocabulary: latency, throughput, GC pauses, developer velocity, operability.
- Read:
  - `docs/architecture_tech_choices.md`
  - `docs/repo_file_catalog.md` (Go and Python entries)
- Run:
  - Compare startup paths:
    - Python default supervisor
    - Go override targets (`make ingest-go`, `make bars1s-go`, `make baragg-go`)
- Observe:
  - What changes (implementation) vs what must not change (contracts).
- Explain:
  - "Interface compatibility enables progressive migration with rollback."

### Day 20 - Interview Drill Day
- Task:
  - Answer 25 architecture questions from memory (prepare from your notes).
  - For every answer include: decision, reason, tradeoff, alternative.
- Validation:
  - If answers are hand-wavy, revisit architecture and tech-choice docs.

### Day 21 - Final Capstone
- Task:
  - Whiteboard full architecture, failure scenarios, and migration plan.
  - Use `docs/repo_file_catalog.md` to verify all files were reviewed.
- Output:
  - Produce a 2-page architecture brief and a 2-page tradeoff brief.

## Mandatory Master Checklist (No File Left Behind)

Use this exact loop after day 21:
1. Open `docs/repo_file_catalog.md`.
2. For each file, answer four questions in notes:
   - What does it consume?
   - What does it produce?
   - What can fail here?
   - What alternative implementation exists?
3. Mark file reviewed only when all four answers exist.

If you cannot answer all four for a file, that file is not complete.

## What "Complete Understanding" Looks Like

You are complete when you can:
- Explain every architecture stage and its fallback.
- Trace one event from tick to fill and to recon.
- Defend Python vs Go vs C++ placement decisions.
- Explain why each config, schema, and migration exists.
- Describe an alternative for each file type and key service choice.

## Common Failure Modes During Study

- Reading too much, running too little.
- Learning code syntax before data contracts.
- Ignoring failure paths and only learning happy path.
- Skipping SQL schema/migrations and then misunderstanding runtime state.
- Comparing Python and Go by opinion instead of topic/table contract behavior.

## Fast Recovery If You Get Stuck

- Return to one flow only: `ticks -> bars.1s -> bars.5m -> orders -> fills`.
- Use dry mode and one symbol subset.
- Inspect logs, one topic, one table, one metric at a time.
- Rebuild confidence via `docs/runbook_dry_mode.md`.
