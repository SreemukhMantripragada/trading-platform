# Dry-Mode Runbook (Step-by-Step, Non-Coder Friendly)

This runbook is the safest way to run and observe the platform without placing real broker orders.

Use this with:
- `docs/study_cheatsheet.md` (rapid pre-flight checklist)
- `docs/study_playbook.md` (single-screen command order + simple design reasons)
- `docs/study_plan_21d.md`
- `docs/repo_file_catalog.md`
- `docs/architecture_tech_choices.md`

If you want one consolidated flow map first, read `docs/study_playbook.md` Section 1 before running commands here.

## Safety Rules

- Keep `DRY_RUN=1` unless you explicitly intend live trading.
- Do not run `zerodha-live*` commands while learning.
- Run only the minimum pipeline first, then add components.

## 0) Prerequisites

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Infra boot:

```bash
make docker-config-list
make up
make doctor
```

Expected:
- Docker services up: Kafka, Postgres, Prometheus, Grafana.
- No critical infra errors in `make doctor` output.

## 1) Verify Infrastructure Is Ready

```bash
make ps
make logs
```

Check these container names:
- `kafka`
- `postgres`
- `prometheus`
- `grafana`
- `kafka-ui`

## 2) Start a Minimal Data Path (No Strategies Yet)

Open 3 terminals.

Terminal A (ingestion)
```bash
source .venv/bin/activate
python ingestion/smoke_producer.py
```

Terminal B (1s bars)
```bash
source .venv/bin/activate
python compute/bar_builder_1s.py
```

Terminal C (multi-timeframe)
```bash
source .venv/bin/activate
python compute/bar_aggregator_1m_to_multi.py
```

What this proves:
- Tick-like messages enter system.
- 1-second bars are produced and persisted.
- Multi-timeframe bars are generated.

## 3) Validate Data in Postgres

```bash
docker exec -it postgres psql -U trader -d trading -c "SELECT COUNT(*) FROM bars_1s;"
docker exec -it postgres psql -U trader -d trading -c "SELECT COUNT(*) FROM bars_1m;"
docker exec -it postgres psql -U trader -d trading -c "SELECT symbol, ts, o, h, l, c, vol FROM bars_1s ORDER BY ts DESC LIMIT 5;"
```

If counts stay zero:
- Confirm Terminal A is producing.
- Confirm Kafka broker env points to `localhost:9092`.
- Confirm no JSON/DB errors in Terminal B/C logs.

## 4) Add Strategy + Risk + Execution (Dry)

Open additional terminals.

Terminal D (strategy)
```bash
source .venv/bin/activate
make strat-1m
```

Terminal E (risk sizing)
```bash
source .venv/bin/activate
make risk-v2
```

Terminal F (budget guard)
```bash
source .venv/bin/activate
make budget-guard
```

Terminal G (dry gateway)
```bash
source .venv/bin/activate
make gateway-dry
```

Terminal H (dry poller)
```bash
source .venv/bin/activate
make poller-dry
```

What this proves:
- Strategies emit `orders`.
- Risk layers transform to `orders.sized` and `orders.allowed`.
- Dry gateway emits `fills` without live broker side effects.

## 5) Validate Orders and Fills

```bash
docker exec -it postgres psql -U trader -d trading -c "SELECT status, COUNT(*) FROM orders GROUP BY status ORDER BY status;"
docker exec -it postgres psql -U trader -d trading -c "SELECT COUNT(*) FROM live_orders;"
docker exec -it postgres psql -U trader -d trading -c "SELECT COUNT(*) FROM fills;"
docker exec -it postgres psql -U trader -d trading -c "SELECT client_order_id, symbol, side, qty, price, ts FROM fills ORDER BY ts DESC LIMIT 10;"
```

## 6) Observe Metrics

First list the canonical endpoint map:

```bash
make metrics-list
```

Default local URLs (from `configs/docker_stack.json` -> `infra/.env.docker`):
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000`
- Kafka UI: `http://localhost:8080`

Useful metrics to query in Prometheus:
- `bars_1s_ticks_total`
- `bars_1s_published_total`
- `bar_agg_input_total`
- `risk_orders_approved_total`
- `risk_orders_rejected_total`
- `orders_allowed_total`
- `orders_rejected_total`

## 7) Run via Supervisor (Coordinated)

```bash
source .venv/bin/activate
python orchestrator/process_supervisor.py --config configs/process_supervisor.yaml
```

This gives:
- One command to run the configured service stack.
- Unified lifecycle with graceful shutdown and per-service logs.

## 8) Run Go Implementations (Swappable Path)

Build binaries in Docker (no host Go required):

```bash
make go-build
```

Swap specific services:

```bash
INGEST_CMD=./go/bin/ws_bridge make ingest-go
BARS1S_CMD=./go/bin/bar_builder_1s make bars1s-go
BARAGG_CMD=./go/bin/bar_aggregator_multi make baragg-go
```

Validation rule for Go swap:
- Contracts must remain identical even if implementation changes.
- Check same topic names and equivalent table writes.

## 9) End-of-Day/Batch Validation Path

```bash
source .venv/bin/activate
python orchestrator/eod_pipeline.py
```

Then inspect:
- Recon outputs (drift findings)
- Backtest outputs
- Promotion artifacts (`configs/next_day.yaml`)

## 10) Troubleshooting Matrix

### Symptom: no bars in DB
- Check ingestion producer is active.
- Check `compute/bar_builder_1s.py` running without decode errors.
- Check Kafka topics exist and broker reachable.

### Symptom: orders exist but no fills
- Check risk services are running (`risk-v2`, `budget-guard`).
- Check gateway/poller are running in dry mode.
- Check order status in `orders` and `live_orders` tables.

### Symptom: frequent rejects
- Inspect `extra.risk.reason` in `orders`.
- Verify `configs/risk_budget.yaml` bucket splits and caps.
- Check expected leverage and per-trade limits.

### Symptom: Go binary starts but no output
- Confirm env vars match Python defaults (`KAFKA_BROKER`, topics, `POSTGRES_*`).
- Confirm build succeeded and binary paths are correct.
- Confirm metrics port is unique and service has no startup fatal.

## 11) Safe Shutdown

If running supervisor:
- `Ctrl+C` in supervisor terminal.

If running terminals individually:
- Stop in reverse order: gateway/poller -> risk -> strategy -> compute -> ingestion.

Then stop infra:

```bash
make down
```

## 12) What to Record After Every Session

- Which commands ran.
- Which topics moved.
- Which tables changed.
- Which metrics moved.
- One thing learned about design choice and one alternative.
