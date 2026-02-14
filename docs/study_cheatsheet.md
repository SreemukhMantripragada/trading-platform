# Study Cheatsheet (Quick Revision)

Use this when you need a fast run + explain cycle before demos/interviews.

## 1) Run Order (Minimal Commands)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
make up
make doctor
```

```bash
PERF_SCENARIO=paper-flow LOADS=500 STAGE_SEC=180 WARMUP_SEC=20 SAMPLE_SEC=5 make perf-test
```

```bash
PERF_SCENARIO=pipeline LOADS=500 STAGE_SEC=180 WARMUP_SEC=20 SAMPLE_SEC=5 make perf-test
```

```bash
ls -1t runs/perf_compare | head -1
```

```bash
make down
```

## 2) What to Open While Running

- Grafana: `http://localhost:3000`
- Prometheus: `http://localhost:9090`
- Kafka UI: `http://localhost:8080`
- Benchmark report: `runs/perf_compare/<latest>/report.md`

## 3) 3-Line Architecture Explain (Use in Interviews)

- Ingestion takes broker/sim ticks and publishes to Kafka with queue/backpressure safety.
- Compute normalizes to 1s bars first, then fans out to higher timeframes for reuse by strategy/risk/execution.
- Strategy emits intent, risk gates quantity and budget, execution matches/routes, and monitoring/recon validates integrity.

## 4) Why Python + Go Together

- Python remains the full baseline for research velocity and wide module coverage.
- Go replaces hot paths (ingestion + aggregation) for better concurrency and throughput.
- Contracts (Kafka topics + Postgres tables + metrics) are kept stable so modules are swappable.

## 5) Current Python vs Go Hot-Path Difference

- Python compute is asyncio loop + periodic flush model.
- Go compute is worker-sharded goroutine model with batched writes.
- Current Go commit point is enqueue-time; Python multi-agg commits after flush.

## 6) Go Tuning Knobs (Perf Runs)

- Ingestion: `GO_PRODUCE_WORKERS`, `GO_PRODUCE_QUEUE`, `GO_MAX_BATCH`, `GO_BATCH_FLUSH_MS`
- 1s builder: `GO_BAR1S_WORKERS`, `GO_BAR1S_QUEUE`, `GO_BAR1S_FLUSH_MS`
- Multi-agg: `GO_BARAGG_WORKERS`, `GO_BARAGG_QUEUE`, `GO_BARAGG_FLUSH_MS`

## 7) Quick “If Broken” Checks

- `make ps` -> confirm `kafka`, `postgres`, `prometheus`, `grafana` are running.
- `make metrics-list` -> confirm expected service/port ownership.
- `make logs` -> check first failing service instead of debugging all at once.

## 8) One-Sentence Value Proposition

- "This repo is a contract-stable trading pipeline where Python gives fast strategy iteration and Go improves high-volume ingestion/aggregation without changing downstream interfaces."
