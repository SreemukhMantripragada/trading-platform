# Monitoring & Ops

## Responsibilities
- Expose metrics, health, and reconciliation signals across the trading stack.
- Provide operational tooling to start/stop processes and surface alerts quickly.
- Automate nightly reconciliations between broker data and internal records.

## Tooling & Services
| Path | Description | Outputs |
| --- | --- | --- |
| `monitoring/doctor.py` | TUI helper to verify infra, tail logs, and trigger restarts. | CLI |
| `monitoring/gateway_oms_exporter.py` | Prometheus exporter for OMS/gateway health and lag metrics. | HTTP `/metrics` |
| `monitoring/oms_lifecycle_exporter.py` | Tracks state transitions of orders across the OMS. | HTTP `/metrics` |
| `monitoring/risk_drawdown_exporter.py` | Publishes drawdown and exposure gauges per bucket. | HTTP `/metrics` |
| `monitoring/pairs_exporter.py` | Exposes pairs spread metrics for Grafana dashboards. | HTTP `/metrics` |
| `monitoring/daily_recon.py` | After-close reconciliation of broker bars vs. internal bars. | Postgres `bars_1m_recon`, logs |
| `monitoring/recon_autoheal.py` | Attempts to backfill/replay bars that fail reconciliation. | Kafka replays, Postgres fixes |
| `monitoring/process_supervisor.py` | Sample supervisor script that watches process heartbeats. | CLI / Prometheus |

### Pairs Trading Automation & Observability

The pairs paper stack now exposes consistent Prometheus metrics for every hop in the workflow and is driven by two scheduled GitHub Actions:

| Workflow | When | Purpose |
| --- | --- | --- |
| `.github/workflows/eod.yml` | 16:40 IST (Mon–Fri) | Runs the end-of-day pipeline (`orchestrator/eod_pipeline.py`) for reconciliation, auto-heal and next-day selection generation. |
| `.github/workflows/pre-market.yml` | 08:30 IST (Mon–Fri) | Validates and promotes the latest selections to `configs/pairs_next_day.yaml` via `orchestrator/pre_market_setup.py`, ensuring the live stack has an up‑to‑date whitelist. |

| Service | Launcher | Default metrics port |
| --- | --- | --- |
| Zerodha websocket ingestor | `ingestion/zerodha_ws.py` | 8111 |
| 1s bar builder | `compute/bar_builder_1s.py` | 8112 |
| 1m bar aggregator | `compute/bar_aggregator_1m.py` | 8118 |
| 1m→multi aggregator | `compute/bar_aggregator_1m_to_multi.py` | 8113 |
| Pair watch producer | `compute/pair_watch_producer.py` | 8114 |
| Pairs executor | `execution/pairs_executor.py` | 8115 |
| Risk manager v2 | `risk/manager_v2.py` | 8116 |
| Paper matcher | `execution/paper_gateway_matcher.py` | 8117 |

Each process exposes throughput counters plus latency histograms (tick ingestion, bar publish, pair decisions, risk approvals, matcher fills) and gauges for backlog/ready-state. The orchestrator (`orchestrator/pairs_paper_entry.py`) injects the port assignments automatically.

## Infrastructure
- Metrics endpoint source-of-truth: `configs/metrics_endpoints.json`.
- Design notes for this setup: `docs/metrics_endpoints.md`.
- Render Prometheus file_sd targets and Grafana control-plane dashboard from the registry: `make metrics-sync`.
- Print a human-readable endpoint map: `make metrics-list`.
- Prometheus configuration lives in `infra/prometheus/prometheus.yml`.
- Prometheus target list is generated in `infra/prometheus/targets_apps.json`.
- Grafana dashboards and provisioning under `infra/grafana/`.
- Canonical dashboard path: `infra/grafana/dashboards/observability/platform_control_plane.json`.
- Canonical dashboard is sectioned into clear panes by component (`Ingestion`, `Compute`, `Strategy`, `Risk`, `Execution`, `Monitoring`); each pane now includes Ops/State charts (throughput, drops, queue/backlog, readiness) plus latency charts for ingestion and 1s bar publish.
- Grafana provider only loads the canonical observability folder (`/var/lib/grafana/dashboards/observability`).
- Legacy dashboards are retained in `infra/grafana/dashboards/` for reference, but are not auto-provisioned.
- Kafka exporter (`kafka-exporter`) and Kafka UI are launched via `make up`.
- Alerts: integrate Grafana alerting with Slack/Teams or pushgateway as required.

## Manual Pipeline Perf Compare (Python vs Go)
- Command: `make perf-test`.
- Default mode (`PERF_SCENARIO=paper-flow`) runs the full paper path concurrently for Python and Go-core stages, then emits a side-by-side report.
- Optional mode (`PERF_SCENARIO=pipeline`) keeps the old ingest+aggregation-only benchmark.
- The target brings up required infra services and leaves `app-supervisor` (live runner) out of the test path.
- Under the hood:
  - Synthetic source: `ingestion/zerodha_ws_sim.py` (Python stack) and `go/bin/ws_bridge` with `SIM_TICKS=true` (Go stack).
  - Go compute path mirrors Python shape: `bar_builder_1s -> bar_aggregator_1m -> bar_aggregator_multi`.
  - Paper-flow mode runs pair-watch, pairs-executor, risk-manager, and paper-matcher concurrently; live-only modules are skipped.
  - Go runtime stages are executed as Docker containers from image `trading-stack-go-builder` on network `trading-stack_core`.
  - Metrics are sampled directly from service `/metrics` endpoints.
  - Prometheus app target host is temporarily switched to `host.docker.internal` during the run so Grafana shows the live benchmark.
  - Targets are restored automatically after completion.
- Output artifacts:
  - `runs/perf_compare/<run_tag>/report.md`
  - `runs/perf_compare/<run_tag>/report.json`
  - per-process logs in the same run folder.
- Runtime knobs:
  - `PERF_SCENARIO` (`paper-flow` default, or `pipeline`)
  - `LOADS` (comma-separated symbol counts, default `50,200,500`)
  - `STAGE_SEC` (default `180`)
  - `WARMUP_SEC` (default `20`)
  - `SAMPLE_SEC` (default `5`)
  - `SIM_BASE_TPS` (baseline ticks/sec per symbol, default `5`)
  - `SIM_HOT_TPS` (hot-symbol burst rate, default `1000`)
  - `SIM_HOT_SYMBOL_PCT` (hot symbol fraction, default `0.002` ~= one hot symbol at 500)
  - `SIM_HOT_ROTATE_SEC`, `SIM_STEP_MS` (burst rotation + scheduling cadence)
  - `GO_PRODUCE_WORKERS`, `GO_PRODUCE_QUEUE`, `GO_MAX_BATCH`, `GO_BATCH_FLUSH_MS` (Go ingestion goroutine tuning)
  - `GO_BAR1S_WORKERS`, `GO_BAR1S_QUEUE`, `GO_BAR1S_FLUSH_MS` (Go 1s builder worker tuning)
  - `GO_BAR1M_WORKERS`, `GO_BAR1M_QUEUE`, `GO_BAR1M_FLUSH_MS` (Go 1m aggregator worker tuning)
  - `GO_BARAGG_WORKERS`, `GO_BARAGG_QUEUE`, `GO_BARAGG_FLUSH_MS` (Go multi-agg worker tuning)
  - `PAIR_TF`, `PAIR_LOOKBACK`, `PAIR_COUNT`, `PAIR_MIN_READY` (paper-flow synthetic pair tuning)

## Operating Playbook
- Run `make doctor` for a quick health check (Kafka connectivity, Postgres, process liveness).
- Use `monitoring/daily_recon.py --date YYYY-MM-DD` after each session; investigate non-zero discrepancies in `bars_1m_recon`.
- Prometheus targets: ensure exporters are reachable on `127.0.0.1:<PORT>` after launching via Makefile.
- Leverage `monitoring/process_supervisor.py` in tmux/daemon contexts to auto-restart critical services.

## Metrics Cheatsheet
- `risk_orders_approved_total`, `risk_orders_rejected_total`: emitted by `risk/manager_v2.py`.
- `orders_emitted_total`: strategy runner throughput.
- `gateway_rate_limited_total`, `gateway_fills_total`: Zerodha gateway stats from exporters.
- `pairs_spread_zscore`: real-time pair anomaly metric for monitoring thresholds.
- `bar_agg_output_{tf}m_total`, `bar_agg_flush_seconds_bucket`, `bar_agg_bar_latency_{tf}m_seconds_bucket`: multi-timeframe bar aggregator throughput plus flush/publish latency (drive the Grafana “Bars Aggregator (Compact)” dashboard).
