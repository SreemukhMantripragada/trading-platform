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
| `monitoring/daily_recon_v2.py` | After-close reconciliation of broker bars vs. internal bars. | Postgres `bars_1m_recon`, logs |
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
- Prometheus configuration lives in `infra/prometheus/prometheus.yml`.
- Grafana dashboards and provisioning under `infra/grafana/`.
- `infra/grafana/dashboards/pairs_trading_observability.json` contains the end-to-end latency dashboard wired to the metrics above.
- Kafka exporter (`kafka-exporter`) and Kafka UI are launched via `make up`.
- Alerts: integrate Grafana alerting with Slack/Teams or pushgateway as required.

## Operating Playbook
- Run `make doctor` for a quick health check (Kafka connectivity, Postgres, process liveness).
- Use `monitoring/daily_recon_v2.py --date YYYY-MM-DD` after each session; investigate non-zero discrepancies in `bars_1m_recon`.
- Prometheus targets: ensure exporters are reachable on `127.0.0.1:<PORT>` after launching via Makefile.
- Leverage `monitoring/process_supervisor.py` in tmux/daemon contexts to auto-restart critical services.

## Metrics Cheatsheet
- `risk_orders_approved_total`, `risk_orders_rejected_total`: emitted by `risk/manager_v2.py`.
- `orders_emitted_total`: strategy runner throughput.
- `gateway_rate_limited_total`, `gateway_fills_total`: Zerodha gateway stats from exporters.
- `pairs_spread_zscore`: real-time pair anomaly metric for monitoring thresholds.
- `bar_agg_output_{tf}m_total`, `bar_agg_flush_seconds_bucket`, `bar_agg_bar_latency_{tf}m_seconds_bucket`: multi-timeframe bar aggregator throughput plus flush/publish latency (drive the Grafana “Bars Aggregator (Compact)” dashboard).
