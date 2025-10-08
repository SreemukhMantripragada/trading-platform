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

## Infrastructure
- Prometheus configuration lives in `infra/prometheus/prometheus.yml`.
- Grafana dashboards and provisioning under `infra/grafana/`.
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
