# Metrics Endpoint Design

This repo now uses a single source of truth for metrics endpoint ownership.

## Source of Truth

- Registry file: `configs/metrics_endpoints.json`
- Reader utility: `tools/metrics_catalog.py`
- Dashboard generator: `tools/generate_grafana_observability.py`
- Prometheus generated targets: `infra/prometheus/targets_apps.json`
- Prometheus scrape config: `infra/prometheus/prometheus.yml` (file_sd from generated targets)
- Grafana control-plane dashboard: `infra/grafana/dashboards/observability/platform_control_plane.json`

## Why This Exists

Before this change, ports were duplicated across:
- orchestrator scripts
- Prometheus static targets
- Makefile command overrides
- service defaults

That made endpoint ownership unclear and caused collisions/drift.

Now the flow is:
1. Edit `configs/metrics_endpoints.json`.
2. Run `make metrics-sync`.
3. Restart stack (`make up` or `docker compose restart prometheus grafana`).

## Operations Commands

- Show endpoint map: `make metrics-list`
- Validate + generate Prom targets + Grafana control-plane dashboard: `make metrics-sync`
- Stack boot now auto-generates targets: `make up`

## Port Ownership Rules

- `8000-8099`: monitoring/exporter and gateway-adjacent endpoints
- `8100-8199`: market-data and hot-path services
- No duplicate ports allowed in registry validation.
- Service names in orchestrators should match `service` or one of `aliases` in the registry.

## How Orchestrators Use It

- `orchestrator/pairs_paper_entry.py` and `orchestrator/pairs_live_entry.py` resolve port assignments via `orchestrator/metrics_ports.py`.
- If a service has an `env_key` override (for example pair watch), that env var is set automatically.

## Prometheus Labels

Generated targets include labels:
- `service`
- `component`
- `language`
- `entrypoint`

This makes Grafana filtering and debugging much easier.

## Change Checklist

When adding a new metric endpoint:
1. Add service entry to `configs/metrics_endpoints.json`.
2. Give a unique port and correct component metadata.
3. Run `make metrics-sync`.
4. Update launcher/make target to use the same port.
5. Verify target appears in Prometheus `Status -> Targets`.
