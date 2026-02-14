#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple


PROM_DS = {"type": "prometheus", "uid": "prometheus"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate the canonical Grafana observability dashboard.")
    parser.add_argument("--registry", default="configs/metrics_endpoints.json", help="Path to metrics registry JSON")
    parser.add_argument(
        "--out",
        default="infra/grafana/dashboards/observability/platform_control_plane.json",
        help="Output dashboard JSON path",
    )
    return parser.parse_args()


def load_services(path: Path) -> List[Dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    services = payload.get("services") or []
    rows: List[Dict[str, Any]] = []
    for item in services:
        if not isinstance(item, dict):
            continue
        name = str(item.get("service", "")).strip()
        if not name:
            continue
        rows.append(
            {
                "service": name,
                "component": str(item.get("component", "unknown")).strip().lower() or "unknown",
                "port": int(item.get("port", 0)),
                "entrypoint": str(item.get("entrypoint", "unknown")),
                "profiles": ", ".join(item.get("profiles") or []),
            }
        )
    return sorted(rows, key=lambda r: (r["component"], r["port"], r["service"]))


def order_components(components: Sequence[str]) -> List[str]:
    preferred = ["ingestion", "compute", "strategy", "risk", "execution", "monitoring"]
    out = [name for name in preferred if name in components]
    out.extend(sorted(name for name in components if name not in preferred))
    return out


def text_markdown(services: List[Dict[str, Any]]) -> str:
    by_component: Dict[str, List[Dict[str, Any]]] = {}
    for svc in services:
        by_component.setdefault(svc["component"], []).append(svc)

    lines = [
        "### Dashboard Guide",
        "",
        "- This dashboard is regenerated from `configs/metrics_endpoints.json`.",
        "- It is content-first: throughput, drops, backlog/state, and focused ingestion/compute latency.",
        "- Panel placement is auto-generated from panel type (no hand-tuned coordinates).",
        "- Endpoints tagged `manual` in Profiles are expected to be down unless started explicitly.",
        "",
        "### Endpoint Ownership Snapshot",
        "",
    ]
    for component in order_components(list(by_component.keys())):
        lines.append(f"**{component.title()}**")
        for svc in sorted(by_component[component], key=lambda r: (r["port"], r["service"])):
            lines.append(f"- `{svc['service']}` : `{svc['port']}` -> `{svc['entrypoint']}`")
        lines.append("")
    return "\n".join(lines)


def prom_selector(extra_selector: str = "") -> str:
    base = 'service=~"$service"'
    if extra_selector:
        return f"{base},{extra_selector}"
    return base


def counter_rate_expr(metric: str, extra_selector: str = "", window: str = "$__rate_interval") -> str:
    return f"sum(rate({metric}{{{prom_selector(extra_selector)}}}[{window}]))"


def gauge_sum_expr(metric: str, extra_selector: str = "") -> str:
    return f"sum({metric}{{{prom_selector(extra_selector)}}})"


def gauge_max_expr(metric: str, extra_selector: str = "") -> str:
    return f"max({metric}{{{prom_selector(extra_selector)}}})"


def hist_avg_expr(metric: str, extra_selector: str = "", window: str = "$__rate_interval") -> str:
    sel = prom_selector(extra_selector)
    return (
        f"sum(rate({metric}_sum{{{sel}}}[{window}])) "
        f"/ clamp_min(sum(rate({metric}_count{{{sel}}}[{window}])), 1e-9)"
    )


def hist_p95_expr(metric: str, extra_selector: str = "") -> str:
    sel = prom_selector(extra_selector)
    return f"histogram_quantile(0.95, sum(rate({metric}_bucket{{{sel}}}[5m])) by (le))"


def target_list(items: Sequence[Tuple[str, str]]) -> List[Dict[str, Any]]:
    targets: List[Dict[str, Any]] = []
    for idx, (legend, expr) in enumerate(items):
        ref_id = chr(ord("A") + idx) if idx < 26 else f"A{idx}"
        targets.append({"refId": ref_id, "expr": expr, "legendFormat": legend})
    return targets


def stat_panel(
    title: str,
    expr: str,
    *,
    unit: str = "none",
    min_value: float | None = None,
    max_value: float | None = None,
    thresholds: List[Dict[str, Any]] | None = None,
    mappings: List[Dict[str, Any]] | None = None,
) -> Dict[str, Any]:
    defaults: Dict[str, Any] = {"unit": unit}
    if min_value is not None:
        defaults["min"] = min_value
    if max_value is not None:
        defaults["max"] = max_value
    if thresholds is not None:
        defaults["thresholds"] = {"mode": "absolute", "steps": thresholds}
    if mappings is not None:
        defaults["mappings"] = mappings
    return {
        "type": "stat",
        "title": title,
        "datasource": PROM_DS,
        "targets": [{"refId": "A", "expr": expr}],
        "fieldConfig": {"defaults": defaults, "overrides": []},
        "options": {
            "colorMode": "background",
            "graphMode": "none",
            "justifyMode": "center",
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
        },
    }


def timeseries_panel(title: str, items: Sequence[Tuple[str, str]], *, unit: str = "none") -> Dict[str, Any]:
    return {
        "type": "timeseries",
        "title": title,
        "datasource": PROM_DS,
        "targets": target_list(items),
        "fieldConfig": {"defaults": {"unit": unit}, "overrides": []},
        "options": {"legend": {"displayMode": "table", "placement": "bottom", "showLegend": True}},
    }


def text_panel(title: str, content: str) -> Dict[str, Any]:
    return {
        "type": "text",
        "title": title,
        "options": {"content": content, "mode": "markdown"},
    }


def table_panel(title: str) -> Dict[str, Any]:
    return {
        "type": "table",
        "title": title,
        "datasource": PROM_DS,
        "targets": [
            {
                "refId": "A",
                "expr": 'up{job="apps",service=~"$service"}',
                "instant": True,
                "format": "table",
            }
        ],
        "options": {"showHeader": True},
    }


def service_up_panel(service: str, port: int) -> Dict[str, Any]:
    return stat_panel(
        f"{service} :{port}",
        f'up{{job="apps",service="{service}"}}',
        min_value=0,
        max_value=1,
        thresholds=[
            {"color": "red", "value": None},
            {"color": "green", "value": 1},
        ],
        mappings=[
            {
                "type": "value",
                "options": {
                    "0": {"text": "DOWN", "color": "red"},
                    "1": {"text": "UP", "color": "green"},
                },
            }
        ],
    )


def global_rate_targets() -> List[Tuple[str, str]]:
    return [
        ("Python ingest ticks/s", counter_rate_expr("zerodha_ticks_received_total")),
        ("Go ingest ticks/s", counter_rate_expr("ingest_ticks_total")),
        ("Python bars1s/s", counter_rate_expr("bars_1s_published_total")),
        ("Go bars1s/s", counter_rate_expr("bars1s_published_total")),
        ("Pair signals/s", counter_rate_expr("pairwatch_signals_emitted_total")),
        ("Orders emitted/s", counter_rate_expr("pairs_orders_emitted_total")),
        ("Risk approved/s", counter_rate_expr("risk_orders_approved_total")),
        ("Fills/s", counter_rate_expr("exec_orders_filled_total")),
    ]


def global_state_targets() -> List[Tuple[str, str]]:
    return [
        ("Python queue depth", gauge_max_expr("zerodha_queue_depth")),
        ("Go queue depth", gauge_max_expr("ingest_queue_depth")),
        ("Python open 1s bars", gauge_max_expr("bars_1s_open_symbols")),
        ("Go open 1s bars", gauge_max_expr("bars1s_open_symbols")),
        ("Open pair positions", gauge_max_expr("pairs_executor_open_pairs")),
    ]


def component_rate_targets(component: str) -> List[Tuple[str, str]]:
    mapping: Dict[str, List[Tuple[str, str]]] = {
        "ingestion": [
            ("Python ticks/s", counter_rate_expr("zerodha_ticks_received_total")),
            ("Go ticks/s", counter_rate_expr("ingest_ticks_total")),
            ("Python drops/s", counter_rate_expr("zerodha_ticks_dropped_total")),
            ("Go drops/s", counter_rate_expr("ingest_ticks_dropped_total")),
        ],
        "compute": [
            ("Python bars1s ticks/s", counter_rate_expr("bars_1s_ticks_total")),
            ("Go bars1s ticks/s", counter_rate_expr("bars1s_ticks_total")),
            ("Python bars1s publish/s", counter_rate_expr("bars_1s_published_total")),
            ("Go bars1s publish/s", counter_rate_expr("bars1s_published_total")),
            ("Bars1m publish/s", counter_rate_expr("bars_1m_published_total")),
            ("Python agg 3m/s", counter_rate_expr("bar_agg_output_3m_total")),
            ("Go agg 3m/s", counter_rate_expr("baragg_output_total", 'tf="3m"')),
            ("Pair signals/s", counter_rate_expr("pairwatch_signals_emitted_total")),
        ],
        "strategy": [
            ("Pair signals/s", counter_rate_expr("pairwatch_signals_emitted_total")),
            ("Pair bars ingested/s", counter_rate_expr("pairwatch_bars_ingested_total")),
        ],
        "risk": [
            ("Risk approved/s", counter_rate_expr("risk_orders_approved_total")),
            ("Risk rejected/s", counter_rate_expr("risk_orders_rejected_total")),
        ],
        "execution": [
            ("Signals seen/s", counter_rate_expr("pairs_signals_total")),
            ("Orders emitted/s", counter_rate_expr("pairs_orders_emitted_total")),
            ("Rejects/s", counter_rate_expr("pairs_rejects_total")),
            ("Fills/s", counter_rate_expr("exec_orders_filled_total")),
        ],
        "monitoring": [
            ("Pair entries/s", counter_rate_expr("pair_entries_total")),
            ("Pair exits/s", counter_rate_expr("pair_exits_total")),
            ("Recon unknown/s", counter_rate_expr("recon_unknown_broker_orders_total")),
        ],
    }
    return mapping.get(component, [])


def component_state_targets(component: str) -> List[Tuple[str, str]]:
    mapping: Dict[str, List[Tuple[str, str]]] = {
        "ingestion": [
            ("Python queue depth", gauge_max_expr("zerodha_queue_depth")),
            ("Go queue depth", gauge_max_expr("ingest_queue_depth")),
        ],
        "compute": [
            ("Python open 1s bars", gauge_max_expr("bars_1s_open_symbols")),
            ("Go open 1s bars", gauge_max_expr("bars1s_open_symbols")),
            ("Open 1m windows", gauge_max_expr("bars_1m_active_windows")),
            ("Python agg active 3m", gauge_sum_expr("bar_agg_active_windows", 'tf="3"')),
            ("Go agg active 3m", gauge_sum_expr("baragg_active_windows", 'tf="3m"')),
            ("Pair engines ready", gauge_sum_expr("pairwatch_pair_ready")),
        ],
        "strategy": [
            ("Pair engines ready", gauge_sum_expr("pairwatch_pair_ready")),
            ("Pair bar staleness max(s)", gauge_max_expr("pairwatch_last_bar_staleness_seconds")),
            ("Pair zscore max", gauge_max_expr("pairwatch_zscore")),
        ],
        "risk": [
            ("Tradable equity", gauge_max_expr("risk_manager_tradable_equity_inr")),
            ("Total equity", gauge_max_expr("risk_manager_equity_inr")),
        ],
        "execution": [
            ("Open pair positions", gauge_max_expr("pairs_executor_open_pairs")),
        ],
        "monitoring": [
            ("OMS open orders", gauge_max_expr("oms_open_orders_now")),
            ("Gateway pending orders", gauge_max_expr("gateway_orders_pending_now")),
            ("Drawdown pct max", gauge_max_expr("risk_drawdown_pct")),
            ("Pair zscore max", gauge_max_expr("pair_zscore")),
        ],
    }
    return mapping.get(component, [])


def component_latency_avg_targets(component: str) -> List[Tuple[str, str]]:
    mapping: Dict[str, List[Tuple[str, str]]] = {
        "ingestion": [
            ("Python ingest", hist_avg_expr("zerodha_tick_latency_seconds")),
            ("Go ingest", hist_avg_expr("ingest_latency_seconds")),
        ],
        "compute": [
            ("Python bars1s publish", hist_avg_expr("bars_1s_publish_latency_seconds")),
            ("Go bars1s publish", hist_avg_expr("bars1s_publish_latency_seconds")),
        ],
    }
    return mapping.get(component, [])


def component_latency_p95_targets(component: str) -> List[Tuple[str, str]]:
    mapping: Dict[str, List[Tuple[str, str]]] = {
        "ingestion": [
            ("Python ingest", hist_p95_expr("zerodha_tick_latency_seconds")),
            ("Go ingest", hist_p95_expr("ingest_latency_seconds")),
        ],
        "compute": [
            ("Python bars1s publish", hist_p95_expr("bars_1s_publish_latency_seconds")),
            ("Go bars1s publish", hist_p95_expr("bars1s_publish_latency_seconds")),
        ],
    }
    return mapping.get(component, [])


def build_dashboard(services: List[Dict[str, Any]]) -> Dict[str, Any]:
    panels: List[Dict[str, Any]] = []
    panel_id = 1

    def add(panel: Dict[str, Any]) -> None:
        nonlocal panel_id
        panel["id"] = panel_id
        panel_id += 1
        panels.append(panel)

    by_component: Dict[str, List[Dict[str, Any]]] = {}
    for svc in services:
        by_component.setdefault(svc["component"], []).append(svc)
    total_services = len(services)

    add(
        stat_panel(
            "Configured Targets",
            'count(up{job="apps",service=~"$service"})',
            min_value=0,
            max_value=float(total_services or 1),
            thresholds=[
                {"color": "red", "value": None},
                {"color": "yellow", "value": max(total_services - 2, 1)},
                {"color": "green", "value": max(total_services, 1)},
            ],
        )
    )
    add(
        stat_panel(
            "Targets Down",
            'sum(1 - up{job="apps",service=~"$service"})',
            min_value=0,
            max_value=float(total_services or 1),
            thresholds=[
                {"color": "green", "value": None},
                {"color": "red", "value": 1},
            ],
        )
    )

    add(timeseries_panel("Global Flow Throughput (events/s)", global_rate_targets(), unit="ops"))
    add(timeseries_panel("Global Backpressure and State", global_state_targets(), unit="none"))
    add(text_panel("Service and Port Ownership", text_markdown(services)))
    add(table_panel("Live Service Inventory (service/component/instance/entrypoint)"))

    for component in order_components(list(by_component.keys())):
        comp_services = sorted(by_component[component], key=lambda r: (r["port"], r["service"]))
        comp_title = component.title()
        comp_selector = f'component="{component}",service=~"$service"'
        add(
            stat_panel(
                f"{comp_title} Targets",
                f'sum(up{{job="apps",{comp_selector}}})',
                min_value=0,
                max_value=float(len(comp_services)),
                thresholds=[
                    {"color": "red", "value": None},
                    {"color": "yellow", "value": max(len(comp_services) - 1, 1)},
                    {"color": "green", "value": max(len(comp_services), 1)},
                ],
            )
        )
        add(
            stat_panel(
                f"{comp_title} Down",
                f'sum(1 - up{{job="apps",{comp_selector}}})',
                min_value=0,
                max_value=float(len(comp_services)),
                thresholds=[
                    {"color": "green", "value": None},
                    {"color": "red", "value": 1},
                ],
            )
        )

        rates = component_rate_targets(component)
        if rates:
            add(timeseries_panel(f"{comp_title} Throughput and Drops (events/s)", rates, unit="ops"))

        state = component_state_targets(component)
        if state:
            add(timeseries_panel(f"{comp_title} State and Backlog", state, unit="none"))

        latency_avg = component_latency_avg_targets(component)
        if latency_avg:
            add(timeseries_panel(f"{comp_title} Latency (avg s)", latency_avg, unit="s"))

        latency_p95 = component_latency_p95_targets(component)
        if latency_p95:
            add(timeseries_panel(f"{comp_title} Latency (p95 s)", latency_p95, unit="s"))

        for svc in comp_services:
            add(service_up_panel(svc["service"], svc["port"]))

    return {
        "id": None,
        "uid": "platform-obs",
        "title": "Platform Observability - Sectioned Control Plane",
        "tags": ["observability", "control-plane", "prometheus"],
        "timezone": "browser",
        "schemaVersion": 38,
        "version": 1,
        "refresh": "10s",
        "editable": True,
        "time": {"from": "now-6h", "to": "now"},
        "templating": {
            "list": [
                {
                    "name": "service",
                    "label": "Service",
                    "type": "query",
                    "datasource": PROM_DS,
                    "query": 'label_values(up{job="apps"}, service)',
                    "refresh": 1,
                    "multi": True,
                    "includeAll": True,
                    "allValue": ".*",
                    "current": {"selected": True, "text": "All", "value": "$__all"},
                }
            ]
        },
        "panels": panels,
    }


def panel_size(panel: Dict[str, Any]) -> Tuple[int, int]:
    ptype = panel.get("type")
    title = str(panel.get("title", ""))
    if ptype == "timeseries":
        return (12, 6)
    if ptype in {"text", "table"}:
        return (12, 9)
    if ptype == "stat":
        if " :" in title:
            # Service UP/DOWN tiles.
            return (4, 3)
        return (6, 3)
    return (12, 5)


def apply_auto_grid(panels: List[Dict[str, Any]]) -> None:
    x = 0
    y = 0
    row_h = 0
    for panel in panels:
        w, h = panel_size(panel)
        if x + w > 24:
            y += row_h
            x = 0
            row_h = 0
        panel["gridPos"] = {"x": x, "y": y, "w": w, "h": h}
        x += w
        row_h = max(row_h, h)


def main() -> int:
    args = parse_args()
    services = load_services(Path(args.registry))
    dashboard = build_dashboard(services)
    apply_auto_grid(dashboard["panels"])
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(dashboard, indent=2) + "\n", encoding="utf-8")
    print(f"[grafana-dashboard] wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
