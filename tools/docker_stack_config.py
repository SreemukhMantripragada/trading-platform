#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

REQUIRED_KEYS = [
    "ZOOKEEPER_CLIENT_PORT",
    "ZOOKEEPER_ALLOW_ANONYMOUS_LOGIN",
    "KAFKA_OUTSIDE_HOST",
    "KAFKA_OUTSIDE_PORT",
    "KAFKA_INSIDE_PORT",
    "KAFKA_AUTO_CREATE_TOPICS_ENABLE",
    "ALLOW_PLAINTEXT_LISTENER",
    "KAFKA_BROKER_ID",
    "KUI_CLUSTER_NAME",
    "KAFKA_UI_PORT",
    "KAFKA_EXPORTER_PORT",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
    "POSTGRES_DB",
    "POSTGRES_PORT",
    "PROMETHEUS_PORT",
    "GRAFANA_PORT",
    "GF_SECURITY_ADMIN_PASSWORD",
    "APP_METRICS_HOST",
    "APP_SUPERVISOR_CONFIG",
    "LIVE_TRADING",
    "AUTO_SWITCH_TO_LIVE",
    "TOPICS_DRY_RUN",
    "RF",
]

NUMERIC_KEYS = {
    "ZOOKEEPER_CLIENT_PORT",
    "KAFKA_OUTSIDE_PORT",
    "KAFKA_INSIDE_PORT",
    "KAFKA_BROKER_ID",
    "KAFKA_UI_PORT",
    "KAFKA_EXPORTER_PORT",
    "POSTGRES_PORT",
    "PROMETHEUS_PORT",
    "GRAFANA_PORT",
    "LIVE_TRADING",
    "AUTO_SWITCH_TO_LIVE",
    "TOPICS_DRY_RUN",
    "RF",
    "P_TICKS",
    "P_BARS",
    "P_ORDERS",
    "P_MISC",
    "P_DLQ",
    "TICKS_RETENTION_MS",
    "BARS1S_RETENTION_MS",
    "BARS1M_RETENTION_MS",
    "BARS3M_RETENTION_MS",
    "BARS5M_RETENTION_MS",
    "BARS15M_RETENTION_MS",
    "ORDERS_RETENTION_MS",
    "ALLOWED_RETENTION_MS",
    "EXEC_RETENTION_MS",
    "FILLS_RETENTION_MS",
    "CANCELS_RETENTION_MS",
    "DLQ_RETENTION_MS",
    "PAIRS_RETENTION_MS",
    "SEGMENT_BYTES",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate and render docker stack env config.")
    parser.add_argument("--registry", default="configs/docker_stack.json", help="Path to docker stack registry JSON")
    parser.add_argument("--write-env", default=None, help="Write generated env file to this path")
    parser.add_argument("--check", action="store_true", help="Validate config and fail on errors")
    return parser.parse_args()


def load_registry(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def flatten_registry(registry: Dict[str, Any]) -> Tuple[List[Tuple[str, Dict[str, str]]], Dict[str, str]]:
    sections: List[Tuple[str, Dict[str, str]]] = []
    flat: Dict[str, str] = {}
    for section in registry.get("sections", []):
        name = str(section.get("name") or "unnamed")
        raw_vars = section.get("vars") or {}
        vars_map: Dict[str, str] = {}
        for k, v in raw_vars.items():
            vars_map[str(k)] = str(v)
            flat[str(k)] = str(v)
        sections.append((name, vars_map))
    return sections, flat


def validate(registry: Dict[str, Any]) -> List[str]:
    errors: List[str] = []
    sections, flat = flatten_registry(registry)
    if not sections:
        errors.append("no sections defined")

    for key in REQUIRED_KEYS:
        if key not in flat:
            errors.append(f"missing required key: {key}")

    for key, value in flat.items():
        if key in NUMERIC_KEYS:
            try:
                int(value)
            except ValueError:
                errors.append(f"expected integer for {key}, got {value!r}")

    seen: Dict[str, str] = {}
    for section_name, vars_map in sections:
        for key in vars_map:
            if key in seen:
                errors.append(f"duplicate key {key} in sections {seen[key]} and {section_name}")
            else:
                seen[key] = section_name

    return errors


def render_env(sections: List[Tuple[str, Dict[str, str]]]) -> str:
    lines: List[str] = []
    lines.append("# AUTO-GENERATED FILE. Do not edit manually.")
    lines.append("# Source: configs/docker_stack.json")
    lines.append("")
    for name, vars_map in sections:
        lines.append(f"# [{name}]")
        for key, value in vars_map.items():
            lines.append(f"{key}={value}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def print_summary(sections: List[Tuple[str, Dict[str, str]]]) -> None:
    print("Section              Key                          Value")
    print("-------------------  ---------------------------  ------------------------------")
    for section_name, vars_map in sections:
        for key, value in vars_map.items():
            shown = value if len(value) <= 30 else value[:27] + "..."
            print(f"{section_name:19}  {key:27}  {shown}")


def main() -> int:
    args = parse_args()
    registry_path = Path(args.registry)
    registry = load_registry(registry_path)
    sections, _flat = flatten_registry(registry)

    if args.check:
        errors = validate(registry)
        if errors:
            for err in errors:
                print(f"[docker-config] ERROR: {err}", file=sys.stderr)
            return 1

    print_summary(sections)

    if args.write_env:
        out = render_env(sections)
        out_path = Path(args.write_env)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(out, encoding="utf-8")
        print(f"\n[docker-config] wrote env file -> {out_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
