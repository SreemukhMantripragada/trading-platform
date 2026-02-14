#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple


def load_registry(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def iter_service_alias_rows(registry: Dict[str, Any]) -> List[Tuple[str, Dict[str, Any]]]:
    rows: List[Tuple[str, Dict[str, Any]]] = []
    for service in registry.get("services", []):
        name = service.get("service")
        if isinstance(name, str) and name:
            rows.append((name, service))
        for alias in service.get("aliases") or []:
            if isinstance(alias, str) and alias:
                rows.append((alias, service))
    return rows


def validate_registry(registry: Dict[str, Any]) -> List[str]:
    errors: List[str] = []
    seen_service: Dict[str, str] = {}
    seen_port: Dict[int, str] = {}

    rows = iter_service_alias_rows(registry)
    for name, service in rows:
        if name in seen_service:
            errors.append(f"duplicate service/alias name: {name} ({seen_service[name]} and {service.get('service')})")
        else:
            seen_service[name] = str(service.get("service"))

    for service in registry.get("services", []):
        port = service.get("port")
        svc = str(service.get("service"))
        if not isinstance(port, int) or port <= 0:
            errors.append(f"invalid port for service {svc}: {port!r}")
            continue
        if port in seen_port:
            errors.append(f"duplicate metrics port {port}: {seen_port[port]} and {svc}")
        else:
            seen_port[port] = svc

    return errors


def render_prometheus_targets(registry: Dict[str, Any], host: str) -> List[Dict[str, Any]]:
    targets: List[Dict[str, Any]] = []
    for service in registry.get("services", []):
        svc = str(service.get("service"))
        port = int(service["port"])
        labels = {
            "service": svc,
            "component": str(service.get("component", "unknown")),
            "language": str(service.get("language", "unknown")),
            "entrypoint": str(service.get("entrypoint", "unknown")),
        }
        targets.append(
            {
                "targets": [f"{host}:{port}"],
                "labels": labels,
            }
        )
    return targets


def print_table(registry: Dict[str, Any]) -> None:
    print("Service                Port  Component    Language  Entrypoint")
    print("---------------------  ----  -----------  --------  -------------------------------")
    for service in sorted(registry.get("services", []), key=lambda r: (int(r.get("port", 0)), str(r.get("service", "")))):
        name = str(service.get("service", ""))
        port = int(service.get("port", 0))
        component = str(service.get("component", ""))
        language = str(service.get("language", ""))
        entrypoint = str(service.get("entrypoint", ""))
        print(f"{name:21}  {port:4d}  {component:11}  {language:8}  {entrypoint}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Inspect and sync metrics endpoint registry.")
    p.add_argument("--registry", default="configs/metrics_endpoints.json", help="Path to metrics registry JSON")
    p.add_argument("--host", default=None, help="Host/IP for Prometheus targets (defaults to registry host)")
    p.add_argument("--check", action="store_true", help="Validate unique names and unique ports")
    p.add_argument("--write-prom-targets", default=None, help="Write Prometheus file_sd JSON to this path")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    registry_path = Path(args.registry)
    registry = load_registry(registry_path)

    if args.check:
        errors = validate_registry(registry)
        if errors:
            for err in errors:
                print(f"[metrics-catalog] ERROR: {err}", file=sys.stderr)
            return 1

    print_table(registry)

    if args.write_prom_targets:
        host = args.host or str(registry.get("host") or "host.docker.internal")
        out_path = Path(args.write_prom_targets)
        rendered = render_prometheus_targets(registry, host)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(rendered, indent=2) + "\n", encoding="utf-8")
        print(f"\n[metrics-catalog] wrote Prometheus targets -> {out_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
