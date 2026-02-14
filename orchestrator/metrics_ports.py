from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Any, Optional

ROOT_DIR = Path(__file__).resolve().parents[1]
REGISTRY_PATH = ROOT_DIR / "configs" / "metrics_endpoints.json"


def _load_registry() -> Dict[str, Any]:
    raw = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
    out: Dict[str, Any] = {
        "host": raw.get("host", "host.docker.internal"),
        "metrics_path": raw.get("metrics_path", "/metrics"),
        "services": {},
    }
    for item in raw.get("services", []):
        service = item.get("service")
        if not service:
            continue
        aliases = list(item.get("aliases") or [])
        names = [service, *aliases]
        for name in names:
            out["services"][name] = item
    return out


_REGISTRY = _load_registry()


def metrics_port_for(service_name: str) -> Optional[str]:
    row = _REGISTRY["services"].get(service_name)
    if not row:
        return None
    port = row.get("port")
    if port is None:
        return None
    return str(port)


def metrics_env_key_for(service_name: str) -> str:
    row = _REGISTRY["services"].get(service_name)
    if not row:
        return "METRICS_PORT"
    key = row.get("env_key")
    if isinstance(key, str) and key:
        return key
    return "METRICS_PORT"
