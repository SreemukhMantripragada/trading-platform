"""Prepare live pairs configuration ahead of market open.

This script validates the most recent selection bundle (default: ``configs/next_day.yaml``),
normalises it into the structure expected by the live stack, optionally copies it to
``configs/pairs_next_day.yaml`` and emits a lightweight summary artefact under
``runs/pre_market/``.

The goal is to run this automation shortly before the session starts so the live
processes pick up a fresh whitelist without manual intervention.

Usage::

    python orchestrator/pre_market_setup.py --apply

Optional arguments control the source/target paths, the maximum acceptable age for the
input file and whether the operation should run as a dry-run.

Environment:
    * SOURCE_PATH (fallback ``configs/next_day.yaml``)
    * TARGET_PATH (fallback ``configs/pairs_next_day.yaml``)
    * SUMMARY_PATH (fallback ``runs/pre_market/summary.json``)
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import yaml

from backtest.persistence import write_next_day_yaml

DEFAULT_SOURCE = Path(os.getenv("SOURCE_PATH", "configs/next_day.yaml"))
DEFAULT_TARGET = Path(os.getenv("TARGET_PATH", "configs/pairs_next_day.yaml"))
DEFAULT_SUMMARY = Path(os.getenv("SUMMARY_PATH", "runs/pre_market/summary.json"))


def _ensure_recent(path: Path, max_age_hours: float) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing next-day file: {path}")
    if max_age_hours <= 0:
        return
    mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    age_hours = (datetime.now(timezone.utc) - mtime).total_seconds() / 3600.0
    if age_hours > max_age_hours:
        raise RuntimeError(
            f"Source file {path} is {age_hours:.2f}h old (limit {max_age_hours}h)."
        )


def _normalise_entry(raw: Dict[str, Any]) -> Dict[str, Any]:
    tf = raw.get("timeframe") or raw.get("tf")
    if tf is None:
        raise ValueError(f"Missing timeframe in entry: {raw}")
    return {
        "symbol": str(raw.get("symbol")),
        "strategy": str(raw.get("strategy") or "PAIRS_MEANREV"),
        "timeframe": int(tf),
        "params": raw.get("params") or {},
        "stats": raw.get("stats") or {},
        "score": raw.get("score"),
        "max_dd": raw.get("max_dd"),
        "risk_per_trade": raw.get("risk_per_trade"),
        "bucket": raw.get("bucket", "MED"),
    }


def _load_picks(doc: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, float] | None, float | None]:
    picks: List[Dict[str, Any]] = []
    selections = doc.get("selections")
    if selections:
        for entry in selections:
            picks.append(_normalise_entry(entry))
    elif doc.get("use"):
        for entry in doc["use"]:
            picks.append(_normalise_entry(entry))
    else:
        raise ValueError("No selections found in next-day document")

    budgets = doc.get("budgets")
    if budgets:
        budgets = {str(k): float(v) for k, v in budgets.items()}
    reserve = doc.get("reserve_frac")
    if reserve is not None:
        reserve = float(reserve)
    return picks, budgets, reserve


def main() -> None:
    parser = argparse.ArgumentParser(description="Prepare pairs_next_day.yaml for live trading")
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--target", type=Path, default=DEFAULT_TARGET)
    parser.add_argument("--summary", type=Path, default=DEFAULT_SUMMARY)
    parser.add_argument("--max-age-hours", type=float, default=12.0, help="fail if source older than this")
    parser.add_argument("--dry-run", action="store_true", help="only validate, do not write outputs")
    parser.add_argument("--apply", action="store_true", help="write target even if dry-run is the default")
    args = parser.parse_args()

    _ensure_recent(args.source, args.max_age_hours)

    doc = yaml.safe_load(args.source.read_text()) or {}
    picks, budgets, reserve = _load_picks(doc)
    if not picks:
        raise RuntimeError("No eligible selections in next-day file")

    snapshot = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": str(args.source),
        "target": str(args.target),
        "count": len(picks),
        "max_age_hours": args.max_age_hours,
    }

    should_write = args.apply and not args.dry_run
    if should_write:
        write_next_day_yaml(str(args.target), picks, budgets, reserve)
        target_ts = Path(args.target).stat().st_mtime
        snapshot["target_mtime"] = datetime.fromtimestamp(target_ts, tz=timezone.utc).isoformat()
    else:
        print("[pre-market] validation successful (dry-run)")

    args.summary.parent.mkdir(parents=True, exist_ok=True)
    args.summary.write_text(json.dumps(snapshot, indent=2, sort_keys=True))
    print(f"[pre-market] summary → {args.summary} (pairs={len(picks)})")


if __name__ == "__main__":
    main()
