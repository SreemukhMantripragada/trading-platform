# risk/budget_source.py
# Central budget provider: reads Zerodha funds (if token present) or config fallback.
from __future__ import annotations
import os, yaml
from typing import Dict, Any

CFG_FILE = os.getenv("RISK_CFG", "configs/risk.yaml")
RESERVE_PCT = float(os.getenv("RESERVE_PCT", "0.20"))  # keep 20% cash aside
TOTAL_FALLBACK = float(os.getenv("TOTAL_BUDGET_FALLBACK", "100000"))  # INR

def _load_cfg() -> Dict[str, Any]:
    if not os.path.exists(CFG_FILE):  # minimal sane default
        return {"splits": {"LOW": 0.5, "MED": 0.3, "HIGH": 0.2},
                "max_per_trade": {"LOW": 20000, "MED": 20000, "HIGH": 20000}}
    with open(CFG_FILE) as f:
        return yaml.safe_load(f) or {}

def _zerodha_funds() -> float:
    """Return 'available cash for trading' if KITE token is present; else 0."""
    try:
        from kiteconnect import KiteConnect
        import json
        api_key = os.getenv("KITE_API_KEY")
        tokfile = os.getenv("ZERODHA_TOKEN_FILE", "ingestion/auth/token.json")
        if not api_key or not os.path.exists(tokfile):
            return 0.0
        t = json.load(open(tokfile))
        access = os.getenv("KITE_ACCESS_TOKEN") or t.get("access_token")
        if not access:
            return 0.0
        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access)
        f = kite.margins("equity")
        # Conservatively use "available.live_balance" if present; else fallback to net
        avail = (((f or {}).get("available") or {}).get("live_balance")) or (f or {}).get("net")
        return float(avail or 0.0)
    except Exception:
        return 0.0

def compute_budgets() -> Dict[str, Dict[str, float]]:
    """
    Returns:
      {
        "LOW":  {"budget": xxx, "max_per_trade": yyy},
        "MED":  {...},
        "HIGH": {...}
      }
    Where `budget` = (available_cash * (1-RESERVE_PCT)) * split.
    """
    cfg = _load_cfg()
    splits = cfg.get("splits") or {"LOW": 0.5, "MED": 0.3, "HIGH": 0.2}
    max_per_trade = cfg.get("max_per_trade") or {}

    total = _zerodha_funds()
    if total <= 0:
        total = TOTAL_FALLBACK

    tradable = max(0.0, total * (1.0 - RESERVE_PCT))
    out: Dict[str, Dict[str, float]] = {}
    for b in ("LOW", "MED", "HIGH"):
        sub = tradable * float(splits.get(b, 0.0))
        out[b] = {
            "budget": float(sub),
            "max_per_trade": float(max_per_trade.get(b, sub))  # default allow whole bucket per trade
        }
    return out
