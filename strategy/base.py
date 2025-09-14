"""
strategy/base.py
Canonical contracts for strategies and ensembles. Keep child strats tiny:
- implement decide(symbol, bar, ctx) -> one of:
    None  (no action), or dict:
      {"action":"BUY"/"SELL",
       "symbol":sym,
       "reason":"...",
       "stop_px": <optional float>,
       "risk_bucket": "LOW"/"MED"/"HIGH",
       "extras": {...}}  # any fields you want forwarded
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List

@dataclass
class Bar:
    ts: int           # epoch seconds (UTC)
    o: float; h: float; l: float; c: float
    vol: int = 0
    n: int = 0

class Context:
    """
    Strategy context provides rolling history and helpers.
    - ctx.hist[symbol] : most-recent-first list[Bar]
    - ctx.get_window(symbol, n) -> list[Bar]
    - ctx.user : dict bag for sharing state across strategies
    """
    def __init__(self):
        self.hist: Dict[str, List[Bar]] = {}
        self.user: Dict[str, Any] = {}

    def push_bar(self, symbol: str, bar: Bar, max_keep: int = 300):
        h = self.hist.setdefault(symbol, [])
        h.insert(0, bar)
        if len(h) > max_keep:
            del h[max_keep:]

    def get_window(self, symbol: str, n: int) -> List[Bar]:
        return (self.hist.get(symbol) or [])[:n]

class StrategyBase:
    """
    Minimal required surface. Keep implementations deterministic/pure on inputs.
    """
    name: str = "GENERIC"
    default_bucket: str = "MED"

    def decide(self, symbol: str, bar: Bar, ctx: Context) -> Optional[Dict[str, Any]]:
        raise NotImplementedError

class EnsembleBase:
    """
    Combine multiple strategies. Default is K-of-N voting with optional weights.
    """
    name: str = "ENSEMBLE"

    def __init__(self, members: List[StrategyBase], k: int = 1, weights: Optional[Dict[str, float]] = None):
        self.members = members
        self.k = max(1, k)
        self.weights = weights or {}

    def decide(self, symbol: str, bar: Bar, ctx: Context) -> Optional[Dict[str, Any]]:
        votes = []
        for m in self.members:
            r = m.decide(symbol, bar, ctx)
            if r and r.get("action") in ("BUY", "SELL"):
                w = float(self.weights.get(m.name, 1.0))
                votes.extend([r] * int(max(1, w)))
        if not votes:
            return None
        buys = sum(1 for v in votes if v["action"] == "BUY")
        sells= sum(1 for v in votes if v["action"] == "SELL")
        if max(buys, sells) < self.k:
            return None
        top = "BUY" if buys >= sells else "SELL"
        # Forward the strongest member's reason if available
        reason = f"{self.name}: vote({buys}B/{sells}S, k={self.k})"
        return {"action": top, "symbol": symbol, "reason": reason, "risk_bucket": "MED", "extras": {}}
