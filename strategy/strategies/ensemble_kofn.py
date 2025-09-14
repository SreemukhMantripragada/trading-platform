# strategy/strategies/ensemble_kofn.py
# K-of-N voting ensemble that wraps child strategies defined in config.
from __future__ import annotations
import importlib
from typing import List, Dict, Any, Optional
from strategy.base import EnsembleBase, StrategyBase, Context, Bar

def _mk(spec: Dict[str, Any]) -> StrategyBase:
    m = importlib.import_module(spec["module"])
    cls = getattr(m, spec["class"])
    return cls(**(spec.get("params") or {}))

class EnsembleKofN(StrategyBase):
    """
    Params:
      members: [ {module: strategy.strategies.sma_cross, class: SMACross, params:{...}}, ... ]
      k: 1..N
      weights: {SMACross: 1.0, RSIReversion: 1.0}
      bucket: MED
    """
    def __init__(self, members: List[Dict[str, Any]], k: int = 1,
                 weights: Optional[Dict[str, float]] = None, bucket: str = "MED"):
        self.members = [_mk(s) for s in members]
        self.ens = EnsembleBase(self.members, k=k, weights=weights or {})
        self.default_bucket = bucket.upper()
        self.name = "ENSEMBLE_KOFN"

    def decide(self, symbol: str, bar: Bar, ctx: Context) -> Optional[Dict[str, Any]]:
        r = self.ens.decide(symbol, bar, ctx)
        if not r:
            return None
        # enforce ensemble-level bucket unless member returned a specific one
        r["risk_bucket"] = self.default_bucket
        r["reason"] = f"{self.name}: {r.get('reason','')}"
        return r
