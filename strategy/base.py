"""
strategy/base.py
Common signal + base class for all strategies (single or ensemble).
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Dict, Any

# One canonical signal shape used everywhere
@dataclass
class Signal:
    side: str                 # "BUY" | "SELL" | "EXIT" | "NONE"
    strength: float = 0.0     # 0..1 confidence/score
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    reason: str = ""

    @staticmethod
    def none(reason: str = "") -> "Signal":
        return Signal(side="NONE", strength=0.0, reason=reason)

class BaseStrategy:
    """
    Contract:
      - name: stable id used in registry + logging
      - params: dict used for serialization / grid search
      - warmup_bars(): how many bars before signals are reliable
      - on_bar(...): MUST be pure(ish) w.r.t. one bar step; maintain any rolling state inside the instance.

    Note: 'ctx' can include feature caches, cross-symbol info, etc.
    """
    def __init__(self, name: str, **params: Any) -> None:
        self.name = name
        self.params = params

    def warmup_bars(self, tf: str) -> int:
        """Bars required before first signal. Override per strategy."""
        return 0

    def on_bar(
        self,
        symbol: str,
        tf: str,                # e.g. "1m", "5m"
        ts: int,                # epoch seconds
        o: float, h: float, l: float, c: float,
        vol: int,
        ctx: Dict[str, Any],
    ) -> Signal:
        """Return a Signal each bar. MUST be overridden."""
        raise NotImplementedError("on_bar must be implemented")
