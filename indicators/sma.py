# indicators/sma.py
from typing import Sequence, Dict, Any
from .base import Indicator

class SMA(Indicator):
    name = "SMA"
    def __init__(self, period: int = 20):
        self.period = period
        self.lookback = period

    def update(self, closes: Sequence[float]) -> Dict[str, Any]:
        ready = len(closes) >= self.period
        v = sum(closes[-self.period:]) / self.period if ready else None
        return {"ready": ready, f"sma{self.period}": v}