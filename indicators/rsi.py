# indicators/rsi.py
from typing import Sequence, Dict, Any
from .base import Indicator

class RSI(Indicator):
    name = "RSI"
    def __init__(self, period: int = 14):
        self.period = period
        self.lookback = period + 1

    def update(self, closes: Sequence[float]) -> Dict[str, Any]:
        if len(closes) < self.lookback:
            return {"ready": False, f"rsi{self.period}": None}
        gains = losses = 0.0
        for i in range(len(closes)-self.period, len(closes)):
            d = closes[i] - closes[i-1]
            if d > 0: gains += d
            elif d < 0: losses += -d
        avg_gain = gains / self.period
        avg_loss = losses / self.period
        rsi = 100.0 if avg_loss == 0 else 100.0 - (100.0 / (1.0 + (avg_gain/avg_loss)))
        return {"ready": True, f"rsi{self.period}": rsi}
