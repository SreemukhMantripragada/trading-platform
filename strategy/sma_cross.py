from __future__ import annotations
from collections import deque
from typing import Dict, Any
from .base import BaseStrategy, Signal, HOLD
from .indicators import sma

class SMA_Cross(BaseStrategy):
    name = "SMA_Cross"
    def __init__(self, **params: Any) -> None:
        super().__init__(**params)
        self.fast = int(self.params.get("fast", 20))
        self.slow = int(self.params.get("slow", 50))
        self.warmup_bars = max(self.fast, self.slow) + 5
        self.buffers["closes"] = deque(maxlen=max(500, self.warmup_bars+5))
        self._last_pos = 0  # +1 long, -1 short, 0 flat

    def on_bar(self, symbol, timeframe, ts, o,h,l,c,vol, features: Dict[str,Any]):
        closes = self.buffers["closes"]; closes.append(float(c))
        if len(closes) < self.warmup_bars: return self._set_last(HOLD)
        f = sma(closes, self.fast); s = sma(closes, self.slow)
        if f is None or s is None: return self._set_last(HOLD)
        if f > s and self._last_pos <= 0:
            self._last_pos = +1
            return self._set_last(Signal("BUY", reason=f"SMA {self.fast}>{self.slow}"))
        if f < s and self._last_pos >= 0:
            self._last_pos = -1
            return self._set_last(Signal("SELL", reason=f"SMA {self.fast}<{self.slow}"))
        return self._set_last(HOLD)
