"""
strategy/examples/basic_strategies.py
SMA_Cross and RSI_Reversion implemented using BaseStrategy.
"""

from __future__ import annotations
from collections import deque
from typing import Dict, Any
from .base import BaseStrategy, Signal

# ---------- SMA Cross ----------
class SMA_Cross(BaseStrategy):
    def __init__(self, fast: int = 10, slow: int = 50, **kw):
        super().__init__(name="SMA_CROSS", fast=fast, slow=slow, **kw)
        self.fast = int(fast)
        self.slow = int(slow)
        self._prices = {}  # symbol -> deque

    def warmup_bars(self, tf: str) -> int:
        return max(self.fast, self.slow)

    def on_bar(self, symbol, tf, ts, o, h, l, c, vol, ctx: Dict[str, Any]) -> Signal:
        dq = self._prices.setdefault(symbol, deque(maxlen=self.slow))
        dq.append(float(c))
        if len(dq) < self.slow:
            return Signal.none("warmup")
        fast_ma = sum(list(dq)[-self.fast:]) / self.fast
        slow_ma = sum(dq) / self.slow
        if fast_ma > slow_ma * 1.0005:
            return Signal(side="BUY", strength=0.8, reason=f"fast>{self.fast} slow>{self.slow}")
        if fast_ma < slow_ma * 0.9995:
            return Signal(side="SELL", strength=0.8, reason=f"fast<{self.fast} slow<{self.slow}")
        return Signal.none("no_edge")

# ---------- RSI Mean-Reversion ----------
class RSI_Reversion(BaseStrategy):
    def __init__(self, period: int = 14, overbought: int = 70, oversold: int = 30, **kw):
        super().__init__(name="RSI_REV", period=period, overbought=overbought, oversold=oversold, **kw)
        self.n = int(period); self.ob = int(overbought); self.os = int(oversold)
        self._closes = {}  # symbol -> deque

    def warmup_bars(self, tf: str) -> int:
        return self.n + 1

    def _rsi(self, closes):
        gains = 0.0; losses = 0.0
        for i in range(1, len(closes)):
            ch = closes[i] - closes[i-1]
            if ch >= 0: gains += ch
            else: losses -= ch
        if self.n == 0: return 50.0
        avg_gain = gains / self.n
        avg_loss = losses / self.n
        if avg_loss == 0: return 100.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    def on_bar(self, symbol, tf, ts, o, h, l, c, vol, ctx: Dict[str, Any]) -> Signal:
        dq = self._closes.setdefault(symbol, deque(maxlen=self.n + 1))
        dq.append(float(c))
        if len(dq) < self.n + 1:
            return Signal.none("warmup")
        r = self._rsi(list(dq))
        if r <= self.os:
            # mean-rev long with soft SL at recent low
            sl = min(list(dq)) * 0.99
            return Signal(side="BUY", strength=min(1.0, (self.os - r) / 30.0), stop_loss=sl, reason=f"RSI={r:.1f}<=OS")
        if r >= self.ob:
            sl = max(list(dq)) * 1.01
            return Signal(side="SELL", strength=min(1.0, (r - self.ob) / 30.0), stop_loss=sl, reason=f"RSI={r:.1f}>=OB")
        return Signal.none("no_edge")
