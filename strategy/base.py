# strategy/base.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Dict, Any, List

@dataclass
class Candle:
    symbol: str
    ts: "datetime"   # tz-aware UTC
    o: float; h: float; l: float; c: float
    vol: int

@dataclass
class Signal:
    # action: BUY (open long or add), SELL (close long or go short if allowed), FLAT (no action)
    action: str                # 'BUY' | 'SELL' | 'FLAT'
    confidence: float = 1.0    # 0..1
    stop: Optional[float] = None
    target: Optional[float] = None
    reason: str = ""

class BaseStrategy:
    """Abstract parent for all strategies."""
    name: str = "Base"
    warmup: int = 0            # min bars required before signals
    params: Dict[str, Any] = {}  # overridden per child

    def __init__(self, **kwargs):
        self.params = {**self.params, **kwargs}
        self._history: List[Candle] = []

    # --- lifecycle ---
    def on_start(self, symbol: str): pass
    def on_end(self, symbol: str): pass

    # --- mandatory: implement your logic ---
    def on_bar(self, bar: Candle) -> Signal:
        """Return Signal(...) each bar (or Signal('FLAT'))."""
        raise NotImplementedError

    # --- helpers ---
    def push(self, bar: Candle):
        self._history.append(bar)
        # limit memory: keep last ~1000 bars per symbol
        if len(self._history) > 2000:
            self._history = self._history[-1000:]

    @property
    def hist(self) -> List[Candle]:
        return self._history

    def close(self, n: int = 1) -> List[float]:
        return [b.c for b in self._history[-n:]] if n > 0 else []

    def high(self, n: int = 1) -> List[float]:
        return [b.h for b in self._history[-n:]]

    def low(self, n: int = 1) -> List[float]:
        return [b.l for b in self._history[-n:]]

    def sma(self, n: int) -> Optional[float]:
        arr = self.close(n)
        return sum(arr)/n if len(arr) == n else None

    def ema(self, n: int) -> Optional[float]:
        arr = self.close(n)
        if len(arr) < n: return None
        k = 2/(n+1); ema = arr[0]
        for x in arr[1:]: ema = x*k + ema*(1-k)
        return ema

    def rsi(self, n: int) -> Optional[float]:
        arr = self.close(n+1)
        if len(arr) < n+1: return None
        gains = []; losses = []
        for i in range(1, len(arr)):
            d = arr[i] - arr[i-1]
            gains.append(max(d,0)); losses.append(max(-d,0))
        avg_g = sum(gains)/n; avg_l = sum(losses)/n
        if avg_l == 0: return 100.0
        rs = avg_g/avg_l
        return 100 - (100/(1+rs))
