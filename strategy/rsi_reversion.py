"""
RSI Reversion strategy (refactored to BaseStrategy).

Idea:
- Go LONG when RSI < buy_threshold
- Go SHORT when RSI > sell_threshold
- EXIT when RSI returns to mid band (e.g., 50) or stop/trailing rules (optional)

Params:
  period: int (default 14)
  buy_threshold: float (default 30)
  sell_threshold: float (default 70)
  exit_band: float (default 50)
  warmup_bars: int (default 50)  # ensure enough data for reliable RSI

Notes:
- This is intentionally simple; plug in your own risk/SL logic via runner+risk manager.
"""
from __future__ import annotations
from collections import deque
from typing import Dict, Any
from .base import BaseStrategy, Signal, HOLD, rsi_from_closes

class RSI_Reversion(BaseStrategy):
    name = "RSI_Reversion"

    def __init__(self, **params: Any) -> None:
        super().__init__(**params)
        self.period = int(self.params.get("period", 14))
        self.buy_th = float(self.params.get("buy_threshold", 30.0))
        self.sell_th = float(self.params.get("sell_threshold", 70.0))
        self.exit_band = float(self.params.get("exit_band", 50.0))
        self.warmup_bars = int(self.params.get("warmup_bars", 50))
        self.buffers["closes"] = deque(maxlen=max(500, self.warmup_bars + self.period + 5))

    def on_bar(self, symbol: str, timeframe: str, ts: int,
               o: float, h: float, l: float, c: float, vol: int,
               features: Dict[str, Any]) -> Signal:
        closes: deque = self.buffers["closes"]
        closes.append(float(c))

        ready = features.get("ready", False) and (len(closes) >= self.warmup_bars)
        if not ready:
            return self._set_last(HOLD)

        rsi = rsi_from_closes(closes, self.period)
        if rsi is None:
            return self._set_last(HOLD)

        # Simple rules
        if rsi < self.buy_th:
            return self._set_last(Signal(side="BUY", strength=min(1.0, (self.buy_th - rsi) / 20.0),
                                         reason=f"RSI {rsi:.1f} < {self.buy_th}"))
        if rsi > self.sell_th:
            return self._set_last(Signal(side="SELL", strength=min(1.0, (rsi - self.sell_th) / 20.0),
                                         reason=f"RSI {rsi:.1f} > {self.sell_th}"))

        # Mean-revert to band → EXIT if currently in a position (runner decides using last_signal/state)
        return self._set_last(Signal(side="EXIT", strength=0.5, reason=f"RSI mean-revert {rsi:.1f} ~ {self.exit_band}"))
