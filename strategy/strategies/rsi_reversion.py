# strategy/strategies/rsi_reversion_v2.py
from __future__ import annotations
from strategy.base import BaseStrategy, Candle, Signal

class RSIReversion(BaseStrategy):
    name = "RSI_REV"
    warmup = 20
    params = dict(period=14, lo=30, hi=70, stop_pct=0.006, tgt_pct=0.008)

    def on_bar(self, bar: Candle) -> Signal:
        self.push(bar)
        p = self.params["period"]
        if len(self.hist) < p + 1: return Signal("FLAT")
        r = self.rsi(p)
        if r is None: return Signal("FLAT")

        if r <= self.params["lo"]:
            stop = bar.c * (1 - self.params["stop_pct"]) if self.params["stop_pct"] else None
            tgt  = bar.c * (1 + self.params["tgt_pct"])  if self.params["tgt_pct"]  else None
            return Signal("BUY", confidence=0.7, stop=stop, target=tgt, reason=f"RSI {r:.1f}<=lo")
        if r >= self.params["hi"]:
            stop = bar.c * (1 + self.params["stop_pct"]) if self.params["stop_pct"] else None
            tgt  = bar.c * (1 - self.params["tgt_pct"])  if self.params["tgt_pct"]  else None
            return Signal("SELL", confidence=0.7, stop=stop, target=tgt, reason=f"RSI {r:.1f}>=hi")

        return Signal("FLAT")
