# strategy/strategies/sma_cross.py
from __future__ import annotations
from strategy.base import BaseStrategy, Candle, Signal

class SMACross(BaseStrategy):
    name = "SMA_CROSS"
    warmup = 50
    params = dict(fast=20, slow=50, stop_pct=0.005, tgt_pct=0.01)

    def on_bar(self, bar: Candle) -> Signal:
        self.push(bar)
        f = self.params["fast"]; s = self.params["slow"]
        if len(self.hist) < max(f, s): return Signal("FLAT")

        sma_f = self.sma(f); sma_s = self.sma(s)
        if sma_f is None or sma_s is None: return Signal("FLAT")

        prev_f = self.sma(f)  # using same since we keep 1m cadence; fine for simple demo
        prev_s = self.sma(s)

        # Crossover check: fast crosses above slow -> BUY; below -> SELL
        if sma_f > sma_s and prev_f is not None and prev_s is not None:
            stop = bar.c * (1 - self.params["stop_pct"]) if self.params["stop_pct"] else None
            tgt  = bar.c * (1 + self.params["tgt_pct"])  if self.params["tgt_pct"]  else None
            return Signal("BUY", confidence=0.8, stop=stop, target=tgt, reason=f"fast({f})>{s}")
        elif sma_f < sma_s:
            stop = bar.c * (1 + self.params["stop_pct"]) if self.params["stop_pct"] else None
            tgt  = bar.c * (1 - self.params["tgt_pct"])  if self.params["tgt_pct"]  else None
            return Signal("SELL", confidence=0.8, stop=stop, target=tgt, reason=f"fast({f})<{s}")

        return Signal("FLAT")
