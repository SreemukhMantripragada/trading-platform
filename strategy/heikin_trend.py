from __future__ import annotations
from typing import Dict, Any
from .base import BaseStrategy, Signal, HOLD

class HeikinAshi_Trend(BaseStrategy):
    name="HeikinAshi_Trend"
    def __init__(self, **p):
        super().__init__(**p)
        self.warmup_bars=5
        self.prev_ha_open=None; self.prev_ha_close=None
    def on_bar(self,symbol,tf,ts,o,h,l,c,vol,features):
        # Compute HA
        ha_close = (o+h+l+c)/4.0
        ha_open  = (self.prev_ha_open + self.prev_ha_close)/2.0 if self.prev_ha_open is not None else (o+c)/2.0
        self.prev_ha_open, self.prev_ha_close = ha_open, ha_close
        if ha_close>ha_open: return self._set_last(Signal("BUY",reason="HA up"))
        if ha_close<ha_open: return self._set_last(Signal("SELL",reason="HA down"))
        return self._set_last(HOLD)
