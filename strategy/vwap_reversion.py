from __future__ import annotations
from collections import deque
from typing import Dict, Any
from .base import BaseStrategy, Signal, HOLD
from .indicators import vwap

class VWAP_Reversion(BaseStrategy):
    name="VWAP_Reversion"
    def __init__(self, **p):
        super().__init__(**p)
        self.win=int(self.params.get("win",60))
        self.k=float(self.params.get("k",0.003))  # 0.3% distance
        self.warmup_bars=self.win+5
        self.buffers["typ"]=deque(maxlen=5000); self.buffers["vol"]=deque(maxlen=5000)
    def on_bar(self,symbol,tf,ts,o,h,l,c,vol,features):
        typ=(float(h)+float(l)+float(c))/3.0
        self.buffers["typ"].append(typ); self.buffers["vol"].append(int(vol))
        if len(self.buffers["typ"])<self.warmup_bars: return self._set_last(HOLD)
        v=vwap(self.buffers["typ"], self.buffers["vol"], self.win)
        if v is None: return self._set_last(HOLD)
        dist=(c - v)/v
        if dist < -self.k: return self._set_last(Signal("BUY",reason=f"below VWAP {dist:.3%}", take_profit=v))
        if dist >  self.k: return self._set_last(Signal("SELL",reason=f"above VWAP {dist:.3%}", take_profit=v))
        return self._set_last(HOLD)
