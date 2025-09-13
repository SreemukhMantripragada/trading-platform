from __future__ import annotations
from collections import deque
from typing import Dict, Any
from .base import BaseStrategy, Signal, HOLD
from .indicators import donchian

class Donchian_Breakout(BaseStrategy):
    name="Donchian_Breakout"
    def __init__(self, **p):
        super().__init__(**p)
        self.n=int(self.params.get("n",20)); self.warmup_bars=self.n+5
        self.buffers["highs"]=deque(maxlen=2000); self.buffers["lows"]=deque(maxlen=2000)
    def on_bar(self,symbol,tf,ts,o,h,l,c,vol,features):
        H=self.buffers["highs"]; L=self.buffers["lows"]
        H.append(float(h)); L.append(float(l))
        if len(H)<self.warmup_bars: return self._set_last(HOLD)
        hi, lo = donchian(H,L,self.n)
        if c>hi: return self._set_last(Signal("BUY",reason=f"Donchian breakout {self.n}H"))
        if c<lo: return self._set_last(Signal("SELL",reason=f"Donchian breakdown {self.n}L"))
        return self._set_last(HOLD)
