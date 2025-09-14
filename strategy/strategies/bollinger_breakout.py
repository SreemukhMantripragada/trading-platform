from __future__ import annotations
from collections import deque
from typing import Dict, Any
from .base import BaseStrategy, Signal, HOLD
from .indicators import bollinger

class Bollinger_Breakout(BaseStrategy):
    name="Bollinger_Breakout"
    def __init__(self, **p):
        super().__init__(**p)
        self.n=int(self.params.get("n",20)); self.k=float(self.params.get("k",2.0))
        self.warmup_bars=self.n+5
        self.buffers["closes"]=deque(maxlen=1000)
    def on_bar(self,symbol,tf,ts,o,h,l,c,vol,features):
        cl=self.buffers["closes"]; cl.append(float(c))
        if len(cl)<self.warmup_bars: return self._set_last(HOLD)
        m,up,lo = bollinger(cl,self.n,self.k)
        if c>up: return self._set_last(Signal("BUY",reason=f"BB breakout > {up:.2f}", stop_loss=m))
        if c<lo: return self._set_last(Signal("SELL",reason=f"BB breakdown < {lo:.2f}", stop_loss=m))
        return self._set_last(HOLD)
