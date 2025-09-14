from __future__ import annotations
from collections import deque
from typing import Dict, Any
from .base import BaseStrategy, Signal, HOLD
from .indicators import atr

class Supertrend_Trend(BaseStrategy):
    name="Supertrend_Trend"
    def __init__(self, **p):
        super().__init__(**p)
        self.n=int(self.params.get("n",10)); self.mult=float(self.params.get("mult",3.0))
        self.warmup_bars=self.n+20
        self.buffers["h"]=deque(maxlen=4000); self.buffers["l"]=deque(maxlen=4000); self.buffers["c"]=deque(maxlen=4000)
        self.trend=0  # +1/-1
    def on_bar(self,symbol,tf,ts,o,h,l,c,vol,features):
        H,L,C=self.buffers["h"],self.buffers["l"],self.buffers["c"]
        H.append(float(h)); L.append(float(l)); C.append(float(c))
        if len(C)<self.warmup_bars: return self._set_last(HOLD)
        a=atr(H,L,C,self.n)
        if a is None: return self._set_last(HOLD)
        basic_upper=( (H[-1]+L[-1])/2.0 ) + self.mult*a
        basic_lower=( (H[-1]+L[-1])/2.0 ) - self.mult*a
        # minimal ST flip logic
        if c > basic_upper and self.trend <= 0:
            self.trend=+1; return self._set_last(Signal("BUY",reason="Supertrend up", stop_loss=basic_lower))
        if c < basic_lower and self.trend >= 0:
            self.trend=-1; return self._set_last(Signal("SELL",reason="Supertrend down", stop_loss=basic_upper))
        return self._set_last(HOLD)
