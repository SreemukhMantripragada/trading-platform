from __future__ import annotations
from collections import deque
from typing import Dict, Any
from .base import BaseStrategy, Signal, HOLD
from .indicators import ema

class EMA_Cross(BaseStrategy):
    name = "EMA_Cross"
    def __init__(self, **p): 
        super().__init__(**p)
        self.fast=int(self.params.get("fast",12)); self.slow=int(self.params.get("slow",26))
        self.warmup_bars = max(self.fast, self.slow) + 20
        self.buffers["closes"]=deque(maxlen=max(600,self.warmup_bars+5))
        self.pos=0

    def on_bar(self, symbol, timeframe, ts, o,h,l,c,vol, features):
        cl=self.buffers["closes"]; cl.append(float(c))
        if len(cl)<self.warmup_bars: return self._set_last(HOLD)
        f=ema(cl,self.fast); s=ema(cl,self.slow)
        if f is None or s is None: return self._set_last(HOLD)
        if f>s and self.pos<=0: self.pos=+1; return self._set_last(Signal("BUY",reason="EMA bull cross"))
        if f<s and self.pos>=0: self.pos=-1; return self._set_last(Signal("SELL",reason="EMA bear cross"))
        return self._set_last(HOLD)
