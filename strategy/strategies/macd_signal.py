from __future__ import annotations
from collections import deque
from typing import Dict, Any
from .base import BaseStrategy, Signal, HOLD
from .indicators import macd

class MACD_Signal(BaseStrategy):
    name="MACD_Signal"
    def __init__(self, **p):
        super().__init__(**p)
        self.fast=int(self.params.get("fast",12)); self.slow=int(self.params.get("slow",26)); self.signal=int(self.params.get("signal",9))
        self.warmup_bars=self.slow+self.signal+20
        self.buffers["closes"]=deque(maxlen=max(800,self.warmup_bars+5))
        self.pos=0
    def on_bar(self, symbol, timeframe, ts, o,h,l,c,vol, features):
        cl=self.buffers["closes"]; cl.append(float(c))
        if len(cl)<self.warmup_bars: return self._set_last(HOLD)
        m=macd(cl,self.fast,self.slow,self.signal)
        if m is None: return self._set_last(HOLD)
        macd_line, sig, _ = m
        if macd_line>sig and self.pos<=0: self.pos=+1; return self._set_last(Signal("BUY",reason="MACD>Signal"))
        if macd_line<sig and self.pos>=0: self.pos=-1; return self._set_last(Signal("SELL",reason="MACD<Signal"))
        return self._set_last(HOLD)
