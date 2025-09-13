from __future__ import annotations
from collections import deque
from typing import Dict, Any
from .base import BaseStrategy, Signal, HOLD
from .indicators import ema, atr

class Keltner_Breakout(BaseStrategy):
    name="Keltner_Breakout"
    def __init__(self, **p):
        super().__init__(**p)
        self.n_ma=int(self.params.get("n_ma",20)); self.n_atr=int(self.params.get("n_atr",14)); self.mult=float(self.params.get("mult",2.0))
        self.warmup_bars=max(self.n_ma,self.n_atr)+5
        self.buffers["h"]=deque(maxlen=3000); self.buffers["l"]=deque(maxlen=3000); self.buffers["c"]=deque(maxlen=3000)
    def on_bar(self,symbol,tf,ts,o,h,l,c,vol,features):
        H,L,C=self.buffers["h"],self.buffers["l"],self.buffers["c"]
        H.append(float(h)); L.append(float(l)); C.append(float(c))
        if len(C)<self.warmup_bars: return self._set_last(HOLD)
        mid=ema(C,self.n_ma); a=atr(H,L,C,self.n_atr)
        if mid is None or a is None: return self._set_last(HOLD)
        up=mid + self.mult*a; dn=mid - self.mult*a
        if c>up: return self._set_last(Signal("BUY",reason="Keltner breakout", stop_loss=mid))
        if c<dn: return self._set_last(Signal("SELL",reason="Keltner breakdown", stop_loss=mid))
        return self._set_last(HOLD)
