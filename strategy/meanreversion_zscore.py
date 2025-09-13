from __future__ import annotations
from collections import deque
from statistics import mean, pstdev
from typing import Dict, Any
from .base import BaseStrategy, Signal, HOLD

class MeanRev_ZScore(BaseStrategy):
    name="MeanRev_ZScore"
    def __init__(self, **p):
        super().__init__(**p)
        self.win=int(self.params.get("win",60)); self.z_enter=float(self.params.get("z_enter",2.0)); self.z_exit=float(self.params.get("z_exit",0.5))
        self.warmup_bars=self.win+5
        self.buffers["c"]=deque(maxlen=5000); self.pos=0
    def on_bar(self,symbol,tf,ts,o,h,l,c,vol,features):
        C=self.buffers["c"]; C.append(float(c))
        if len(C)<self.warmup_bars: return self._set_last(HOLD)
        m=mean(C); s=pstdev(C) or 1e-9
        z=(c-m)/s
        if self.pos==0:
            if z<-self.z_enter: self.pos=+1; return self._set_last(Signal("BUY",reason=f"z={z:.2f}"))
            if z> self.z_enter: self.pos=-1; return self._set_last(Signal("SELL",reason=f"z={z:.2f}"))
        else:
            if abs(z)<self.z_exit: self.pos=0; return self._set_last(Signal("EXIT",reason=f"|z|<{self.z_exit}"))
        return self._set_last(HOLD)
