from __future__ import annotations
from collections import deque
from typing import Dict, Any
from .base import BaseStrategy, Signal, HOLD
from .indicators import bollinger

class Bollinger_Reversion(BaseStrategy):
    name="Bollinger_Reversion"
    def __init__(self, **p):
        super().__init__(**p)
        self.n=int(self.params.get("n",20)); self.k=float(self.params.get("k",2.0))
        self.exit_band=float(self.params.get("exit_band",0.0))  # revert to mean
        self.warmup_bars=self.n+5
        self.buffers["closes"]=deque(maxlen=1000); self.pos=0
    def on_bar(self,symbol,tf,ts,o,h,l,c,vol,features):
        cl=self.buffers["closes"]; cl.append(float(c))
        if len(cl)<self.warmup_bars: return self._set_last(HOLD)
        m,up,lo = bollinger(cl,self.n,self.k)
        if self.pos==0:
            if c<lo: self.pos=+1; return self._set_last(Signal("BUY",reason="BB low reversion", take_profit=m))
            if c>up: self.pos=-1; return self._set_last(Signal("SELL",reason="BB high reversion", take_profit=m))
        else:
            if (self.pos>0 and c>=m) or (self.pos<0 and c<=m):
                self.pos=0; return self._set_last(Signal("EXIT",reason="mean touch"))
        return self._set_last(HOLD)
