from __future__ import annotations
from collections import deque
from typing import Dict, Any
from .base import BaseStrategy, Signal, HOLD

class Momentum_ROC(BaseStrategy):
    name="Momentum_ROC"
    def __init__(self, **p):
        super().__init__(**p)
        self.n=int(self.params.get("n",20)); self.th=float(self.params.get("th",0.005))
        self.warmup_bars=self.n+5
        self.buffers["closes"]=deque(maxlen=2000)
    def on_bar(self,symbol,tf,ts,o,h,l,c,vol,features):
        cl=self.buffers["closes"]; cl.append(float(c))
        if len(cl)<self.warmup_bars: return self._set_last(HOLD)
        prev=cl[-self.n-1]
        roc=(c - prev)/prev
        if roc>self.th:  return self._set_last(Signal("BUY",reason=f"ROC {roc:.2%}"))
        if roc<-self.th: return self._set_last(Signal("SELL",reason=f"ROC {roc:.2%}"))
        return self._set_last(HOLD)
