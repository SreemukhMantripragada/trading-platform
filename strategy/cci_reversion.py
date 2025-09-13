from __future__ import annotations
from collections import deque
from typing import Dict, Any
from .base import BaseStrategy, Signal, HOLD

class CCI_Reversion(BaseStrategy):
    name="CCI_Reversion"
    def __init__(self, **p):
        super().__init__(**p)
        self.n=int(self.params.get("n",20)); self.buy=-100.0; self.sell=100.0
        self.warmup_bars=self.n+5
        self.buffers["tp"]=deque(maxlen=2000)
    def on_bar(self,symbol,tf,ts,o,h,l,c,vol,features):
        tp=(float(h)+float(l)+float(c))/3.0
        buf=self.buffers["tp"]; buf.append(tp)
        if len(buf)<self.warmup_bars: return self._set_last(HOLD)
        sma = sum(buf[-self.n:])/self.n
        dev = (sum(abs(x - sma) for x in buf[-self.n:])/self.n) or 1e-9
        cci = (tp - sma)/(0.015*dev)
        if cci < self.buy:  return self._set_last(Signal("BUY",reason=f"CCI {cci:.1f}"))
        if cci > self.sell: return self._set_last(Signal("SELL",reason=f"CCI {cci:.1f}"))
        return self._set_last(HOLD)
