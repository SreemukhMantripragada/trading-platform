from __future__ import annotations
from typing import Dict, Any
from .base import BaseStrategy, Signal, HOLD

class OpeningRangeBreakout(BaseStrategy):
    name="OpeningRangeBreakout"
    def __init__(self, **p):
        super().__init__(**p)
        self.minutes=int(self.params.get("minutes",15))
        self._range=None; self._done_day=False
        self.warmup_bars=1
    def on_bar(self,symbol,tf,ts,o,h,l,c,vol,features):
        # ts is epoch seconds (UTC). Compute minutes since IST open (09:15 IST).
        # For simplicity assume IN_TOPIC is 1m and first 15 bars belong to OR.
        # Keep a simple day reset when near midnight UTC.
        if self._range is None:
            self._range=[float(h), float(l), 1]  # hi, lo, count
            return self._set_last(HOLD)
        hi,lo,cnt=self._range
        if cnt < self.minutes:
            hi=max(hi,float(h)); lo=min(lo,float(l)); self._range=[hi,lo,cnt+1]
            return self._set_last(HOLD)
        if self._done_day: return self._set_last(HOLD)
        if c>hi: self._done_day=True; return self._set_last(Signal("BUY",reason=f"ORB > {hi:.2f}"))
        if c<lo: self._done_day=True; return self._set_last(Signal("SELL",reason=f"ORB < {lo:.2f}"))
        return self._set_last(HOLD)
