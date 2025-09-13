# indicators/atr.py
from typing import Sequence, Dict, Any, Tuple
from .base import Indicator

def true_range(bar_prev: Tuple[float,float,float,float], bar_cur: Tuple[float,float,float,float]) -> float:
    # (o,h,l,c) tuples (we use c_prev)
    _, h, l, c_prev = bar_cur[0], bar_cur[1], bar_cur[2], bar_prev[3]
    return max(bar_cur[1] - bar_cur[2], abs(bar_cur[1] - c_prev), abs(bar_cur[2] - c_prev))

class ATR(Indicator):
    name = "ATR"
    def __init__(self, period: int = 14):
        self.period = period
        self.lookback = period + 1

    def update(self, hlc_series: Sequence[Tuple[float,float,float,float]]) -> Dict[str, Any]:
        if len(hlc_series) < self.lookback:
            return {"ready": False, f"atr{self.period}": None}
        trs = []
        for i in range(1, self.period+1):
            prev = hlc_series[-(i+1)]
            cur  = hlc_series[-i]
            tr = max(cur[1]-cur[2], abs(cur[1]-prev[3]), abs(cur[2]-prev[3]))
            trs.append(tr)
        atr = sum(trs)/self.period
        return {"ready": True, f"atr{self.period}": atr}
