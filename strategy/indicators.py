# strategy/indicators.py
# Tiny helpers for rolling indicators computed from ctx windows.
from __future__ import annotations
from typing import List, Optional

def _closes(bars: List) -> List[float]:
    return [b.c for b in bars]

def sma(series: List[float], n: int) -> Optional[float]:
    if n <= 0 or len(series) < n:
        return None
    return sum(series[:n]) / float(n)

def rsi_wilder(series: List[float], period: int = 14) -> Optional[float]:
    # Approx Wilder RSI using simple avgs over the last `period`
    if period <= 0 or len(series) < period + 1:
        return None
    gains = []
    losses = []
    for i in range(period):
        diff = series[i] - series[i + 1]
        if diff >= 0:
            gains.append(diff)
        else:
            losses.append(-diff)
    avg_gain = (sum(gains) / period) if gains else 0.0
    avg_loss = (sum(losses) / period) if losses else 0.0
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))
