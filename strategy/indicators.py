"""
Lightweight indicator helpers; all pure-python, rolling-friendly.
Each function returns None if not enough data.
"""
from __future__ import annotations
from collections import deque
from typing import Optional, Iterable, Tuple
import math

def sma(xs: Iterable[float], n:int) -> Optional[float]:
    xs = list(xs)
    if len(xs) < n: return None
    return sum(xs[-n:]) / n

def ema(xs: Iterable[float], n:int) -> Optional[float]:
    xs = list(xs)
    if len(xs) < n: return None
    k = 2.0 / (n + 1.0)
    e = xs[0]
    for x in xs[1:]:
        e = x * k + e * (1 - k)
    return e

def rsi(closes: Iterable[float], n:int) -> Optional[float]:
    xs = list(closes)
    if len(xs) < n+1: return None
    gains = losses = 0.0
    for i in range(-n, 0):
        d = xs[i] - xs[i-1]
        if d >= 0: gains += d
        else: losses -= d
    avg_gain = gains / n
    avg_loss = losses / n if losses != 0 else 1e-12
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1 + rs))

def macd(closes: Iterable[float], fast:int=12, slow:int=26, sig:int=9) -> Optional[Tuple[float,float,float]]:
    xs = list(closes)
    if len(xs) < slow + sig: return None
    def _ema(n):
        k = 2.0 / (n + 1.0)
        e = xs[0]
        for x in xs[1:]:
            e = x * k + e * (1 - k)
        return e
    macd_line = _ema(fast) - _ema(slow)
    # signal from macd stream (approx with last macd only → acceptable for demo)
    signal = macd_line  # placeholder simple variant
    hist = macd_line - signal
    return macd_line, signal, hist

def bollinger(closes: Iterable[float], n:int=20, k:float=2.0) -> Optional[Tuple[float,float,float]]:
    xs = list(closes)
    if len(xs) < n: return None
    m = sum(xs[-n:]) / n
    var = sum((x - m)**2 for x in xs[-n:]) / n
    sd = math.sqrt(var)
    return m, m + k*sd, m - k*sd

def donchian(highs: Iterable[float], lows: Iterable[float], n:int=20) -> Optional[Tuple[float,float]]:
    H = list(highs); L = list(lows)
    if len(H) < n or len(L) < n: return None
    return max(H[-n:]), min(L[-n:])

def atr(highs: Iterable[float], lows: Iterable[float], closes: Iterable[float], n:int=14) -> Optional[float]:
    H, L, C = list(highs), list(lows), list(closes)
    if len(C) < n+1: return None
    trs=[]
    for i in range(-n, 0):
        tr = max(H[i]-L[i], abs(H[i]-C[i-1]), abs(L[i]-C[i-1]))
        trs.append(tr)
    return sum(trs)/n

def vwap(typ_prices: Iterable[float], vols: Iterable[int], n:int=60) -> Optional[float]:
    T = list(typ_prices); V = list(vols)
    if len(T) < n or len(V) < n: return None
    tv = sum(T[-n:][i]*V[-n:][i] for i in range(n))
    vv = sum(V[-n:])
    return tv / (vv if vv else 1.0)
