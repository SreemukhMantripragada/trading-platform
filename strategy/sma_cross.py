# strategy/sma_cross.py
from typing import Dict, Any
from libs.signal import Signal, NONE
from .base import Strategy

class SMA_Cross(Strategy):
    name = "SMA_Cross"
    warmup_bars = 20
    provides_stop = False
    default_bucket = "LOW"

    def __init__(self, fast: int = 5, slow: int = 20):
        assert fast < slow
        self.fast = fast; self.slow = slow
        self._state = {}  # per-symbol state: last relation

    def on_bar(self, symbol, tf, ts, o,h,l,c,vol, feats: Dict[str, Any]):
        f = feats.get(f"sma{self.fast}")
        s = feats.get(f"sma{self.slow}")
        ready = feats.get("ready", False) and (f is not None and s is not None)
        if not ready: return NONE
        st = 1 if f > s else (-1 if f < s else 0)
        prev = self._state.get(symbol, 0)
        self._state[symbol] = st
        if prev <= 0 and st > 0:
            return Signal(side="BUY", score=0.7, tags={"why":"sma_up"})
        if prev >= 0 and st < 0:
            return Signal(side="SELL", score=0.7, tags={"why":"sma_dn"})
        return NONE
