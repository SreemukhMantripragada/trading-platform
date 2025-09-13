# strategy/rsi_reversion.py
from typing import Dict, Any
from libs.signal import Signal, NONE
from .base import Strategy

class RSI_Reversion(Strategy):
    name = "RSI_Reversion"
    warmup_bars = 15
    provides_stop = True
    default_bucket = "MED"

    def __init__(self, period=14, buy_th=30, sell_th=70, stop_k_atr=1.5):
        self.p=period; self.buy_th=buy_th; self.sell_th=sell_th; self.k=stop_k_atr

    def on_bar(self, symbol, tf, ts, o,h,l,c,vol, feats: Dict[str, Any]):
        rsi = feats.get(f"rsi{self.p}")
        atr = feats.get(f"atr14") or feats.get(f"atr{self.p}")
        if not feats.get("ready", False) or rsi is None or atr is None:
            return NONE
        if rsi < self.buy_th:
            return Signal(side="BUY", score=0.6, entry_px=c, stop_px=max(0.01, c - self.k*atr), tags={"why":f"rsi<{self.buy_th}"})
        if rsi > self.sell_th:
            return Signal(side="SELL", score=0.6, entry_px=c, stop_px=max(0.01, c + self.k*atr), tags={"why":f"rsi>{self.sell_th}"})
        return NONE
