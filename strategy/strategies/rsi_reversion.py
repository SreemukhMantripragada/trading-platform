# strategy/strategies/rsi_reversion.py
# Mean-reversion: BUY when RSI <= low_th, SELL when RSI >= high_th (cooldown to avoid churn).
from __future__ import annotations
import time
from typing import Optional, Dict, Any
from strategy.base import StrategyBase, Context, Bar
from strategy.indicators import rsi_wilder, _closes

class RSIReversion(StrategyBase):
    def __init__(self, period: int = 14, low_th: float = 30.0, high_th: float = 70.0,
                 cooldown_sec: int = 30, bucket: str = "LOW"):
        assert period > 1, "RSI period must be >1"
        self.period = int(period)
        self.low = float(low_th)
        self.high = float(high_th)
        self.cool = int(cooldown_sec)
        self.default_bucket = bucket.upper()
        self._ts_ok = {}   # sym -> next allowed ts

    def _gate(self, sym: str) -> bool:
        now = int(time.time())
        nxt = int(self._ts_ok.get(sym, 0))
        if now < nxt:
            return False
        self._ts_ok[sym] = now + self.cool
        return True

    def decide(self, symbol: str, bar: Bar, ctx: Context) -> Optional[Dict[str, Any]]:
        closes = _closes(ctx.get_window(symbol, self.period + 1))
        rsi = rsi_wilder(closes, self.period)
        if rsi is None:
            return None
        if rsi <= self.low and self._gate(symbol):
            return {
                "action": "BUY",
                "symbol": symbol,
                "reason": f"RSI({self.period})={rsi:.1f}<=low({self.low})",
                "risk_bucket": self.default_bucket
            }
        if rsi >= self.high and self._gate(symbol):
            return {
                "action": "SELL",
                "symbol": symbol,
                "reason": f"RSI({self.period})={rsi:.1f}>=high({self.high})",
                "risk_bucket": self.default_bucket
            }
        return None
