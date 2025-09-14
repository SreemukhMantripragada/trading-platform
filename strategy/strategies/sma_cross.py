# strategy/strategies/sma_cross.py
# Classic SMA fast/slow cross. Emits BUY on fast↑cross slow, SELL on fast↓cross slow.
from __future__ import annotations
import time
from typing import Optional, Dict, Any
from strategy.base import StrategyBase, Context, Bar
from strategy.indicators import sma, _closes

class SMACross(StrategyBase):
    def __init__(self, fast: int = 10, slow: int = 20, cooldown_sec: int = 30, bucket: str = "MED"):
        assert fast > 0 and slow > 0 and fast < slow, "Require 0<fast<slow"
        self.fast = fast
        self.slow = slow
        self.cool = int(cooldown_sec)
        self.default_bucket = bucket.upper()
        self._last = {}      # sym -> last (fast-slow) sign
        self._ts_ok = {}     # sym -> next allowed ts

    def _gate(self, sym: str) -> bool:
        now = int(time.time())
        nxt = int(self._ts_ok.get(sym, 0))
        if now < nxt:
            return False
        self._ts_ok[sym] = now + self.cool
        return True

    def decide(self, symbol: str, bar: Bar, ctx: Context) -> Optional[Dict[str, Any]]:
        win = ctx.get_window(symbol, max(self.slow + 1, 2))
        closes = _closes(win)
        f = sma(closes, self.fast)
        s = sma(closes, self.slow)
        if f is None or s is None:
            return None

        diff = f - s
        prev = self._last.get(symbol)
        self._last[symbol] = diff

        if prev is None:
            return None  # need previous to detect a crossing

        crossed_up = prev <= 0 and diff > 0
        crossed_dn = prev >= 0 and diff < 0

        if crossed_up and self._gate(symbol):
            return {
                "action": "BUY",
                "symbol": symbol,
                "reason": f"SMA {self.fast}/{self.slow} cross up",
                "risk_bucket": self.default_bucket,
                # stop_px optional; left None for notional sizing path
            }
        if crossed_dn and self._gate(symbol):
            return {
                "action": "SELL",
                "symbol": symbol,
                "reason": f"SMA {self.fast}/{self.slow} cross down",
                "risk_bucket": self.default_bucket,
            }
        return None
