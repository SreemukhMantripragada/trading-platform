# strategy/strategies/orb_breakout.py
from __future__ import annotations
import time
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any
from strategy.base import StrategyBase, Context, Bar

IST = timezone(timedelta(hours=5, minutes=30))

def ist_day_epoch(ts_utc: int) -> int:
    """Return YYYYMMDD of the IST date for a UTC epoch seconds."""
    d = datetime.fromtimestamp(ts_utc, tz=timezone.utc).astimezone(IST).date()
    return d.year * 10000 + d.month * 100 + d.day

def ist_minutes(ts_utc: int) -> int:
    """Minutes since IST midnight for the given UTC epoch seconds."""
    dt = datetime.fromtimestamp(ts_utc, tz=timezone.utc).astimezone(IST)
    return dt.hour * 60 + dt.minute

class ORBBreakout(StrategyBase):
    """
    Opening Range Breakout:
      - Build high/low over first `window_min` minutes after 09:15 IST.
      - After window closes, BUY on break above range high; SELL on break below range low.
      - One signal per side per day; optional cooldown to avoid churn.
      - Stop as entry * (1 ± stop_bps/1e4).

    Params:
      window_min: int (e.g., 15)
      stop_bps: int (e.g., 50 => 0.50%)
      bucket: LOW/MED/HIGH
      cooldown_sec: gate between signals on same symbol
    """
    def __init__(self, window_min: int = 15, stop_bps: int = 50,
                 bucket: str = "MED", cooldown_sec: int = 60):
        assert window_min > 0
        self.window_min = int(window_min)
        self.stop_bps = int(stop_bps)
        self.default_bucket = bucket.upper()
        self.cool = int(cooldown_sec)

        # intraday state
        self._day = {}         # sym -> yyyymmdd (IST)
        self._rng = {}         # sym -> {"hi":float, "lo":float, "done":bool}
        self._last_ts_ok = {}  # sym -> next allowed wall time (epoch s)
        self._fired_buy = {}   # sym,day -> bool
        self._fired_sell = {}  # sym,day -> bool

    def _reset_if_new_day(self, sym: str, day: int):
        if self._day.get(sym) != day:
            self._day[sym] = day
            self._rng[sym] = {"hi": float("-inf"), "lo": float("inf"), "done": False}
            self._fired_buy[(sym, day)]  = False
            self._fired_sell[(sym, day)] = False

    def _gate(self, sym: str) -> bool:
        now = int(time.time())
        nxt = int(self._last_ts_ok.get(sym, 0))
        if now < nxt:
            return False
        self._last_ts_ok[sym] = now + self.cool
        return True

    def decide(self, symbol: str, bar: Bar, ctx: Context) -> Optional[Dict[str, Any]]:
        day = ist_day_epoch(bar.ts)
        self._reset_if_new_day(symbol, day)

        mins = ist_minutes(bar.ts)
        start = 9 * 60 + 15
        end_open = start + self.window_min

        R = self._rng[symbol]

        # Build opening range while in the window
        if start <= mins < end_open:
            R["hi"] = max(R["hi"], bar.h)
            R["lo"] = min(R["lo"], bar.l)
            return None

        # Mark window done once we cross end
        if not R["done"] and mins >= end_open and R["hi"] > R["lo"]:
            R["done"] = True

        if not R["done"]:
            return None  # do nothing before the opening range completes

        # Post-window breakout checks
        if not self._fired_buy[(symbol, day)] and bar.c > R["hi"] and self._gate(symbol):
            stop = bar.c * (1.0 - self.stop_bps / 10000.0)
            self._fired_buy[(symbol, day)] = True
            return {
                "action": "BUY",
                "symbol": symbol,
                "reason": f"ORB {self.window_min}m breakout > {R['hi']:.2f}",
                "risk_bucket": self.default_bucket,
                "stop_px": stop
            }

        if not self._fired_sell[(symbol, day)] and bar.c < R["lo"] and self._gate(symbol):
            stop = bar.c * (1.0 + self.stop_bps / 10000.0)
            self._fired_sell[(symbol, day)] = True
            return {
                "action": "SELL",
                "symbol": symbol,
                "reason": f"ORB {self.window_min}m breakdown < {R['lo']:.2f}",
                "risk_bucket": self.default_bucket,
                "stop_px": stop
            }

        return None
