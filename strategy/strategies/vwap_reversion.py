# strategy/strategies/vwap_reversion.py
from __future__ import annotations
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any
from strategy.base import StrategyBase, Context, Bar

IST = timezone(timedelta(hours=5, minutes=30))

def ist_day_epoch(ts_utc: int) -> int:
    d = datetime.fromtimestamp(ts_utc, tz=timezone.utc).astimezone(IST).date()
    return d.year * 10000 + d.month * 100 + d.day

class VWAPReversion(StrategyBase):
    """
    Session VWAP mean-reversion:
      - Maintains session (IST day) PV and V sums to compute VWAP.
      - BUY when price < vwap * (1 - band_bps/1e4).
      - SELL when price > vwap * (1 + band_bps/1e4).
      - stop_px = entry * (1 ± stop_bps/1e4).

    Params:
      band_bps: int (e.g., 20 => 0.20% away from VWAP)
      stop_bps: int (e.g., 40 => 0.40% stop)
      min_vol:  int minimal bar volume to act
      cooldown_sec: throttle repeated signals
      bucket: LOW/MED/HIGH
    """
    def __init__(self, band_bps: int = 20, stop_bps: int = 40,
                 min_vol: int = 0, cooldown_sec: int = 30, bucket: str = "LOW"):
        self.band_bps = int(band_bps)
        self.stop_bps = int(stop_bps)
        self.min_vol  = int(min_vol)
        self.cool = int(cooldown_sec)
        self.default_bucket = bucket.upper()

        # per-symbol session accumulators
        self._day = {}       # sym -> yyyymmdd (IST)
        self._pv = {}        # sym -> sum(price*vol)
        self._v  = {}        # sym -> sum(vol)
        self._ts_ok = {}     # sym -> next allowed ts

    def _reset_if_new_day(self, sym: str, day: int):
        if self._day.get(sym) != day:
            self._day[sym] = day
            self._pv[sym] = 0.0
            self._v[sym]  = 0

    def _gate(self, sym: str, now_ts: int) -> bool:
        nxt = int(self._ts_ok.get(sym, 0))
        if now_ts < nxt: return False
        self._ts_ok[sym] = now_ts + self.cool
        return True

    def decide(self, symbol: str, bar: Bar, ctx: Context) -> Optional[Dict[str, Any]]:
        day = ist_day_epoch(bar.ts)
        self._reset_if_new_day(symbol, day)

        if bar.vol is not None and bar.vol < self.min_vol:
            return None

        # update session accumulators (use close*vol for simplicity)
        self._pv[symbol] = float(self._pv.get(symbol, 0.0)) + float(bar.c) * int(bar.vol or 0)
        self._v[symbol]  = int(self._v.get(symbol, 0)) + int(bar.vol or 0)

        v = max(1, int(self._v[symbol]))
        vwap = self._pv[symbol] / v

        band = self.band_bps / 10000.0
        low_thr  = vwap * (1.0 - band)
        high_thr = vwap * (1.0 + band)

        if bar.c < low_thr and self._gate(symbol, bar.ts):
            stop = bar.c * (1.0 - self.stop_bps / 10000.0)
            return {
                "action": "BUY",
                "symbol": symbol,
                "reason": f"VWAP MR: c={bar.c:.2f} < {low_thr:.2f} (vwap={vwap:.2f})",
                "risk_bucket": self.default_bucket,
                "stop_px": stop
            }
        if bar.c > high_thr and self._gate(symbol, bar.ts):
            stop = bar.c * (1.0 + self.stop_bps / 10000.0)
            return {
                "action": "SELL",
                "symbol": symbol,
                "reason": f"VWAP MR: c={bar.c:.2f} > {high_thr:.2f} (vwap={vwap:.2f})",
                "risk_bucket": self.default_bucket,
                "stop_px": stop
            }
        return None
