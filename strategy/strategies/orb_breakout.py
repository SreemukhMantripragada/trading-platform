# strategy/strategies/orb_breakout_v2.py
from __future__ import annotations
from typing import Optional, Tuple, Dict, Any
from datetime import timedelta
from zoneinfo import ZoneInfo

from strategy.base import BaseStrategy, Candle, Signal

__all__ = ["ORBBreakout"]

IST = ZoneInfo("Asia/Kolkata")


class ORBBreakout(BaseStrategy):
    """
    Opening Range Breakout (ORB), IST session.

    Behavior:
      - Build opening range [HH, LL] from 09:15 IST for `orb_minutes`.
      - After the window ends, emit a one-shot breakout:
          BUY  when close > HH
          SELL when close < LL
      - Optionally apply stop/target as percentages of breakout price.

    Usage (unchanged):
        strat = ORBBreakout(orb_minutes=15, stop_pct=0.006, tgt_pct=0.012)
        sig = strat.on_bar(candle)

    Params (self.params):
        orb_minutes (int)   default 15     # opening range duration in minutes
        start_h     (int)   default 9      # start hour (local, IST)
        start_m     (int)   default 15     # start minute (local, IST)
        tz          (str)   default "Asia/Kolkata"
        stop_pct    (float) default 0.0
        tgt_pct     (float) default 0.0
        edge_only   (bool)  default True   # keep as one-shot (recommended)

    Grid config (optional):
        timeframe + params_by_tf supported -> 'orb_minutes' if you drive it that way.
    """
    name = "ORB_BRK"

    _DEFAULTS = dict(
        orb_minutes=15,
        start_h=9,
        start_m=15,
        tz="Asia/Kolkata",
        stop_pct=0.0,
        tgt_pct=0.0,
        edge_only=True,  # ORB usually one-shot per day
    )

    # --- state (per day) ---
    _day_key: Optional[tuple[int, int, int]]
    _orb_high: Optional[float]
    _orb_low: Optional[float]
    _orb_done: bool
    _tz: ZoneInfo

    def __init__(
        self,
        *,
        # direct overrides
        orb_minutes: Optional[int] = None,
        start_h: Optional[int] = None,
        start_m: Optional[int] = None,
        tz: Optional[str] = None,
        stop_pct: Optional[float] = None,
        tgt_pct: Optional[float] = None,
        edge_only: Optional[bool] = None,
        # grid-style
        timeframe: Optional[str] = None,
        params_by_tf: Optional[Dict[str, Dict[str, Any]]] = None,
        # passthrough
        params: Optional[dict] = None,
        **base_kwargs,
    ) -> None:
        merged = dict(self._DEFAULTS)
        if params:
            merged.update(params)

        # merge per-timeframe config if provided
        if params_by_tf and timeframe is not None and timeframe in params_by_tf:
            tf_params = dict(params_by_tf[timeframe])
            if "orb_minutes" not in tf_params:
                if "period" in tf_params:
                    tf_params["orb_minutes"] = tf_params["period"]
            merged.update(tf_params)

        # explicit kwargs override
        if orb_minutes is not None: merged["orb_minutes"] = int(orb_minutes)
        if start_h is not None:     merged["start_h"] = int(start_h)
        if start_m is not None:     merged["start_m"] = int(start_m)
        if tz is not None:          merged["tz"] = str(tz)
        if stop_pct is not None:    merged["stop_pct"] = float(stop_pct)
        if tgt_pct is not None:     merged["tgt_pct"] = float(tgt_pct)
        if edge_only is not None:   merged["edge_only"] = bool(edge_only)

        # normalize
        merged["orb_minutes"] = max(1, int(merged.get("orb_minutes", 15)))
        merged["start_h"] = max(0, min(23, int(merged.get("start_h", 9))))
        merged["start_m"] = max(0, min(59, int(merged.get("start_m", 15))))
        merged["tz"] = str(merged.get("tz", "Asia/Kolkata"))

        super().__init__(params=merged, **base_kwargs)

        # ORB doesn't need global history warmup; we gate by clock window
        self.warmup = 1

        self._tz = ZoneInfo(self.params["tz"])
        self._day_key = None
        self._orb_high = None
        self._orb_low = None
        self._orb_done = False

    # keep external call unchanged
    def on_bar(self, bar: Candle) -> Signal:
        return self.step(bar)

    def on_start(self, symbol: str) -> None:
        # per-session reset
        self._day_key = None
        self._orb_high = None
        self._orb_low = None
        self._orb_done = False

    def on_reset(self) -> None:
        super().on_reset()
        self._day_key = None
        self._orb_high = None
        self._orb_low = None
        self._orb_done = False

    # -------- core ----------
    def _on_bar(self, hist: Tuple[Candle, ...], bar: Candle) -> Signal:
        minutes = int(self.params["orb_minutes"])
        start_h = int(self.params["start_h"])
        start_m = int(self.params["start_m"])
        edge_only = bool(self.params["edge_only"])

        # Local trading day (tz-aware)
        local_dt = bar.ts.astimezone(self._tz)
        dkey = (local_dt.year, local_dt.month, local_dt.day)

        # New day -> reset
        if self._day_key != dkey:
            self._day_key = dkey
            self._orb_high = None
            self._orb_low = None
            self._orb_done = False

        # Opening range window
        start_dt = local_dt.replace(hour=start_h, minute=start_m, second=0, microsecond=0)
        end_dt = start_dt + timedelta(minutes=minutes)

        if start_dt <= local_dt < end_dt:
            # build ORB
            self._orb_high = max(self._orb_high if self._orb_high is not None else bar.h, bar.h)
            self._orb_low = min(self._orb_low if self._orb_low is not None else bar.l, bar.l)
            return Signal.flat("orb-building")

        # If ORB not ready yet (e.g., before start window or missing bars)
        if self._orb_high is None or self._orb_low is None:
            return Signal.flat("orb-not-ready")

        c = bar.c
        stop_pct = float(self.params.get("stop_pct") or 0.0)
        tgt_pct  = float(self.params.get("tgt_pct")  or 0.0)

        if not self._orb_done:
            if c > self._orb_high:
                self._orb_done = True
                stop = (c * (1 - stop_pct)) if stop_pct > 0 else None
                tgt  = (c * (1 + tgt_pct))  if tgt_pct  > 0 else None
                return Signal.buy(confidence=0.8, stop=stop, target=tgt,
                                  reason=f"ORB up (n={minutes})")
            if c < self._orb_low:
                self._orb_done = True
                stop = (c * (1 + stop_pct)) if stop_pct > 0 else None
                tgt  = (c * (1 - tgt_pct))  if tgt_pct  > 0 else None
                return Signal.sell(confidence=0.8, stop=stop, target=tgt,
                                   reason=f"ORB down (n={minutes})")

        # If you want continuous stance after breakout:
        if not edge_only and self._orb_done:
            if c > self._orb_high:
                return Signal.buy(confidence=0.6,
                                  reason=f"above ORB high (n={minutes})")
            if c < self._orb_low:
                return Signal.sell(confidence=0.6,
                                   reason=f"below ORB low (n={minutes})")

        return Signal.flat("no-breakout")
