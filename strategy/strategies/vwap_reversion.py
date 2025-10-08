# strategy/strategies/vwap_reversion.py
from __future__ import annotations
from typing import Optional, Tuple, Dict, Any
from datetime import timezone, timedelta, date

from strategy.base import BaseStrategy, Candle, Signal

__all__ = ["VWAPReversion"]


class VWAPReversion(BaseStrategy):
    """
    Intraday VWAP mean-reversion using z-score of (close - vwap).

    - Resets VWAP and deviation stats at each local day boundary
    - Welford online mean/variance for deviations
    - SELL when z > +z_entry, BUY when z < -z_entry (state-based by default)
    - Set edge_only=True to only emit when crossing into over/under zones

    Usage (unchanged):
        strat = VWAPReversion(z=1.0, min_samples=20, edge_only=False)
        sig = strat.on_bar(candle)

    Params (self.params):
        z (float)                    default 1.0     # z-score entry threshold
        min_samples (int)            default 20      # min intraday samples before signalling
        edge_only (bool)             default False   # one-shot on threshold crossings if True
        tz_offset_minutes (int)      default 330     # local timezone offset minutes (IST=+330)
        stop_pct (float)             default 0.0
        tgt_pct  (float)             default 0.0

    Optional grid config:
        timeframe: "1" | "3" | "5" | ...
        params_by_tf: {"<tf>": { z?: float, min_samples?: int, edge_only?: bool, tz_offset_minutes?: int }}
        (also accepts 'period' if you want to drive min_samples from a period knob)
    """
    name = "VWAP_REVERSION"

    _DEFAULTS = dict(
        z=1.0,
        min_samples=20,
        edge_only=False,
        tz_offset_minutes=330,  # IST by default
        stop_pct=0.0,
        tgt_pct=0.0,
    )

    # --- intraday state ---
    _cpv: float           # cumulative price*volume
    _cvol: float          # cumulative volume
    _mu: Optional[float]  # mean of deviations (close - vwap)
    _s2: Optional[float]  # sum of squared diffs for Welford
    _n: int               # samples in current day
    _cur_day: Optional[date]
    _prev_zone: Optional[int]  # -1 under, 0 inside, +1 over
    _tz: timezone

    def __init__(
        self,
        *,
        z: Optional[float] = None,
        min_samples: Optional[int] = None,
        edge_only: Optional[bool] = None,
        tz_offset_minutes: Optional[int] = None,
        stop_pct: Optional[float] = None,
        tgt_pct: Optional[float] = None,
        timeframe: Optional[str] = None,
        params_by_tf: Optional[Dict[str, Dict[str, Any]]] = None,
        params: Optional[dict] = None,
        **base_kwargs,
    ) -> None:
        merged = dict(self._DEFAULTS)
        if params:
            merged.update(params)

        # merge per-timeframe config if provided
        if params_by_tf and timeframe is not None and timeframe in params_by_tf:
            tf_params = dict(params_by_tf[timeframe])
            # allow driving min_samples via 'period'
            if "min_samples" not in tf_params:
                if "period" in tf_params: tf_params["min_samples"] = tf_params["period"]
            merged.update(tf_params)

        # explicit kwargs override everything
        if z is not None:                   merged["z"] = float(z)
        if min_samples is not None:         merged["min_samples"] = int(min_samples)
        if edge_only is not None:           merged["edge_only"] = bool(edge_only)
        if tz_offset_minutes is not None:   merged["tz_offset_minutes"] = int(tz_offset_minutes)
        if stop_pct is not None:            merged["stop_pct"] = float(stop_pct)
        if tgt_pct is not None:             merged["tgt_pct"] = float(tgt_pct)

        # normalize
        merged["min_samples"] = max(1, int(merged.get("min_samples", 20)))
        merged["z"] = float(merged.get("z", 1.0))
        merged["tz_offset_minutes"] = int(merged.get("tz_offset_minutes", 330))

        super().__init__(params=merged, **base_kwargs)

        self.warmup = 1  # we gate on min_samples per-day instead of global history size

        self._cpv = 0.0
        self._cvol = 0.0
        self._mu = None
        self._s2 = None
        self._n = 0
        self._cur_day = None
        self._prev_zone = None
        self._tz = timezone(timedelta(minutes=int(self.params["tz_offset_minutes"])))

    # keep external call unchanged
    def on_bar(self, bar: Candle) -> Signal:
        return self.step(bar)

    def on_reset(self) -> None:
        super().on_reset()
        self._reset_day(None)
        self._prev_zone = None

    # -------- strategy core ----------
    def _on_bar(self, hist: Tuple[Candle, ...], bar: Candle) -> Signal:
        z_thr = float(self.params["z"])
        min_n = int(self.params["min_samples"])
        edge_only = bool(self.params["edge_only"])

        # day roll based on local time
        d = bar.ts.astimezone(self._tz).date()
        if self._cur_day != d:
            self._reset_day(d)

        # update VWAP accumulators (protect against zero vol)
        v = max(float(bar.vol), 1.0)
        self._cpv += bar.c * v
        self._cvol += v
        vwap = self._cpv / self._cvol if self._cvol > 0 else bar.c

        # deviation and Welford update
        dev = bar.c - vwap
        self._update_meanvar(dev)

        if self._n < min_n:
            return Signal.flat("init")

        # unbiased sample std (n-1) once we have n>=2
        sd = (self._s2 / (self._n - 1)) ** 0.5 if (self._s2 is not None and self._n > 1) else 0.0
        if sd == 0.0:
            return Signal.flat("degenerate-sd")

        z = dev / sd

        # zone relative to ±z_thr
        if z > z_thr:
            zone = +1
        elif z < -z_thr:
            zone = -1
        else:
            zone = 0

        # baseline per day
        if self._prev_zone is None:
            self._prev_zone = zone
            return Signal.flat("baseline")

        crossed_up = (self._prev_zone <= 0) and (zone == +1)   # inside/under -> over
        crossed_dn = (self._prev_zone >= 0) and (zone == -1)   # inside/over  -> under
        self._prev_zone = zone

        # optional stops/targets (reversion bias: SELL when over, BUY when under)
        stop_pct = float(self.params.get("stop_pct") or 0.0)
        tgt_pct  = float(self.params.get("tgt_pct")  or 0.0)
        px = bar.c
        stop_long = (px * (1 - stop_pct)) if stop_pct > 0 else None
        tgt_long  = (px * (1 + tgt_pct))  if tgt_pct  > 0 else None
        stop_short = (px * (1 + stop_pct)) if stop_pct > 0 else None
        tgt_short  = (px * (1 - tgt_pct))  if tgt_pct  > 0 else None

        reason = f"(z={z_thr},min={min_n})"

        if edge_only:
            if crossed_up:
                return Signal.sell(confidence=0.75, stop=stop_short, target=tgt_short, reason=f"vwap_over {reason}")
            if crossed_dn:
                return Signal.buy(confidence=0.75, stop=stop_long, target=tgt_long, reason=f"vwap_under {reason}")
            return Signal.flat("no-cross")

        # state-based (continuous while outside thresholds)
        if zone == +1:
            return Signal.sell(confidence=0.6, stop=stop_short, target=tgt_short, reason=f"vwap_over {reason}")
        elif zone == -1:
            return Signal.buy(confidence=0.6, stop=stop_long, target=tgt_long, reason=f"vwap_under {reason}")
        else:
            return Signal.flat("inside")

    # ---- intraday helpers ----
    def _reset_day(self, day: Optional[date]) -> None:
        self._cpv = 0.0
        self._cvol = 0.0
        self._mu = None
        self._s2 = None
        self._n = 0
        self._cur_day = day
        self._prev_zone = None  # reset edge detector per day

    def _update_meanvar(self, x: float) -> None:
        """Welford's algorithm on the deviation series."""
        self._n += 1
        if self._mu is None:
            self._mu = x
            self._s2 = 0.0
            return
        d = x - self._mu
        self._mu += d / self._n
        # s2 tracks sum of squared diffs from mean
        self._s2 = (self._s2 or 0.0) + d * (x - self._mu)
