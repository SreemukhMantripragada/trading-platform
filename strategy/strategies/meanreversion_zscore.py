# strategy/strategies/mean_reversion_zscore.py
from __future__ import annotations
from typing import Optional, Tuple, Dict, Any
from collections import deque
import math

from strategy.base import BaseStrategy, Candle, Signal

__all__ = ["MeanReversionZScore"]


class MeanReversionZScore(BaseStrategy):
    """
    Z-score mean reversion on close.

    Behavior (state-based by default, matching your original):
      - Maintain rolling window of closes (length = period)
      - Compute mean and population std over the window
      - z = (close - mean) / std
      - SELL when z > z_entry (over), BUY when z < -z_entry (under), else FLAT

    Usage (unchanged):
        strat = MeanReversionZScore(period=20, z_entry=1.5, edge_only=False)
        sig = strat.on_bar(candle)

    Params (self.params):
        period   (int)   default 20
        z_entry  (float) default 1.5
        edge_only(bool)  default False   # True: emit only on threshold crossings
        stop_pct (float) default 0.0
        tgt_pct  (float) default 0.0

    Grid config support:
        timeframe: "1" | "3" | "5" | "15" | ...
        params_by_tf: { "<tf>": { period: int, z_entry?: float, edge_only?: bool } }
    """
    name = "MEAN_REVERSION_ZSCORE"

    _DEFAULTS = dict(
        period=20,
        z_entry=1.5,
        edge_only=False,  # continuous stance while outside thresholds (like your original)
        stop_pct=0.0,
        tgt_pct=0.0,
    )

    # internal rolling window & prior zone:
    # zone: -1 = below -z, 0 = inside [-z,+z], +1 = above +z
    _win: deque[float]
    _prev_zone: Optional[int]

    def __init__(
        self,
        *,
        # direct overrides
        period: Optional[int] = None,
        z_entry: Optional[float] = None,
        edge_only: Optional[bool] = None,
        stop_pct: Optional[float] = None,
        tgt_pct: Optional[float] = None,
        # grid style
        timeframe: Optional[str] = None,
        params_by_tf: Optional[Dict[str, Dict[str, Any]]] = None,
        # passthrough dict
        params: Optional[dict] = None,
        **base_kwargs,
    ) -> None:
        merged = dict(self._DEFAULTS)
        if params:
            merged.update(params)

        if params_by_tf and timeframe is not None and timeframe in params_by_tf:
            tf_params = dict(params_by_tf[timeframe])
            merged.update(tf_params)

        if period is not None:     merged["period"] = int(period)
        if z_entry is not None:    merged["z_entry"] = float(z_entry)
        if edge_only is not None:  merged["edge_only"] = bool(edge_only)
        if stop_pct is not None:   merged["stop_pct"] = float(stop_pct)
        if tgt_pct is not None:    merged["tgt_pct"] = float(tgt_pct)

        # normalize
        merged["period"] = max(1, int(merged.get("period", 20)))
        merged["z_entry"] = float(merged.get("z_entry", 1.5))

        super().__init__(params=merged, **base_kwargs)

        n = int(self.params["period"])
        self.warmup = n
        self._win = deque(maxlen=n)
        self._prev_zone = None

    # keep external call unchanged
    def on_bar(self, bar: Candle) -> Signal:
        return self.step(bar)

    def on_reset(self) -> None:
        super().on_reset()
        self._win.clear()
        self._prev_zone = None

    # -------- strategy core ----------
    def _on_bar(self, hist: Tuple[Candle, ...], bar: Candle) -> Signal:
        n = int(self.params["period"])
        z_entry = float(self.params["z_entry"])
        edge_only = bool(self.params["edge_only"])

        self._win.append(bar.c)
        if len(self._win) < n:
            return Signal.flat("init")

        # mean & population std over window
        xs = self._win
        m = sum(xs) / n
        var = sum((x - m) * (x - m) for x in xs) / n
        sd = math.sqrt(var)
        if sd == 0.0:
            return Signal.flat("degenerate-sd")

        z = (bar.c - m) / sd

        # zone relative to +/- z_entry
        if z > z_entry:
            zone = +1
        elif z < -z_entry:
            zone = -1
        else:
            zone = 0

        # baseline on first full window
        if self._prev_zone is None:
            self._prev_zone = zone
            return Signal.flat("baseline")

        crossed_up = (self._prev_zone <= 0) and (zone == +1)  # inside/under -> overbought
        crossed_dn = (self._prev_zone >= 0) and (zone == -1)  # inside/over -> oversold
        self._prev_zone = zone

        # optional risk targets
        stop_pct = float(self.params.get("stop_pct") or 0.0)
        tgt_pct  = float(self.params.get("tgt_pct")  or 0.0)
        px = bar.c
        stop_long = (px * (1 - stop_pct)) if stop_pct > 0 else None
        tgt_long  = (px * (1 + tgt_pct))  if tgt_pct  > 0 else None
        stop_short = (px * (1 + stop_pct)) if stop_pct > 0 else None
        tgt_short  = (px * (1 - tgt_pct))  if tgt_pct  > 0 else None

        reason = f"(n={n},z={z_entry:.3f})"

        if edge_only:
            # mean reversion: SELL when entering overbought; BUY when entering oversold
            if crossed_up:
                return Signal.sell(confidence=0.75, stop=stop_short, target=tgt_short,
                                   reason=f"z_over {reason}")
            if crossed_dn:
                return Signal.buy(confidence=0.75, stop=stop_long, target=tgt_long,
                                  reason=f"z_under {reason}")
            return Signal.flat("no-cross")

        # state-based (continuous while outside thresholds)
        if zone == +1:
            return Signal.sell(confidence=0.6, stop=stop_short, target=tgt_short,
                               reason=f"z_over {reason}")
        elif zone == -1:
            return Signal.buy(confidence=0.6, stop=stop_long, target=tgt_long,
                              reason=f"z_under {reason}")
        else:
            return Signal.flat("inside")
