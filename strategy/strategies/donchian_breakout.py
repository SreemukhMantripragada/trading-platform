# strategy/strategies/donchian_breakout.py
from __future__ import annotations
from typing import Optional, Tuple, Dict, Any
from collections import deque

from strategy.base import BaseStrategy, Candle, Signal

__all__ = ["DonchianBreakout"]


class DonchianBreakout(BaseStrategy):
    """
    Donchian channel breakout strategy.

    Behavior:
      - Compute rolling highest-high and lowest-low over `period` bars.
      - BUY when close breaks >= highest-high.
      - SELL when close breaks <= lowest-low.
      - Otherwise FLAT.

    Usage (unchanged):
        strat = DonchianBreakout(period=20, edge_only=True)
        sig = strat.on_bar(candle)

    Params (self.params):
        period (int)        default 20
        edge_only (bool)    default True     # one-shot on breakout edges
        stop_pct (float)    default 0.0
        tgt_pct  (float)    default 0.0

    Grid config:
        timeframe: "1" | "3" | "5" | ...
        params_by_tf: { "<tf>": { period: int, edge_only?: bool } }
    """
    name = "DONCHIAN_BREAKOUT"

    _DEFAULTS = dict(
        period=20,
        edge_only=True,
        stop_pct=0.0,
        tgt_pct=0.0,
    )

    # internal rolling windows and prior zone
    # zone: -1 = below LL, 0 = inside channel, +1 = above HH
    _highs: deque[float]
    _lows: deque[float]
    _prev_zone: Optional[int]

    def __init__(
        self,
        *,
        # direct overrides
        period: Optional[int] = None,
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
        if edge_only is not None:  merged["edge_only"] = bool(edge_only)
        if stop_pct is not None:   merged["stop_pct"] = float(stop_pct)
        if tgt_pct is not None:    merged["tgt_pct"] = float(tgt_pct)

        # normalize
        merged["period"] = max(1, int(merged.get("period", 20)))

        super().__init__(params=merged, **base_kwargs)

        n = int(self.params["period"])
        self.warmup = n
        self._highs = deque(maxlen=n)
        self._lows = deque(maxlen=n)
        self._prev_zone = None

    # keep external call unchanged
    def on_bar(self, bar: Candle) -> Signal:
        return self.step(bar)

    def on_reset(self) -> None:
        super().on_reset()
        self._highs.clear()
        self._lows.clear()
        self._prev_zone = None

    # -------- strategy core ----------
    def _on_bar(self, hist: Tuple[Candle, ...], bar: Candle) -> Signal:
        n = int(self.params["period"])
        edge_only = bool(self.params["edge_only"])

        # update rolling windows
        self._highs.append(bar.h)
        self._lows.append(bar.l)

        if len(self._highs) < n:
            return Signal.flat("init")

        hh = max(self._highs)
        ll = min(self._lows)
        c_now = bar.c

        # determine current zone relative to channel
        if c_now >= hh:
            zone = +1
        elif c_now <= ll:
            zone = -1
        else:
            zone = 0

        # baseline on first full channel
        if self._prev_zone is None:
            self._prev_zone = zone
            return Signal.flat("baseline")

        crossed_up = (self._prev_zone <= 0) and (zone == +1)   # inside/below -> breakout up
        crossed_dn = (self._prev_zone >= 0) and (zone == -1)   # inside/above -> breakout down
        self._prev_zone = zone

        # optional stops/targets
        stop_pct = float(self.params.get("stop_pct") or 0.0)
        tgt_pct  = float(self.params.get("tgt_pct")  or 0.0)
        stop_long = (c_now * (1 - stop_pct)) if stop_pct > 0 else None
        tgt_long  = (c_now * (1 + tgt_pct))  if tgt_pct  > 0 else None
        stop_short = (c_now * (1 + stop_pct)) if stop_pct > 0 else None
        tgt_short  = (c_now * (1 - tgt_pct))  if tgt_pct  > 0 else None

        reason = f"(n={n})"

        if edge_only:
            if crossed_up:
                return Signal.buy(confidence=0.8, stop=stop_long, target=tgt_long,
                                  reason=f"donchian_up {reason}")
            if crossed_dn:
                return Signal.sell(confidence=0.8, stop=stop_short, target=tgt_short,
                                   reason=f"donchian_down {reason}")
            return Signal.flat("no-cross")

        # Non-edge (state): keep stance while outside the channel
        if zone == +1:
            return Signal.buy(confidence=0.6, stop=stop_long, target=tgt_long,
                              reason=f"above HH {reason}")
        elif zone == -1:
            return Signal.sell(confidence=0.6, stop=stop_short, target=tgt_short,
                               reason=f"below LL {reason}")
        else:
            return Signal.flat("inside")
