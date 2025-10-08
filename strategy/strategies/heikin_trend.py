# strategy/strategies/heikin_trend.py
from __future__ import annotations
from typing import Optional, Tuple, Dict, Any
from strategy.base import BaseStrategy, Candle, Signal

__all__ = ["HeikinTrend"]


class HeikinTrend(BaseStrategy):
    """
    Heikin-Ashi trend strategy.

    Behavior:
      - Heikin Close (HC) = (O + H + L + C) / 4
      - Heikin Open  (HO) = prev(HO + HC) / 2   (first bar uses real O, matching your original)
      - If HC > HO -> uptrend, HC < HO -> downtrend, else neutral

    Usage (unchanged):
        strat = HeikinTrend(edge_only=True)
        sig = strat.on_bar(candle)

    Params (self.params):
        edge_only (bool)    default True    # only signal when trend flips
        stop_pct (float)    default 0.0
        tgt_pct  (float)    default 0.0
    """
    name = "HEIKIN_TREND"

    _DEFAULTS = dict(
        edge_only=True,
        stop_pct=0.0,
        tgt_pct=0.0,
    )

    # internal state
    _ho: Optional[float]     # heikin open
    _hc: Optional[float]     # heikin close
    _prev_sign: Optional[int]  # +1 up, -1 down, 0 flat

    def __init__(
        self,
        *,
        edge_only: Optional[bool] = None,
        stop_pct: Optional[float] = None,
        tgt_pct: Optional[float] = None,
        params: Optional[Dict[str, Any]] = None,
        **base_kwargs,
    ) -> None:
        merged = dict(self._DEFAULTS)
        if params:
            merged.update(params)
        if edge_only is not None: merged["edge_only"] = bool(edge_only)
        if stop_pct is not None:  merged["stop_pct"] = float(stop_pct)
        if tgt_pct is not None:   merged["tgt_pct"] = float(tgt_pct)

        super().__init__(params=merged, **base_kwargs)

        self.warmup = 1  # need at least one bar to form initial HO/HC relation
        self._ho = None
        self._hc = None
        self._prev_sign = None

    # keep external call unchanged
    def on_bar(self, bar: Candle) -> Signal:
        return self.step(bar)

    def on_reset(self) -> None:
        super().on_reset()
        self._ho = None
        self._hc = None
        self._prev_sign = None

    # -------- strategy core ----------
    def _on_bar(self, hist: Tuple[Candle, ...], bar: Candle) -> Signal:
        edge_only = bool(self.params["edge_only"])
        c = bar

        # Heikin values
        hc = (c.o + c.h + c.l + c.c) / 4.0
        ho = c.o if self._ho is None else (self._ho + (self._hc if self._hc is not None else hc)) / 2.0

        self._ho, self._hc = ho, hc

        # Determine sign (trend)
        if hc > ho:
            sign = +1
        elif hc < ho:
            sign = -1
        else:
            sign = 0

        # First determination: set baseline
        if self._prev_sign is None:
            self._prev_sign = sign
            return Signal.flat("baseline")

        flipped_up = (self._prev_sign <= 0) and (sign == +1)
        flipped_dn = (self._prev_sign >= 0) and (sign == -1)
        self._prev_sign = sign

        stop_pct = float(self.params.get("stop_pct") or 0.0)
        tgt_pct  = float(self.params.get("tgt_pct")  or 0.0)
        px = c.c
        stop_long = (px * (1 - stop_pct)) if stop_pct > 0 else None
        tgt_long  = (px * (1 + tgt_pct))  if tgt_pct  > 0 else None
        stop_short = (px * (1 + stop_pct)) if stop_pct > 0 else None
        tgt_short  = (px * (1 - tgt_pct))  if tgt_pct  > 0 else None

        if edge_only:
            if flipped_up:
                return Signal.buy(confidence=0.75, stop=stop_long, target=tgt_long, reason="heikin_up")
            if flipped_dn:
                return Signal.sell(confidence=0.75, stop=stop_short, target=tgt_short, reason="heikin_down")
            return Signal.flat("no-flip")

        # Non-edge: continuous stance
        if sign == +1:
            return Signal.buy(confidence=0.6, stop=stop_long, target=tgt_long, reason="heikin_up")
        elif sign == -1:
            return Signal.sell(confidence=0.6, stop=stop_short, target=tgt_short, reason="heikin_down")
        else:
            return Signal.flat("heikin_equal")
