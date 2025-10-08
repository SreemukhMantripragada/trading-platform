# strategy/strategies/keltner_breakout.py
from __future__ import annotations
from typing import Optional, Tuple, Dict, Any
from collections import deque

from strategy.base import BaseStrategy, Candle, Signal

__all__ = ["KeltnerBreakout"]


class KeltnerBreakout(BaseStrategy):
    """
    Keltner Channel breakout strategy.

    Behavior:
      - Center line: EMA(close, ema_period) (SMA-seeded)
      - Channel width: ATR(atr_period) using SMA of True Range
      - BUY if close > center + mult * ATR
      - SELL if close < center - mult * ATR
      - Otherwise FLAT

    Usage (unchanged):
        strat = KeltnerBreakout(ema_period=20, atr_period=14, mult=2.0, edge_only=True)
        sig = strat.on_bar(candle)

    Params (self.params):
        ema_period (int)    default 20      # EMA of close
        atr_period (int)    default 14      # ATR lookback (SMA of TR)
        mult (float)        default 2.0     # channel multiplier
        edge_only (bool)    default True    # one-shot on breakouts
        stop_pct (float)    default 0.0
        tgt_pct  (float)    default 0.0

    Grid config:
        timeframe: "1" | "3" | "5" | ...
        params_by_tf: { "<tf>": { ema_period|period|peiod|ema: int,
                                   atr_period|atr: int,
                                   mult|k: float,
                                   edge_only?: bool } }
    """
    name = "KELTNER_BREAKOUT"

    _DEFAULTS = dict(
        ema_period=20,
        atr_period=14,
        mult=2.0,
        edge_only=True,
        stop_pct=0.0,
        tgt_pct=0.0,
    )

    # State
    _ema: Optional[float]
    _ema_seed: deque[float]     # holds first ema_period closes to seed EMA via SMA
    _alpha: float
    _prev_close: Optional[float]
    _tr_window: deque[float]    # rolling TR values (length atr_period)
    _prev_zone: Optional[int]   # -1 below lower, 0 inside, +1 above upper

    def __init__(
        self,
        *,
        # direct overrides
        ema_period: Optional[int] = None,
        atr_period: Optional[int] = None,
        mult: Optional[float] = None,
        edge_only: Optional[bool] = None,
        stop_pct: Optional[float] = None,
        tgt_pct: Optional[float] = None,
        # grid style
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

            # aliases / typos
            if "ema_period" not in tf_params:
                if "period" in tf_params: tf_params["ema_period"] = tf_params["period"]
                elif "peiod" in tf_params: tf_params["ema_period"] = tf_params["peiod"]
                elif "ema" in tf_params: tf_params["ema_period"] = tf_params["ema"]
            if "atr_period" not in tf_params and "atr" in tf_params:
                tf_params["atr_period"] = tf_params["atr"]
            if "mult" not in tf_params and "k" in tf_params:
                tf_params["mult"] = tf_params["k"]

            merged.update(tf_params)

        # explicit kwargs override
        if ema_period is not None: merged["ema_period"] = int(ema_period)
        if atr_period is not None: merged["atr_period"] = int(atr_period)
        if mult is not None:       merged["mult"] = float(mult)
        if edge_only is not None:  merged["edge_only"] = bool(edge_only)
        if stop_pct is not None:   merged["stop_pct"] = float(stop_pct)
        if tgt_pct is not None:    merged["tgt_pct"] = float(tgt_pct)

        # normalize
        merged["ema_period"] = max(1, int(merged.get("ema_period", 20)))
        merged["atr_period"] = max(1, int(merged.get("atr_period", 14)))
        merged["mult"] = float(merged.get("mult", 2.0))

        super().__init__(params=merged, **base_kwargs)

        ep = int(self.params["ema_period"])
        ap = int(self.params["atr_period"])

        # Warmup: need ep closes for EMA seed; ATR needs ap TRs (i.e., ap+1 closes)
        self.warmup = max(ep, ap + 1)

        self._ema = None
        self._ema_seed = deque(maxlen=ep)
        self._alpha = 2.0 / (ep + 1.0)
        self._prev_close = None
        self._tr_window = deque(maxlen=ap)
        self._prev_zone = None

    # keep external call unchanged
    def on_bar(self, bar: Candle) -> Signal:
        return self.step(bar)

    def on_reset(self) -> None:
        super().on_reset()
        self._ema = None
        self._ema_seed.clear()
        self._prev_close = None
        self._tr_window.clear()
        self._prev_zone = None

    # -------- strategy core ----------
    def _on_bar(self, hist: Tuple[Candle, ...], bar: Candle) -> Signal:
        ep = int(self.params["ema_period"])
        ap = int(self.params["atr_period"])
        mult = float(self.params["mult"])
        edge_only = bool(self.params["edge_only"])

        c, h, l = bar.c, bar.h, bar.l

        # --- EMA(close) with SMA seeding ---
        if self._ema is None:
            self._ema_seed.append(c)
            if len(self._ema_seed) < ep:
                # not enough to seed EMA -> FLAT
                # still update prev_close for TR calc
                self._update_tr(h, l, c)
                return Signal.flat("init-ema-seed")
            if len(self._ema_seed) == ep:
                self._ema = sum(self._ema_seed) / ep
        else:
            self._ema = self._ema + self._alpha * (c - self._ema)

        # --- ATR via SMA of TR over atr_period ---
        self._update_tr(h, l, c)
        if len(self._tr_window) < ap:
            return Signal.flat("init-atr")

        atr = sum(self._tr_window) / ap

        upper = self._ema + mult * atr
        lower = self._ema - mult * atr

        # Determine zone relative to channel
        if c > upper:
            zone = +1
        elif c < lower:
            zone = -1
        else:
            zone = 0

        # First time with full channel: set baseline
        if self._prev_zone is None:
            self._prev_zone = zone
            return Signal.flat("baseline")

        crossed_up = (self._prev_zone <= 0) and (zone == +1)   # inside/below -> breakout up
        crossed_dn = (self._prev_zone >= 0) and (zone == -1)   # inside/above -> breakout down
        self._prev_zone = zone

        stop_pct = float(self.params.get("stop_pct") or 0.0)
        tgt_pct  = float(self.params.get("tgt_pct")  or 0.0)
        stop_long = (c * (1 - stop_pct)) if stop_pct > 0 else None
        tgt_long  = (c * (1 + tgt_pct))  if tgt_pct  > 0 else None
        stop_short = (c * (1 + stop_pct)) if stop_pct > 0 else None
        tgt_short  = (c * (1 - tgt_pct))  if tgt_pct  > 0 else None

        reason = f"(ema={ep},atr={ap},mult={mult})"

        if edge_only:
            if crossed_up:
                return Signal.buy(confidence=0.8, stop=stop_long, target=tgt_long,
                                  reason=f"keltner_up {reason}")
            if crossed_dn:
                return Signal.sell(confidence=0.8, stop=stop_short, target=tgt_short,
                                   reason=f"keltner_down {reason}")
            return Signal.flat("no-cross")

        # Non-edge (state): maintain stance while outside channel
        if zone == +1:
            return Signal.buy(confidence=0.6, stop=stop_long, target=tgt_long,
                              reason=f"above upper {reason}")
        elif zone == -1:
            return Signal.sell(confidence=0.6, stop=stop_short, target=tgt_short,
                               reason=f"below lower {reason}")
        else:
            return Signal.flat("inside")

    # ---- helpers ----
    def _update_tr(self, h: float, l: float, c: float) -> None:
        """
        Update rolling True Range window and previous close.
        TR = max(h - l, |h - prev_close|, |l - prev_close|).
        """
        if self._prev_close is None:
            # can't compute TR on the very first bar; just set prev_close
            self._prev_close = c
            return
        tr = max(h - l, abs(h - self._prev_close), abs(l - self._prev_close))
        self._tr_window.append(tr)
        self._prev_close = c
