from __future__ import annotations
from typing import Optional, Tuple
from strategy.base import BaseStrategy, Candle, Signal

class EMACross(BaseStrategy):
    """
    EMA crossover strategy (edge-triggered by default):
    
    Usage:
        strat = EMACross(fast=10, slow=30, stop_pct=0.01, tgt_pct=0.02)
        sig = strategy.on_bar(candle)

    Params:
        fast (int)       default 12
        slow (int)       default 50
        stop_pct (float) default 0.005   # 0.5%
        tgt_pct (float)  default 0.012   # 1.2%
        edge_only (bool) default True    # emit only on cross events
    """
    name = "EMA_CROSS"
    # class-level defaults; instance may override via __init__ kwargs
    _DEFAULTS = dict(fast=12, slow=50, stop_pct=0.005, tgt_pct=0.012, edge_only=True)
    warmup = 60  # will be reset in __init__ to slow

    # internal EMA state
    _ema_fast: Optional[float]
    _ema_slow: Optional[float]
    _prev_diff: Optional[float]

    def __init__(
            self,
            *, 
            fast: Optional[int] = None, 
            slow: Optional[int] = None,
            stop_pct: Optional[float] = None, 
            tgt_pct: Optional[float] = None,
            edge_only: Optional[bool] = None, 
            **base_kwargs
        ) -> None:
        # Merge class defaults + user overrides into params dict expected by BaseStrategy
        merged = dict(self._DEFAULTS)
        if fast is not None:      merged["fast"] = int(fast)
        if slow is not None:      merged["slow"] = int(slow)
        if stop_pct is not None:  merged["stop_pct"] = float(stop_pct)
        if tgt_pct is not None:   merged["tgt_pct"] = float(tgt_pct)
        if edge_only is not None: merged["edge_only"] = bool(edge_only)

        super().__init__(params=merged, **base_kwargs)

        # Warmup: Enough bars to seed the longer EMA
        if slow is not None: 
            self.warmup = slow
        else:
            self.warmup = int(self._DEFAULTS["slow"])

        # init internal state
        self._ema_fast = None
        self._ema_slow = None
        self._prev_diff = None

    
    def on_bar(self, bar: Candle) -> Signal:
        """
        External interaction is the same
        """
        return self.step(bar)

    # ---- Reset state when strategy resets ----
    def on_reset(self) -> None:
        # Call parent's on_reset method.
        super().on_reset()
        self._ema_fast = None
        self._ema_slow = None
        self._prev_diff = None

    # ---- Strategy core  ----
    def _on_bar(self, hist: Tuple[Candle, ...], bar: Candle) -> Signal:
        f = int(self.params["fast"])
        s = int(self.params["slow"])
        edge_only: bool = bool(self.params.get("edge_only", True))
        c = bar.c

        # Initialize EMAs using SMA seeds when enough bars exist
        closes = tuple(b.c for b in hist)
        if self._ema_fast is None and len(closes) >= f:
            self._ema_fast = sum(closes[-f:]) / f
        if self._ema_slow is None and len(closes) >= s:
            self._ema_slow = sum(closes[-s:]) / s

        # If either EMA still missing (first warmup bar), stay flat
        if self._ema_fast is None or self._ema_slow is None:
            if self._ema_fast is not None and self._ema_slow is not None and self._prev_diff is None:
                self._prev_diff = self._ema_fast - self._ema_slow
            return Signal.flat("Init")

        # Update EMAs with current close
        self._ema_fast = self._update_ema(self._ema_fast, c, f)
        self._ema_slow = self._update_ema(self._ema_slow, c, s)
        diff = self._ema_fast - self._ema_slow

        # First tick after both EMAs exist: set baseline diff
        if self._prev_diff is None:
            self._prev_diff = diff
            return Signal.flat("Baseline")

        crossed_up = self._prev_diff <= 0.0 and diff > 0.0
        crossed_down = self._prev_diff >= 0.0 and diff < 0.0
        self._prev_diff = diff  

        stop_pct = self.params.get("stop_pct")
        tgt_pct  = self.params.get("tgt_pct")

        if edge_only:
            if crossed_up:
                return Signal.buy(
                    confidence=0.8,
                    stop=(c * (1 - stop_pct)) if stop_pct else None,
                    target=(c * (1 + tgt_pct)) if tgt_pct else None,
                    reason=f"ema{f}↑ema{s}",
                )
            if crossed_down:
                return Signal.sell(
                    confidence=0.8,
                    stop=(c * (1 + stop_pct)) if stop_pct else None,
                    target=(c * (1 - tgt_pct)) if tgt_pct else None,
                    reason=f"ema{f}↓ema{s}",
                )
            return Signal.flat("no-cross")

        # Non-edge: emit while in-state
        if diff > 0.0:
            return Signal.buy(
                confidence=0.6,
                stop=(c * (1 - stop_pct)) if stop_pct else None,
                target=(c * (1 + tgt_pct)) if tgt_pct else None,
                reason=f"ema{f}>ema{s}",
            )
        elif diff < 0.0:
            return Signal.sell(
                confidence=0.6,
                stop=(c * (1 + stop_pct)) if stop_pct else None,
                target=(c * (1 - tgt_pct)) if tgt_pct else None,
                reason=f"ema{f}<ema{s}",
            )
        else:
            return Signal.flat("equal")

    @staticmethod
    def _update_ema(prev: float, price: float, n: int) -> float:
        alpha = 2.0 / (n + 1.0)
        return prev + alpha * (price - prev)
