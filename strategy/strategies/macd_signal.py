from __future__ import annotations
from typing import Optional, Tuple, List
from strategy.base import BaseStrategy, Candle, Signal, Action

class MACD(BaseStrategy):
    """
    MACD crossover strategy (edge-triggered by default).

    Usage:
        strat = MACD(fast=12, slow=26, signal=9, stop_pct=0.006, tgt_pct=0.015)
        sig = strat.on_bar(candle) 

    Params:
        fast (int)        default 12    # EMA fast period
        slow (int)        default 26    # EMA slow period
        signal (int)      default 9     # EMA of MACD line
        stop_pct (float)  default 0.006
        tgt_pct  (float)  default 0.015
        edge_only (bool)  default True  # emit only on MACD-vs-signal crosses
    """
    name = "MACD_SIGNAL"
    _DEFAULTS = dict(fast=12, slow=26, signal=9,
                     stop_pct=0.006, tgt_pct=0.015,
                     edge_only=True)

    # internal state
    _ema_fast: Optional[float]
    _ema_slow: Optional[float]
    _signal: Optional[float]         
    _macd_seed: List[float]         
    _prev_hist: Optional[float]  

    def __init__(
            self, *,
            fast: Optional[int] = None,
            slow: Optional[int] = None,
            signal: Optional[int] = None,
            stop_pct: Optional[float] = None,
            tgt_pct: Optional[float] = None,
            edge_only: Optional[bool] = None,
            params: Optional[dict] = None,
            **base_kwargs
        ) -> None:
        merged = dict(self._DEFAULTS)
        if params:           merged.update(params)
        if fast is not None: merged["fast"] = int(fast)
        if slow is not None: merged["slow"] = int(slow)
        if signal is not None: merged["signal"] = int(signal)
        if stop_pct is not None: merged["stop_pct"] = float(stop_pct)
        if tgt_pct is not None:  merged["tgt_pct"] = float(tgt_pct)
        if edge_only is not None: merged["edge_only"] = bool(edge_only)

        super().__init__(params=merged, **base_kwargs)

        f, s, sig = int(merged["fast"]), int(merged["slow"]), int(merged["signal"])
        # Warmup: enough to seed slow EMA and then seed signal EMA from MACD values
        self.warmup = s + sig

        self._ema_fast = None
        self._ema_slow = None
        self._signal = None
        self._macd_seed = []
        self._prev_hist = None

    # keep external interaction the same
    def on_bar(self, bar: Candle) -> Signal:
        return self.step(bar)

    def on_reset(self) -> None:
        super().on_reset()
        self._ema_fast = None
        self._ema_slow = None
        self._signal = None
        self._macd_seed = []
        self._prev_hist = None

    def _on_bar(self, hist: Tuple[Candle, ...], bar: Candle) -> Signal:
        f = int(self.params["fast"])
        s = int(self.params["slow"])
        sig_n = int(self.params["signal"])
        edge_only: bool = bool(self.params.get("edge_only", True))
        c = bar.c

        closes = tuple(b.c for b in hist)
        # Seed price EMAs via SMA once enough closes exist
        if self._ema_fast is None and len(closes) >= f:
            self._ema_fast = sum(closes[-f:]) / f
        if self._ema_slow is None and len(closes) >= s:
            self._ema_slow = sum(closes[-s:]) / s

        # Not ready to form MACD yet
        if self._ema_fast is None or self._ema_slow is None:
            return Signal.flat("Init")

        # Update price EMAs with current close, then compute MACD value
        self._ema_fast = self._update_ema(self._ema_fast, c, f)
        self._ema_slow = self._update_ema(self._ema_slow, c, s)
        macd_val = self._ema_fast - self._ema_slow

        # Seed signal EMA (EMA of MACD) via SMA of first 'sig_n' macd values
        if self._signal is None:
            self._macd_seed.append(macd_val)
            if len(self._macd_seed) < sig_n:
                return Signal.flat("init-signal-seed")
            if len(self._macd_seed) == sig_n:
                self._signal = sum(self._macd_seed) / sig_n
                hist_now = macd_val - self._signal
                self._prev_hist = hist_now
                return Signal.flat("baseline")
        
        # Normal updates
        self._signal = self._update_ema(self._signal, macd_val, sig_n)
        hist_now = macd_val - self._signal  # MACD histogram

        # First tick after signal exists: just set prev and continue
        if self._prev_hist is None:
            self._prev_hist = hist_now
            return Signal.flat("baseline")

        crossed_up = self._prev_hist <= 0.0 and hist_now > 0.0   # MACD crossed above signal
        crossed_down = self._prev_hist >= 0.0 and hist_now < 0.0   # MACD crossed below signal
        self._prev_hist = hist_now

        stop_pct = self.params.get("stop_pct")
        tgt_pct  = self.params.get("tgt_pct")

        if edge_only:
            if crossed_up:
                return Signal.buy(
                    confidence=0.8,
                    stop=(c * (1 - stop_pct)) if stop_pct else None,
                    target=(c * (1 + tgt_pct)) if tgt_pct else None,
                    reason=f"MACD↑signal ({f},{s},{sig_n})",
                )
            if crossed_down:
                return Signal.sell(
                    confidence=0.8,
                    stop=(c * (1 + stop_pct)) if stop_pct else None,
                    target=(c * (1 - tgt_pct)) if tgt_pct else None,
                    reason=f"MACD↓signal ({f},{s},{sig_n})",
                )
            return Signal.flat("no-cross")

        # Non-edge: emit while in-state
        if hist_now > 0.0:
            return Signal.buy(
                confidence=0.6,
                stop=(c * (1 - stop_pct)) if stop_pct else None,
                target=(c * (1 + tgt_pct)) if tgt_pct else None,
                reason=f"MACD>signal ({f},{s},{sig_n})",
            )
        elif hist_now < 0.0:
            return Signal.sell(
                confidence=0.6,
                stop=(c * (1 + stop_pct)) if stop_pct else None,
                target=(c * (1 - tgt_pct)) if tgt_pct else None,
                reason=f"MACD<signal ({f},{s},{sig_n})",
            )
        else:
            return Signal.flat("equal")

    @staticmethod
    def _update_ema(prev: float, value: float, n: int) -> float:
        alpha = 2.0 / (n + 1.0)
        return prev + alpha * (value - prev)
