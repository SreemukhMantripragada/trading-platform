"""
strategy/ensemble_strategy.py
Ensemble that wraps multiple child strategies (already instantiated).
Rules (configurable):
  - mode: "votes" (N of M) or "weighted" (sum(weights*score) >= thresh)
  - agree_side: require all signals to agree on side? (True/False)
"""

from __future__ import annotations
from typing import Dict, Any, List, Tuple
from .base import BaseStrategy, Signal

class EnsembleStrategy(BaseStrategy):
    def __init__(
        self,
        children: List[Tuple[str, BaseStrategy]],  # [(name, obj)]
        mode: str = "votes",
        k: int = 2,
        weights: Dict[str, float] | None = None,
        threshold: float = 1.0,
        agree_side: bool = True,
        **kw
    ):
        super().__init__(name="ENSEMBLE", mode=mode, k=k, threshold=threshold, agree_side=agree_side, **kw)
        self.children = children
        self.mode = mode
        self.k = int(k)
        self.weights = weights or {}
        self.threshold = float(threshold)
        self.agree_side = bool(agree_side)

    def warmup_bars(self, tf: str) -> int:
        return max((c.warmup_bars(tf) for _, c in self.children), default=0)

    def on_bar(self, symbol, tf, ts, o, h, l, c, vol, ctx: Dict[str, Any]) -> Signal:
        sigs = []
        for cname, child in self.children:
            s = child.on_bar(symbol, tf, ts, o, h, l, c, vol, ctx)
            if s.side == "NONE": continue
            sigs.append((cname, s))
        if not sigs:
            return Signal.none("no_child_signal")

        # Optionally require same direction
        if self.agree_side:
            sides = {s.side for _, s in sigs}
            if len(sides) > 1:
                return Signal.none("disagree_side")

        # votes: need at least k signals with the same side
        if self.mode == "votes":
            side = sigs[0][1].side
            agree = sum(1 for _, s in sigs if s.side == side)
            if agree >= self.k:
                strength = min(1.0, agree / max(1, len(self.children)))
                return Signal(side=side, strength=strength, reason=f"votes={agree}/{len(self.children)}")
            return Signal.none(f"votes<{self.k}")

        # weighted: sum(weights * strength) >= threshold
        if self.mode == "weighted":
            side = sigs[0][1].side if self.agree_side else max(
                ("BUY","SELL"), key=lambda sd: sum(self.weights.get(n,1.0)*s.strength for n,s in sigs if s.side==sd)
            )
            score = sum(self.weights.get(n, 1.0) * s.strength for n, s in sigs if s.side == side)
            if score >= self.threshold:
                return Signal(side=side, strength=min(1.0, score), reason=f"weighted={score:.2f}")
            return Signal.none(f"weighted<{self.threshold:.2f}")

        return Signal.none("unknown_mode")
