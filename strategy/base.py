from typing import Dict, Any
from libs.signal import Signal, NONE

class Strategy:
    name = "BaseStrategy"
    warmup_bars: int = 0
    provides_stop: bool = False
    default_bucket: str = "LOW"

    def on_bar(self, symbol: str, tf: str, ts: int,
               o: float, h: float, l: float, c: float, vol: int,
               feats: Dict[str, Any]) -> Signal:
        """Return a Signal; do NOT do sizing here."""
        return NONE
