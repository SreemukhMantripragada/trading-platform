# indicators/base.py
from typing import Dict, Any, Sequence
class Indicator:
    name = "BaseIndicator"
    lookback: int = 1           # required bars

    def update(self, prices: Sequence[float]) -> Dict[str, Any]:
        """Return {'ready': bool, '<feature>': value, ...}"""
        raise NotImplementedError
