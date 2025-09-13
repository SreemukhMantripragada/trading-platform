# libs/signal.py
from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any, Literal

Side = Literal["BUY", "SELL", "EXIT", "NONE"]

@dataclass
class Signal:
    side: Side                 # "BUY" | "SELL" | "EXIT" | "NONE"
    score: float = 1.0         # confidence 0..1
    entry_px: Optional[float] = None
    stop_px: Optional[float] = None
    ttl_sec: Optional[int] = None
    tags: Dict[str, Any] = None   # reason/feature values/etc.

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        if d.get("tags") is None: d["tags"] = {}
        return d

NONE = Signal(side="NONE", score=0.0)
