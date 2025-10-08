from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, Deque, Protocol, runtime_checkable, Generic, TypeVar, Tuple
from collections import deque
from datetime import datetime, timezone
from abc import ABC, abstractmethod
from enum import Enum
import logging

# ---------- Core types ----------
class Action(str, Enum):
    """
    Action class creates a enum for consistency throughout.
    Inherits from both string and Enum classes. The benefit of inheriting from str:Action is that we get a string-based enumeration for consistency, making its values easily usable as strings
    """
    BUY = "BUY"
    SELL = "SELL"
    FLAT = "FLAT"

@dataclass(slots=True, frozen=True)
class Candle:
    """
    Candle class to store a single candle stick data.

    Attributes:
    1. Open, High, Close, Low and Volume traded. 
    2. Timezone aware timestamp(ts)
    3. Traded Symbol

    __post__init() function executes right after __init__() is called.
    Used here to validate the timestamp is timezone aware.
    """
    symbol: str
    ts: datetime
    o: float
    h: float
    l: float
    c: float
    vol: float

    def __post_init__(self) -> None:
        # validation-only (no mutation; dataclass is frozen)
        if self.ts.tzinfo is None or self.ts.tzinfo.utcoffset(self.ts) is None:
            raise ValueError("Candle.ts must be timezone-aware (UTC recommended).")

@dataclass(slots=True, frozen=True)
class Signal:
    """
    Signal Class uses Action enum to create and return signal objects.

    Attributes:
    1. Action Enum
    2. Confidence of the signal range [0, 1]
    3. Stoploss, Target and Reason(Intent of the signal)

    __post__init() verifies if the confidence is within range.

    @staticmethod decorator is used to define the static methods=> These functions donot belong to any instance or an object of this class. They donot have a compulsory self keyword as an argument.
    They cannot alter the object's state(variables).
    Here: Using them as convinience factories that return flat, buy and sell Signal objects
    """
    action: Action
    confidence: float = 1.0
    stop: Optional[float] = None
    target: Optional[float] = None
    reason: str = ""

    def __post_init__(self) -> None:
        if not (0.0 <= self.confidence <= 1.0):
            raise ValueError("Signal.confidence must be within [0, 1].")

    # Convenience factories
    @staticmethod
    def flat(reason: str = "", confidence: float = 1.0) -> "Signal":
        return Signal(Action.FLAT, confidence=confidence, reason=reason)

    @staticmethod
    def buy(confidence: float = 1.0, *, stop: Optional[float] = None,
            target: Optional[float] = None, reason: str = "") -> "Signal":
        return Signal(Action.BUY, confidence=confidence, stop=stop, target=target, reason=reason)

    @staticmethod
    def sell(confidence: float = 1.0, *, stop: Optional[float] = None,
             target: Optional[float] = None, reason: str = "") -> "Signal":
        return Signal(Action.SELL, confidence=confidence, stop=stop, target=target, reason=reason)


@runtime_checkable
class HistoryStore(Protocol):
    """
    Contract for a historical data store of candlestick data.
    A mandatory interface(abstract class) that provides no implementation details.
    @runtime_checkable decorator checks during runtime regarding the validation of the instances that extend from this class.
    The ... (ellipsis) denote that there is no function definition shared here. Similar to pass keyword.
    """
    def append(self, bar: Candle) -> None: ...
    def size(self) -> int: ...
    def snapshot(self) -> Tuple[Candle, ...]: ...
    def clear(self) -> None: ...
    def tail(self, n: int) -> Tuple[Candle, ...]: ...

class DequeHistory(HistoryStore):
    """
    Fast, memory-bounded Candle store. snapshot/tail return immutable tuples.
    Maxlength of the queue defaults to 2000.
    """
    __slots__ = ("_dq",)

    def __init__(self, maxlen: int = 2000) -> None:
        self._dq: Deque[Candle] = deque(maxlen=maxlen)

    def append(self, bar: Candle) -> None:
        self._dq.append(bar)

    def size(self) -> int:
        return len(self._dq)

    def snapshot(self) -> Tuple[Candle, ...]:
        # Immutable view prevents accidental external mutation
        return tuple(self._dq)

    def clear(self) -> None:
        self._dq.clear()

    def tail(self, n: int) -> Tuple[Candle, ...]:
        if n <= 0:
            return ()
        if n >= len(self._dq):
            return tuple(self._dq)
        # Convert once for slicing; still O(n) but minimal allocation
        return tuple(list(self._dq)[-n:])


class BaseStrategy(ABC):
    """
    Minimal base for candle stick based strategies.
    """
    name: str = "Base"
    warmup: int = 0  # bars required before signals are considered valid

    def __init__(
        self,
        *,
        params: Optional[Dict[str, Any]] = None,
        history: Optional[HistoryStore] = None,
        logger: Optional[logging.Logger] = None,
        enforce_monotonic_ts: bool = True,
    ) -> None:
        self.params: Dict[str, Any] = dict(params or {})
        self.history: HistoryStore = history or DequeHistory()
        self.log = logger or logging.getLogger(self.__class__.__name__)
        self._last_ts: Optional[datetime] = None
        self._enforce_monotonic_ts = enforce_monotonic_ts

    # ----- lifecycle -----
    def on_start(self, symbol: str) -> None:
        pass

    def on_end(self, symbol: str) -> None:
        pass

    def on_reset(self) -> None:
        self.history.clear()
        self._last_ts = None

    def on_error(self, exc: Exception) -> None:
        # keep helpful logging but no extra state
        self.log.exception(f"Strategy error in {self.name}: {exc}")

    # ----- public step (simple, safe path) -----
    def step(self, bar: Candle) -> Signal:
        """Ingest one bar and return a Signal."""
        try:
            self._ingest(bar)
            if self.ready():
                sig = self._on_bar(self.hist(), bar)  # subclass hook
                return sig if isinstance(sig, Signal) else Signal.flat("invalid return coerced")
            return Signal.flat("Warmup")
        except Exception as exc:
            self.on_error(exc)
            return Signal.flat("Error")

    # ----- subclass hook -----
    @abstractmethod
    def _on_bar(self, hist: Tuple[Candle, ...], bar: Candle) -> Signal:
        """Implement strategy logic. 'hist' is an immutable tuple of Candles."""

    # ----- helpers -----
    def _ingest(self, bar: Candle) -> None:
        # Check if the timestamp of the new candle is less than the previous candle. 
        if self._enforce_monotonic_ts and self._last_ts is not None and bar.ts <= self._last_ts:
            raise ValueError(f"Non-monotonic bar timestamp: {bar.ts} <= {self._last_ts}")
        self.history.append(bar)
        self._last_ts = bar.ts

    def hist(self) -> Tuple[Candle, ...]:
        return self.history.snapshot()

    def size(self) -> int:
        return self.history.size()

    def ready(self) -> bool:
        return self.size() >= self.warmup

    def get_param(self, key: str, default: Any = None) -> Any:
        return self.params.get(key, default)
