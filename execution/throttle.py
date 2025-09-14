"""
execution/throttle.py
Async token-bucket rate limiter for per-symbol and per-bucket throttles.

Usage:
  T = Throttles(sym_qps=2.0, bucket_qps={"LOW":1.0,"MED":1.0,"HIGH":0.5})
  await T.allow(symbol="RELIANCE", bucket="MED")
"""
from __future__ import annotations
import time, asyncio
from collections import defaultdict
from typing import Dict

class TokenBucket:
    def __init__(self, rate_per_sec: float, burst: int = 5):
        self.rate=rate_per_sec; self.capacity=float(burst); self.tokens=float(burst); self.ts=time.monotonic()

    async def take(self):
        while True:
            now=time.monotonic()
            self.tokens=min(self.capacity, self.tokens + (now - self.ts)*self.rate)
            self.ts=now
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return
            await asyncio.sleep(max(0.01, (1.0 - self.tokens)/max(self.rate,1e-6)))

class Throttles:
    def __init__(self, sym_qps: float = 2.0, bucket_qps: Dict[str,float] | None = None):
        self.sym_rate=float(sym_qps)
        self.bucket_rate={k:float(v) for k,v in (bucket_qps or {}).items()}
        self._sym=defaultdict(lambda: TokenBucket(self.sym_rate, burst=5))
        self._buck={k: TokenBucket(v, burst=5) for k,v in self.bucket_rate.items()}

    async def allow(self, symbol: str, bucket: str):
        await self._sym[symbol].take()
        if bucket in self._buck:
            await self._buck[bucket].take()
