"""
execution/matcher_client.py
- Sends orders to C++ matcher over TCP (JSON lines).
- Receives fill events (JSON per line).

Protocol (simple demo):
  send: {"op":"NEW","coid":"...","symbol":"RELIANCE","side":"BUY","qty":100}
  recv: {"coid":"...","ts":<epoch>, "symbol":"RELIANCE","side":"BUY","qty":100,"price":<fill>}

If matcher not reachable, caller should fallback to immediate paper fill.
"""
from __future__ import annotations
import asyncio, ujson as json
from typing import AsyncIterator, Dict, Any, Optional

class MatcherClient:
    def __init__(self, host:str="127.0.0.1", port:int=5556, timeout:float=1.0):
        self.host=host; self.port=port; self.timeout=timeout
        self._r=None; self._w=None

    async def connect(self) -> None:
        self._r, self._w = await asyncio.wait_for(asyncio.open_connection(self.host, self.port), timeout=self.timeout)

    async def close(self) -> None:
        try:
            if self._w: self._w.close(); await self._w.wait_closed()
        finally:
            self._r=self._w=None

    async def send_order(self, coid:str, symbol:str, side:str, qty:int, price:Optional[float]=None) -> None:
        msg={"op":"NEW","coid":coid,"symbol":symbol,"side":side,"qty":int(qty)}
        if price is not None: msg["price"]=float(price)
        self._w.write((json.dumps(msg)+"\n").encode()); await self._w.drain()

    async def recv_fills(self) -> AsyncIterator[Dict[str,Any]]:
        while True:
            line = await self._r.readline()
            if not line: break
            yield json.loads(line.decode())
