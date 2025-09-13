"""
Zerodha instrument resolver for order placement.
Expects configs/tokens.csv with at least:
  instrument_token, tradingsymbol, exchange, tick_size, lot_size
If exchange missing, defaults to "NSE".
"""
from __future__ import annotations
import csv

class ZMap:
    def __init__(self, csv_path: str = "configs/tokens.csv"):
        self.by_symbol = {}
        with open(csv_path, newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                sym = (row.get("tradingsymbol") or row.get("symbol") or "").strip()
                if not sym: continue
                self.by_symbol[sym] = {
                    "exchange": (row.get("exchange") or "NSE").strip(),
                    "tradingsymbol": sym,
                    "lot_size": int(row.get("lot_size") or 1),
                    "tick_size": float(row.get("tick_size") or 0.05),
                    "instrument_token": int(row.get("instrument_token") or 0)
                }

    def resolve(self, symbol: str) -> dict | None:
        return self.by_symbol.get(symbol)
