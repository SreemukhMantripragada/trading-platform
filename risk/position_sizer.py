"""
risk/position_sizer.py
Given (entry_px, stop_px, bucket, product, symbol) => qty, notional.
- Uses budgets from risk/budget_source.compute_budgets()
- Risk per trade: either pct of bucket budget or absolute INR
- For F&O (NFO): enforce lot size; cap by bucket budget and optional margin hint.

Call size_order(...) from the runner before emitting an order.
"""
from __future__ import annotations
import math, os, asyncpg
from typing import Optional, Tuple, Dict, Any
from risk.budget_source import compute_budgets

PG_HOST=os.getenv("POSTGRES_HOST","localhost"); PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading"); PG_USER=os.getenv("POSTGRES_USER","trader"); PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")

async def _lot_size(symbol:str)->int:
    """
    Lookup NFO lot size if present; default to 1 for equities.
    """
    if "-" not in symbol and symbol.isalpha():
        return 1  # cash equity
    pool = await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS, min_size=1, max_size=1)
    try:
        async with pool.acquire() as con:
            r = await con.fetchrow("SELECT lot_size FROM instruments_nfo WHERE tradingsymbol=$1", symbol)
            return int((r or {}).get("lot_size") or 1)
    finally:
        await pool.close()

def _risk_qty(entry:float, stop:Optional[float], risk_inr:float)->int:
    """
    Position size by risk: Risk INR / (|entry-stop|). If no stop, use notional-only later.
    """
    if not stop or stop <= 0 or stop == entry:
        return 0
    per_share = abs(entry - stop)
    if per_share <= 1e-8:
        return 0
    return max(0, int(math.floor(risk_inr / per_share)))

async def size_order(
    symbol:str,
    side:str,
    entry_px:float,
    stop_px: Optional[float],
    bucket:str="MED",
    product:str="CNC",
    risk_pct_per_trade:float=0.01,     # 1% of bucket budget
    risk_abs_inr:Optional[float]=None, # if provided, overrides pct
    leverage_hint:float=5.0,           # day leverage for equities (Zerodha MIS ~ 5x typical)
) -> Tuple[int, float, Dict[str, Any]]:
    """
    Returns: (qty, notional_inr, meta)
      meta includes {"cap_budget":..., "risk_used":..., "lot_size":..., "why": "..."}
    """
    budgets = compute_budgets()
    bspec   = budgets.get(bucket.upper(), {})
    cap     = float(bspec.get("budget", 0.0))
    max_per = float(bspec.get("max_per_trade", cap))

    # Risk allocation per trade
    risk_inr = float(risk_abs_inr) if risk_abs_inr is not None else max(0.0, cap * float(risk_pct_per_trade))

    # Risk-based qty (if stop given)
    qty_risk = _risk_qty(entry_px, stop_px, risk_inr)

    # Notional caps
    notional_cap = min(cap, max_per)
    # Leverage-cap notional if intraday leveraged product (e.g., MIS)
    if product in ("MIS","INTRADAY"):
        notional_cap = min(notional_cap * leverage_hint, notional_cap)

    # Raw qty from notional cap
    qty_notional = int(math.floor(notional_cap / max(entry_px, 1e-6)))

    # If F&O, snap to lot size
    lot = await _lot_size(symbol)
    def snap(q:int)->int: return (q//lot)*lot if lot>1 else q

    # Combine: if a stop exists and yields a smaller qty than notional cap, honor risk_qty.
    if qty_risk > 0:
        qty = min(qty_risk, qty_notional)
    else:
        qty = qty_notional

    qty = snap(qty)
    notional = entry_px * qty

    why = []
    if qty <= 0:
        why.append("qty<=0")
    if risk_abs_inr is not None:
        why.append(f"risk_abs={risk_abs_inr}")
    else:
        why.append(f"risk_pct={risk_pct_per_trade:.3f}")
    if stop_px:
        why.append("stop_based")
    if lot>1:
        why.append(f"lot={lot}")

    meta = {
        "cap_budget": cap,
        "risk_used": risk_inr,
        "lot_size": lot,
        "why": ",".join(why)
    }
    return qty, notional, meta
