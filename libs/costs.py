"""
Cost/slippage estimators for equities.
- Reads configs/costs.yaml structure.
- Returns a dict suitable to attach on orders (runner) or PnL (backtests).

Assumptions:
- Brokerage is per-leg (flat).
- STT on sell for intraday (tune if you trade delivery).
- GST on (brokerage + exchange txns).
- Stamp duty on buy side.
- Slippage in basis points (bps) applied to price for est_fill_price.
"""
from __future__ import annotations
from typing import Dict

def _get_bucket_overrides(costs_cfg:dict, bucket:str) -> dict:
    ovr = (costs_cfg.get("bucket_overrides") or {}).get(bucket) or {}
    return ovr

def _slippage_bps(costs_cfg:dict, bucket:str) -> float:
    base = float(costs_cfg["equities"].get("slippage_bps", 0.0))
    ovr  = _get_bucket_overrides(costs_cfg, bucket)
    return float(ovr.get("slippage_bps", base))

def estimate_order_costs(side:str, price:float, qty:int, bucket:str, costs_cfg:dict) -> Dict:
    """
    Returns:
      {
        "est_fill_price": float,   # price ± slippage
        "est_total_cost_inr": float,
        "breakdown": {...}
      }
    """
    eq = costs_cfg["equities"]
    notional = float(price) * int(qty)

    # slippage
    bps = _slippage_bps(costs_cfg, bucket)
    slip_px = price * (bps / 10000.0)   # 1 bps = 0.01%
    est_fill_price = price + (slip_px if side == "BUY" else -slip_px)

    brokerage = float(eq.get("brokerage_per_leg", 0.0))
    exch_pct  = float(eq.get("exchange_txn_pct", 0.0))
    sebi_pct  = float(eq.get("sebi_charges_pct", 0.0))
    gst_pct   = float(eq.get("gst_pct", 0.0))
    stt_pct   = float(eq.get("stt_pct", 0.0))
    stamp_pct = float(eq.get("stamp_duty_pct", 0.0))

    exch = notional * exch_pct
    sebi = notional * sebi_pct
    # Intraday STT is usually on sell side (smaller rate); keep configurable
    stt  = (notional * stt_pct) if side == "SELL" else 0.0
    stamp= (notional * stamp_pct) if side == "BUY"  else 0.0
    gst  = (brokerage + exch) * gst_pct

    total = brokerage + exch + sebi + stt + stamp + gst

    return {
        "est_fill_price": round(est_fill_price, 4),
        "est_total_cost_inr": round(total, 2),
        "breakdown": {
            "brokerage": round(brokerage,2),
            "exchange_txn": round(exch,2),
            "sebi": round(sebi,2),
            "stt": round(stt,2),
            "stamp_duty": round(stamp,2),
            "gst": round(gst,2),
            "slippage_bps": bps
        }
    }
