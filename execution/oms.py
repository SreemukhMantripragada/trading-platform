"""
execution/oms.py
- Idempotent order upsert (client_order_id = PK)
- Validates & applies state transitions
- Writes audit trail
- Produces a stable audit_hash (sha256 of key fields)

Intended usage:
  oms = OMS(pool)
  await oms.upsert_new(order_dict)
  await oms.transition(coid, "ACK", note="accepted by EMS")
  await oms.transition(coid, "FILLED", note="paper fill", meta={"fill_qty": 100})
"""
from __future__ import annotations
import hashlib, json, asyncpg, time
from typing import Optional, Dict, Any

VALID = {
  "NEW": {"ACK","REJECTED"},
  "ACK": {"PARTIAL","FILLED","CANCELED","REJECTED"},
  "PARTIAL": {"PARTIAL","FILLED","CANCELED","REJECTED"}
}

def _audit_hash(o: Dict[str, Any]) -> str:
    core = {
        "client_order_id": o["client_order_id"],
        "ts": int(o["ts"]),
        "symbol": o["symbol"],
        "side": o["side"],
        "qty": int(o["qty"]),
        "order_type": o["order_type"],
        "strategy": o["strategy"],
        "risk_bucket": o["risk_bucket"]
    }
    s = json.dumps(core, sort_keys=True, separators=(",",":"))
    return hashlib.sha256(s.encode()).hexdigest()

class OMS:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    async def upsert_new(self, order: Dict[str, Any]) -> None:
        """
        Insert NEW if absent; if present, do nothing (idempotent).
        """
        order = dict(order)
        if "ts" not in order or order["ts"] is None:
            order["ts"] = time.time()
        try:
            order["ts"] = int(order["ts"])
        except Exception:
            order["ts"] = int(time.time())
        order["client_order_id"] = str(order.get("client_order_id") or "").strip()
        if not order["client_order_id"]:
            raise ValueError("order missing client_order_id")
        order["symbol"] = str(order.get("symbol") or "").strip()
        order["side"] = str(order.get("side") or "").upper()
        order["order_type"] = str(order.get("order_type") or "MKT").upper()
        order["strategy"] = str(order.get("strategy") or "UNKNOWN")
        order["risk_bucket"] = str(order.get("risk_bucket") or "MED").upper()
        order["qty"] = int(order.get("qty") or 0)
        if isinstance(order.get("extra"), dict):
            extra = order["extra"]
        else:
            extra = {}
        order["extra"] = extra
        order.setdefault("status", "NEW")
        order["audit_hash"] = _audit_hash(order)
        sql = """
        INSERT INTO orders(client_order_id, ts, symbol, side, qty, order_type, strategy, risk_bucket, status, extra, audit_hash)
        VALUES($1, to_timestamp($2), $3, $4, $5, $6, $7, $8, 'NEW', $9, $10)
        ON CONFLICT (client_order_id) DO NOTHING;
        """
        async with self.pool.acquire() as con:
            async with con.transaction():
                await con.execute(sql, order["client_order_id"], int(order["ts"]),
                                  order["symbol"], order["side"], int(order["qty"]),
                                  order["order_type"], order["strategy"], order["risk_bucket"],
                                  json.dumps(order.get("extra") or {}), order["audit_hash"])
                await con.execute(
                    "INSERT INTO order_audit(client_order_id, from_st, to_st, note, meta) VALUES($1,$2,$3,$4,$5)",
                    order["client_order_id"], None, "NEW", "insert", json.dumps({})
                )

    async def transition(self, coid: str, to_state: str, note:str="", meta: Optional[Dict[str,Any]]=None) -> None:
        """
        Apply a valid state transition; if already in 'to_state', no-op.
        """
        meta = meta or {}
        async with self.pool.acquire() as con:
            async with con.transaction():
                row = await con.fetchrow("SELECT status FROM orders WHERE client_order_id=$1 FOR UPDATE", coid)
                if not row:
                    raise ValueError(f"unknown order {coid}")
                cur = row["status"]
                if cur == to_state:
                    return
                if cur not in VALID or to_state not in VALID[cur]:
                    # Allow idempotent repeated terminal transitions as safe no-op
                    if cur in ("FILLED","CANCELED","REJECTED"):
                        return
                    raise ValueError(f"invalid transition {cur}->{to_state} for {coid}")
                await con.execute("UPDATE orders SET status=$1, last_update=now() WHERE client_order_id=$2",
                                  to_state, coid)
                await con.execute(
                    "INSERT INTO order_audit(client_order_id, from_st, to_st, note, meta) VALUES($1,$2,$3,$4,$5)",
                    coid, cur, to_state, note, json.dumps(meta)
                )
