#!/usr/bin/env python3
"""
orchestrator/pairs_watch_and_trade.py

Runs the pair_watch producer logic and immediately places market
orders for every ENTER signal using a fixed per-leg notional (default ₹200k).
Everything happens in this single process—no downstream listener required.
"""

from __future__ import annotations

import asyncio
import os
import time
from typing import Any, Dict, Optional, Set, Tuple
from pathlib import Path

import asyncpg
import ujson as json
from aiokafka import AIOKafkaProducer
import json as pyjson
from dotenv import load_dotenv
load_dotenv("infra/.env", override=False)
try:
    from kiteconnect import KiteConnect
except Exception:  # pragma: no cover - kite may be unavailable
    KiteConnect = None  # type: ignore

from compute import pair_watch_producer as pw
from execution.oms import OMS

BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
ORDERS_TOPIC = os.getenv("ORDERS_TOPIC", os.getenv("OUT_TOPIC", "orders"))

NOTIONAL_PER_LEG = max(1.0, float(os.getenv("WATCH_TRADE_NOTIONAL", "375000")))
MAX_OPEN_TRADES = int(os.getenv("WATCH_TRADE_MAX_OPEN", "3"))

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "trading")
PG_USER = os.getenv("POSTGRES_USER", "trader")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "trader")

KITE_API_KEY = os.getenv("KITE_API_KEY")
KITE_TOKEN_FILE = os.getenv("ZERODHA_TOKEN_FILE", "ingestion/auth/token.json")
KITE_PRODUCT = os.getenv("WATCH_TRADE_PRODUCT", os.getenv("ZERODHA_PRODUCT", "MIS"))
KITE_VARIETY = os.getenv("WATCH_TRADE_VARIETY", "regular")
KITE_EXCHANGE = os.getenv("WATCH_TRADE_EXCHANGE", "NSE")
KITE_VALIDITY = os.getenv("WATCH_TRADE_VALIDITY", "DAY")

_order_producer: Optional[AIOKafkaProducer] = None
_oms: Optional[OMS] = None
_kite: Optional[KiteConnect] = None
_orig_notify = pw.notify_signal
_open_pairs: Set[str] = set()
_pair_state: Dict[str, Dict[str, Any]] = {}
_z_trace: Dict[str, Tuple[Optional[float], Optional[float]]] = {}
STATE_PATH = Path(os.getenv("WATCH_TRADE_STATE", "runs/pairs_watch_live.json"))
STATE_PATH.parent.mkdir(parents=True, exist_ok=True)


def _qty_for_price(price: float) -> int:
    px = float(price or 0.0)
    if px <= 0:
        return 1
    qty = int(NOTIONAL_PER_LEG // px)
    return max(qty, 1)


def _coid(pair_id: str, leg: str) -> str:
    return f"WATCHTRADE:{pair_id}:{leg}:{int(time.time()*1000)}"


def _load_kite_client() -> Optional[KiteConnect]:
    if KiteConnect is None:
        print("[watch-trade] kiteconnect library not available; live order placement disabled.")
        return None
    if not KITE_API_KEY:
        print("[watch-trade] KITE_API_KEY missing; live order placement disabled.")
        return None
    try:
        token_doc = pyjson.load(open(KITE_TOKEN_FILE))
        access = token_doc["access_token"]
    except Exception as exc:
        print(f"[watch-trade] unable to load Zerodha token ({KITE_TOKEN_FILE}): {exc}")
        return None
    try:
        kite = KiteConnect(api_key=KITE_API_KEY)
        kite.set_access_token(access)
        profile = kite.profile()
        print(
            f"[watch-trade] Zerodha session ready for {profile.get('user_id')} {profile.get('user_name')}"
        )
        return kite
    except Exception as exc:
        print(f"[watch-trade] Zerodha init failed: {exc}")
        return None


async def _persist_state() -> None:
    snapshot = {
        "ts": int(time.time()),
        "max_open": MAX_OPEN_TRADES,
        "open_count": len(_open_pairs),
        "open_pairs": list(_pair_state.values()),
    }
    try:
        txt = pyjson.dumps(snapshot, indent=2)
        await asyncio.to_thread(STATE_PATH.write_text, txt)
    except Exception as exc:
        print(f"[watch-trade] state persist failed: {exc}", flush=True)


async def _emit_order(
    *,
    pair_id: str,
    pair_symbol: str,
    symbol: str,
    side: str,
    qty: int,
    risk_bucket: str,
    reason: str,
    leg: str,
) -> bool:
    if _oms is None or _order_producer is None:
        print("[watch-trade] OMS/Kafka unavailable; skipping order")
        return False
    ts = int(time.time())
    order = {
        "client_order_id": _coid(pair_id, leg),
        "symbol": symbol,
        "side": side,
        "qty": qty,
        "order_type": "MKT",
        "strategy": "PAIRS_WATCH_SIMPLE",
        "risk_bucket": risk_bucket,
        "status": "NEW",
        "ts": ts,
        "reason": reason,
        "extra": {
            "pair_id": pair_id,
            "pair_symbol": pair_symbol,
            "leg": leg,
            "source": "pairs_watch_and_trade",
        },
    }
    broker_id, broker_error = await _place_live_order(symbol, side, qty)
    if broker_id:
        order["extra"]["broker_order_id"] = broker_id
    if broker_error:
        order["extra"]["broker_error"] = broker_error
    try:
        if _oms is not None:
            await _oms.upsert_new(order)
        if _order_producer is not None:
            await _order_producer.send_and_wait(
                ORDERS_TOPIC, json.dumps(order).encode(), key=symbol.encode()
            )
        status_msg = f"broker_id={broker_id}" if broker_id else f"broker_error={broker_error}"
        print(
            f"[watch-trade] SENT {side} {qty} {symbol} pair={pair_id} leg={leg} bucket={risk_bucket} {status_msg}",
            flush=True,
        )
        return True
    except Exception as exc:
        print(f"[watch-trade] order emit failed: {exc} order={order}", flush=True)
        return False


async def _place_orders_from_payload(payload: Dict[str, Any]) -> None:
    action = str(payload.get("action") or "").upper()
    if _kite is None:
        print("[watch-trade] skip: kite unavailable")
        return
    is_entry = action.startswith("ENTER")
    is_exit = action == "EXIT"
    pair_id = str(payload.get("pair_id") or "")
    pair_symbol = str(
        payload.get("pair_symbol") or f"{payload.get('a_symbol')}-{payload.get('b_symbol')}"
    )
    risk_bucket = str(payload.get("risk_bucket") or "MED").upper()
    reason = payload.get("reason") or action.lower()
    current_open = len(_open_pairs)
    if is_entry and pair_id not in _open_pairs and current_open >= MAX_OPEN_TRADES:
        print(f"[watch-trade] skip: hit max open trades ({current_open}/{MAX_OPEN_TRADES})")
        return

    a_symbol = str(payload.get("a_symbol") or "")
    b_symbol = str(payload.get("b_symbol") or "")
    px_a = float(payload.get("pxA") or 0.0)
    px_b = float(payload.get("pxB") or 0.0)

    side_a = str(payload.get("sideA") or "").upper()
    side_b = str(payload.get("sideB") or "").upper()

    if not side_a or not side_b:
        if "LONG_A_SHORT_B" in action:
            side_a, side_b = "BUY", "SELL"
        elif "SHORT_A_LONG_B" in action:
            side_a, side_b = "SELL", "BUY"
        elif action == "EXIT":
            if side_a == "" and side_b == "":
                side_a, side_b = "SELL", "BUY"
        else:
            side_a = side_a or "BUY"
            side_b = side_b or "SELL"

    z_cur = payload.get("z")
    try:
        z_cur = float(z_cur) if z_cur is not None else None
    except (TypeError, ValueError):
        z_cur = None
    prev_z = None
    if z_cur is not None:
        last_pair = _z_trace.get(pair_id)
        prev_z = last_pair[0] if last_pair else None
        prev_print = f"{prev_z:.3f}" if prev_z is not None else "NA"
        print(f"[watch-trade] Z {pair_symbol or pair_id}: latest={z_cur:.3f} prev={prev_print}")
        _z_trace[pair_id] = (z_cur, prev_z)

    qty_a = _qty_for_price(px_a)
    qty_b = _qty_for_price(px_b)

    ok_a = await _emit_order(
        pair_id=pair_id,
        pair_symbol=pair_symbol,
        symbol=a_symbol,
        side=side_a,
        qty=qty_a,
        risk_bucket=risk_bucket,
        reason=reason,
        leg="A",
    )
    ok_b = await _emit_order(
        pair_id=pair_id,
        pair_symbol=pair_symbol,
        symbol=b_symbol,
        side=side_b,
        qty=qty_b,
        risk_bucket=risk_bucket,
        reason=reason,
        leg="B",
    )
    if ok_a and ok_b:
        now_ts = int(time.time())
        if is_entry:
            _open_pairs.add(pair_id)
            _pair_state[pair_id] = {
                "pair_id": pair_id,
                "pair_symbol": pair_symbol,
                "a_symbol": a_symbol,
                "b_symbol": b_symbol,
                "sideA": side_a,
                "sideB": side_b,
                "qtyA": qty_a,
                "qtyB": qty_b,
                "pxA": px_a,
                "pxB": px_b,
                "bucket": risk_bucket,
                "reason": reason,
                "action": action,
                "last_update": now_ts,
            }
            if z_cur is not None:
                _pair_state[pair_id]["z"] = z_cur
            await _persist_state()
        elif is_exit:
            removed = False
            if pair_id in _open_pairs:
                _open_pairs.discard(pair_id)
                removed = True
            if pair_id in _pair_state:
                _pair_state.pop(pair_id, None)
                removed = True
            if pair_id in _z_trace:
                _z_trace.pop(pair_id, None)
            if removed:
                await _persist_state()


async def _place_live_order(symbol: str, side: str, qty: int) -> tuple[Optional[str], Optional[str]]:
    if _kite is None:
        return None, "kite_disabled"
    qty = max(1, int(qty))
    side = side.upper()

    def _submit() -> Optional[str]:
        resp = _kite.place_order(
            variety=KITE_VARIETY,
            exchange=KITE_EXCHANGE,
            tradingsymbol=symbol,
            transaction_type=side,
            quantity=qty,
            product=KITE_PRODUCT,
            order_type="MARKET",
            validity=KITE_VALIDITY,
        )
        broker_id = resp.get("order_id") if isinstance(resp, dict) else None
        return str(broker_id) if broker_id else None

    try:
        broker_id = await asyncio.to_thread(_submit)
        return broker_id, None
    except Exception as exc:
        err = str(exc)
        print(f"[watch-trade] broker order failed {symbol} {side} qty={qty}: {err}", flush=True)
        return None, err


def notify_signal(spec, payload, *, z, action, reason):  # type: ignore[override]
    loop = asyncio.get_running_loop()
    loop.create_task(_place_orders_from_payload(dict(payload)))
    _orig_notify(spec, payload, z=z, action=action, reason=reason)


async def runner() -> None:
    global _oms, _order_producer, _kite, STATE_PATH, _open_pairs, _pair_state, NOTIONAL_PER_LEG, MAX_OPEN_TRADES
    load_dotenv(".env", override=False)
    state_override = os.getenv("WATCH_TRADE_STATE")
    if state_override:
        STATE_PATH = Path(state_override)
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    NOTIONAL_PER_LEG = max(1.0, float(os.getenv("WATCH_TRADE_NOTIONAL", str(NOTIONAL_PER_LEG))))
    MAX_OPEN_TRADES = int(os.getenv("WATCH_TRADE_MAX_OPEN", str(MAX_OPEN_TRADES)))
    pairwatch_state = Path(os.getenv("PAIRWATCH_STATE_PATH", os.getenv("WATCH_TRADE_PAIRWATCH_STATE", "runs/pairwatch_state.json")))
    pairwatch_state.parent.mkdir(parents=True, exist_ok=True)
    os.environ["PAIRWATCH_STATE_PATH"] = str(pairwatch_state)
    os.environ.setdefault("PAIRWATCH_STATE_EXPORT", "1")
    os.environ.setdefault("PAIRWATCH_STATE_INTERVAL_SEC", os.getenv("WATCH_TRADE_PAIRWATCH_STATE_INTERVAL", "10"))
    os.environ.setdefault("PAIRWATCH_SOURCE_MODE", "kite_poll")
    _open_pairs.clear()
    _pair_state.clear()
    await _persist_state()
    pool = None
    try:
        pool = await asyncpg.create_pool(
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DB,
            user=PG_USER,
            password=PG_PASS,
        )
        _oms = OMS(pool)
    except Exception as exc:
        _oms = None
        print(f"[watch-trade] DB/OMS disabled: {exc}")
    _kite = _load_kite_client()
    if _kite is None:
        print("[watch-trade] ERROR: Zerodha client unavailable. Exiting without placing paper orders.")
        if pool is not None:
            try:
                await pool.close()
            except Exception:
                pass
        return
    try:
        producer = AIOKafkaProducer(bootstrap_servers=BROKER, acks="all", linger_ms=5)
        await producer.start()
        _order_producer = producer
    except Exception as exc:
        _order_producer = None
        print(f"[watch-trade] Kafka disabled: {exc}")
    if _oms is None or _order_producer is None:
        print("[watch-trade] ERROR: Required infrastructure unavailable (OMS/Kafka). Exiting.")
        if _order_producer is not None:
            try:
                await _order_producer.stop()
            except Exception:
                pass
        if pool is not None:
            try:
                await pool.close()
            except Exception:
                pass
        return
    live_flag = "enabled" if _kite is not None else "disabled"
    print(
        f"[watch-trade] starting pair watcher with instant orders; "
        f"notional_per_leg={NOTIONAL_PER_LEG:.0f} broker={BROKER} live_orders={live_flag}",
        flush=True,
    )
    prev_notify = pw.notify_signal
    pw.notify_signal = notify_signal  # type: ignore[assignment]
    try:
        await pw.main()
    finally:
        pw.notify_signal = prev_notify  # type: ignore[assignment]
        if _order_producer is not None:
            try:
                await _order_producer.stop()
            except Exception:
                pass
        if pool is not None:
            try:
                await pool.close()
            except Exception:
                pass


if __name__ == "__main__":
    asyncio.run(runner())
