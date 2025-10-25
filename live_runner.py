# live_runner.py
from __future__ import annotations
import sys, os
# Redirect stderr to null (optional; uncomment if you want to silence lib noise)
sys.stderr = open(os.devnull, "w")

import asyncio, yaml, ujson as json, importlib, math, time, hashlib
from typing import Dict, Any, Tuple, Optional, List
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, time as dtime
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from aiokafka import AIOKafkaConsumer
from kiteconnect import KiteConnect

# ---------- Load env ----------
load_dotenv(".env"); load_dotenv("infra/.env")

# Kafka
BROKER        = os.getenv("KAFKA_BROKER", "localhost:9092")
GROUP_ID      = os.getenv("KAFKA_GROUP", "live_runner")
NEXT_DAY_PATH = os.getenv("NEXT_DAY", "configs/next_day.yaml")

# Zerodha
KITE_API_KEY  = os.getenv("KITE_API_KEY", "")
TOKEN_JSON    = os.getenv("KITE_TOKEN_JSON", "ingestion/auth/token.json")

# Execution settings
LIVE_TRADING          = bool(int(os.getenv("LIVE_TRADING", "1")))            # 0=dry run, 1=live
LIVE_TRADING = False
EXCHANGE              = os.getenv("EXCHANGE", "NSE")
PRODUCT               = os.getenv("PRODUCT", "MIS")                           # intraday
VARIETY               = os.getenv("VARIETY", "regular")
ORDER_TYPE            = os.getenv("ORDER_TYPE", "MARKET")
LEVERAGE_PER_STOCK    = float(os.getenv("LEVERAGE_PER_STOCK", "5.0"))
MAX_QTY_PER_TRADE     = int(os.getenv("MAX_QTY_PER_TRADE", "5000"))
CASH_FALLBACK_FRACTION= float(os.getenv("CASH_FALLBACK_FRACTION", "0.10"))
# Default notional = PER_STOCK_BASE_CASH * LEVERAGE_PER_STOCK; 40k * 5x ≈ ₹2L
PER_STOCK_BASE_CASH   = float(os.getenv("PER_STOCK_BASE_CASH", "40000"))
PNL_LOG_INTERVAL_SEC  = int(os.getenv("PNL_LOG_INTERVAL_SEC", "60"))

# Historical warmup
WARMUP_CANDLES        = int(os.getenv("WARMUP_CANDLES", "1000"))              # per TF/symbol via Kite historical
INSTRUMENTS_CSV       = os.getenv("INSTRUMENTS_CSV", "configs/tokens.csv")    # instrument_token map for historical

# Square-off (intraday)
IST                   = ZoneInfo("Asia/Kolkata")
SQUAREOFF_STR         = os.getenv("SQUAREOFF_IST", "15:25")
HH, MM = map(int, SQUAREOFF_STR.split(":"))
SQUAREOFF_T           = dtime(HH, MM)

# Polling
ORDERBOOK_POLL_SEC    = float(os.getenv("ORDERBOOK_POLL_SEC", "10"))
HEARTBEAT_SEC         = float(os.getenv("HEARTBEAT_SEC", "60"))

# Optional HTTP state
STATE_HTTP_ENABLED    = bool(int(os.getenv("STATE_HTTP_ENABLED", "1")))
STATE_HTTP_HOST       = os.getenv("STATE_HTTP_HOST", "127.0.0.1")
STATE_HTTP_PORT       = int(os.getenv("STATE_HTTP_PORT", "8787"))

# Auto switch DRY -> LIVE at 09:15
AUTO_SWITCH_TO_LIVE   = int(os.getenv("AUTO_SWITCH_TO_LIVE", "1"))
AUTO_SWITCH_TO_LIVE = 0
SWITCH_TO_LIVE_STR    = os.getenv("SWITCH_TO_LIVE_IST", "09:15")
SW_HH, SW_MM = map(int, SWITCH_TO_LIVE_STR.split(":"))
SWITCH_TO_LIVE_T      = dtime(SW_HH, SW_MM)

# Strategy import map (canonical names)
STRAT_PREFIX   = os.getenv("STRAT_MODULE_PREFIX", "strategy.strategies")
strategy_modules = {
    "EMACross": "ema_cross",
    "MACD": "macd_signal",
    "ROCMomentum": "momentum_roc",
    "BollingerBreakout": "bollinger_breakout",
    "BollingerReversion": "bollinger_reversion",
    "DonchianBreakout": "donchian_breakout",
    "KeltnerBreakout": "keltner_breakout",
    "SupertrendTrend": "supertrend_trend",
    "HeikinTrend": "heikin_trend",
    "CCIReversion": "cci_reversion",
    "MeanReversionZScore": "meanreversion_zscore",
    "VWAPReversion": "vwap_reversion",
    "EnsembleKofN": "ensemble_kofn",
    "ORBBreakout": "orb_breakout",
}

# ---------- time helpers ----------
def to_unix_ts(val) -> int:
    if isinstance(val, datetime):
        dt = val
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    if isinstance(val, str):
        s = val.strip()
        if s.endswith('Z'):
            s = s[:-1] + '+00:00'
        if ' ' in s and 'T' not in s:
            s = s.replace(' ', 'T')
        if len(s) >= 5 and (s[-5] in ['+', '-']) and s[-3] != ':':
            s = s[:-2] + ':' + s[-2:]
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    raise TypeError(f"Unsupported date type for `r['date']`: {type(val)}")

def ist_now() -> datetime:
    return datetime.now(tz=IST)

def is_squareoff_now() -> bool:
    return ist_now().time() >= SQUAREOFF_T

def topic_today(tf: int) -> str:
    return f"bars.{tf}m.{ist_now().strftime('%Y%m%d')}"

# ---------- tokens.csv lookup ----------
def _instrument_token(symbol: str) -> Optional[int]:
    """
    Look up instrument_token from configs/tokens.csv (tradingsymbol match, subscribe=1 preferred).
    Expected headers: instrument_token,tradingsymbol,exchange,subscribe
    """
    try:
        import csv
        with open(INSTRUMENTS_CSV, "r", newline="") as f:
            rdr = csv.DictReader(f)
            best = None
            for row in rdr:
                tsym = (row.get("tradingsymbol") or "").strip()
                if tsym != symbol:
                    continue
                tok = row.get("instrument_token")
                if not tok:
                    continue
                sub  = (row.get("subscribe") or "").strip().lower()
                # prefer subscribed rows
                if sub in ("1","true","y","yes"):
                    return int(tok)
                best = int(tok)
            return best
    except Exception:
        return None

def import_class(sname: str):
    mod = strategy_modules.get(sname)
    if not mod:
        raise RuntimeError(f"Unknown strategy: {sname}")
    module = f"{STRAT_PREFIX}.{mod}"
    pkg = importlib.import_module(module)
    return getattr(pkg, sname)

def load_nextday(path: str) -> List[dict]:
    if not os.path.exists(path):
        print(f"[live] next_day YAML not found: {path}")
        return []
    doc = yaml.safe_load(open(path)) or {}
    return doc.get("selections", []) or []

def bars_hash(r: dict) -> str:
    s = f"{r.get('o')}|{r.get('h')}|{r.get('l')}|{r.get('c')}|{r.get('vol')}"
    return hashlib.md5(s.encode()).hexdigest()

# ----------- Zerodha wrapper -----------
class KiteWrap:
    def __init__(self):
        self.live = LIVE_TRADING
        self.k: Optional[KiteConnect] = None

    def connect(self):
        try:
            tok = None
            if os.path.exists(TOKEN_JSON):
                import json as _json
                T = _json.load(open(TOKEN_JSON))
                tok = T.get("access_token")
            assert KITE_API_KEY and tok, "Missing API key or token.json"
            k = KiteConnect(api_key=KITE_API_KEY)
            k.set_access_token(tok)
            self.k = k
            print("[live] KiteConnect ready.")
        except Exception as e:
            self.k = None
            print(f"[live] KiteConnect init failed: {e}")

    def live_balance(self) -> float:
        """Available cash balance (quiet on errors)."""
        if not self.k:
            return 0.0
        try:
            m = self.k.margins("equity")
            return float(m.get("available", {}).get("live_balance", 0.0))
        except Exception:
            return 0.0

    async def place(self, symbol: str, side: str, qty: int) -> Optional[str]:
        if qty <= 0:
            return None
        if not self.live:
            print(f"[dry-run] {side} {symbol} x{qty}")
            await asyncio.sleep(2)  # pacing
            return f"DRY-{int(time.time())}"
        if not self.k:
            return None
        try:
            tx = "BUY" if side.upper() == "BUY" else "SELL"
            order_id = self.k.place_order(
                variety=VARIETY,
                exchange=EXCHANGE,
                tradingsymbol=symbol,
                transaction_type=tx,
                quantity=int(qty),
                product=PRODUCT,
                order_type=ORDER_TYPE
            )
            print(f"[live] placed {tx} {symbol} x{qty} -> {order_id}")
            await asyncio.sleep(2)  # pacing between orders
            return order_id
        except Exception:
            # silent by design; still pace to avoid bursts
            await asyncio.sleep(2)
            return None

    async def squareoff(self, symbol: str, position_side: int, qty: int) -> Optional[str]:
        if qty <= 0:
            return None
        side = "SELL" if position_side > 0 else "BUY"
        return await self.place(symbol, side, qty)

# ----------- State model -----------
@dataclass
class LiveState:
    symbol: str
    strategy_name: str
    tf: int
    obj: Any
    params: dict
    risk_per_trade: float
    bucket: str
    # position as *shares*
    position: int = 0
    entry_price: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    last_reason: str = ""
    open_order_id: Optional[str] = None
    # monitoring
    last_price: Optional[float] = None
    last_bar_ts: Optional[int] = None
    last_signal_side: Optional[str] = None
    last_signal_true: bool = False
    last_signal_at_ist: Optional[str] = None
    # freshness / health
    last_update_ist: Optional[str] = None
    last_update_ts: Optional[int] = None
    health_status: str = "INIT"              # OK | WARN | STALE | INIT
    health_age_sec: Optional[int] = None

# ----------- Sizing helper -----------
def calc_qty(price: float, stop: Optional[float], avail_cash: float, risk_per_trade: float, leverage: float) -> int:
    """
    Simple size: spend (PER_STOCK_BASE_CASH * leverage) notionally.
    Reservation is handled separately; here we only compute qty.
    """
    if price <= 0:
        return 0
    if avail_cash < PER_STOCK_BASE_CASH:
        return 0
    target_notional = leverage * PER_STOCK_BASE_CASH
    qty = int(math.floor(max(0.0, target_notional / price)))
    return max(0, min(qty, MAX_QTY_PER_TRADE))

def record_unrealized_pnl(states: Dict[Tuple[str,str,int], LiveState]) -> Dict[str, float]:
    per_sym: Dict[str, float] = {}
    total = 0.0
    for st in states.values():
        if st.position == 0 or st.entry_price is None or st.last_price is None:
            continue
        pnl = (st.last_price - st.entry_price) * st.position if st.position > 0 else (st.entry_price - st.last_price) * abs(st.position)
        per_sym[st.symbol] = per_sym.get(st.symbol, 0.0) + pnl
        total += pnl
    per_sym["_TOTAL_"] = total
    return per_sym

# ----------- Historical warmup via Kite -----------
def _kite_time_range_for_warmup(tf_min: int, count: int) -> tuple[datetime, datetime]:
    end_ist = ist_now()
    start_ist = end_ist - timedelta(minutes=(count * tf_min + 2 * tf_min))
    start_ist -= timedelta(days=1)
    return (start_ist.astimezone(timezone.utc), end_ist.astimezone(timezone.utc))

def _kite_interval(tf_min: int) -> str:
    return {3: "3minute", 5: "5minute", 15: "15minute"}[int(tf_min)]

async def hydrate_from_kite(kite: KiteWrap, states_by_tf: Dict[int, List[LiveState]]):
    """
    For each TF & symbol, fetch ~WARMUP_CANDLES bars using Kite historical API
    and feed to strategy.on_bar to warm indicators/state.
    """
    if not kite.k:
        kite.connect()
    if not kite.k:
        print("[hydrate] Kite not ready; skipping historical warmup.")
        return

    for tf, states in states_by_tf.items():
        if not states:
            continue
        symbols = sorted({st.symbol for st in states})
        interval = _kite_interval(tf)
        for sym in symbols:
            tok = _instrument_token(sym)
            if not tok:
                print(f"[hydrate] token not found for {sym}; skip.")
                continue
            start_utc, end_utc = _kite_time_range_for_warmup(tf, WARMUP_CANDLES)
            try:
                data = kite.k.historical_data(
                    instrument_token=tok,
                    from_date=start_utc,
                    to_date=end_utc,
                    interval=interval,
                    continuous=False,
                    oi=False
                )
            except Exception:
                data = []

            if not data:
                print(f"[hydrate] no hist for {sym} tf={tf}m")
                continue

            # feed in chronological order
            for st in states:
                if st.symbol != sym:
                    continue
                for r in data:
                    ts = to_unix_ts(r["date"])
                    o, h, l, c, v = float(r["open"]), float(r["high"]), float(r["low"]), float(r["close"]), int(r.get("volume") or 0)
                    try:
                        sig = st.obj.on_bar(sym, f"{tf}m", ts, o, h, l, c, v, {})
                    except TypeError:
                        from strategy.base import Candle
                        sig = st.obj.on_bar(Candle(
                            symbol=sym, ts=datetime.fromtimestamp(ts, tz=timezone.utc),
                            o=o, h=h, l=l, c=c, vol=v
                        ))
                    st.last_price = c
                    st.last_bar_ts = ts
                    st.last_update_ts = ts
                    st.last_update_ist = ist_now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[hydrate] {sym} tf={tf}m : fed {len(data)} bars")

# ----------- Capital manager (atomic reservations) -----------
class CapitalManager:
    """
    Tracks reserved base-cash per symbol, with an async lock to make checks atomic.
    Reservation represents intent to use PER_STOCK_BASE_CASH base margin for a live position.
    """
    def __init__(self, kite: KiteWrap):
        self.kite = kite
        self._lock = asyncio.Lock()
        self._reserved: Dict[str, float] = {}  # symbol -> base_cash

    async def try_reserve(self, symbol: str, base_cash: float) -> bool:
        async with self._lock:
            bal = self.kite.live_balance()
            used = sum(self._reserved.values())
            free = max(0.0, bal - used)
            if free >= base_cash:
                self._reserved[symbol] = base_cash
                return True
            return False

    async def release(self, symbol: str):
        async with self._lock:
            self._reserved.pop(symbol, None)

    async def free_cash(self) -> float:
        async with self._lock:
            bal = self.kite.live_balance()
            used = sum(self._reserved.values())
            return max(0.0, bal - used)

    async def snapshot(self) -> tuple[float, float, Dict[str, float]]:
        async with self._lock:
            bal = self.kite.live_balance()
            used = sum(self._reserved.values())
            free = max(0.0, bal - used)
            return bal, free, dict(self._reserved)

# ----------- Runner -----------
class LiveRunner:
    def __init__(self):
        self.states: Dict[Tuple[str, str, int], LiveState] = {}
        self.states_by_tf: Dict[int, list[LiveState]] = {}
        self.kite = KiteWrap()
        self.kite.connect()

        # Capital reservations (atomic)
        self.capital = CapitalManager(self.kite)

        # Dedup caches
        self.last_ts_seen: Dict[Tuple[str,int], int] = {}
        self.last_payload_hash: Dict[Tuple[str,int], str] = {}
        self.dedup_drops = 0

        # Exclusive owner per symbol (strategy lock while in trade)
        # value is key=(symbol, strategy_name, tf) of the owner
        self.active_owner: Dict[str, Tuple[str,str,int]] = {}

        # Debounce per symbol for fresh entries
        self.last_entry_ts: Dict[str, float] = {}
        self.entry_debounce_sec = 10.0

    def load_selections(self, path: str) -> List[int]:
        sels = load_nextday(path)
        if not sels:
            print("[live] selections empty; exiting.")
            raise SystemExit(2)

        self.states_by_tf.clear()
        for r in sels:
            sym = str(r["symbol"])
            sname = str(r["strategy"])
            tf = int(r["timeframe"])
            params = r.get("params") or {}
            risk = float(r.get("risk_per_trade", 0.01))
            bucket = str(r.get("bucket", "MED"))
            Strat = import_class(sname)
            obj = Strat(**params)

            st = LiveState(
                symbol=sym, strategy_name=sname, tf=tf, obj=obj,
                params=params, risk_per_trade=risk, bucket=bucket
            )
            self.states[(sym, sname, tf)] = st
            self.states_by_tf.setdefault(tf, []).append(st)

        need_tfs = sorted(self.states_by_tf.keys())
        print(f"[live] strategies: {len(self.states)} across TFs={need_tfs}")
        return need_tfs

    async def cash_heartbeat(self):
        while True:
            try:
                bal, free, reserved = await self.capital.snapshot()
                rtxt = ", ".join(f"{k}:₹{v:,.0f}" for k,v in reserved.items()) or "-"
                print(f"[balance] live=₹{bal:,.2f} | reserved=₹{sum(reserved.values()):,.2f} | free=₹{free:,.2f} | held: [{rtxt}]")
            except Exception:
                pass
            await asyncio.sleep(HEARTBEAT_SEC)

    async def orderbook_poll(self):
        while True:
            await asyncio.sleep(ORDERBOOK_POLL_SEC)

    async def squareoff_guard(self):
        while True:
            if is_squareoff_now():
                print("[live] SQUAREOFF → closing any open positions...")
                for key, st in list(self.states.items()):
                    if st.position != 0:
                        qty = abs(st.position)
                        await self.kite.squareoff(st.symbol, +1 if st.position>0 else -1, qty)
                        st.position = 0
                        st.entry_price = None
                        st.open_order_id = None
                        st.stop_loss = None
                        st.take_profit = None
                        # release symbol owner and reservation
                        owner = self.active_owner.get(st.symbol)
                        if owner == key:
                            self.active_owner.pop(st.symbol, None)
                        await self.capital.release(st.symbol)
                print("[live] Square-off complete. You may stop the runner.")
                return
            await asyncio.sleep(5)

    async def auto_switch_to_live(self):
        if not AUTO_SWITCH_TO_LIVE or self.kite.live:
            return
        print(f"[live] Auto-switch armed → LIVE at {SWITCH_TO_LIVE_T.strftime('%H:%M')} IST")
        while True:
            now_ist = ist_now().time()
            if now_ist >= SWITCH_TO_LIVE_T:
                if not self.kite.k:
                    self.kite.connect()
                if self.kite.k:
                    self.kite.live = True
                    print("[live] ✅ Switched DRY → LIVE.")
                else:
                    print("[live] ⚠️ Tried to switch to LIVE but Kite isn’t ready; staying DRY.")
                return
            await asyncio.sleep(10)

    async def pnl_monitor(self):
        while True:
            try:
                per = record_unrealized_pnl(self.states)
                tot = per.get("_TOTAL_", 0.0)
                owners = ", ".join(f"{sym}:{self.active_owner[sym][1]}" for sym in self.active_owner.keys())
                bal, free, _ = await self.capital.snapshot()
                print(f"[pnl] unrealized total ≈ ₹{tot:,.2f} | owners: [{owners}] | free_cash=₹{free:,.0f}")
                if self.dedup_drops:
                    print(f"[pnl] dedup drops in last interval: {self.dedup_drops}")
                    self.dedup_drops = 0
            except Exception:
                pass
            await asyncio.sleep(PNL_LOG_INTERVAL_SEC)

    async def strategy_health_monitor(self, interval_sec: int = 30):

        def _secs(tf_min: int, mult: float) -> int:
            return int(tf_min * 60 * mult)
        while True:
            try:
                now = datetime.now(tz=timezone.utc)
                hdr = f"\n[health {ist_now().strftime('%Y-%m-%d %H:%M:%S')}] symbol        | strategy                | tf  | status | age   | last_ist_time       | last_sig | pos | owner?"
                print(hdr)
                print("-" * len(hdr))
                for (sym, sname, tf), st in self.states.items():
                    if st.last_update_ts is None:
                        st.health_status = "INIT"
                        st.health_age_sec = None
                        age_s = "-"
                    else:
                        age = int((now - datetime.fromtimestamp(st.last_update_ts, tz=timezone.utc)).total_seconds())
                        st.health_age_sec = age
                        if age <= _secs(tf, 1.5):
                            st.health_status = "OK"
                        elif age <= _secs(tf, 3.0):
                            st.health_status = "WARN"
                        else:
                            st.health_status = "STALE"
                        age_s = f"{age}s"
                    pos_str = "FLAT" if st.position == 0 else ("LONG" if st.position > 0 else "SHORT")
                    is_owner = (self.active_owner.get(sym) == (sym, sname, tf))
                    print(f"{sym:<12} | {sname:<22} | {tf:>2}m | {st.health_status:<6} | {age_s:<5} | "
                          f"{(st.last_update_ist or '-'): <19} | {(st.last_signal_side or '-'): <7} | {pos_str:<5} | "
                          f"{'YES' if is_owner else 'NO'}")
            except Exception:
                pass
            await asyncio.sleep(interval_sec)

    async def _process_record(self, st: LiveState, key: Tuple[str,str,int], r: dict):
        """
        Process one bar for a single strategy:
        - per-symbol exclusive ownership enforced
        - atomic capital gate via reservation
        - 2s pacing after each place (handled inside KiteWrap.place)
        """
        sym = r["symbol"]
        tf = st.tf
        ts = int(r["ts"])
        o,h,l,c = float(r["o"]),float(r["h"]),float(r["l"]),float(r["c"])
        vol = int(r.get("vol", 0))

        # Feed to strategy
        try:
            sig = st.obj.on_bar(sym, f"{tf}m", ts, o, h, l, c, vol, {})
        except TypeError:
            from strategy.base import Candle
            sig = st.obj.on_bar(
                Candle(symbol=sym, ts=datetime.fromtimestamp(ts, tz=timezone.utc),
                       o=o, h=h, l=l, c=c, vol=vol)
            )

        # Extract signal semantics
        side = getattr(sig, "side", getattr(sig, "action", "HOLD"))
        st.stop_loss = getattr(sig, "stop_loss", getattr(sig, "stop", st.stop_loss))
        st.take_profit = getattr(sig, "take_profit", getattr(sig, "target", st.take_profit))
        st.last_reason = getattr(sig, "reason", "") or ""
        st.last_price = c
        st.last_bar_ts = ts
        st.last_signal_side = side
        st.last_signal_true = side in ("BUY","SELL","EXIT")
        st.last_signal_at_ist = ist_now().strftime("%Y-%m-%d %H:%M:%S")
        st.last_update_ts = ts
        st.last_update_ist = st.last_signal_at_ist

        # No actionable signal
        if side not in ("BUY","SELL","EXIT"):
            return

        # --- Exclusive ownership rule ---
        owner = self.active_owner.get(sym)  # current owner key if any
        if owner is not None and owner != key:
            # Another strategy currently owns this symbol: only allow it to finish.
            return

        # --- EXIT first (owner or no owner) ---
        if side == "EXIT" and st.position != 0:
            qty = abs(st.position)
            oid = await self.kite.place(st.symbol, "SELL" if st.position > 0 else "BUY", qty)
            st.position = 0
            st.entry_price = None
            st.open_order_id = oid
            # release ownership and capital reservation on flat
            if self.active_owner.get(sym) == key:
                self.active_owner.pop(sym, None)
            await self.capital.release(sym)
            return

        # --- Debounce new entries ---
        now = time.time()
        last_t = self.last_entry_ts.get(sym, 0.0)
        if st.position == 0 and now - last_t < self.entry_debounce_sec:
            return

        # Fresh balance snapshot (non-atomic; the atomic part is reservation below)
        bal = self.kite.live_balance()
        if bal < PER_STOCK_BASE_CASH:
            return

        # --- Entry / Flip (taking ownership if entering from flat) ---
        if side == "BUY":
            # flip short -> flat
            if st.position < 0:
                cov = await self.kite.place(st.symbol, "BUY", abs(st.position))
                st.position = 0
                st.entry_price = None
                st.open_order_id = cov
                # keep going to potential long entry
            if st.position == 0:
                # atomic reserve before sizing/placing
                ok = await self.capital.try_reserve(sym, PER_STOCK_BASE_CASH)
                if not ok:
                    return
                qty = calc_qty(c, st.stop_loss, bal, st.risk_per_trade, LEVERAGE_PER_STOCK)
                if qty <= 0:
                    await self.capital.release(sym)
                    return
                oid = await self.kite.place(st.symbol, "BUY", qty)
                if oid is None:
                    # placement failed → release reservation
                    await self.capital.release(sym)
                    return
                st.position = +qty
                st.entry_price = c
                st.open_order_id = oid
                # take ownership on entry and record debounce time
                self.active_owner[sym] = key
                self.last_entry_ts[sym] = time.time()

        elif side == "SELL":
            # flip long -> flat
            if st.position > 0:
                cov = await self.kite.place(st.symbol, "SELL", abs(st.position))
                st.position = 0
                st.entry_price = None
                st.open_order_id = cov
            if st.position == 0:
                ok = await self.capital.try_reserve(sym, PER_STOCK_BASE_CASH)
                if not ok:
                    return
                qty = calc_qty(c, st.stop_loss, bal, st.risk_per_trade, LEVERAGE_PER_STOCK)
                if qty <= 0:
                    await self.capital.release(sym)
                    return
                oid = await self.kite.place(st.symbol, "SELL", qty)
                if oid is None:
                    await self.capital.release(sym)
                    return
                st.position = -qty
                st.entry_price = c
                st.open_order_id = oid
                self.active_owner[sym] = key
                self.last_entry_ts[sym] = time.time()

    async def _tf_runner(self, tf: int):
        """
        Dedicated consumer loop for a TF:
        - subscribes to bars.{tf}m.YYYYMMDD,
        - per-symbol dedup (ts / payload hash),
        - dispatches bar to relevant strategies with exclusive ownership + capital reservations.
        """
        topic = topic_today(tf)
        gid = f"{GROUP_ID}.{tf}m"
        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=BROKER,
            enable_auto_commit=False,
            auto_offset_reset="latest",
            group_id=gid,
            value_deserializer=lambda b: json.loads(b.decode()),
            key_deserializer=lambda b: b.decode() if b else None
        )
        await consumer.start()
        print(f"[live] TF runner started → {topic} (group={gid})")

        try:
            async for m in consumer:
                r = m.value
                sym = r.get("symbol")
                if sym is None:
                    await consumer.commit()
                    continue

                # Dedup
                key_sym_tf = (sym, tf)
                ts = int(r.get("ts", 0))
                hsh = bars_hash(r)
                last_ts = self.last_ts_seen.get(key_sym_tf, -1)
                if ts < last_ts:
                    self.dedup_drops += 1
                    await consumer.commit()
                    continue
                if ts == last_ts and hsh == self.last_payload_hash.get(key_sym_tf):
                    self.dedup_drops += 1
                    await consumer.commit()
                    continue
                self.last_ts_seen[key_sym_tf] = ts
                self.last_payload_hash[key_sym_tf] = hsh

                # Dispatch to strategies for this (tf, symbol)
                for (s_sym, sname, s_tf), st in self.states.items():
                    if s_tf != tf or s_sym != sym:
                        continue
                    try:
                        await self._process_record(st, (s_sym, sname, s_tf), r)
                    except Exception:
                        pass

                await consumer.commit()
        finally:
            await consumer.stop()
            print(f"[live] TF runner stopped → {topic}")

    async def http_state_server(self):
        try:
            from aiohttp import web
        except Exception:
            print("[http] aiohttp not installed; skip state server.")
            return

        async def handle_state(_req):
            items = []
            for (sym, sname, tf), st in self.states.items():
                items.append(dict(
                    symbol=sym, strategy=sname, tf=tf, position=st.position,
                    entry_price=st.entry_price, last_price=st.last_price,
                    stop_loss=st.stop_loss, take_profit=st.take_profit,
                    last_signal_side=st.last_signal_side,
                    last_signal_true=st.last_signal_true,
                    last_reason=st.last_reason,
                    last_at=st.last_signal_at_ist,
                    health_status=st.health_status,
                    health_age_sec=st.health_age_sec,
                    last_update_ist=st.last_update_ist,
                    owner=(self.active_owner.get(sym) == (sym, sname, tf))
                ))
            bal, free, reserved = await self.capital.snapshot()
            return web.json_response({
                "items": items,
                "capital": {"live": bal, "free": free, "reserved": reserved},
                "ts": ist_now().isoformat()
            })

        from aiohttp import web
        app = web.Application()
        app.router.add_get("/state", handle_state)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, STATE_HTTP_HOST, STATE_HTTP_PORT)
        await site.start()
        print(f"[http] state server on http://{STATE_HTTP_HOST}:{STATE_HTTP_PORT}/state")
        while True:
            await asyncio.sleep(3600)

    async def run(self):
        # 1) Build state from next_day and TF list
        need_tfs = self.load_selections(NEXT_DAY_PATH)

        # 2) Historical warmup first (so strategies are ready before Kafka)
        print("[hydrate] starting via Kite…")
        await hydrate_from_kite(self.kite, self.states_by_tf)
        print("[hydrate] done.")

        # 3) Background tasks
        hb = asyncio.create_task(self.cash_heartbeat())
        ob = asyncio.create_task(self.orderbook_poll())
        sg = asyncio.create_task(self.squareoff_guard())
        pm = asyncio.create_task(self.pnl_monitor())
        # hm = asyncio.create_task(self.strategy_health_monitor(interval_sec=30))

        http_task = None
        if STATE_HTTP_ENABLED:
            http_task = asyncio.create_task(self.http_state_server())

        sw = asyncio.create_task(self.auto_switch_to_live())

        # 4) Kafka consumers (after warmup)
        tf_tasks = [asyncio.create_task(self._tf_runner(tf)) for tf in need_tfs]

        try:
            await asyncio.gather(*tf_tasks)
        finally:
            for t in (hb, ob, sg, pm, hm, http_task, sw):
                if t:
                    t.cancel()

# ---------- main ----------
if __name__ == "__main__":
    asyncio.run(LiveRunner().run())
