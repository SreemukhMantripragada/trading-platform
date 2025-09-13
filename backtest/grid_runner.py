"""
backtest/grid_runner.py
- Loads grid spec (configs/grid.yaml)
- For each TF/strategy/param combo and symbol:
    * fetch bars from Postgres (bars_{tf}m)
    * run a simple long-only backtest loop using the strategy signal
    * apply slippage + costs
- Writes results to backtest_runs/backtest_results
- Prints Top-5 overall and saves to configs/next_day.yaml (via top5_export)

Notes:
- Strategy class must expose:
    name: str
    reset()
    on_bar(symbol, tf_str, ts, o,h,l,c,vol, features) -> Signal(side, stop_loss, take_profit, strength, reason)
  (This matches your live runner interface; if import fails, falls back to two built-ins.)
"""
from __future__ import annotations
import os, csv, math, yaml, asyncpg, importlib, ujson as json
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from typing import Dict, Any, List, Tuple

PG_HOST=os.getenv("POSTGRES_HOST","localhost")
PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading")
PG_USER=os.getenv("POSTGRES_USER","trader")
PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")

GRID_YAML=os.getenv("GRID_CFG","configs/grid.yaml")

@dataclass
class Sig:
    side: str
    stop_loss: float|None = None
    take_profit: float|None = None
    strength: float = 1.0
    reason: str = ""

# --- tiny built-ins if your modules aren't importable -------------------------
class _SMA:
    def __init__(self, fast=10, slow=20): self.f=fast; self.s=slow; self.buf=[]
    @property
    def name(self): return f"SMA_CROSS(f{self.f},s{self.s})"
    def reset(self): self.buf=[]
    def on_bar(self, sym, tf, ts, o,h,l,c,vol,features):
        self.buf.append(c); 
        if len(self.buf) < max(self.f,self.s): return Sig(side="HOLD")
        f = sum(self.buf[-self.f:])/self.f; s = sum(self.buf[-self.s:])/self.s
        if f > s:  return Sig(side="BUY", reason="f>s")
        if f < s:  return Sig(side="SELL", reason="f<s")
        return Sig(side="HOLD")

class _RSI:
    def __init__(self, period=14, overbought=70, oversold=30):
        self.p=period; self.ob=overbought; self.os=oversold; self.prev=None; self.g=[]; self.l=[]
    @property
    def name(self): return f"RSI_REV(p{self.p})"
    def reset(self): self.prev=None; self.g=[]; self.l=[]
    def on_bar(self, sym, tf, ts, o,h,l,c,vol,features):
        if self.prev is None: self.prev=c; return Sig(side="HOLD")
        ch=c-self.prev; self.prev=c
        self.g.append(max(0,ch)); self.l.append(max(0,-ch))
        if len(self.g)>self.p: self.g.pop(0); self.l.pop(0)
        if len(self.g)<self.p: return Sig(side="HOLD")
        avg_gain=sum(self.g)/self.p; avg_loss=sum(self.l)/self.p
        rs=float('inf') if avg_loss==0 else (avg_gain/avg_loss)
        rsi=100-(100/(1+rs))
        if rsi < self.os: return Sig(side="BUY",  reason=f"rsi={rsi:.1f}")
        if rsi > self.ob: return Sig(side="SELL", reason=f"rsi={rsi:.1f}")
        return Sig(side="HOLD")

def _load_symbols(path:str, limit:int)->List[str]:
    out=[]
    with open(path, newline="") as f:
        r=csv.DictReader(f)
        for row in r:
            s=(row.get("symbol") or row.get("tradingsymbol") or "").strip()
            if s: out.append(s)
    return out[:limit] if limit>0 else out

async def _fetch_bars(con, sym:str, tf:int, start:str, end:str)->List[Dict[str,Any]]:
    tbl = f"bars_{tf}m" if tf>1 else "bars_1m"
    sql = f"SELECT extract(epoch from ts)::bigint AS ts, o,h,l,c,vol FROM {tbl} WHERE symbol=$1 AND ts::date >= $2::date AND ts::date <= $3::date ORDER BY ts"
    rows = await con.fetch(sql, sym, start, end)
    return [{"ts": int(r["ts"]), "o": float(r["o"]), "h": float(r["h"]), "l": float(r["l"]), "c": float(r["c"]), "vol": int(r["vol"])} for r in rows]

def _slip(px:float, bps:int)->float:
    return px * (bps/10000.0)

def _apply_costs(entry_px:float, exit_px:float, qty:int, cfg:dict)->float:
    slip = _slip(entry_px, cfg["slippage_bps"]) + _slip(exit_px, cfg["slippage_bps"])
    gross = (exit_px - entry_px) * qty
    broker = 2 * cfg["brokerage_per_order_inr"]
    taxes  = (abs(entry_px)*qty + abs(exit_px)*qty) * cfg["taxes_pct"]
    return gross - broker - taxes - slip*qty

def _squareoff_ts(day_ts:int, hhmm:str, ist=timezone(timedelta(hours=5,minutes=30))) -> int:
    d=datetime.fromtimestamp(day_ts, tz=timezone.utc).astimezone(ist).date()
    t=datetime.combine(d, time.fromisoformat(hhmm), ist).astimezone(timezone.utc)
    return int(t.timestamp())

def _strat_instance(mod:str, cls:str, params:dict):
    try:
        M=importlib.import_module(mod); C=getattr(M, cls); return C(**params)
    except Exception:
        # fallbacks
        if cls.upper().startswith("SMA"): return _SMA(**params)
        return _RSI(**params)

async def main():
    spec=yaml.safe_load(open(GRID_YAML))
    symbols=_load_symbols(spec["universe"]["symbols_file"], spec["universe"]["limit"])
    tfs=[int(x) for x in spec["timeframes"]]
    costs=spec["costs"]; budget=spec["budget"]; start=spec["date"]["start"]; end=spec["date"]["end"]
    combos=[]
    for s in spec["strategies"]:
        base={"name": s["name"], "module": s["module"], "class": s["class"]}
        grids=s.get("params") or {}
        # cartesian
        keys=list(grids.keys())
        vals=[grids[k] for k in keys]
        def _recurse(i, cur):
            if i==len(keys):
                combos.append((base, cur.copy())); return
            k=keys[i]
            for v in vals[i]:
                cur[k]=v; _recurse(i+1, cur)
        _recurse(0, {})

    pool=await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
    async with pool.acquire() as con:
        r=await con.fetchrow(
            "INSERT INTO backtest_runs(label,start_date,end_date,tfs,strategies,costs) VALUES($1,$2,$3,$4,$5,$6) RETURNING run_id",
            spec.get("label","grid"), start, end, ",".join(map(str,tfs)), json.dumps(spec["strategies"]), json.dumps(costs)
        )
        run_id=int(r["run_id"])
    print(f"[grid] run_id={run_id} symbols={len(symbols)} tfs={tfs} combos={len(combos)}")

    try:
        async with pool.acquire() as con:
            for tf in tfs:
                tf_str = f"{tf}m"
                so_key = budget.get("squareoff_ist","15:15")
                for base, params in combos:
                    strat=_strat_instance(base["module"], base["class"], params)
                    sname=f'{base["name"]}'
                    for sym in symbols:
                        bars=await _fetch_bars(con, sym, tf, start, end)
                        if not bars: continue
                        # day square-off handling
                        day_so=None
                        pos_qty=0; entry_px=0.0
                        pnl=0.0; n=0; wins=0
                        eq=0.0; peak=0.0; max_dd=0.0
                        strat.reset()
                        for b in bars:
                            ts,o,h,l,c,vol = b["ts"], b["o"], b["h"], b["l"], b["c"], b["vol"]
                            # compute daily squareoff boundary once per day
                            if (day_so is None) or (ts >= day_so):
                                day_so=_squareoff_ts(ts, so_key)
                            sig = strat.on_bar(sym, tf_str, ts, o,h,l,c,vol, {})
                            # Exit at square-off if still in position
                            if pos_qty>0 and ts>=day_so:
                                pnl += _apply_costs(entry_px, c, pos_qty, costs); n+=1
                                if c>entry_px: wins+=1
                                pos_qty=0
                            # Signals: long-only (BUY to enter, SELL to exit)
                            if sig.side=="BUY" and pos_qty==0:
                                # size by backtest budget
                                qty = int(max(0, budget["per_symbol_inr"] // max(c,1)))
                                if qty>0: pos_qty=qty; entry_px=c
                            elif sig.side in ("SELL","EXIT") and pos_qty>0:
                                pnl += _apply_costs(entry_px, c, pos_qty, costs); n+=1
                                if c>entry_px: wins+=1
                                pos_qty=0
                            # equity curve approximation (mark-to-market)
                            eq = pnl + (pos_qty*(c-entry_px) if pos_qty>0 else 0.0)
                            peak=max(peak, eq); max_dd=max(max_dd, peak-eq)
                        # close residual at final bar
                        if pos_qty>0:
                            c=bars[-1]["c"]
                            pnl += _apply_costs(entry_px, c, pos_qty, costs); n+=1
                            if c>entry_px: wins+=1
                        win_rate=(wins/n) if n>0 else None
                        sharpe=None  # keep simple for now
                        await con.execute(
                            "INSERT INTO backtest_results(run_id,symbol,strategy,tf_min,params,gross_pnl,net_pnl,n_trades,win_rate,max_dd,sharpe) "
                            "VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) "
                            "ON CONFLICT (run_id,symbol,strategy,tf_min,params) DO NOTHING",
                            run_id, sym, sname, tf, json.dumps(params), pnl, pnl, n, win_rate, max_dd, sharpe
                        )
        # preview Top-5
        async with pool.acquire() as con:
            rows=await con.fetch(
              "SELECT symbol,strategy,tf_min,params,net_pnl,n_trades,win_rate FROM backtest_results "
              "WHERE run_id=$1 ORDER BY net_pnl DESC LIMIT 5", run_id)
        print("[grid] Top-5:")
        for r in rows:
            print(f"  {r['symbol']} {r['strategy']} {r['tf_min']}m pnl=₹{r['net_pnl']:.0f} trades={r['n_trades']} wr={r['win_rate']}")
    finally:
        await pool.close()

if __name__=="__main__":
    import asyncio; asyncio.run(main())
