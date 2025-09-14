# backtest/grid_search.py
from __future__ import annotations
import os, yaml, itertools, math, json, csv
from datetime import datetime, timezone, timedelta, time as dtime
import asyncpg, asyncio
from typing import Dict, Any, List, Tuple
from strategy.base import Context, Bar
import importlib

PG_HOST=os.getenv("POSTGRES_HOST","localhost"); PG_PORT=int(os.getenv("POSTGRES_PORT","5432"))
PG_DB=os.getenv("POSTGRES_DB","trading"); PG_USER=os.getenv("POSTGRES_USER","trader"); PG_PASS=os.getenv("POSTGRES_PASSWORD","trader")

def parse_cfg(path="backtest/backtest.yaml")->Dict[str,Any]:
    return yaml.safe_load(open(path))

def ist(dt_utc:datetime)->datetime:
    return dt_utc.astimezone(timezone(timedelta(hours=5, minutes=30)))

def ist_minutes(dt_utc:datetime)->int:
    dt=ist(dt_utc); return dt.hour*60 + dt.minute

def combos(params:Dict[str,List[Any]])->List[Dict[str,Any]]:
    keys=list(params.keys()); vals=[params[k] if isinstance(params[k], list) else [params[k]] for k in keys]
    out=[]
    for prod in itertools.product(*vals):
        out.append({k:v for k,v in zip(keys, prod)})
    return out

async def load_bars(pool, symbols:List[str], start:str, end:str)->Dict[str,List[Bar]]:
    sql = """
      SELECT symbol, ts, o,h,l,c,vol,n_trades
      FROM bars_1m
      WHERE symbol = ANY($1::text[]) AND ts >= $2 AND ts <= $3
      ORDER BY ts
    """
    start_dt = datetime.fromisoformat(start+"T00:00:00+00:00")
    end_dt   = datetime.fromisoformat(end+"T23:59:59+00:00")
    out={s:[] for s in symbols}
    async with pool.acquire() as con:
        rows=await con.fetch(sql, symbols, start_dt, end_dt)
    for r in rows:
        out[r["symbol"]].append(Bar(
            ts=int(r["ts"].timestamp()), o=float(r["o"]), h=float(r["h"]), l=float(r["l"]), c=float(r["c"]),
            vol=int(r["vol"] or 0), n=int(r["n_trades"] or 0)
        ))
    return out

def cost_side(notional:float, slippage_bps:int, brokerage_fixed:float, tax_pct:float)->float:
    slip = notional * (slippage_bps/10000.0)
    taxes = notional * tax_pct
    return slip + taxes + brokerage_fixed

def sharpe(pnl_series:List[float])->float:
    if not pnl_series: return 0.0
    mu = sum(pnl_series)/len(pnl_series)
    var = sum((x-mu)**2 for x in pnl_series)/max(1, len(pnl_series)-1)
    sd = math.sqrt(max(var, 1e-12))
    return (mu / sd) * math.sqrt(252*6.5*60)  # rough scale from 1m

def make_instance(module:str, clsname:str, params:Dict[str,Any]):
    mod=importlib.import_module(module); cls=getattr(mod, clsname); return cls(**params)

async def run_one(pool, cfg:Dict[str,Any], name:str, module:str, cls:str, params:Dict[str,Any])->Dict[str,Any]:
    symbols = cfg["symbols"]; per_cap = float(cfg.get("per_stock_capital", 10000))
    sl_bps  = int(cfg.get("slippage_bps", 8))
    br_fixed= float(cfg.get("brokerage_fixed", 20))
    tax_pct = float(cfg.get("tax_pct", 0.001))
    eod_str = cfg.get("squareoff_ist", "15:15")
    eod_min = int(eod_str.split(":")[0])*60 + int(eod_str.split(":")[1])

    bars = await load_bars(pool, symbols, cfg["start"], cfg["end"])
    ctx  = Context()
    strat= make_instance(module, cls, params)

    total_pnl=0.0; n_trades=0; wins=0
    daily_pnls:List[float]=[]
    position:Dict[str, Tuple[int,float]]={} # sym -> (qty, entry_px)

    day_curr=None; day_pnl_acc=0.0

    for sym in symbols:
        for b in bars[sym]:
            # day roll
            dkey=ist(datetime.fromtimestamp(b.ts, tz=timezone.utc)).date()
            if day_curr is None: day_curr=dkey
            if dkey != day_curr:
                daily_pnls.append(day_pnl_acc); day_pnl_acc=0.0; day_curr=dkey

            ctx.push_bar(sym, b)
            qty, entry = position.get(sym, (0,0.0))

            # square-off time?
            if ist_minutes(datetime.fromtimestamp(b.ts, tz=timezone.utc)) >= eod_min and qty != 0:
                # exit at close
                notional = abs(qty)*b.c
                c = cost_side(notional, sl_bps, br_fixed, tax_pct)
                pnl = (b.c - entry)*qty - c
                total_pnl += pnl; day_pnl_acc += pnl; n_trades += 1; wins += (pnl>0)
                position.pop(sym, None)
                continue

            # no pyramiding
            if qty != 0: 
                continue

            # entry check
            sig = strat.decide(sym, b, ctx)
            if not sig: 
                continue
            side=sig["action"]
            # size by per-stock capital notional (simple backtest sizing)
            raw_qty = int(per_cap // max(b.c, 1e-6))
            if raw_qty <= 0: 
                continue
            # apply entry costs
            notional = raw_qty * b.c
            entry_cost = cost_side(notional, sl_bps, br_fixed, tax_pct)
            # store position
            position[sym] = (raw_qty if side=="BUY" else -raw_qty, b.c)
            total_pnl -= entry_cost; day_pnl_acc -= entry_cost

    # close any residual (safety)
    for sym,(q,e) in list(position.items()):
        # assume last price = entry (neutral)
        pass

    if day_curr is not None:
        daily_pnls.append(day_pnl_acc)
    sr=sharpe(daily_pnls)
    winr = (wins/max(1,n_trades))*100.0
    return {"name":name, "module":module, "class":cls, "params":params,
            "pnl": round(total_pnl,2), "trades": n_trades, "win_rate": round(winr,1), "sharpe": round(sr,3)}

async def main():
    cfg=parse_cfg()
    grids=cfg["grids"]
    tasks=[]
    pool=await asyncpg.create_pool(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)

    for g in grids:
        name=g["name"]; module=g["module"]; cls=g["class"]
        for p in combos(g["params"]):
            tasks.append(run_one(pool, cfg, f"{name}_{p}", module, cls, p))

    print(f"[bt] running {len(tasks)} combos…")
    results = await asyncio.gather(*tasks)
    # rank by sharpe then pnl
    results.sort(key=lambda r: (r["sharpe"], r["pnl"]), reverse=True)
    os.makedirs("backtest/results", exist_ok=True)
    with open("backtest/results/top5.json","w") as f: json.dump(results[:5], f, indent=2)
    with open("backtest/results/top5.csv","w", newline="") as f:
        w=csv.writer(f); w.writerow(["rank","name","pnl","trades","win_rate","sharpe","params"])
        for i,r in enumerate(results[:5],1):
            w.writerow([i,r["name"],r["pnl"],r["trades"],r["win_rate"],r["sharpe"],json.dumps(r["params"])])
    print("[bt] wrote backtest/results/top5.(json|csv)")
    await pool.close()

if __name__=="__main__":
    asyncio.run(main())
