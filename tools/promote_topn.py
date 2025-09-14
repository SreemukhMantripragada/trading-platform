"""
tools/promote_topn.py
Reads top-N runs from bt_* tables and writes configs/next_day.yaml.

Run:
  . .venv/bin/activate && TF=1m N=5 python tools/promote_topn.py
"""
from __future__ import annotations
import os, asyncio, yaml
from backtest.results_store import ResultsStore

TF=os.getenv("TF","1m"); N=int(os.getenv("N","5"))
OUT=os.getenv("OUT","configs/next_day.yaml")

TEMPLATE_HEADER = {"use":[]}

async def main():
    rs=ResultsStore()
    top = await rs.top_n(tf=TF, metric="sharpe", n=N, min_trades=10)
    doc={"use":[]}
    for r in top:
        # strategy-level promotion; symbol-level selection can be added if desired
        doc["use"].append({"strategy": r["strategy"], "tf": 1 if TF.endswith("m") else 1, "params": r["params_json"]})
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w") as f:
        yaml.safe_dump(doc, f, sort_keys=False)
    print(f"[promote] wrote {OUT} with {len(doc['use'])} entries")

if __name__=="__main__":
    asyncio.run(main())
