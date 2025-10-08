#!/usr/bin/env python3
"""
pick_best_config.py

Rank multiple next_day.yaml-like strategy configs for Nifty intraday.
- If you only have YAML files: uses heuristics grounded in typical Nifty intraday behavior.
- If you also supply a backtest CSV: uses data-driven scoring (Sharpe, drawdown, win rate, etc.).

USAGE
-----
1) Put your YAML configs in a folder, e.g. ./configs
2) (Optional) Put a backtest CSV in the same folder (or pass --results path)
   Expected columns (case-insensitive, flexible names handled):
     - config / config_name (filename or label)
     - sharpe
     - cagr or return or avg_profit_per_trade
     - max_dd or drawdown
     - win_rate or wr
     - trades or n_trades
     - exposure or capital_usage (optional)
3) Run:
   python pick_best_config.py --dir ./configs [--results ./configs/results.csv] [--top 10]

OUTPUT
------
- Prints a ranked table to stdout
- Writes ranked_results.csv in the directory
"""

import argparse
import os
import sys
import glob
import re
import pandas as pd
import numpy as np

# --- Simple YAML parser (works for key: value configs) ---
def simple_yaml_load(text):
    data = {}
    stack = [data]
    indents = [0]
    last_key_at_indent = {}
    for line in text.splitlines():
        if not line.strip() or line.strip().startswith("#"):
            continue
        indent = len(line) - len(line.lstrip(' '))
        key_val = line.strip().split(':', 1)
        if len(key_val) != 2:
            continue
        key = key_val[0].strip()
        val = key_val[1].strip()
        while indents and indent < indents[-1]:
            stack.pop()
            indents.pop()
        if val == "":
            new_dict = {}
            stack[-1][key] = new_dict
            stack.append(new_dict)
            indents.append(indent + 2)  # assume 2-space nesting
            last_key_at_indent[indent] = key
        else:
            if re.match(r'^-?\d+(\.\d+)?$', val):
                coerced = float(val) if ('.' in val) else int(val)
            elif val.lower() in ("true", "false"):
                coerced = (val.lower() == "true")
            else:
                coerced = val.split('#')[0].strip()
            stack[-1][key] = coerced
    return data

# --- Key sets for detection ---
TP_KEYS = ["tp_pct","take_profit","take_profit_pct"]
SL_KEYS = ["sl_pct","stop_loss","stop_loss_pct"]
TSL_KEYS = ["trailing_stop_pct","tsl_pct","trail_pct"]
NAME_KEYS = ["name","strategy_name","label"]

def flatten(d, parent_key="", out=None):
    if out is None: out = {}
    for k, v in d.items():
        nk = f"{parent_key}.{k}" if parent_key else k
        if isinstance(v, dict):
            flatten(v, nk, out)
        else:
            out[nk] = v
    return out

def extract_params(cfg, fname):
    flat = flatten(cfg)
    def find_any(keys):
        for k, v in flat.items():
            base = k.split('.')[-1].lower()
            if base in [kk.lower() for kk in keys]:
                return v
        return None
    tp = find_any(TP_KEYS)
    sl = find_any(SL_KEYS)
    tsl = find_any(TSL_KEYS)
    rr = None
    if tp is not None and sl is not None and sl != 0:
        rr = float(tp) / float(sl)
    name = cfg.get("name", os.path.basename(fname))
    return {
        "name": name,
        "file": os.path.basename(fname),
        "tp_pct": tp,
        "sl_pct": sl,
        "rr": rr,
        "trailing_stop_pct": tsl,
    }

def heuristic_score(row):
    score = 0.0
    tp = row.get("tp_pct")
    sl = row.get("sl_pct")
    rr = row.get("rr")
    tsl = row.get("trailing_stop_pct")

    def bump(x, low, high, w_good=1.0, w_ok=0.5):
        if x is None: return 0
        if low <= x <= high:
            return w_good
        dist = min(abs(x - low), abs(x - high))
        return max(0.0, w_ok - 0.1 * dist)

    if tp is not None:
        score += bump(tp, 0.5, 1.5, 1.5, 0.6)
        if tp < 0.1 or tp > 3.0: score -= 1.0
    if sl is not None:
        score += bump(sl, 0.3, 1.0, 1.2, 0.5)
        if sl < 0.1 or sl > 3.0: score -= 1.0
    if rr is not None:
        score += bump(rr, 1.2, 2.0, 1.2, 0.5)
    if tsl is not None:
        score += 0.2
    return score
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", required=True, help="Directory with YAML configs")
    ap.add_argument("--results", default=None, help="Optional backtest results CSV path")
    ap.add_argument("--top", type=int, default=3, help="How many top rows to print")
    args = ap.parse_args()
    print(1)
    # Only pick files starting with next_day*
    yaml_paths = sorted(
        glob.glob(os.path.join(args.dir, "next_day*.yml"))
        + glob.glob(os.path.join(args.dir, "next_day*.yaml"))
    )
    print(yaml_paths)
    print(1)
    if not yaml_paths:
        print(f"No YAML files starting with 'next_day' found in {args.dir}", file=sys.stderr)
        sys.exit(1)

    rows = []
    for p in yaml_paths:
        try:
            with open(p, "r", encoding="utf-8") as f:
                cfg_text = f.read()
            cfg = simple_yaml_load(cfg_text)
            rows.append(extract_params(cfg, p))
        except Exception as e:
            rows.append({"name": os.path.basename(p), "file": os.path.basename(p), "error": str(e)})

    df = pd.DataFrame(rows)
    df["heuristic_score"] = df.apply(heuristic_score, axis=1)
    df = df.sort_values("heuristic_score", ascending=False).reset_index(drop=True)
    df.insert(0, "rank", range(1, len(df)+1))

    display_cols = ["rank","name","file","tp_pct","sl_pct","rr","trailing_stop_pct","heuristic_score"]
    print(df[display_cols].to_string(index=False))

    out_path = os.path.join(args.dir, "ranked_results.csv")
    df.to_csv(out_path, index=False)
    print(f"\nSaved: {out_path}")
