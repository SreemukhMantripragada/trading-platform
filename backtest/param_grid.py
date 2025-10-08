# backtest/param_grid.py
from __future__ import annotations
from typing import Dict, Any, Iterable, List
import itertools
import json

def _expand(d: Dict[str, Iterable[Any]]) -> List[Dict[str, Any]]:
    if not d: return [dict()]
    keys = list(d.keys())
    vals = [list(v if isinstance(v, (list, tuple)) else [v]) for v in d.values()]
    out = []
    for combo in itertools.product(*vals):
        out.append({k: v for k, v in zip(keys, combo)})
    return out

def expand_params(params_cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Old behavior: expand flat params dict to list of dicts."""
    return _expand(params_cfg or {})

def expand_params_for_tf(strat_cfg: Dict[str, Any], tf: int) -> List[Dict[str, Any]]:
    """
    New: if 'params_by_tf' exists, use that map for the given timeframe (string key),
    else fall back to flat 'params'.
    """
    pbt = strat_cfg.get("params_by_tf")
    if pbt:
        block = pbt.get(str(tf)) or pbt.get(int(tf)) or {}
        return _expand(block)
    return expand_params(strat_cfg.get("params", {}))

def apply_constraint(param_sets: List[Dict[str, Any]], expr: str | None) -> List[Dict[str, Any]]:
    if not expr: return param_sets
    out = []
    for p in param_sets:
        # very small, safe eval context
        try:
            if eval(expr, {"__builtins__": {}}, dict(p)):
                out.append(p)
        except Exception:
            # if constraint errors out, skip that combo
            pass
    return out

def variant_key(name: str, params: Dict[str, Any]) -> str:
    return f"{name}__" + "_".join(f"{k}={v}" for k, v in sorted(params.items(), key=lambda kv: kv[0]))
