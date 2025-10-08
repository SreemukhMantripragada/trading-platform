# Scoring Engine

## Overview
`backtest/scorer.py` powers the selection logic for nightly batch runs, parameter sweeps, and next-day promotions. It standardizes how strategy configurations are filtered, scored, and promoted so downstream systems (risk, execution, live scheduling) receive consistent candidates.

## Core Functions
| Function | Purpose |
| --- | --- |
| `apply_constraints(rows, gates)` | Hard filters rows with configurable gates (minimum trades, win rate, maximum drawdown, etc.). Annotates rejected rows with `rejected_reason`. |
| `score_rows(rows, weights)` | Computes composite scores per row using robust z-scores across (strategy × timeframe) cohorts. Updates `row['score']` in place and returns rows sorted descending. |
| `aggregate_by_config(rows)` | Groups rows by `(strategy, timeframe, params)` for config-level evaluation. |
| `aggregate_by_symbol_config(rows)` | Groups by `(symbol, strategy, timeframe, params)` to preserve per-symbol insights. |
| `select_topk_per_symbol_tf(rows, k)` | Enforces per-symbol, per-timeframe cardinality constraints (e.g., top-3). |
| `decide_next_day(rows, policy)` | Applies policy rules (e.g., per-bucket limits, freshness requirements) to create next-day watchlists. |

## Data Model
Expected row fields (subset is acceptable; missing values are imputed):
- Identification: `symbol`, `strategy`, `timeframe`/`tf`/`tf_min`, `params` (dict), `n_trades`.
- Performance: `R`, `pnl_total`, `sharpe`, `sortino`, `win_rate`, `stability`, `max_dd`, `ulcer`.
- Microstructure/ops: `turnover`, `avg_hold_min`, `latency_ms`, `penalties`.
- Freshness: `asof`/`end_date`/`last_ts` to drive exponential decay.

## Defaults
- `DEFAULT_GATES`: require positive return, optional minimum trades, win-rate and drawdown ceilings, slippage caps.
- `DEFAULT_WEIGHTS`: prioritize normalized return (`w_return = 2.5`), Sharpe, Sortino; apply lower weights to turnover/hold penalties; support `neg_return_penalty` and optional `freshness_half_life_days`.
- Winsorization trims outliers (98% central mass) before scoring to reduce fragility from occasional extreme values.

## Customizing
1. Clone defaults: `gates = {**DEFAULT_GATES, "min_trades": 50, "max_drawdown": 0.12}`.
2. Adjust weights: favor faster turnover by increasing `w_turnover`, or penalize latency by setting `w_latency`.
3. Implement bespoke penalties: populate `row['penalties']` with aggregated cost estimates (positive numbers reduce scores).
4. Freshness decay: set `weights['freshness_half_life_days'] = 90.0` to halve scores every ~90 days without updates.

## Typical Pipeline
```python
from backtest import scorer

rows = load_backtest_results(...)  # list[dict]
filtered = scorer.apply_constraints(rows, {"min_trades": 75})
ranked = scorer.score_rows(filtered, {"w_return": 3.0, "neg_return_penalty": 3.0})
top_per_symbol = scorer.select_topk_per_symbol_tf(ranked, k=3)
next_day = scorer.decide_next_day(top_per_symbol, {"max_per_bucket": {"LIQUID": 5}})
save_watchlist(next_day)
```

## Downstream Consumers
- `backtest/grid_runner_parallel.py`: bulk grid search scoring.
- `tools/promote_topn.py`: produces `configs/next_day.yaml` for live trading.
- `analytics/pairs/find_pairs.py`: reuses scoring utilities for spread selection.
- `ui/backtest_app.py`: surfaces ranked outcomes for human review.

## Operational Tips
- Log rejections (`row['rejected_reason']`) to understand why promising configs drop out.
- Track score distributions per cohort; dramatic shifts often signal data integrity issues.
- Version-control weight/gate presets to maintain reproducibility between research and live promotion.
