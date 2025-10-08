# Strategy

## Responsibilities
- Transform time-bar data into trading signals across single-name, ensemble, and pairs strategies.
- Persist signal states and candidate orders to Postgres for auditability.
- Provide reusable indicator and strategy abstractions for rapid experimentation.

## Runners & Engines
| Path | Description | Inputs → Outputs |
| --- | --- | --- |
| `strategy/runner_modular.py` | Async runner that wires indicators, strategies, and ensembles from YAML configs; writes to `orders` and `orders` table. | Kafka `bars.{tf}` → Kafka `orders`, Postgres `orders` |
| `strategy/runner_unified.py` | Legacy unified runner supporting multiple timeframes within one process. | Kafka `bars.*` → Kafka `orders` |
| `strategy/runner_ensemble.py` | Coordinates ensemble voting on top of per-strategy signals. | Kafka `signals.*` → Kafka `orders` |
| `strategy/pairs_live.py` | Executes cointegration-based pairs trading, producing entry/exit orders. | Kafka `bars.1m` → Kafka `orders` |
| `strategy/pairs_monitor.py` | Observability companion tracking live spread metrics. | Kafka `pairs.signals` → Prometheus / logs |

## Strategy Catalog
- `strategy/base.py`: Base classes defining lifecycle hooks (`on_bar`, `on_tick`, `on_snapshot`) and state helpers.
- `strategy/strategies/`: Directory of concrete strategies (e.g., `sma_cross`, `ema_cross`, `bollinger_breakout`, `ensemble_kofn`).
- `strategy/ensemble_engine.py`: Majority voting / K-of-N aggregator supporting weights and minimum score filters.
- `strategy/registry.yaml`: Declarative list of strategies, parameters, and timeframes.
- `strategy/ensemble.yaml`: Ensemble configuration (mode, weights, quorum).
- `configs/indicators.yaml`: Indicator definitions per timeframe (SMA, RSI, ATR, etc.).

## Configuration Workflow
1. Update `configs/indicators.yaml` with indicator parameters for each timeframe.
2. Register strategies with parameters and eligible timeframes in `strategy/registry.yaml`.
3. Tune ensemble behavior in `strategy/ensemble.yaml` (e.g., `mode: majority`, `k: 3`).
4. Populate `configs/next_day*.yaml` with watchlists and notional sizes for overnight scheduling.

## Running Locally
- 1-minute runner: `make strat-1m`
- 5-minute runner: `make strat-5m`
- Pairs live: `make pairs-live`
- Ensemble monitor: `python strategy/runner_ensemble.py --tf 1m`

## Development Notes
- Indicators live in `indicators/` and return dictionaries with `ready` flags; ensure strategies gate on readiness.
- Signals flow through `libs/signal.py` (e.g., `Signal(side="BUY", tags={"reason": "crossover"})`).
- Persisted orders include `extra.signal` payloads; downstream risk systems expect `entry_px`, `stop_px`, and `confidence` fields when available.
- Use `backtest/engine.py` for offline validation before promoting strategies to runners. Results are stored via `backtest/results_store.py`.
