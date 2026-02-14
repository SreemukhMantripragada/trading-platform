# Ingestion

## Responsibilities
- Stream live ticks from Zerodha Kite WebSockets into Kafka with schema validation.
- Backfill and merge historical bars to maintain the golden time series.
- Produce smoke data and consume dead-letter queues for operational hygiene.
- Maintain instrument metadata, auth tokens, and exchange-specific utilities.

## Services & Scripts
| Path | Description | Topics / Storage |
| --- | --- | --- |
| `ingestion/zerodha_ws.py` | Live tick consumer publishing to Kafka `ticks`; handles reconnects, symbol subscriptions, and JSON schema validation. | Kafka `ticks` |
| `ingestion/dlq_consumer.py` | Replays and classifies events from DLQ topics; stores failures for manual triage. | Kafka `*_dlq` |
| `ingestion/merge_hist_daily.py` | Periodic job merging vendor + Zerodha historical candles into Postgres. | Postgres `bars_*` |
| `ingestion/zerodha_hist_merge.py` | One-off script to reconcile historical bars between vendor and broker feeds. | Postgres `bars_1m`, parquet |
| `ingestion/backfill_60d_vendor.py` | Vendor backfill helper to seed 60 days of bars for new symbols. | Postgres `bars_*` |
| `ingestion/nfo_instruments.py` | Refresh NSE derivative instrument master, writing to Postgres/configs. | Postgres metadata |
| `ingestion/smoke_producer.py` | Generates deterministic tick streams for local dry runs. | Kafka `ticks`, `bars` |
| `ingestion/auth/` | Stores Kite API tokens (`token.json`) generated via `make kite-exchange`. | Filesystem |

## Configuration
- Environment: `.env` for broker/app secrets. Docker infra config is single-sourced in `configs/docker_stack.json` and rendered to `infra/.env.docker` via `make docker-config-sync` / `make up`.
- Auth: run `make kite-url` → `make kite-exchange REQUEST_TOKEN=...` to refresh the access token in `ingestion/auth/token.json`. Keep the file out of version control.
- Symbols: maintain tradable universe in `configs/tokens.csv` and `configs/universe_*.csv`.
- JSON Schemas: see `schemas/*.schema.json` to validate payload structure before publishing.

## Running Locally
- Live websocket bridge: `make ws` (loads `.env` and infra env defaults, streams to the default Kafka broker).
- DLQ inspection: `python ingestion/dlq_consumer.py --topic ticks.dlq --dump out.jsonl`.
- Historical merges: `make merge-hist D1=20240101 D2=20240105`.
- Instrument sync: `make nfo-sync`.

## Operational Notes
- DLQ consumers annotate failures with reason codes; triage outputs to avoid silent data loss.
- For topic-init dry runs, set `TOPICS_DRY_RUN` in `configs/docker_stack.json`, then run `make docker-config-sync` (or `make up`).
- Historical merge scripts assume Postgres connectivity; verify `infra/docker-compose.yml` is running (`make up`) before executing.
- For dry runs without broker credentials, set `KITE_API_KEY`, `KITE_API_SECRET`, `KITE_ACCESS_TOKEN` to dummy values and rely on smoke producers.
