# Compute

## Responsibilities
- Normalize high-frequency ticks into bar aggregates across multiple timeframes.
- Run derived signal producers (pair watchlists, smoke producers) for testing and analytics.
- Persist golden bars to Postgres and archive long-term history to object storage.

## Pipelines
| Path | Description | Topics / Storage |
| --- | --- | --- |
| `compute/bar_builder_1s.py` | Consumes Kafka `ticks`, performs price/volume sanity checks, and emits normalized 1-second bars. | Kafka `bars.1s` |
| `compute/bar_aggregator_1m.py` | Roll up 1s bars to 1-minute OHLCV and load into Postgres `bars_1m`. | Kafka `bars.1m`, Postgres |
| `compute/bar_aggregator_1m_to_multi.py` | Derive higher timeframes (3m/5m/15m/…) from the canonical 1m stream and fan out to dedicated topics. | Kafka `bars.{tf}`, Postgres |
| `compute/pair_watch_producer.py` | Emits correlation/cointegration metrics for candidate pairs. | Kafka `pairs.signals` |
| `compute/sinks/s3_archiver.py` | Nightly parquet archive job for bars beyond on-disk retention (>100d). | S3/object storage |
| `compute/smoke_orders_producer.py` | Generates sample orders for stress testing downstream services. | Kafka `orders`, `orders.dlq` |
| `compute/smoke_fill_producer.py` | Emits synthetic fills to validate accounting and OMS behavior. | Kafka `fills` |

## Configuration
- Timeframe list and retention controlled via environment variables (`APP_BARS1S_*`, `BARS*_RETENTION_MS` in `infra/.env`).
- Postgres connection parameters (`POSTGRES_*`) loaded by each process.
- Topic names default to `bars.{tf}`; override via `IN_TOPIC`, `OUT_TOPIC`, and `TF` environment variables.

## Running Locally
- 1s builder: `python compute/bar_builder_1s.py`
- 1m aggregator: `python compute/bar_aggregator_1m.py`
- Multi-timeframe aggregator: `make agg-multi`
- S3 archive dry run: `AWS_PROFILE=dev python compute/sinks/s3_archiver.py --start 2024-01-01 --end 2024-01-02 --dry-run`

## Operational Notes
- Keep the JSON Schemas under `schemas/` aligned with emitted payloads when modifying builders.
- Aggregators rely on strictly ordered timestamps; clock skew in data producers can inflate partial-bar counts.
- S3 archiver expects AWS credentials via standard environment variables (`AWS_ACCESS_KEY_ID`, etc.).
- Smoke producers publish to dedicated topics by default; gate them behind feature flags in production environments.
