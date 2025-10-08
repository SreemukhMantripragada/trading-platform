# Execution

## Responsibilities
- Provide an order management system (OMS) that deduplicates, persists, and routes orders to paper or live brokers.
- Support hybrid execution flows: paper emulator, C++ price-time matcher, and Zerodha gateway.
- Publish fills back into Kafka/Postgres for downstream accounting and analytics.

## Components
| Path | Description | Inputs → Outputs |
| --- | --- | --- |
| `execution/oms.py` | Central coordinator handling idempotency, retries, and routing decisions. | Kafka `orders.allowed` → Kafka `orders.exec`, Postgres `live_orders` |
| `execution/paper_gateway.py` | Simple paper broker emulator for development. | Kafka `orders.exec` → Kafka `fills` |
| `execution/paper_gateway_matcher.py` | Paper gateway backed by the C++ matcher for realistic queue priority. | Kafka `orders.exec` → Kafka `fills` |
| `execution/cpp/matcher/` | Native matching engine compiled via `make matcher-build`. | FIFO/price-time matching |
| `execution/zerodha_gateway.py` | Talks to Kite REST API; respects rate limits via token bucket. | Kafka `orders.allowed` → Zerodha, Kafka `fills` |
| `execution/zerodha_poller.py` | Polls Broker orderbook to reconcile acknowledgements and partial fills. | Zerodha → Postgres, Kafka `fills` |
| `execution/shadow_mirror.py` | Mirrors live orders to a paper queue for shadow execution. | Kafka `orders`, `orders.shadow` |
| `execution/exit_engine.py` | Emits exit orders based on strategy/risk signals. | Kafka `orders`, `fills` |
| `execution/cancel_listener.py` | Listens for cancel broadcasts and applies them to live orders. | Kafka `cancel`, Postgres |

## Modes
- **Dry run**: Set `DRY_RUN=1` (default) to bypass API calls; fills are simulated at reference prices.
- **Paper**: Route through `paper_gateway_matcher.py` to test interaction of strategies, risk, and OMS with realistic fills.
- **Live**: Enable `DRY_RUN=0`, ensure valid Kite token, and start `zerodha_gateway.py` alongside `zerodha_poller.py` for status reconciliation.

## Configuration
- Environment variables: `IN_TOPIC`, `OUT_TOPIC`, `FILL_TOPIC`, `MATCHER_HOST`, `MATCHER_PORT`, `BROKER_RATE_PER_SEC`, `KITE_API_KEY`, `ZERODHA_TOKEN_FILE`.
- Postgres schemas: `live_orders`, `fills`, `executions`, `kill_switches`.
- Kill switch: `libs/killswitch.py` can be toggled to gracefully stop all execution components.

## Running Locally
- Paper matcher flow: `make exec-paper-matcher` (starts risk → paper gateway + matcher).
- Live gateway dry run: `make gateway-dry` and `make poller-dry`.
- Live trading: `make zerodha-live-allowed` followed by `make poller-live`.
- Kill switch broadcast: `make kill-all B=all`.

## Operational Notes
- Always bring up `risk/order_budget_guard.py` before routing to `zerodha_gateway.py` to preserve throttles.
- `zerodha_gateway.py` writes state to `live_orders`; monitor this table for stuck orders or backoffs.
- Prometheus metrics are exposed via ports declared in the Makefile (e.g., `monitoring/gateway_oms_exporter.py` on `8017`).
- Matcher binary lives in `execution/cpp/matcher/build/matcher`; rebuild when modifying C++ sources.
