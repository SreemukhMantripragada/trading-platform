
.PHONY: up down ps logs topics doctor matcher-build matcher-run strat-1m strat-5m risk-v2 exec-paper-matcher agg-multi

up: 
	cd infra && docker compose up -d && docker compose ps
down: 
	cd infra && docker compose down -v
ps: 
	cd infra && docker compose ps
logs: 
	cd infra && docker compose logs -f

topics:
	docker cp infra/scripts/create-topics.sh kafka:/usr/local/bin/create-topics
	docker exec -t kafka bash -lc 'BROKER=kafka:29092 RF=1 DRY_RUN=0 /usr/local/bin/create-topics'
doctor: 
	python3 monitoring/doctor.py

matcher-build:
	cd execution/cpp/matcher && cmake -S . -B build && cmake --build build -j

matcher-run:
	./execution/cpp/matcher/build/matcher

strat-1m:
	. .venv/bin/activate && TF=1m IN_TOPIC=bars.1m KAFKA_BROKER=${KAFKA_BOOT:-localhost:9092} \
	POSTGRES_HOST=${POSTGRES_HOST:-localhost} POSTGRES_PORT=${POSTGRES_PORT:-5432} \
	POSTGRES_DB=${POSTGRES_DB:-trading} POSTGRES_USER=${POSTGRES_USER:-trader} POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-trader} \
	python strategy/runner_modular.py

strat-5m:
	. .venv/bin/activate && TF=5m IN_TOPIC=bars.5m KAFKA_BROKER=${KAFKA_BOOT:-localhost:9092} \
	POSTGRES_HOST=${POSTGRES_HOST:-localhost} POSTGRES_PORT=${POSTGRES_PORT:-5432} \
	POSTGRES_DB=${POSTGRES_DB:-trading} POSTGRES_USER=${POSTGRES_USER:-trader} POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-trader} \
	python strategy/runner_modular.py

risk-v2:
	. .venv/bin/activate && IN_TOPIC=orders OUT_TOPIC=orders.sized KAFKA_BROKER=${KAFKA_BOOT:-localhost:9092} \
	POSTGRES_HOST=${POSTGRES_HOST:-localhost} POSTGRES_PORT=${POSTGRES_PORT:-5432} \
	POSTGRES_DB=${POSTGRES_DB:-trading} POSTGRES_USER=${POSTGRES_USER:-trader} POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-trader} \
	python risk/manager_v2.py

exec-paper-matcher:
	. .venv/bin/activate && IN_TOPIC=orders.sized KAFKA_BROKER=${KAFKA_BOOT:-localhost:9092} \
	POSTGRES_HOST=${POSTGRES_HOST:-localhost} POSTGRES_PORT=${POSTGRES_PORT:-5432} \
	POSTGRES_DB=${POSTGRES_DB:-trading} POSTGRES_USER=${POSTGRES_USER:-trader} POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-trader} \
	MATCHER_HOST=127.0.0.1 MATCHER_PORT=5555 \
	python execution/paper_gateway_matcher.py

agg-multi:
	. .venv/bin/activate && IN_TOPIC=bars.1m KAFKA_BROKER=${KAFKA_BOOT:-localhost:9092} \
	POSTGRES_HOST=${POSTGRES_HOST:-localhost} POSTGRES_PORT=${POSTGRES_PORT:-5432} \
	POSTGRES_DB=${POSTGRES_DB:-trading} POSTGRES_USER=${POSTGRES_USER:-trader} POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-trader} \
	python compute/bar_aggregator_from_1m.py

.PHONY: hist-merge
hist-merge:
	. .venv/bin/activate && D1=${D1} D2=${D2} python ingestion/zerodha_hist_merge.py

.PHONY: ws hist-merge recon-v2 pairs-make pairs-monitor bt-ui
ws:
	. .venv/bin/activate && python ingestion/zerodha_ws_v2.py
recon-v2:
	. .venv/bin/activate && python monitoring/daily_recon_v2.py --date ${D}
pairs-make:
	. .venv/bin/activate && python research/pairs_maker.py --symbols ${SYMS} --days ${DAYS:-60}
pairs-monitor:
	. .venv/bin/activate && python strategy/pairs_monitor.py
bt-ui:
	. .venv/bin/activate && streamlit run ui/backtest_app.py --server.address 0.0.0.0 --server.port 8501

# --- Gateway/OMS exporter ---
exporter-gw:
	. .venv/bin/activate && \
	METRICS_PORT=8016 \
	POSTGRES_HOST=${POSTGRES_HOST} POSTGRES_PORT=${POSTGRES_PORT} POSTGRES_DB=${POSTGRES_DB} POSTGRES_USER=${POSTGRES_USER} POSTGRES_PASSWORD=${POSTGRES_PASSWORD} \
	python monitoring/gateway_oms_exporter.py

# --- EOD one-button ---
eod:
	. .venv/bin/activate && python orchestrator/eod_pipeline.py

exporter-oms:
	. .venv/bin/activate && \
	METRICS_PORT=8017 \
	POSTGRES_HOST=${POSTGRES_HOST} POSTGRES_PORT=${POSTGRES_PORT} POSTGRES_DB=${POSTGRES_DB} POSTGRES_USER=${POSTGRES_USER} POSTGRES_PASSWORD=${POSTGRES_PASSWORD} \
	python monitoring/oms_lifecycle_exporter.py

# Exporters
exporter-dd:
	. .venv/bin/activate && METRICS_PORT=8018 python monitoring/risk_drawdown_exporter.py

# Kill broadcast + cancel listener
kill-all:
	. .venv/bin/activate && python execution/kill_broadcast.py --scope ALL --reason "manual"

kill-bucket:
	. .venv/bin/activate && python execution/kill_broadcast.py --scope BUCKET --bucket $(B) --reason "manual"

cancel-listener:
	. .venv/bin/activate && python execution/cancel_listener.py

# Shadow toggle (start/stop processes your way; here foreground helpers)
shadow-on:
	. .venv/bin/activate && KAFKA_BROKER=${KAFKA_BOOT:-localhost:9092} IN_TOPIC=orders OUT_TOPIC=orders.paper python execution/shadow_mirror.py
# ---- APPEND THESE TARGETS TO YOUR EXISTING Makefile ----

pairs-exp:
	. .venv/bin/activate && METRICS_PORT=8019 python monitoring/pairs_exporter.py

merge-hist:
	. .venv/bin/activate && python ingestion/merge_hist_daily.py

archive-day:
	. .venv/bin/activate && python compute/sinks/s3_archiver.py

promote-topn:
	. .venv/bin/activate && TF=$(TF) N=$(N) python tools/promote_topn.py

rebalance:
	. .venv/bin/activate && BASE_RISK=configs/risk_budget.yaml OUT_RISK=configs/risk_budget.runtime.yaml \
		python risk/rebalance_daemon.py


# Optional: one-shot Grafana import via API (requires admin creds)
grafana-import-backtest:
	curl -sS -u $${GF_USER:-admin}:$${GF_PASS:-admin} -H 'Content-Type: application/json' \
		-X POST $${GF_URL:-http://localhost:3000}/api/dashboards/db \
		--data-binary @infra/grafana/dashboards/backtest_results.json | jq .

grafana-import-pairs:
	curl -sS -u $${GF_USER:-admin}:$${GF_PASS:-admin} -H 'Content-Type: application/json' \
		-X POST $${GF_URL:-http://localhost:3000}/api/dashboards/db \
		--data-binary @infra/grafana/dashboards/pairs.json | jq .

# ---- APPEND TO Makefile ----
pairs-exec:
	. .venv/bin/activate && KAFKA_BROKER=${KAFKA_BOOT:-localhost:9092} \
	IN_TOPIC=pairs.signals OUT_TOPIC=orders METRICS_PORT=8020 \
	RISK_BUDGET=configs/risk_budget.runtime.yaml \
	python execution/pairs_executor.py

pairs-sim:
	. .venv/bin/activate && KAFKA_BROKER=${KAFKA_BOOT:-localhost:9092} python tools/sim_pairs_signal.py

topic-pairs:
	docker exec -it kafka bash -lc "kafka-topics.sh --bootstrap-server kafka:29092 --create --if-not-exists --topic pairs.signals --replication-factor 1 --partitions 3"

grafana-import-pairs-pnl:
	curl -sS -u $${GF_USER:-admin}:$${GF_PASS:-admin} -H 'Content-Type: application/json' \
		-X POST $${GF_URL:-http://localhost:3000}/api/dashboards/db \
		--data-binary @infra/grafana/dashboards/pairs_pnl.json | jq .

# ---- append to Makefile ----
oms-health:
	. .venv/bin/activate && METRICS_PORT=8016 python execution/oms_health_exporter.py

recon-broker:
	. .venv/bin/activate && METRICS_PORT=8021 ENFORCE=0 QTY_TOL=0 BREACH_SYMS=0 KILL_QTY_TOL=0 \
		python monitoring/broker_reconciler.py

grafana-import-oms:
	curl -sS -u $${GF_USER:-admin}:$${GF_PASS:-admin} -H 'Content-Type: application/json' \
		-X POST $${GF_URL:-http://localhost:3000}/api/dashboards/db \
		--data-binary @infra/grafana/dashboards/oms_health.json | jq .

budget-exp:
	. .venv/bin/activate && METRICS_PORT=8022 python risk/budget_source.py

nfo-sync:
	. .venv/bin/activate && python ingestion/nfo_instruments.py

zerodha-canary:
	. .venv/bin/activate && DRY_RUN=1 KAFKA_BROKER=${KAFKA_BOOT:-localhost:9092} \
		IN_TOPIC=orders FILL_TOPIC=fills METRICS_PORT=8017 python execution/zerodha_gateway.py

zerodha-live:
	. .venv/bin/activate && DRY_RUN=0 KAFKA_BROKER=${KAFKA_BOOT:-localhost:9092} \
		IN_TOPIC=orders FILL_TOPIC=fills METRICS_PORT=8017 python execution/zerodha_gateway.py

# --- append to Makefile ---
budget-guard:
	. .venv/bin/activate && KAFKA_BROKER=${KAFKA_BOOT:-localhost:9092} \
	IN_TOPIC=orders OUT_TOPIC=orders.allowed METRICS_PORT=8023 \
	python risk/order_budget_guard.py

zerodha-canary-allowed:
	. .venv/bin/activate && DRY_RUN=1 KAFKA_BROKER=${KAFKA_BOOT:-localhost:9092} \
	IN_TOPIC=orders.allowed FILL_TOPIC=fills METRICS_PORT=8017 python execution/zerodha_gateway.py

zerodha-live-allowed:
	. .venv/bin/activate && DRY_RUN=0 KAFKA_BROKER=${KAFKA_BOOT:-localhost:9092} \
	IN_TOPIC=orders.allowed FILL_TOPIC=fills METRICS_PORT=8017 python execution/zerodha_gateway.py

# --- append to Makefile ---

oms-ddl:
	docker exec -i postgres psql -U ${POSTGRES_USER:-trader} -d ${POSTGRES_DB:-trading} -f infra/postgres/init/10_oms.sql

gateway-dry:
	. .venv/bin/activate && DRY_RUN=1 KAFKA_BROKER=${KAFKA_BOOT:-localhost:9092} \
	IN_TOPIC=orders.allowed FILL_TOPIC=fills python execution/zerodha_gateway.py

gateway-live:
	. .venv/bin/activate && DRY_RUN=0 KAFKA_BROKER=${KAFKA_BOOT:-localhost:9092} \
	IN_TOPIC=orders.allowed FILL_TOPIC=fills python execution/zerodha_gateway.py

exit-engine:
	. .venv/bin/activate && KAFKA_BROKER=${KAFKA_BOOT:-localhost:9092} \
	OUT_TOPIC=orders EXIT_SCAN_SEC=5 python execution/exit_engine.py

# --- append to Makefile ---

poller-dry:
	. .venv/bin/activate && DRY_RUN=1 POLL_SEC=4 \
	KAFKA_BROKER=${KAFKA_BOOT:-localhost:9092} FILL_TOPIC=fills \
	python execution/zerodha_poller.py

poller-live:
	. .venv/bin/activate && DRY_RUN=0 POLL_SEC=4 \
	KAFKA_BROKER=${KAFKA_BOOT:-localhost:9092} FILL_TOPIC=fills \
	python execution/zerodha_poller.py

bt-grid:
	. .venv/bin/activate && python backtest/grid_search.py

# --- append to Makefile ---

pairs-ddl:
	docker exec -i postgres psql -U ${POSTGRES_USER:-trader} -d ${POSTGRES_DB:-trading} \
		-f infra/postgres/init/12_pairs.sql

pairs-scan:
	. .venv/bin/activate && PAIRS_LOOKBACK_DAYS=45 PAIRS_PROMOTE_TOP_K=8 \
	python backtest/pairs_scan.py

pairs-live:
	. .venv/bin/activate && KAFKA_BROKER=${KAFKA_BOOT:-localhost:9092} \
	IN_TOPIC=bars.1m OUT_TOPIC=orders PAIRS_PER_TRADE_NOTIONAL=20000 \
	python strategy/pairs_live.py


# ===== Zerodha auth helpers =====
.PHONY: kite-url kite-url-open kite-exchange kite-login kite-verify kite-clear-token

kite-url:
	. .venv/bin/activate; python -c 'import os; from dotenv import load_dotenv; load_dotenv(".env"); load_dotenv("infra/.env"); ak=os.getenv("KITE_API_KEY"); redir=os.getenv("KITE_REDIRECT_URL") or "https://127.0.0.1/"; assert ak, "KITE_API_KEY missing"; print(f"https://kite.trade/connect/login?api_key={ak}&v=3&redirect_params=redirect_url={redir}")'

kite-url-open:
	@URL="$$(make -s kite-url)"; echo "$$URL"; open "$$URL"

# Usage: make kite-exchange REQUEST_TOKEN=xxxxxxxxxxxxxxxx
kite-exchange:
	@test -n "$(REQUEST_TOKEN)" || (echo "Usage: make kite-exchange REQUEST_TOKEN=..." && exit 1)
	. .venv/bin/activate; python -c 'import os, json, time; from dotenv import load_dotenv; from kiteconnect import KiteConnect; load_dotenv(".env"); load_dotenv("infra/.env"); ak=os.getenv("KITE_API_KEY"); sk=os.getenv("KITE_API_SECRET"); rt=os.getenv("REQUEST_TOKEN") or "$(REQUEST_TOKEN)"; assert ak and sk, "KITE_API_KEY/SECRET missing"; assert rt, "REQUEST_TOKEN missing"; k=KiteConnect(api_key=ak); tok=k.generate_session(rt, api_secret=sk)["access_token"]; os.makedirs("ingestion/auth", exist_ok=True); json.dump({"api_key":ak,"access_token":tok,"ts":int(time.time())}, open("ingestion/auth/token.json","w"), indent=2); print("✅ Saved access_token to ingestion/auth/token.json")'

kite-verify:
	. .venv/bin/activate; python -c 'import os, json; from dotenv import load_dotenv; from kiteconnect import KiteConnect; load_dotenv(".env"); load_dotenv("infra/.env"); ak=os.getenv("KITE_API_KEY"); T=json.load(open("ingestion/auth/token.json")); tok=T.get("access_token"); assert tok, "token.json missing/empty"; k=KiteConnect(api_key=ak); k.set_access_token(tok); p=k.profile(); print("✅ Token valid for:", p.get("user_id"), p.get("user_name"))'

kite-clear-token:
	rm -f ingestion/auth/token.json; echo "🗑  deleted ingestion/auth/token.json"
