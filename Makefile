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
	docker exec -i kafka bash -lc 'BROKER=kafka:29092; for t in ticks "bars.1s" "bars.1m" "bars.tf.3m" "bars.tf.5m" "bars.tf.15m" orders fills dlq; do /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server $$BROKER --create --if-not-exists --topic $$t --partitions 3 --replication-factor 1 || true; done; /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server $$BROKER --list'
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

.PHONY: bt-grid
bt-grid:
	. .venv/bin/activate && python backtest/grid_runner.py --symbols ${SYMS} --tfs ${TFS:-1m} --days ${DAYS:-30}

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

# (Stop: Ctrl+C or manage via your process supervisor/pm2/systemd)
