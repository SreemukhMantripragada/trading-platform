.PHONY: help up down reset ps logs doctor topics docker-config-list docker-config-sync metrics-list metrics-sync go-build perf-test legacy
.DEFAULT_GOAL := help

DOCKER_STACK_REGISTRY ?= configs/docker_stack.json
DOCKER_STACK_ENV ?= infra/.env.docker
METRICS_REGISTRY ?= configs/metrics_endpoints.json
METRICS_TARGETS ?= infra/prometheus/targets_apps.json
OBS_DASHBOARD ?= infra/grafana/dashboards/observability/platform_control_plane.json
LEGACY_MAKEFILE ?= Makefile.legacy
GO_BUILD_OS ?= linux
GO_BUILD_ARCH ?= $(shell uname -m | sed -e 's/x86_64/amd64/' -e 's/aarch64/arm64/')
DOCKER_COMPOSE_CMD = cd infra && docker compose --env-file $(notdir $(DOCKER_STACK_ENV))

help:
	@echo "Core targets:"
	@echo "  make up                # full Docker unit (infra + app supervisor)"
	@echo "  make down              # stop Docker unit"
	@echo "  make reset             # stop + remove volumes"
	@echo "  make ps                # container status"
	@echo "  make logs              # follow logs"
	@echo "  make doctor            # infra health checks"
	@echo "  make topics            # (re)create Kafka topics"
	@echo "  make docker-config-list"
	@echo "  make docker-config-sync"
	@echo "  make metrics-list"
	@echo "  make metrics-sync      # refresh Prom targets + observability dashboard"
	@echo "  make go-build          # build Go binaries via compose tools profile"
	@echo "  make perf-test         # infra-up benchmark + report (default: full paper-flow)"
	@echo ""
	@echo "Legacy targets are available via: make legacy TARGET=<name>"

up: docker-config-sync metrics-sync
	$(DOCKER_COMPOSE_CMD) up -d
	$(DOCKER_COMPOSE_CMD) ps

down: docker-config-sync
	$(DOCKER_COMPOSE_CMD) down

reset: docker-config-sync
	$(DOCKER_COMPOSE_CMD) down -v

ps: docker-config-sync
	$(DOCKER_COMPOSE_CMD) ps

logs: docker-config-sync
	$(DOCKER_COMPOSE_CMD) logs -f

doctor:
	python3 monitoring/doctor.py

topics: docker-config-sync
	@set -a; . $(DOCKER_STACK_ENV); set +a; \
	docker cp infra/scripts/create-topics.sh kafka:/usr/local/bin/create-topics; \
	docker exec -t kafka bash -lc "BROKER=kafka:$${KAFKA_INSIDE_PORT} RF=$${RF} DRY_RUN=$${TOPICS_DRY_RUN} /usr/local/bin/create-topics"

# Docker stack config registry helpers
docker-config-list:
	python3 tools/docker_stack_config.py --registry $(DOCKER_STACK_REGISTRY) --check

docker-config-sync:
	python3 tools/docker_stack_config.py --registry $(DOCKER_STACK_REGISTRY) --check --write-env $(DOCKER_STACK_ENV)

metrics-list:
	python3 tools/metrics_catalog.py --registry $(METRICS_REGISTRY) --check

metrics-sync: docker-config-sync
	@set -a; . $(DOCKER_STACK_ENV); set +a; \
	python3 tools/metrics_catalog.py --registry $(METRICS_REGISTRY) --host "$${APP_METRICS_HOST:-host.docker.internal}" --check --write-prom-targets $(METRICS_TARGETS)
	python3 tools/generate_grafana_observability.py --registry $(METRICS_REGISTRY) --out $(OBS_DASHBOARD)

# Build Go ingestion/aggregation binaries inside the compose stack tool profile.
go-build: docker-config-sync
	cd infra && GO_BUILD_OS=$(GO_BUILD_OS) GO_BUILD_ARCH=$(GO_BUILD_ARCH) docker compose --env-file $(notdir $(DOCKER_STACK_ENV)) --profile tools build go-builder
	cd infra && GO_BUILD_OS=$(GO_BUILD_OS) GO_BUILD_ARCH=$(GO_BUILD_ARCH) docker compose --env-file $(notdir $(DOCKER_STACK_ENV)) --profile tools run --rm go-builder

perf-test: docker-config-sync metrics-sync
	$(DOCKER_COMPOSE_CMD) up -d zookeeper kafka kafka-topics-init postgres prometheus grafana kafka-ui kafka-exporter
	python3 tools/pipeline_perf_compare.py \
		--scenario "$${PERF_SCENARIO:-paper-flow}" \
		--loads "$${LOADS:-50,200,500}" \
		--stage-seconds "$${STAGE_SEC:-180}" \
		--warmup-seconds "$${WARMUP_SEC:-20}" \
		--sample-seconds "$${SAMPLE_SEC:-5}" \
		--sim-base-tps "$${SIM_BASE_TPS:-5}" \
		--sim-hot-tps "$${SIM_HOT_TPS:-1000}" \
		--sim-hot-symbol-pct "$${SIM_HOT_SYMBOL_PCT:-0.002}" \
		--sim-hot-rotate-sec "$${SIM_HOT_ROTATE_SEC:-15}" \
		--sim-step-ms "$${SIM_STEP_MS:-100}" \
		--go-produce-workers "$${GO_PRODUCE_WORKERS:-16}" \
		--go-produce-queue "$${GO_PRODUCE_QUEUE:-20000}" \
		--go-max-batch "$${GO_MAX_BATCH:-1024}" \
		--go-batch-flush-ms "$${GO_BATCH_FLUSH_MS:-20}" \
		--go-bar1s-workers "$${GO_BAR1S_WORKERS:-8}" \
		--go-bar1s-queue "$${GO_BAR1S_QUEUE:-8000}" \
		--go-bar1s-flush-ms "$${GO_BAR1S_FLUSH_MS:-250}" \
		--go-bar1m-workers "$${GO_BAR1M_WORKERS:-8}" \
		--go-bar1m-queue "$${GO_BAR1M_QUEUE:-8000}" \
		--go-bar1m-flush-ms "$${GO_BAR1M_FLUSH_MS:-500}" \
		--go-baragg-workers "$${GO_BARAGG_WORKERS:-8}" \
		--go-baragg-queue "$${GO_BARAGG_QUEUE:-8000}" \
		--go-baragg-flush-ms "$${GO_BARAGG_FLUSH_MS:-500}" \
		--pair-timeframe "$${PAIR_TF:-1}" \
		--pair-lookback "$${PAIR_LOOKBACK:-4}" \
		--pair-count "$${PAIR_COUNT:-0}" \
		--pair-min-ready "$${PAIR_MIN_READY:-2}"

legacy:
	@test -n "$(TARGET)" || (echo "Usage: make legacy TARGET=<target-name>" && exit 1)
	@$(MAKE) -f $(LEGACY_MAKEFILE) $(TARGET)

%:
	@if [ -f "$(LEGACY_MAKEFILE)" ] && $(MAKE) -f $(LEGACY_MAKEFILE) -n $@ >/dev/null 2>&1; then \
		echo "[make] '$@' moved to $(LEGACY_MAKEFILE)."; \
		$(MAKE) -f $(LEGACY_MAKEFILE) $@; \
	else \
		echo "Unknown target '$@'. Run 'make help'."; \
		exit 2; \
	fi
