.PHONY: help up down reset ps logs doctor topics docker-config-list docker-config-sync metrics-list metrics-sync go-build legacy
.DEFAULT_GOAL := help

DOCKER_STACK_REGISTRY ?= configs/docker_stack.json
DOCKER_STACK_ENV ?= infra/.env.docker
METRICS_REGISTRY ?= configs/metrics_endpoints.json
METRICS_TARGETS ?= infra/prometheus/targets_apps.json
LEGACY_MAKEFILE ?= Makefile.legacy
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
	@echo "  make metrics-sync"
	@echo "  make go-build          # build Go binaries in Docker"
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

# Build Go ingestion/aggregation binaries in a container (no host Go needed)
go-build:
	docker build -f infra/docker/go-builder.Dockerfile -t trading-go-builder . && \
	docker create --name trading-go-builder-temp trading-go-builder && \
	docker cp trading-go-builder-temp:/out/ws_bridge go/bin/ws_bridge && \
	docker cp trading-go-builder-temp:/out/bar_builder_1s go/bin/bar_builder_1s && \
	docker cp trading-go-builder-temp:/out/bar_aggregator_multi go/bin/bar_aggregator_multi && \
	docker rm trading-go-builder-temp

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
