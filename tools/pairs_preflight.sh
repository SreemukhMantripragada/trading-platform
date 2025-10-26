#!/usr/bin/env bash

# Quick pre-flight checklist for the live pairs stack.
# Run on the EC2 host from the repo root before market open.

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'

ok()   { printf "${GREEN}✔ %s${NC}\n" "$1"; }
warn() { printf "${YELLOW}⚠ %s${NC}\n" "$1"; }
fail() { printf "${RED}✖ %s${NC}\n" "$1"; exit 1; }

REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || true)"
if [ -z "${REPO_ROOT}" ]; then
  fail "Run this script inside the trading-platform repository."
fi
cd "${REPO_ROOT}"

printf "${YELLOW}Pairs Preflight — $(date '+%Y-%m-%d %H:%M %Z')${NC}\n"

# 1. Config file exists and is recent
CFG="configs/pairs_next_day.yaml"
if [ ! -f "${CFG}" ]; then
  fail "${CFG} not found. Run make pairs-pipeline first."
fi

CFG_AGE_MIN=$(( ( $(date +%s) - $(stat -c %Y "${CFG}" 2>/dev/null || stat -f %m "${CFG}") ) / 60 ))
if [ "${CFG_AGE_MIN}" -gt 720 ]; then
  warn "${CFG} is older than 12 hours (${CFG_AGE_MIN} min); consider refreshing it."
else
  ok "${CFG} present (last updated ${CFG_AGE_MIN} minutes ago)."
fi

# 2. Python available
if command -v python3 >/dev/null 2>&1; then
  PY_VER="$(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:3])))')"
  ok "python3 available (version ${PY_VER})."
else
  fail "python3 not found in PATH."
fi

# 3. Virtualenv optional check
if [ -d ".venv" ]; then
  . .venv/bin/activate
  ok "Activated local virtualenv."
else
  warn "Virtualenv (.venv) not found; relying on system Python packages."
fi

# 4. Docker Compose services (Postgres & Kafka)
if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  pushd infra >/dev/null
  for svc in postgres kafka; do
    if docker compose ps "${svc}" 2>/dev/null | grep -q 'Up'; then
      ok "docker compose service '${svc}' is Up."
    else
      warn "docker compose service '${svc}' not running. Start with 'cd infra && docker compose up -d'."
    fi
  done
  popd >/dev/null
else
  warn "docker compose not available; skipping container health checks."
fi

# 5. Sanity query against Postgres
if command -v psql >/dev/null 2>&1; then
  if PGPASSWORD=${POSTGRES_PASSWORD:-trader} psql \
      -h ${POSTGRES_HOST:-localhost} \
      -p ${POSTGRES_PORT:-5432} \
      -U ${POSTGRES_USER:-trader} \
      -d ${POSTGRES_DB:-trading} \
      -c "SELECT NOW();" >/dev/null 2>&1; then
    ok "Postgres reachable (${POSTGRES_HOST:-localhost}:${POSTGRES_PORT:-5432})."
  else
    warn "Unable to connect to Postgres via psql. Ensure database is up and credentials are correct."
  fi
else
  warn "psql not found; skipping Postgres connectivity check."
fi

# 6. Kafka topic present (optional)
if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  if docker exec kafka kafka-topics.sh --bootstrap-server kafka:29092 --list 2>/dev/null | grep -q 'pairs.signals'; then
    ok "Kafka topic 'pairs.signals' detected."
  else
    warn "Kafka topic 'pairs.signals' not detected. Ensure Kafka is running and topics created."
  fi
fi

# 7. Show a preview of the config to sanity check
echo ""
echo "Top entries from ${CFG}:"
head -n 20 "${CFG}"

printf "\n${GREEN}Preflight checks complete.${NC}\n"
