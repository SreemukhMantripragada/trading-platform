#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "${REPO_ROOT}"

if [ -d .venv ]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
fi

POSTGRES_HOST=${POSTGRES_HOST:-localhost}
POSTGRES_PORT=${POSTGRES_PORT:-5432}
POSTGRES_DB=${POSTGRES_DB:-trading}
POSTGRES_USER=${POSTGRES_USER:-trader}
POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-trader}

PAIRS_REMOTE_HOST=${PAIRS_REMOTE_HOST:-${REMOTE:-}}
PAIRS_REMOTE_PATH=${PAIRS_REMOTE_PATH:-${REMOTE_PATH:-~/trading-platform/configs/pairs_next_day.yaml}}
SSH_KEY_PATH=${SSH_KEY_PATH:-${PAIRS_REMOTE_KEY:-${REPO_ROOT}/trading-ec2-key.pem}}
if [ -z "${PAIRS_REMOTE_HOST}" ]; then
  echo "PAIRS_REMOTE_HOST (or REMOTE) must be set for pipeline sync" >&2
  exit 4
fi

SQL_FILE="${REPO_ROOT}/infra/postgres/init/18_buildbars_multi.sql"
if [ ! -f "${SQL_FILE}" ]; then
  echo "SQL file not found: ${SQL_FILE}" >&2
  exit 1
fi

export PGPASSWORD="${POSTGRES_PASSWORD}"
if command -v psql >/dev/null 2>&1; then
  PSQL_CMD=(psql --host="${POSTGRES_HOST}" --port="${POSTGRES_PORT}" --username="${POSTGRES_USER}" --dbname="${POSTGRES_DB}")
  USE_DOCKER_PSQL=0
else
  echo "[pairs-pipeline] local psql not found, attempting docker exec postgres"
  PSQL_CMD=(docker exec -i postgres psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}")
  USE_DOCKER_PSQL=1
fi

echo "[pairs-pipeline] backfilling vendor data (60d)"
python3 ingestion/backfill_60d_vendor.py

echo "[pairs-pipeline] refreshing higher timeframe materialized views"
if [ "${USE_DOCKER_PSQL}" -eq 1 ]; then
  cat "${SQL_FILE}" | "${PSQL_CMD[@]}"
else
  "${PSQL_CMD[@]}" --file "${SQL_FILE}"
fi

echo "[pairs-pipeline] scanning for new pairs"
python3 analytics/pairs/find_pairs.py

echo "[pairs-pipeline] running pairs backtest grid"
python3 -m backtest.pairs_backtest_db

NEXT_DAY_FILE="${REPO_ROOT}/configs/pairs_next_day.yaml"
if [ ! -f "${NEXT_DAY_FILE}" ]; then
  echo "Expected ${NEXT_DAY_FILE} but it was not generated" >&2
  exit 2
fi

echo "[pairs-pipeline] pushing config to ${PAIRS_REMOTE_HOST}"
if [ ! -f "${SSH_KEY_PATH}" ]; then
  echo "SSH key not found: ${SSH_KEY_PATH}" >&2
  exit 4
fi
SSH_KEY_PATH="${SSH_KEY_PATH}" ./tools/push_pairs_config.sh "${PAIRS_REMOTE_HOST}" "${PAIRS_REMOTE_PATH}"

echo "[pairs-pipeline] done"
