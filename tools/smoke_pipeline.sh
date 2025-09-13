#!/usr/bin/env bash
set -euo pipefail
# Preconditions: docker infra up, topics created, Postgres schema applied, venv active
# 1) backfill sample 1m
POSTGRES_HOST=${POSTGRES_HOST:-localhost} POSTGRES_PORT=${POSTGRES_PORT:-5432} POSTGRES_DB=${POSTGRES_DB:-trading} POSTGRES_USER=${POSTGRES_USER:-trader} POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-trader} \
python compute/backfill_bars_1m.py RELIANCE data/historical/RELIANCE_1m_sample.csv

# 2) start matcher (background)
( make matcher-build && nohup make matcher-run > /tmp/matcher.log 2>&1 & echo $! > /tmp/matcher.pid )

# 3) start agg-multi, strategy 1m, risk, exec (each in background)
nohup make agg-multi           > /tmp/agg.log 2>&1 &
nohup make strat-1m            > /tmp/strat1m.log 2>&1 &
nohup make risk-v2             > /tmp/riskv2.log 2>&1 &
nohup make exec-paper-matcher  > /tmp/exec.log 2>&1 &

echo "[smoke] services launched. Now inject replay..."
# 4) replay 5 minutes to generate orders/fills
python replay/injector.py RELIANCE 2025-09-01T03:45:00Z 2025-09-01T03:50:00Z 8.0
echo "[smoke] done. Check Postgres tables 'orders', 'fills', 'positions' and Grafana."
