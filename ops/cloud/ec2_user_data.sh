#!/bin/bash
# ops/cloud/ec2_user_data.sh
set -euxo pipefail

# 1) basics
apt-get update
apt-get install -y docker.io docker-compose-plugin git make python3-venv

# 2) checkout your repo (read-only PAT recommended)
REPO_URL="${REPO_URL:-https://github.com/SreemukhMantripragada/trading-platform.git}"
APP_HOME="/opt/trading"
mkdir -p "$APP_HOME"
cd "$APP_HOME"
if [ ! -d ".git" ]; then
  git clone "$REPO_URL" .
fi

# 3) docker infra (single source: configs/docker_stack.json -> infra/.env.docker)
make up

# 4) python venv for app processes
python3 -m venv .venv
. .venv/bin/activate
pip install -U pip
pip install -r requirements.txt

# 5) (re)create topics (idempotent)
make topics

# 6) apply DDL
docker exec -i postgres psql -U trader -d trading -f infra/postgres/init/08_budget.sql
docker exec -i postgres psql -U trader -d trading -f infra/postgres/init/09_accounting.sql

# 7) launch supervisor (tmux-less) via systemd
cat >/etc/systemd/system/trader-supervisor.service <<'UNIT'
[Unit]
Description=Trading Process Supervisor
After=docker.service network-online.target
Wants=network-online.target

[Service]
WorkingDirectory=/opt/trading
Environment="KAFKA_BOOT=localhost:9092"
ExecStart=/bin/bash -lc '. .venv/bin/activate && python monitoring/process_supervisor.py'
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload
systemctl enable --now trader-supervisor
