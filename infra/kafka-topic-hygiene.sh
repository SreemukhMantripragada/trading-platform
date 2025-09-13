#!/usr/bin/env bash
set -euo pipefail
BROKER="${BROKER:-kafka:29092}"

create_topic() {
  local t="$1"; local p="${2:-3}"; local rf="${3:-1}"
  if ! kafka-topics.sh --bootstrap-server "$BROKER" --list | grep -q "^${t}$"; then
    kafka-topics.sh --bootstrap-server "$BROKER" --create --topic "$t" --partitions "$p" --replication-factor "$rf"
    echo "created $t"
  fi
}

alter_cfg() {
  local t="$1"; shift
  kafka-configs.sh --bootstrap-server "$BROKER" --alter --entity-type topics --entity-name "$t" --add-config "$*"
}

# Core topics (create if missing)
for t in ticks bars.1s bars.1m bars.3m bars.5m bars.15m bars.60m orders fills intents dlq; do
  create_topic "$t" 3 1
done

# Aggregated bars: compact+delete, retain 30d
for t in bars.3m bars.5m bars.15m bars.60m; do
  alter_cfg "$t" "cleanup.policy=compact,delete,retention.ms=2592000000,min.cleanable.dirty.ratio=0.5,segment.bytes=268435456"
done

# 1m bars moderate retention (7d), delete only
alter_cfg "bars.1m" "cleanup.policy=delete,retention.ms=604800000,segment.bytes=268435456"

# ticks shorter retention (2d)
alter_cfg "ticks" "cleanup.policy=delete,retention.ms=172800000,segment.bytes=268435456"

# orders/fills: 7d
alter_cfg "orders" "cleanup.policy=delete,retention.ms=604800000"
alter_cfg "fills"  "cleanup.policy=delete,retention.ms=604800000"

# DLQ: 30d
alter_cfg "dlq" "cleanup.policy=delete,retention.ms=2592000000"

echo "topic hygiene OK"
