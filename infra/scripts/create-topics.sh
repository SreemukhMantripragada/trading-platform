#!/usr/bin/env bash
set -euo pipefail
BIN="/opt/bitnami/kafka/bin"
export PATH="$BIN:$PATH"
# --- Broker & defaults ---
BROKER="${BROKER:-localhost:29092}"
RF="${RF:-1}"                 # replication factor
P_TICKS="${P_TICKS:-6}"       # partitions for high-throughput topics
P_BARS="${P_BARS:-6}"
P_ORDERS="${P_ORDERS:-6}"
P_MISC="${P_MISC:-3}"
P_DLQ="${P_DLQ:-1}"

# Retention defaults (ms)
TICKS_RETENTION_MS="${TICKS_RETENTION_MS:-86400000}"      # 1d
BARS1S_RETENTION_MS="${BARS1S_RETENTION_MS:-604800000}"   # 7d
BARS1M_RETENTION_MS="${BARS1M_RETENTION_MS:-2592000000}"  # 30d
BARS3M_RETENTION_MS="${BARS3M_RETENTION_MS:-5184000000}"  # 60d
BARS5M_RETENTION_MS="${BARS5M_RETENTION_MS:-7776000000}"  # 90d
BARS15M_RETENTION_MS="${BARS15M_RETENTION_MS:-15552000000}" # 180d

ORDERS_RETENTION_MS="${ORDERS_RETENTION_MS:-1209600000}"  # 14d
ALLOWED_RETENTION_MS="${ALLOWED_RETENTION_MS:-1209600000}" # 14d
EXEC_RETENTION_MS="${EXEC_RETENTION_MS:-2592000000}"      # 30d
FILLS_RETENTION_MS="${FILLS_RETENTION_MS:-2592000000}"    # 30d
CANCELS_RETENTION_MS="${CANCELS_RETENTION_MS:-1209600000}" # 14d
DLQ_RETENTION_MS="${DLQ_RETENTION_MS:-2592000000}"        # 30d
PAIRS_RETENTION_MS="${PAIRS_RETENTION_MS:-1209600000}"    # 14d

SEGMENT_BYTES="${SEGMENT_BYTES:-268435456}"               # 256MB
CLEANABLE_RATIO="${CLEANABLE_RATIO:-0.1}"

DRY_RUN="${DRY_RUN:-0}"

# --- helpers ---
_exists() {
  command -v "$1" >/dev/null 2>&1
}

need() {
  if ! _exists "$1"; then
    echo "ERROR: required command '$1' not found in PATH" >&2
    exit 1
  fi
}

run() {
  if [ "$DRY_RUN" = "1" ]; then
    echo "DRY: $*"
  else
    eval "$@"
  fi
}

kcreate() {
  local topic="$1" part="$2" rf="$3"
  run "kafka-topics.sh --bootstrap-server '$BROKER' --create --if-not-exists --topic '$topic' --partitions $part --replication-factor $rf"
}

kconfig() {
  local topic="$1" cfg="$2"
  run "kafka-configs.sh --bootstrap-server '$BROKER' --alter --entity-type topics --entity-name '$topic' --add-config $cfg"
}

echo "== Kafka topic setup =="
echo "BROKER=$BROKER DRY_RUN=$DRY_RUN RF=$RF"

need kafka-topics.sh
need kafka-configs.sh

# --- Topic definitions ---
# ticks
kcreate "ticks" "$P_TICKS" "$RF"
kconfig "ticks" "retention.ms=$TICKS_RETENTION_MS,segment.bytes=$SEGMENT_BYTES,cleanup.policy=delete"

# bars.* (streaming, derived from ticks)
kcreate "bars.1s"  "$P_BARS" "$RF"
kconfig "bars.1s"  "retention.ms=$BARS1S_RETENTION_MS,segment.bytes=$SEGMENT_BYTES,cleanup.policy=delete"

kcreate "bars.1m"  "$P_BARS" "$RF"
kconfig "bars.1m"  "retention.ms=$BARS1M_RETENTION_MS,segment.bytes=$SEGMENT_BYTES,cleanup.policy=delete"

kcreate "bars.3m"  "$P_BARS" "$RF"
kconfig "bars.3m"  "retention.ms=$BARS3M_RETENTION_MS,segment.bytes=$SEGMENT_BYTES,cleanup.policy=delete"

kcreate "bars.5m"  "$P_BARS" "$RF"
kconfig "bars.5m"  "retention.ms=$BARS5M_RETENTION_MS,segment.bytes=$SEGMENT_BYTES,cleanup.policy=delete"

kcreate "bars.15m" "$P_BARS" "$RF"
kconfig "bars.15m" "retention.ms=$BARS15M_RETENTION_MS,segment.bytes=$SEGMENT_BYTES,cleanup.policy=delete"

# (Optional placeholder if you ever stream golden bars; DB is source of truth)
kcreate "bars.1m.golden" "1" "$RF"
kconfig "bars.1m.golden" "retention.ms=$BARS1M_RETENTION_MS,segment.bytes=$SEGMENT_BYTES,cleanup.policy=delete"

# orders pipeline
kcreate "orders" "$P_ORDERS" "$RF"
kconfig "orders" "cleanup.policy=[compact,delete],min.cleanable.dirty.ratio=$CLEANABLE_RATIO,retention.ms=$ORDERS_RETENTION_MS,segment.bytes=$SEGMENT_BYTES"

kcreate "orders.sized" "$P_ORDERS" "$RF"
kconfig "orders.sized" "cleanup.policy=[compact,delete],min.cleanable.dirty.ratio=$CLEANABLE_RATIO,retention.ms=$ALLOWED_RETENTION_MS,segment.bytes=$SEGMENT_BYTES"

kcreate "orders.allowed" "$P_ORDERS" "$RF"
kconfig "orders.allowed" "cleanup.policy=[compact,delete],min.cleanable.dirty.ratio=$CLEANABLE_RATIO,retention.ms=$ALLOWED_RETENTION_MS,segment.bytes=$SEGMENT_BYTES"

kcreate "orders.exec" "$P_ORDERS" "$RF"
kconfig "orders.exec" "cleanup.policy=delete,retention.ms=$EXEC_RETENTION_MS,segment.bytes=$SEGMENT_BYTES"

kcreate "fills" "$P_ORDERS" "$RF"
kconfig "fills" "cleanup.policy=delete,retention.ms=$FILLS_RETENTION_MS,segment.bytes=$SEGMENT_BYTES"

kcreate "cancels" "$P_MISC" "$RF"
kconfig "cancels" "cleanup.policy=delete,retention.ms=$CANCELS_RETENTION_MS,segment.bytes=$SEGMENT_BYTES"

# analytics/aux
kcreate "pairs.signals" "$P_MISC" "$RF"
kconfig "pairs.signals" "cleanup.policy=delete,retention.ms=$PAIRS_RETENTION_MS,segment.bytes=$SEGMENT_BYTES"

# dead letter
kcreate "dlq" "$P_DLQ" "$RF"
kconfig "dlq" "cleanup.policy=delete,retention.ms=$DLQ_RETENTION_MS,segment.bytes=$SEGMENT_BYTES"

echo "== Topic list =="
run "kafka-topics.sh --bootstrap-server '$BROKER' --list"
echo "Done."