#!/usr/bin/env bash
# infra/scripts/create-topics-extended.sh
set -euo pipefail
BROKER="${BROKER:-kafka:29092}"

create() {
  local t="$1" p="${2:-3}" rf="${3:-1}" cfg="${4:-}"
  if /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server "$BROKER" --list | grep -q "^$t$"; then
    echo "topic $t exists"
  else
    /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server "$BROKER" --create --if-not-exists \
      --topic "$t" --partitions "$p" --replication-factor "$rf"
    echo "created $t"
  fi
  if [[ -n "$cfg" ]]; then
    /opt/bitnami/kafka/bin/kafka-configs.sh --bootstrap-server "$BROKER" --alter --entity-type topics \
      --entity-name "$t" --add-config "$cfg" || true
  fi
}

# Ticks/bars keep short retention local; change for prod.
create ticks         6 1 "retention.ms=86400000"
create bars.1s       6 1 "retention.ms=172800000"
create bars.1m       6 1 "retention.ms=604800000"
create orders        6 1 "retention.ms=604800000"
create orders.allowed 6 1 "retention.ms=604800000"
create fills         6 1 "retention.ms=604800000"
create dlq           3 1 "cleanup.policy=compact,delete"
create pairs.signals 3 1 "retention.ms=604800000"
create control.kill  1 1 "cleanup.policy=compact"
echo "done."
