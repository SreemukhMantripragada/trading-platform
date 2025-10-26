#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF' >&2
Usage: push_pairs_config.sh <user@host> [remote-path]

Copies configs/pairs_next_day.yaml to the remote EC2 host.
- <user@host>  SSH target, e.g. ubuntu@ec2-1-2-3-4.compute.amazonaws.com
- [remote-path] overrides the default ~/trading-platform/configs/pairs_next_day.yaml

The script copies the file directly to the target path using scp.
EOF
}

if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
  usage
  exit 0
fi

if [ $# -lt 1 ] || [ $# -gt 2 ]; then
  usage
  exit 1
fi

if ! command -v ssh >/dev/null 2>&1; then
  echo "ssh is required on the local machine" >&2
  exit 2
fi

if ! command -v scp >/dev/null 2>&1; then
  echo "scp is required on the local machine" >&2
  exit 2
fi

REMOTE="$1"
REMOTE_TARGET="${2:-~/trading-platform/configs/pairs_next_day.yaml}"

REPO_ROOT="$(git rev-parse --show-toplevel)"
LOCAL_CONFIG="${REPO_ROOT}/configs/pairs_next_day.yaml"
KEY_FILE="${SSH_KEY_PATH:-${REPO_ROOT}/trading-ec2-key.pem}"

if [ ! -f "${LOCAL_CONFIG}" ]; then
  echo "Expected config not found at ${LOCAL_CONFIG}" >&2
  exit 3
fi

REMOTE_DIR="$(dirname "${REMOTE_TARGET}")"

SSH_OPTS=()
if [ -f "${KEY_FILE}" ]; then
  SSH_OPTS=(-i "${KEY_FILE}")
elif [ -n "${SSH_KEY_PATH:-}" ]; then
  echo "Warning: SSH key path ${SSH_KEY_PATH} not found; falling back to default ssh config" >&2
fi

scp "${SSH_OPTS[@]}" -C "${LOCAL_CONFIG}" "${REMOTE}:${REMOTE_TARGET}"

ssh "${SSH_OPTS[@]}" "${REMOTE}" "mkdir -p '${REMOTE_DIR}'"

echo "Config pushed to ${REMOTE}:${REMOTE_TARGET}"
