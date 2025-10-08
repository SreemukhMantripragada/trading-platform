#!/usr/bin/env bash
# tools/archive_prune.sh
# Move non-essential files/dirs into archive/ to keep a minimal working tree.
# DRY-RUN by default. Use --apply to execute moves.

set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# Whitelist: keep only what's needed for live pairs + bars + OMS + recon/backtest
KEEP=(
  ".git"
  ".github"
  ".venv"
  ".gitignore"
  
  "README.md"
  "Makefile"
  "requirements.txt"
  "infra/docker-compose.yml"
  "infra/grafana"
  "infra/postgres/init"
  "infra/prometheus"
  "configs"
  "schemas"
  "ingestion/zerodha_ws.py"
  "ingestion/backfill_60d_vendor.py"
  "compute/bar_builder_1s.py"
  "compute/bar_aggregator_1m.py"
  "compute/bar_aggregator_1m_to_multi.py"
  "strategy/pairs_meanrev.py"
  "analytics/pairs/find_pairs.py"
  "backtest/pairs_backtest.py"
  "execution/oms.py"
  "execution/ems_paper.py"
  "execution/matcher_client.py"
  "execution/cpp/matcher"
  "monitoring/daily_recon.py"
  "libs/killswitch.py"
  "libs/schema.py"
  "brokers/zerodha_account.py"
  "tools/archive_prune.sh"
)

APPLY=0
if [[ "${1:-}" == "--apply" ]]; then APPLY=1; fi

# helper to test keep prefix (no associative arrays in Bash 3)
is_kept() {
  local p="$1" k
  for k in "${KEEP[@]}"; do
    if [[ "$p" == "$k" || "$p" == "$k/"* ]]; then
      return 0
    fi
  done
  return 1
}

ARCHIVE="archive/$(date +%Y%m%d_%H%M%S)"
mkdir -p "$ARCHIVE"

# Walk tree (portable to BSD find: no -mindepth/-maxdepth)
# We filter depth (<= 3) in Bash by counting slashes in the relative path.
while IFS= read -r -d '' path; do
  rel="${path#./}"
  [[ "$rel" == "." || -z "$rel" ]] && continue
  [[ "$rel" == archive* ]] && continue

  # depth = number of '/' in rel
  tmp="${rel//[^\/]/}"       # keep only slashes
  depth=${#tmp}              # count them
  if (( depth > 3 )); then
    continue
  fi

  if is_kept "$rel"; then
    echo "KEEP   $rel"
  else
    dest="$ARCHIVE/$rel"
    echo "MOVE   $rel -> $dest"
    if [[ $APPLY -eq 1 ]]; then
      mkdir -p "$(dirname "$dest")"
      # try git mv, fall back to mv if not tracked
      if ! git mv -k "$rel" "$dest" 2>/dev/null; then
        mkdir -p "$(dirname "$dest")"
        mv "$rel" "$dest"
      fi
    fi
  fi
done < <(find . -print0)

if [[ $APPLY -eq 0 ]]; then
  echo
  echo "[dry-run] Nothing moved. Re-run with: bash tools/archive_prune.sh --apply"
else
  echo "[apply] Completed. Review 'git status' and commit."
fi
