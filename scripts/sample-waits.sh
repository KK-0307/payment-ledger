#!/usr/bin/env bash
# Samples what PostgreSQL backends are waiting on while a benchmark runs.
#
#   scripts/sample-waits.sh bench/scenarios/01-baseline.json [out-dir]
#
# Every 50 ms it records (wait_event_type, wait_event) for active backends of
# the benchmark database, then prints the distribution. "Lock/transactionid"
# or "Lock/tuple" samples mean a backend is blocked on another transaction's
# row lock (e.g. the hot processor_clearing account row).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCENARIO="${1:?scenario file}"
OUT="${2:-$ROOT/data/wait-samples}"
URL="$("$ROOT/scripts/devdb.sh" url)"
PSQL="$(dirname "$(command -v pg_ctl 2>/dev/null || echo /opt/homebrew/opt/postgresql@16/bin/pg_ctl)")/psql"
mkdir -p "$OUT"
SAMPLES="$OUT/samples.txt"
: > "$SAMPLES"

"$ROOT/bin/bench" run -scenario "$SCENARIO" -admin-db "$URL" -out "$OUT/results" -logs "$OUT/logs" &
BENCH=$!
while kill -0 "$BENCH" 2>/dev/null; do
  "$PSQL" "$URL" -XAtc "SELECT coalesce(wait_event_type,'CPU/none') || '/' || coalesce(wait_event,'-')
                         FROM pg_stat_activity
                         WHERE datname LIKE 'bench_%' AND state = 'active' AND pid <> pg_backend_pid()" >> "$SAMPLES" 2>/dev/null || true
  sleep 0.05
done
wait "$BENCH"
echo "backend samples by wait event:"
sort "$SAMPLES" | uniq -c | sort -rn | head -15
