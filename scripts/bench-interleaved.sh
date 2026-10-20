#!/usr/bin/env bash
# Runs benchmark scenarios for several arms (binaries and/or settings),
# interleaved, so that drift on a shared machine (thermal state, other load,
# the page cache) lands on every arm alike instead of on whichever arm was
# measured last.
#
#   scripts/bench-interleaved.sh OUTROOT 'SCENARIO_GLOB' REPEAT ARM [ARM...]
#   ARM = label:path/to/api-binary[:KEY=VALUE,KEY=VALUE]
#
# For each scenario file and each repetition every arm runs once, one run
# each, back to back, with the order rotated between repetitions (arm 1 first,
# then arm 2 first, ...). The KEY=VALUE pairs are added to that arm's API
# environment (bench run -env) and recorded in its result files. Results go to
# OUTROOT/<label>/. Needs bin/bench (make build) and a running dev database.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
OUT="${1:?output root}"; GLOB="${2:?scenario glob}"; REPEAT="${3:?repeat}"; shift 3
[ "$#" -ge 1 ] || { echo "need at least one ARM" >&2; exit 2; }
URL="$(scripts/devdb.sh url)"

labels=(); bins=(); envs=()
for a in "$@"; do
  IFS=: read -r label bin env <<<"$a"
  labels+=("$label"); bins+=("$bin"); envs+=("${env:-}")
  mkdir -p "$OUT/$label"
done
n=${#labels[@]}

# shellcheck disable=SC2086 # the glob is expanded here on purpose
for sc in $GLOB; do
  for ((rep = 0; rep < REPEAT; rep++)); do
    for ((k = 0; k < n; k++)); do
      i=$(((k + rep) % n))
      args=()
      if [ -n "${envs[$i]}" ]; then
        IFS=, read -ra kvs <<<"${envs[$i]}"
        for kv in "${kvs[@]}"; do args+=(-env "$kv"); done
      fi
      echo "=== $(date -u +%H:%M:%S) $sc rep $((rep + 1))/$REPEAT arm ${labels[$i]}"
      ./bin/bench run -scenario "$sc" -repeat 1 -api-bin "${bins[$i]}" -admin-db "$URL" -out "$OUT/${labels[$i]}" "${args[@]+"${args[@]}"}"
    done
  done
done
echo "INTERLEAVED RUN DONE"
