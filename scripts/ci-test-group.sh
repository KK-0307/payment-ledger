#!/usr/bin/env bash
# Runs a named group of integration tests and fails unless every named test
# actually ran and passed. A plain `go test -run` exits 0 when the regex
# matches nothing (a renamed test) or when a test was skipped because
# TEST_DATABASE_URL was missing; both would make a CI group vacuously green.
#
#   scripts/ci-test-group.sh "ledger invariants" "./internal/ledger ./internal/audit" TestA TestB ...
#
# Needs TEST_DATABASE_URL (a PostgreSQL URL with CREATE DATABASE rights).
set -euo pipefail
group="$1"; pkgs="$2"; shift 2
: "${TEST_DATABASE_URL:?TEST_DATABASE_URL is required}"
[ "$#" -gt 0 ] || { echo "no tests named for group $group" >&2; exit 2; }

regex="^($(IFS='|'; echo "$*"))\$"
out="$(mktemp)"
trap 'rm -f "$out"' EXIT

echo "::group::$group"
# shellcheck disable=SC2086 # $pkgs is a deliberate word list
go test -count=1 -p "${CI_TEST_PACKAGES_PARALLEL:-2}" -v -run "$regex" $pkgs 2>&1 | tee "$out"
status=${PIPESTATUS[0]}
echo "::endgroup::"
[ "$status" -eq 0 ] || { echo "group '$group': go test failed" >&2; exit "$status"; }

missing=0
for t in "$@"; do
  if ! grep -q -- "^--- PASS: $t " "$out"; then
    echo "group '$group': $t did not run and pass" >&2
    missing=1
  fi
done
if grep -q -- '--- SKIP' "$out"; then
  echo "group '$group': a test was skipped" >&2
  missing=1
fi
[ "$missing" -eq 0 ] && echo "group '$group': all $# named tests passed"
exit "$missing"
