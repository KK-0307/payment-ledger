#!/usr/bin/env bash
# Project-local PostgreSQL cluster for development, tests and benchmarks.
# Data lives in .devdb/ (git-ignored); nothing is installed as a login service.
#
#   scripts/devdb.sh start | stop | status | psql | url | destroy
#
# DEVDB_HOME (default: .devdb in the repository) and DEVDB_PORT (default
# 54329) move the cluster. Benchmarks should run it OUTSIDE any folder that a
# cloud-sync client watches (macOS "Desktop & Documents in iCloud", Dropbox,
# OneDrive): the sync daemon rewrites and re-reads PostgreSQL's data files
# while it runs and makes timings swing by tens of percent.
set -euo pipefail
export LC_ALL="${LC_ALL:-en_US.UTF-8}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HOME_DIR="${DEVDB_HOME:-$ROOT/.devdb}"
DATA="$HOME_DIR/data"
LOG="$HOME_DIR/postgres.log"
PORT="${DEVDB_PORT:-54329}"
USER_NAME="ledger"
DB_NAME="ledger"

if ! command -v pg_ctl >/dev/null 2>&1; then
  for d in /opt/homebrew/opt/postgresql@16/bin /usr/local/opt/postgresql@16/bin /usr/lib/postgresql/16/bin; do
    [ -x "$d/pg_ctl" ] && PATH="$d:$PATH" && break
  done
fi
command -v pg_ctl >/dev/null || { echo "pg_ctl not found; install PostgreSQL 16" >&2; exit 1; }

url() { echo "postgres://$USER_NAME@127.0.0.1:$PORT/$DB_NAME?sslmode=disable"; }

case "${1:-}" in
  start)
    if [ ! -f "$DATA/PG_VERSION" ]; then
      mkdir -p "$DATA"
      initdb -D "$DATA" -U "$USER_NAME" --auth=trust -E UTF8 --locale=C >/dev/null
      cat >>"$DATA/postgresql.conf" <<CONF
listen_addresses = '127.0.0.1'
port = $PORT
max_connections = 400
shared_buffers = 512MB
unix_socket_directories = ''
CONF
    fi
    if pg_ctl -D "$DATA" status >/dev/null 2>&1; then echo "already running: $(url)"; exit 0; fi
    pg_ctl -D "$DATA" -l "$LOG" -w start >/dev/null
    psql -h 127.0.0.1 -p "$PORT" -U "$USER_NAME" -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='$DB_NAME'" | grep -q 1 \
      || createdb -h 127.0.0.1 -p "$PORT" -U "$USER_NAME" "$DB_NAME"
    echo "running: $(url)"
    ;;
  stop)    pg_ctl -D "$DATA" -m fast stop ;;
  status)  pg_ctl -D "$DATA" status ;;
  psql)    shift; exec psql "$(url)" "$@" ;;
  url)     url ;;
  destroy) pg_ctl -D "$DATA" -m fast stop 2>/dev/null || true; rm -rf "$HOME_DIR" ;;
  *) echo "usage: $0 start|stop|status|psql|url|destroy" >&2; exit 2 ;;
esac
