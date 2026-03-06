#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yml}"
SQLITE_PATH="${SQLITE_PATH:-/app/instance/monarch.db}"
BATCH_SIZE="${BATCH_SIZE:-1000}"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required" >&2
  exit 1
fi

echo "Building/starting services so importer script is available in the app container..."
docker compose -f "${COMPOSE_FILE}" up -d --build

echo "Running SQLite -> PostgreSQL import from app container..."
docker compose -f "${COMPOSE_FILE}" exec -T app \
  python /app/scripts/import_sqlite_to_postgres.py \
  --sqlite-path "${SQLITE_PATH}" \
  --batch-size "${BATCH_SIZE}" "$@"
