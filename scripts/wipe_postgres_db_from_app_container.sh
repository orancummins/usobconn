#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yml}"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required" >&2
  exit 1
fi

echo "Building/starting services so wipe script is available in the app container..."
docker compose -f "${COMPOSE_FILE}" up -d --build

echo "Wiping PostgreSQL database from app container..."
docker compose -f "${COMPOSE_FILE}" exec -T app \
  python /app/scripts/wipe_postgres_db.py --yes "$@"
