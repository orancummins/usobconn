#!/usr/bin/env bash
set -euo pipefail

CLEAN_DB=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    --clean|--wipe-db)
      CLEAN_DB=true
      shift
      ;;
    -h|--help)
      cat <<'EOF'
Usage: ./run.sh [--clean]

Options:
  --clean      Recreate PostgreSQL data volume (destructive, starts with empty DB)
  --wipe-db    Alias for --clean
  -h, --help   Show this help
EOF
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      echo "Use --help for usage." >&2
      exit 1
      ;;
  esac
done

IMAGE_NAME="${IMAGE_NAME:-usobconn}"
IMAGE_TAG="${IMAGE_TAG:-latest}"
CONTAINER_NAME="${CONTAINER_NAME:-usobconn}"
HOST_PORT="${HOST_PORT:-9093}"
CONTAINER_PORT="${CONTAINER_PORT:-9093}"
ENABLE_SCHEDULER="${ENABLE_SCHEDULER:-true}"
FLASK_SECRET_KEY="${FLASK_SECRET_KEY:-change-me}"
APP_PASSWORD="${APP_PASSWORD:-}"
NETWORK_NAME="${NETWORK_NAME:-usobconn-net}"
DB_CONTAINER_NAME="${DB_CONTAINER_NAME:-usobconn-postgres}"
DB_NAME="${DB_NAME:-usobconn}"
DB_USER="${DB_USER:-usobconn}"
DB_PASSWORD="${DB_PASSWORD:-usobconn}"
DB_PORT="${DB_PORT:-5432}"
DB_VOLUME="${DB_VOLUME:-usobconn-postgres-data}"
DATABASE_URL="${DATABASE_URL:-postgresql+psycopg://${DB_USER}:${DB_PASSWORD}@${DB_CONTAINER_NAME}:${DB_PORT}/${DB_NAME}}"

mkdir -p instance

if ! docker network inspect "${NETWORK_NAME}" >/dev/null 2>&1; then
  docker network create "${NETWORK_NAME}" >/dev/null
fi

if [ "${CLEAN_DB}" = "true" ]; then
  echo "Cleaning PostgreSQL data volume '${DB_VOLUME}'..."
  if docker ps -a --format '{{.Names}}' | grep -qx "${DB_CONTAINER_NAME}"; then
    docker rm -f "${DB_CONTAINER_NAME}" >/dev/null
  fi
  if docker volume inspect "${DB_VOLUME}" >/dev/null 2>&1; then
    docker volume rm "${DB_VOLUME}" >/dev/null
  fi
fi

if ! docker volume inspect "${DB_VOLUME}" >/dev/null 2>&1; then
  docker volume create "${DB_VOLUME}" >/dev/null
fi

if docker ps -a --format '{{.Names}}' | grep -qx "${DB_CONTAINER_NAME}"; then
  if ! docker ps --format '{{.Names}}' | grep -qx "${DB_CONTAINER_NAME}"; then
    docker start "${DB_CONTAINER_NAME}" >/dev/null
  fi
else
  docker run -d \
    --name "${DB_CONTAINER_NAME}" \
    --network "${NETWORK_NAME}" \
    -e POSTGRES_DB="${DB_NAME}" \
    -e POSTGRES_USER="${DB_USER}" \
    -e POSTGRES_PASSWORD="${DB_PASSWORD}" \
    -v "${DB_VOLUME}:/var/lib/postgresql/data" \
    postgres:16 >/dev/null
fi

echo "Waiting for PostgreSQL to become ready..."
for i in $(seq 1 60); do
  if docker run --rm --network "${NETWORK_NAME}" \
    -e PGPASSWORD="${DB_PASSWORD}" \
    postgres:16 \
    pg_isready -h "${DB_CONTAINER_NAME}" -p "${DB_PORT}" -U "${DB_USER}" >/dev/null 2>&1; then
    break
  fi
  if [ "${i}" -eq 60 ]; then
    echo "PostgreSQL did not become ready in time." >&2
    exit 1
  fi
  sleep 1
done

if docker ps -a --format '{{.Names}}' | grep -qx "${CONTAINER_NAME}"; then
  docker rm -f "${CONTAINER_NAME}" >/dev/null
fi

docker run -d \
  --name "${CONTAINER_NAME}" \
  --network "${NETWORK_NAME}" \
  -p "${HOST_PORT}:${CONTAINER_PORT}" \
  -e ENABLE_SCHEDULER="${ENABLE_SCHEDULER}" \
  -e FLASK_SECRET_KEY="${FLASK_SECRET_KEY}" \
  -e APP_PASSWORD="${APP_PASSWORD}" \
  -e DATABASE_URL="${DATABASE_URL}" \
  -v "$(pwd)/instance:/app/instance" \
  "${IMAGE_NAME}:${IMAGE_TAG}" >/dev/null

echo "App started in background: ${CONTAINER_NAME}"
echo "URL: http://localhost:${HOST_PORT}"
echo "Logs: docker logs -f ${CONTAINER_NAME}"
echo "Stop: docker stop ${CONTAINER_NAME}"
