#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BRANCH="${BRANCH:-main}"
REMOTE="${REMOTE:-origin}"
LOCK_DIR="${LOCK_DIR:-/tmp/usobconn-auto-redeploy.lock}"

log() {
  printf '[%s] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*"
}

if ! mkdir "${LOCK_DIR}" 2>/dev/null; then
  log "Another run is in progress. Exiting."
  exit 0
fi
trap 'rmdir "${LOCK_DIR}"' EXIT

cd "${REPO_DIR}"

if [ ! -d .git ]; then
  log "Not a git repository: ${REPO_DIR}"
  exit 1
fi

if ! git diff --quiet || ! git diff --cached --quiet; then
  log "Working tree has local changes. Skipping redeploy."
  exit 0
fi

log "Fetching ${REMOTE}/${BRANCH}..."
git fetch --quiet "${REMOTE}" "${BRANCH}"

LOCAL_SHA="$(git rev-parse HEAD)"
REMOTE_SHA="$(git rev-parse "${REMOTE}/${BRANCH}")"
BASE_SHA="$(git merge-base HEAD "${REMOTE}/${BRANCH}")"

if [ "${LOCAL_SHA}" = "${REMOTE_SHA}" ]; then
  log "No new commits on ${REMOTE}/${BRANCH}."
  exit 0
fi

if [ "${LOCAL_SHA}" != "${BASE_SHA}" ]; then
  if [ "${REMOTE_SHA}" = "${BASE_SHA}" ]; then
    log "Local branch is ahead of ${REMOTE}/${BRANCH}. Skipping redeploy."
  else
    log "Local branch has diverged from ${REMOTE}/${BRANCH}. Manual intervention required."
  fi
  exit 0
fi

log "New commits detected. Pulling latest ${BRANCH}..."
git pull --ff-only "${REMOTE}" "${BRANCH}"

log "Building Docker image..."
"${REPO_DIR}/build.sh"

log "Restarting app container..."
"${REPO_DIR}/run.sh"

log "Redeploy complete."
