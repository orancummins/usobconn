#!/usr/bin/env bash
set -euo pipefail

IMAGE_NAME="${IMAGE_NAME:-usobconn}"
IMAGE_TAG="${IMAGE_TAG:-latest}"

docker build -t "${IMAGE_NAME}:${IMAGE_TAG}" .

rm -rf __pycache__ instance

echo "Built ${IMAGE_NAME}:${IMAGE_TAG}"
echo "Cleaned generated folders: __pycache__, instance"
