#!/usr/bin/env bash
# Build the scanner image (Dockerfile.scanner) and push to your Mac Mini registry.
#
# Prerequisites:
#   - Docker, repo root as context
#   - Registry reachable at REGISTRY (plain HTTP on port 5005)
#
# Docker Desktop / dockerd: add to daemon.json "insecure-registries":
#   ["100.72.195.102:5005"]
#
set -euo pipefail

REGISTRY="${REGISTRY:-100.72.195.102:5005}"
IMAGE="${IMAGE:-car-scanner}"
TAG="${TAG:-v1}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

cd "${ROOT}"

FULL_IMAGE="${REGISTRY}/${IMAGE}:${TAG}"

echo "Building ${FULL_IMAGE} (context: ${ROOT})"
docker build -f Dockerfile.scanner -t "${FULL_IMAGE}" .

echo "Pushing ${FULL_IMAGE}"
docker push "${FULL_IMAGE}"

echo "Done: ${FULL_IMAGE}"
