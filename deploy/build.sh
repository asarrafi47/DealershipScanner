#!/usr/bin/env bash
# Build and push dealership Docker images.
# Usage:
#   ./deploy/build.sh                    # build + push both images with :latest
#   ./deploy/build.sh --tag v1.2.3       # tag with version string
#   ./deploy/build.sh --push             # build and push (default: build only)
#   REGISTRY=ghcr.io/myorg ./deploy/build.sh --push

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
REGISTRY="${REGISTRY:-ghcr.io/CHANGE_ME}"
TAG="${TAG:-latest}"
PUSH=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tag) TAG="$2"; shift 2 ;;
    --push) PUSH=true; shift ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done

WEB_IMAGE="$REGISTRY/dealership-web:$TAG"
SCANNER_IMAGE="$REGISTRY/dealership-scanner:$TAG"

echo "==> Building $WEB_IMAGE"
docker build -f "$REPO_ROOT/Dockerfile.web" -t "$WEB_IMAGE" "$REPO_ROOT"

echo "==> Building $SCANNER_IMAGE"
docker build -f "$REPO_ROOT/Dockerfile.scanner" -t "$SCANNER_IMAGE" "$REPO_ROOT"

if $PUSH; then
  echo "==> Pushing $WEB_IMAGE"
  docker push "$WEB_IMAGE"
  echo "==> Pushing $SCANNER_IMAGE"
  docker push "$SCANNER_IMAGE"
fi

echo "==> Done. Images:"
echo "    $WEB_IMAGE"
echo "    $SCANNER_IMAGE"

# Patch the image refs into the k8s manifests (optional — or use kustomize image transforms)
if $PUSH && [[ "$TAG" != "latest" ]]; then
  echo ""
  echo "==> To deploy:"
  echo "    sed -i 's|REGISTRY/dealership-web:latest|$WEB_IMAGE|g' deploy/k8s/deployment-web.yaml"
  echo "    sed -i 's|REGISTRY/dealership-scanner:latest|$SCANNER_IMAGE|g' deploy/k8s/cronjob-scanner.yaml deploy/k8s/cronjob-enrichment.yaml"
  echo "    kubectl apply -k deploy/k8s/"
fi
