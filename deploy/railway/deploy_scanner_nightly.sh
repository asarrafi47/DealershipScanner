#!/usr/bin/env bash
# Deploy the scanner-nightly Railway service (docs/RAILWAY_SCANNING.md).
#
# `railway up` from the repo root would pick up the root railway.toml (Dockerfile.web
# + a /health check this job never answers). Stage a clean copy of HEAD instead —
# tracked files only, so no .env or workspace/ ever leaves the machine — with
# deploy/railway/railway.scanner-nightly.json as its root railway.json.
#
#   deploy/railway/deploy_scanner_nightly.sh            # deploy HEAD, detached
#   SERVICE=scanner-nightly deploy/railway/deploy_scanner_nightly.sh
#
# A deploy starts the container once: it idles (exit 0) unless SCAN_FLEET=1 or
# SCAN_DEALERS is set on the service.
set -euo pipefail
REPO="$(cd "$(dirname "$0")/../.." && pwd)"
SERVICE="${SERVICE:-scanner-nightly}"
STAGE="$(mktemp -d "${TMPDIR:-/tmp}/scanner-nightly.XXXXXX")"
trap 'rm -rf "$STAGE"' EXIT
git -C "$REPO" archive --format=tar HEAD | tar -x -C "$STAGE"
rm -f "$STAGE/railway.toml" "$STAGE/railway.json"
cp "$REPO/deploy/railway/railway.scanner-nightly.json" "$STAGE/railway.json"
echo "deploying $(git -C "$REPO" rev-parse --short HEAD) to $SERVICE from $STAGE"
cd "$REPO"  # the Railway project link lives on the repo directory
railway up "$STAGE" --path-as-root --service "$SERVICE" --detach
