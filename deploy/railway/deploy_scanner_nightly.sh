#!/usr/bin/env bash
# Deploy the scanner-nightly Railway service (docs/RAILWAY_SCANNING.md) from a
# tagged release on main (remediation P2B.2, owner decision D-REL10).
#
# `railway up` from the repo root would pick up the root railway.toml (Dockerfile.web
# + a /health check this job never answers). Stage a clean copy of the release
# instead: `git archive v$(cat VERSION)` (tracked files only, so no .env or
# workspace/ scratch ever leaves the machine), with the archive's own
# deploy/railway/railway.scanner-nightly.json as the stage's root railway.json,
# plus BUILD_COMMIT and BUILD_TAG. The guard, the stage, the options
# (--dry-run, --keep-stage DIR) and the exit codes live in
# deploy/railway/_guarded_deploy.sh, shared with deploy_web.sh.
#
#   deploy/railway/deploy_scanner_nightly.sh --dry-run   # guard + stage + plan; never calls railway
#   deploy/railway/deploy_scanner_nightly.sh             # deploy the release, detached
#   SERVICE=scanner-nightly deploy/railway/deploy_scanner_nightly.sh
#
# Mid-fleet hotfix only (D-REL10): ALLOW_UNRELEASED_DEPLOY=1 deploys the
# committed tree of HEAD without a release, with a loud warning and
# BUILD_TAG=unreleased. Cut a release afterwards.
#
# A deploy starts the container once: it idles (exit 0) unless SCAN_FLEET=1 or
# SCAN_DEALERS is set on the service.
set -euo pipefail
# shellcheck source=deploy/railway/_guarded_deploy.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_guarded_deploy.sh"

SERVICE="${SERVICE:-scanner-nightly}"
if [ "$SERVICE" = "web" ]; then
  echo "deploy_scanner_nightly.sh: refusing SERVICE=web (this stage runs Dockerfile.scanner); use deploy/railway/deploy_web.sh" >&2
  exit 2
fi

_scanner_nightly_stage() {
  local stage="$1"
  rm -f "$stage/railway.toml" "$stage/railway.json"
  cp "$stage/deploy/railway/railway.scanner-nightly.json" "$stage/railway.json"
}

guarded_deploy deploy_scanner_nightly.sh "$SERVICE" _scanner_nightly_stage \
  "verify: railway logs --service $SERVICE   # the container idles unless SCAN_FLEET=1 or SCAN_DEALERS is set" \
  "$@"
