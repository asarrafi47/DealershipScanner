#!/usr/bin/env bash
# Deploy the scanner-nightly Railway service (docs/RAILWAY_SCANNING.md) from a
# tagged release on main (remediation P2B.2, owner decision D-REL10).
#
# Stages a clean copy of the release: `git archive v$(cat VERSION)` (tracked
# files only, so no .env or workspace/ scratch ever leaves the machine), plus
# BUILD_COMMIT and BUILD_TAG. Railway builds it with scanner-nightly's own
# service settings (Dockerfile.scanner, no healthcheck, restart NEVER,
# 1 replica, 16 vCPU / 16 GB, no cron), not with a config-as-code file: Railway
# stops reading those on 2026-12-01 (N4.1, D-N24), and the stage no longer
# carries a railway.json. The deploy is refused unless the service's live
# settings equal deploy/railway/service_settings.json (one read-only
# `railway api` query). The guard, the settings check, the stage, the options
# (--dry-run, --no-settings-check, --keep-stage DIR) and the exit codes live in
# deploy/railway/_guarded_deploy.sh, shared with deploy_web.sh.
#
#   deploy/railway/deploy_scanner_nightly.sh --dry-run   # guard + settings + stage + plan; uploads nothing
#   deploy/railway/deploy_scanner_nightly.sh             # deploy the release, detached
#   SERVICE=scanner-nightly deploy/railway/deploy_scanner_nightly.sh
#
# SERVICE must have an entry (serviceId and settings) in service_settings.json.
#
# Mid-fleet hotfix only (D-REL10): ALLOW_UNRELEASED_DEPLOY=1 deploys the
# committed tree of HEAD without a release, with a loud warning and
# BUILD_TAG=unreleased. It never overrides the settings check. Cut a release
# afterwards.
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

guarded_deploy deploy_scanner_nightly.sh "$SERVICE" "" \
  "verify: railway logs --service $SERVICE   # the container idles unless SCAN_FLEET=1 or SCAN_DEALERS is set" \
  "$@"
