#!/usr/bin/env bash
# Deploy the web Railway service from a tagged release on main
# (remediation P2B.2, owner decision D-REL10).
#
# Ships `git archive v$(cat VERSION)` plus BUILD_COMMIT and BUILD_TAG. Railway
# builds it with web's own service settings (Dockerfile.web, healthcheck
# /health within 300 s, restart ON_FAILURE x5, 1 replica), not with a
# config-as-code file: Railway stops reading those on 2026-12-01 (N4.1, D-N24).
# Refuses unless the tracked tree is clean, HEAD == main on origin, HEAD carries
# the annotated tag v$(cat VERSION) that origin also has, and web's live
# settings equal deploy/railway/service_settings.json (one read-only
# `railway api` query). Guard, settings check, stage, options and exit codes:
# deploy/railway/_guarded_deploy.sh.
#
#   deploy/railway/deploy_web.sh --dry-run                         # guard + settings + stage + plan; uploads nothing
#   deploy/railway/deploy_web.sh --dry-run --keep-stage /tmp/web   # then: docker build -f Dockerfile.web /tmp/web
#   deploy/railway/deploy_web.sh                                   # deploy, detached
#
# ALLOW_UNRELEASED_DEPLOY=1 overrides a failed release guard (never the
# settings check) with a loud warning. It exists for a mid-fleet scanner
# hotfix; web has no such emergency, so cut a release instead.
set -euo pipefail
# shellcheck source=deploy/railway/_guarded_deploy.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_guarded_deploy.sh"

guarded_deploy deploy_web.sh web "" \
  "verify: curl -fsS https://sarraficars.com/api/health   # expect this version and commit" \
  "$@"
