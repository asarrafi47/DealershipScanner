#!/usr/bin/env bash
# Deploy the web Railway service from a tagged release on main
# (remediation P2B.2, owner decision D-REL10).
#
# Ships `git archive v$(cat VERSION)` plus BUILD_COMMIT and BUILD_TAG, with the
# root railway.toml (Dockerfile.web, healthcheck /health) as the service
# config. Refuses unless the tracked tree is clean, HEAD == main on origin, and
# HEAD carries the annotated tag v$(cat VERSION) that origin also has. Guard,
# stage, options and exit codes: deploy/railway/_guarded_deploy.sh.
#
#   deploy/railway/deploy_web.sh --dry-run                         # guard + stage + plan; never calls railway
#   deploy/railway/deploy_web.sh --dry-run --keep-stage /tmp/web   # then: docker build -f Dockerfile.web /tmp/web
#   deploy/railway/deploy_web.sh                                   # deploy, detached
#
# ALLOW_UNRELEASED_DEPLOY=1 overrides a failed guard with a loud warning. It
# exists for a mid-fleet scanner hotfix; web has no such emergency, so cut a
# release instead.
set -euo pipefail
# shellcheck source=deploy/railway/_guarded_deploy.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_guarded_deploy.sh"

guarded_deploy deploy_web.sh web "" \
  "verify: curl -fsS https://sarraficars.com/api/health   # expect this version and commit" \
  "$@"
