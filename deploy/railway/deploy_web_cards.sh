#!/usr/bin/env bash
# Deploy the web-cards Railway service (the nightly listings-card builder) from the
# SAME tagged release as the web service (docs/RELEASING.md, web-cards).
#
# web-cards is Dockerfile.web with a start command of
# `python -m backend.scripts.build_listings_grid_cards` and a cron schedule of
# 08:30 UTC. It must run the same code revision as web: the stored cards carry the
# card revision (CARD_REV, a hash of the serializer sources) and a builder from another
# tag would rebuild every card to a revision the site does not serve. So this script
# deploys `git archive v$(cat VERSION)` exactly like deploy_web.sh, and refuses unless
# HEAD is that tag, on main at origin. Run it right after deploy_web.sh, from the same
# checkout.
#
# Guard, settings check, stage, options and exit codes live in
# deploy/railway/_guarded_deploy.sh, shared with deploy_web.sh and
# deploy_scanner_nightly.sh. The settings check compares the live service with the
# `web-cards` entry of deploy/railway/service_settings.json (Dockerfile.web, no health
# check, restart NEVER, 1 replica, cron `30 8 * * *`, the start command). The service id is
# not stored in the repository: export RAILWAY_WEB_CARDS_SERVICE_ID first.
#
#   export RAILWAY_WEB_CARDS_SERVICE_ID=<id from the Railway dashboard>
#   deploy/railway/deploy_web_cards.sh --dry-run   # guard + settings + stage + plan; uploads nothing
#   deploy/railway/deploy_web_cards.sh             # deploy the release, detached
#
# A deploy builds the image and may start the container once, which runs the builder: harmless
# (a pass over a current store only reads). Otherwise it runs at the next cron time, or when
# the owner triggers it (docs/RELEASING.md, web-cards, step 6).
set -euo pipefail
# shellcheck source=deploy/railway/_guarded_deploy.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_guarded_deploy.sh"

SERVICE="${SERVICE:-web-cards}"
case "$SERVICE" in
  web | scanner-nightly)
    echo "deploy_web_cards.sh: refusing SERVICE=$SERVICE (this stage runs the card builder as a cron job); use deploy/railway/deploy_web.sh or deploy_scanner_nightly.sh" >&2
    exit 2
    ;;
esac

guarded_deploy deploy_web_cards.sh "$SERVICE" "" \
  "verify: railway logs --service $SERVICE   # after the first run: a line starting 'done in'" \
  "$@"
