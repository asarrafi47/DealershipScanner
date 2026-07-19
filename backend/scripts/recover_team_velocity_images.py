#!/usr/bin/env python3
"""
Thin CLI wrapper over the Team Velocity handler's image/carfax completion pass.

The real logic lives in :mod:`backend.parsers.team_velocity` — a self-contained
platform handler whose :func:`recover_dealer` fetches each VDP (browser UA, no
browser process), regexes the ``:photoUrls`` gallery + the per-car Carfax report
link, and fills ``cars.image_url`` / ``cars.gallery`` / ``cars.carfax_url``
without ever overwriting a real http value. The delta scan calls the same
function per Team Velocity dealer after upsert; this script is just the manual
entry point.

Usage (repo root)::

  python3 backend/scripts/recover_team_velocity_images.py --dry-run
  python3 backend/scripts/recover_team_velocity_images.py --limit 30
  python3 backend/scripts/recover_team_velocity_images.py --dealer-id markkia-com
  python3 backend/scripts/recover_team_velocity_images.py            # all 3 dealers
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
os.chdir(_REPO_ROOT)
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from backend.utils.project_env import load_project_dotenv

load_project_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("recover_tv_images")

from backend.parsers.team_velocity import TEAM_VELOCITY_DEALERS, recover_dealer


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dealer-id", action="append", default=None,
                    help="restrict to one/more dealers (default: all 3 Team Velocity)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=None,
                    help="max cars per dealer (for pilots)")
    ap.add_argument("--workers", type=int, default=8, help="concurrent VDP fetches")
    ns = ap.parse_args()

    dealer_ids = ns.dealer_id or list(TEAM_VELOCITY_DEALERS)
    log.info("Team Velocity completion: dealers=%s dry_run=%s limit=%s workers=%d",
             dealer_ids, ns.dry_run, ns.limit, ns.workers)

    grand: dict[str, int] = {}
    for did in dealer_ids:
        stats = recover_dealer(did, dry_run=ns.dry_run, limit=ns.limit, workers=ns.workers)
        log.info("[%s] %s", did, json.dumps(stats))
        for k, v in stats.items():
            grand[k] = grand.get(k, 0) + v
    log.info("TOTAL%s: %s", " (dry-run)" if ns.dry_run else "", json.dumps(grand))


if __name__ == "__main__":
    main()
