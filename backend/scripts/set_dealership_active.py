"""
Activate or deactivate rooftops, by id or by website.

    .venv/bin/python -m backend.scripts.set_dealership_active --activate --id 92 170 264 410 411
    .venv/bin/python -m backend.scripts.set_dealership_active --deactivate --website https://...

Deactivating a rooftop hides its whole inventory, and the scanner will not visit it again
-- so this is a decision to be made deliberately and by a person, not a side effect of some
larger task. It exists as its own script for exactly that reason: an agent working a
cleanup ticket set is_active=0 on five rows on its own initiative, and having a named,
auditable operation makes both the doing and the undoing explicit.

Prints the before/after state of every row it touches so the change is visible in the log
rather than inferred.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from backend.db.connect import connect as db_connect  # noqa: E402

_log = logging.getLogger("set_active")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--activate", action="store_true")
    mode.add_argument("--deactivate", action="store_true")
    ap.add_argument("--id", nargs="*", type=int, default=[])
    ap.add_argument("--website", nargs="*", default=[])
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if not args.id and not args.website:
        _log.error("nothing selected: pass --id and/or --website")
        return 2

    target = 1 if args.activate else 0

    conn = db_connect(autocommit=True)
    cur = conn.cursor()

    # Build the predicate from what was actually supplied. An earlier version passed
    # `args.website or [""]` as a placeholder, which made the clause read
    # `website_url = ANY(ARRAY[''])` -- and that matches every rooftop whose website is
    # blank. Selecting five ids picked up 86 rows. It happened to be harmless because the
    # extras were already in the target state, but the same call with --deactivate would
    # have silently disabled 81 dealerships.
    clauses: list[str] = []
    params: list[object] = []
    if args.id:
        clauses.append("id = ANY(%s)")
        params.append(args.id)
    if args.website:
        clauses.append("website_url = ANY(%s)")
        params.append(args.website)

    cur.execute(
        "SELECT id, name, website_url, COALESCE(is_active, 1) FROM dealerships "
        f"WHERE {' OR '.join(clauses)}",
        params,
    )
    rows = cur.fetchall()
    if not rows:
        _log.warning("no matching rooftops")
        return 1

    changed = 0
    for dealership_id, name, website, was in rows:
        if was == target:
            _log.info("  id=%-4s %-34s already %s", dealership_id, (name or "")[:34],
                      "active" if target else "inactive")
            continue
        cur.execute("UPDATE dealerships SET is_active = %s WHERE id = %s", (target, dealership_id))
        changed += 1
        _log.info("  id=%-4s %-34s %s -> %s  (%s)", dealership_id, (name or "")[:34],
                  "active" if was else "inactive",
                  "active" if target else "inactive", (website or "")[:44])

    _log.info("%d rooftop(s) changed, %d already in the requested state",
              changed, len(rows) - changed)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
