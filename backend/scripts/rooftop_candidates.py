#!/usr/bin/env python3
"""List or update the unrostered-rooftop queue (N8.13).

  PYTHONPATH=. python backend/scripts/rooftop_candidates.py [--all] [--json]
  PYTHONPATH=. python backend/scripts/rooftop_candidates.py --mark ID --status profiled

The queue is ``workspace/rooftop_candidates.json`` (``ROOFTOP_CANDIDATES_PATH``), filled by
scans that run with ``SCANNER_ROOFTOP_CANDIDATES=1``. The dealer-discovery workflow takes the
``new`` rows, runs the full NETWORK_SCAN_PROCESS loop for each, adds the roster entry and marks
it ``profiled``. Reads and edits the queue file only; no network, no database writes.
"""
from __future__ import annotations

import argparse
import json
import sys

from backend.attribution import candidates as cand

STATUSES = ("new", "profiled", "ignored")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--all", action="store_true", help="include profiled and ignored rows")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--mark", metavar="ID", help="candidate id to update")
    ap.add_argument("--status", choices=STATUSES, default="profiled")
    args = ap.parse_args(argv)
    path = cand.store_path()
    if args.mark:
        store = cand.load(path)
        item = (store.get("candidates") or {}).get(args.mark)
        if item is None:
            print(f"no candidate {args.mark!r} in {path}", file=sys.stderr)
            return 1
        item["status"] = args.status
        path.write_text(json.dumps(store, indent=1, sort_keys=True))
        print(f"{args.mark}: {args.status}")
        return 0
    rows = list((cand.load(path).get("candidates") or {}).values()) if args.all else cand.new_candidates(path)
    if args.json:
        print(json.dumps(rows, indent=1))
        return 0
    for c in rows:
        print(f"{c.get('status', 'new'):9} {c.get('car_count', 0):5} cars  {c.get('name') or '?'} | "
              f"{c.get('address') or '-'} | store {c.get('store_id') or '-'} | {c.get('host') or '-'} | "
              f"{c.get('first_seen')}..{c.get('last_seen')} via {','.join(c.get('seen_via') or [])}")
    print(f"{len(rows)} candidate(s) in {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
