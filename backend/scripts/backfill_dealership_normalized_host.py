#!/usr/bin/env python3
"""Backfill ``dealerships.normalized_host`` from canonical website URLs."""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from backend.db.dealership_url import normalized_dealership_host  # noqa: E402
from backend.db.inventory_db import db_conn  # noqa: E402
from backend.utils.project_env import bootstrap_inventory_script  # noqa: E402


def main() -> int:
    bootstrap_inventory_script()
    updated = 0
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, website_url, dealer_website_url, normalized_host
            FROM dealerships
            """
        )
        rows = cur.fetchall()
        for row in rows:
            if isinstance(row, dict):
                rid = int(row["id"])
                website_url = row.get("website_url")
                dealer_website_url = row.get("dealer_website_url")
                existing = row.get("normalized_host")
            else:
                rid, website_url, dealer_website_url, existing = row
            host = normalized_dealership_host(
                row={"website_url": website_url, "dealer_website_url": dealer_website_url}
            )
            if not host or (existing and str(existing).strip().lower() == host):
                continue
            cur.execute(
                "UPDATE dealerships SET normalized_host = ? WHERE id = ?",
                (host, int(rid)),
            )
            updated += 1
        conn.commit()
    print(f"Updated normalized_host on {updated} dealership row(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
