#!/usr/bin/env python3
"""N21.10 (W19): ``cars.dealership_registry_id`` for DealerOn cosmos dealers that lack it.

A cosmos dealer is one whose stored recipe replays ``/cosmos/srp/vehicles/``. For each,
the active cars with no registry id get one from, in order:

1. the id its own stamped cars already carry (``registry_dealer_ids``), the only
   source that tells stores on a shared host apart (N8.14);
2. the registry row whose website host equals the cars' ``dealer_url`` host, only when
   that host names ONE store (``registry_id_by_dealer_host``).

A shared host with no stamped sibling, a host with no registry row, and a dealer whose
cars resolve to more than one id are reported and left alone: never a host guess.

DRY RUN BY DEFAULT. ``--apply`` writes and needs an owner-approved diff and a backup.
"""
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from backend.db.dealer_geo import normalize_dealer_host  # noqa: E402
from backend.listings.dealer_registry_match import (  # noqa: E402
    registry_dealer_ids,
    registry_id_by_dealer_host,
    shared_registry_hosts,
)

COSMOS_MARKER = "%/cosmos/srp/vehicles/%"
_UNLINKED = "(dealership_registry_id IS NULL OR CAST(dealership_registry_id AS INTEGER) <= 0)"


@dataclass
class DealerPlan:
    dealer_id: str
    unlinked: int
    registry_id: int | None = None
    source: str = ""        # "stamped_siblings" | "host"
    skipped: str = ""       # "shared_host" | "no_registry_row" | "ambiguous" | "no_url"


@dataclass
class Plan:
    cosmos_dealers: int = 0
    dealers: list[DealerPlan] = field(default_factory=list)

    @property
    def fills(self) -> list[DealerPlan]:
        return [d for d in self.dealers if d.registry_id]

    @property
    def skipped(self) -> list[DealerPlan]:
        return [d for d in self.dealers if not d.registry_id]


def _cell(row: Any, i: int, key: str) -> Any:
    return row.get(key) if isinstance(row, dict) else row[i]


def cosmos_dealer_ids(conn: Any) -> list[str]:
    cur = conn.cursor()
    cur.execute("SELECT dealer_id FROM dealer_recipes WHERE recipes_json LIKE ? ORDER BY dealer_id",
                (COSMOS_MARKER,))
    return [str(_cell(r, 0, "dealer_id")) for r in cur.fetchall() if _cell(r, 0, "dealer_id")]


def build_plan(conn: Any) -> Plan:
    plan = Plan()
    dealer_ids = cosmos_dealer_ids(conn)
    plan.cosmos_dealers = len(dealer_ids)
    if not dealer_ids:
        return plan
    stamped = {did: rid for rid, did in registry_dealer_ids(conn).items()}
    by_host = registry_id_by_dealer_host(conn)
    shared = shared_registry_hosts(conn)
    cur = conn.cursor()
    for did in dealer_ids:
        cur.execute(
            f"""SELECT dealer_url, COUNT(*) FROM cars
                WHERE dealer_id = ? AND COALESCE(listing_active, 1) = 1 AND {_UNLINKED}
                GROUP BY dealer_url""",
            (did,),
        )
        rows = [(str(_cell(r, 0, "dealer_url") or ""), int(_cell(r, 1, "count") or 0)) for r in cur.fetchall()]
        n = sum(c for _, c in rows)
        if not n:
            continue  # nothing lacks an id: not a candidate
        d = DealerPlan(dealer_id=did, unlinked=n)
        plan.dealers.append(d)
        if did in stamped:
            d.registry_id, d.source = stamped[did], "stamped_siblings"
            continue
        hosts = {normalize_dealer_host(u) for u, _ in rows if u}
        hosts.discard("")
        if not hosts:
            d.skipped = "no_url"
        elif hosts & shared:
            d.skipped = "shared_host"
        else:
            ids = {by_host[h] for h in hosts if h in by_host}
            if len(ids) == 1 and all(h in by_host for h in hosts):
                d.registry_id, d.source = ids.pop(), "host"
            else:
                d.skipped = "ambiguous" if ids else "no_registry_row"
    return plan


def apply_plan(conn: Any, plan: Plan) -> int:
    cur = conn.cursor()
    total = 0
    for d in plan.fills:
        cur.execute(
            f"""UPDATE cars SET dealership_registry_id = ?
                WHERE dealer_id = ? AND COALESCE(listing_active, 1) = 1 AND {_UNLINKED}""",
            (d.registry_id, d.dealer_id),
        )
        total += int(cur.rowcount or 0)
    conn.commit()
    return total


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true", help="Write the ids. Default is a dry run.")
    args = ap.parse_args(argv)
    from backend.db.inventory_db import db_conn

    with db_conn() as conn:
        plan = build_plan(conn)
        for d in plan.fills:
            print(f"  {d.dealer_id}: {d.unlinked} row(s) -> registry {d.registry_id} ({d.source})")
        for d in plan.skipped:
            print(f"  {d.dealer_id}: {d.unlinked} row(s) left alone ({d.skipped})")
        print(f"{plan.cosmos_dealers} cosmos dealer(s); {len(plan.fills)} to fill "
              f"({sum(d.unlinked for d in plan.fills)} rows), {len(plan.skipped)} left alone.")
        if not args.apply:
            print("Dry run. Re-run with --apply after backup and owner approval.")
            return 0
        print(f"Updated {apply_plan(conn, plan)} row(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
