#!/usr/bin/env python3
"""
Junk prices to NULL (N6.7, IL-06, owner decision D-N14).

    .venv/bin/python -m backend.scripts.repair_junk_prices              # dry run
    .venv/bin/python -m backend.scripts.repair_junk_prices --apply \\
        --backup-file workspace/backups/junk_prices_<stamp>.csv --expect-db <db>

Active cars whose stored price the write path would now refuse (same rules as
:mod:`backend.utils.price_floor`):

* ``below_floor``: $1 to $999 (234 in the 10-13 clean room);
* ``above_cap``: over $1,000,000 (1: an Accord at $6,001,323);
* ``repeated``: the same price on 5 or more distinct models at one dealer, at or
  above the floor (the wider 10-15 rule found 9 pairs / 205 cars; D-N14 says 5).

``--dry-run`` is the default: the session is read-only and the script prints counts per
class, per dealer, and the first rows. ``--apply`` needs ``--backup-file`` (a new CSV that
receives ``id,vin,dealer_id,price,reason`` before the write) and, on Postgres,
``--expect-db`` equal to the connected database. It sets ``price`` to NULL only; the
incomplete-listings index is re-judged by the next rebuild (N6.1). Prices at or above the
floor outside the repeated-value pairs are never touched.
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Callable, Iterable

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from backend.db import inventory_db, inventory_pg  # noqa: E402
from backend.scripts.repair_new_null_mileage_index import (  # noqa: E402
    Refused,
    _current_db,
    _session_setup,
    check_target,
    describe_target,
    open_backup,
)
from backend.utils.price_floor import PRICE_CAP, PRICE_FLOOR, repeated_value_pairs  # noqa: E402

EXIT_OK, EXIT_REFUSED = 0, 2
BACKUP_COLUMNS = ("id", "vin", "dealer_id", "price", "reason")
_ACTIVE_SQL = (
    "SELECT id, vin, dealer_id, model, price FROM cars "
    "WHERE COALESCE(listing_active, 1) = 1 AND price IS NOT NULL AND price > 0"
)


def find_junk(rows: Iterable[tuple]) -> list[dict[str, Any]]:
    """Classify ``(id, vin, dealer_id, model, price)`` rows; one entry per junk row."""
    rows = [tuple(r) for r in rows]
    pairs = repeated_value_pairs((r[2], r[3], r[4]) for r in rows if r[4] is not None and r[4] >= PRICE_FLOOR)
    out: list[dict[str, Any]] = []
    for car_id, vin, dealer_id, _model, price in rows:
        p = int(round(float(price)))
        if p < PRICE_FLOOR:
            reason = "below_floor"
        elif p > PRICE_CAP:
            reason = "above_cap"
        elif (dealer_id, p) in pairs:
            reason = "repeated"
        else:
            continue
        out.append({"id": car_id, "vin": vin, "dealer_id": dealer_id, "price": p, "reason": reason})
    return out


def run(
    *,
    apply: bool = False,
    backup_file: str | None = None,
    expect_db: str | None = None,
    log: Callable[[str], None] = print,
) -> dict[str, Any]:
    if apply and not backup_file:
        raise Refused("--apply needs --backup-file <new file>")
    is_pg = inventory_pg.is_inventory_postgres()
    target = describe_target()
    inv = inventory_db.get_conn()
    try:
        _session_setup(inv, is_postgres=is_pg, read_only=not apply)
        current_db = _current_db(inv, is_pg)
        target["db"] = current_db
        log("target: " + " ".join(f"{k}={v}" for k, v in target.items()))
        check_target(is_postgres=is_pg, current_db=current_db, expect_db=expect_db, apply=apply)
        junk = find_junk(inv.execute(_ACTIVE_SQL).fetchall())
        by_reason = Counter(j["reason"] for j in junk)
        by_dealer = Counter(j["dealer_id"] for j in junk)
        summary: dict[str, Any] = {
            "mode": "apply" if apply else "dry-run",
            "junk_rows": len(junk),
            "by_reason": dict(by_reason),
            "dealers": len(by_dealer),
            "top_dealers": by_dealer.most_common(10),
            "written": 0,
        }
        if apply and junk:
            fh = open_backup(backup_file)  # type: ignore[arg-type]
            try:
                fh.seek(0)
                fh.truncate()
                w = csv.writer(fh)
                w.writerow(BACKUP_COLUMNS)
                for j in junk:
                    w.writerow([j[c] for c in BACKUP_COLUMNS])
                fh.flush()
                os.fsync(fh.fileno())
            finally:
                fh.close()
            for i in range(0, len(junk), 500):
                ids = [j["id"] for j in junk[i : i + 500]]
                marks = ", ".join("?" for _ in ids)
                inv.execute(f"UPDATE cars SET price = NULL WHERE id IN ({marks})", ids)
            inv.commit()
            summary["written"] = len(junk)
        return summary
    finally:
        inv.close()


def main(argv: Iterable[str] | None = None, *, log: Callable[[str], None] = print) -> int:
    ap = argparse.ArgumentParser(description="Set junk active prices to NULL (dry run by default).")
    ap.add_argument("--apply", action="store_true", help="write the repair (needs --backup-file)")
    ap.add_argument("--backup-file")
    ap.add_argument("--expect-db")
    args = ap.parse_args(list(argv) if argv is not None else None)
    try:
        summary = run(apply=args.apply, backup_file=args.backup_file, expect_db=args.expect_db, log=log)
    except Refused as exc:
        log(f"REFUSED: {exc}")
        return EXIT_REFUSED
    for k, v in summary.items():
        log(f"{k}: {v}")
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
