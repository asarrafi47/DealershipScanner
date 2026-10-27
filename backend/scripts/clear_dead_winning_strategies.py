#!/usr/bin/env python3
"""
Clear cached winning strategies that no longer exist (N33 P17B.8a).

    .venv/bin/python -m backend.scripts.clear_dead_winning_strategies              # dry run
    .venv/bin/python -m backend.scripts.clear_dead_winning_strategies --apply \\
        --backup-file workspace/backups/dealer_scan_profile_<stamp>.csv --expect-db <db>

P17B.4 shrank the recovery chain to ``backend.scanner.dealer.profile.RECOVERY_STRATEGY_ORDER``.
``dealer_scan_profile.last_winning_strategy`` can still hold a retired name
(``dealer_venom_typesense``, ``dealer_on_cosmos``, ``dealer_inspire_algolia``,
``pixel_motion_html``, ``html_next_data``, ``jsonld_listing_html``). The reader already ignores
them (``get_cached_winning_strategy`` logs and returns None), so this is cosmetic hygiene.

A dead name is any non-empty value that is not in the live chain, so the list never goes stale.
``--dry-run`` is the default: the session is read-only and the script prints the count per dead
name. ``--apply`` needs ``--backup-file`` (a new CSV that receives the whole
``dealer_scan_profile`` table first) and, on Postgres, ``--expect-db`` equal to the connected
database. It sets ``last_winning_strategy`` to NULL in one transaction, verifies that no dead
name is left and the row count is unchanged (rolling back otherwise), and leaves ``updated_at``
alone so its isoformat stays uniform with ``profile.py``. Run it against a local copy; prod only
with the owner present.
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
from backend.scanner.dealer.profile import RECOVERY_STRATEGY_ORDER  # noqa: E402
from backend.scripts.repair_new_null_mileage_index import (  # noqa: E402
    Refused,
    _current_db,
    _session_setup,
    check_target,
    describe_target,
)

EXIT_OK, EXIT_REFUSED = 0, 2
BACKUP_COLUMNS = ("dealer_id", "last_winning_strategy", "platform_hints_json", "updated_at")
_COUNTS_SQL = (
    "SELECT last_winning_strategy, COUNT(*) FROM dealer_scan_profile "
    "WHERE last_winning_strategy IS NOT NULL GROUP BY last_winning_strategy"
)


def dead_names(counts: Iterable[tuple[Any, int]], live: Iterable[str] = RECOVERY_STRATEGY_ORDER) -> dict[str, int]:
    """``(value, rows)`` pairs to ``{name: rows}`` for every stored name outside ``live``."""
    live_set = {str(s) for s in live}
    out: Counter[str] = Counter()
    for value, n in counts:
        name = str(value).strip() if value is not None else ""
        if name and name not in live_set:
            out[name] += int(n)
    return dict(out)


def _write_backup(inv: Any, path: str) -> int:
    p = Path(path)
    if p.exists():
        raise Refused(f"backup file already exists, refusing to overwrite: {p}")
    if not p.parent.is_dir():
        raise Refused(f"backup directory does not exist: {p.parent}")
    rows = inv.execute(f"SELECT {', '.join(BACKUP_COLUMNS)} FROM dealer_scan_profile ORDER BY dealer_id").fetchall()
    with open(p, "x", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(BACKUP_COLUMNS)
        for r in rows:
            w.writerow(list(r))
        fh.flush()
        os.fsync(fh.fileno())
    return len(rows)


def _total(inv: Any) -> int:
    return int(inv.execute("SELECT COUNT(*) FROM dealer_scan_profile").fetchone()[0])


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
        dead = dead_names(inv.execute(_COUNTS_SQL).fetchall())
        total = _total(inv)
        summary: dict[str, Any] = {
            "mode": "apply" if apply else "dry-run",
            "rows_total": total,
            "dead_names": dead,
            "dead_rows": sum(dead.values()),
            "written": 0,
        }
        if apply and dead:
            summary["backup_rows"] = _write_backup(inv, backup_file)  # type: ignore[arg-type]
            marks = ", ".join("?" for _ in dead)
            names = list(dead)
            inv.execute(f"UPDATE dealer_scan_profile SET last_winning_strategy = NULL WHERE last_winning_strategy IN ({marks})", names)
            left = int(inv.execute(f"SELECT COUNT(*) FROM dealer_scan_profile WHERE last_winning_strategy IN ({marks})", names).fetchone()[0])
            after = _total(inv)
            if left or after != total:
                inv.rollback()
                raise Refused(f"verification failed (dead rows left {left}, row count {total} -> {after}); rolled back")
            inv.commit()
            summary["written"] = summary["dead_rows"]
            summary["dead_rows_after"] = left
            summary["rows_total_after"] = after
        return summary
    finally:
        inv.close()


def main(argv: Iterable[str] | None = None, *, log: Callable[[str], None] = print) -> int:
    ap = argparse.ArgumentParser(description="NULL retired last_winning_strategy values (dry run by default).")
    ap.add_argument("--apply", action="store_true", help="write the cleanup (needs --backup-file)")
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
