#!/usr/bin/env python3
"""P8A.7: driver for the local vPIC fuel-only heal (PHEV labels, the 32 BEV rows).

Wraps ``heal_from_vpic`` with the data-repair discipline: a backup table whose
row count must equal the active count before any write, a guarded rollback, and
a per-dealer ``summary.md`` appender. Every step except ``backup`` is a dry run
unless ``--apply`` is given; ``apply`` additionally needs ``--owner-ok`` and a
verified backup.

  PYTHONPATH=. .venv/bin/python backend/scripts/heal_p8a7_driver.py preflight
  PYTHONPATH=. .venv/bin/python backend/scripts/heal_p8a7_driver.py backup --date 20261026
  PYTHONPATH=. .venv/bin/python backend/scripts/heal_p8a7_driver.py heal --backup cars_precedence_backup_20261026
  PYTHONPATH=. .venv/bin/python backend/scripts/heal_p8a7_driver.py heal --backup T --apply --owner-ok
  PYTHONPATH=. .venv/bin/python backend/scripts/heal_p8a7_driver.py rollback --backup T [--apply]
  PYTHONPATH=. .venv/bin/python backend/scripts/heal_p8a7_driver.py log --backup T [--apply]

Rollback is guarded: a row is restored only while its current value still equals
the ``value`` the heal recorded in the field's provenance, so a later rescan or
a better source is never clobbered. Preconditions the script cannot see
(``ssh mini pgrep -f scanner.py``) are the operator's: ``--mini-checked``.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))

from backend.utils.project_env import load_project_dotenv  # noqa: E402

load_project_dotenv()

ACTIVE = "COALESCE(listing_active,1)=1"
DEFAULT_FIELDS = ("fuel_type",)
_BACKUP_RE = re.compile(r"^cars_precedence_backup_[a-z0-9_]+$")
_CHUNK = 2000


def backup_table_name(date: str | None = None) -> str:
    name = "cars_precedence_backup_" + (date or time.strftime("%Y%m%d"))
    if not _BACKUP_RE.match(name):
        raise ValueError(f"bad backup table name: {name!r}")
    return name


def _check_table(name: str) -> str:
    if not _BACKUP_RE.match(name or ""):
        raise ValueError(f"backup table must match cars_precedence_backup_<tag>: {name!r}")
    return name


def lock_files() -> list[str]:
    paths = {_REPO_ROOT / "workspace" / "scanner.lock"}
    if os.environ.get("SCANNER_LOCK_PATH"):
        paths.add(Path(os.environ["SCANNER_LOCK_PATH"]))
    paths.update((_REPO_ROOT / "workspace").glob("scanner*.lock"))
    return sorted(str(p) for p in paths if p.exists())


def preflight(conn, *, mini_checked: bool = False, locks: list[str] | None = None) -> list[str]:
    """Problems that forbid a write ([] = clear): scanner clients, lock files, the mini check."""
    problems: list[str] = []
    cur = conn.cursor()
    cur.execute("SELECT pid, application_name FROM pg_stat_activity WHERE datname = current_database() "
                "AND pid <> pg_backend_pid() AND application_name ILIKE '%scanner%'")
    for pid, app in cur.fetchall():
        problems.append(f"scanner client in pg_stat_activity: pid {pid} ({app})")
    for p in (lock_files() if locks is None else locks):
        problems.append(f"lock file present: {p}")
    if not mini_checked:
        problems.append("`ssh mini pgrep -f scanner.py` not confirmed (pass --mini-checked)")
    return problems


def create_backup(conn, table: str, fields: tuple[str, ...] = DEFAULT_FIELDS) -> dict[str, Any]:
    """Snapshot id, vin, *fields* and spec_source_json of the active rows; the
    row count must equal the active count or the table is dropped and this raises."""
    _check_table(table)
    cols = ", ".join(("id", "vin", *fields, "spec_source_json"))
    cur = conn.cursor()
    cur.execute(f"CREATE TABLE {table} AS SELECT {cols} FROM cars WHERE {ACTIVE}")
    conn.commit()
    try:
        return verify_backup(conn, table)
    except RuntimeError:
        cur.execute(f"DROP TABLE {table}")
        conn.commit()
        raise


def verify_backup(conn, table: str) -> dict[str, Any]:
    _check_table(table)
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    n = cur.fetchone()[0]
    cur.execute(f"SELECT COUNT(*) FROM cars WHERE {ACTIVE}")
    active = cur.fetchone()[0]
    if n != active:
        raise RuntimeError(f"backup {table} has {n} rows, active count is {active}")
    return {"table": table, "rows": n, "active": active}


def _prov_value(spec_json: Any, field: str) -> tuple[bool, Any]:
    """(has_value, value) of the field's nhtsa_vpic provenance entry."""
    try:
        d = json.loads(spec_json) if isinstance(spec_json, str) else (spec_json or {})
    except (json.JSONDecodeError, TypeError):
        return False, None
    e = d.get(field) if isinstance(d, dict) else None
    if isinstance(e, dict) and e.get("source") == "nhtsa_vpic" and "value" in e:
        return True, e["value"]
    return False, None


def _changed_rows(conn, table: str, fields: tuple[str, ...]):
    """Rows whose current value differs from the backup, with both sides."""
    cur = conn.cursor()
    sel = ", ".join(f"b.{f}, c.{f}" for f in fields)
    cond = " OR ".join(f"b.{f} IS DISTINCT FROM c.{f}" for f in fields)
    cur.execute(f"SELECT c.id, c.vin, c.dealer_id, c.spec_source_json, b.spec_source_json, {sel} "
                f"FROM {table} b JOIN cars c ON c.id = b.id WHERE {cond} ORDER BY c.id")
    for r in cur.fetchall():
        vals = {f: (r[5 + 2 * i], r[6 + 2 * i]) for i, f in enumerate(fields)}  # (backup, current)
        yield {"id": r[0], "vin": r[1], "dealer_id": r[2], "cur_spec": r[3], "bak_spec": r[4], "vals": vals}


def guarded_rollback(conn, table: str, fields: tuple[str, ...] = DEFAULT_FIELDS, *, apply: bool = False) -> dict[str, Any]:
    """Restore backed-up values only where the current value is still the one
    the heal wrote (provenance ``value``); everything else is left alone."""
    _check_table(table)
    stats = {"changed": 0, "restored": 0, "skipped_not_ours": 0, "dry_run": not apply}
    cur = conn.cursor()
    todo: list[tuple[int, dict[str, Any], str]] = []
    for row in _changed_rows(conn, table, fields):
        stats["changed"] += 1
        try:
            cur_prov = json.loads(row["cur_spec"]) if row["cur_spec"] else {}
            bak_prov = json.loads(row["bak_spec"]) if row["bak_spec"] else {}
        except (json.JSONDecodeError, TypeError):
            stats["skipped_not_ours"] += 1
            continue
        if not isinstance(cur_prov, dict) or not isinstance(bak_prov, dict):
            stats["skipped_not_ours"] += 1
            continue
        restore: dict[str, Any] = {}
        for f, (old, now) in row["vals"].items():
            if old == now:
                continue
            ok, written = _prov_value(cur_prov, f)
            if ok and written == now:
                restore[f] = old
                if f in bak_prov:
                    cur_prov[f] = bak_prov[f]
                else:
                    cur_prov.pop(f, None)
        if not restore:
            stats["skipped_not_ours"] += 1
            continue
        stats["restored"] += 1
        todo.append((row["id"], restore, json.dumps(cur_prov, ensure_ascii=False)))
    if apply:
        for i, (car_id, restore, spec) in enumerate(todo, 1):
            sets = ", ".join(f"{f} = ?" for f in restore)
            cur.execute(f"UPDATE cars SET {sets}, spec_source_json = ? WHERE id = ?",
                        (*restore.values(), spec, car_id))
            if i % _CHUNK == 0:
                conn.commit()
        conn.commit()
    return stats


def dealer_lines(conn, table: str, fields: tuple[str, ...] = DEFAULT_FIELDS, *, date: str | None = None) -> dict[str, str]:
    """dealer_id -> one dated summary line counting transitions per field."""
    date = date or time.strftime("%Y-%m-%d")
    per: dict[str, dict[str, int]] = {}
    for row in _changed_rows(conn, table, fields):
        for f, (old, now) in row["vals"].items():
            if old != now:
                key = f"{f} {old or 'NULL'} -> {now or 'NULL'}"
                d = per.setdefault(row["dealer_id"] or "_unknown", {})
                d[key] = d.get(key, 0) + 1
    return {dealer: f"- {date} P8A.7 vPIC fact heal (label repair, not a scan): "
                    + "; ".join(f"{k} x{v}" for k, v in sorted(t.items())) + f" (backup {table})."
            for dealer, t in per.items()}


def append_dealer_logs(lines: dict[str, str], *, root: Path | None = None, apply: bool = False) -> int:
    """Append each line to workspace/dealer_logs/<dealer>/summary.md once (re-runs skip a present line)."""
    root = root or (_REPO_ROOT / "workspace" / "dealer_logs")
    n = 0
    for dealer, line in sorted(lines.items()):
        path = root / dealer / "summary.md"
        if path.exists() and line in path.read_text(encoding="utf-8"):
            continue
        n += 1
        if apply:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as fh:
                fh.write(("\n" if path.exists() and path.stat().st_size else "") + line + "\n")
    return n


def _invariants() -> dict[str, int]:
    """Stored-tier invariant counts on the module's own read-only connection."""
    from backend.scripts.data_quality_invariants import open_readonly_connection, run_stored_tier

    ro = open_readonly_connection()
    try:
        return {r.id: r.count for r in run_stored_tier(ro)}
    finally:
        ro.close()


def _fields(s: str | None) -> tuple[str, ...]:
    return tuple(f.strip() for f in (s or "").split(",") if f.strip()) or DEFAULT_FIELDS


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("step", choices=["preflight", "backup", "heal", "rollback", "log"])
    ap.add_argument("--date", help="backup table suffix (default today, YYYYMMDD)")
    ap.add_argument("--backup", help="existing backup table name")
    ap.add_argument("--fields", help="comma-separated columns (default fuel_type)")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--owner-ok", action="store_true", help="the owner's apply OK (heal --apply needs it)")
    ap.add_argument("--mini-checked", action="store_true", help="I confirmed no scanner runs on the mini")
    args = ap.parse_args(argv)
    fields = _fields(args.fields)
    from backend.db.inventory_db import get_conn

    conn = get_conn()
    try:
        if args.step == "preflight":
            problems = preflight(conn, mini_checked=args.mini_checked)
            print(json.dumps({"problems": problems}))
            return 1 if problems else 0
        if args.step == "backup":
            print(json.dumps(create_backup(conn, backup_table_name(args.date), fields)))
            return 0
        table = _check_table(args.backup or "")
        if args.step == "heal":
            from backend.enrichment.vpic_facts import heal_rows

            print("backup:", json.dumps(verify_backup(conn, table)))
            print("invariants before:", json.dumps(_invariants(), sort_keys=True))
            if args.apply:
                if not args.owner_ok:
                    print("refused: --apply needs --owner-ok", file=sys.stderr)
                    return 2
                problems = preflight(conn, mini_checked=args.mini_checked)
                if problems:
                    print("refused: " + json.dumps(problems), file=sys.stderr)
                    return 2
            stats = heal_rows(conn, dry_run=not args.apply, fields=list(fields))
            ex = stats.pop("examples", [])
            print(("APPLIED " if args.apply else "DRY RUN ") + json.dumps(stats))
            for e in ex:
                print("  ", e)
            if args.apply:
                print("invariants after:", json.dumps(_invariants(), sort_keys=True))
        elif args.step == "rollback":
            print(json.dumps(guarded_rollback(conn, table, fields, apply=args.apply)))
        else:
            lines = dealer_lines(conn, table, fields)
            print(json.dumps({"dealers": len(lines), "appended": append_dealer_logs(lines, apply=args.apply),
                              "dry_run": not args.apply}))
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
