"""
P13B.4a: census, dump, verify and dry-run DROP list for the Postgres backup tables.

The plan (D-DB1 (a), extended 2026-10-18) exports and then drops the backup tables
that pile up in ``public``. This module is the tooling for the first half. It NEVER
drops anything: the last step, ``plan``, prints the exact ``DROP TABLE`` list for owner
review, and the drop itself is a migration (P13B.4b) the owner applies after the dumps
exist. ``MIGRATE_ON_BOOT`` / ``INVENTORY_AUTO_MIGRATE`` apply a merged migration
automatically, so that file must not merge before ``plan`` shows every table VERIFIED.

Steps (each a subcommand; the DSN comes from ``--dsn`` or ``INVENTORY_DATABASE_URL``
and is never printed):

1. ``census``  Read-only. Every candidate table that exists, with exact rows and size,
               its category, and the tables that LOOK like backups but are in no list
               (reported, never dropped). Live quarantine tables are protected.
2. ``dump``    ``pg_dump -Fc -t public.<t> ...`` of every existing candidate into
               ``workspace/backups/backup_tables_<db>_<date>.dump`` plus a
               ``.manifest.json`` (row counts at dump time, sha256, pg_dump version).
3. ``verify``  ``pg_restore`` of the dump into a SCRATCH database (name must be
               ``cars_draft_*``, ``cars_cr_*`` or ``cars_rehearsal``, never the source) and
               a row-count compare against the manifest. A table is VERIFIED when the
               restored count equals the dumped count. The manifest records it.
4. ``plan``    Dry run. ``to_regclass`` per candidate, the live row count against the
               manifest, and the DROP list. A table that is not VERIFIED, or whose live
               count changed since the dump, is BLOCKED and gets no DROP line.

    .venv/bin/python -m backend.scripts.backup_tables_export census
    .venv/bin/python -m backend.scripts.backup_tables_export dump --include-plan-snapshots
    .venv/bin/python -m backend.scripts.backup_tables_export verify --scratch-db cars_draft_bk
    .venv/bin/python -m backend.scripts.backup_tables_export plan

Restore one table from a dump (needed by ``backfill_extended_specs
--repair-provenance-from``, which reads an ``epa_extended_specs_bak_*`` table):

    createdb cars_draft_restore
    pg_restore --no-owner -d cars_draft_restore -t epa_extended_specs_bak_badhp_20260712 \
        workspace/backups/backup_tables_cars_20261027.dump
    # or into the live DB under its original name (table must not exist):
    pg_restore --no-owner -d "$INVENTORY_DATABASE_URL" -t <table> <dump>

The tables are plain snapshots (no foreign keys into live tables), so a restore needs
no other table. See docs/db_layer/SCHEMA_LEDGER_2026_10.md, "Backup tables".
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable

_REPO_ROOT = Path(__file__).resolve().parents[2]
BACKUP_DIR = _REPO_ROOT / "workspace" / "backups"

_SAFE_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
# Same shape as scripts/cleanroom/build_db.sh's scratch-name guard, plus cars_draft_* for drafts.
_SCRATCH_RE = re.compile(r"^(cars_cr_[a-z0-9_]+|cars_draft_[a-z0-9_]+|cars_rehearsal)$")

# D-DB1 list, extended 2026-10-18: V001's 12 backup tables, the EPA link backup, the
# prod-only owner backup, and the four untracked log tables (D-N30: adopt in V026,
# export and drop here; the exporter only dumps them, the owner decides at plan time).
CORE_TABLES: tuple[str, ...] = (
    "cars_backup_capacity_20260718",
    "cars_backup_mcpeek_prescan_20260712",
    "cars_backup_mileage_contam_20260712",
    "cars_backup_modelname_20260712",
    "cars_backup_stock_contam_20260711_171841",
    "cars_backup_stock_contam_20260711_171901",
    "cars_backup_vinrepair_20260713",
    "epa_extended_specs_bak_20260711_101806",
    "epa_extended_specs_bak_20260712_083724",
    "epa_extended_specs_bak_badhp_20260712",
    "epa_extended_specs_bak_badratio_20260712",
    "epa_master_dump_id_map",
    "cars_epa_link_backup_20260921",
    "cars_owner_backup_20260929",
)
LOG_TABLES: tuple[str, ...] = (
    "image_summary_restore_log",
    "make_canon_log",
    "model_canon_log",
    "package_value_purge_log",
)
# The plan's own snapshots (cars_retire_backup_*, cars_precedence_backup_*, ...): dumped
# only with --include-plan-snapshots, as "the owner decides".
PLAN_SNAPSHOT_RE = re.compile(
    r"^(cars_retire_backup_[a-z0-9_]+|cars_precedence_backup_[a-z0-9_]+|"
    r"cars_epa_link_snapshot_[a-z0-9_]+|cars_owner_backup_[a-z0-9_]+)$"
)
# Live tables that look like backups but feed readers. Never candidates.
PROTECTED: frozenset[str] = frozenset({"cars_trim_quarantine", "package_values_msrp_quarantine"})
# Names that merely resemble backups; used only to report "unlisted" tables.
_LOOKS_LIKE_BACKUP_RE = re.compile(r"(bak|backup|dump|snapshot|_log$)")


def classify(name: str, *, include_plan_snapshots: bool = False, include_logs: bool = True) -> str | None:
    """Category of ``name`` as a drop candidate, or None. Protected and unsafe names are None."""
    if name in PROTECTED or not _SAFE_NAME_RE.match(name or ""):
        return None
    if name in CORE_TABLES:
        return "core"
    if include_logs and name in LOG_TABLES:
        return "log"
    if include_plan_snapshots and PLAN_SNAPSHOT_RE.match(name):
        return "plan_snapshot"
    return None


def assert_scratch_db(name: str, source_db: str | None = None) -> str:
    if not _SCRATCH_RE.match(name or ""):
        raise ValueError(f"{name!r} is not a scratch DB name (cars_draft_*, cars_cr_*, cars_rehearsal)")
    if source_db and name == source_db:
        raise ValueError("the scratch DB is the source DB")
    return name


def _quote_ident(name: str) -> str:
    if not _SAFE_NAME_RE.match(name):
        raise ValueError(f"unsafe table name {name!r}")
    return '"' + name + '"'


def dsn_from_env(explicit: str | None = None) -> str:
    dsn = explicit or os.environ.get("INVENTORY_DATABASE_URL") or ""
    if not dsn:
        raise SystemExit("no DSN: pass --dsn or set INVENTORY_DATABASE_URL")
    return dsn


def dsn_with_dbname(dsn: str, dbname: str) -> str:
    """``dsn`` pointing at ``dbname`` (URL form or key=value form)."""
    if "://" in dsn:
        base, sep, query = dsn.partition("?")
        head, _, _old = base.rpartition("/")
        return f"{head}/{dbname}{sep}{query}"
    if re.search(r"\bdbname=\S+", dsn):
        return re.sub(r"\bdbname=\S+", f"dbname={dbname}", dsn)
    return f"{dsn} dbname={dbname}"


def _connect(dsn: str, *, readonly: bool = True):
    import psycopg

    conn = psycopg.connect(dsn, autocommit=True, connect_timeout=10)
    if readonly:
        conn.execute("SET default_transaction_read_only = on")
        conn.execute("SET statement_timeout = '60s'")
    return conn


def current_db(conn) -> str:
    return conn.execute("SELECT current_database()").fetchone()[0]


def list_public_tables(conn) -> list[str]:
    """Names of ordinary public tables that could be backups (cheap name filter in SQL)."""
    rows = conn.execute(
        "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p') ORDER BY 1"
    ).fetchall()
    return [r[0] for r in rows]


def table_stats(conn, name: str) -> dict[str, Any] | None:
    """Exact rows and total size of ``public.<name>``; None when it does not exist."""
    exists = conn.execute("SELECT to_regclass(%s)", (f"public.{_quote_ident(name)}",)).fetchone()[0]
    if exists is None:
        return None
    rows = conn.execute(f"SELECT count(*) FROM public.{_quote_ident(name)}").fetchone()[0]
    size = conn.execute("SELECT pg_total_relation_size(%s::regclass)", (f"public.{_quote_ident(name)}",)).fetchone()[0]
    return {"rows": int(rows), "bytes": int(size)}


def census(conn, *, include_plan_snapshots: bool = False) -> dict[str, Any]:
    """Candidates that exist (with stats), candidates absent here, and unlisted look-alikes."""
    present = list_public_tables(conn)
    candidates: list[dict[str, Any]] = []
    unlisted: list[str] = []
    for name in present:
        cat = classify(name, include_plan_snapshots=include_plan_snapshots)
        if cat is not None:
            st = table_stats(conn, name) or {"rows": 0, "bytes": 0}
            candidates.append({"table": name, "category": cat, **st})
        elif name in PROTECTED:
            continue
        elif _LOOKS_LIKE_BACKUP_RE.search(name):
            unlisted.append(name)
    wanted = list(CORE_TABLES) + list(LOG_TABLES)
    absent = [t for t in wanted if t not in present]
    return {"db": current_db(conn), "candidates": candidates, "absent": absent, "unlisted": unlisted}


def dump_path(db: str, date: str | None = None, directory: Path | None = None) -> Path:
    if not _SAFE_NAME_RE.match(db.lower()):
        raise ValueError(f"unsafe database name {db!r}")
    return (directory or BACKUP_DIR) / f"backup_tables_{db}_{date or time.strftime('%Y%m%d')}.dump"


def manifest_path(dump: Path) -> Path:
    return dump.with_suffix(dump.suffix + ".manifest.json")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def pg_dump_cmd(dsn: str, tables: list[str], out: Path) -> list[str]:
    cmd = ["pg_dump", "-Fc", "--no-owner", "-f", str(out), "-d", dsn]
    for t in tables:
        cmd += ["-t", f"public.{_quote_ident(t)}"]
    return cmd


def pg_restore_cmd(dsn: str, dump: Path) -> list[str]:
    return ["pg_restore", "--no-owner", "--exit-on-error", "-d", dsn, str(dump)]


def make_dump(conn, dsn: str, *, include_plan_snapshots: bool = False, directory: Path | None = None,
              date: str | None = None, runner: Callable[..., Any] = subprocess.run) -> Path:
    """Dump every existing candidate into one file and write its manifest. Refuses to overwrite."""
    info = census(conn, include_plan_snapshots=include_plan_snapshots)
    cands = info["candidates"]
    if not cands:
        raise SystemExit("no backup tables present; nothing to dump")
    out = dump_path(info["db"], date, directory)
    if out.exists() or manifest_path(out).exists():
        raise SystemExit(f"{out} already exists; refusing to overwrite a dump")
    out.parent.mkdir(parents=True, exist_ok=True)
    tables = [c["table"] for c in cands]
    res = runner(pg_dump_cmd(dsn, tables, out), check=False, capture_output=True, text=True)
    if getattr(res, "returncode", 1) != 0:
        out.unlink(missing_ok=True)
        err = (getattr(res, "stderr", "") or "").replace(dsn, "<dsn>")
        raise SystemExit(f"pg_dump failed: {err.strip()[:500]}")
    ver = runner(["pg_dump", "--version"], check=False, capture_output=True, text=True)
    manifest = {
        "db": info["db"],
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "pg_dump": (getattr(ver, "stdout", "") or "").strip(),
        "dump": out.name,
        "bytes": out.stat().st_size,
        "sha256": sha256_file(out),
        "tables": {c["table"]: {"category": c["category"], "rows": c["rows"], "verified": False} for c in cands},
    }
    manifest_path(out).write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return out


def load_manifest(dump: Path) -> dict[str, Any]:
    mp = manifest_path(dump)
    if not mp.exists():
        raise SystemExit(f"no manifest next to {dump}")
    return json.loads(mp.read_text())


def verify_dump(dump: Path, admin_dsn: str, scratch_db: str, *, source_db: str | None = None,
                runner: Callable[..., Any] = subprocess.run, connect: Callable[..., Any] = _connect) -> dict[str, Any]:
    """Restore ``dump`` into the (existing, empty) scratch DB and compare row counts to the manifest.

    The scratch DB is created and dropped by the operator (or the test); this function only
    writes into it and refuses a name outside the scratch family. Returns per-table results
    and rewrites the manifest with ``verified`` flags.
    """
    assert_scratch_db(scratch_db, source_db)
    manifest = load_manifest(dump)
    if sha256_file(dump) != manifest["sha256"]:
        raise SystemExit("dump sha256 differs from its manifest")
    scratch_dsn = dsn_with_dbname(admin_dsn, scratch_db)
    res = runner(pg_restore_cmd(scratch_dsn, dump), check=False, capture_output=True, text=True)
    if getattr(res, "returncode", 1) != 0:
        err = (getattr(res, "stderr", "") or "").replace(scratch_dsn, "<dsn>")
        raise SystemExit(f"pg_restore failed: {err.strip()[:500]}")
    conn = connect(scratch_dsn)
    results: dict[str, Any] = {}
    try:
        for name, meta in manifest["tables"].items():
            st = table_stats(conn, name)
            ok = st is not None and st["rows"] == meta["rows"]
            results[name] = {"expected": meta["rows"], "restored": None if st is None else st["rows"], "ok": ok}
            meta["verified"] = ok
    finally:
        conn.close()
    manifest["verified_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    manifest_path(dump).write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return results


def plan_drops(conn, manifest: dict[str, Any]) -> dict[str, Any]:
    """Dry run: which manifest tables may be dropped. Executes nothing but SELECTs."""
    if manifest.get("db") and manifest["db"] != current_db(conn):
        raise SystemExit(f"manifest is for database {manifest['db']!r}, connected to {current_db(conn)!r}")
    drop: list[str] = []
    blocked: list[tuple[str, str]] = []
    absent: list[str] = []
    for name, meta in sorted(manifest["tables"].items()):
        if name in PROTECTED or not _SAFE_NAME_RE.match(name):
            blocked.append((name, "protected or unsafe name"))
            continue
        st = table_stats(conn, name)
        if st is None:
            absent.append(name)
        elif not meta.get("verified"):
            blocked.append((name, "dump not verified"))
        elif st["rows"] != meta["rows"]:
            blocked.append((name, f"rows changed since the dump ({meta['rows']} -> {st['rows']}); dump again"))
        else:
            drop.append(name)
    return {"db": manifest.get("db"), "drop": drop, "blocked": blocked, "absent": absent}


def format_plan(plan: dict[str, Any]) -> str:
    lines = [f"-- DRY RUN for database {plan['db']}: nothing was executed.",
             "-- Review, then put these in the P13B.4b migration; do not run by hand."]
    for name in plan["drop"]:
        lines.append(f"DROP TABLE IF EXISTS public.{_quote_ident(name)};")
    for name, why in plan["blocked"]:
        lines.append(f"-- BLOCKED {name}: {why}")
    for name in plan["absent"]:
        lines.append(f"-- absent here {name}")
    lines.append(f"-- {len(plan['drop'])} to drop, {len(plan['blocked'])} blocked, {len(plan['absent'])} absent")
    return "\n".join(lines)


def _fmt_bytes(n: int) -> str:
    for unit in ("B", "kB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return str(n)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--dsn", help="default: INVENTORY_DATABASE_URL (never printed)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("census", "dump", "plan", "verify"):
        p = sub.add_parser(name)
        if name in ("census", "dump"):
            p.add_argument("--include-plan-snapshots", action="store_true",
                           help="also cars_retire_backup_*, cars_precedence_backup_*, cars_epa_link_snapshot_*, cars_owner_backup_*")
        if name in ("verify", "plan"):
            p.add_argument("--dump", type=Path, help="default: newest backup_tables_<db>_*.dump in workspace/backups")
        if name == "verify":
            p.add_argument("--scratch-db", required=True, help="existing empty DB: cars_draft_*, cars_cr_*, cars_rehearsal")
    args = ap.parse_args(argv)
    dsn = dsn_from_env(args.dsn)

    if args.cmd in ("verify", "plan") and not args.dump:
        found = sorted(BACKUP_DIR.glob("backup_tables_*.dump"))
        if not found:
            raise SystemExit("no dump in workspace/backups; run `dump` first")
        args.dump = found[-1]

    conn = _connect(dsn)
    try:
        if args.cmd == "census":
            info = census(conn, include_plan_snapshots=args.include_plan_snapshots)
            print(f"database {info['db']}")
            total = 0
            for c in info["candidates"]:
                total += c["bytes"]
                print(f"  {c['category']:<13} {c['table']:<46} {c['rows']:>10} rows {_fmt_bytes(c['bytes']):>10}")
            print(f"{len(info['candidates'])} candidates, {_fmt_bytes(total)}")
            for t in info["unlisted"]:
                print(f"  UNLISTED (looks like a backup, in no list, never dropped): {t}")
            return 0
        if args.cmd == "dump":
            out = make_dump(conn, dsn, include_plan_snapshots=args.include_plan_snapshots)
            print(f"wrote {out} and {manifest_path(out).name}")
            return 0
        if args.cmd == "verify":
            res = verify_dump(args.dump, dsn, args.scratch_db, source_db=current_db(conn))
            for name, r in res.items():
                print(f"  {'OK ' if r['ok'] else 'BAD'} {name}: dumped {r['expected']}, restored {r['restored']}")
            return 0 if all(r["ok"] for r in res.values()) else 1
        print(format_plan(plan_drops(conn, load_manifest(args.dump))))
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
