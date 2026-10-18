#!/usr/bin/env python3
"""
Schema drift report: what the migration chain builds vs what a live database has.

``migrations/`` is supposed to be the inventory schema, but runtime DDL, one-off
repair scripts and V019's record-only history mean a live database can hold
objects the chain never creates, lack objects it does create, or define the same
name differently. This tool answers "which, exactly" for one target database:

1. Build the chain into a throwaway database ``pytest_chain_<hex>`` on a LOCAL
   server (D-TC9: never ``cars``, ``cars_cleanroom``, a non-local host or port
   15432; a name that already exists is refused, never reused or dropped).
   Until ``migrate.py`` learns the conditional V019 record-only rule (P4.2),
   the tool records V019 without running it itself, exactly as a fresh
   ``migrate --apply`` must; once ``migrate.RECORD_ONLY`` exists it calls
   ``migrate.main(['--apply'])`` against the scratch database instead.
2. Open the target through :func:`backend.db.connect.connect` with
   ``read_only=True`` and refuse unless the server reports
   ``default_transaction_read_only`` and ``transaction_read_only`` both ``on``.
   Every statement on the target is a SELECT, a SHOW or ``SET statement_timeout``.
3. Snapshot both with the catalog queries of
   ``backend/tests/test_schema_from_migrations.py::_snapshot`` (copied, not
   edited: tables, columns, indexes, constraints), plus sequences, non-extension
   functions, triggers, views and extensions.
4. Diff the two snapshots (a pure function), then explain each difference from
   :data:`KNOWN_DRIFT` (sourced from docs/db_layer/SCHEMA_LEDGER_2026_10.md and the
   owner decisions) or from the target's own ``schema_migrations`` (objects of a
   migration the target has not applied). Whatever is left is UNEXPLAINED.
5. Drop the scratch database in ``finally``.

Exit code: 0 = no unexplained drift, 1 = unexplained drift, 2 = refused or failed.

Building V001 (2,252 lines, pgvector) costs a few seconds of CPU: run on AC power
for repeated runs.

Usage:
  # a local DB on the scratch server (the usual case)
  python -m backend.scripts.schema_drift_report --target-db cars
  python -m backend.scripts.schema_drift_report --target-db cars_cleanroom --json-out /tmp/cr.json

  # any other target: the inventory DSN from the environment (INVENTORY_DATABASE_URL,
  # then DATABASE_URL, then .env), opened read-only. Prod is an OWNER step only:
  INVENTORY_DATABASE_URL="$DATABASE_PUBLIC_URL" python -m backend.scripts.schema_drift_report

The scratch server defaults to the local Homebrew socket
(``postgresql:///postgres?host=/tmp&port=5432``); ``--scratch-admin-dsn`` names
another local server. The chain is always built locally, whatever the target is.
"""
from __future__ import annotations

import argparse
import fnmatch
import json
import logging
import os
import re
import sys
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

log = logging.getLogger("schema_drift_report")

MIGRATIONS_DIR = _REPO_ROOT / "migrations"
DEFAULT_SCRATCH_ADMIN_DSN = "postgresql:///postgres?host=/tmp&port=5432"
SCRATCH_PREFIX = "pytest_chain_"
_SCRATCH_NAME_RE = re.compile(r"^pytest_chain_[a-z0-9_]{1,40}$")
# D-TC9: the guard refuses these by name, wherever they live.
PROTECTED_DBS = frozenset({"cars", "cars_cleanroom", "railway", "postgres", "template0", "template1"})
# The mini's SSH tunnel to the MBP inventory server (memory: mini launches use 15432).
TUNNEL_PORT = "15432"
_LOCAL_HOSTS = frozenset({"", "localhost", "127.0.0.1", "::1"})
TARGET_STATEMENT_TIMEOUT = "120s"

EXIT_CLEAN = 0
EXIT_UNEXPLAINED = 1
EXIT_REFUSED = 2


class Refused(RuntimeError):
    """A guard refused the run before anything was created or read."""


class ReadOnlyRequired(Refused):
    """The target session would accept writes."""


# ---------------------------------------------------------------------------
# Snapshot: catalog queries
# ---------------------------------------------------------------------------

# Copied verbatim from backend/tests/test_schema_from_migrations.py::_snapshot
# (2026-10-18, 164096b33). Keep them identical so the parity test and this report
# see the same schema; the extra queries below are this tool's own.
_SQL_TABLES = (
    "SELECT table_name FROM information_schema.tables "
    "WHERE table_schema='public' AND table_type='BASE TABLE'"
)
_SQL_COLUMNS = (
    "SELECT table_name, column_name, data_type, is_nullable, "
    "COALESCE(column_default, '') FROM information_schema.columns "
    "WHERE table_schema='public'"
)
_SQL_INDEXES = (
    "SELECT tablename, regexp_replace(indexdef, '^CREATE (UNIQUE )?INDEX \\S+ ON ', ''), "
    "indexdef ~ '^CREATE UNIQUE', indexname FROM pg_indexes WHERE schemaname='public'"
)
_SQL_CONSTRAINTS = (
    "SELECT conrelid::regclass::text, contype, pg_get_constraintdef(oid), conname "
    "FROM pg_constraint WHERE connamespace='public'::regnamespace "
    "AND contype IN ('p','u','f','c')"
)
# Extra: not in _snapshot.
_SQL_SEQUENCES = (
    "SELECT c.relname, COALESCE(t.relname || '.' || a.attname, ''), s.seqtypid::regtype::text "
    "FROM pg_class c "
    "JOIN pg_namespace n ON n.oid = c.relnamespace "
    "JOIN pg_sequence s ON s.seqrelid = c.oid "
    "LEFT JOIN pg_depend d ON d.classid = 'pg_class'::regclass AND d.objid = c.oid "
    "  AND d.refclassid = 'pg_class'::regclass AND d.deptype IN ('a', 'i') "
    "LEFT JOIN pg_class t ON t.oid = d.refobjid "
    "LEFT JOIN pg_attribute a ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid "
    "WHERE n.nspname = 'public' AND c.relkind = 'S'"
)
_SQL_FUNCTIONS = (
    "SELECT p.proname || '(' || pg_get_function_identity_arguments(p.oid) || ')', "
    "pg_get_functiondef(p.oid) "
    "FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
    "WHERE n.nspname = 'public' AND p.prokind IN ('f', 'p') "
    "AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid = 'pg_proc'::regclass "
    "  AND d.objid = p.oid AND d.deptype = 'e')"
)
_SQL_TRIGGERS = (
    "SELECT c.relname, t.tgname, pg_get_triggerdef(t.oid) "
    "FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid "
    "JOIN pg_namespace n ON n.oid = c.relnamespace "
    "WHERE n.nspname = 'public' AND NOT t.tgisinternal"
)
_SQL_VIEWS = (
    "SELECT viewname, definition FROM pg_views WHERE schemaname = 'public' "
    "UNION ALL SELECT matviewname, definition FROM pg_matviews WHERE schemaname = 'public'"
)
_SQL_EXTENSIONS = "SELECT extname FROM pg_extension"


def empty_snapshot() -> dict[str, Any]:
    return {
        "tables": set(),
        "columns": {},      # (table, column) -> (data_type, is_nullable, default)
        "indexes": {},      # name -> (table, definition without the name, unique)
        "constraints": {},  # (table, name) -> (contype, definition)
        "sequences": {},    # name -> (owned by "table.column" or "", data type)
        "functions": {},    # "name(args)" -> definition
        "triggers": {},     # (table, name) -> definition
        "views": {},        # name -> definition
        "extensions": set(),
    }


def snapshot(conn: Any) -> dict[str, Any]:
    """Read one database's schema. Issues SELECTs only; ends with a rollback."""
    snap = empty_snapshot()
    cur = conn.cursor()
    try:
        cur.execute(_SQL_TABLES)
        snap["tables"] = {r[0] for r in cur.fetchall()}
        cur.execute(_SQL_COLUMNS)
        snap["columns"] = {(r[0], r[1]): (r[2], r[3], r[4]) for r in cur.fetchall()}
        cur.execute(_SQL_INDEXES)
        snap["indexes"] = {r[3]: (r[0], r[1], bool(r[2])) for r in cur.fetchall()}
        cur.execute(_SQL_CONSTRAINTS)
        snap["constraints"] = {(_bare(r[0]), r[3]): (r[1], r[2]) for r in cur.fetchall()}
        cur.execute(_SQL_SEQUENCES)
        snap["sequences"] = {r[0]: (r[1], r[2]) for r in cur.fetchall()}
        cur.execute(_SQL_FUNCTIONS)
        snap["functions"] = {r[0]: r[1] for r in cur.fetchall()}
        cur.execute(_SQL_TRIGGERS)
        snap["triggers"] = {(r[0], r[1]): r[2] for r in cur.fetchall()}
        cur.execute(_SQL_VIEWS)
        snap["views"] = {r[0]: r[1] for r in cur.fetchall()}
        cur.execute(_SQL_EXTENSIONS)
        snap["extensions"] = {r[0] for r in cur.fetchall()}
    finally:
        try:
            cur.close()
        finally:
            conn.rollback()
    return snap


def _bare(regclass_text: str) -> str:
    """``public.cars`` / ``"Odd"`` -> ``cars`` / ``Odd`` (regclass text depends on search_path)."""
    name = regclass_text.split(".", 1)[1] if regclass_text.startswith("public.") else regclass_text
    return name[1:-1] if len(name) >= 2 and name[0] == name[-1] == '"' else name


def snapshot_to_jsonable(snap: dict[str, Any]) -> dict[str, Any]:
    return {
        "tables": sorted(snap["tables"]),
        "columns": [[t, c, *v] for (t, c), v in sorted(snap["columns"].items())],
        "indexes": [[n, *v] for n, v in sorted(snap["indexes"].items())],
        "constraints": [[t, n, *v] for (t, n), v in sorted(snap["constraints"].items())],
        "sequences": [[n, *v] for n, v in sorted(snap["sequences"].items())],
        "functions": [[n, d] for n, d in sorted(snap["functions"].items())],
        "triggers": [[t, n, d] for (t, n), d in sorted(snap["triggers"].items())],
        "views": [[n, d] for n, d in sorted(snap["views"].items())],
        "extensions": sorted(snap["extensions"]),
    }


def snapshot_from_jsonable(data: dict[str, Any]) -> dict[str, Any]:
    snap = empty_snapshot()
    snap["tables"] = set(data.get("tables", ()))
    snap["columns"] = {(r[0], r[1]): tuple(r[2:5]) for r in data.get("columns", ())}
    snap["indexes"] = {r[0]: (r[1], r[2], bool(r[3])) for r in data.get("indexes", ())}
    snap["constraints"] = {(r[0], r[1]): (r[2], r[3]) for r in data.get("constraints", ())}
    snap["sequences"] = {r[0]: (r[1], r[2]) for r in data.get("sequences", ())}
    snap["functions"] = {r[0]: r[1] for r in data.get("functions", ())}
    snap["triggers"] = {(r[0], r[1]): r[2] for r in data.get("triggers", ())}
    snap["views"] = {r[0]: r[1] for r in data.get("views", ())}
    snap["extensions"] = set(data.get("extensions", ()))
    return snap


# ---------------------------------------------------------------------------
# Diff: pure, over two snapshot dicts
# ---------------------------------------------------------------------------

LIVE_ONLY = "live_only"
CHAIN_ONLY = "chain_only"
MISMATCH = "mismatch"
_KIND_ORDER = {LIVE_ONLY: 0, CHAIN_ONLY: 1, MISMATCH: 2}
_CATEGORY_ORDER = {
    c: i for i, c in enumerate(
        ("migration", "extension", "table", "column", "index", "constraint",
         "sequence", "function", "trigger", "view")
    )
}


@dataclass(frozen=True)
class Drift:
    kind: str          # live_only | chain_only | mismatch
    category: str      # table | column | index | constraint | sequence | function | trigger | view | extension | migration
    name: str          # table, "table.column", index, constraint, sequence, "fn(args)", trigger, view, extension, "V026"
    table: str = ""    # owning table, when there is one
    detail: str = ""
    chain: Any = None  # the chain's definition (mismatch / chain_only)
    live: Any = None   # the target's definition (mismatch / live_only)
    children: tuple[str, ...] = ()  # objects folded under a one-sided table

    @property
    def key(self) -> tuple:
        return (_KIND_ORDER.get(self.kind, 9), _CATEGORY_ORDER.get(self.category, 99),
                self.table, self.name)


def _owner_table(owned_by: str) -> str:
    return owned_by.split(".", 1)[0] if owned_by else ""


def _fmt(value: Any) -> str:
    if isinstance(value, tuple):
        return "(" + ", ".join(repr(v) if v == "" else str(v) for v in value) + ")"
    return str(value)


def diff_snapshots(chain: dict[str, Any], live: dict[str, Any]) -> list[Drift]:
    """Every difference between the chain-built snapshot and the live one.

    A table that exists on one side only is ONE item; its columns, indexes,
    constraints, owned sequences and triggers are folded into ``children``
    instead of being listed again. Indexes that back a PRIMARY KEY / UNIQUE
    constraint are compared as constraints, not twice. A one-sided index or
    constraint whose definition matches a one-sided one on the other side is
    reported as a single name mismatch.
    """
    out: list[Drift] = []
    live_only_t = live["tables"] - chain["tables"]
    chain_only_t = chain["tables"] - live["tables"]
    folded: dict[tuple[str, str], list[str]] = {}

    def fold(side: str, table: str, label: str) -> bool:
        one_sided = live_only_t if side == LIVE_ONLY else chain_only_t
        if table in one_sided:
            folded.setdefault((side, table), []).append(label)
            return True
        return False

    # columns
    for key in sorted(set(live["columns"]) | set(chain["columns"])):
        table, col = key
        lv, cv = live["columns"].get(key), chain["columns"].get(key)
        if lv is not None and cv is None:
            if not fold(LIVE_ONLY, table, f"column {col}"):
                out.append(Drift(LIVE_ONLY, "column", f"{table}.{col}", table, _fmt(lv), live=lv))
        elif cv is not None and lv is None:
            if not fold(CHAIN_ONLY, table, f"column {col}"):
                out.append(Drift(CHAIN_ONLY, "column", f"{table}.{col}", table, _fmt(cv), chain=cv))
        elif lv != cv:
            out.append(Drift(MISMATCH, "column", f"{table}.{col}", table,
                             f"chain={_fmt(cv)} live={_fmt(lv)}", chain=cv, live=lv))

    # constraints, keyed (table, name); then pair renames by (table, type, def)
    out += _diff_named(
        "constraint", chain["constraints"], live["constraints"],
        table_of=lambda k, v: k[0], name_of=lambda k: k[1],
        same_def=lambda k, v: (k[0], v[0], v[1]), fold=fold,
    )

    # indexes, keyed by name, minus the ones that back a p/u constraint on their side
    def standalone(snap: dict[str, Any]) -> dict[str, Any]:
        backed = {(t, n) for (t, n), (ct, _d) in snap["constraints"].items() if ct in ("p", "u")}
        return {n: v for n, v in snap["indexes"].items() if (v[0], n) not in backed}

    out += _diff_named(
        "index", standalone(chain), standalone(live),
        table_of=lambda k, v: v[0], name_of=lambda k: k,
        same_def=lambda k, v: v, fold=fold,
    )

    # sequences (fold the ones owned by a one-sided table)
    for name in sorted(set(live["sequences"]) | set(chain["sequences"])):
        lv, cv = live["sequences"].get(name), chain["sequences"].get(name)
        if lv is not None and cv is None:
            owner = _owner_table(lv[0])
            if not fold(LIVE_ONLY, owner, f"sequence {name}"):
                out.append(Drift(LIVE_ONLY, "sequence", name, owner, _fmt(lv), live=lv))
        elif cv is not None and lv is None:
            owner = _owner_table(cv[0])
            if not fold(CHAIN_ONLY, owner, f"sequence {name}"):
                out.append(Drift(CHAIN_ONLY, "sequence", name, owner, _fmt(cv), chain=cv))
        elif lv != cv:
            out.append(Drift(MISMATCH, "sequence", name, _owner_table(lv[0]),
                             f"chain={_fmt(cv)} live={_fmt(lv)}", chain=cv, live=lv))

    # triggers (fold the ones on a one-sided table)
    for key in sorted(set(live["triggers"]) | set(chain["triggers"])):
        table, name = key
        lv, cv = live["triggers"].get(key), chain["triggers"].get(key)
        if lv is not None and cv is None:
            if not fold(LIVE_ONLY, table, f"trigger {name}"):
                out.append(Drift(LIVE_ONLY, "trigger", name, table, lv, live=lv))
        elif cv is not None and lv is None:
            if not fold(CHAIN_ONLY, table, f"trigger {name}"):
                out.append(Drift(CHAIN_ONLY, "trigger", name, table, cv, chain=cv))
        elif lv != cv:
            out.append(Drift(MISMATCH, "trigger", name, table, "definition differs", chain=cv, live=lv))

    for category in ("functions", "views"):
        cat = category[:-1]
        for name in sorted(set(live[category]) | set(chain[category])):
            lv, cv = live[category].get(name), chain[category].get(name)
            if lv is not None and cv is None:
                out.append(Drift(LIVE_ONLY, cat, name, live=lv))
            elif cv is not None and lv is None:
                out.append(Drift(CHAIN_ONLY, cat, name, chain=cv))
            elif lv != cv:
                out.append(Drift(MISMATCH, cat, name, detail="definition differs", chain=cv, live=lv))

    for name in sorted(live["extensions"] - chain["extensions"]):
        out.append(Drift(LIVE_ONLY, "extension", name))
    for name in sorted(chain["extensions"] - live["extensions"]):
        out.append(Drift(CHAIN_ONLY, "extension", name))

    for table in sorted(live_only_t):
        kids = tuple(sorted(folded.get((LIVE_ONLY, table), ())))
        out.append(Drift(LIVE_ONLY, "table", table, table, f"{len(kids)} folded object(s)", children=kids))
    for table in sorted(chain_only_t):
        kids = tuple(sorted(folded.get((CHAIN_ONLY, table), ())))
        out.append(Drift(CHAIN_ONLY, "table", table, table, f"{len(kids)} folded object(s)", children=kids))
    return sorted(out, key=lambda d: d.key)


def _diff_named(
    category: str,
    chain: dict[Any, Any],
    live: dict[Any, Any],
    *,
    table_of: Callable[[Any, Any], str],
    name_of: Callable[[Any], str],
    same_def: Callable[[Any, Any], Any],
    fold: Callable[[str, str, str], bool],
) -> list[Drift]:
    out: list[Drift] = []
    live_only: dict[Any, Any] = {}
    chain_only: dict[Any, Any] = {}
    for key in set(live) | set(chain):
        lv, cv = live.get(key), chain.get(key)
        if lv is not None and cv is None:
            if not fold(LIVE_ONLY, table_of(key, lv), f"{category} {name_of(key)}"):
                live_only[key] = lv
        elif cv is not None and lv is None:
            if not fold(CHAIN_ONLY, table_of(key, cv), f"{category} {name_of(key)}"):
                chain_only[key] = cv
        elif lv != cv:
            out.append(Drift(MISMATCH, category, name_of(key), table_of(key, lv),
                             f"chain={_fmt(cv)} live={_fmt(lv)}", chain=cv, live=lv))
    # same definition under another name: one item, not two
    by_def: dict[Any, list[Any]] = {}
    for key, cv in chain_only.items():
        by_def.setdefault(same_def(key, cv), []).append(key)
    for key in sorted(live_only, key=str):
        lv = live_only[key]
        twins = by_def.get(same_def(key, lv))
        if twins:
            ckey = twins.pop(0)
            cv = chain_only.pop(ckey)
            del live_only[key]
            out.append(Drift(MISMATCH, category, name_of(key), table_of(key, lv),
                             f"same definition, name differs: chain={name_of(ckey)} live={name_of(key)}",
                             chain=(name_of(ckey), *cv), live=(name_of(key), *lv)))
    for key, lv in live_only.items():
        out.append(Drift(LIVE_ONLY, category, name_of(key), table_of(key, lv), _fmt(lv), live=lv))
    for key, cv in chain_only.items():
        out.append(Drift(CHAIN_ONLY, category, name_of(key), table_of(key, cv), _fmt(cv), chain=cv))
    return out


def diff_migrations(chain_files: dict[int, str], recorded: dict[int, str]) -> list[Drift]:
    """The target's ``schema_migrations`` against the files the chain was built from.

    ``chain_files`` / ``recorded`` map version -> checksum. A version recorded
    with no file means this checkout is older than the target: its live-only
    objects cannot be explained from here.
    """
    out: list[Drift] = []
    for v in sorted(set(chain_files) - set(recorded)):
        out.append(Drift(CHAIN_ONLY, "migration", f"V{v:03d}", detail="not recorded on the target (pending)"))
    for v in sorted(set(recorded) - set(chain_files)):
        out.append(Drift(LIVE_ONLY, "migration", f"V{v:03d}",
                         detail="recorded on the target, no file in this checkout (run from a newer checkout)"))
    for v in sorted(set(recorded) & set(chain_files)):
        if recorded[v] != chain_files[v]:
            out.append(Drift(MISMATCH, "migration", f"V{v:03d}", detail="checksum drift (an applied file was edited)",
                             chain=chain_files[v], live=recorded[v]))
    return out


# ---------------------------------------------------------------------------
# Explanations
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Known:
    """One expected difference and why. ``name`` / ``targets`` are fnmatch patterns."""

    kind: str
    category: str
    name: str
    reason: str
    targets: tuple[str, ...] = ()  # target database names; () = every target

    def matches(self, item: Drift, target_db: str) -> bool:
        if item.kind != self.kind or item.category != self.category:
            return False
        if not fnmatch.fnmatchcase(item.name, self.name):
            return False
        return not self.targets or any(fnmatch.fnmatchcase(target_db, t) for t in self.targets)


_LEDGER = "docs/db_layer/SCHEMA_LEDGER_2026_10.md"
_V019_ONLY = (
    "V019-only object: a fresh chain build records V019 without running it, so the chain "
    f"does not create it; present on prod and local ({_LEDGER}, 'Objects the chain names only "
    "in V019'). V026 adopts it (P4.3)."
)
_LOG_TABLE = (
    f"untracked log table, no creator in git history ({_LEDGER}, out-of-chain objects); "
    "D-N30: adopted in V026 (P4.3), exported and dropped later in P13B.4."
)
_CLEANROOM_EXCLUDED = (
    "V001 backup table left out of the scratch build on purpose: workspace/cleanroom_2026_10/"
    "build_db.sh and scripts/cleanroom/build_db.sh (dump mode) run pg_dump -T 'public.cars_backup_*' "
    "-T 'public.*_bak_*'. cars_cleanroom is frozen (D-N1)."
)
_FROZEN_TARGETS = ("cars_cleanroom",)

KNOWN_DRIFT: tuple[Known, ...] = (
    Known(LIVE_ONLY, "table", "car_move_log", _V019_ONLY),
    Known(LIVE_ONLY, "table", "cars_trim_quarantine", _V019_ONLY),
    Known(LIVE_ONLY, "table", "package_values_msrp_quarantine", _V019_ONLY),
    Known(LIVE_ONLY, "index", "uq_option_rejections_standing",
          "out-of-chain UNIQUE index from runtime DDL (backend/scripts/feed_package_registry_from_vision.py); "
          f"D-DB7 (a): adopted in V026 (P4.3); 0 duplicate groups on prod and local ({_LEDGER})."),
    Known(LIVE_ONLY, "table", "cars_epa_link_backup_20260921",
          "one-off backup table, no creator in git history; EPA recovery source until P10D, "
          f"then dropped with D-DB1 (P13B.4) ({_LEDGER})."),
    Known(LIVE_ONLY, "table", "cars_owner_backup_20260929",
          "prod-only backup from the 2026-09-29 owner-guard repair; added to the D-DB1 drop list "
          f"(P13B.4) ({_LEDGER})."),
    Known(LIVE_ONLY, "table", "image_summary_restore_log", _LOG_TABLE),
    Known(LIVE_ONLY, "table", "make_canon_log", _LOG_TABLE),
    Known(LIVE_ONLY, "table", "model_canon_log", _LOG_TABLE),
    Known(LIVE_ONLY, "table", "package_value_purge_log", _LOG_TABLE),
    Known(CHAIN_ONLY, "table", "cars_backup_*", _CLEANROOM_EXCLUDED, ("cars_cleanroom", "cars_cr_*")),
    Known(CHAIN_ONLY, "table", "*_bak_*", _CLEANROOM_EXCLUDED, ("cars_cleanroom", "cars_cr_*")),
)


_IDENT = r'(?:"[^"]+"|[A-Za-z_][A-Za-z0-9_$]*)'
_QUAL = rf"(?:{_IDENT}\.)?({_IDENT})"
_OBJECT_RES: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("table", re.compile(rf"CREATE\s+(?:UNLOGGED\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?{_QUAL}", re.I)),
    ("index", re.compile(
        rf"CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:CONCURRENTLY\s+)?(?:IF\s+NOT\s+EXISTS\s+)?({_IDENT})\s+ON\b", re.I)),
    ("sequence", re.compile(rf"CREATE\s+SEQUENCE\s+(?:IF\s+NOT\s+EXISTS\s+)?{_QUAL}", re.I)),
    ("constraint", re.compile(rf"\bCONSTRAINT\s+({_IDENT})", re.I)),
    ("view", re.compile(rf"CREATE\s+(?:OR\s+REPLACE\s+)?(?:MATERIALIZED\s+)?VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?{_QUAL}",
                        re.I)),
    ("trigger", re.compile(rf"CREATE\s+(?:OR\s+REPLACE\s+)?TRIGGER\s+({_IDENT})", re.I)),
    ("extension", re.compile(rf"CREATE\s+EXTENSION\s+(?:IF\s+NOT\s+EXISTS\s+)?({_IDENT})", re.I)),
)
_ALTER_ADD_COL_RE = re.compile(
    rf"ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:ONLY\s+)?{_QUAL}\s+ADD\s+(?:COLUMN\s+)?(?:IF\s+NOT\s+EXISTS\s+)?({_IDENT})",
    re.I,
)
_NOT_COLUMNS = {"constraint", "primary", "unique", "foreign", "check", "exclude"}


def _unq(name: str) -> str:
    return name[1:-1] if name.startswith('"') else name.lower()


def migration_objects(directory: Path) -> dict[tuple[str, str], int]:
    """(category, name) -> the lowest version whose file creates it (static parse).

    Used only to explain chain-only objects of a migration the target has not
    applied. Columns are matched from ``ALTER TABLE ... ADD COLUMN``; columns of
    a new table are folded under that table by the diff anyway.
    """
    found: dict[tuple[str, str], int] = {}
    for path in sorted(directory.glob("V*__*.sql")):
        m = re.match(r"^V(\d+)__", path.name)
        if not m:
            continue
        version = int(m.group(1))
        sql = re.sub(r"/\*.*?\*/", " ", path.read_text(encoding="utf-8"), flags=re.S)
        sql = re.sub(r"--[^\n]*", " ", sql)
        for category, rx in _OBJECT_RES:
            for hit in rx.finditer(sql):
                found.setdefault((category, _unq(hit.group(1))), version)
        for hit in _ALTER_ADD_COL_RE.finditer(sql):
            col = _unq(hit.group(2))
            if col not in _NOT_COLUMNS:
                found.setdefault(("column", f"{_unq(hit.group(1))}.{col}"), version)
    return found


@dataclass
class Classified:
    item: Drift
    explanation: str | None = None

    @property
    def explained(self) -> bool:
        return self.explanation is not None


def classify(
    items: Iterable[Drift],
    target_db: str,
    *,
    pending: Iterable[int] = (),
    objects: dict[tuple[str, str], int] | None = None,
    known: Iterable[Known] = KNOWN_DRIFT,
) -> tuple[list[Classified], list[Known]]:
    """Attach an explanation to every item that has one. Pure.

    Returns the classified items and the :class:`Known` entries that matched
    nothing on this target (stale: prune them once the object is gone or in the chain).
    """
    known = tuple(known)
    pending = set(pending)
    objects = objects or {}
    frozen = any(fnmatch.fnmatchcase(target_db, t) for t in _FROZEN_TARGETS)
    used: set[int] = set()
    out: list[Classified] = []
    for item in items:
        why = None
        for i, k in enumerate(known):
            if k.matches(item, target_db):
                why = k.reason
                used.add(i)
                break
        if why is None and pending and item.kind == CHAIN_ONLY:
            if item.category == "migration":
                version = int(item.name[1:])
            else:
                version = objects.get((item.category, item.name))
                if version is None and item.table:
                    version = objects.get(("table", item.table))
            if version in pending:
                why = (f"created by V{version:03d}, which this database has not applied"
                       + (" (frozen at V025 by D-N1: it never receives a migration)" if frozen
                          else " (pending: migrate --dry-run, then --apply per D-DB6)"))
        out.append(Classified(item, why))
    stale = [k for i, k in enumerate(known) if i not in used and (
        not k.targets or any(fnmatch.fnmatchcase(target_db, t) for t in k.targets))]
    return out, stale


# ---------------------------------------------------------------------------
# Guards and connections
# ---------------------------------------------------------------------------


def _conninfo(dsn: str) -> dict[str, str]:
    from psycopg.conninfo import conninfo_to_dict

    return {k: str(v) for k, v in conninfo_to_dict(dsn).items() if v is not None}


def with_dbname(dsn: str, dbname: str) -> str:
    """Same server and credentials, another database, always as a libpq URL.

    URL form because ``inventory_pg.inventory_postgres_dsn()`` (what migrate.py
    connects through) accepts only ``postgresql://`` strings. Every parameter,
    host included, goes in the percent-encoded query string.
    """
    from urllib.parse import quote, urlencode

    params = {k: v for k, v in sorted(_conninfo(dsn).items()) if k != "dbname"}
    query = urlencode(params, quote_via=quote, safe="/")
    return f"postgresql:///{quote(dbname, safe='')}" + (f"?{query}" if query else "")


def describe_dsn(dsn: str) -> str:
    """host/port/db for the report; never the password, and non-local hosts redacted."""
    try:
        info = _conninfo(dsn)
    except Exception:
        return "<unparseable dsn>"
    host = info.get("host", "")
    shown = host if host in _LOCAL_HOSTS or host.startswith("/") else "<redacted-host>"
    return f"host={shown or 'default-socket'} port={info.get('port', '5432')} db={info.get('dbname', '?')}"


def check_scratch_server(admin_dsn: str) -> None:
    """D-TC9: scratch databases live only on a local server, never via the 15432 tunnel."""
    info = _conninfo(admin_dsn)
    # libpq falls back to PGHOST / PGHOSTADDR / PGPORT when the DSN omits them.
    host_spec = info.get("host") or os.environ.get("PGHOST", "")
    for host in host_spec.split(","):
        if not (host in _LOCAL_HOSTS or host.startswith("/")):
            raise Refused(f"scratch server must be local (unix socket or loopback); got host {host!r}")
    addr = info.get("hostaddr") or os.environ.get("PGHOSTADDR", "")
    for a in addr.split(",") if addr else ():
        if a not in ("127.0.0.1", "::1"):
            raise Refused(f"scratch server must be local; got hostaddr {a!r}")
    port_spec = info.get("port") or os.environ.get("PGPORT", "")
    if TUNNEL_PORT in port_spec.split(","):
        raise Refused(f"scratch server port {TUNNEL_PORT} is the mini's tunnel to the inventory server; refused")


def check_scratch_name(name: str, target_db: str) -> None:
    if not _SCRATCH_NAME_RE.match(name):
        raise Refused(f"scratch name {name!r} must match {_SCRATCH_NAME_RE.pattern} (D-TC9: pytest_* only)")
    if name in PROTECTED_DBS:
        raise Refused(f"scratch name {name!r} is a protected database")
    if name == target_db:
        raise Refused(f"scratch name {name!r} equals the target database; refused")


def new_scratch_name() -> str:
    return f"{SCRATCH_PREFIX}{uuid.uuid4().hex[:12]}"


def open_target_read_only(dsn: str, connect_fn: Callable[..., Any] | None = None) -> Any:
    """Open the target through ``connect(read_only=True)`` and prove the server agrees."""
    if connect_fn is None:
        from backend.db import connect as _connect_mod

        connect_fn = _connect_mod.connect
    conn = connect_fn(read_only=True, dsn=dsn)
    try:
        cur = conn.cursor()
        try:
            cur.execute("SHOW default_transaction_read_only")
            dflt = str((cur.fetchone() or ("",))[0]).lower()
            cur.execute("SHOW transaction_read_only")
            now = str((cur.fetchone() or ("",))[0]).lower()
            if dflt != "on" or now != "on":
                raise ReadOnlyRequired(
                    f"target session is not read-only (default_transaction_read_only={dflt or '?'}, "
                    f"transaction_read_only={now or '?'}); refusing to read it"
                )
            cur.execute(f"SET statement_timeout = '{TARGET_STATEMENT_TIMEOUT}'")
        finally:
            cur.close()
        # A plain SET is undone by a rollback; end this read-only transaction (it ran
        # two SHOWs and a SET, nothing else) so the timeout holds for the session.
        conn.commit()
    except BaseException:
        try:
            conn.rollback()
        finally:
            conn.close()
        raise
    return conn


def read_target_facts(conn: Any) -> dict[str, Any]:
    """Server version, current database, recorded migrations and write counters. SELECT/SHOW only."""
    cur = conn.cursor()
    try:
        cur.execute("SHOW server_version")
        version = str(cur.fetchone()[0])
        cur.execute("SELECT current_database()")
        dbname = str(cur.fetchone()[0])
        cur.execute("SELECT to_regclass('public.schema_migrations')")
        row = cur.fetchone()
        recorded: dict[int, str] = {}
        if row and row[0]:
            cur.execute("SELECT version, checksum FROM public.schema_migrations")
            recorded = {int(v): str(c) for v, c in cur.fetchall()}
        counters = write_counters(cur)
    finally:
        try:
            cur.close()
        finally:
            conn.rollback()
    return {"server_version": version, "dbname": dbname, "recorded": recorded, "counters_before": counters}


def write_counters(cur: Any) -> list[int] | None:
    cur.execute(
        "SELECT tup_inserted, tup_updated, tup_deleted FROM pg_stat_database "
        "WHERE datname = current_database()"
    )
    row = cur.fetchone()
    return [int(x) for x in row] if row else None


def read_write_counters(conn: Any) -> list[int] | None:
    cur = conn.cursor()
    try:
        return write_counters(cur)
    finally:
        try:
            cur.close()
        finally:
            conn.rollback()


# ---------------------------------------------------------------------------
# Chain build
# ---------------------------------------------------------------------------


def runner_records_v019(migrate_mod: Any) -> bool:
    """True once migrate.py carries P4.2's conditional record-only rule for V019."""
    rule = getattr(migrate_mod, "RECORD_ONLY", None)
    try:
        return 19 in rule  # type: ignore[operator]
    except TypeError:
        return False


def build_chain(scratch_dsn: str, directory: Path, builder: str = "auto") -> tuple[str, dict[int, str]]:
    """Build the chain into the (empty) scratch DB. Returns (builder used, version -> checksum)."""
    from backend.scripts import migrate

    files = migrate.discover_migrations(directory)
    if not files:
        raise Refused(f"no migration files in {directory}")
    use_runner = builder == "runner" or (builder == "auto" and runner_records_v019(migrate))
    if use_runner:
        _build_with_runner(scratch_dsn, directory, migrate)
        used = "migrate.main(['--apply'])"
    else:
        _build_record_v019(scratch_dsn, files, migrate)
        used = "own loop: V019 recorded without running (fresh build, pre-P4.2 rule)"
    import psycopg

    with psycopg.connect(scratch_dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT version, checksum FROM public.schema_migrations")
        recorded = {int(v): str(c) for v, c in cur.fetchall()}
    expected = {m.version: m.checksum for m in files}
    if recorded != expected:
        raise RuntimeError(
            f"chain build incomplete: recorded {sorted(recorded)} vs files {sorted(expected)}"
        )
    return used, expected


def _build_record_v019(scratch_dsn: str, files: list[Any], migrate: Any) -> None:
    import psycopg

    conn = psycopg.connect(scratch_dsn, autocommit=False)
    try:
        cur = conn.cursor()
        cur.execute(migrate._CREATE_TABLE_SQL)
        conn.commit()
        cur.close()
        fresh = 1 in {m.version for m in files}  # V001 pending in this same run
        for mig in files:
            if mig.version == 19 and fresh:
                cur = conn.cursor()
                migrate._record(cur, mig)
                conn.commit()
                cur.close()
            else:
                migrate.apply_migration(conn, mig)
    finally:
        conn.close()


def _build_with_runner(scratch_dsn: str, directory: Path, migrate: Any) -> None:
    """``migrate.main(['--apply'])`` with the inventory DSN pinned to the scratch DB.

    migrate.py finds its database through the environment, so the scratch DSN is
    put in ``INVENTORY_DATABASE_URL`` for the call only, and the call is refused
    unless the repo's own resolver returns exactly the scratch DSN.
    """
    from backend.db import connect as connect_mod
    from backend.db import inventory_pg

    keys = ("INVENTORY_DATABASE_URL", "DATABASE_URL")
    saved = {k: os.environ.get(k) for k in keys}
    try:
        os.environ["INVENTORY_DATABASE_URL"] = scratch_dsn
        os.environ.pop("DATABASE_URL", None)
        # Both repo resolvers (migrate.py uses the first today; P4.2 may move to the second).
        resolved = {inventory_pg.inventory_postgres_dsn(), connect_mod.inventory_dsn(require=False)}
        if resolved != {scratch_dsn}:
            raise Refused("inventory DSN did not resolve to the scratch database; refusing migrate --apply")
        rc = migrate.main(["--apply", "--dir", str(directory)])
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
    if rc != 0:
        raise RuntimeError(f"migrate --apply on the scratch database exited {rc}")


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


@dataclass
class Report:
    generated_at: str
    target: str
    target_db: str
    server_version: str
    scratch: str
    scratch_server: str
    builder: str
    chain_versions: list[int]
    recorded_versions: list[int]
    items: list[Classified] = field(default_factory=list)
    stale_known: list[Known] = field(default_factory=list)
    counters_before: list[int] | None = None
    counters_after: list[int] | None = None
    timings: dict[str, float] = field(default_factory=dict)

    @property
    def unexplained(self) -> list[Classified]:
        return [c for c in self.items if not c.explained]

    def to_jsonable(self) -> dict[str, Any]:
        def item(c: Classified) -> dict[str, Any]:
            d = asdict(c.item)
            d["children"] = list(c.item.children)
            d["explanation"] = c.explanation
            return d

        return {
            "generated_at": self.generated_at,
            "target": self.target,
            "target_db": self.target_db,
            "server_version": self.server_version,
            "scratch": self.scratch,
            "scratch_server": self.scratch_server,
            "builder": self.builder,
            "chain_versions": self.chain_versions,
            "recorded_versions": self.recorded_versions,
            "counters_before": self.counters_before,
            "counters_after": self.counters_after,
            "timings": self.timings,
            "summary": {
                "items": len(self.items),
                "explained": len(self.items) - len(self.unexplained),
                "unexplained": len(self.unexplained),
                LIVE_ONLY: sum(c.item.kind == LIVE_ONLY for c in self.items),
                CHAIN_ONLY: sum(c.item.kind == CHAIN_ONLY for c in self.items),
                MISMATCH: sum(c.item.kind == MISMATCH for c in self.items),
            },
            "items": [item(c) for c in self.items],
            "stale_known": [asdict(k) for k in self.stale_known],
        }


def _range(versions: list[int]) -> str:
    if not versions:
        return "none"
    return f"V{min(versions):03d}..V{max(versions):03d} ({len(versions)})"


def render_text(rep: Report) -> str:
    lines = [
        f"schema drift report  {rep.generated_at}",
        f"  chain : migrations/ {_range(rep.chain_versions)}, built into {rep.scratch} "
        f"on {rep.scratch_server} by {rep.builder}; dropped afterwards",
        f"  target: {rep.target} (PostgreSQL {rep.server_version}), read-only session verified",
        f"  target schema_migrations: {_range(rep.recorded_versions)}",
    ]
    if rep.counters_before is not None:
        delta = (None if rep.counters_after is None
                 else [a - b for a, b in zip(rep.counters_after, rep.counters_before)])
        lines.append(
            f"  target write counters (ins/upd/del, all sessions): before={rep.counters_before} "
            f"after={rep.counters_after} delta={delta}"
        )
    titles = {
        LIVE_ONLY: "LIVE-ONLY (the target has it, the chain does not)",
        CHAIN_ONLY: "CHAIN-ONLY (the chain creates it, the target lacks it)",
        MISMATCH: "DEFINITION MISMATCHES (same name, different definition)",
    }
    for kind in (LIVE_ONLY, CHAIN_ONLY, MISMATCH):
        group = [c for c in rep.items if c.item.kind == kind]
        lines.append("")
        lines.append(f"{titles[kind]}: {len(group)}")
        for c in group:
            it = c.item
            where = f" on {it.table}" if it.table and it.category not in ("table", "column") else ""
            tag = "explained" if c.explained else "UNEXPLAINED"
            lines.append(f"  [{tag}] {it.category} {it.name}{where}: {it.detail}".rstrip(": "))
            if it.children:
                lines.append(f"      folded: {', '.join(it.children)}")
            if c.explained:
                lines.append(f"      why: {c.explanation}")
    lines.append("")
    n_unexp = len(rep.unexplained)
    lines.append(f"UNEXPLAINED: {n_unexp}   explained: {len(rep.items) - n_unexp}   total: {len(rep.items)}")
    if rep.stale_known:
        lines.append("Known-drift entries that matched nothing on this target (prune once confirmed):")
        for k in rep.stale_known:
            lines.append(f"  {k.kind} {k.category} {k.name}")
    return "\n".join(lines)


def run_report(
    *,
    target_dsn: str,
    admin_dsn: str = DEFAULT_SCRATCH_ADMIN_DSN,
    scratch_name: str | None = None,
    directory: Path = MIGRATIONS_DIR,
    builder: str = "auto",
    connect_fn: Callable[..., Any] | None = None,
    admin_connect: Callable[[str], Any] | None = None,
    scratch_connect: Callable[[str], Any] | None = None,
    build_fn: Callable[[str, Path, str], tuple[str, dict[int, str]]] | None = None,
    snapshot_fn: Callable[[Any], dict[str, Any]] = snapshot,
    snapshot_dir: Path | None = None,
) -> Report:
    """Guard, build, snapshot both, diff, classify, drop. Raises :class:`Refused` before any write."""
    check_scratch_server(admin_dsn)
    target_db = _conninfo(target_dsn).get("dbname", "")
    name = scratch_name or new_scratch_name()
    check_scratch_name(name, target_db)
    scratch_dsn = with_dbname(admin_dsn, name)
    if admin_connect is None or scratch_connect is None:
        import psycopg

        admin_connect = admin_connect or (lambda dsn: psycopg.connect(dsn, autocommit=True))
        scratch_connect = scratch_connect or psycopg.connect

    build_fn = build_fn or build_chain
    timings: dict[str, float] = {}

    # Read-only target first: refuse before creating anything.
    t0 = time.monotonic()
    tconn = open_target_read_only(target_dsn, connect_fn)
    try:
        facts = read_target_facts(tconn)
        if facts["dbname"] == name:
            raise Refused(f"target database {facts['dbname']!r} equals the scratch name; refused")
        target_snap = snapshot_fn(tconn)
        timings["target_snapshot_s"] = round(time.monotonic() - t0, 2)

        admin = admin_connect(admin_dsn)
        created = False
        try:
            cur = admin.cursor()
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,))
            if cur.fetchone():
                raise Refused(f"scratch database {name!r} already exists; refusing to reuse or drop it")
            cur.execute(f'CREATE DATABASE "{name}" TEMPLATE template0')
            created = True
            cur.close()
            t1 = time.monotonic()
            used, chain_files = build_fn(scratch_dsn, directory, builder)
            timings["chain_build_s"] = round(time.monotonic() - t1, 2)
            sconn = scratch_connect(scratch_dsn)
            try:
                chain_snap = snapshot_fn(sconn)
            finally:
                sconn.close()
        finally:
            if created:
                try:
                    cur = admin.cursor()
                    cur.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
                    cur.close()
                except Exception as exc:  # pragma: no cover - reported, never hidden
                    log.error("could not drop scratch database %s: %s (drop it by hand)", name, exc)
            admin.close()

        counters_after = read_write_counters(tconn)
    finally:
        try:
            tconn.rollback()
        finally:
            tconn.close()

    if snapshot_dir is not None:
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        (snapshot_dir / "chain_snapshot.json").write_text(
            json.dumps(snapshot_to_jsonable(chain_snap), indent=1), encoding="utf-8")
        (snapshot_dir / f"target_{facts['dbname']}_snapshot.json").write_text(
            json.dumps(snapshot_to_jsonable(target_snap), indent=1), encoding="utf-8")

    recorded = facts["recorded"]
    items = diff_migrations(chain_files, recorded) + diff_snapshots(chain_snap, target_snap)
    # Only a DB with a ledger has "pending" migrations; for one without
    # schema_migrations every chain object would read as pending and hide drift.
    pending = sorted(set(chain_files) - set(recorded)) if recorded else []
    classified, stale = classify(items, facts["dbname"], pending=pending, objects=migration_objects(directory))
    return Report(
        generated_at=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        target=describe_dsn(target_dsn),
        target_db=facts["dbname"],
        server_version=facts["server_version"],
        scratch=name,
        scratch_server=describe_dsn(admin_dsn),
        builder=used,
        chain_versions=sorted(chain_files),
        recorded_versions=sorted(recorded),
        items=classified,
        stale_known=stale,
        counters_before=facts["counters_before"],
        counters_after=counters_after,
        timings=timings,
    )


def _resolve_target(args: argparse.Namespace) -> str:
    if args.target_db:
        if args.target_db.startswith(SCRATCH_PREFIX):
            raise Refused("--target-db names a scratch database")
        return with_dbname(args.scratch_admin_dsn, args.target_db)
    from backend.db.connect import inventory_dsn

    return str(inventory_dsn(require=True))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Schema drift report: chain-built scratch DB vs a live DB (read-only).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--target-db", metavar="NAME",
                        help="a database on the scratch server (e.g. cars, cars_cleanroom). "
                             "Default: the inventory DSN from the environment / .env")
    parser.add_argument("--scratch-admin-dsn", default=os.environ.get("SCHEMA_DRIFT_SCRATCH_DSN")
                        or DEFAULT_SCRATCH_ADMIN_DSN,
                        help="LOCAL server where the pytest_chain_* DB is built and dropped "
                             f"(default {DEFAULT_SCRATCH_ADMIN_DSN})")
    parser.add_argument("--scratch-name", help="override the generated pytest_chain_<hex> name")
    parser.add_argument("--dir", default=str(MIGRATIONS_DIR), help="migrations directory")
    parser.add_argument("--builder", choices=("auto", "own", "runner"), default="auto",
                        help="auto: migrate.main once P4.2's RECORD_ONLY exists, else own loop")
    parser.add_argument("--json-out", metavar="PATH", help="also write the full report as JSON")
    parser.add_argument("--save-snapshots", metavar="DIR", help="write both snapshots as JSON")
    parser.add_argument("-q", "--quiet", action="store_true", help="only the summary line")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                        datefmt="%H:%M:%S")

    try:
        target_dsn = _resolve_target(args)
        rep = run_report(
            target_dsn=target_dsn,
            admin_dsn=args.scratch_admin_dsn,
            scratch_name=args.scratch_name,
            directory=Path(args.dir),
            builder=args.builder,
            snapshot_dir=Path(args.save_snapshots) if args.save_snapshots else None,
        )
    except Refused as exc:
        print(f"REFUSED: {exc}", file=sys.stderr)
        return EXIT_REFUSED
    except Exception as exc:
        print(f"FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
        return EXIT_REFUSED

    text = render_text(rep)
    if args.quiet:
        n = len(rep.unexplained)
        print(f"{rep.target}: UNEXPLAINED {n} / {len(rep.items)} difference(s)")
    else:
        print(text)
    if args.json_out:
        Path(args.json_out).write_text(json.dumps(rep.to_jsonable(), indent=1, default=str), encoding="utf-8")
    return EXIT_UNEXPLAINED if rep.unexplained else EXIT_CLEAN


if __name__ == "__main__":
    raise SystemExit(main())
