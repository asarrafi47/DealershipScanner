#!/usr/bin/env python3
"""
Capture the schema that module-level runtime DDL builds on Postgres (P4.3).

``backend/tests/fixtures/runtime_ddl_schema_golden.json`` (2026-10-01) covers the
runtime DDL in ``backend/db``. Outside it, modules and scripts still create tables
on first use: haiku_spec_cache existed only that way, and the
V019-only tables and ``uq_option_rejections_standing`` came from scripts. This tool
runs that DDL against throwaway databases and writes what it built to
``backend/tests/fixtures/module_ddl_schema_golden.json``, which
``test_schema_from_migrations`` checks against the chain. The golden is captured,
never hand-written: ``--check`` re-captures and fails on any difference.

Two scratch databases on the LOCAL server (D-TC9: ``pytest_module_ddl_*`` only,
never ``cars``/``cars_cleanroom``, a non-local host or port 15432; both dropped in
``finally``):

* ``empty``: the module ``ensure_*`` helpers, called for real, each on a database
  holding only what the earlier steps built.
* ``v001_v014``: the scripts' DDL statements, read from their source (scripts run
  only from ``main()``; reading avoids import side effects). It holds what they run
  on: ``migrations/V001__baseline.sql`` (``package_values`` for
  ``CREATE TABLE ... (LIKE package_values)``) and ``V014__option_rejections.sql``
  (the table of ``uq_option_rejections_standing``). Those prerequisites are not
  part of the golden.

Every step must build something; a step that builds nothing fails the capture
(its DDL found its objects already there, so the capture would prove nothing).
The golden records, per step, exactly the tables, columns, indexes, constraints
and sequences the step added.

Usage:
  python -m backend.scripts.capture_module_ddl_golden            # rewrite the golden
  python -m backend.scripts.capture_module_ddl_golden --check    # exit 1 if it differs
"""
from __future__ import annotations

import argparse
import ast
import json
import logging
import os
import re
import sys
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterator

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from backend.scripts import schema_drift_report as sdr  # noqa: E402

log = logging.getLogger("capture_module_ddl_golden")

GOLDEN_PATH = _REPO_ROOT / "backend" / "tests" / "fixtures" / "module_ddl_schema_golden.json"
MIGRATIONS_DIR = _REPO_ROOT / "migrations"
DEFAULT_ADMIN_DSN = sdr.DEFAULT_SCRATCH_ADMIN_DSN
SCRATCH_PREFIX = "pytest_module_ddl_"
_SCRATCH_NAME_RE = re.compile(r"^pytest_module_ddl_[a-z0-9_]{1,40}$")
PREREQ_FILES = ("V001__baseline.sql", "V014__option_rejections.sql")

EXIT_OK = 0
EXIT_DIFFERS = 1
EXIT_FAILED = 2

# Runtime DDL this capture deliberately leaves out, and why. Kept in the golden so
# the list is reviewed with it.
EXCLUDED: tuple[tuple[str, str], ...] = (
    ("backend/db/* runtime DDL (inventory_pg legacy DDL, dealerships_db, comments_db, "
     "dictionary_schema, incomplete_listings_db, dealer_portal_db, grid_cards_repo)",
     "captured in runtime_ddl_schema_golden.json (2026-10-01); backend/scanner/database.py "
     "_ensure_schema, schema_repo.init_inventory_db and import_epa_master.ensure_table reach "
     "Postgres only through init_postgres_inventory"),
    ("backend/vector/pgvector_service.py ensure_schema (*_embeddings tables)",
     "no *_embeddings table exists on prod or local (P0A.2), so D-DB7 adopts none; it connects "
     "through PGVECTOR_URL and vector(dim) depends on the embedding model"),
    ("backend/db/search_analytics_db.py, backend/db/user_history_db.py",
     "users database (get_users_conn: SQLite / SQLCipher), not the inventory Postgres"),
    ("backend/db/users_db, backend/db/admin_users_db.py, backend/utils/ip_rate_limit.py, "
     "backend/dev/routes.py, backend/enrichment/dictionary_catalog.py",
     "SQLite-only stores (REMEDIATION_PLAN P13A.3 allowlist)"),
    ("backend/scripts/link_cars_to_catalog.py ensure_link_columns",
     "ALTERs cars; epa_master_id, epa_match_confidence and epa_match_method are V001 columns"),
    ("backend/scripts/repair_vin_make_model.py",
     "one-off CREATE TABLE ... AS backup table (D-DB1), not schema"),
    ("backend/scripts/migrate.py schema_migrations", "runner-owned bookkeeping table"),
)


class Refused(RuntimeError):
    """A guard refused the run before anything was created."""


# ---------------------------------------------------------------------------
# DDL read from script source
# ---------------------------------------------------------------------------


def _module_tree(rel_path: str) -> ast.Module:
    return ast.parse((_REPO_ROOT / rel_path).read_text(encoding="utf-8"), filename=rel_path)


def module_constant(rel_path: str, name: str) -> str:
    """The string a module assigns to ``name`` at top level."""
    for node in _module_tree(rel_path).body:
        if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == name for t in node.targets):
            if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
                return node.value.value
    raise LookupError(f"{rel_path}: no top-level string constant {name}")


def execute_literals(rel_path: str, function: str, prefixes: tuple[str, ...] = ("CREATE", "ALTER")) -> list[str]:
    """String literals passed to ``<x>.execute(...)`` inside ``function``, in source
    order, whose first word is one of ``prefixes`` (the function's DDL)."""
    for node in ast.walk(_module_tree(rel_path)):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == function:
            found: list[tuple[int, int, str]] = []
            for call in ast.walk(node):
                if (isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute)
                        and call.func.attr == "execute" and call.args
                        and isinstance(call.args[0], ast.Constant) and isinstance(call.args[0].value, str)):
                    text = call.args[0].value
                    if text.strip().split(None, 1)[0].upper() in prefixes:
                        found.append((call.lineno, call.col_offset, text))
            if not found:
                raise LookupError(f"{rel_path}:{function}: no DDL literal passed to execute()")
            return [t for _l, _c, t in sorted(found)]
    raise LookupError(f"{rel_path}: no function {function}")


# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Step:
    source: str          # repo-relative file
    call: str            # what runs
    db: str              # "empty" | "v001_v014"
    run: Callable[[Any], None]


def _run_statements(statements: list[str]) -> Callable[[Any], None]:
    def run(conn: Any) -> None:
        cur = conn.cursor()
        for sql in statements:
            cur.execute(sql)
        cur.close()
    return run


def _haiku(conn: Any) -> None:
    from backend.enrichment import service

    service.ensure_enrichment_columns(conn)


def _job_queue(conn: Any) -> None:
    from backend.scanner import job_queue

    job_queue.ensure_job_tables(conn)


def _recipe_store(conn: Any) -> None:
    from backend.scanner import recipe_store

    saved = recipe_store._table_ready
    recipe_store._table_ready = False
    try:
        recipe_store._ensure_table(conn)
    finally:
        recipe_store._table_ready = saved


def _scan_profile(conn: Any) -> None:
    from backend.scanner.dealer import profile

    profile.ensure_dealer_scan_profile_table(conn.cursor())


def _generations(conn: Any) -> None:
    from backend.catalog import generations

    generations.ensure_model_generations_table(conn)


def _market_pricing(conn: Any) -> None:
    from backend.intelligence import market_pricing

    market_pricing.ensure_schema(conn)


def _vin_owner_conflicts(conn: Any) -> None:
    from backend.scanner import database

    conn.cursor().execute(database._VIN_OWNER_CONFLICTS_DDL)  # what record_vin_owner_conflicts runs


def build_steps() -> list[Step]:
    """Every captured DDL source, in run order (pure apart from reading source files)."""
    modules = [
        Step("backend/enrichment/service.py", "ensure_enrichment_columns (Postgres: haiku_spec_cache)", "empty", _haiku),
        Step("backend/scanner/job_queue.py", "ensure_job_tables", "empty", _job_queue),
        Step("backend/scanner/recipe_store.py", "_ensure_table", "empty", _recipe_store),
        Step("backend/scanner/dealer/profile.py", "ensure_dealer_scan_profile_table", "empty", _scan_profile),
        Step("backend/catalog/generations.py", "ensure_model_generations_table", "empty", _generations),
        Step("backend/intelligence/market_pricing.py", "ensure_schema", "empty", _market_pricing),
        Step("backend/scanner/database.py", "_VIN_OWNER_CONFLICTS_DDL (record_vin_owner_conflicts)", "empty",
             _vin_owner_conflicts),
    ]
    scripts = [
        ("backend/scripts/apply_attribution_moves.py", "_DDL",
         [module_constant("backend/scripts/apply_attribution_moves.py", "_DDL")]),
        ("backend/scripts/heal_matcher_trims.py", "_QUARANTINE_DDL",
         [module_constant("backend/scripts/heal_matcher_trims.py", "_QUARANTINE_DDL")]),
        ("backend/scripts/quarantine_package_value_msrps.py", "_ensure_quarantine_table",
         execute_literals("backend/scripts/quarantine_package_value_msrps.py", "_ensure_quarantine_table")),
        ("backend/scripts/feed_package_registry_from_vision.py", "main: CREATE UNIQUE INDEX",
         execute_literals("backend/scripts/feed_package_registry_from_vision.py", "main", ("CREATE",))),
    ]
    return modules + [Step(src, call, "v001_v014", _run_statements(stmts)) for src, call, stmts in scripts]


# ---------------------------------------------------------------------------
# Snapshot deltas
# ---------------------------------------------------------------------------


def delta(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    """What ``after`` has that ``before`` lacks, in golden form. Pure.

    Columns of tables that already existed count too (an ALTER ... ADD COLUMN).
    """
    cols: dict[str, dict[str, list[str]]] = {}
    for (table, col), (_dt, nullable, default) in after["columns"].items():
        if (table, col) in before["columns"] or table not in after["tables"]:
            continue
        ftype = after.get("column_types", {}).get((table, col), (_dt,))[0]
        cols.setdefault(table, {})[col] = [ftype, nullable, default]
    return {
        "tables": {t: dict(sorted(cols.get(t, {}).items())) for t in sorted(set(cols) | (after["tables"] - before["tables"]))},
        "indexes": [{"name": n, "table": v[0], "def": v[1]}
                    for n, v in sorted(after["indexes"].items()) if n not in before["indexes"]],
        "constraints": [{"name": n, "table": t, "type": v[0], "def": v[1]}
                        for (t, n), v in sorted(after["constraints"].items()) if (t, n) not in before["constraints"]],
        "sequences": [{"name": n, "owned_by": v[0]}
                      for n, v in sorted(after["sequences"].items()) if n not in before["sequences"]],
    }


def _is_empty_delta(d: dict[str, Any]) -> bool:
    return not (d["tables"] or d["indexes"] or d["constraints"] or d["sequences"])


def assemble(captured: list[tuple[Step, dict[str, Any]]]) -> dict[str, Any]:
    """The golden document from each step's delta. Pure; deterministic order."""
    tables: dict[str, dict[str, list[str]]] = {}
    indexes, constraints, sequences, sources = [], [], [], []
    for step, d in captured:
        for t, cols in d["tables"].items():
            tables.setdefault(t, {}).update(cols)
        indexes += d["indexes"]
        constraints += d["constraints"]
        sequences += d["sequences"]
        sources.append({
            "source": step.source, "call": step.call, "db": step.db,
            "tables": sorted(d["tables"]), "indexes": [i["name"] for i in d["indexes"]],
            "constraints": [c["name"] for c in d["constraints"]],
            "sequences": [s["name"] for s in d["sequences"]],
        })
    return {
        "_about": (
            "Schema that module-level runtime DDL builds on Postgres, captured by "
            "backend/scripts/capture_module_ddl_golden.py (never hand-edited; --check re-captures). "
            "'empty' steps call each module's ensure_* on an empty database; 'v001_v014' steps run the "
            "scripts' DDL statements, read from their source, on a database holding migrations V001 and "
            "V014 (what they run on). test_schema_from_migrations asserts the chain creates all of it."
        ),
        "tables": {t: dict(sorted(cols.items())) for t, cols in sorted(tables.items())},
        "indexes": sorted(indexes, key=lambda i: (i["table"], i["name"])),
        "constraints": sorted(constraints, key=lambda c: (c["table"], c["name"])),
        "sequences": sorted(sequences, key=lambda s: s["name"]),
        "sources": sources,
        "excluded": [{"what": w, "why": y} for w, y in EXCLUDED],
    }


def render(doc: dict[str, Any]) -> str:
    return json.dumps(doc, indent=1, sort_keys=False) + "\n"


# ---------------------------------------------------------------------------
# Scratch databases
# ---------------------------------------------------------------------------


def check_scratch_name(name: str) -> None:
    if not _SCRATCH_NAME_RE.match(name):
        raise Refused(f"scratch name {name!r} must match {_SCRATCH_NAME_RE.pattern} (D-TC9: pytest_* only)")
    if name in sdr.PROTECTED_DBS:
        raise Refused(f"scratch name {name!r} is a protected database")


@contextmanager
def inventory_env(dsn: str) -> Iterator[None]:
    """Point the repo's inventory resolver at ``dsn`` (module helpers pick their
    dialect from it) and refuse unless it resolves to exactly that DSN."""
    from backend.db import inventory_pg

    keys = ("INVENTORY_DATABASE_URL", "DATABASE_URL", "INVENTORY_SCHEMA_CHECK", "INVENTORY_AUTO_MIGRATE")
    saved = {k: os.environ.get(k) for k in keys}
    try:
        os.environ["INVENTORY_DATABASE_URL"] = dsn
        os.environ.pop("DATABASE_URL", None)
        os.environ["INVENTORY_SCHEMA_CHECK"] = "warn"
        os.environ.pop("INVENTORY_AUTO_MIGRATE", None)
        if inventory_pg.inventory_postgres_dsn() != dsn or not inventory_pg.is_inventory_postgres():
            raise Refused("inventory DSN did not resolve to the scratch database; refusing")
        yield
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def _apply_prereqs(conn: Any, directory: Path) -> None:
    cur = conn.cursor()
    for name in PREREQ_FILES:
        cur.execute((directory / name).read_text(encoding="utf-8"))
        # V001 is pg_dump output: it empties search_path for the session.
        cur.execute("SET search_path TO DEFAULT")
    conn.commit()
    cur.close()


def capture(admin_dsn: str = DEFAULT_ADMIN_DSN, directory: Path = MIGRATIONS_DIR,
            steps: list[Step] | None = None) -> dict[str, Any]:
    """Create the scratch DBs, run every step, drop them; return the golden document."""
    import psycopg

    sdr.check_scratch_server(admin_dsn)
    steps = build_steps() if steps is None else steps
    tag = uuid.uuid4().hex[:10]
    names = {"empty": f"{SCRATCH_PREFIX}{tag}_e", "v001_v014": f"{SCRATCH_PREFIX}{tag}_p"}
    for n in names.values():
        check_scratch_name(n)
    admin = psycopg.connect(admin_dsn, autocommit=True)
    created: list[str] = []
    try:
        for n in names.values():
            if admin.execute("SELECT 1 FROM pg_database WHERE datname = %s", (n,)).fetchone():
                raise Refused(f"scratch database {n!r} already exists; refusing to reuse or drop it")
            admin.execute(f'CREATE DATABASE "{n}" TEMPLATE template0')
            created.append(n)
        dsns = {k: sdr.with_dbname(admin_dsn, n) for k, n in names.items()}
        with psycopg.connect(dsns["v001_v014"]) as pconn:
            _apply_prereqs(pconn, directory)
        captured: list[tuple[Step, dict[str, Any]]] = []
        for step in steps:
            with inventory_env(dsns[step.db]), psycopg.connect(dsns[step.db]) as conn:
                before = sdr.snapshot(conn)
                step.run(conn)
                conn.commit()
                after = sdr.snapshot(conn)
            d = delta(before, after)
            if _is_empty_delta(d):
                raise RuntimeError(f"{step.source} ({step.call}) built nothing on the {step.db} database")
            log.info("%s: %s", step.source, ", ".join(sorted(d["tables"])) or [i["name"] for i in d["indexes"]])
            captured.append((step, d))
        return assemble(captured)
    finally:
        for n in created:
            try:
                admin.execute(f'DROP DATABASE IF EXISTS "{n}" WITH (FORCE)')
            except Exception as exc:  # pragma: no cover - reported, never hidden
                log.error("could not drop scratch database %s: %s (drop it by hand)", n, exc)
        admin.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("--scratch-admin-dsn", default=os.environ.get("SCHEMA_DRIFT_SCRATCH_DSN") or DEFAULT_ADMIN_DSN,
                        help="LOCAL server where the pytest_module_ddl_* DBs are built and dropped")
    parser.add_argument("--out", default=str(GOLDEN_PATH), help="golden path (default: the test fixture)")
    parser.add_argument("--check", action="store_true", help="compare with --out instead of writing it")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    try:
        text = render(capture(args.scratch_admin_dsn))
    except Refused as exc:
        log.error("refused: %s", exc)
        return EXIT_FAILED
    except Exception as exc:
        log.error("capture failed: %s", exc)
        return EXIT_FAILED
    out = Path(args.out)
    if args.check:
        current = out.read_text(encoding="utf-8") if out.exists() else ""
        if current != text:
            log.error("%s differs from a fresh capture; re-run without --check and review the diff", out)
            return EXIT_DIFFERS
        log.info("%s matches a fresh capture", out)
        return EXIT_OK
    out.write_text(text, encoding="utf-8")
    log.info("wrote %s", out)
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
