"""Static guard against new runtime DDL (REMEDIATION_PLAN P13A.3).

The migration chain (``migrations/``) is the only DDL source for Postgres. Review
reports and the haiku spec cache were once added as runtime ``CREATE TABLE`` in
application code instead of migrations; this test makes that fail in CI.

It parses every non-test ``.py`` under ``backend/`` and ``scripts/`` and flags any
string literal (docstrings excluded) that holds a CREATE, ALTER or DROP statement
for a table, index, view, extension, schema, sequence, trigger, function, type or
column. A file with such a literal must be listed below.

* ``SQLITE_ONLY`` modules never reach Postgres.
* ``SNAPSHOT_AND_REPAIR_SCRIPTS`` are named operator scripts (dry run by default).
* ``KNOWN_MODULE_DDL`` are the module ``ensure_*`` and adapter branches that
  P13B.2 / P13B.3 remove. That list only shrinks: an entry whose file no longer
  holds DDL fails ``test_allowlist_has_no_stale_entries`` until it is deleted.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
_ROOTS = ("backend", "scripts")

_DDL_RE = re.compile(
    r"(?:^|;)\s*(?:CREATE|ALTER|DROP)\s+"
    r"(?:OR\s+REPLACE\s+|UNIQUE\s+|TEMP(?:ORARY)?\s+|MATERIALIZED\s+|UNLOGGED\s+)*"
    r"(?:TABLE|INDEX|VIEW|EXTENSION|SCHEMA|SEQUENCE|TRIGGER|FUNCTION|TYPE|COLUMN)\b",
    re.IGNORECASE | re.MULTILINE,
)

SQLITE_ONLY = {
    "backend/db/users_db/schema.py",
    "backend/db/admin_users_db.py",
    "backend/db/user_history_db.py",
    "backend/db/search_analytics_db.py",
    "backend/db/dictionary_schema.py",
    "backend/enrichment/dictionary_catalog.py",
}

SNAPSHOT_AND_REPAIR_SCRIPTS = {
    "backend/scripts/apply_attribution_moves.py",
    "backend/scripts/dump_baseline_schema.py",
    "backend/scripts/feed_package_registry_from_vision.py",
    "backend/scripts/heal_forced_induction_from_evidence.py",  # N24 P10C.9 CREATE TEMP TABLE for the set-based apply
    "backend/scripts/heal_matcher_trims.py",
    "backend/scripts/heal_p8a7_driver.py",
    "backend/scripts/heal_vdp_junk_values.py",
    "backend/scripts/import_epa_master.py",  # P10A.8 CREATE TEMP TABLE for the set-based refresh
    "backend/scripts/link_cars_to_catalog.py",
    "backend/scripts/migrate.py",
    "backend/scripts/quarantine_package_value_msrps.py",
    "backend/scripts/relabel_spec_provenance.py",
    "backend/utils/db_snapshot.py",
}

# Remove an entry when P13B.2 / P13B.3 (or another unit) deletes its DDL.
KNOWN_MODULE_DDL = {
    "backend/catalog/generations.py",
    "backend/db/dealer_portal_db.py",
    "backend/db/incomplete_listings_db.py",
    "backend/db/inventory_pg.py",
    "backend/db/repositories/grid_cards_repo.py",
    "backend/db/repositories/schema_repo.py",
    "backend/db/retirement_log.py",
    "backend/enrichment/service.py",
    "backend/intelligence/market_pricing.py",
    "backend/scanner/database.py",
    "backend/scanner/dealer/profile.py",
    "backend/scanner/job_queue.py",
    "backend/scanner/recipe_store.py",
    "backend/vector/pgvector_service.py",
}

ALLOWLIST = SQLITE_ONLY | SNAPSHOT_AND_REPAIR_SCRIPTS | KNOWN_MODULE_DDL


def _docstring_ids(tree: ast.AST) -> set[int]:
    ids: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            first = node.body[0] if node.body else None
            if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant):
                ids.add(id(first.value))
    return ids


def source_has_ddl(source: str) -> bool:
    """True when a non-docstring string literal in ``source`` holds DDL."""
    tree = ast.parse(source)
    skip = _docstring_ids(tree)
    return any(
        isinstance(n, ast.Constant)
        and isinstance(n.value, str)
        and id(n) not in skip
        and _DDL_RE.search(n.value)
        for n in ast.walk(tree)
    )


def _files_with_ddl(root: Path = _REPO) -> set[str]:
    found: set[str] = set()
    for base in _ROOTS:
        for path in sorted((root / base).rglob("*.py")):
            rel = path.relative_to(root).as_posix()
            if "/tests/" in f"/{rel}" or "/migrations/" in f"/{rel}" or path.name.startswith("test_"):
                continue
            if source_has_ddl(path.read_text(encoding="utf-8")):
                found.add(rel)
    return found


def test_no_new_runtime_ddl_outside_allowlist():
    new = sorted(_files_with_ddl() - ALLOWLIST)
    assert not new, (
        "runtime DDL in non-allowlisted modules (add a migration under migrations/ "
        f"instead): {new}"
    )


def test_allowlist_has_no_stale_entries():
    stale = sorted(ALLOWLIST - _files_with_ddl())
    assert not stale, f"allowlist entries with no DDL left; delete them: {stale}"


def test_guard_fails_on_injected_ddl(tmp_path):
    mod = tmp_path / "backend" / "newfeature.py"
    mod.parent.mkdir(parents=True)
    mod.write_text(
        'def ensure(cur):\n    cur.execute("CREATE TABLE IF NOT EXISTS x (id int)")\n'
    )
    (tmp_path / "scripts").mkdir()
    assert _files_with_ddl(tmp_path) == {"backend/newfeature.py"}
    assert "backend/newfeature.py" not in ALLOWLIST


def test_guard_ignores_docstrings_and_dml():
    assert not source_has_ddl('"""CREATE TABLE in prose."""\nx = "SELECT 1"\n')
    assert source_has_ddl('q = """\n  ALTER TABLE cars ADD COLUMN y int\n"""\n')
