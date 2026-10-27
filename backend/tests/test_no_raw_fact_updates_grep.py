"""Exit gate for P8B.7: no raw fact UPDATE outside ``apply_fact_proposals``.

Fact columns (the precedence ranked fields) may only be written through
``backend.vehicle_facts.apply``, which ranks every candidate and writes the value
with its provenance in one statement. This test parses every non-test ``.py`` under
``backend/`` and ``scripts/`` and flags

* a string literal (f-string text included) holding ``UPDATE cars SET`` whose SET
  clause names a fact column, or whose SET list is built at run time, and
* an ``update_car_row_partial(...)`` call whose literal dict names a fact column.

Files below are the audited exceptions; the list only shrinks. The recovery scripts
(INCOMPLETE section 2.19) must never appear in it.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

from backend.vehicle_facts import precedence as prec

_REPO = Path(__file__).resolve().parents[2]
_ROOTS = ("backend", "scripts")

FACT_COLUMNS = set(prec.FIELDS) | {"engine", "drivetrain", "fuel_type", "cylinders"}

_UPDATE_RE = re.compile(r"UPDATE\s+cars\s+SET\b(?P<set>.*?)(?:\bWHERE\b|$)", re.IGNORECASE | re.DOTALL)

# The writers themselves: the apply helper and the column-level primitive it calls.
WRITERS = {
    "backend/vehicle_facts/apply.py",
    "backend/db/repositories/cars_repo.py",
}

RECOVERY_SCRIPTS = {
    "backend/scripts/heal_from_recipes.py",
    "backend/scripts/harvest_carscommerce.py",
    "backend/scripts/harvest_html_jsonld.py",
    "backend/scripts/recover_incomplete_listings.py",
    "backend/scripts/repair_inventory_fields.py",
    "backend/scripts/backfill_specs.py",
}

# Audited exceptions: each writes a fact column outside apply_fact_proposals for a
# stated reason. Remove an entry when the file stops doing so.
KNOWN_RAW_WRITERS: dict[str, str] = {
    # vPIC is the top-ranked source; the decode job writes its own provenance in the same UPDATE.
    "backend/enrichment/vpic_facts.py": "vPIC decode writer (rank 100), provenance in the same UPDATE",
    # Column-level primitives with a dynamic SET list built from non-ranked or already-ranked keys.
    "backend/enrichment/service.py": "batch writer; fact keys arrive already ranked by the enrichment service",
    "backend/dev/scan_lab.py": "scan-lab sandbox DB, never the fleet",
    "backend/parsers/team_velocity.py": "VDP image/carfax patch; no fact column",
    # Operator repair scripts: dry run by default, snapshot first, provenance written with the value.
    "backend/scripts/heal_cylinders_from_vpic.py": "vPIC-sourced cylinders / forced induction heal",
    "backend/scripts/heal_ev_cylinders.py": "vPIC-sourced EV cylinder NULLing heal",
    "backend/scripts/heal_p8a7_driver.py": "restores a prior value it wrote itself (revert of its own change)",
    "backend/scripts/heal_stock_code_contamination.py": "NULLs stock-code contamination (stock_number, mileage)",
    "backend/scripts/heal_vdp_junk_values.py": "NULLs parser-junk values after quarantine, provenance recorded",
    "backend/scripts/migrate_placeholder_nulls.py": "one-shot placeholder-to-NULL migration",
    "backend/scripts/repair_injection_fuel.py": "NULLs injected fuel labels (removal, never a new value)",
    "backend/scripts/repair_vin_make_model.py": "make/model repair from the VIN; no ranked fact column",
}


def _py_files():
    for root in _ROOTS:
        for p in sorted((_REPO / root).rglob("*.py")):
            rel = p.relative_to(_REPO).as_posix()
            if "/tests/" in rel or rel.startswith("backend/tests/") or "/__pycache__/" in rel:
                continue
            yield rel, p


def _docstring_ids(tree: ast.AST) -> set[int]:
    ids: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            body = getattr(node, "body", [])
            if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant) \
                    and isinstance(body[0].value.value, str):
                ids.add(id(body[0].value))
    return ids


def _set_hits(text: str) -> list[str]:
    hits: list[str] = []
    for m in _UPDATE_RE.finditer(text):
        clause = m.group("set")
        cols = {c.lower() for c in re.findall(r"([A-Za-z_]+)\s*=", clause)}
        named = sorted(cols & FACT_COLUMNS)
        if named:
            hits.append("UPDATE cars SET " + ",".join(named))
        elif "{" in clause or not cols:
            hits.append("UPDATE cars SET <dynamic>")
    return hits


def scan_source(source: str) -> list[str]:
    """Raw fact UPDATE findings in one module's source."""
    tree = ast.parse(source)
    skip = _docstring_ids(tree)
    out: list[str] = []
    fstring_parts: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.JoinedStr):
            text = "".join(
                v.value if isinstance(v, ast.Constant) else "{}" for v in node.values
            )
            out += _set_hits(text)
            for v in node.values:
                fstring_parts.add(id(v))
        elif isinstance(node, ast.Call):
            fn = node.func
            name = fn.attr if isinstance(fn, ast.Attribute) else getattr(fn, "id", "")
            if name == "update_car_row_partial" and len(node.args) >= 2 \
                    and isinstance(node.args[1], ast.Dict):
                keys = {k.value for k in node.args[1].keys
                        if isinstance(k, ast.Constant) and isinstance(k.value, str)}
                named = sorted(keys & FACT_COLUMNS)
                if named:
                    out.append("update_car_row_partial " + ",".join(named))
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str) \
                and id(node) not in skip and id(node) not in fstring_parts:
            out += _set_hits(node.value)
    return out


def _findings() -> dict[str, list[str]]:
    found: dict[str, list[str]] = {}
    for rel, p in _py_files():
        if rel in WRITERS:
            continue
        hits = scan_source(p.read_text(encoding="utf-8"))
        if hits:
            found[rel] = sorted(set(hits))
    return found


def test_scanner_flags_raw_fact_updates():
    assert scan_source('cur.execute("UPDATE cars SET drivetrain = ? WHERE id = ?")')
    assert scan_source('cur.execute(f"UPDATE cars SET {sets} WHERE id = ?")')
    assert scan_source("update_car_row_partial(1, {'fuel_type': 'Hybrid'})")
    assert not scan_source('cur.execute("UPDATE cars SET price = NULL WHERE id = ?")')
    assert not scan_source("update_car_row_partial(1, {'internal_notes': 'x'})")


def test_no_raw_fact_updates_outside_known_writers():
    found = _findings()
    unexpected = {k: v for k, v in found.items() if k not in KNOWN_RAW_WRITERS}
    assert not unexpected, f"raw fact UPDATE outside apply_fact_proposals: {unexpected}"


def test_known_raw_writers_has_no_stale_entries():
    found = _findings()
    stale = sorted(k for k in KNOWN_RAW_WRITERS if k not in found)
    assert not stale, f"no longer writing fact columns raw, delete from the list: {stale}"


def test_recovery_scripts_have_no_raw_fact_updates():
    assert not (RECOVERY_SCRIPTS & set(KNOWN_RAW_WRITERS))
    found = _findings()
    for rel in sorted(RECOVERY_SCRIPTS):
        assert (_REPO / rel).exists(), rel
        assert rel not in found, (rel, found.get(rel))
