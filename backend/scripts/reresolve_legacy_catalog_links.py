#!/usr/bin/env python3
"""P10B.6 (D-ER3): re-resolve every car linked to a legacy catalog row with those rows
excluded, then archive the legacy rows nothing references any more. Reversible.

What it does, one make at a time (one transaction per make; a rerun with the same
``--run-id`` resumes: a car already moved no longer links to a legacy row, an archived row
is gone from ``epa_master``):

1. The cars (active or not) linked to a row whose ``epa_master.source`` is the legacy label
   are re-resolved through ``link_cars_to_catalog.relink_cars(exclude_sources=...)`` - the
   resolver future scans use, so the dealer's engine text and the VIN decode still decide
   (the catalog never outranks them). A car that no longer resolves loses its link.
2. Each changed car is written to ``epa_relink_snapshot`` (V037): run_id, car_id, old and new
   link with confidence and method. The snapshot and the link writes commit together.
3. Legacy rows no car links to and no ``epa_extended_specs`` row hangs off are copied into
   ``epa_master_archive`` under their ORIGINAL ids (reason ``legacy_root``, ``superseded_by``
   = the content twin, or NULL), recorded in ``epa_relink_archived``, and deleted. A
   referenced row is never deleted.

``--undo <run_id>`` reinserts the archived rows with their original ids and puts every car
back on its old link, confidence and method (a car a later scan already moved is left alone
and counted). Rows are stamped ``undone_at``; a second undo is a no-op.

DRY RUN by default: nothing is written, and the report also covers the UNLINKED cars whose
LIMIT-1 ``knowledge_engine.lookup_epa_by_trim`` row would change once the legacy rows are
gone. ``--apply`` is owner-gated (P10B.8) and runs only against the local database. Reports:
cars relinked vs cleared, cars that landed on their content twin (verification only), atv_type
transitions (Hybrid, FFV, Diesel, FCV ...), rows archived vs kept (and why).

  PYTHONPATH=. .venv/bin/python backend/scripts/reresolve_legacy_catalog_links.py
  PYTHONPATH=. .venv/bin/python backend/scripts/reresolve_legacy_catalog_links.py --makes toyota --report out.json
  PYTHONPATH=. .venv/bin/python backend/scripts/reresolve_legacy_catalog_links.py --apply --run-id legacy-1
  PYTHONPATH=. .venv/bin/python backend/scripts/reresolve_legacy_catalog_links.py --undo legacy-1 --apply

Exit status 2: refused (nothing written).
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

_REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))

from backend.utils.project_env import load_project_dotenv

load_project_dotenv()

from backend.catalog.resolver import CatalogUnavailableError  # noqa: E402
from backend.db.inventory_db import get_conn  # noqa: E402
from backend.scripts.link_cars_to_catalog import relink_cars  # noqa: E402

EXIT_REFUSED = 2
# The plan labels the root-DICTIONARY rows ``dictionary_legacy_root``; the provenance backfill
# (P10A.6) stamps them ``legacy``. Without --source the one that exists in epa_master is used.
LEGACY_SOURCE_LABELS = ("dictionary_legacy_root", "legacy")
LEGACY_ARCHIVE_REASON = "legacy_root"
_CHUNK = 1000
_SAMPLE = 20
# every epa_master column except the sequence default: what an archive copy keeps and undo returns
_ARCHIVE_COLUMNS = ("id", "epa_vehicle_id", "year", "make", "model", "cylinders", "displacement",
                    "trany", "drive", "fuel_type", "city08", "highway08", "city_e", "highway_e",
                    "atv_type", "trim", "body_style", "engine_description", "engine_display",
                    "forced_induction", "t_charger", "s_charger", "source", "natural_key",
                    "loaded_at")
_COLS_SQL = ", ".join(f'"{c}"' for c in _ARCHIVE_COLUMNS)
_TWIN_FIELDS = ("year", "model", "trim", "cylinders", "displacement", "drive", "trany", "fuel_type")


class Refused(RuntimeError):
    """A precondition failed: nothing was written."""


def archive_reason(source: str) -> str:
    return LEGACY_ARCHIVE_REASON if source in LEGACY_SOURCE_LABELS else source


def new_run_id() -> str:
    return "reresolve-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _in(ids: Sequence) -> str:
    return ",".join("?" * len(ids))


def _chunks(seq: Sequence, n: int = _CHUNK):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def _count(cur, sql: str, params: tuple = ()) -> int:
    cur.execute(sql, params)
    return int(cur.fetchone()[0])


# --- reading the plan -------------------------------------------------------------------------

def detect_source(cur, requested: str | None) -> str:
    """The legacy label to use: the requested one, else the first label present."""
    if requested:
        return requested
    for label in LEGACY_SOURCE_LABELS:
        if _count(cur, "SELECT count(*) FROM epa_master WHERE source=?", (label,)):
            return label
    raise Refused("no epa_master row carries a legacy source "
                  f"({', '.join(LEGACY_SOURCE_LABELS)}); run the provenance backfill (P10A.6) or pass --source")


def check_preconditions(conn, source: str) -> None:
    """Refuse when a run would only clear cars: the catalog needs other rows to relink to."""
    cur = conn.cursor()
    if not _count(cur, "SELECT count(*) FROM epa_master WHERE source=?", (source,)):
        raise Refused(f"no epa_master rows with source={source!r}: nothing to re-resolve")
    if not _count(cur, "SELECT count(*) FROM epa_master WHERE source IS DISTINCT FROM CAST(? AS TEXT)", (source,)):
        raise Refused(f"every catalog row has source={source!r}: excluding them would clear every car")
    for table in ("epa_relink_snapshot", "epa_relink_archived", "epa_master_archive"):
        try:
            cur.execute(f"SELECT 1 FROM {table} LIMIT 1")
            cur.fetchall()
        except Exception as exc:  # noqa: BLE001
            try:
                conn.rollback()
            except Exception:  # noqa: BLE001
                pass
            raise Refused(f"{table} is missing (apply migrations V036 and V037 first): {exc}") from exc


def source_makes(cur, source: str) -> list[str]:
    cur.execute('SELECT DISTINCT lower(make) FROM epa_master WHERE source=? AND make IS NOT NULL ORDER BY 1', (source,))
    return [r[0] for r in cur.fetchall()]


def _source_rows(cur, source: str, make_lc: str) -> list[dict]:
    cols = ("id",) + _TWIN_FIELDS + ("atv_type",)
    cur.execute(
        "SELECT id, year, model, \"trim\", cylinders, displacement, drive, trany, fuel_type, atv_type "
        "FROM epa_master WHERE source=? AND lower(make)=? ORDER BY id", (source, make_lc))
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def _twin_key(row: dict) -> tuple:
    return tuple(str(row[f]).lower() if isinstance(row[f], str) else row[f] for f in _TWIN_FIELDS)


def twin_map(cur, source: str, make_lc: str, rows: list[dict]) -> dict[int, int | None]:
    """legacy id -> the lowest id of a non-legacy row with the same content (year, model,
    trim, engine, drive, transmission, fuel), or None."""
    cur.execute(
        "SELECT id, year, model, \"trim\", cylinders, displacement, drive, trany, fuel_type FROM epa_master "
        "WHERE source IS DISTINCT FROM CAST(? AS TEXT) AND lower(make)=? ORDER BY id", (source, make_lc))
    cols = ("id",) + _TWIN_FIELDS
    by_key: dict[tuple, int] = {}
    for r in cur.fetchall():
        d = dict(zip(cols, r))
        by_key.setdefault(_twin_key(d), d["id"])
    return {r["id"]: by_key.get(_twin_key(r)) for r in rows}


def _cars_linked_to(cur, row_ids: list[int]) -> dict[int, list[int]]:
    """catalog id -> ids of the cars (active or not) linked to it."""
    out: dict[int, list[int]] = {}
    for part in _chunks(row_ids):
        cur.execute(f"SELECT epa_master_id, id FROM cars WHERE epa_master_id IN ({_in(part)})", tuple(part))
        for mid, cid in cur.fetchall():
            out.setdefault(int(mid), []).append(int(cid))
    return out


def _specced(cur, row_ids: list[int]) -> set[int]:
    out: set[int] = set()
    for part in _chunks(row_ids):
        cur.execute(f"SELECT epa_master_id FROM epa_extended_specs WHERE epa_master_id IN ({_in(part)})", tuple(part))
        out.update(int(r[0]) for r in cur.fetchall())
    return out


def _atv(cur, ids: set[int]) -> dict[int, str | None]:
    out: dict[int, str | None] = {}
    todo = sorted(ids)
    for part in _chunks(todo):
        cur.execute(f"SELECT id, atv_type FROM epa_master WHERE id IN ({_in(part)})", tuple(part))
        out.update({int(i): a for i, a in cur.fetchall()})
    return out


def plan_make(cur, source: str, make_lc: str) -> dict:
    """Everything a make's run would do, with no writes."""
    rows = _source_rows(cur, source, make_lc)
    row_ids = [r["id"] for r in rows]
    twins = twin_map(cur, source, make_lc, rows)
    linked = _cars_linked_to(cur, row_ids)
    car_ids = sorted({c for cs in linked.values() for c in cs})
    res = relink_cars(car_ids, exclude_sources=(source,), dry_run=True) if car_ids else {
        "stats": {}, "changes": []}
    changes = res["changes"]
    changed_ids = {c["id"] for c in changes}
    if res["stats"].get("skipped_catalog_error"):
        raise CatalogUnavailableError(
            f"{res['stats']['skipped_catalog_error']} car(s) of make {make_lc!r} could not be resolved")
    specced = _specced(cur, row_ids)
    remaining_cars = {rid: [c for c in cs if c not in changed_ids] for rid, cs in linked.items()}
    archive, kept = [], []
    for r in rows:
        rid = r["id"]
        if remaining_cars.get(rid):
            kept.append({"id": rid, "reason": "cars_still_linked", "cars": len(remaining_cars[rid])})
        elif rid in specced:
            kept.append({"id": rid, "reason": "extended_specs"})
        else:
            archive.append(rid)
    relinked = cleared = on_twin = 0
    atv_ids = set()
    for c in changes:
        old, new = c["old"][0], c["new"][0]
        atv_ids.update(i for i in (old, new) if i is not None)
        if new is None:
            cleared += 1
        else:
            relinked += 1
            if old is not None and twins.get(int(old)) == int(new):
                on_twin += 1
    atv = _atv(cur, atv_ids)
    trans = Counter()
    for c in changes:
        old, new = c["old"][0], c["new"][0]
        a0 = atv.get(int(old)) if old is not None else None
        a1 = atv.get(int(new)) if new is not None else None
        if (a0 or "") != (a1 or ""):
            trans[f"{a0 or '-'} -> {a1 or '-'}"] += 1
    return {
        "make": make_lc, "legacy_rows": len(rows), "cars_examined": len(car_ids),
        "changes": changes, "relinked": relinked, "cleared": cleared, "landed_on_twin": on_twin,
        "atv_transitions": dict(trans), "archive": archive,
        "archive_twin": {rid: twins.get(rid) for rid in archive}, "kept": kept,
        "stats": res["stats"],
    }


def unlinked_lookup_report(cur, source: str, make_lc: str) -> dict:
    """UNLINKED cars whose LIMIT-1 trim lookup would answer differently once the legacy rows
    are gone (``lookup_epa_by_trim``; a read-time fallback, nothing is stored)."""
    from backend.enrichment.knowledge_engine import lookup_epa_by_trim

    cur.execute(
        "SELECT year, make, model, \"trim\", count(*), sum(CASE WHEN COALESCE(listing_active,1)=1 THEN 1 ELSE 0 END) "
        "FROM cars WHERE epa_master_id IS NULL AND lower(make)=? AND \"trim\" IS NOT NULL AND \"trim\" <> '' "
        "AND EXISTS (SELECT 1 FROM epa_master m WHERE m.source=? AND m.year=cars.year AND lower(m.make)=?) "
        "GROUP BY year, make, model, \"trim\"", (make_lc, source, make_lc))
    groups = cur.fetchall()
    cars = changed_cars = changed_active = 0
    changed_groups, sample = 0, []
    for year, make, model, trim, n, n_active in groups:
        n, n_active = int(n), int(n_active or 0)
        cars += n
        before = lookup_epa_by_trim(year, make, model, trim)
        after = lookup_epa_by_trim(year, make, model, trim, exclude_sources=(source,))
        if before == after:
            continue
        changed_groups += 1
        changed_cars += n
        changed_active += n_active
        if len(sample) < _SAMPLE:
            sample.append({"year": year, "make": make, "model": model, "trim": trim, "cars": n,
                           "before": before, "after": after})
    return {"unlinked_cars_in_groups": cars, "groups_changed": changed_groups,
            "cars_changed": changed_cars, "active_cars_changed": changed_active, "sample": sample}


# --- apply ------------------------------------------------------------------------------------

def apply_make(conn, source: str, run_id: str, plan: dict) -> dict:
    """One transaction: link writes + snapshot, then archive the rows nothing references."""
    cur = conn.cursor()
    reason = archive_reason(source)
    out = {"moved": 0, "moved_since": 0, "archived": 0}
    try:
        for c in plan["changes"]:
            old, new = c["old"], c["new"]
            sql = ("UPDATE cars SET epa_master_id=?, epa_match_confidence=?, epa_match_method=? "
                   "WHERE id=? AND epa_master_id IS NOT DISTINCT FROM CAST(? AS BIGINT)")
            cur.execute(sql, (new[0], new[1], new[2], c["id"], old[0]))
            if cur.rowcount != 1:
                out["moved_since"] += 1  # a scan relinked it since the plan was read
                continue
            cur.execute(
                "INSERT INTO epa_relink_snapshot (run_id, car_id, old_id, new_id, method, old_confidence, "
                "old_method, new_confidence) VALUES (?,?,?,?,?,?,?,?)",
                (run_id, c["id"], old[0], new[0], new[2], old[1], old[2], new[1]))
            out["moved"] += 1
        for rid in plan["archive"]:
            twin = plan["archive_twin"].get(rid)
            cur.execute(
                f"INSERT INTO epa_master_archive ({_COLS_SQL}, archive_reason, superseded_by) "
                f"SELECT {_COLS_SQL}, ?, ? FROM epa_master m WHERE m.id=? AND m.source=? "
                "AND NOT EXISTS (SELECT 1 FROM cars c WHERE c.epa_master_id=m.id) "
                "AND NOT EXISTS (SELECT 1 FROM epa_extended_specs x WHERE x.epa_master_id=m.id)",
                (reason, twin, rid, source))
            if cur.rowcount != 1:
                continue  # referenced again since the plan: it stays
            cur.execute("DELETE FROM epa_master WHERE id=?", (rid,))
            cur.execute("INSERT INTO epa_relink_archived (run_id, epa_master_id, superseded_by, reason) "
                        "VALUES (?,?,?,?)", (run_id, rid, twin, reason))
            out["archived"] += 1
        # pre-commit assertion: nothing the run archived is still linked
        if plan["archive"]:
            arch_ids = list(plan["archive"])
            for part in _chunks(arch_ids):
                dangling = _count(cur, "SELECT count(*) FROM cars c WHERE c.epa_master_id IN (%s) AND NOT EXISTS "
                                       "(SELECT 1 FROM epa_master m WHERE m.id=c.epa_master_id)" % _in(part), tuple(part))
                if dangling:
                    raise RuntimeError(f"{dangling} car(s) would dangle after archiving")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return out


def run(*, apply: bool, source: str | None, run_id: str | None, makes: Sequence[str] = (),
        lookup_report: bool = True) -> dict:
    conn = get_conn()
    try:
        cur = conn.cursor()
        source = detect_source(cur, source)
        check_preconditions(conn, source)
        all_makes = source_makes(cur, source)
        wanted = [m.strip().lower() for m in makes if m.strip()]
        todo = [m for m in all_makes if not wanted or m in wanted]
        run_id = run_id or new_run_id()
        report: dict[str, Any] = {"run_id": run_id, "source": source, "apply": apply, "makes": {}}
        totals = Counter()
        atv_totals: Counter = Counter()
        kept_all: list[dict] = []
        for mk in todo:
            plan = plan_make(cur, source, mk)
            entry = {k: plan[k] for k in ("legacy_rows", "cars_examined", "relinked", "cleared",
                                          "landed_on_twin", "atv_transitions")}
            entry["to_archive"] = len(plan["archive"])
            entry["kept"] = len(plan["kept"])
            if apply:
                entry["applied"] = apply_make(conn, source, run_id, plan)
                cur = conn.cursor()
            elif lookup_report:
                entry["unlinked_lookup"] = unlinked_lookup_report(cur, source, mk)
            report["makes"][mk] = entry
            for k in ("legacy_rows", "cars_examined", "relinked", "cleared", "landed_on_twin"):
                totals[k] += plan[k] if k in plan else 0
            totals["to_archive"] += len(plan["archive"])
            atv_totals.update(plan["atv_transitions"])
            kept_all.extend(dict(k, make=mk) for k in plan["kept"])
            if apply:
                for k in ("moved", "moved_since", "archived"):
                    totals["applied_" + k] += entry["applied"][k]
            elif lookup_report:
                for k in ("cars_changed", "active_cars_changed"):
                    totals["unlinked_lookup_" + k] += entry["unlinked_lookup"][k]
        report["totals"] = dict(totals)
        report["atv_transitions"] = dict(atv_totals)
        report["kept"] = kept_all
        return report
    finally:
        conn.close()


# --- undo -------------------------------------------------------------------------------------

def undo(run_id: str, *, apply: bool) -> dict:
    """Reinsert the run's archived rows under their original ids, then restore every car the
    run moved (that no later scan moved again) to its old link. One transaction."""
    conn = get_conn()
    cur = conn.cursor()
    out = {"run_id": run_id, "apply": apply, "rows_to_restore": 0, "rows_restored": 0,
           "rows_already_present": 0, "cars_to_restore": 0, "cars_restored": 0,
           "cars_moved_since": 0, "cars_old_row_missing": 0}
    try:
        cur.execute("SELECT epa_master_id, reason FROM epa_relink_archived WHERE run_id=? AND undone_at IS NULL "
                    "ORDER BY epa_master_id", (run_id,))
        rows = [(int(a), b) for a, b in cur.fetchall()]
        cur.execute("SELECT car_id, old_id, new_id, old_confidence, old_method FROM epa_relink_snapshot "
                    "WHERE run_id=? AND undone_at IS NULL ORDER BY car_id", (run_id,))
        cars = cur.fetchall()
        if not rows and not cars:
            raise Refused(f"nothing to undo for run_id {run_id!r} (unknown, or already undone)")
        out["rows_to_restore"], out["cars_to_restore"] = len(rows), len(cars)
        for rid, reason in rows:
            cur.execute("SELECT max(archived_at) FROM epa_master_archive WHERE id=? AND archive_reason=?",
                        (rid, reason))
            at = cur.fetchone()[0]
            if at is None:
                raise Refused(f"epa_master_archive has no copy of row {rid} (reason {reason!r}); nothing changed")
            if _count(cur, "SELECT count(*) FROM epa_master WHERE id=?", (rid,)):
                out["rows_already_present"] += 1
            else:
                cur.execute(f"INSERT INTO epa_master ({_COLS_SQL}) SELECT {_COLS_SQL} FROM epa_master_archive "
                            "WHERE id=? AND archive_reason=? AND archived_at=?", (rid, reason, at))
                out["rows_restored"] += 1
            cur.execute("DELETE FROM epa_master_archive WHERE id=? AND archive_reason=? AND archived_at=?",
                        (rid, reason, at))
        for car_id, old_id, new_id, old_conf, old_method in cars:
            if old_id is not None and not _count(cur, "SELECT count(*) FROM epa_master WHERE id=?", (old_id,)):
                out["cars_old_row_missing"] += 1
                continue
            cur.execute(
                "UPDATE cars SET epa_master_id=?, epa_match_confidence=?, epa_match_method=? "
                "WHERE id=? AND epa_master_id IS NOT DISTINCT FROM CAST(? AS BIGINT)",
                (old_id, old_conf, old_method, car_id, new_id))
            if cur.rowcount == 1:
                out["cars_restored"] += 1
            else:
                out["cars_moved_since"] += 1
        cur.execute("UPDATE epa_relink_snapshot SET undone_at=now() WHERE run_id=? AND undone_at IS NULL", (run_id,))
        cur.execute("UPDATE epa_relink_archived SET undone_at=now() WHERE run_id=? AND undone_at IS NULL", (run_id,))
        if apply:
            conn.commit()
        else:
            conn.rollback()  # dry run: the counts above came from the real statements
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return out


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--apply", action="store_true", help="write (default: dry run)")
    ap.add_argument("--undo", metavar="RUN_ID", help="reverse a run (dry run unless --apply)")
    ap.add_argument("--run-id", help="name this run; reuse the id to resume an interrupted run")
    ap.add_argument("--source", help=f"epa_master.source to retire (default: whichever of {LEGACY_SOURCE_LABELS} exists)")
    ap.add_argument("--makes", default="", help="comma-separated makes (default: every make with such rows)")
    ap.add_argument("--no-lookup-report", action="store_true", help="skip the unlinked-car lookup report (dry run)")
    ap.add_argument("--report", help="write the JSON report here")
    args = ap.parse_args(argv)
    try:
        if args.undo:
            out = undo(args.undo, apply=args.apply)
        else:
            out = run(apply=args.apply, source=args.source, run_id=args.run_id,
                      makes=args.makes.split(","), lookup_report=not args.no_lookup_report)
    except Refused as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return EXIT_REFUSED
    except CatalogUnavailableError as exc:
        print(f"catalog unavailable, run stopped (finished makes stay committed; rerun resumes): {exc}",
              file=sys.stderr)
        return EXIT_REFUSED
    text = json.dumps(out, indent=2, default=str)
    if args.report:
        Path(args.report).write_text(text)
    print(json.dumps(out.get("totals", out), indent=2, default=str))
    if not args.apply:
        print("dry run: nothing written (--apply to write)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
