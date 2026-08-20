#!/usr/bin/env python3
"""
Heal matcher-written trims: quarantine + NULL trims that provenance proves were
inferred by a matcher and that the car's own listing text never names.

WHAT THIS TARGETS (and nothing else)
------------------------------------
Active cars where BOTH hold:

1. ``spec_source_json['trim'].source`` is ``nhtsa_vpic`` or ``inventory_repair``
   — the trim was written by a matcher/repair pass, not read off the listing.
2. The trim appears nowhere in the listing's own text (title, model_full_raw,
   description, source_url), by the exact token logic of the
   ``car_trim_not_named_by_listing`` invariant
   (``backend/scripts/data_quality_invariants.py``), whose helpers this script
   imports directly so the two can never drift apart.

Rows with no trim provenance are deliberately OUT of scope: that 5,532-row
bucket is a mix of real dealer dataLayer trims the invariant cannot see and
pre-provenance writes, and blanking a real dealer-stated trim is worse than
leaving a matcher guess. Only provenance-proven matcher writes (~3,078 rows as
measured 2026-08-07) are touched. Rows are NEVER deleted; the trim column is
set to NULL and the old value is snapshotted into ``cars_trim_quarantine``
first, with provenance merged into ``spec_source_json`` (source
``matcher_trim_heal``) so the heal itself is auditable and reversible.

THE RE-FILL TRAP — WHY THIS SCRIPT REFUSES TO RUN WITHOUT THE WRITE-TIME GUARD
------------------------------------------------------------------------------
A blanked trim makes the row "incomplete" again:
``row_candidate_for_structured_spec_backfill``
(``backend/enrichment/spec_structured_backfill.py:120-149``) treats an
effectively-empty ``trim`` as a backfill trigger, so the very next structured
backfill pass would ask vPIC for a trim and write the SAME matcher guess back,
with fresh provenance — the heal would silently undo itself within a night.

The fix for that loop is the write-time guard being added to
``backend.utils.spec_field_normalize`` (``trim_named_in_listing``), which stops
the backfill from writing a trim the listing does not name. This script
therefore REFUSES to run — even in dry-run — unless that predicate can be
imported. If the import fails, the guard has not shipped, and running the heal
would only churn the same rows.

USAGE
-----
  PYTHONPATH=. python backend/scripts/heal_matcher_trims.py             # dry run (default)
  PYTHONPATH=. python backend/scripts/heal_matcher_trims.py --dry-run   # same, explicit
  PYTHONPATH=. python backend/scripts/heal_matcher_trims.py --apply     # write
  PYTHONPATH=. python backend/scripts/heal_matcher_trims.py --limit 50  # testing

``--apply`` refuses to touch more than MAX_APPLY_ROWS (3,500) rows: the
measured target population is ~3,078, so a materially larger count means the
selection is matching something it should not, and a human must look first.
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))

from backend.utils.project_env import load_project_dotenv

load_project_dotenv()

from backend.db.inventory_db import get_conn  # noqa: E402

# Token logic imported from the invariant itself (data_quality_invariants.py
# :591-595 / :615-631) so "not named by the listing" means the same thing to
# the measurement and to the heal, permanently.
from backend.scripts.data_quality_invariants import (  # noqa: E402
    _TRIM_SPLIT_RE,
    _norm_words,
)
from backend.utils.spec_provenance import merge_spec_source_json, utc_now_iso  # noqa: E402

#: Trim provenance sources that mean "a matcher wrote this", the only rows in scope.
MATCHER_TRIM_SOURCES = frozenset({"nhtsa_vpic", "inventory_repair"})

#: --apply hard ceiling. Measured target population 2026-08-07: 2,989 nhtsa_vpic
#: + 89 inventory_repair = 3,078. Anything well above that means the selection
#: broke, not that the backlog grew.
MAX_APPLY_ROWS = 3500

HEAL_SOURCE = "matcher_trim_heal"

_ROW_COLS = (
    "id", "vin", "trim", "title", "model_full_raw", "description",
    "source_url", "spec_source_json",
)


# --------------------------------------------------------------------------
# Guard: refuse to run before the write-time backfill guard has shipped
# --------------------------------------------------------------------------


def require_write_time_guard() -> None:
    """
    Import the backfill write-time guard predicate or die.

    See "THE RE-FILL TRAP" in the module docstring: without
    ``trim_named_in_listing`` in ``backend.utils.spec_field_normalize``, the
    structured backfill would re-write every trim this script blanks.
    """
    try:
        from backend.utils.spec_field_normalize import trim_named_in_listing  # noqa: F401
    except ImportError as exc:
        raise SystemExit(
            "REFUSING TO RUN: backend.utils.spec_field_normalize.trim_named_in_listing "
            f"is not importable ({exc}).\n"
            "That predicate is the write-time guard that stops "
            "spec_structured_backfill from re-writing a matcher trim onto every "
            "row this script blanks (an empty trim makes the row a vPIC backfill "
            "candidate again — spec_structured_backfill.py:120-149). Running the "
            "heal before the guard ships would only churn the same rows. Ship the "
            "guard first, then run this."
        )


# --------------------------------------------------------------------------
# Pure selection logic (unit-tested in backend/tests/test_trim_invariant.py)
# --------------------------------------------------------------------------


def trim_provenance_source(spec_source_json: Any) -> str | None:
    """The ``source`` recorded for the trim field, or None if absent/unparseable."""
    raw = spec_source_json
    if raw is None:
        return None
    if isinstance(raw, str):
        if not raw.strip():
            return None
        try:
            raw = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return None
    if not isinstance(raw, dict):
        return None
    entry = raw.get("trim")
    if not isinstance(entry, dict):
        return None
    src = entry.get("source")
    return str(src) if src is not None and str(src).strip() else None


def trim_named_in_listing_text(row: dict[str, Any]) -> bool:
    """
    Does the listing's own text name the trim?

    Exact replica of the invariant's check (``_invariant_trim_not_named``,
    data_quality_invariants.py:615-631), sharing its ``_norm_words`` /
    ``_TRIM_SPLIT_RE`` helpers: a slash/comma-list trim counts as named if ANY
    component appears, matching is punctuation-insensitive, and the URL slug
    counts as listing text.
    """
    blob = _norm_words(
        " ".join(
            str(x or "")
            for x in (
                row.get("title"),
                row.get("model_full_raw"),
                row.get("description"),
                row.get("source_url"),
            )
        )
    )
    parts = [p for p in _TRIM_SPLIT_RE.split(str(row.get("trim") or "")) if p.strip()]
    for part in parts:
        token = _norm_words(part).strip()
        if token and f" {token} " in blob:
            return True
    return False


def heal_target_source(row: dict[str, Any]) -> str | None:
    """
    Decide whether *row* is a heal target.

    Returns the matcher provenance source ('nhtsa_vpic' / 'inventory_repair')
    when the row should be healed, else None. A row is a target only when:
      - it has a non-empty trim,
      - the trim's provenance source is a matcher source (rows with NO
        provenance are never touched — they may be real dealer trims), and
      - the listing text does not name the trim.
    """
    trim = row.get("trim")
    if trim is None or not str(trim).strip():
        return None
    src = trim_provenance_source(row.get("spec_source_json"))
    if src not in MATCHER_TRIM_SOURCES:
        return None
    if trim_named_in_listing_text(row):
        return None
    # Second, MORE lenient check: the write-time guard's predicate splits at
    # letter<->digit boundaries, so 'GLB250' counts as named by a title saying
    # 'GLB 250'. The invariant does not digit-split (its count includes that
    # false-positive class); healing must not null a trim the dealer spelled
    # with a space. A row is healed only when BOTH checks agree it is unnamed.
    from backend.utils.spec_field_normalize import trim_named_in_listing

    if trim_named_in_listing(row, trim):
        return None
    return src


# --------------------------------------------------------------------------
# Quarantine + heal
# --------------------------------------------------------------------------

_QUARANTINE_DDL = """
    CREATE TABLE IF NOT EXISTS cars_trim_quarantine (
        car_id INTEGER,
        vin TEXT,
        old_trim TEXT,
        provenance_source TEXT,
        title TEXT,
        trim_provenance_json TEXT,
        quarantined_at TEXT
    )
"""


def _select_candidate_rows(cur, limit: int | None) -> list[dict[str, Any]]:
    sql = (
        f"SELECT {', '.join(_ROW_COLS)} FROM cars "
        "WHERE COALESCE(listing_active, 1) = 1 "
        "AND trim IS NOT NULL AND BTRIM(trim) <> '' "
        "AND spec_source_json IS NOT NULL"
    )
    if limit:
        sql += f" LIMIT {int(limit)}"
    cur.execute(sql)
    return [dict(zip(_ROW_COLS, r)) for r in cur.fetchall()]


def _reconstructed_vpic_trim(response_json: Any) -> str:
    """Rebuild the trim vPIC would have written: 'Trim Trim2' joined by a space.

    Mirrors backend/enrichment/nhtsa_vpic.py:274-278. Used to identify rows
    written BEFORE provenance stamping existed: a stored trim that exactly
    equals the cached decode, on a listing that never names it, is
    matcher-written in all but paperwork.
    """
    raw = response_json
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return ""
    if not isinstance(raw, dict):
        return ""
    results = raw.get("Results")
    d = results[0] if isinstance(results, list) and results else raw
    if not isinstance(d, dict):
        return ""
    parts = [str(d.get(k) or "").strip() for k in ("Trim", "Trim2")]
    return " ".join(p for p in parts if p)


_CACHE_ROW_COLS = tuple(_ROW_COLS) + ("response_json",)


def _select_cache_match_rows(cur, limit: int | None) -> list[dict[str, Any]]:
    """Rows with NO trim provenance whose VIN has a cached vPIC decode."""
    sql = (
        f"SELECT {', '.join('cars.' + c for c in _ROW_COLS)}, v.response_json "
        "FROM cars JOIN nhtsa_vpic_cache v ON v.vin = cars.vin "
        "WHERE COALESCE(cars.listing_active, 1) = 1 "
        "AND cars.trim IS NOT NULL AND BTRIM(cars.trim) <> '' "
        "AND (cars.spec_source_json IS NULL "
        "     OR cars.spec_source_json NOT LIKE '%\"trim\"%')"
    )
    if limit:
        sql += f" LIMIT {int(limit)}"
    cur.execute(sql)
    return [dict(zip(_CACHE_ROW_COLS, r)) for r in cur.fetchall()]


def cache_match_is_target(row: dict[str, Any]) -> bool:
    """True when a provenance-less trim equals the cached vPIC decode and the
    listing never names it (both the invariant's check and the digit-boundary-
    insensitive predicate must agree it is unnamed)."""
    trim = str(row.get("trim") or "").strip()
    if not trim:
        return False
    if trim_provenance_source(row.get("spec_source_json")) is not None:
        return False
    vt = _reconstructed_vpic_trim(row.get("response_json"))
    if not vt or vt.strip().lower() != trim.lower():
        return False
    if trim_named_in_listing_text(row):
        return False
    from backend.utils.spec_field_normalize import trim_named_in_listing

    return not trim_named_in_listing(row, trim)


def _trim_provenance_entry_json(spec_source_json: Any) -> str:
    """Snapshot of the spec_source_json['trim'] entry, as JSON text."""
    raw = spec_source_json
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return json.dumps({"_unparseable": True})
    if isinstance(raw, dict) and isinstance(raw.get("trim"), dict):
        return json.dumps(raw["trim"], ensure_ascii=False)
    return json.dumps(None)


def _quarantine_and_null(cur, row: dict[str, Any], src: str) -> None:
    """Snapshot the row into cars_trim_quarantine, then NULL the trim (never delete)."""
    old_trim = row.get("trim")
    cur.execute(
        "INSERT INTO cars_trim_quarantine "
        "(car_id, vin, old_trim, provenance_source, title, trim_provenance_json, quarantined_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            row.get("id"),
            row.get("vin"),
            old_trim,
            src,
            row.get("title"),
            _trim_provenance_entry_json(row.get("spec_source_json")),
            utc_now_iso(),
        ),
    )
    new_src = merge_spec_source_json(
        row.get("spec_source_json") if isinstance(row.get("spec_source_json"), str) else None,
        {
            "trim": {
                "source": HEAL_SOURCE,
                "detail": f"was {old_trim!r} from {src}; not named by listing text",
            }
        },
    )
    cur.execute(
        "UPDATE cars SET trim = NULL, spec_source_json = ? WHERE id = ?",
        (new_src, row.get("id")),
    )


def heal_matcher_trims(
    *, apply: bool, limit: int | None, include_cache_matches: bool = False
) -> dict[str, int]:
    conn = get_conn()
    cur = conn.cursor()
    rows = _select_candidate_rows(cur, limit)

    targets: list[tuple[dict[str, Any], str]] = []
    by_source: Counter[str] = Counter()
    for row in rows:
        src = heal_target_source(row)
        if src is not None:
            targets.append((row, src))
            by_source[src] += 1

    if include_cache_matches:
        cache_rows = _select_cache_match_rows(cur, limit)
        rows = rows + cache_rows
        for row in cache_rows:
            if cache_match_is_target(row):
                targets.append((row, "nhtsa_vpic_cache_match"))
                by_source["nhtsa_vpic_cache_match"] += 1

    stats = {"scanned": len(rows), "targets": len(targets), "healed": 0}

    print(f"scanned {len(rows)} active rows with trim + provenance", flush=True)
    print(f"targets (matcher-written trim not named by listing): {len(targets)}", flush=True)
    for src, n in by_source.most_common():
        print(f"  {src}: {n}", flush=True)
    print("samples (first 20):", flush=True)
    for row, src in targets[:20]:
        print(
            f"  car {row.get('id')} vin={row.get('vin')} trim={row.get('trim')!r} "
            f"src={src} title={str(row.get('title') or '')[:80]!r}",
            flush=True,
        )

    if not apply:
        conn.close()
        return stats

    if len(targets) > MAX_APPLY_ROWS:
        conn.close()
        raise SystemExit(
            f"REFUSING --apply: {len(targets)} targets exceeds the {MAX_APPLY_ROWS}-row "
            "ceiling. The measured population is ~3,078; a count this size means the "
            "selection is matching rows it should not. Investigate before applying."
        )

    cur.execute(_QUARANTINE_DDL)
    for row, src in targets:
        _quarantine_and_null(cur, row, src)
        stats["healed"] += 1
    conn.commit()
    conn.close()
    return stats


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Quarantine + NULL matcher-written trims the listing never names"
    )
    ap.add_argument(
        "--dry-run", action="store_true",
        help="Report only; the default when --apply is absent",
    )
    ap.add_argument("--apply", action="store_true", help="Write changes (default: dry run)")
    ap.add_argument("--limit", type=int, default=None, help="Only scan the first N rows (testing)")
    ap.add_argument(
        "--include-cache-matches", action="store_true",
        help="Also target provenance-less trims that exactly equal the VIN's "
             "cached vPIC decode (writes from before provenance stamping)",
    )
    args = ap.parse_args()
    if args.apply and args.dry_run:
        ap.error("--apply and --dry-run are mutually exclusive")

    # Hard prerequisite — see "THE RE-FILL TRAP" in the module docstring.
    require_write_time_guard()

    mode = "APPLY" if args.apply else "DRY RUN"
    print(f"heal_matcher_trims ({mode})", flush=True)
    stats = heal_matcher_trims(
        apply=args.apply, limit=args.limit,
        include_cache_matches=args.include_cache_matches,
    )
    print(f"done: {stats}", flush=True)


if __name__ == "__main__":
    main()
