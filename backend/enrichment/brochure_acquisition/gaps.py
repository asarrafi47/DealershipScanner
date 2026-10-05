"""
Gap list: active inventory folded into one entry per real vehicle, ranked by car count.

Moved verbatim out of ``backend/scripts/fetch_oem_brochures.py`` (audit
datascripts.md F7); that script is still the CLI and re-exports these names.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.db.connect import connection as db_connection
from backend.enrichment.brochure_sources import (
    gap_group_key,
    unsupported_reason,
)
from backend.enrichment.dictionary_catalog import canonical_make, catalog_key
from backend.enrichment.dictionary_paths import (
    BROCHURE_TEXT_DIR,
)


def _fetch_inventory_rows() -> list[tuple[int, str, str, int]]:
    with db_connection(timeout=None) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT year, make, model, COUNT(*)
                FROM cars
                WHERE listing_removed_at IS NULL
                  AND year IS NOT NULL
                  AND make IS NOT NULL AND btrim(make) <> ''
                  AND model IS NOT NULL AND btrim(model) <> ''
                GROUP BY 1, 2, 3
                """
            )
            return [(int(y), m, mo, int(c)) for y, m, mo, c in cur.fetchall()]


def build_gap_list(
    rows: list[tuple[int, str, str, int]], have: set[str]
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Fold inventory rows into one entry per real vehicle, ranked by car count.

    ``have`` is the set of corpus stems (``2026__mazda__cx50``).

    Two counts come out of this and they answer different questions:

    * ``uncovered_vehicles`` -- distinct vehicles with **no** brochure text under
      any of their inventory spellings. This is the acquisition backlog.
    * ``alias_only_keys`` -- catalog keys with no corpus file whose sibling
      spelling *does* have one. Those vehicles are not missing a document; the
      exact-match lookup in ``dictionary_derived`` just cannot see it. Fetching
      for them would re-download a document we already hold, which is how the
      first run of this script stored the CX-50 spec deck twice.
    """
    groups: dict[str, dict[str, Any]] = {}
    total_cars = 0
    raw_keys: set[str] = set()

    for year, make, model, count in rows:
        total_cars += count
        ck = catalog_key(year, make, model).replace("|", "__")
        raw_keys.add(ck)
        gk = gap_group_key(year, make, model)
        entry = groups.setdefault(
            gk,
            {
                "group_key": gk,
                "year": year,
                "make": canonical_make(make),
                "cars": 0,
                "variants": [],
            },
        )
        entry["cars"] += count
        entry["variants"].append(
            {
                "make": make,
                "model": model,
                "cars": count,
                "catalog_key": ck,
                "has_text": ck in have,
            }
        )

    gaps: list[dict[str, Any]] = []
    covered_cars = 0
    covered_vehicles = 0
    alias_only: list[dict[str, Any]] = []

    for entry in groups.values():
        variants = sorted(entry["variants"], key=lambda v: (-v["cars"], v["catalog_key"]))
        entry["variants"] = variants
        entry["model"] = variants[0]["model"]
        covered_variants = [v for v in variants if v["has_text"]]

        if covered_variants:
            covered_cars += entry["cars"]
            covered_vehicles += 1
            held = covered_variants[0]["catalog_key"]
            alias_only.extend(
                {
                    "missing_key": v["catalog_key"],
                    "document_held_under": held,
                    "cars": v["cars"],
                }
                for v in variants
                if not v["has_text"]
            )
            continue

        # Where a fetched document should be filed. The corpus convention strips
        # the make prefix ("2010__mazda__3.json"), so prefer a spelling that
        # already does; otherwise the most common spelling.
        stripped = [
            v
            for v in variants
            if catalog_key(entry["year"], v["make"], v["model"]).split("|")[-1]
            == entry["group_key"].split("|")[-1]
        ]
        primary = (stripped or variants)[0]
        entry["primary"] = primary
        entry["primary_catalog_key"] = primary["catalog_key"]
        entry["alias_catalog_keys"] = [
            v["catalog_key"] for v in variants if v["catalog_key"] != primary["catalog_key"]
        ]
        entry["unsupported_reason"] = unsupported_reason(entry["make"])
        gaps.append(entry)

    gaps.sort(key=lambda g: (-g["cars"], g["group_key"]))

    stats = {
        "active_cars": total_cars,
        "corpus_files": len(have),
        "raw_catalog_key_combos": len(raw_keys),
        "distinct_vehicles": len(groups),
        "covered_vehicles": covered_vehicles,
        "uncovered_vehicles": len(gaps),
        "covered_cars": covered_cars,
        "gap_cars": total_cars - covered_cars,
        "alias_only_keys": len({a["missing_key"] for a in alias_only}),
        "alias_only_cars": sum(a["cars"] for a in alias_only),
    }
    stats["alias_only"] = sorted(alias_only, key=lambda a: -a["cars"])
    return gaps, stats


def load_gap_list() -> tuple[list[dict[str, Any]], dict[str, Any]]:
    have = {p.stem for p in BROCHURE_TEXT_DIR.glob("*.json") if not p.name.startswith("_")}
    return build_gap_list(_fetch_inventory_rows(), have)


def write_gap_artifact(
    gaps: list[dict[str, Any]], stats: dict[str, Any], path: Path, brand: str | None
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "brand_filter": brand,
        "stats": stats,
        "gaps": gaps,
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path
