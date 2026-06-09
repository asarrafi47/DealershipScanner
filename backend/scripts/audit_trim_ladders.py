#!/usr/bin/env python3
"""Audit trim ladder resolution for every make/model in active inventory."""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.db.inventory_db import DB_PATH
from backend.enrichment.trim_ladder import (
    _ladder_steps_plausible_for_model,
    _load_json_ladders,
    _pick_ladder_def,
    resolve_trim_ladder,
)
from backend.enrichment.trim_ladder_knowledge import preserve_trim_label

# Package/appearance lines that belong on BMW SAV configurator ladders, not motor trims.
_BMW_PACKAGE_LINES = frozenset(
    {
        "m competition",
        "competition",
        "m sport",
        "sport line",
        "luxury line",
        "modern line",
        "xline",
    }
)

# Jeep SUV trims that indicate Grand Cherokee / Cherokee ladder bleed.
_JEEP_GC_TRIMS = frozenset(
    {
        "summit reserve",
        "summit",
        "limited",
        "altitude",
        "premium",
        "laredo",
        "overland",
        "trailhawk",
        "high altitude",
    }
)

# Wagoneer-specific series trims.
_JEEP_WAGONEER_TRIMS = frozenset(
    {
        "l series iii",
        "l series ii",
        "l series i",
        "series iii",
        "series ii",
        "series i",
        "carbide",
        "obsidian",
        "launch edition",
    }
)

_BMW_MOTOR_TRIM_RE = re.compile(r"^[xs]Drive\d{2}[ie]|^M\d{2,3}i$", re.I)


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())


def _sample_cars(conn: sqlite3.Connection) -> list[dict]:
    cur = conn.execute(
        """
        SELECT c.id, c.make, c.model, c.year, c.trim, c.n AS model_count
        FROM (
            SELECT id, make, model, year, trim,
                   ROW_NUMBER() OVER (
                       PARTITION BY LOWER(TRIM(make)), LOWER(TRIM(model))
                       ORDER BY
                           CASE WHEN trim IS NOT NULL AND TRIM(trim) != '' THEN 0 ELSE 1 END,
                           year DESC,
                           id DESC
                   ) AS rn,
                   COUNT(*) OVER (
                       PARTITION BY LOWER(TRIM(make)), LOWER(TRIM(model))
                   ) AS n
            FROM cars
            WHERE COALESCE(listing_active, 1) = 1
              AND make IS NOT NULL AND TRIM(make) != ''
              AND model IS NOT NULL AND TRIM(model) != ''
        ) c
        WHERE c.rn = 1
        ORDER BY c.n DESC, c.make, c.model
        """
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _step_names(steps: list[dict]) -> list[str]:
    return [str(s.get("name") or "").strip() for s in steps if str(s.get("name") or "").strip()]


def _listing_looks_like_motor_trim(make: str, model: str, trim: str) -> bool:
    mk = _norm(make)
    if mk == "bmw" and _BMW_MOTOR_TRIM_RE.match(trim.replace(" ", "")):
        return True
    if mk == "bmw" and preserve_trim_label(trim, make, model):
        preserved = preserve_trim_label(trim, make, model)
        if preserved and _BMW_MOTOR_TRIM_RE.match(preserved.replace(" ", "")):
            return True
    if mk == "mercedesbenz" and re.search(r"\b\d{3}[a-z]?\b", trim, re.I):
        return True
    if mk == "jeep" and _norm(model) == "wagoneer" and trim.lower() in _JEEP_WAGONEER_TRIMS:
        return True
    return False


def _audit_car(car: dict) -> dict:
    make = car["make"]
    model = car["model"]
    year = car["year"]
    trim = car.get("trim") or ""

    ladder_def = _pick_ladder_def(make, model, year)
    result = resolve_trim_ladder(make=make, model=model, year=year, trim=trim)

    issues: list[str] = []
    if not result:
        issues.append("no_ladder")
        return {"car": car, "issues": issues, "result": None, "source": None}

    source = str(result.get("source") or ladder_def.get("source") if ladder_def else "")
    steps = result.get("steps") or []
    names = _step_names(steps)
    names_lower = {n.lower() for n in names}
    matched = bool(result.get("matched"))
    mk = _norm(make)
    mod = _norm(model)

    if source == "oem_knowledge":
        issues.append("generic_fallback")

    if trim and not matched:
        source_lower = source.lower()
        # EPA / Complete_Options / curated ladders are still useful when trim text differs
        # (dealer motor trim vs package line, EPA aggregate names, etc.).
        if source_lower not in {"curated"} and "epa" not in source_lower and "complete_options" not in source_lower:
            issues.append("trim_unmatched")

    if ladder_def and not _ladder_steps_plausible_for_model(steps, make, model):
        issues.append("implausible_ladder")

    if mk == "bmw" and mod in {"x1", "x2", "x3", "x4", "x5", "x6", "x7"}:
        has_motor = any(_BMW_MOTOR_TRIM_RE.match(n.replace(" ", "")) for n in names)
        all_packages = names_lower and names_lower <= _BMW_PACKAGE_LINES
        if all_packages and (_listing_looks_like_motor_trim(make, model, trim) or mod in {"x1", "x2", "x3", "x4", "x5", "x6", "x7"}):
            issues.append("bmw_package_lines_not_motor_trims")

    if mk == "jeep" and mod == "wagoneer":
        gc_hits = names_lower & _JEEP_GC_TRIMS
        if gc_hits and not (names_lower & _JEEP_WAGONEER_TRIMS):
            issues.append("jeep_wagoneer_grand_cherokee_bleed")

    if mk == "jeep" and mod == "grandwagoneer":
        if names_lower & _JEEP_GC_TRIMS and not (names_lower & _JEEP_WAGONEER_TRIMS):
            issues.append("jeep_grand_wagoneer_gc_bleed")

    if mk == "jeep" and mod in {"wrangler", "gladiator", "compass", "renegade"}:
        if names_lower & {"summit reserve", "summit", "limited", "altitude"} and not names_lower & {
            "rubicon",
            "sahara",
            "sport",
            "willys",
            "overland",
            "mojave",
            "high altitude",
        }:
            issues.append("jeep_wrong_line_trims")

    junk_patterns = (
        r"internet movie",
        r"badges",
        r"body-m",
        r"special interiors",
        r"at the ",
    )
    motor_trim_re = re.compile(r"^(?:M?\d{3}i?|M\d{2,3})$", re.I)
    for n in names:
        if motor_trim_re.match(n):
            continue
        if any(re.search(p, n, re.I) for p in junk_patterns):
            issues.append("junk_trim_name")
            break

    if source == "oem_knowledge":
        from backend.enrichment.trim_ladder_knowledge import (
            GENERIC_TRIM_ORDER,
            _make_fallback_trim_order,
            _model_trim_order,
        )

        model_steps = list(_model_trim_order(make, model)[:8])
        make_steps = list(_make_fallback_trim_order(make, model)[:8])
        generic_steps = list(GENERIC_TRIM_ORDER[:5])
        if names and names != generic_steps and (names == model_steps[: len(names)] or names == make_steps[: len(names)]):
            issues = [i for i in issues if i != "generic_fallback"]

    if len(steps) < 2:
        issues.append("too_few_steps")

    return {
        "car": car,
        "issues": issues,
        "result": {
            "source": source,
            "matched": matched,
            "listing_trim": result.get("listing_trim"),
            "steps": names[:8],
            "step_count": len(names),
        },
        "source": source,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit trim ladder resolution for inventory.")
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON summary on stdout",
    )
    args = parser.parse_args()

    _load_json_ladders.cache_clear()

    conn = sqlite3.connect(DB_PATH)
    cars = _sample_cars(conn)
    conn.close()

    flagged: list[dict] = []
    issue_counts: dict[str, int] = defaultdict(int)
    source_counts: dict[str, int] = defaultdict(int)

    for car in cars:
        audit = _audit_car(car)
        source = audit.get("source") or "none"
        source_counts[source] += 1
        if audit["issues"]:
            flagged.append(audit)
            for issue in audit["issues"]:
                issue_counts[issue] += 1

    if args.json:
        payload = {
            "audited_pairs": len(cars),
            "flagged_pairs": len(flagged),
            "issue_counts": dict(issue_counts),
            "source_counts": dict(source_counts),
            "flagged": [
                {
                    "make": a["car"]["make"],
                    "model": a["car"]["model"],
                    "year": a["car"]["year"],
                    "trim": a["car"].get("trim"),
                    "model_count": a["car"]["model_count"],
                    "issues": a["issues"],
                    "result": a["result"],
                }
                for a in flagged
            ],
        }
        print(json.dumps(payload, indent=2))
        return 1 if flagged else 0

    print(f"Audited {len(cars)} make/model pairs from inventory ({DB_PATH})")
    print()
    print("Ladder sources:")
    for src, n in sorted(source_counts.items(), key=lambda x: (-x[1], x[0])):
        print(f"  {src}: {n}")
    print()
    print(f"Flagged {len(flagged)} make/model pairs with potential issues:")
    for issue, n in sorted(issue_counts.items(), key=lambda x: (-x[1], x[0])):
        print(f"  {issue}: {n}")
    print()

    severity_order = {
        "bmw_package_lines_not_motor_trims": 0,
        "jeep_wagoneer_grand_cherokee_bleed": 0,
        "jeep_grand_wagoneer_gc_bleed": 0,
        "jeep_wrong_line_trims": 0,
        "implausible_ladder": 1,
        "junk_trim_name": 1,
        "generic_fallback": 2,
        "trim_unmatched": 3,
        "no_ladder": 4,
        "too_few_steps": 4,
    }

    flagged.sort(
        key=lambda a: (
            min(severity_order.get(i, 5) for i in a["issues"]),
            -a["car"]["model_count"],
            a["car"]["make"],
            a["car"]["model"],
        )
    )

    print("Top issues (by inventory volume):")
    print("-" * 100)
    for audit in flagged[:80]:
        car = audit["car"]
        res = audit["result"] or {}
        issues = ", ".join(audit["issues"])
        trim = car.get("trim") or "(no trim)"
        steps = res.get("steps") or []
        print(
            f"{car['make']:16} {car['model']:22} n={car['model_count']:4}  "
            f"year={car['year']}  trim={trim!r}"
        )
        print(f"  source={res.get('source')} matched={res.get('matched')} issues=[{issues}]")
        print(f"  steps={steps}")
        print()

    if len(flagged) > 80:
        print(f"... and {len(flagged) - 80} more flagged pairs")

    return 1 if flagged else 0


if __name__ == "__main__":
    raise SystemExit(main())
