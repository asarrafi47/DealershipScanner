"""Inventory trim-ladder audit thresholds (Phase 5 regression gate)."""

from __future__ import annotations

from collections import defaultdict

import pytest

from backend.db.inventory_db import get_conn
from backend.scripts.audit_trim_ladders import _audit_car, _sample_cars


# Severe issues that must stay near zero after curated ladders + quality gating.
_HARD_ISSUE_MAX = {
    "bmw_package_lines_not_motor_trims": 0,
    "jeep_wagoneer_grand_cherokee_bleed": 0,
    "jeep_grand_wagoneer_gc_bleed": 0,
    "jeep_wrong_line_trims": 0,
    "implausible_ladder": 2,
    "junk_trim_name": 0,
}

# Soft caps — inventory coverage improves over time; block runaway regressions.
_SOFT_ISSUE_MAX = {
    "generic_fallback": 10,
    "trim_unmatched": 25,
    "no_ladder": 220,
    "too_few_steps": 5,
}


def _run_inventory_audit() -> tuple[list[dict], dict[str, int]]:
    conn = get_conn()
    try:
        cars = _sample_cars(conn)
    finally:
        conn.close()

    flagged: list[dict] = []
    issue_counts: dict[str, int] = defaultdict(int)
    for car in cars:
        audit = _audit_car(car)
        if audit["issues"]:
            flagged.append(audit)
            for issue in audit["issues"]:
                issue_counts[issue] += 1
    return flagged, dict(issue_counts)


def test_trim_ladder_inventory_audit_thresholds() -> None:
    flagged, issue_counts = _run_inventory_audit()
    hard_failures: list[str] = []
    soft_failures: list[str] = []

    for issue, limit in _HARD_ISSUE_MAX.items():
        count = issue_counts.get(issue, 0)
        if count > limit:
            hard_failures.append(f"{issue}: {count} > {limit}")

    for issue, limit in _SOFT_ISSUE_MAX.items():
        count = issue_counts.get(issue, 0)
        if count > limit:
            soft_failures.append(f"{issue}: {count} > {limit}")

    if hard_failures or soft_failures:
        lines = [f"flagged_pairs={len(flagged)}"]
        for issue, n in sorted(issue_counts.items(), key=lambda x: (-x[1], x[0])):
            lines.append(f"  {issue}: {n}")
        if hard_failures:
            lines.append("hard failures:")
            lines.extend(f"  - {x}" for x in hard_failures)
        if soft_failures:
            lines.append("soft failures:")
            lines.extend(f"  - {x}" for x in soft_failures)
        pytest.fail("\n".join(lines))
