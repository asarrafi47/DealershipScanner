"""Quality checks on ref_vehicle and related tables."""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass


@dataclass
class ValidationIssue:
    code: str
    message: str
    detail: str | None = None


def run_validations(conn: sqlite3.Connection) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []

    dup = conn.execute(
        """
        SELECT model_year, series_name, COALESCE(variant_name,''), COALESCE(body_style,''),
               COALESCE(engine,''), COUNT(*) AS c
        FROM ref_vehicle
        GROUP BY 1, 2, 3, 4, 5
        HAVING c > 1
        LIMIT 50
        """
    ).fetchall()
    if dup:
        issues.append(
            ValidationIssue(
                "duplicate_vehicle_specs",
                "Multiple rows share the same year/series/variant/body/engine grouping (first 50 groups).",
                str(dup),
            )
        )

    orphan_vp = conn.execute(
        """
        SELECT vp.vehicle_id, vp.package_id
        FROM ref_vehicle_package vp
        LEFT JOIN ref_vehicle v ON v.id = vp.vehicle_id
        WHERE v.id IS NULL
        LIMIT 20
        """
    ).fetchall()
    if orphan_vp:
        issues.append(
            ValidationIssue(
                "orphan_vehicle_package",
                "Vehicle_package rows without parent vehicle.",
                str(orphan_vp),
            )
        )

    orphan_pk = conn.execute(
        """
        SELECT vp.vehicle_id, vp.package_id
        FROM ref_vehicle_package vp
        LEFT JOIN ref_package p ON p.id = vp.package_id
        WHERE p.id IS NULL
        LIMIT 20
        """
    ).fetchall()
    if orphan_pk:
        issues.append(
            ValidationIssue(
                "orphan_package_link",
                "Vehicle_package rows pointing to missing ref_package.",
                str(orphan_pk),
            )
        )

    missing_provenance = conn.execute(
        """
        SELECT v.id
        FROM ref_vehicle v
        LEFT JOIN ref_source s ON s.id = v.source_id
        WHERE s.id IS NULL OR length(trim(COALESCE(s.label,''))) = 0
        LIMIT 20
        """
    ).fetchall()
    if missing_provenance:
        issues.append(
            ValidationIssue(
                "missing_source",
                "Vehicles missing ref_source row or empty label.",
                str(missing_provenance),
            )
        )

    return issues
