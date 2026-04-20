"""Flatten normalized reference tables into the inventory-style CSV template."""
from __future__ import annotations

import csv
import sqlite3
from pathlib import Path
from typing import Iterable

from vehicle_reference.utils.mpg import normalize_mpg_cell

# Exact header order for downstream compatibility.
CSV_COLUMNS = [
    "Year",
    "Make",
    "Model",
    "Trim",
    "Body Style",
    "Engine",
    "Transmission",
    "Drivetrain",
    "Fuel Type",
    "MPG",
    "Packages",
    "PackageDetails",
    "ExtColorOpts",
    "IntColorOpts",
    "PassengerSeating",
]


def _trim_display(variant_name: str | None, trim_line: str | None) -> str:
    v = (variant_name or "").strip()
    t = (trim_line or "").strip()
    if v and t:
        return f"{v} | {t}"
    return v or t


def iter_export_rows(
    conn: sqlite3.Connection,
    *,
    brand_code: str = "bmw",
    series_name: str | None = None,
    year_from: int | None = None,
    year_to: int | None = None,
    complete_only: bool = False,
    qa_missing_options: bool = False,
) -> Iterable[dict[str, str]]:
    params: list[object] = [brand_code]
    where = ["b.code = ?"]
    if series_name:
        where.append("v.series_name = ?")
        params.append(series_name)
    if year_from is not None:
        where.append("v.model_year >= ?")
        params.append(year_from)
    if year_to is not None:
        where.append("v.model_year <= ?")
        params.append(year_to)
    if complete_only:
        where.append(
            """
            NULLIF(trim(COALESCE(v.engine,'')), '') IS NOT NULL
            AND NULLIF(trim(COALESCE(v.transmission,'')), '') IS NOT NULL
            AND NULLIF(trim(COALESCE(v.drivetrain,'')), '') IS NOT NULL
            AND NULLIF(trim(COALESCE(v.fuel_type,'')), '') IS NOT NULL
            AND NULLIF(trim(COALESCE(v.mpg_text,'')), '') IS NOT NULL
            AND NULLIF(trim(COALESCE(v.body_style,'')), '') IS NOT NULL
            AND NULLIF(trim(COALESCE(v.variant_name,'')), '') IS NOT NULL
            """
        )
    if qa_missing_options:
        where.append(
            """
            (
              NOT EXISTS (SELECT 1 FROM ref_vehicle_exterior ve WHERE ve.vehicle_id = v.id)
              OR NOT EXISTS (SELECT 1 FROM ref_vehicle_interior vi WHERE vi.vehicle_id = v.id)
              OR NOT EXISTS (SELECT 1 FROM ref_vehicle_package vp WHERE vp.vehicle_id = v.id)
            )
            """
        )

    sql = f"""
    SELECT
      v.id AS vehicle_id,
      v.model_year,
      b.display_name AS make,
      v.series_name,
      v.variant_name,
      v.trim_line,
      v.body_style,
      v.engine,
      v.transmission,
      v.drivetrain,
      v.fuel_type,
      v.mpg_text,
      v.passenger_seating,
      v.uncertainty_notes,
      src.label AS source_label,
      src.url AS source_url
    FROM ref_vehicle v
    JOIN ref_brand b ON b.id = v.brand_id
    JOIN ref_source src ON src.id = v.source_id
    WHERE {" AND ".join(where)}
    ORDER BY v.model_year, v.series_name, v.variant_name, v.trim_line, v.id
    """
    cur = conn.execute(sql, params)
    for row in cur:
        vid = row["vehicle_id"]
        pkgs = conn.execute(
            """
            SELECT p.package_name, p.package_details
            FROM ref_vehicle_package vp
            JOIN ref_package p ON p.id = vp.package_id
            WHERE vp.vehicle_id = ?
            ORDER BY vp.sort_order, p.package_name
            """,
            (vid,),
        ).fetchall()
        pkg_names = [r[0] for r in pkgs if r[0]]
        pkg_details = [r[1] for r in pkgs if r[1]]

        ext = conn.execute(
            """
            SELECT e.color_name
            FROM ref_vehicle_exterior ve
            JOIN ref_exterior_color e ON e.id = ve.exterior_color_id
            WHERE ve.vehicle_id = ?
            ORDER BY e.color_name
            """,
            (vid,),
        ).fetchall()
        intc = conn.execute(
            """
            SELECT i.color_name
            FROM ref_vehicle_interior vi
            JOIN ref_interior_color i ON i.id = vi.interior_color_id
            WHERE vi.vehicle_id = ?
            ORDER BY i.color_name
            """,
            (vid,),
        ).fetchall()

        trim = _trim_display(row["variant_name"], row["trim_line"])
        uncertainty = (row["uncertainty_notes"] or "").strip()
        _ = (row["source_label"], row["source_url"])

        def _blank(s: str | None) -> str:
            if s is None:
                return ""
            s = str(s).strip()
            return s

        yield {
            "Year": str(row["model_year"]),
            "Make": _blank(row["make"]),
            "Model": _blank(row["series_name"]),
            "Trim": trim,
            "Body Style": _blank(row["body_style"]),
            "Engine": _blank(row["engine"]),
            "Transmission": _blank(row["transmission"]),
            "Drivetrain": _blank(row["drivetrain"]),
            "Fuel Type": _blank(row["fuel_type"]),
            "MPG": normalize_mpg_cell(_blank(row["mpg_text"])),
            "Packages": "; ".join(pkg_names),
            "PackageDetails": " | ".join(pkg_details),
            "ExtColorOpts": "; ".join(r[0] for r in ext),
            "IntColorOpts": "; ".join(r[0] for r in intc),
            "PassengerSeating": _blank(row["passenger_seating"]),
            "_uncertainty_notes": uncertainty,
        }


def export_to_csv(
    conn: sqlite3.Connection,
    out_path: Path,
    *,
    brand_code: str = "bmw",
    series_name: str | None = None,
    year_from: int | None = None,
    year_to: int | None = None,
    include_uncertainty_column: bool = False,
    complete_only: bool = False,
    qa_missing_options: bool = False,
) -> int:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    cols = list(CSV_COLUMNS)
    if include_uncertainty_column:
        cols.append("UncertaintyNotes")

    count = 0
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        for r in iter_export_rows(
            conn,
            brand_code=brand_code,
            series_name=series_name,
            year_from=year_from,
            year_to=year_to,
            complete_only=complete_only,
            qa_missing_options=qa_missing_options,
        ):
            row_out = {k: r.get(k, "") for k in CSV_COLUMNS}
            if include_uncertainty_column:
                row_out["UncertaintyNotes"] = r.get("_uncertainty_notes", "")
            w.writerow(row_out)
            count += 1
    return count
