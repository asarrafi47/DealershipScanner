"""Load curated JSON seeds into the reference database (no remote fetching)."""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any


def _resolve_source(cur: sqlite3.Cursor, spec: dict[str, Any]) -> int:
    label = (spec.get("label") or "").strip() or "unspecified_source"
    url = (spec.get("url") or "").strip() or None
    group = (spec.get("source_group_key") or "").strip() or None
    notes = (spec.get("notes") or "").strip() or None
    if url:
        cur.execute(
            "SELECT id FROM ref_source WHERE url = ? AND label = ? LIMIT 1",
            (url, label),
        )
        row = cur.fetchone()
        if row:
            return int(row[0])
    else:
        cur.execute(
            "SELECT id FROM ref_source WHERE url IS NULL AND label = ? LIMIT 1",
            (label,),
        )
        row = cur.fetchone()
        if row:
            return int(row[0])
    cur.execute(
        """
        INSERT INTO ref_source (label, url, source_group_key, notes)
        VALUES (?, ?, ?, ?)
        """,
        (label, url, group, notes),
    )
    return int(cur.lastrowid)


def _get_brand_id(cur: sqlite3.Cursor, code: str) -> int:
    cur.execute("SELECT id FROM ref_brand WHERE code = ?", (code,))
    row = cur.fetchone()
    if not row:
        raise KeyError(f"Unknown brand code {code!r}; run brand bootstrap first.")
    return int(row[0])


def ingest_coverage_lines(
    conn: sqlite3.Connection,
    payload: dict[str, Any],
    *,
    brand_code: str,
) -> int:
    """Insert coverage lines from JSON. Returns rows inserted/updated."""
    cur = conn.cursor()
    brand_id = _get_brand_id(cur, brand_code)
    n = 0
    for line in payload.get("coverage_lines", []):
        src_id = None
        if line.get("source"):
            src_id = _resolve_source(cur, line["source"])
        cur.execute(
            """
            INSERT INTO ref_coverage_line (
              brand_id, series_key, display_name, line_category,
              first_model_year_us, last_model_year_us, year_range_uncertain,
              coverage_status, notes, source_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(brand_id, series_key) DO UPDATE SET
              display_name = excluded.display_name,
              line_category = excluded.line_category,
              first_model_year_us = excluded.first_model_year_us,
              last_model_year_us = excluded.last_model_year_us,
              year_range_uncertain = excluded.year_range_uncertain,
              coverage_status = excluded.coverage_status,
              notes = excluded.notes,
              source_id = COALESCE(excluded.source_id, ref_coverage_line.source_id)
            """,
            (
                brand_id,
                line["series_key"],
                line["display_name"],
                line.get("line_category"),
                line.get("first_model_year_us"),
                line.get("last_model_year_us"),
                1 if line.get("year_range_uncertain", True) else 0,
                line.get("coverage_status", "pending"),
                line.get("notes"),
                src_id,
            ),
        )
        n += 1
    conn.commit()
    return n


def ingest_vehicle_bundle(
    conn: sqlite3.Connection, v: dict[str, Any], *, brand_code: str, commit: bool = True
) -> int:
    """Insert one vehicle row plus linked packages and colors."""
    cur = conn.cursor()
    brand_id = _get_brand_id(cur, brand_code)
    src_id = _resolve_source(cur, v["source"])

    ext_src = (v.get("external_source") or "").strip() or None
    ext_id = (v.get("external_record_id") or "").strip() or None
    if ext_src and ext_id:
        cur.execute(
            """
            DELETE FROM ref_vehicle
            WHERE brand_id = ? AND external_source = ? AND external_record_id = ?
            """,
            (brand_id, ext_src, ext_id),
        )

    cur.execute(
        """
        INSERT INTO ref_vehicle (
          brand_id, model_year, market, series_name, variant_name, trim_line,
          body_style, engine, transmission, drivetrain, fuel_type, mpg_text,
          passenger_seating, uncertainty_notes, source_id,
          external_source, external_record_id, internal_notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            brand_id,
            int(v["model_year"]),
            (v.get("market") or "US").strip(),
            v["series_name"].strip(),
            (v.get("variant_name") or "").strip() or None,
            (v.get("trim_line") or "").strip() or None,
            (v.get("body_style") or "").strip() or None,
            (v.get("engine") or "").strip() or None,
            (v.get("transmission") or "").strip() or None,
            (v.get("drivetrain") or "").strip() or None,
            (v.get("fuel_type") or "").strip() or None,
            (v.get("mpg_text") or "").strip() or None,
            (v.get("passenger_seating") or "").strip() or None,
            (v.get("uncertainty_notes") or "").strip() or None,
            src_id,
            ext_src,
            ext_id,
            (v.get("internal_notes") or "").strip() or None,
        ),
    )
    vehicle_id = int(cur.lastrowid)

    for i, pkg in enumerate(v.get("packages") or []):
        ps = pkg.get("source") or v["source"]
        pid_src = _resolve_source(cur, ps)
        cur.execute(
            """
            INSERT INTO ref_package (package_code, package_name, package_details, source_id)
            VALUES (?, ?, ?, ?)
            """,
            (
                (pkg.get("package_code") or "").strip() or None,
                pkg["package_name"].strip(),
                (pkg.get("package_details") or "").strip() or None,
                pid_src,
            ),
        )
        pid = int(cur.lastrowid)
        cur.execute(
            """
            INSERT INTO ref_vehicle_package (vehicle_id, package_id, sort_order)
            VALUES (?, ?, ?)
            """,
            (vehicle_id, pid, i),
        )

    for col in v.get("exterior_colors") or []:
        cs = col.get("source") or v["source"]
        cid_src = _resolve_source(cur, cs)
        cur.execute(
            "INSERT INTO ref_exterior_color (color_name, source_id) VALUES (?, ?)",
            (col["color_name"].strip(), cid_src),
        )
        eid = int(cur.lastrowid)
        cur.execute(
            "INSERT INTO ref_vehicle_exterior (vehicle_id, exterior_color_id) VALUES (?, ?)",
            (vehicle_id, eid),
        )

    for col in v.get("interior_colors") or []:
        cs = col.get("source") or v["source"]
        cid_src = _resolve_source(cur, cs)
        cur.execute(
            "INSERT INTO ref_interior_color (color_name, source_id) VALUES (?, ?)",
            (col["color_name"].strip(), cid_src),
        )
        iid = int(cur.lastrowid)
        cur.execute(
            "INSERT INTO ref_vehicle_interior (vehicle_id, interior_color_id) VALUES (?, ?)",
            (vehicle_id, iid),
        )

    if commit:
        conn.commit()
    return vehicle_id


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def ingest_seed_file(conn: sqlite3.Connection, path: Path, *, brand_code: str) -> tuple[int, int]:
    """Returns (coverage_lines_upserted, vehicles_inserted)."""
    data = load_json(path)
    cov = ingest_coverage_lines(conn, data, brand_code=brand_code) if data.get("coverage_lines") else 0
    vehicles = 0
    for v in data.get("vehicles", []):
        ingest_vehicle_bundle(conn, v, brand_code=brand_code)
        vehicles += 1
    return cov, vehicles
