"""Gap analysis vs target BMW lines (EPA-backed rows + optional brochure data)."""
from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path

from vehicle_reference.sources.epa_bmw_ingest import TARGET_BASE_MODELS

TARGET_SERIES = sorted(TARGET_BASE_MODELS)


def _year_range(conn: sqlite3.Connection, brand_id: int) -> tuple[int | None, int | None]:
    row = conn.execute(
        "SELECT MIN(model_year), MAX(model_year) FROM ref_vehicle WHERE brand_id = ?",
        (brand_id,),
    ).fetchone()
    return (row[0], row[1])


def build_qa_report(conn: sqlite3.Connection, *, brand_id: int) -> str:
    y_min, y_max = _year_range(conn, brand_id)
    lines: list[str] = []
    lines.append("# BMW vehicle reference QA report")
    lines.append("")
    lines.append(f"- Generated: {datetime.now().isoformat(timespec='seconds')}")
    lines.append(f"- Vehicle year span in DB: {y_min} – {y_max}")
    lines.append("")
    lines.append("## Rows missing brochure-style options (EPA-only is expected)")
    lines.append("")
    lines.append(
        "Counts of vehicles with **no** linked exterior colors, interior colors, "
        "or factory packages (EPA ingest does not provide these)."
    )
    lines.append("")

    gap = conn.execute(
        """
        SELECT v.series_name, COUNT(*)
        FROM ref_vehicle v
        WHERE v.brand_id = ?
          AND NOT EXISTS (SELECT 1 FROM ref_vehicle_exterior e WHERE e.vehicle_id = v.id)
          AND NOT EXISTS (SELECT 1 FROM ref_vehicle_interior i WHERE i.vehicle_id = v.id)
          AND NOT EXISTS (SELECT 1 FROM ref_vehicle_package p WHERE p.vehicle_id = v.id)
        GROUP BY 1
        ORDER BY 1
        """,
        (brand_id,),
    ).fetchall()
    for s, c in gap:
        lines.append(f"- **{s}**: {c} vehicles")
    lines.append("")

    lines.append("## Target series × model years with **zero** vehicles")
    lines.append("")
    if y_min is None:
        lines.append("_No vehicles in database._")
    else:
        years = list(range(y_min, y_max + 1))
        for series in TARGET_SERIES:
            missing_years: list[int] = []
            for y in years:
                n = conn.execute(
                    """
                    SELECT COUNT(*) FROM ref_vehicle
                    WHERE brand_id = ? AND model_year = ? AND series_name = ?
                    """,
                    (brand_id, y, series),
                ).fetchone()[0]
                if n == 0:
                    missing_years.append(y)
            if missing_years:
                sample = missing_years[:15]
                more = f" (+{len(missing_years) - 15} more)" if len(missing_years) > 15 else ""
                lines.append(
                    f"- **{series}**: no rows for {len(missing_years)} years in span "
                    f"(e.g. {sample}{more})"
                )
    lines.append("")
    lines.append("## EPA external id coverage")
    lines.append("")
    epa_n = conn.execute(
        """
        SELECT COUNT(*) FROM ref_vehicle
        WHERE brand_id = ? AND external_source = 'epa_fueleconomy'
        """,
        (brand_id,),
    ).fetchone()[0]
    other_n = conn.execute(
        "SELECT COUNT(*) FROM ref_vehicle WHERE brand_id = ?",
        (brand_id,),
    ).fetchone()[0] - epa_n
    lines.append(f"- Rows with `external_source=epa_fueleconomy`: **{epa_n}**")
    lines.append(f"- Other rows (brochure/manual JSON/CSV): **{other_n}**")
    lines.append("")
    return "\n".join(lines)


def write_qa_report(conn: sqlite3.Connection, out_path: Path, *, brand_id: int) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(build_qa_report(conn, brand_id=brand_id), encoding="utf-8")
