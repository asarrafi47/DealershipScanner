"""Command-line entrypoints: rebuild, EPA ingest, structured imports, export, QA."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from vehicle_reference.core.db import apply_schema, connect
from vehicle_reference.core.paths import REF_DATA_DIR, REF_DB_PATH, REF_SAMPLES_DIR, REF_SEEDS_DIR, ensure_ref_dirs
from vehicle_reference.csv_export.flat_export import CSV_COLUMNS, export_to_csv
from vehicle_reference.ingestion.bundle import ingest_seed_file
from vehicle_reference.ingestion.structured import ingest_csv_with_manifest_path, ingest_json_document
from vehicle_reference.quality.qa_report import write_qa_report
from vehicle_reference.quality.validate import run_validations
from vehicle_reference.sources.epa_bmw_ingest import default_year_range, ingest_epa_bmw_range


def _ensure_brand_bmw(conn) -> None:
    conn.execute(
        """
        INSERT INTO ref_brand (code, display_name)
        VALUES ('bmw', 'BMW')
        ON CONFLICT(code) DO NOTHING
        """
    )
    conn.commit()


def _bmw_brand_id(conn) -> int:
    row = conn.execute("SELECT id FROM ref_brand WHERE code = 'bmw'").fetchone()
    if not row:
        raise SystemExit("BMW brand row missing; run rebuild first.")
    return int(row[0])


def cmd_rebuild(db_path: Path) -> int:
    ensure_ref_dirs()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()
    conn = connect(db_path)
    apply_schema(conn)
    _ensure_brand_bmw(conn)
    total_cov = total_v = 0
    seed_files = sorted(REF_SEEDS_DIR.glob("*.json"))
    if not seed_files:
        print(f"No JSON seeds under {REF_SEEDS_DIR}", file=sys.stderr)
    for path in seed_files:
        cov, v = ingest_seed_file(conn, path, brand_code="bmw")
        total_cov += cov
        total_v += v
        print(f"Ingested {path.name}: coverage_lines={cov}, vehicles={v}")
    conn.close()
    print(f"Rebuild complete: {db_path} (coverage upserts={total_cov}, vehicles={total_v})")
    return 0


def cmd_ingest_epa(db_path: Path, y0: int, y1: int, sleep_s: float) -> int:
    conn = connect(db_path)
    ins, sk = ingest_epa_bmw_range(
        conn,
        brand_code="bmw",
        year_from=y0,
        year_to=y1,
        sleep_s=sleep_s,
        log=print,
    )
    conn.close()
    print(f"EPA ingest done: inserted={ins}, skipped_menu_or_filter={sk}")
    return 0


def cmd_ingest_json(db_path: Path, path: Path) -> int:
    conn = connect(db_path)
    n = ingest_json_document(conn, path, brand_code="bmw")
    conn.close()
    print(f"Ingested {n} vehicles from {path}")
    return 0


def cmd_ingest_csv(db_path: Path, csv_path: Path, manifest_path: Path) -> int:
    conn = connect(db_path)
    n = ingest_csv_with_manifest_path(conn, csv_path, manifest_path, brand_code="bmw")
    conn.close()
    print(f"Ingested {n} CSV rows from {csv_path}")
    return 0


def cmd_populate(db_path: Path, y0: int, y1: int, sleep_s: float) -> int:
    cmd_rebuild(db_path)
    return cmd_ingest_epa(db_path, y0, y1, sleep_s)


def cmd_validate(db_path: Path) -> int:
    conn = connect(db_path)
    issues = run_validations(conn)
    conn.close()
    if not issues:
        print("Validation OK: no issues reported.")
        return 0
    for i in issues:
        print(f"[{i.code}] {i.message}")
        if i.detail:
            print(i.detail)
    return 1


def cmd_qa_report(db_path: Path, out: Path) -> int:
    conn = connect(db_path)
    bid = _bmw_brand_id(conn)
    write_qa_report(conn, out, brand_id=bid)
    conn.close()
    print(f"Wrote QA report to {out}")
    return 0


def cmd_export(
    db_path: Path,
    out_path: Path,
    *,
    series_name: str | None,
    year_from: int | None,
    year_to: int | None,
    include_uncertainty: bool,
    complete_only: bool,
    qa_missing_options: bool,
) -> int:
    conn = connect(db_path)
    n = export_to_csv(
        conn,
        out_path,
        brand_code="bmw",
        series_name=series_name,
        year_from=year_from,
        year_to=year_to,
        include_uncertainty_column=include_uncertainty,
        complete_only=complete_only,
        qa_missing_options=qa_missing_options,
    )
    conn.close()
    scope = series_name or "all models"
    print(f"Wrote {n} rows to {out_path} ({scope})")
    return 0


def cmd_export_sample(db_path: Path) -> int:
    ensure_ref_dirs()
    out_path = REF_SAMPLES_DIR / "bmw_reference_template_sample.csv"
    return cmd_export(
        db_path,
        out_path,
        series_name=None,
        year_from=None,
        year_to=None,
        include_uncertainty=False,
        complete_only=False,
        qa_missing_options=False,
    )


def cmd_export_series_samples(db_path: Path) -> int:
    """Write sample CSVs for X2, 3 Series, and X3 under data/vehicle_reference/samples/."""
    ensure_ref_dirs()
    mapping = (
        ("bmw_x2_epa_sample.csv", "X2"),
        ("bmw_3series_epa_sample.csv", "3 Series"),
        ("bmw_x3_epa_sample.csv", "X3"),
    )
    conn = connect(db_path)
    for fname, series in mapping:
        p = REF_SAMPLES_DIR / fname
        export_to_csv(conn, p, brand_code="bmw", series_name=series)
        print(f"Wrote {p} ({series})")
    conn.close()
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Vehicle reference DB (BMW first).")
    p.add_argument("--db", type=Path, default=REF_DB_PATH, help="SQLite database path")
    sub = p.add_subparsers(dest="command", required=True)

    pr = sub.add_parser("rebuild", help="Drop DB, apply schema, ingest seeds/bmw/*.json")
    pr.set_defaults(func=lambda a: cmd_rebuild(a.db))

    pe = sub.add_parser("ingest-epa", help="Load U.S. EPA FuelEconomy.gov BMW rows (replaces prior EPA slice)")
    pe.add_argument("--year-from", type=int, default=None)
    pe.add_argument("--year-to", type=int, default=None)
    pe.add_argument("--sleep", type=float, default=0.1, help="Delay between EPA HTTP calls (seconds)")
    pe.set_defaults(
        func=lambda a: cmd_ingest_epa(
            a.db,
            a.year_from if a.year_from is not None else default_year_range()[0],
            a.year_to if a.year_to is not None else default_year_range()[1],
            a.sleep,
        )
    )

    pj = sub.add_parser("ingest-json", help="Import vehicles from JSON with source_manifest header")
    pj.add_argument("path", type=Path, help="Path to JSON document")
    pj.set_defaults(func=lambda a: cmd_ingest_json(a.db, a.path))

    pc = sub.add_parser("ingest-csv", help="Import vehicles from CSV + manifest JSON (column_map)")
    pc.add_argument("--csv", type=Path, required=True)
    pc.add_argument("--manifest", type=Path, required=True)
    pc.set_defaults(func=lambda a: cmd_ingest_csv(a.db, a.csv, a.manifest))

    pp = sub.add_parser("populate", help="rebuild + ingest-epa (default 2000–current year)")
    pp.add_argument("--year-from", type=int, default=None)
    pp.add_argument("--year-to", type=int, default=None)
    pp.add_argument("--sleep", type=float, default=0.1)
    pp.set_defaults(
        func=lambda a: cmd_populate(
            a.db,
            a.year_from if a.year_from is not None else default_year_range()[0],
            a.year_to if a.year_to is not None else default_year_range()[1],
            a.sleep,
        )
    )

    pv = sub.add_parser("validate", help="Run structural QA checks on the database")
    pv.set_defaults(func=lambda a: cmd_validate(a.db))

    pq = sub.add_parser("qa-report", help="Write gap / coverage markdown report")
    pq.add_argument(
        "--out",
        type=Path,
        default=REF_DATA_DIR / "reports" / "bmw_reference_qa.md",
        help="Output markdown path",
    )
    pq.set_defaults(func=lambda a: cmd_qa_report(a.db, a.out))

    pxs = sub.add_parser(
        "export-series-samples",
        help="Write X2, 3 Series, and X3 sample CSVs under data/vehicle_reference/samples/",
    )
    pxs.set_defaults(func=lambda a: cmd_export_series_samples(a.db))

    pe2 = sub.add_parser("export", help=f"Flatten to CSV ({', '.join(CSV_COLUMNS[:5])}...)")
    pe2.add_argument("--out", type=Path, required=True, help="Output CSV path")
    pe2.add_argument(
        "--model",
        "--series",
        dest="model",
        type=str,
        default=None,
        help='Filter ref_vehicle.series_name (e.g. "X2", "3 Series")',
    )
    pe2.add_argument("--year-from", type=int, default=None)
    pe2.add_argument("--year-to", type=int, default=None)
    pe2.add_argument(
        "--include-uncertainty",
        action="store_true",
        help="Append UncertaintyNotes column (not in stock template)",
    )
    pe2.add_argument(
        "--complete-only",
        action="store_true",
        help="Only rows with all core scalar specs (engine, trans, drive, fuel, mpg, body, trim)",
    )
    pe2.add_argument(
        "--qa-missing-options",
        action="store_true",
        help="Only rows missing exterior colors, interior colors, or packages (EPA QA)",
    )
    pe2.set_defaults(
        func=lambda a: cmd_export(
            a.db,
            a.out,
            series_name=a.model,
            year_from=a.year_from,
            year_to=a.year_to,
            include_uncertainty=a.include_uncertainty,
            complete_only=a.complete_only,
            qa_missing_options=a.qa_missing_options,
        )
    )

    ps = sub.add_parser(
        "export-sample",
        help=f"Write bundled sample to {REF_SAMPLES_DIR / 'bmw_reference_template_sample.csv'}",
    )
    ps.set_defaults(func=lambda a: cmd_export_sample(a.db))

    pa = sub.add_parser(
        "export-all",
        help=f"Export all BMW rows (default: {REF_DATA_DIR / 'bmw_export_all.csv'})",
    )
    pa.add_argument(
        "--out",
        type=Path,
        default=REF_DATA_DIR / "bmw_export_all.csv",
        help="Output CSV path",
    )
    pa.add_argument("--include-uncertainty", action="store_true")
    pa.add_argument("--complete-only", action="store_true")
    pa.add_argument("--qa-missing-options", action="store_true")
    pa.set_defaults(
        func=lambda a: cmd_export(
            a.db,
            a.out,
            series_name=None,
            year_from=None,
            year_to=None,
            include_uncertainty=a.include_uncertainty,
            complete_only=a.complete_only,
            qa_missing_options=a.qa_missing_options,
        )
    )

    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
