"""Dictionary schema helpers (EPA + Complete_Options in inventory DB)."""

from __future__ import annotations

from typing import Any


def ensure_dictionary_tables(cur: Any, *, postgres: bool = False) -> None:
    if postgres:
        from backend.db.inventory_pg import pg_add_columns

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS dictionary_options (
                id BIGSERIAL PRIMARY KEY,
                year INTEGER,
                make TEXT,
                model TEXT,
                trim TEXT,
                engine_options TEXT,
                engine_display TEXT,
                forced_induction TEXT,
                transmission TEXT,
                drivetrain TEXT,
                fuel_type TEXT,
                body_style TEXT,
                cylinders INTEGER,
                displacement DOUBLE PRECISION,
                mpg_city DOUBLE PRECISION,
                mpg_highway DOUBLE PRECISION,
                mpg_combined DOUBLE PRECISION,
                exterior_colors TEXT,
                packages TEXT,
                package_details TEXT,
                options TEXT,
                option_details TEXT
            )
            """
        )
        pg_add_columns(
            cur,
            "epa_master",
            [
                ("trim", "TEXT"),
                ("body_style", "TEXT"),
                ("engine_description", "TEXT"),
                ("engine_display", "TEXT"),
                ("forced_induction", "TEXT"),
                ("city08", "DOUBLE PRECISION"),
                ("highway08", "DOUBLE PRECISION"),
                ("city_e", "DOUBLE PRECISION"),
                ("highway_e", "DOUBLE PRECISION"),
                ("atv_type", "TEXT"),
            ],
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_dict_options_lookup "
            "ON dictionary_options(year, make, model)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_dict_options_trim "
            "ON dictionary_options(year, make, model, trim)"
        )
        return

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS dictionary_options (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER,
            make TEXT,
            model TEXT,
            trim TEXT,
            engine_options TEXT,
            engine_display TEXT,
            forced_induction TEXT,
            transmission TEXT,
            drivetrain TEXT,
            fuel_type TEXT,
            body_style TEXT,
            cylinders INTEGER,
            displacement REAL,
            mpg_city REAL,
            mpg_highway REAL,
            mpg_combined REAL,
            exterior_colors TEXT,
            packages TEXT,
            package_details TEXT,
            options TEXT,
            option_details TEXT
        )
        """
    )
    cur.execute("PRAGMA table_info(epa_master)")
    epa_cols = {row[1] for row in cur.fetchall()}
    for col, ctype in [
        ("trim", "TEXT"),
        ("body_style", "TEXT"),
        ("engine_description", "TEXT"),
        ("engine_display", "TEXT"),
        ("forced_induction", "TEXT"),
        ("city08", "REAL"),
        ("highway08", "REAL"),
        ("city_e", "REAL"),
        ("highway_e", "REAL"),
        ("atv_type", "TEXT"),
    ]:
        if col not in epa_cols:
            try:
                cur.execute(f"ALTER TABLE epa_master ADD COLUMN {col} {ctype}")
            except Exception:
                pass
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_dict_options_lookup "
        "ON dictionary_options(year, make, model)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_dict_options_trim "
        "ON dictionary_options(year, make, model, trim)"
    )
