"""
``model_generations``: (make, model) → generation code + model-year range.

Most real-world vehicle knowledge — known issues, engine families, "which
E350 is the turbo one" — is true of a *generation*, not a single year. This
table gives every reference store (and the listing assistant) that key, and is
the antidote to the year-collision failures of 2026-07-19.

Rows seeded here are curated from public model history and flagged
``status='estimated'`` until human-verified; ``generation_for()`` is the only
lookup the rest of the codebase should use.
"""
from __future__ import annotations

from functools import lru_cache
from typing import Any

from backend.db.inventory_db import get_conn

_DDL = """
CREATE TABLE IF NOT EXISTS model_generations (
    id          BIGSERIAL PRIMARY KEY,
    make        TEXT NOT NULL,
    model       TEXT NOT NULL,
    generation  TEXT NOT NULL,
    year_start  INTEGER NOT NULL,
    year_end    INTEGER,
    notes       TEXT,
    status      TEXT NOT NULL DEFAULT 'estimated',
    UNIQUE (make, model, generation)
)
"""

_DDL_SQLITE = _DDL.replace("BIGSERIAL PRIMARY KEY", "INTEGER PRIMARY KEY AUTOINCREMENT")

# (make, model-prefix, generation, start, end-or-None, note)
# Volume model lines in inventory as of 2026-07; year_end=None → current.
_SEED: list[tuple[str, str, str, int, int | None, str]] = [
    ("Toyota", "Camry", "XV50", 2012, 2017, "2.5 I4 / 3.5 V6"),
    ("Toyota", "Camry", "XV70", 2018, 2024, "2.5 I4 / 3.5 V6 / hybrid"),
    ("Toyota", "Camry", "XV80", 2025, None, "hybrid-only 2.5"),
    ("Toyota", "Tacoma", "N300 (3rd gen)", 2016, 2023, "3.5 V6 2GR-FKS"),
    ("Toyota", "Tacoma", "N400 (4th gen)", 2024, None, "2.4T i-FORCE / i-FORCE MAX hybrid"),
    ("Toyota", "RAV4", "XA40", 2013, 2018, "2.5 I4"),
    ("Toyota", "RAV4", "XA50", 2019, None, "2.5 I4 / hybrid / Prime PHEV"),
    ("Toyota", "Corolla", "E170", 2014, 2019, "1.8 I4"),
    ("Toyota", "Corolla", "E210", 2020, None, "1.8/2.0 I4 / hybrid"),
    ("Toyota", "Tundra", "XK50", 2007, 2021, "4.6/5.7 V8"),
    ("Toyota", "Tundra", "XK70", 2022, None, "3.4TT V6 / i-FORCE MAX hybrid"),
    ("Toyota", "4Runner", "N280 (5th gen)", 2010, 2024, "4.0 V6 1GR-FE"),
    ("Toyota", "4Runner", "N400 (6th gen)", 2025, None, "2.4T / hybrid"),
    ("Toyota", "Highlander", "XU50", 2014, 2019, "3.5 V6"),
    ("Toyota", "Highlander", "XU70", 2020, None, "2.4T / hybrid"),
    ("Toyota", "Grand Highlander", "1st gen", 2024, None, "2.4T / hybrid MAX"),
    ("Toyota", "Sienna", "XL30", 2011, 2020, "3.5 V6"),
    ("Toyota", "Sienna", "XL40", 2021, None, "hybrid-only 2.5"),
    ("Toyota", "Sequoia", "XK60", 2008, 2022, "5.7 V8"),
    ("Toyota", "Sequoia", "XK80", 2023, None, "3.4TT hybrid only"),
    ("Toyota", "Corolla Cross", "1st gen", 2022, None, "2.0 I4 / hybrid"),
    ("Toyota", "Prius", "XW50", 2016, 2022, "1.8 hybrid"),
    ("Toyota", "Prius", "XW60", 2023, None, "2.0 hybrid"),
    ("Ford", "F-150", "13th gen", 2015, 2020, "aluminum body; 2.7/3.5 EcoBoost, 5.0 V8"),
    ("Ford", "F-150", "14th gen", 2021, None, "PowerBoost hybrid available"),
    ("Ford", "Explorer", "U502", 2011, 2019, "FWD-based"),
    ("Ford", "Explorer", "U625", 2020, None, "RWD-based; 2.3T/3.0TT"),
    ("Ford", "Maverick", "1st gen", 2022, None, "2.5 hybrid / 2.0T"),
    ("Ford", "Bronco", "6th gen", 2021, None, "2.3T / 2.7TT"),
    ("Ford", "Bronco Sport", "1st gen", 2021, None, "1.5T / 2.0T"),
    ("Ford", "Escape", "4th gen", 2020, None, "1.5T / 2.0T / hybrid"),
    ("Ford", "Mustang", "S550", 2015, 2023, "2.3T / 5.0 V8"),
    ("Ford", "Mustang", "S650", 2024, None, "2.3T / 5.0 V8"),
    ("Ford", "Mustang Mach-E", "1st gen", 2021, None, "EV"),
    ("Chevrolet", "Silverado 1500", "K2XX", 2014, 2018, "4.3 V6 / 5.3 / 6.2 V8"),
    ("Chevrolet", "Silverado 1500", "T1XX", 2019, None, "2.7T / 5.3 / 6.2 / 3.0 diesel"),
    ("Chevrolet", "Equinox", "3rd gen", 2018, 2024, "1.5T"),
    ("Chevrolet", "Tahoe", "T1XX", 2021, None, "5.3 / 6.2 V8 / 3.0 diesel"),
    ("Honda", "CR-V", "5th gen", 2017, 2022, "1.5T"),
    ("Honda", "CR-V", "6th gen", 2023, None, "1.5T / hybrid"),
    ("Honda", "Civic", "10th gen", 2016, 2021, "2.0 / 1.5T"),
    ("Honda", "Civic", "11th gen", 2022, None, "2.0 / 1.5T / hybrid"),
    ("Honda", "Accord", "10th gen", 2018, 2022, "1.5T / 2.0T / hybrid"),
    ("Honda", "Accord", "11th gen", 2023, None, "1.5T / hybrid"),
    ("BMW", "X5", "F15", 2014, 2018, "N55 3.0T / 4.4TT V8"),
    ("BMW", "X5", "G05", 2019, None, "B58 3.0T / 4.4TT V8 / 45e-50e PHEV"),
    ("BMW", "X3", "G01", 2018, 2024, "B46 2.0T / B58 3.0T (M40i)"),
    ("BMW", "X3", "G45", 2025, None, "2.0T / 3.0T mild hybrid"),
    ("BMW", "3 Series", "F30", 2012, 2018, "N20/B46 2.0T, N55/B58 3.0T; NA era ended 2011"),
    ("BMW", "3 Series", "G20", 2019, None, "B46 2.0T / B58 3.0T"),
    ("BMW", "5 Series", "G30", 2017, 2023, "2.0T / 3.0T / 4.4TT"),
    ("BMW", "i4", "G26", 2022, None, "EV"),
    ("Mercedes-Benz", "C-Class", "W204", 2008, 2014, "NA V6 era (C300 3.0/3.5)"),
    ("Mercedes-Benz", "C-Class", "W205", 2015, 2021, "2.0T C300; 63 = 4.0TT from 2015"),
    ("Mercedes-Benz", "C-Class", "W206", 2022, None, "2.0T mild hybrid"),
    ("Mercedes-Benz", "E-Class", "W212", 2010, 2016, "NA V6 E350 3.5; turbo V6 E400"),
    ("Mercedes-Benz", "E-Class", "W213", 2017, 2023, "2.0T E300/E350; 3.0T E450"),
    ("Mercedes-Benz", "E-Class", "W214", 2024, None, "2.0T/3.0T mild hybrid"),
    ("Mercedes-Benz", "GLC", "X253", 2016, 2022, "2.0T GLC300"),
    ("Mercedes-Benz", "GLC", "X254", 2023, None, "2.0T mild hybrid"),
    ("Mercedes-Benz", "GLE", "W166", 2016, 2019, "3.5 V6 / 3.0TT"),
    ("Mercedes-Benz", "GLE", "V167", 2020, None, "2.0T / 3.0T mild hybrid"),
    ("Mercedes-Benz", "GLB", "X247", 2020, None, "2.0T"),
    ("Mercedes-Benz", "GLS", "X167", 2020, None, "3.0T / 4.0TT"),
    ("Mercedes-Benz", "Sprinter 2500", "VS30", 2019, None, "2.0T diesel / 3.0 V6 diesel"),
    ("Lexus", "RX", "AL20", 2016, 2022, "3.5 V6 RX350 / hybrid 450h"),
    ("Lexus", "RX", "AL30", 2023, None, "2.4T RX350 / hybrids"),
    ("Lexus", "ES", "XZ10", 2019, None, "2.5 / 3.5 V6 / hybrid"),
    ("Tesla", "Model 3", "1st gen", 2017, None, "EV; Highland refresh 2024"),
    ("Tesla", "Model Y", "1st gen", 2020, None, "EV; Juniper refresh 2025"),
    ("Hyundai", "Elantra", "AD", 2017, 2020, "2.0 I4"),
    ("Hyundai", "Elantra", "CN7", 2021, None, "2.0 / 1.6T / hybrid"),
    ("Hyundai", "IONIQ 5", "NE", 2022, None, "EV"),
    ("Mazda", "CX-5", "KF", 2017, None, "2.5 / 2.5T"),
    ("Audi", "Q5", "FY (2nd gen)", 2018, None, "2.0T / SQ5 3.0T"),
    ("Nissan", "Rogue", "T33", 2021, None, "1.5T VC-Turbo"),
    ("Nissan", "Altima", "L34", 2019, None, "2.5 / 2.0 VC-Turbo"),
    ("Jeep", "Wrangler", "JL", 2018, None, "2.0T / 3.6 V6 / 4xe PHEV"),
    ("Jeep", "Grand Cherokee", "WL", 2022, None, "2.0T 4xe / 3.6 V6 / 5.7 V8"),
    ("Subaru", "Outback", "6th gen", 2020, None, "2.5 / 2.4T"),
    ("Ram", "1500", "DT", 2019, None, "3.6 eTorque / 5.7 V8 / 3.0 Hurricane"),
]


def ensure_model_generations_table(conn=None) -> None:
    owns = conn is None
    if owns:
        conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(_DDL)
    except Exception:
        cur.execute(_DDL_SQLITE)
    if owns:
        conn.commit()
        conn.close()


def seed_model_generations(*, dry_run: bool = False) -> int:
    """Idempotent seed; returns number of rows inserted."""
    conn = get_conn()
    cur = conn.cursor()
    ensure_model_generations_table(conn)
    inserted = 0
    for make, model, gen, start, end, note in _SEED:
        cur.execute(
            "SELECT 1 FROM model_generations WHERE make=? AND model=? AND generation=?",
            (make, model, gen),
        )
        if cur.fetchone():
            continue
        inserted += 1
        if dry_run:
            continue
        cur.execute(
            "INSERT INTO model_generations (make, model, generation, year_start, year_end, notes, status) "
            "VALUES (?, ?, ?, ?, ?, ?, 'estimated')",
            (make, model, gen, start, end, note),
        )
    if not dry_run:
        conn.commit()
    conn.close()
    return inserted


@lru_cache(maxsize=4096)
def _generation_rows(make_l: str) -> tuple[tuple[str, str, int, int | None, str], ...]:
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT model, generation, year_start, year_end, COALESCE(notes,'') "
            "FROM model_generations WHERE lower(make)=?",
            (make_l,),
        )
        return tuple((str(m), str(g), int(ys), (int(ye) if ye is not None else None), str(n))
                     for m, g, ys, ye, n in cur.fetchall())
    except Exception:
        return ()
    finally:
        conn.close()


def clear_generation_cache() -> None:
    _generation_rows.cache_clear()


def generation_for(make: str | None, model: str | None, year: Any) -> dict[str, Any] | None:
    """{'generation', 'year_start', 'year_end', 'notes'} or None."""
    try:
        y = int(year)
    except (TypeError, ValueError):
        return None
    mk = (make or "").strip().lower()
    md = (model or "").strip().lower()
    if not mk or not md or not y:
        return None
    best: dict[str, Any] | None = None
    for m, gen, ys, ye, notes in _generation_rows(mk):
        ml = m.lower()
        # model row must prefix-match the listing model ("Camry" matches "Camry Hybrid")
        if not (md == ml or md.startswith(ml + " ") or ml.startswith(md + " ")):
            continue
        if y < ys or (ye is not None and y > ye):
            continue
        cand = {"generation": gen, "year_start": ys, "year_end": ye, "notes": notes}
        # Prefer the longest (most specific) model-name match
        if best is None or len(ml) > best.get("_mlen", 0):
            cand["_mlen"] = len(ml)
            best = cand
    if best:
        best.pop("_mlen", None)
    return best
