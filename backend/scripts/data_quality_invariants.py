#!/usr/bin/env python3
"""
Data-quality invariants: a standing check that the spec pipeline has not started
producing nonsense.

WHY THIS EXISTS
---------------
Every defect this file encodes actually shipped to shoppers and was found months
later, by hand, by a person reading a car page. A 2011 Grand Cherokee Laredo was
given the HEMI's 360 hp. Half a Wikipedia sentence rendered as a specification.
Thousands of cars were told they were a trim they are not. A towing figure was
parsed into the curb-weight column on 7,767 catalog rows. Nothing in the system
noticed any of it. Each one is a cheap assertion, and this is that suite.

WHAT IT IS NOT
--------------
It is not a data repair tool and it never becomes one. It issues ``SET SESSION
CHARACTERISTICS AS TRANSACTION READ ONLY`` before anything else, so an UPDATE
from this script raises ``ReadOnlySqlTransaction`` from the server rather than
being a review question. It reports; a human fixes.

THE THRESHOLD MODEL
-------------------
Several of these invariants are violated *today* by a known backlog. A raw count
is therefore not a usable signal — 7,767 known-bad rows would drown out the one
new row that means a scraper broke last night. So every invariant carries a
BASELINE count in a checked-in JSON file, and the script fails only when the
count exceeds its baseline. The question it answers is "did this get worse",
not "is this nonzero".

An invariant with NO baseline entry fails the run. That is deliberate: adding an
invariant must force a human to look at its current count and record it, rather
than letting a new check quietly pass because nobody registered it.

TWO TIERS
---------
``stored``  — SQL over ``epa_extended_specs`` / ``cars``. Seconds. What is in the
              database.
``rendered``— runs the real production render path (``merge_verified_specs`` +
              ``serialize_car_for_api``) on one representative car per distinct
              active (year, make, model, trim) cohort, and asserts on the dict
              the car page actually receives. Minutes.

The two tiers disagree on purpose. The stored table holds a nameplate-wide
``battery_kwh`` on every 7 Series; the render path suppresses it. Only the
rendered tier can tell you whether a shopper sees a thing, and only the stored
tier can tell you whether the pipeline is still writing it. A regression in
either is a regression.

MEASURED RUNTIME (2026-08-02, live inventory: 90,423 cars, 73,389 active,
49,912 ``epa_extended_specs`` rows, 14,606 distinct active cohorts)
    stored tier alone   2.5 - 2.8 s
    stored + rendered   617 s  (10.3 min)
Fine for a nightly job. ``--sql-only`` is the fast mode; it is NOT a substitute,
because it checks nothing a shopper actually sees.

USAGE
-----
    python -m backend.scripts.data_quality_invariants                 # full run
    python -m backend.scripts.data_quality_invariants --sql-only      # CI gate, seconds
    python -m backend.scripts.data_quality_invariants --json out.json
    python -m backend.scripts.data_quality_invariants --write-baseline

EXIT CODES
----------
    0  every invariant at or below its baseline
    1  at least one invariant above baseline, or missing a baseline entry
    2  the run itself failed (DB unreachable, import error) — never confused
       with "clean", because a check that did not run is not a check that passed
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

DEFAULT_BASELINE_PATH = Path(__file__).with_name("data_quality_invariants_baseline.json")
DICTIONARY_DIR = REPO_ROOT / "backend" / "dictionary"

#: Fields on the serialized car dict that a shopper reads as a specification.
SHOPPER_SPEC_FIELDS = (
    "horsepower",
    "torque_lb_ft",
    "curb_weight_lb",
    "zero_to_60_sec",
    "tow_capacity_lb",
    "fuel_tank_gal",
    "ev_range_miles",
    "battery_kwh",
)

#: Numeric spec columns of ``epa_extended_specs`` that the nameplate-constant
#: signature is meaningful for. Each is a figure that DIFFERS between trims of
#: the same nameplate in every real vehicle line, which is the whole point.
NAMEPLATE_VARYING_COLUMNS = (
    "battery_kwh",
    "curb_weight_lb",
    "zero_to_60_sec",
    "tow_capacity_lb",
    "fuel_tank_gal",
    "horsepower",
    "torque_lb_ft",
    "ev_range_miles",
)

#: A nameplate needs this many rows and this many distinct trims before "one
#: distinct value" means anything. Two rows sharing a number is a coincidence;
#: 211 rows across every trim and model year is a stamped constant.
NAMEPLATE_MIN_ROWS = 8
NAMEPLATE_MIN_TRIMS = 2


# --------------------------------------------------------------------------
# Result plumbing
# --------------------------------------------------------------------------


@dataclass
class InvariantResult:
    id: str
    title: str
    detects: str
    impossible_because: str
    tier: str
    count: int
    denominator: int | None = None
    examples: list[Any] = field(default_factory=list)
    elapsed_sec: float = 0.0
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "title": self.title,
            "detects": self.detects,
            "impossible_because": self.impossible_because,
            "tier": self.tier,
            "count": self.count,
            "denominator": self.denominator,
            "examples": self.examples[:20],
            "elapsed_sec": round(self.elapsed_sec, 3),
            "error": self.error,
        }


@dataclass
class SqlInvariant:
    id: str
    title: str
    detects: str
    impossible_because: str
    count_sql: str
    example_sql: str | None = None
    denominator_sql: str | None = None


# --------------------------------------------------------------------------
# STORED-TIER INVARIANTS  (SQL; what the pipeline wrote)
# --------------------------------------------------------------------------

SQL_INVARIANTS: tuple[SqlInvariant, ...] = (
    SqlInvariant(
        id="curb_equals_tow",
        title="curb weight identical to tow rating",
        detects="a towing figure parsed into the curb-weight column of epa_extended_specs",
        impossible_because=(
            "empty weight and maximum trailer weight are two unrelated measurements "
            "taken by different tests; exact equality is a column collision, not a vehicle"
        ),
        count_sql="""
            SELECT COUNT(*) FROM epa_extended_specs
            WHERE curb_weight_lb IS NOT NULL AND curb_weight_lb = tow_capacity_lb
        """,
        example_sql="""
            SELECT year, make, model, trim, curb_weight_lb, tow_capacity_lb
            FROM epa_extended_specs
            WHERE curb_weight_lb IS NOT NULL AND curb_weight_lb = tow_capacity_lb
            ORDER BY year DESC, make, model LIMIT 20
        """,
        denominator_sql="SELECT COUNT(*) FROM epa_extended_specs WHERE curb_weight_lb IS NOT NULL",
    ),
    SqlInvariant(
        id="curb_over_light_duty_ceiling",
        title="curb weight over 8,000 lb on a light-duty vehicle",
        detects="the same mis-parsed towing figure, caught by magnitude instead of collision",
        impossible_because=(
            "no light-duty passenger vehicle weighs four tons empty; the make/model "
            "families that legitimately do (Ram 2500/3500, F-250+, chassis cabs) are "
            "excluded by the heavy-duty pattern below"
        ),
        count_sql="""
            SELECT COUNT(*) FROM epa_extended_specs
            WHERE curb_weight_lb > 8000
              AND NOT (
                    model ~* '(2500|3500|4500|5500|F-?[23456]50|Super ?Duty|Sierra ?[23]500|Silverado ?[23]500|Transit|Savana|Express|E-?[34]50|Hummer)'
              )
        """,
        example_sql="""
            SELECT year, make, model, trim, curb_weight_lb
            FROM epa_extended_specs
            WHERE curb_weight_lb > 8000
              AND NOT (model ~* '(2500|3500|4500|5500|F-?[23456]50|Super ?Duty|Sierra ?[23]500|Silverado ?[23]500|Transit|Savana|Express|E-?[34]50|Hummer)')
            ORDER BY curb_weight_lb DESC LIMIT 20
        """,
        denominator_sql="SELECT COUNT(*) FROM epa_extended_specs WHERE curb_weight_lb IS NOT NULL",
    ),
    SqlInvariant(
        id="curb_kg_lb_disagree",
        title="curb weight in kg and lb disagree by more than 5%",
        detects="one of the two unit columns was written from a different source row than the other",
        impossible_because=(
            "kg and lb are the same fact in two units; they cannot describe two "
            "different vehicles, so any drift beyond rounding means one was overwritten"
        ),
        count_sql="""
            SELECT COUNT(*) FROM epa_extended_specs
            WHERE curb_weight_lb IS NOT NULL AND curb_weight_kg IS NOT NULL
              AND ABS(curb_weight_kg * 2.20462 - curb_weight_lb) > 0.05 * curb_weight_lb
        """,
        example_sql="""
            SELECT year, make, model, trim, curb_weight_lb, curb_weight_kg,
                   ROUND((curb_weight_kg * 2.20462)::numeric, 0) AS kg_as_lb
            FROM epa_extended_specs
            WHERE curb_weight_lb IS NOT NULL AND curb_weight_kg IS NOT NULL
              AND ABS(curb_weight_kg * 2.20462 - curb_weight_lb) > 0.05 * curb_weight_lb
            LIMIT 20
        """,
        denominator_sql="""
            SELECT COUNT(*) FROM epa_extended_specs
            WHERE curb_weight_lb IS NOT NULL AND curb_weight_kg IS NOT NULL
        """,
    ),
    SqlInvariant(
        id="zero_to_60_sub_four_no_badge",
        title="sub-4.0s 0-60 with no performance badge in the trim",
        detects="a halo trim's acceleration figure stamped onto the base cars of the nameplate",
        impossible_because=(
            "a sub-4-second car is always badged or electric; a Camry LE, an Accord "
            "I4 CVT and a 330i do not reach 60 in under four seconds"
        ),
        count_sql="""
            SELECT COUNT(*) FROM epa_extended_specs
            WHERE zero_to_60_sec IS NOT NULL AND zero_to_60_sec < 4.0
              AND NOT (COALESCE(trim,'') || ' ' || COALESCE(model,'') ~*
                   '(AMG|\\mM[2-8]\\M|Competition|\\bRS\\b|\\bGT[3RS]?\\b|Type ?R|\\bSRT\\b|Hellcat|TRX|Trackhawk|Raptor|Shelby|\\bZL1\\b|\\bZ06\\b|\\bZR1\\b|Blackwing|\\bV-?Series\\b|Redeye|Demon|Turbo ?S|Quadrifoglio|Nismo|\\bSTI\\b|Plaid|Performance|\\bN\\b Line|Polestar)')
              AND NOT (COALESCE(make,'') ~* '(Ferrari|Lamborghini|McLaren|Bugatti|Rimac|Koenigsegg|Lotus|Porsche|Tesla|Lucid|Rivian|Polestar)')
        """,
        example_sql="""
            SELECT year, make, model, trim, zero_to_60_sec
            FROM epa_extended_specs
            WHERE zero_to_60_sec IS NOT NULL AND zero_to_60_sec < 4.0
              AND NOT (COALESCE(trim,'') || ' ' || COALESCE(model,'') ~*
                   '(AMG|\\mM[2-8]\\M|Competition|\\bRS\\b|\\bGT[3RS]?\\b|Type ?R|\\bSRT\\b|Hellcat|TRX|Trackhawk|Raptor|Shelby|\\bZL1\\b|\\bZ06\\b|\\bZR1\\b|Blackwing|\\bV-?Series\\b|Redeye|Demon|Turbo ?S|Quadrifoglio|Nismo|\\bSTI\\b|Plaid|Performance|\\bN\\b Line|Polestar)')
              AND NOT (COALESCE(make,'') ~* '(Ferrari|Lamborghini|McLaren|Bugatti|Rimac|Koenigsegg|Lotus|Porsche|Tesla|Lucid|Rivian|Polestar)')
            ORDER BY zero_to_60_sec LIMIT 20
        """,
        denominator_sql="SELECT COUNT(*) FROM epa_extended_specs WHERE zero_to_60_sec IS NOT NULL",
    ),
    SqlInvariant(
        id="car_trim_not_named_by_listing",
        title="a car claims a trim its own listing text never names",
        detects=(
            "a trim assigned by a matcher rather than read off the listing — the "
            "mechanism that told 6,226 cars they were a trim they are not"
        ),
        impossible_because=(
            "a dealer who is selling a Laredo says Laredo somewhere: in the title, "
            "the raw model string or the VDP URL. A trim that appears in none of "
            "the three was inferred, not observed"
        ),
        count_sql="",  # computed in Python, see _invariant_trim_not_named
    ),
    SqlInvariant(
        id="epa_link_year_mismatch",
        title="cars.epa_master_id points at a different model year",
        detects="a catalog resolver link that landed on the wrong vehicle",
        impossible_because=(
            "the link exists to say 'this listing IS this catalog vehicle'; a 2019 "
            "listing bound to a 2015 catalog row inherits four-year-old specs, and "
            "every read-time join then serves them as fact"
        ),
        count_sql="""
            SELECT COUNT(*) FROM cars c JOIN epa_master e ON e.id = c.epa_master_id
            WHERE c.listing_active = 1 AND c.year IS NOT NULL AND e.year IS NOT NULL
              AND c.year <> e.year
        """,
        example_sql="""
            SELECT c.id, c.year, c.make, c.model, c.trim, e.year AS epa_year, e.model AS epa_model,
                   c.epa_match_method
            FROM cars c JOIN epa_master e ON e.id = c.epa_master_id
            WHERE c.listing_active = 1 AND c.year IS NOT NULL AND e.year IS NOT NULL
              AND c.year <> e.year
            LIMIT 20
        """,
        denominator_sql="SELECT COUNT(*) FROM cars WHERE listing_active = 1 AND epa_master_id IS NOT NULL",
    ),
    SqlInvariant(
        id="epa_link_make_mismatch",
        title="cars.epa_master_id points at a different make",
        detects="a catalog resolver link that crossed manufacturers entirely",
        impossible_because="a Honda listing is never the same vehicle as a Hyundai catalog row",
        count_sql="""
            SELECT COUNT(*) FROM cars c JOIN epa_master e ON e.id = c.epa_master_id
            WHERE c.listing_active = 1
              AND LOWER(BTRIM(c.make)) <> LOWER(BTRIM(e.make))
        """,
        example_sql="""
            SELECT c.id, c.year, c.make, c.model, e.make AS epa_make, e.model AS epa_model
            FROM cars c JOIN epa_master e ON e.id = c.epa_master_id
            WHERE c.listing_active = 1 AND LOWER(BTRIM(c.make)) <> LOWER(BTRIM(e.make))
            LIMIT 20
        """,
        denominator_sql="SELECT COUNT(*) FROM cars WHERE listing_active = 1 AND epa_master_id IS NOT NULL",
    ),
    SqlInvariant(
        id="epa_link_dangling",
        title="cars.epa_master_id points at a row that no longer exists",
        detects="a catalog rebuild that renumbered epa_master without re-resolving the links",
        impossible_because=(
            "the link is a foreign key in intent; a dangling one silently degrades "
            "every read-time spec join to 'no data' with no error anywhere"
        ),
        count_sql="""
            SELECT COUNT(*) FROM cars c LEFT JOIN epa_master e ON e.id = c.epa_master_id
            WHERE c.listing_active = 1 AND c.epa_master_id IS NOT NULL AND e.id IS NULL
        """,
        example_sql="""
            SELECT c.id, c.year, c.make, c.model, c.epa_master_id
            FROM cars c LEFT JOIN epa_master e ON e.id = c.epa_master_id
            WHERE c.listing_active = 1 AND c.epa_master_id IS NOT NULL AND e.id IS NULL
            LIMIT 20
        """,
        denominator_sql="SELECT COUNT(*) FROM cars WHERE listing_active = 1 AND epa_master_id IS NOT NULL",
    ),
    SqlInvariant(
        id="electric_car_with_cylinders",
        title="an electric car with a non-zero cylinder count",
        detects="a gas trim's engine record merged onto the EV trim of the same nameplate",
        impossible_because="a battery-electric vehicle has no cylinders",
        count_sql="""
            SELECT COUNT(*) FROM cars
            WHERE listing_active = 1 AND LOWER(BTRIM(fuel_type)) IN ('electric','ev')
              AND cylinders IS NOT NULL AND cylinders > 0
        """,
        example_sql="""
            SELECT id, year, make, model, trim, fuel_type, cylinders, engine_description
            FROM cars
            WHERE listing_active = 1 AND LOWER(BTRIM(fuel_type)) IN ('electric','ev')
              AND cylinders IS NOT NULL AND cylinders > 0
            LIMIT 20
        """,
        denominator_sql="""
            SELECT COUNT(*) FROM cars
            WHERE listing_active = 1 AND LOWER(BTRIM(fuel_type)) IN ('electric','ev')
        """,
    ),
    SqlInvariant(
        id="cylinders_not_a_real_layout",
        title="a cylinder count no engine has been built with",
        detects="a number parsed out of the wrong part of an engine string",
        impossible_because=(
            "production engines come in 0 (electric), 2, 3, 4, 5, 6, 8, 10, 12 and "
            "16 cylinders; 7 and 9 are not manufactured"
        ),
        count_sql="""
            SELECT COUNT(*) FROM cars
            WHERE listing_active = 1 AND cylinders IS NOT NULL
              AND cylinders NOT IN (0,2,3,4,5,6,8,10,12,16)
        """,
        example_sql="""
            SELECT id, year, make, model, cylinders, engine_description
            FROM cars
            WHERE listing_active = 1 AND cylinders IS NOT NULL
              AND cylinders NOT IN (0,2,3,4,5,6,8,10,12,16)
            LIMIT 20
        """,
        denominator_sql="SELECT COUNT(*) FROM cars WHERE listing_active = 1 AND cylinders IS NOT NULL",
    ),
    SqlInvariant(
        id="mpg_out_of_physical_range",
        title="an EPA mpg figure outside 5-150",
        detects="an mpg column fed a price, a range, a kWh/100mi or a stray integer",
        impossible_because=(
            "no EPA-rated vehicle sold in the US is under 5 mpg or over 150 mpg; "
            "MPGe figures do not belong in the mpg columns"
        ),
        count_sql="""
            SELECT COUNT(*) FROM cars
            WHERE listing_active = 1 AND (
                  (mpg_city IS NOT NULL AND (mpg_city < 5 OR mpg_city > 150))
               OR (mpg_highway IS NOT NULL AND (mpg_highway < 5 OR mpg_highway > 150)))
        """,
        example_sql="""
            SELECT id, year, make, model, mpg_city, mpg_highway, fuel_type
            FROM cars
            WHERE listing_active = 1 AND (
                  (mpg_city IS NOT NULL AND (mpg_city < 5 OR mpg_city > 150))
               OR (mpg_highway IS NOT NULL AND (mpg_highway < 5 OR mpg_highway > 150)))
            LIMIT 20
        """,
        denominator_sql="""
            SELECT COUNT(*) FROM cars
            WHERE listing_active = 1 AND (mpg_city IS NOT NULL OR mpg_highway IS NOT NULL)
        """,
    ),
    SqlInvariant(
        id="price_out_of_range",
        title="an asking price at or below zero, or above $2,000,000",
        detects="a price field fed a payment, a discount, a zero placeholder or a VIN fragment",
        impossible_because="a listed retail vehicle in this inventory is neither free nor a supercar auction",
        count_sql="""
            SELECT COUNT(*) FROM cars
            WHERE listing_active = 1 AND price IS NOT NULL AND (price <= 0 OR price > 2000000)
        """,
        example_sql="""
            SELECT id, year, make, model, price, dealer_name
            FROM cars
            WHERE listing_active = 1 AND price IS NOT NULL AND (price <= 0 OR price > 2000000)
            LIMIT 20
        """,
        denominator_sql="SELECT COUNT(*) FROM cars WHERE listing_active = 1 AND price IS NOT NULL",
    ),
    SqlInvariant(
        id="mileage_out_of_range",
        title="an odometer reading below zero or above 600,000",
        detects="a mileage column fed a price, a stock number or a negative sentinel",
        impossible_because="a retail listing with a negative or seven-figure odometer is a parse, not a car",
        count_sql="""
            SELECT COUNT(*) FROM cars
            WHERE listing_active = 1 AND mileage IS NOT NULL AND (mileage < 0 OR mileage > 600000)
        """,
        example_sql="""
            SELECT id, year, make, model, mileage, condition
            FROM cars
            WHERE listing_active = 1 AND mileage IS NOT NULL AND (mileage < 0 OR mileage > 600000)
            LIMIT 20
        """,
        denominator_sql="SELECT COUNT(*) FROM cars WHERE listing_active = 1 AND mileage IS NOT NULL",
    ),
    SqlInvariant(
        id="msrp_far_below_price",
        title="MSRP less than half the asking price",
        detects="an MSRP column holding a different number entirely (a payment, a fee, a rebate)",
        impossible_because=(
            "a dealer may ask over sticker, but not double it; the gap is a "
            "different quantity in the field, and it feeds the discount badge"
        ),
        count_sql="""
            SELECT COUNT(*) FROM cars
            WHERE listing_active = 1 AND msrp IS NOT NULL AND price IS NOT NULL
              AND price > 0 AND msrp > 0 AND msrp < price * 0.5
        """,
        example_sql="""
            SELECT id, year, make, model, price, msrp
            FROM cars
            WHERE listing_active = 1 AND msrp IS NOT NULL AND price IS NOT NULL
              AND price > 0 AND msrp > 0 AND msrp < price * 0.5
            LIMIT 20
        """,
        denominator_sql="""
            SELECT COUNT(*) FROM cars
            WHERE listing_active = 1 AND msrp IS NOT NULL AND price IS NOT NULL AND price > 0
        """,
    ),
)


# --------------------------------------------------------------------------
# Read-only connection
# --------------------------------------------------------------------------


def _inventory_url() -> str:
    url = os.environ.get("INVENTORY_DATABASE_URL")
    if url:
        return url.strip().strip("'\"")
    env_path = REPO_ROOT / ".env"
    if env_path.exists():
        m = re.search(r"^INVENTORY_DATABASE_URL=(.+)$", env_path.read_text(), re.M)
        if m:
            return m.group(1).strip().strip("'\"")
    raise RuntimeError("INVENTORY_DATABASE_URL is not set and is not in .env")


def open_readonly_connection():
    """
    A psycopg connection on which every transaction is READ ONLY.

    This is the enforcement, not a convention: an UPDATE issued from this script
    raises ``ReadOnlySqlTransaction`` from the server. The invariant suite must
    never be a way to "fix" data, and this makes that structural.
    """
    import psycopg

    conn = psycopg.connect(_inventory_url())
    with conn.cursor() as cur:
        cur.execute("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
    conn.commit()
    return conn


# --------------------------------------------------------------------------
# Stored tier
# --------------------------------------------------------------------------


def _scalar(cur, sql: str) -> int:
    cur.execute(sql)
    row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _rows(cur, sql: str) -> list[dict[str, Any]]:
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


_TRIM_SPLIT_RE = re.compile(r"[/,;+&|]| or ", re.IGNORECASE)


def _norm_words(s: Any) -> str:
    return " " + re.sub(r"[^a-z0-9]+", " ", str(s or "").lower()).strip() + " "


def _invariant_trim_not_named(cur, spec: SqlInvariant) -> InvariantResult:
    """
    A car whose ``trim`` appears in none of its own listing text.

    Checked against title, model_full_raw, description and source_url. A trim
    written as a slash list ("Latitude/North") passes if ANY component is named,
    so a dealer's own multi-trim label is not counted against the pipeline.
    """
    t0 = time.time()
    cur.execute(
        """
        SELECT id, year, make, model, trim, title, model_full_raw, description, source_url
        FROM cars
        WHERE listing_active = 1 AND trim IS NOT NULL AND BTRIM(trim) <> ''
        """
    )
    bad: list[dict[str, Any]] = []
    total = 0
    for cid, yr, mk, md, trim, title, mfr, desc, surl in cur:
        total += 1
        blob = _norm_words(" ".join(str(x or "") for x in (title, mfr, desc, surl)))
        parts = [p for p in _TRIM_SPLIT_RE.split(str(trim)) if p.strip()]
        named = False
        for part in parts:
            token = _norm_words(part).strip()
            if token and f" {token} " in blob:
                named = True
                break
        if not named:
            bad.append(
                {"id": cid, "year": yr, "make": mk, "model": md, "trim": trim, "title": title}
            )
    return InvariantResult(
        id=spec.id,
        title=spec.title,
        detects=spec.detects,
        impossible_because=spec.impossible_because,
        tier="stored",
        count=len(bad),
        denominator=total,
        examples=bad[:20],
        elapsed_sec=time.time() - t0,
    )


def _invariant_nameplate_constant(cur) -> list[InvariantResult]:
    """
    One distinct value for a spec stamped across every trim and year of a nameplate.

    This is the model-level contamination signature. It is reported per column
    because the columns fail independently and a regression in one must not be
    hidden by the backlog in another.
    """
    out: list[InvariantResult] = []
    for col in NAMEPLATE_VARYING_COLUMNS:
        t0 = time.time()
        sql = f"""
            SELECT make, model, COUNT(*) AS rows_n, MIN({col}) AS stamped_value,
                   COUNT(DISTINCT COALESCE(trim,'')) AS trims_n,
                   COUNT(DISTINCT year) AS years_n
            FROM epa_extended_specs
            WHERE {col} IS NOT NULL
            GROUP BY make, model
            HAVING COUNT(*) >= {NAMEPLATE_MIN_ROWS}
               AND COUNT(DISTINCT {col}) = 1
               AND COUNT(DISTINCT COALESCE(trim,'')) >= {NAMEPLATE_MIN_TRIMS}
            ORDER BY COUNT(*) DESC
        """
        hits = _rows(cur, sql)
        denom = _scalar(
            cur,
            f"""SELECT COUNT(*) FROM (
                    SELECT 1 FROM epa_extended_specs WHERE {col} IS NOT NULL
                    GROUP BY make, model
                    HAVING COUNT(*) >= {NAMEPLATE_MIN_ROWS}
                       AND COUNT(DISTINCT COALESCE(trim,'')) >= {NAMEPLATE_MIN_TRIMS}
                ) t""",
        )
        out.append(
            InvariantResult(
                id=f"nameplate_constant_{col}",
                title=f"one {col} value stamped across every trim of a nameplate",
                detects=(
                    f"model-level contamination of {col}: a single figure written to "
                    f"every trim and model year of a nameplate"
                ),
                impossible_because=(
                    f"{col} is precisely what differs between trims of the same "
                    "nameplate; a nameplate-wide constant is right only by accident "
                    "(BMW 7 Series carried 14.4 kWh on all 211 rows, gas cars included)"
                ),
                tier="stored",
                count=len(hits),
                denominator=denom,
                examples=[
                    {
                        "make": h["make"],
                        "model": h["model"],
                        "value": float(h["stamped_value"]) if h["stamped_value"] is not None else None,
                        "rows": h["rows_n"],
                        "trims": h["trims_n"],
                        "years": h["years_n"],
                    }
                    for h in hits[:5]
                ],
                elapsed_sec=time.time() - t0,
            )
        )
    return out


def run_stored_tier(conn) -> list[InvariantResult]:
    results: list[InvariantResult] = []
    with conn.cursor() as cur:
        for spec in SQL_INVARIANTS:
            if spec.id == "car_trim_not_named_by_listing":
                results.append(_invariant_trim_not_named(cur, spec))
                continue
            t0 = time.time()
            try:
                count = _scalar(cur, spec.count_sql)
                denom = _scalar(cur, spec.denominator_sql) if spec.denominator_sql else None
                examples = _rows(cur, spec.example_sql) if spec.example_sql else []
                err = None
            except Exception as exc:  # a broken check is never a passing check
                count, denom, examples, err = -1, None, [], f"{type(exc).__name__}: {exc}"
                conn.rollback()
            results.append(
                InvariantResult(
                    id=spec.id,
                    title=spec.title,
                    detects=spec.detects,
                    impossible_because=spec.impossible_because,
                    tier="stored",
                    count=count,
                    denominator=denom,
                    examples=[_jsonable(e) for e in examples],
                    elapsed_sec=time.time() - t0,
                    error=err,
                )
            )
        results.extend(_invariant_nameplate_constant(cur))
    return results


# --------------------------------------------------------------------------
# Rendered tier — the production render path, asserted on
# --------------------------------------------------------------------------


def _f(v: Any) -> float | None:
    if v is None or v == "":
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(f) else f


def _close(a: float | None, b: float | None, rel: float = 0.005) -> bool:
    if a is None or b is None:
        return False
    return abs(a - b) <= max(rel * max(abs(a), abs(b)), 1e-6)


def _key(year: Any, make: Any, model: Any) -> tuple:
    try:
        y = int(year)
    except (TypeError, ValueError):
        y = -1
    return (y, str(make or "").strip().lower(), str(model or "").strip().lower())


def _load_attribution_tables(conn) -> dict[str, dict]:
    """
    In-memory copies of the three spec stores, so provenance can be attributed
    without a query per car.

    ``epa`` maps (year, make, model) -> {field: {values seen}} from the SCRAPED
    table; ``ai_model`` and ``ai_engine`` map the same key to the AI-researched
    values. A rendered figure that equals an AI value and equals no scraped value
    for the same cohort came from the AI table.
    """
    epa: dict[tuple, dict[str, set]] = {}
    ai_model: dict[tuple, dict[str, float]] = {}
    ai_engine: dict[tuple, dict[str, float]] = {}
    with conn.cursor() as cur:
        cur.execute(
            """SELECT year, make, model, horsepower, torque_lb_ft, curb_weight_lb,
                      zero_to_60_sec, tow_capacity_lb, fuel_tank_gal, ev_range_miles, battery_kwh
               FROM epa_extended_specs"""
        )
        fields = (
            "horsepower", "torque_lb_ft", "curb_weight_lb", "zero_to_60_sec",
            "tow_capacity_lb", "fuel_tank_gal", "ev_range_miles", "battery_kwh",
        )
        for row in cur:
            k = _key(row[0], row[1], row[2])
            slot = epa.setdefault(k, {})
            for name, val in zip(fields, row[3:]):
                fv = _f(val)
                if fv is not None:
                    slot.setdefault(name, set()).add(round(fv, 4))

        cur.execute(
            """SELECT year, make, model, horsepower, torque_lb_ft, curb_weight_lb,
                      zero_to_60_sec, tow_capacity_lb, fuel_tank_gal
               FROM ai_model_specs"""
        )
        ai_fields = (
            "horsepower", "torque_lb_ft", "curb_weight_lb",
            "zero_to_60_sec", "tow_capacity_lb", "fuel_tank_gal",
        )
        for row in cur:
            k = _key(row[0], row[1], row[2])
            slot = ai_model.setdefault(k, {})
            for name, val in zip(ai_fields, row[3:]):
                fv = _f(val)
                if fv is not None:
                    slot[name] = round(fv, 4)

        cur.execute(
            """SELECT year, make, model, horsepower, torque_lb_ft, tow_capacity_lb,
                      zero_to_60_sec, fuel_tank_gal, curb_weight_lb
               FROM ai_engine_specs"""
        )
        eng_fields = (
            "horsepower", "torque_lb_ft", "tow_capacity_lb",
            "zero_to_60_sec", "fuel_tank_gal", "curb_weight_lb",
        )
        for row in cur:
            k = _key(row[0], row[1], row[2])
            slot = ai_engine.setdefault(k, {})
            for name, val in zip(eng_fields, row[3:]):
                fv = _f(val)
                if fv is not None:
                    slot[name] = round(fv, 4)
    return {"epa": epa, "ai_model": ai_model, "ai_engine": ai_engine}


_PERF_BADGE_RE = re.compile(
    r"(AMG|\bM[2-8]\b|Competition|\bRS\b|\bGT[3RS]?\b|Type ?R|\bSRT\b|Hellcat|TRX|"
    r"Trackhawk|Raptor|Shelby|\bZL1\b|\bZ06\b|\bZR1\b|Blackwing|\bV-?Series\b|Redeye|"
    r"Demon|Turbo ?S|Quadrifoglio|Nismo|\bSTI\b|Plaid|Performance|\bN Line\b|Polestar)",
    re.IGNORECASE,
)
_PERF_MAKE_RE = re.compile(
    r"(Ferrari|Lamborghini|McLaren|Bugatti|Rimac|Koenigsegg|Lotus|Porsche|Tesla|"
    r"Lucid|Rivian|Polestar)",
    re.IGNORECASE,
)
#: Nameplates the 8,000 lb curb ceiling ABSTAINS on. Two groups, both of which
#: really do weigh more than four tons empty: heavy-duty pickups / cutaway vans,
#: and the GMC Hummer EV, whose published curb weight is about 9,000 lb (its
#: battery alone is roughly 2,900 lb). Abstaining is the fail-closed choice — the
#: check says nothing about these rather than raising an alarm it cannot justify.
#: Everything else over the ceiling is a towing figure in the wrong column.
_HEAVY_DUTY_RE = re.compile(
    r"(2500|3500|4500|5500|F-?[23456]50|Super ?Duty|Sierra ?[23]500|Silverado ?[23]500|"
    r"Transit|Savana|Express|E-?[34]50|Hummer)",
    re.IGNORECASE,
)


def _plugin_capable(fuel_text: str) -> bool:
    ft = (fuel_text or "").strip().lower()
    if "plug-in" in ft or "plug in" in ft or "phev" in ft:
        return True
    if ft in ("electric", "ev"):
        return True
    return "electric" in ft and "gas" not in ft and "hybrid" not in ft


def _fuel_bucket(value: Any) -> str:
    """The literal the fuel FILTER matches on, normalized only for case/spacing."""
    return re.sub(r"\s+", " ", str(value or "").strip()).lower()


@dataclass
class RenderedFindings:
    curb_equals_tow: list = field(default_factory=list)
    curb_over_ceiling: list = field(default_factory=list)
    sub_four_no_badge: list = field(default_factory=list)
    ai_sourced_spec: list = field(default_factory=list)
    ev_field_on_non_ev: list = field(default_factory=list)
    fuel_card_vs_filter: list = field(default_factory=list)
    ladder_bullet_uncited: list = field(default_factory=list)
    ladder_citation_doc_missing: list = field(default_factory=list)
    cohorts: int = 0
    ladder_bullets: int = 0
    ladders: int = 0


def run_rendered_tier(conn, *, cohort_limit: int | None, check_ladder: bool = True) -> tuple[list[InvariantResult], float]:
    from backend.enrichment.knowledge_engine_specs import merge_verified_specs
    from backend.utils.car_serialize import serialize_car_for_api

    resolve_trim_ladder = None
    if check_ladder:
        from backend.enrichment.trim_ladder import resolve_trim_ladder  # noqa: F401

    tables = _load_attribution_tables(conn)
    epa, ai_model, ai_engine = tables["epa"], tables["ai_model"], tables["ai_engine"]

    limit_sql = f" LIMIT {int(cohort_limit)}" if cohort_limit else ""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT ON (year, make, model, trim) * FROM cars "
            "WHERE listing_active = 1 "
            "ORDER BY year, make, model, trim, id" + limit_sql
        )
        cols = [d[0] for d in cur.description]
        reps = [dict(zip(cols, r)) for r in cur.fetchall()]

    fnd = RenderedFindings()
    fnd.cohorts = len(reps)
    t0 = time.time()

    for car in reps:
        vs = merge_verified_specs(car)
        ser = serialize_car_for_api(car, verified_specs=vs, include_extended_display=True)
        ident = {
            "id": car.get("id"),
            "year": car.get("year"),
            "make": car.get("make"),
            "model": car.get("model"),
            "trim": car.get("trim"),
        }
        badge_text = " ".join(
            str(car.get(k) or "") for k in ("trim", "model", "title", "model_full_raw")
        )

        curb = _f(ser.get("curb_weight_lb"))
        tow = _f(ser.get("tow_capacity_lb"))
        if curb is not None and tow is not None and _close(curb, tow, rel=0.0):
            fnd.curb_equals_tow.append({**ident, "curb_weight_lb": curb, "tow_capacity_lb": tow})
        if curb is not None and curb > 8000 and not _HEAVY_DUTY_RE.search(badge_text):
            fnd.curb_over_ceiling.append({**ident, "curb_weight_lb": curb})

        z60 = _f(ser.get("zero_to_60_sec"))
        if (
            z60 is not None
            and z60 < 4.0
            and not _PERF_BADGE_RE.search(badge_text)
            and not _PERF_MAKE_RE.search(str(car.get("make") or ""))
        ):
            fnd.sub_four_no_badge.append({**ident, "zero_to_60_sec": z60})

        ev_ok = _plugin_capable(str(ser.get("fuel_type") or car.get("fuel_type") or ""))
        if not ev_ok:
            for f_ in ("battery_kwh", "ev_range_miles"):
                if _f(ser.get(f_)) is not None:
                    fnd.ev_field_on_non_ev.append({**ident, "field": f_, "value": ser.get(f_),
                                                   "fuel_type": ser.get("fuel_type")})

        # AI PROVENANCE, stated conservatively. A rendered figure is attributed to
        # the AI tables only when BOTH hold:
        #   * it equals the value ai_model_specs / ai_engine_specs carries for this
        #     (year, make, model), and
        #   * epa_extended_specs carries NO value at all for that field on this
        #     nameplate-year, so there was nothing scraped for the render path to
        #     have preferred.
        # The second condition is stricter than "the scraped value differs" on
        # purpose: it can only UNDER-count. It also leaves one gap I have not
        # closed — a window-sticker figure (``_sticker_specs_from_packages``) that
        # happens to equal the AI value would be attributed here wrongly. That
        # would be a false alarm, not a missed defect.
        k = _key(car.get("year"), car.get("make"), car.get("model"))
        ai_here = {**ai_model.get(k, {}), **ai_engine.get(k, {})}
        epa_here = epa.get(k, {})
        if ai_here:
            for f_ in SHOPPER_SPEC_FIELDS:
                rv = _f(ser.get(f_))
                av = ai_here.get(f_)
                if rv is None or av is None:
                    continue
                if not _close(rv, av):
                    continue
                if epa_here.get(f_):
                    continue  # a scraped value exists for this field — not AI-only
                fnd.ai_sourced_spec.append({**ident, "field": f_, "value": rv,
                                            "matches_ai_value": av})

        card = _fuel_bucket(ser.get("fuel_type"))
        stored = _fuel_bucket(car.get("fuel_type"))
        if card and stored and card != stored and card not in ("—", "-"):
            fnd.fuel_card_vs_filter.append({**ident, "card": ser.get("fuel_type"),
                                            "filter_value": car.get("fuel_type")})

        if resolve_trim_ladder is not None:
            try:
                ladder = resolve_trim_ladder(
                    make=car.get("make"), model=car.get("model"),
                    year=car.get("year"), trim=car.get("trim"),
                )
            except Exception:
                ladder = None
            if ladder:
                fnd.ladders += 1
                for step in ladder.get("steps") or []:
                    adds = step.get("adds") or []
                    cites = step.get("adds_citations") or []
                    fnd.ladder_bullets += len(adds)
                    cited_text = {
                        _norm_words(c.get("text")).strip()
                        for c in cites
                        if isinstance(c, dict) and c.get("text")
                    }
                    for bullet in adds:
                        if _norm_words(bullet).strip() not in cited_text:
                            fnd.ladder_bullet_uncited.append(
                                {**ident, "step": step.get("name"), "bullet": str(bullet)[:160]}
                            )
                    for c in cites:
                        if not isinstance(c, dict):
                            continue
                        src = c.get("source")
                        if not src:
                            fnd.ladder_citation_doc_missing.append(
                                {**ident, "step": step.get("name"), "source": None}
                            )
                            continue
                        if not (DICTIONARY_DIR / str(src)).exists():
                            fnd.ladder_citation_doc_missing.append(
                                {**ident, "step": step.get("name"), "source": str(src)}
                            )

    elapsed = time.time() - t0
    n = fnd.cohorts

    def mk(iid, title, detects, why, hits, denom=None):
        return InvariantResult(
            id=iid, title=title, detects=detects, impossible_because=why,
            tier="rendered", count=len(hits), denominator=denom if denom is not None else n,
            examples=[_jsonable(h) for h in hits[:20]], elapsed_sec=elapsed,
        )

    results = [
        mk("rendered_curb_equals_tow",
           "the car page shows a curb weight identical to the tow rating",
           "the mis-parsed towing figure surviving every guard and reaching a shopper",
           "the page states two independent measurements; they are not the same number",
           fnd.curb_equals_tow),
        mk("rendered_curb_over_ceiling",
           "the car page shows over 8,000 lb curb weight on a light-duty vehicle",
           "the plausibility guard failing to suppress a towing figure",
           "no light-duty passenger vehicle weighs four tons empty",
           fnd.curb_over_ceiling),
        mk("rendered_sub_four_zero_to_60",
           "the car page shows a sub-4.0s 0-60 on an unbadged car",
           "a halo trim's acceleration figure reaching a base car's page",
           "a car this quick is badged or electric; a base sedan is not",
           fnd.sub_four_no_badge),
        mk("rendered_ai_sourced_spec",
           "a shopper-visible spec figure traceable only to an AI-researched table",
           "a generated number rendered under a spec heading with no document behind it",
           "a specification is a measurement; a value that exists in no scraped or EPA "
           "source and only in ai_model_specs / ai_engine_specs was written by a model, "
           "not read off a vehicle",
           fnd.ai_sourced_spec),
        mk("rendered_ev_field_on_non_ev",
           "battery capacity or electric range on a car that cannot be plugged in",
           "an EV trim's row matched onto the gas car of the same nameplate",
           "a car with no plug has no battery capacity and no electric range",
           fnd.ev_field_on_non_ev),
        mk("rendered_fuel_type_card_vs_filter",
           "the card's fuel type differs from the value the search filter matches",
           "a display-only fuel relabel — the card says Hybrid while the filter returns it under Gasoline",
           "one car has one fuel type; a shopper who filters for what the card says "
           "must get that car back",
           fnd.fuel_card_vs_filter),
        mk("ladder_bullet_without_citation",
           "a rendered trim bullet with no citation quoting its own text",
           "a trim feature claim with nothing behind it — the class that put half a "
           "Wikipedia sentence on a car page as a specification",
           "a feature claim is either quoted from a document or it is an assertion "
           "this system is not entitled to make",
           fnd.ladder_bullet_uncited, denom=fnd.ladder_bullets),
        mk("ladder_citation_source_missing",
           "a trim bullet citing a document that is not on disk",
           "a citation pointing at a brochure that was quarantined, renamed or never written",
           "a citation whose document does not exist is indistinguishable from no citation",
           fnd.ladder_citation_doc_missing, denom=fnd.ladder_bullets),
    ]
    return results, elapsed


# --------------------------------------------------------------------------
# Baseline comparison and reporting
# --------------------------------------------------------------------------


def _jsonable(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_jsonable(v) for v in obj]
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj
    return str(obj)


def load_baseline(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}
    return data.get("invariants", {}) if isinstance(data, dict) else {}


def compare(results: list[InvariantResult], baseline: dict[str, dict[str, Any]],
            *, slack: int = 0) -> list[dict[str, Any]]:
    verdicts = []
    for r in results:
        base_entry = baseline.get(r.id)
        base = base_entry.get("count") if isinstance(base_entry, dict) else None
        if r.error:
            status = "ERROR"
        elif base is None:
            status = "NO_BASELINE"
        elif r.count > base + slack:
            status = "REGRESSION"
        elif r.count < base:
            status = "IMPROVED"
        else:
            status = "OK"
        verdicts.append({
            "id": r.id, "tier": r.tier, "title": r.title, "count": r.count,
            "baseline": base, "delta": (r.count - base) if base is not None else None,
            "status": status, "denominator": r.denominator,
            "detects": r.detects, "impossible_because": r.impossible_because,
            "examples": r.examples[:20], "error": r.error,
        })
    return verdicts


_FAIL_STATUSES = {"REGRESSION", "NO_BASELINE", "ERROR"}


def print_summary(verdicts: list[dict[str, Any]], *, elapsed: float, rendered: bool) -> None:
    width = max((len(v["id"]) for v in verdicts), default=20)
    print("\nDATA-QUALITY INVARIANTS")
    print("=" * (width + 46))
    for tier in ("stored", "rendered"):
        rows = [v for v in verdicts if v["tier"] == tier]
        if not rows:
            continue
        print(f"\n[{tier}]")
        for v in rows:
            base = "—" if v["baseline"] is None else str(v["baseline"])
            delta = "" if v["delta"] is None else f" ({v['delta']:+d})"
            denom = f" / {v['denominator']}" if v["denominator"] else ""
            mark = {"OK": "ok  ", "IMPROVED": "down", "REGRESSION": "WORSE",
                    "NO_BASELINE": "NEW ", "ERROR": "ERR "}[v["status"]]
            print(f"  {mark} {v['id']:<{width}}  {v['count']}{denom:<12}  base={base}{delta}")
            if v["status"] in _FAIL_STATUSES:
                print(f"        detects: {v['detects']}")
                if v["error"]:
                    print(f"        error:   {v['error']}")
                for ex in v["examples"][:3]:
                    print(f"        e.g.     {json.dumps(ex, default=str)[:170]}")
    failing = [v for v in verdicts if v["status"] in _FAIL_STATUSES]
    print("\n" + "-" * (width + 46))
    print(f"{len(verdicts)} invariants, {len(failing)} failing, {elapsed:.1f}s"
          f"{'' if rendered else ' (stored tier only)'}")
    if not rendered:
        print("NOTE: the rendered tier did not run; shopper-visible invariants were NOT checked.")


def write_baseline(path: Path, results: list[InvariantResult], *, note: str) -> None:
    import datetime

    payload = {
        "generated_at": datetime.datetime.now(datetime.UTC).isoformat(timespec="seconds"),
        "note": note,
        "invariants": {
            r.id: {
                "count": r.count,
                "denominator": r.denominator,
                "tier": r.tier,
                "title": r.title,
            }
            for r in results
            if not r.error
        },
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


# --------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--baseline", type=Path, default=DEFAULT_BASELINE_PATH)
    ap.add_argument("--json", type=Path, default=None, help="write the machine-readable report here")
    ap.add_argument("--sql-only", action="store_true", help="stored tier only (fast CI gate)")
    ap.add_argument("--render-cohorts", type=int, default=None,
                    help="cap the rendered tier at N cohorts (sampling makes counts "
                         "incomparable to the baseline; use for smoke runs only)")
    ap.add_argument("--no-ladder", action="store_true", help="skip the trim-bullet citation checks")
    ap.add_argument("--slack", type=int, default=0, help="allowed increase over baseline before failing")
    ap.add_argument("--write-baseline", action="store_true",
                    help="record the current counts as the baseline and exit 0")
    ap.add_argument("--note", default="", help="note stored alongside a written baseline")
    args = ap.parse_args(argv)

    t0 = time.time()
    try:
        conn = open_readonly_connection()
    except Exception as exc:
        print(f"FATAL: could not open a read-only inventory connection: {exc}", file=sys.stderr)
        return 2

    rendered_ran = False
    try:
        results = run_stored_tier(conn)
        if not args.sql_only:
            rendered, _ = run_rendered_tier(
                conn, cohort_limit=args.render_cohorts, check_ladder=not args.no_ladder
            )
            results.extend(rendered)
            rendered_ran = True
    except Exception as exc:
        print(f"FATAL: invariant run failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2
    finally:
        conn.close()

    elapsed = time.time() - t0

    if args.write_baseline:
        if args.render_cohorts:
            print("REFUSED: --write-baseline with --render-cohorts would record a sampled "
                  "count as if it were a full one.", file=sys.stderr)
            return 2
        if args.sql_only:
            print("REFUSED: --write-baseline with --sql-only would drop every rendered-tier "
                  "baseline.", file=sys.stderr)
            return 2
        write_baseline(args.baseline, results, note=args.note)
        print(f"Baseline written to {args.baseline} ({len(results)} invariants, {elapsed:.1f}s)")
        return 0

    baseline = load_baseline(args.baseline)
    verdicts = compare(results, baseline, slack=args.slack)
    print_summary(verdicts, elapsed=elapsed, rendered=rendered_ran)

    report = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "elapsed_sec": round(elapsed, 2),
        "rendered_tier_ran": rendered_ran,
        "render_cohort_limit": args.render_cohorts,
        "baseline_path": str(args.baseline),
        "failing": [v["id"] for v in verdicts if v["status"] in _FAIL_STATUSES],
        "invariants": verdicts,
    }
    if args.json:
        args.json.write_text(json.dumps(report, indent=2, default=str) + "\n")

    return 1 if report["failing"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
