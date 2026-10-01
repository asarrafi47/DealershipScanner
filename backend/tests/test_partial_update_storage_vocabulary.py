"""``update_car_row_partial`` must store the same closed vocabulary the upsert does.

The enrichment backfill (``spec_structured_backfill`` tier 1 →
``collect_merge_spec_storage_updates`` → ``update_car_row_partial``) fills a blank
drivetrain / fuel_type / body_style straight from ``merge_verified_specs``, whose
values are EPA / vPIC *display* strings: "4-Wheel Drive", "Regular Gasoline",
"Sport Utility Vehicle [SUV]/Multipurpose Vehicle [MPV]". The scanner upsert runs
every incoming value through ``clean_car_row_dict``; this write path did not, so
those strings landed in the columns verbatim.

Measured on the live database before the fix: 2,524 rows with a drivetrain outside
{AWD, FWD, RWD, 4WD} and 1,075 rows with a fuel_type outside the six presets — the
largest single bucket being 364 rows of '4-Wheel Drive', all stamped
``spec_source_json.drivetrain.source = "inventory_repair"``. Those rows do not
answer the "4WD" facet filter and show up as their own duplicate facet entries.
"""
from __future__ import annotations

import sqlite3

import pytest

from backend.utils.field_clean import (
    coerce_body_style_stored,
    coerce_drivetrain_stored,
    coerce_fuel_type_stored,
)


@pytest.mark.parametrize(
    "epa_drive,expected",
    [
        ("4-Wheel Drive", "4WD"),
        ("Four-Wheel Drive", "4WD"),
        ("Part-time 4-Wheel Drive", "4WD"),
        ("Full-time 4-Wheel Drive", "4WD"),
        ("4-Wheel or All-Wheel Drive", "4WD"),
        ("All-Wheel Drive", "AWD"),
        ("Front-Wheel Drive", "FWD"),
        ("Rear-Wheel Drive", "RWD"),
    ],
)
def test_epa_drive_vocabulary_canonicalizes(epa_drive, expected):
    assert coerce_drivetrain_stored(epa_drive) == expected


def _feed_row(vin):
    """A parsed feed row with the three vocabulary columns left blank."""
    return {
        "vin": vin,
        "title": "2021 GMC Yukon SLT",
        "year": 2021,
        "make": "GMC",
        "model": "Yukon",
        "trim": "SLT",
        "price": 55000,
        "mileage": 12000,
        "dealer_name": "Dealer",
        "dealer_url": "https://dealer.test/",
        "dealer_id": "d1",
    }


def test_partial_update_canonicalizes_epa_display_strings(tmp_path, monkeypatch):
    """The enrichment write path must not create its own facet buckets."""
    import backend.db.inventory_db as inventory_db
    from backend.db.inventory_db import update_car_row_partial
    from backend.scanner.database import upsert_vehicles

    db_path = tmp_path / "inv_vocab.db"
    monkeypatch.setenv("INVENTORY_DB_PATH", str(db_path))
    monkeypatch.setattr(inventory_db, "DB_PATH", str(db_path))
    upsert_vehicles([_feed_row("VOCABTESTVIN00001")])

    conn = sqlite3.connect(str(db_path))
    car_id = conn.execute(
        "SELECT id FROM cars WHERE vin = ?", ("VOCABTESTVIN00001",)
    ).fetchone()[0]
    conn.close()

    # Exactly the shape collect_merge_spec_storage_updates() hands over.
    update_car_row_partial(
        car_id,
        {
            "drivetrain": "4-Wheel Drive",
            "fuel_type": "Regular Gasoline",
            "body_style": "Sport Utility Vehicle [SUV]/Multipurpose Vehicle [MPV]",
        },
    )

    conn = sqlite3.connect(str(db_path))
    stored = conn.execute(
        "SELECT drivetrain, fuel_type, body_style FROM cars WHERE id = ?", (car_id,)
    ).fetchone()
    conn.close()

    assert stored == ("4WD", "Gasoline", "SUV")


def test_partial_update_leaves_already_canonical_values_alone(tmp_path, monkeypatch):
    import backend.db.inventory_db as inventory_db
    from backend.db.inventory_db import update_car_row_partial
    from backend.scanner.database import upsert_vehicles

    db_path = tmp_path / "inv_vocab2.db"
    monkeypatch.setenv("INVENTORY_DB_PATH", str(db_path))
    monkeypatch.setattr(inventory_db, "DB_PATH", str(db_path))
    upsert_vehicles([_feed_row("VOCABTESTVIN00002")])

    conn = sqlite3.connect(str(db_path))
    car_id = conn.execute(
        "SELECT id FROM cars WHERE vin = ?", ("VOCABTESTVIN00002",)
    ).fetchone()[0]
    conn.close()

    update_car_row_partial(
        car_id, {"drivetrain": "AWD", "fuel_type": "Electric", "body_style": "Truck"}
    )

    conn = sqlite3.connect(str(db_path))
    stored = conn.execute(
        "SELECT drivetrain, fuel_type, body_style FROM cars WHERE id = ?", (car_id,)
    ).fetchone()
    conn.close()

    assert stored == ("AWD", "Electric", "Truck")


def test_vocabulary_coercers_pass_through_unknown_text():
    """Unknown values still round-trip; the coercers must not blank real data."""
    assert coerce_drivetrain_stored("Other drive systems") == "Other drive systems"
    # A recognisable drivetrain inside marketing copy canonicalizes (vehicle_facts).
    assert coerce_drivetrain_stored("Advanced 4x4 with Automatic On Demand Engagement") == "4WD"
    assert coerce_fuel_type_stored("Regular Gasoline") == "Gasoline"
    assert coerce_body_style_stored("Sport Utility Vehicle [SUV]/Multipurpose Vehicle [MPV]") == "SUV"
