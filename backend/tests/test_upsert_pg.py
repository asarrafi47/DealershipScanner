"""``upsert_vehicles`` write-path cases on a real Postgres (P14A.6, ``pg`` tier).

The SQLite golden (test_upsert_vehicles_golden.py) pins the whole run; these
cases pin the statement behaviours that only the Postgres adapter path can break
(``?`` -> ``%s``, ``LIKE 'http%'`` escaping, ``rowcount`` through InventoryCursor,
NULL binding, the sidecar tables living in the same database). Each test gets a
fresh migration-built clone (``pg_db``).
"""

from __future__ import annotations

import pytest

from backend.scanner import database as sdb
from backend.scanner.database import upsert_vehicles

pytestmark = pytest.mark.pg

VIN = "1N4BL4DV0PN300001"


@pytest.fixture()
def upsert_env(pg_db, monkeypatch):
    """The network post-write steps stubbed (vPIC backfill, catalog linker); all else real."""
    monkeypatch.setattr("backend.utils.project_env.load_project_dotenv", lambda *a, **k: None)
    monkeypatch.setattr(sdb, "_vin_owner_table_ready", False)
    monkeypatch.delenv("SCANNER_VIN_OWNER_GUARD_HOURS", raising=False)
    monkeypatch.setattr(
        "backend.enrichment.spec_structured_backfill.apply_structured_spec_backfill_for_car",
        lambda *a, **k: {},
    )
    monkeypatch.setattr("backend.catalog.linker.link_cars_by_vins", lambda vins: 0)
    return pg_db


def _car(dealer_id: str = "d-one", vin: str = VIN, **fields) -> dict:
    row = {
        "vin": vin,
        "title": "2023 Nissan Altima SV",
        "year": 2023,
        "make": "Nissan",
        "model": "Altima",
        "trim": "SV",
        "price": 30000,
        "mileage": 12000,
        "image_url": "https://img.example.test/altima.jpg",
        "exterior_color": "Pearl White",
        "interior_color": "Charcoal",
        "transmission": "CVT",
        "drivetrain": "FWD",
        "fuel_type": "Gasoline",
        "dealer_id": dealer_id,
        "dealer_name": dealer_id,
        "dealer_url": f"https://{dealer_id}.example.test",
    }
    row.update(fields)
    return row


def _select(env, sql: str, params=()):
    with env.connect() as conn:
        cur = conn.execute(sql, params)
        return cur.fetchall()


def test_sql_backstop_rowcount_zero_records_the_refusal(upsert_env, monkeypatch):
    assert upsert_vehicles([_car("owner")], {}) == 1
    # Disable the prefetch predicate so only the statement's WHERE (rowcount 0 through
    # InventoryCursor) can stop the claimant.
    monkeypatch.setattr(sdb, "_vin_owned_elsewhere", lambda *a, **k: False)
    stats: dict = {}
    assert upsert_vehicles([_car("claimant", price=1)], stats) == 0
    assert stats["vin_owner_conflicts"] == 1
    assert stats["vin_owner_conflict_owners"] == {"owner": 1}
    dealer, price = _select(upsert_env, "SELECT dealer_id, price FROM cars WHERE vin = %s", (VIN,))[0]
    assert (dealer, float(price)) == ("owner", 30000.0)
    assert _select(
        upsert_env, "SELECT vin, owner_dealer_id, claimant_dealer_id FROM vin_owner_conflicts"
    ) == [(VIN, "owner", "claimant")]


def test_a_stale_owner_lets_the_vin_move(upsert_env, monkeypatch):
    assert upsert_vehicles([_car("owner")], {}) == 1
    with upsert_env.connect() as conn:
        # TEXT comparison of scraped_at against the cutoff ('...Z' on both sides)
        conn.execute("UPDATE cars SET scraped_at = '2020-01-01T00:00:00Z' WHERE vin = %s", (VIN,))
        conn.commit()
    monkeypatch.setattr(sdb, "_vin_owned_elsewhere", lambda *a, **k: False)
    assert upsert_vehicles([_car("claimant")], {}) == 1
    assert _select(upsert_env, "SELECT dealer_id FROM cars WHERE vin = %s", (VIN,)) == [("claimant",)]


def test_keep_if_nonempty_fields_survive_a_sparse_rescan(upsert_env):
    full = _car(
        carfax_url="https://carfax.example.test/x",
        msrp=32000,
        gallery=["https://img.example.test/1.jpg", "https://img.example.test/2.jpg"],
        stock_number="STK1",
    )
    assert upsert_vehicles([full], {}) == 1
    sparse = _car(trim="", exterior_color="", interior_color="", stock_number="", price=None, mileage=None)
    sparse.pop("image_url")
    assert upsert_vehicles([sparse], {}) == 1
    row = _select(
        upsert_env,
        "SELECT trim, exterior_color, interior_color, stock_number, price, carfax_url, msrp, gallery, image_url "
        "FROM cars WHERE vin = %s",
        (VIN,),
    )[0]
    trim, ext, intr, stock, price, carfax, msrp, gallery, image = row
    assert (trim, ext, intr, stock) == ("SV", "Pearl White", "Charcoal", "STK1")
    assert float(price) == 30000.0 and float(msrp) == 32000.0
    assert carfax == "https://carfax.example.test/x"
    assert "2.jpg" in gallery
    assert image == "https://img.example.test/altima.jpg"


def test_like_http_percent_survives_the_adapter(upsert_env):
    """``excluded.image_url LIKE 'http%'`` reaches the server as a literal pattern, not a placeholder."""
    assert upsert_vehicles([_car(image_url="https://img.example.test/a.jpg")], {}) == 1
    # a non-http incoming image never replaces a stored http one
    assert upsert_vehicles([_car(image_url="/static/no-photo.png")], {}) == 1
    assert _select(upsert_env, "SELECT image_url FROM cars WHERE vin = %s", (VIN,)) == [
        ("https://img.example.test/a.jpg",)
    ]
    # but an http image replaces a stored non-http one
    other = "1N4BL4DV0PN300002"
    assert upsert_vehicles([_car(vin=other, image_url="/static/no-photo.png")], {}) == 1
    assert upsert_vehicles([_car(vin=other, image_url="https://img.example.test/b.jpg")], {}) == 1
    assert _select(upsert_env, "SELECT image_url FROM cars WHERE vin = %s", (other,)) == [
        ("https://img.example.test/b.jpg",)
    ]


def test_data_quality_score_takes_the_better_of_stored_and_incoming(upsert_env):
    assert upsert_vehicles([_car()], {}) == 1
    (high,) = _select(upsert_env, "SELECT data_quality_score FROM cars WHERE vin = %s", (VIN,))[0]
    assert high and float(high) > 0
    # an SRP-only rescan scores lower on its own payload; the stored score must not drop
    thin = {"vin": VIN, "title": "2023 Nissan Altima", "make": "Nissan", "model": "Altima",
            "dealer_id": "d-one", "dealer_name": "d-one", "dealer_url": "https://d-one.example.test"}
    assert upsert_vehicles([thin], {}) == 1
    (after,) = _select(upsert_env, "SELECT data_quality_score FROM cars WHERE vin = %s", (VIN,))[0]
    assert float(after) == float(high)


def test_sparse_row_binds_null_parameters(upsert_env):
    """Every optional value is NULL: no 'could not determine data type of parameter' from the
    typed CASE/COALESCE arms, and the NULL columns stay NULL (not '' or 0)."""
    thin = {"vin": VIN, "title": "Unknown vehicle", "dealer_id": "d-one", "dealer_name": "d-one",
            "dealer_url": "https://d-one.example.test"}
    assert upsert_vehicles([thin], {}) == 1
    row = _select(
        upsert_env,
        "SELECT price, msrp, mileage, carfax_url, forced_induction, spin_frames, interior_pano, "
        "listing_removed_at FROM cars WHERE vin = %s",
        (VIN,),
    )[0]
    assert row[0] is None and row[1] is None and row[3] is None and row[7] is None


def test_post_write_syncs_incomplete_listings_in_the_same_database(upsert_env):
    gappy = _car(price=None, exterior_color="")
    assert upsert_vehicles([gappy], {}) == 1
    rows = _select(upsert_env, "SELECT vin, missing_fields_json FROM incomplete_listings")
    assert [r[0] for r in rows] == [VIN]
    assert "price" in rows[0][1]
    # a complete rescan clears the gap
    complete = _car(engine_description="2.5L 4-Cyl", body_style="Sedan", cylinders=4)
    assert upsert_vehicles([complete], {}) == 1
    assert _select(upsert_env, "SELECT vin FROM incomplete_listings") == []
