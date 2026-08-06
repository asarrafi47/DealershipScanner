"""Mild-hybrid (48V BSG) fuel-type correction: gas for eTorque, hands off real hybrids."""

from __future__ import annotations

from typing import Any

import pytest

from backend.utils.fuel_type_normalize import (
    GASOLINE_DISPLAY,
    fill_normalized_fuel_type_for_display,
    is_correctable_hybrid_label,
    match_mild_hybrid_family,
    normalize_fuel_type_for_display,
    normalize_fuel_type_for_storage,
)


def _ram_1500_etorque(**overrides):
    """The reported VIN: 2023 Ram 1500 Limited, 5.7L HEMI eTorque, fed to us as Hybrid."""
    row = {
        "vin": "1C6SRFHT3PN616926",
        "title": "2023 Ram 1500 Limited",
        "year": 2023,
        "make": "Ram",
        "model": "1500",
        "trim": "Limited",
        "fuel_type": "Hybrid",
        "engine_description": "5.7L V8 Mild Hybrid",
        "engine_l": "5.7",
        "cylinders": 8,
        "epa_master_id": 62007,
    }
    row.update(overrides)
    return row


# --- the fix -----------------------------------------------------------------


def test_ram_1500_57_etorque_normalizes_to_gas():
    car = _ram_1500_etorque()
    assert normalize_fuel_type_for_display(car, catalog_fuel_type=None) == GASOLINE_DISPLAY


def test_ram_1500_prefers_linked_catalog_gasoline_row():
    """epa_master says 'Midgrade Gasoline' for the linked trim — cited, still shown as gas."""
    car = _ram_1500_etorque()
    assert (
        normalize_fuel_type_for_display(car, catalog_fuel_type="Midgrade Gasoline")
        == GASOLINE_DISPLAY
    )


def test_ram_1500_36_pentastar_etorque_also_normalizes():
    car = _ram_1500_etorque(
        trim="Big Horn",
        engine_description="Gas/Electric V-6 3.6 L/220",
        engine_l="3.6",
        cylinders=6,
        epa_master_id=None,
    )
    assert normalize_fuel_type_for_display(car) == GASOLINE_DISPLAY


@pytest.mark.parametrize("year", [2019, 2020, 2023, 2026])
def test_ram_1500_covered_from_2019_on(year):
    car = _ram_1500_etorque(year=year, epa_master_id=None)
    assert normalize_fuel_type_for_display(car) == GASOLINE_DISPLAY


def test_ram_1500_with_junk_engine_text_still_normalizes():
    """
    Real row: a 2022 Big Horn whose feed engine reads "6 Cyl - 6 L" (VIN engine
    code says 3.6L eTorque). The nameplate + year + non-plug-in hybrid label are
    already conclusive for this family, so unrecognisable engine text must not
    strand the truck on a "Hybrid" facet the grid cannot fill.
    """
    car = _ram_1500_etorque(
        vin="1C6RRFFG2NN417271",
        year=2022,
        trim="Big Horn",
        engine_description="6 Cyl - 6 L",
        engine_l="6",
        epa_master_id=None,
    )
    assert normalize_fuel_type_for_display(car) == GASOLINE_DISPLAY


def test_ram_1500_with_no_engine_text_at_all_normalizes():
    """The facet cascade projects no engine column; it must still agree with the card."""
    car = _ram_1500_etorque(engine_description=None, engine_l=None, epa_master_id=None)
    assert normalize_fuel_type_for_display(car) == GASOLINE_DISPLAY


def test_ram_1500_diesel_engine_text_blocks_the_match():
    """A compression-ignition engine is not a 48V BSG, whatever the nameplate says."""
    car = _ram_1500_etorque(
        engine_description="3.0L V6 EcoDiesel", engine_l="3.0", epa_master_id=None
    )
    assert normalize_fuel_type_for_display(car) is None


def test_ram_1500_before_etorque_launch_is_untouched():
    """eTorque did not exist on the 2018 DS; leave a 2018 'Hybrid' label alone."""
    car = _ram_1500_etorque(year=2018, epa_master_id=None)
    assert normalize_fuel_type_for_display(car) is None


def test_engine_line_is_not_rewritten():
    """The truck really is a mild hybrid — only the fuel field is corrected."""
    car = _ram_1500_etorque()
    out = {"fuel_type": "Hybrid", "engine_display": "5.7L V8 Mild Hybrid"}
    fill_normalized_fuel_type_for_display(car, out)
    assert out["fuel_type"] == GASOLINE_DISPLAY
    assert out["engine_display"] == "5.7L V8 Mild Hybrid"


def test_mild_hybrid_only_shows_in_engine_display():
    """Rows whose raw engine column is bare still match via the derived display."""
    car = _ram_1500_etorque(engine_description=None, engine_l=None, epa_master_id=None)
    out = {"fuel_type": "Hybrid", "engine_display": "5.7L V8 Mild Hybrid"}
    fill_normalized_fuel_type_for_display(car, out)
    assert out["fuel_type"] == GASOLINE_DISPLAY


# --- real electrified cars must be untouched ---------------------------------


def test_toyota_prius_stays_hybrid():
    car = {
        "title": "2023 Toyota Prius LE",
        "year": 2023,
        "make": "Toyota",
        "model": "Prius",
        "trim": "LE",
        "fuel_type": "Hybrid",
        "engine_description": "2.0L I4 Hybrid",
        "cylinders": 4,
    }
    assert normalize_fuel_type_for_display(car) is None
    out = {"fuel_type": "Hybrid", "engine_display": "2.0L I4"}
    fill_normalized_fuel_type_for_display(car, out)
    assert out["fuel_type"] == "Hybrid"


def test_rav4_prime_stays_plug_in_hybrid():
    car = {
        "title": "2023 Toyota RAV4 Prime XSE",
        "year": 2023,
        "make": "Toyota",
        "model": "RAV4 Prime",
        "trim": "XSE",
        "fuel_type": "Plug-In Hybrid",
        "engine_description": "2.5L I4 Plug-In Hybrid",
        "cylinders": 4,
    }
    assert normalize_fuel_type_for_display(car) is None
    out = {"fuel_type": "Plug-In Hybrid", "engine_display": "2.5L I4"}
    fill_normalized_fuel_type_for_display(car, out)
    assert out["fuel_type"] == "Plug-In Hybrid"


def test_tesla_stays_electric():
    car = {
        "title": "2023 Tesla Model 3 Long Range",
        "year": 2023,
        "make": "Tesla",
        "model": "Model 3",
        "trim": "Long Range",
        "fuel_type": "Electric",
        "engine_description": "Electric",
        "cylinders": 0,
    }
    assert normalize_fuel_type_for_display(car) is None
    out = {"fuel_type": "Electric", "engine_display": "Electric"}
    fill_normalized_fuel_type_for_display(car, out)
    assert out["fuel_type"] == "Electric"


def test_wrangler_4xe_is_never_a_mild_hybrid():
    """Same corporate parent, same 'Hybrid' feed label — but it plugs in."""
    car = {
        "title": "2023 Jeep Wrangler 4xe Rubicon",
        "year": 2023,
        "make": "Jeep",
        "model": "Wrangler 4xe",
        "trim": "Rubicon",
        "fuel_type": "Hybrid",
        "engine_description": "2.0L I4 Turbo Hybrid",
        "cylinders": 4,
    }
    assert normalize_fuel_type_for_display(car) is None


def test_ram_1500_rev_and_ramcharger_excluded_by_nameplate():
    for model in ("1500 REV", "1500 Ramcharger"):
        car = _ram_1500_etorque(model=model, title=f"2026 Ram {model}", year=2026)
        assert normalize_fuel_type_for_display(car, catalog_fuel_type=None) is None, model


def test_catalog_row_that_says_electricity_wins_and_abstains():
    """A bad epa_master link must not be laundered into 'Gasoline'."""
    car = _ram_1500_etorque()
    assert (
        normalize_fuel_type_for_display(
            car, catalog_fuel_type="Regular Gasoline / Electricity"
        )
        is None
    )


def test_ram_promaster_1500_van_does_not_match_the_truck_rule():
    car = _ram_1500_etorque(
        model="ProMaster 1500",
        title="2023 Ram ProMaster 1500 Base",
        trim="Base",
        engine_description="3.6L V6",
        epa_master_id=None,
    )
    assert normalize_fuel_type_for_display(car) is None


def test_already_gas_ram_is_left_alone():
    """No correction to make; the function must not churn correct rows."""
    car = _ram_1500_etorque(fuel_type="Gasoline")
    assert normalize_fuel_type_for_display(car, catalog_fuel_type=None) is None


# --- label gate --------------------------------------------------------------


@pytest.mark.parametrize(
    "label", ["Hybrid", "hybrid", "Gasoline / Electric", "Electric / Gasoline", "MHEV"]
)
def test_correctable_labels(label):
    assert is_correctable_hybrid_label(label) is True


@pytest.mark.parametrize(
    "label",
    [
        None,
        "",
        "—",
        "Gasoline",
        "Diesel",
        "Electric",
        "Hydrogen",
        "Plug-In Hybrid",
        "Plug-in Hybrid",
        "PHEV",
        "4xe",
        "Recharge",
    ],
)
def test_non_correctable_labels(label):
    assert is_correctable_hybrid_label(label) is False


# --- display and filter must agree ------------------------------------------


def test_gasoline_labelled_etorque_is_never_promoted_then_demoted():
    """
    A stored-"Gasoline" eTorque row must serialize as Gasoline on BOTH paths.

    The serializer used to promote any "mild hybrid" engine text to "Hybrid" and
    this module then demoted it back — a round trip whose only lasting effect was
    breaking rows the normalizer did not recognise. Fuel filters read the stored
    column, so a stored-gas row has nothing to correct.
    """
    from backend.utils.car_serialize import (
        serialize_car_for_api,
        serialize_car_for_listings_grid,
    )

    car = _ram_1500_etorque(fuel_type="Gasoline", epa_master_id=None)
    assert serialize_car_for_api(car, include_verified=False, verified_specs={})[
        "fuel_type"
    ] == GASOLINE_DISPLAY
    assert serialize_car_for_listings_grid(car)["fuel_type"] == GASOLINE_DISPLAY


def test_hybrid_labelled_etorque_reads_as_gas_on_both_serialize_paths():
    """The reported VIN's row: fed as "Hybrid", must read as gas on VDP and card."""
    from backend.utils.car_serialize import (
        serialize_car_for_api,
        serialize_car_for_listings_grid,
    )

    car = _ram_1500_etorque(
        engine_description="HEMI 5.7L V8 Multi Displacement VVT eTorque",
        epa_master_id=None,
    )
    ser = serialize_car_for_api(car, include_verified=False, verified_specs={})
    assert ser["fuel_type"] == GASOLINE_DISPLAY
    # The hardware still shows up where hardware belongs.
    assert ser["engine_display"] == "5.7L V8 Mild Hybrid"
    assert serialize_car_for_listings_grid(car)["fuel_type"] == GASOLINE_DISPLAY


def test_persisted_sticker_hybrid_label_cannot_re_promote_a_gas_row():
    """
    The window-sticker parser stored ``sticker_fuel_type: "Hybrid"`` for eTorque
    trucks (3 active non-Ram rows today). Honouring it would put the card back out
    of sync with the fuel filter, which reads the stored "Gasoline" column.
    """
    import json

    from backend.utils.car_serialize import serialize_car_for_listings_grid

    car = _ram_1500_etorque(
        make="Jeep",
        model="Wagoneer",
        trim="Series II",
        title="2022 Jeep Wagoneer Series II",
        year=2022,
        fuel_type="Gasoline",
        engine_description="5.7L V8 eTorque",
        epa_master_id=None,
        packages=json.dumps({"sticker_fuel_type": "Hybrid"}),
    )
    assert serialize_car_for_listings_grid(car)["fuel_type"] == GASOLINE_DISPLAY


def test_sticker_fuel_type_still_wins_when_it_changes_the_fuel_class():
    """Only the hybrid re-promotion is suppressed; a Diesel sticker still overrides."""
    import json

    from backend.utils.car_serialize import serialize_car_for_listings_grid

    car = _ram_1500_etorque(
        model="2500",
        title="2022 Ram 2500 Laramie",
        fuel_type="Gasoline",
        engine_description="6.7L I6",
        epa_master_id=None,
        packages=json.dumps({"sticker_fuel_type": "Diesel"}),
    )
    assert serialize_car_for_listings_grid(car)["fuel_type"] == "Diesel"


def test_facet_projection_row_normalizes_the_same_way_as_the_card():
    """
    The listings cascade selects raw columns only (no ``epa_master_id``); it must
    still reach the same answer the card does, or picking the facet shows nothing.
    """
    facet_row = {
        "make": "Ram",
        "model": "1500",
        "trim": "Limited",
        "year": 2023,
        "engine_description": "HEMI 5.7L V8 Multi Displacement VVT eTorque",
    }
    assert (
        normalize_fuel_type_for_display(
            facet_row, fuel_type="Hybrid", catalog_fuel_type=None
        )
        == GASOLINE_DISPLAY
    )


def test_storage_condition_path_does_not_touch_fuel_or_the_catalog():
    """
    ``infer_condition_for_storage`` runs during enrichment persist and returns
    ``condition`` only — it must not fire the display-only fuel correction (which
    can hit ``epa_master``) for every mild-hybrid row it walks past.
    """
    import backend.utils.fuel_type_normalize as ftn
    from backend.utils.car_serialize import infer_condition_for_storage

    calls: list[Any] = []

    def _boom(car):  # pragma: no cover - must never run
        calls.append(car)
        raise AssertionError("catalog lookup on the storage path")

    original = ftn.catalog_fuel_type_for_car
    ftn.catalog_fuel_type_for_car = _boom
    try:
        car = _ram_1500_etorque(condition=None, mileage=12000)
        assert infer_condition_for_storage(car) == "Used"
    finally:
        ftn.catalog_fuel_type_for_car = original
    assert calls == []


# --- the fuel FILTER reads the stored column ---------------------------------


def _seed_fuel_filter_db(dbp) -> None:
    """Three rows: an eTorque Ram stored as gas, one stored as Hybrid, one real hybrid."""
    import sqlite3

    from backend.db.inventory_db import init_inventory_db

    init_inventory_db()
    conn = sqlite3.connect(str(dbp))
    cur = conn.cursor()
    sql = """
        INSERT INTO cars (
            vin, title, year, make, model, trim, price, mileage,
            image_url, dealer_name, dealer_url, dealer_id, scraped_at,
            fuel_type, cylinders, transmission, drivetrain,
            exterior_color, interior_color, stock_number, gallery,
            engine_l, engine_description, listing_active, listing_removed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    def row(vin, make, model, trim, fuel, eng_l, eng_desc, cyl):
        return (
            vin, f"2023 {make} {model} {trim}", 2023, make, model, trim, 55000, 12000,
            "https://example.com/a.jpg", "Dealer", "https://dealer.test/", "d1",
            "2026-01-01T00:00:00Z", fuel, cyl, "Automatic", "4WD", "Black", "Black",
            "S1", "[]", eng_l, eng_desc, 1, None,
        )

    cur.execute(sql, row("RAMGASETORQUE0001", "Ram", "1500", "Limited", "Gasoline",
                         "5.7", "HEMI 5.7L V8 Multi Displacement VVT eTorque", 8))
    cur.execute(sql, row("RAMHYBETORQUE0002", "Ram", "1500", "Limited", "Hybrid",
                         "5.7", "HEMI 5.7L V8 Multi Displacement VVT eTorque", 8))
    cur.execute(sql, row("PRIUSREALHYBRID03", "Toyota", "Prius", "LE", "Hybrid",
                         "2.0", "2.0L I4 Hybrid", 4))
    conn.commit()
    conn.close()


def test_search_cars_gas_filter_agrees_with_the_card_for_a_stored_gas_etorque(
    monkeypatch, tmp_path
):
    """
    Every fuel filter (``search_cars``, the facet cascade) reads the stored
    ``cars.fuel_type`` column, so the card must display what the filter matched.
    This is the regression guard for the removed display-only "Hybrid" promotion:
    a stored-gas eTorque row must come back under Gasoline AND read as Gasoline.
    """
    import backend.db.inventory_db as inventory_db
    from backend.db.inventory_db import search_cars
    from backend.utils.car_serialize import serialize_car_for_listings_grid

    monkeypatch.setattr(inventory_db, "DB_PATH", str(tmp_path / "inv_fuel.db"))
    _seed_fuel_filter_db(tmp_path / "inv_fuel.db")

    gas = search_cars(makes=["Ram"], fuel_types=["Gasoline"])
    assert "RAMGASETORQUE0001" in {r["vin"] for r in gas}
    # Nothing the gas filter returned may render as anything but gas.
    assert {serialize_car_for_listings_grid(dict(r))["fuel_type"] for r in gas} == {
        GASOLINE_DISPLAY
    }

    hyb = search_cars(fuel_types=["Hybrid"])
    assert "PRIUSREALHYBRID03" in {r["vin"] for r in hyb}, "a real hybrid must stay hybrid"
    assert "RAMGASETORQUE0001" not in {r["vin"] for r in hyb}


def test_storage_normalizer_corrects_the_feed_label_without_reading_the_catalog():
    """
    The write-path twin: this is what has to run in ``clean_car_row_dict`` for the
    fuel filters to ever agree with the card. It must decide from the row alone —
    parsers call it once per listing, before any ``epa_master_id`` is resolved.
    """
    import backend.utils.fuel_type_normalize as ftn

    def _boom(car):  # pragma: no cover - must never run
        raise AssertionError("catalog lookup on the parse path")

    original = ftn.catalog_fuel_type_for_car
    ftn.catalog_fuel_type_for_car = _boom
    try:
        assert normalize_fuel_type_for_storage(_ram_1500_etorque()) == GASOLINE_DISPLAY
    finally:
        ftn.catalog_fuel_type_for_car = original


@pytest.mark.parametrize(
    "car",
    [
        {"make": "Toyota", "model": "Prius", "year": 2023, "fuel_type": "Hybrid",
         "engine_description": "2.0L I4 Hybrid"},
        {"make": "Toyota", "model": "RAV4 Prime", "year": 2023, "fuel_type": "Plug-In Hybrid",
         "engine_description": "2.5L I4 Plug-In Hybrid"},
        {"make": "Tesla", "model": "Model 3", "year": 2023, "fuel_type": "Electric",
         "engine_description": "Electric"},
        {"make": "Ram", "model": "1500", "year": 2018, "fuel_type": "Hybrid",
         "engine_description": "5.7L V8"},
    ],
)
def test_storage_normalizer_leaves_everything_else_alone(car):
    assert normalize_fuel_type_for_storage(car) is None


def _upsert_through_the_scanner(rows, tmp_path, monkeypatch, name):
    """Drive the real scanner ingest function against a throwaway sqlite file."""
    import backend.db.inventory_db as inventory_db
    from backend.scanner.database import upsert_vehicles

    db_path = tmp_path / name
    monkeypatch.setenv("INVENTORY_DB_PATH", str(db_path))
    # DB_PATH is resolved at import time, so the env var alone does not move it.
    monkeypatch.setattr(inventory_db, "DB_PATH", str(db_path))
    upsert_vehicles(rows)
    return db_path


def _feed_row(vin, make, model, trim, year, fuel, engine_desc):
    """A parsed feed row shaped the way a platform parser hands it to the upsert."""
    return {
        "vin": vin,
        "title": f"{year} {make} {model} {trim}",
        "year": year,
        "make": make,
        "model": model,
        "trim": trim,
        "price": 55000,
        "mileage": 12,
        "dealer_name": "Dealer",
        "dealer_url": "https://dealer.test/",
        "dealer_id": "d1",
        "fuel_type": fuel,
        "engine_description": engine_desc,
    }


def test_scanner_ingest_stores_gas_for_a_feed_row_labelled_hybrid(tmp_path, monkeypatch):
    """
    The durable half: a rescan must not be able to put "Hybrid" back in the column.

    This drives ``scanner.database.upsert_vehicles`` — the function every platform
    scan writes through — with a feed row that says "Hybrid", exactly as the Ram
    dealers' feeds do, and asserts the STORED value is gas. A real hybrid fed
    through the same call in the same batch must be stored untouched.
    """
    import sqlite3

    db_path = _upsert_through_the_scanner(
        [
            _feed_row("RAMHYBETORQUE0002", "Ram", "1500", "Limited", 2023, "Hybrid",
                      "HEMI 5.7L V8 Multi Displacement VVT eTorque"),
            _feed_row("PRIUSREALHYBRID03", "Toyota", "Prius", "LE", 2023, "Hybrid",
                      "2.0L I4 Hybrid"),
            _feed_row("RAV4PRIMEPLUGIN04", "Toyota", "RAV4 Prime", "SE", 2023,
                      "Plug-In Hybrid", "2.5L I4 Plug-In Hybrid"),
        ],
        tmp_path,
        monkeypatch,
        "inv_ingest.db",
    )

    conn = sqlite3.connect(str(db_path))
    stored = dict(conn.execute("SELECT vin, fuel_type FROM cars").fetchall())
    conn.close()
    assert stored["RAMHYBETORQUE0002"] == GASOLINE_DISPLAY
    assert stored["PRIUSREALHYBRID03"] == "Hybrid"
    assert stored["RAV4PRIMEPLUGIN04"] == "Plug-In Hybrid"


def test_search_cars_agrees_with_the_card_after_a_hybrid_labelled_feed_row(
    tmp_path, monkeypatch
):
    """
    End to end: feed says "Hybrid" -> ingest -> the gas filter returns the truck,
    the hybrid filter does not, and the card it renders reads gas.
    """
    from backend.db.inventory_db import search_cars
    from backend.utils.car_serialize import serialize_car_for_listings_grid

    _upsert_through_the_scanner(
        [
            _feed_row("RAMHYBETORQUE0002", "Ram", "1500", "Limited", 2023, "Hybrid",
                      "HEMI 5.7L V8 Multi Displacement VVT eTorque"),
            _feed_row("PRIUSREALHYBRID03", "Toyota", "Prius", "LE", 2023, "Hybrid",
                      "2.0L I4 Hybrid"),
        ],
        tmp_path,
        monkeypatch,
        "inv_ingest2.db",
    )

    gas = search_cars(makes=["Ram", "RAM"], models=["1500"], fuel_types=["Gasoline"])
    assert "RAMHYBETORQUE0002" in {r["vin"] for r in gas}
    assert {serialize_car_for_listings_grid(dict(r))["fuel_type"] for r in gas} == {
        GASOLINE_DISPLAY
    }

    hyb_vins = {r["vin"] for r in search_cars(fuel_types=["Hybrid"])}
    assert "RAMHYBETORQUE0002" not in hyb_vins
    assert "PRIUSREALHYBRID03" in hyb_vins, "a real hybrid must stay hybrid"


def test_partial_update_cannot_put_hybrid_back_on_a_mild_hybrid_row(
    tmp_path, monkeypatch
):
    """
    ``update_car_row_partial`` is how post-scan gap-fill / window-sticker enrichment
    writes, and both propose "Hybrid" from mild-hybrid engine text. The guard has to
    live at that write too, or enrichment undoes the upsert correction.
    """
    import sqlite3

    from backend.db.inventory_db import update_car_row_partial

    db_path = _upsert_through_the_scanner(
        [
            _feed_row("RAMHYBETORQUE0002", "Ram", "1500", "Limited", 2023, "Gasoline",
                      "HEMI 5.7L V8 Multi Displacement VVT eTorque"),
            _feed_row("PRIUSREALHYBRID03", "Toyota", "Prius", "LE", 2023, "Gasoline",
                      "2.0L I4 Hybrid"),
        ],
        tmp_path,
        monkeypatch,
        "inv_partial.db",
    )
    conn = sqlite3.connect(str(db_path))
    ids = dict(conn.execute("SELECT vin, id FROM cars").fetchall())
    conn.close()

    update_car_row_partial(ids["RAMHYBETORQUE0002"], {"fuel_type": "Hybrid"})
    update_car_row_partial(ids["PRIUSREALHYBRID03"], {"fuel_type": "Hybrid"})

    conn = sqlite3.connect(str(db_path))
    stored = dict(conn.execute("SELECT vin, fuel_type FROM cars").fetchall())
    conn.close()
    assert stored["RAMHYBETORQUE0002"] == GASOLINE_DISPLAY
    assert stored["PRIUSREALHYBRID03"] == "Hybrid"


def test_family_match_reports_the_octane_the_user_described():
    fam = match_mild_hybrid_family(_ram_1500_etorque())
    assert fam is not None
    assert fam.system == "eTorque"
    assert (fam.recommended_octane, fam.minimum_octane) == (89, 87)
