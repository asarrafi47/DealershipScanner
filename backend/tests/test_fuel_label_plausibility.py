"""
"Electric" fuel-label plausibility: gas/hybrid/PHEV cars stored as "Electric"
must display and filter under their evidence-backed label, true EVs must keep
theirs, and junk cylinder counts on real EVs must be zeroed — without the old
guards re-sealing the defect (cylinders erased because the bad label said BEV).
"""

from __future__ import annotations

import pytest

from backend.utils.engine_consistency import cylinders_conflicts_with_engine_text
from backend.utils.field_clean import clean_car_row_dict
from backend.utils.fuel_label_plausibility import (
    IMPLAUSIBLE,
    PHEV_MISLABEL,
    PLAUSIBLE,
    assess_electric_claim,
    combustion_evidence_from_text,
    corrected_label_for_electric_claim,
    cylinders_override_for_electric_claim,
    is_bare_electric_label,
    is_known_bev_nameplate,
    sanitize_cylinder_count,
)
from backend.utils.fuel_type_normalize import normalize_fuel_type_for_display
from backend.utils.spec_field_normalize import extract_cylinder_count


def _gx550(**overrides):
    """The headline defect: a gas twin-turbo V6 stored and displayed as Electric."""
    row = {
        "vin": "JTJKKMFA5R0000001",
        "title": "2024 Lexus GX 550 Premium",
        "year": 2024,
        "make": "Lexus",
        "model": "GX 550",
        "trim": "Premium",
        "fuel_type": "Electric",
        "engine_description": "3.4L V6 Twin Turbo",
        "engine_l": "3.4",
        "cylinders": 6,
        "epa_master_id": None,
    }
    row.update(overrides)
    return row


def _chr_bev(**overrides):
    """True EV carrying the gas sibling's cylinder count from the feed."""
    row = {
        "vin": "JTNADACB0S0000001",
        "title": "2026 Toyota C-HR BEV XLE",
        "year": 2026,
        "make": "Toyota",
        "model": "C-HR BEV",
        "trim": "XLE",
        "fuel_type": "Electric",
        "engine_description": "Electric",
        "cylinders": 4,
        "epa_master_id": None,
    }
    row.update(overrides)
    return row


def _bmw_330e(**overrides):
    """Plug-in hybrid stored as bare Electric."""
    row = {
        "title": "2025 BMW 330e xDrive",
        "year": 2025,
        "make": "BMW",
        "model": "330e",
        "trim": "xDrive",
        "fuel_type": "Electric",
        "engine_description": None,
        "cylinders": 4,
        "epa_master_id": None,
    }
    row.update(overrides)
    return row


# --- the classifier ----------------------------------------------------------


def test_gx550_gas_v6_is_implausible_electric():
    a = assess_electric_claim(_gx550())
    assert a.verdict == IMPLAUSIBLE
    assert a.suggested_label == "Gasoline"


def test_gx550_nameplate_alone_is_enough():
    """No engine text at all: the GX nameplate has no battery-electric variant."""
    a = assess_electric_claim(_gx550(engine_description=None, engine_l=None))
    assert a.verdict == IMPLAUSIBLE
    assert a.suggested_label == "Gasoline"


def test_ioniq5_is_plausible():
    a = assess_electric_claim(
        {
            "title": "2024 Hyundai Ioniq 5 SEL",
            "year": 2024,
            "make": "Hyundai",
            "model": "Ioniq 5",
            "fuel_type": "Electric",
            "engine_description": None,
        }
    )
    assert a.verdict == PLAUSIBLE


def test_330e_is_a_phev_mislabel():
    a = assess_electric_claim(_bmw_330e())
    assert a.verdict == PHEV_MISLABEL
    assert a.suggested_label == "Plug-In Hybrid"


@pytest.mark.parametrize("model", ["530e", "550e xDrive"])
def test_other_bmw_e_suffixes_are_phev_mislabels(model):
    a = assess_electric_claim(_bmw_330e(model=model, title=f"2025 BMW {model}"))
    assert a.verdict == PHEV_MISLABEL


def test_chr_bev_with_junk_cylinders_is_plausible():
    """cylinders=4 column junk must NOT count as combustion evidence."""
    a = assess_electric_claim(_chr_bev())
    assert a.verdict == PLAUSIBLE


def test_bmw_i4_is_plausible_even_with_layout_shaped_junk_text():
    """The i4's own name looks like a layout token; the BEV table wins first."""
    a = assess_electric_claim(
        {
            "title": "2024 BMW i4 eDrive40",
            "year": 2024,
            "make": "BMW",
            "model": "i4",
            "fuel_type": "Electric",
            "engine_description": "I4",
            "cylinders": 4,
        }
    )
    assert a.verdict == PLAUSIBLE


@pytest.mark.parametrize(
    ("model", "expected"),
    [("ES 350h", "Hybrid"), ("TX 500h", "Hybrid")],
)
def test_lexus_h_suffix_hybrids_route_to_hybrid(model, expected):
    a = assess_electric_claim(
        _gx550(model=model, title=f"2025 Lexus {model}", engine_description=None, engine_l=None)
    )
    assert a.verdict == IMPLAUSIBLE
    assert a.suggested_label == expected


def test_lexus_h_plus_suffix_routes_to_plug_in_hybrid():
    a = assess_electric_claim(
        _gx550(model="RX 450h+", title="2025 Lexus RX 450h+", engine_description=None, engine_l=None)
    )
    assert a.verdict == PHEV_MISLABEL


def test_bmw_gas_i_suffix_trims_are_implausible_electric():
    for model, trim in (("X5", "xDrive40i"), ("X1", "xDrive28i"), ("330i", ""), ("X5", "M60i")):
        a = assess_electric_claim(
            {
                "title": f"2024 BMW {model} {trim}".strip(),
                "year": 2024,
                "make": "BMW",
                "model": model,
                "trim": trim,
                "fuel_type": "Electric",
                "engine_description": None,
            }
        )
        assert a.verdict == IMPLAUSIBLE, (model, trim)
        assert a.suggested_label == "Gasoline"


def test_kona_gas_engine_text_vs_kona_electric():
    gas = assess_electric_claim(
        {
            "year": 2024, "make": "Hyundai", "model": "Kona", "fuel_type": "Electric",
            "engine_description": "2.0L 4-Cylinder",
        }
    )
    assert (gas.verdict, gas.suggested_label) == (IMPLAUSIBLE, "Gasoline")
    ev = assess_electric_claim(
        {
            "year": 2024, "make": "Hyundai", "model": "Kona Electric", "fuel_type": "Electric",
            "engine_description": None,
        }
    )
    assert ev.verdict == PLAUSIBLE


def test_honda_pilot_nameplate_has_no_ev_variant():
    a = assess_electric_claim(
        {"year": 2024, "make": "Honda", "model": "Pilot", "fuel_type": "Electric",
         "engine_description": None}
    )
    assert (a.verdict, a.suggested_label) == (IMPLAUSIBLE, "Gasoline")


def test_unknown_nameplate_with_no_evidence_stays_plausible():
    """Conservative: nothing to go on -> leave the label alone."""
    a = assess_electric_claim(
        {"year": 2024, "make": "Ford", "model": "F-150", "fuel_type": "Electric",
         "engine_description": None}
    )
    assert a.verdict == PLAUSIBLE


def test_corrected_label_only_fires_on_bare_electric_labels():
    assert corrected_label_for_electric_claim(_gx550(fuel_type="Gasoline")) is None
    assert corrected_label_for_electric_claim(_gx550(fuel_type="Plug-In Hybrid")) is None
    assert corrected_label_for_electric_claim(_gx550()) == "Gasoline"


@pytest.mark.parametrize("label", ["Electric", "electric", "EV", "BEV", "Electricity"])
def test_bare_electric_labels(label):
    assert is_bare_electric_label(label) is True


@pytest.mark.parametrize(
    "label", [None, "", "Hybrid", "Plug-In Hybrid", "Gasoline / Electric", "Gas", "Hydrogen"]
)
def test_non_bare_electric_labels(label):
    assert is_bare_electric_label(label) is False


def test_electric_g_class_e_suffix_is_not_a_phev_mislabel():
    """
    Live false-positive found on dry-run: the electric G-Class is fed with a
    "G 580e" TRIM whose e-suffix looks like a PHEV designation. It is a BEV —
    the nameplate table must win before the PHEV pattern, including when the
    identifying token lives in ``trim`` rather than ``model``.
    """
    car = {
        "year": 2027,
        "make": "Mercedes-Benz",
        "model": "G-Class",
        "trim": "G 580e SUV",
        "fuel_type": "Electric",
        "engine_description": None,
        "cylinders": 8,
    }
    a = assess_electric_claim(car)
    assert a.verdict == PLAUSIBLE
    assert cylinders_override_for_electric_claim(car) == 0


def test_known_bev_table_examples():
    assert is_known_bev_nameplate({"make": "Chevrolet", "model": "Blazer EV"}) is True
    assert is_known_bev_nameplate({"make": "Chevrolet", "model": "Silverado EV"}) is True
    assert is_known_bev_nameplate({"make": "Toyota", "model": "C-HR", "year": 2026}) is True
    assert is_known_bev_nameplate({"make": "Toyota", "model": "C-HR", "year": 2021}) is False
    assert is_known_bev_nameplate({"make": "Lexus", "model": "GX 550"}) is False


# --- the read-time correction (display + facet must agree) -------------------


def test_normalize_display_routes_gx550_to_gasoline():
    assert normalize_fuel_type_for_display(_gx550(), catalog_fuel_type=None) == "Gasoline"


def test_normalize_display_routes_330e_to_plug_in_hybrid():
    assert (
        normalize_fuel_type_for_display(_bmw_330e(), catalog_fuel_type=None)
        == "Plug-In Hybrid"
    )


def test_normalize_display_leaves_true_evs_alone():
    for car in (_chr_bev(), {"make": "Tesla", "model": "Model 3", "year": 2023,
                             "fuel_type": "Electric", "engine_description": "Electric"}):
        assert normalize_fuel_type_for_display(car, catalog_fuel_type=None) is None


def test_catalog_that_says_electric_only_wins_and_abstains():
    """A linked catalog row calling the trim a BEV outranks the heuristics."""
    assert normalize_fuel_type_for_display(_gx550(), catalog_fuel_type="Electricity") is None
    # Dual-fuel catalog strings (PHEV) do not protect a bare-electric label.
    assert (
        normalize_fuel_type_for_display(
            _bmw_330e(), catalog_fuel_type="Premium Gasoline / Electricity"
        )
        == "Plug-In Hybrid"
    )


def test_card_and_facet_agree_for_a_gas_marked_electric_car():
    """
    The stored column still says "Electric" (rows are never bulk-relabelled);
    the CARD serializer and the FACET cascade must both surface "Gasoline", or
    picking the corrected facet would show an empty grid — the consistency that
    was just fixed for mild hybrids must hold here too.
    """
    from backend.utils.car_serialize import serialize_car_for_listings_grid

    card = serialize_car_for_listings_grid(_gx550())["fuel_type"]
    # Same projection the facet cascade builds (no title, no epa_master_id).
    facet = normalize_fuel_type_for_display(
        {
            "make": "Lexus",
            "model": "GX 550",
            "trim": "Premium",
            "year": 2024,
            "engine_description": "3.4L V6 Twin Turbo",
            "engine_l": "3.4",
        },
        fuel_type="Electric",
        catalog_fuel_type=None,
    )
    assert card == facet == "Gasoline"


def test_card_shows_plug_in_hybrid_for_a_330e_stored_electric():
    from backend.utils.car_serialize import serialize_car_for_listings_grid

    assert serialize_car_for_listings_grid(_bmw_330e())["fuel_type"] == "Plug-In Hybrid"


def test_card_keeps_electric_for_a_true_ev():
    from backend.utils.car_serialize import serialize_car_for_listings_grid

    assert serialize_car_for_listings_grid(_chr_bev())["fuel_type"] == "Electric"


# --- the un-sealed guards ----------------------------------------------------


def test_engine_consistency_no_longer_erases_combustion_evidence():
    """cylinders=6 + engine "3.4L V6" + fuel "Electric": the LABEL is the liar."""
    assert cylinders_conflicts_with_engine_text(6, "3.4L V6 Twin Turbo", "Electric") is False


def test_engine_consistency_still_blocks_junk_counts_on_real_evs():
    assert cylinders_conflicts_with_engine_text(4, "Electric Motor", "Electric") is True
    assert cylinders_conflicts_with_engine_text(4, None, "Electric") is True
    assert cylinders_conflicts_with_engine_text(4, "", "BEV") is True


def test_engine_consistency_layout_mismatch_unchanged():
    assert cylinders_conflicts_with_engine_text(4, "3.5L V6", "Gasoline") is True
    assert cylinders_conflicts_with_engine_text(6, "3.5L V6", "Gasoline") is False


def test_extract_cylinder_count_no_longer_zeroes_combustion_text():
    assert extract_cylinder_count("3.4L V6 Twin Turbo", None, fuel_type="Electric") == 6
    assert extract_cylinder_count("Electric Motor", None, fuel_type="Electric") == 0
    assert extract_cylinder_count("Electric", None, fuel_type="Electric") == 0


def test_combustion_evidence_ignores_ev_engine_text():
    assert combustion_evidence_from_text("Electric Motor, 77.4 kWh battery") is None
    assert combustion_evidence_from_text(None) is None
    assert combustion_evidence_from_text("3.4L V6") is not None
    assert combustion_evidence_from_text("8 Cylinder") is not None


# --- write-time cylinder hygiene ---------------------------------------------


def test_sanitize_cylinder_count():
    assert sanitize_cylinder_count(99) is None
    assert sanitize_cylinder_count("99") is None
    assert sanitize_cylinder_count(8) == 8
    assert sanitize_cylinder_count(0) == 0
    assert sanitize_cylinder_count("junk") is None
    assert sanitize_cylinder_count(None) is None


def test_clean_car_row_dict_nulls_the_gm_sentinel():
    assert clean_car_row_dict({"cylinders": 99})["cylinders"] is None
    assert clean_car_row_dict({"cylinders": 8})["cylinders"] == 8
    assert clean_car_row_dict({"cylinders": 0})["cylinders"] == 0


def test_cylinders_override_zeroes_plausible_evs_only():
    # True EV with the gas sibling's count: zero it.
    assert cylinders_override_for_electric_claim(_chr_bev()) == 0
    # Gas car marked Electric: the count IS the evidence — keep it.
    assert cylinders_override_for_electric_claim(_gx550()) is None
    # PHEV mislabel: it really has cylinders — keep them.
    assert cylinders_override_for_electric_claim(_bmw_330e()) is None
    # Not an electric claim at all.
    assert cylinders_override_for_electric_claim(_gx550(fuel_type="Gasoline")) is None


# --- the scanner ingest path -------------------------------------------------


def _upsert_through_the_scanner(rows, tmp_path, monkeypatch, name):
    import backend.db.inventory_db as inventory_db
    from backend.scanner.database import upsert_vehicles

    db_path = tmp_path / name
    monkeypatch.setenv("INVENTORY_DB_PATH", str(db_path))
    monkeypatch.setattr(inventory_db, "DB_PATH", str(db_path))
    upsert_vehicles(rows)
    return db_path


def _feed_row(vin, make, model, trim, year, fuel, engine_desc, cylinders):
    return {
        "vin": vin,
        "title": f"{year} {make} {model} {trim}".strip(),
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
        "cylinders": cylinders,
    }


def test_scanner_ingest_write_guard(tmp_path, monkeypatch):
    """
    One batch, all four shapes of the defect:
      - gas GX 550 fed "Electric": label corrected, cylinders KEPT (the evidence)
      - C-HR BEV fed cylinders=4: count zeroed, label kept
      - Blazer EV fed the 99 sentinel: count stored as 0
      - Ioniq 5 fed clean: untouched
    """
    import sqlite3

    db_path = _upsert_through_the_scanner(
        [
            _feed_row("GX550GASELECTRIC1", "Lexus", "GX 550", "Premium", 2024,
                      "Electric", "3.4L V6 Twin Turbo", 6),
            _feed_row("CHRBEVJUNKCYL0002", "Toyota", "C-HR BEV", "XLE", 2026,
                      "Electric", "Electric", 4),
            _feed_row("BLAZEREVSENTINEL3", "Chevrolet", "Blazer EV", "LT", 2025,
                      "Electric", None, 99),
            _feed_row("IONIQ5CLEANROW004", "Hyundai", "Ioniq 5", "SEL", 2025,
                      "Electric", None, 0),
        ],
        tmp_path,
        monkeypatch,
        "inv_fuel_guard.db",
    )

    conn = sqlite3.connect(str(db_path))
    stored = {
        vin: (fuel, cyl)
        for vin, fuel, cyl in conn.execute(
            "SELECT vin, fuel_type, cylinders FROM cars"
        ).fetchall()
    }
    conn.close()
    assert stored["GX550GASELECTRIC1"] == ("Gasoline", 6)
    assert stored["CHRBEVJUNKCYL0002"] == ("Electric", 0)
    assert stored["BLAZEREVSENTINEL3"] == ("Electric", 0)
    assert stored["IONIQ5CLEANROW004"] == ("Electric", 0)


def test_partial_update_cannot_relabel_a_gas_row_electric(tmp_path, monkeypatch):
    """Post-scan enrichment proposing "Electric" onto a gas GX 550 is corrected."""
    import sqlite3

    from backend.db.inventory_db import update_car_row_partial

    db_path = _upsert_through_the_scanner(
        [
            _feed_row("GX550GASELECTRIC1", "Lexus", "GX 550", "Premium", 2024,
                      "Gasoline", "3.4L V6 Twin Turbo", 6),
            _feed_row("IONIQ5CLEANROW004", "Hyundai", "Ioniq 5", "SEL", 2025,
                      "Electric", None, 0),
        ],
        tmp_path,
        monkeypatch,
        "inv_fuel_partial.db",
    )
    conn = sqlite3.connect(str(db_path))
    ids = dict(conn.execute("SELECT vin, id FROM cars").fetchall())
    conn.close()

    update_car_row_partial(ids["GX550GASELECTRIC1"], {"fuel_type": "Electric"})
    # A real EV proposing its own label is untouched; a 99 sentinel is nulled.
    update_car_row_partial(ids["IONIQ5CLEANROW004"], {"fuel_type": "Electric"})
    update_car_row_partial(ids["GX550GASELECTRIC1"], {"cylinders": 99})

    conn = sqlite3.connect(str(db_path))
    stored = {
        vin: (fuel, cyl)
        for vin, fuel, cyl in conn.execute(
            "SELECT vin, fuel_type, cylinders FROM cars"
        ).fetchall()
    }
    conn.close()
    assert stored["GX550GASELECTRIC1"] == ("Gasoline", None)
    assert stored["IONIQ5CLEANROW004"][0] == "Electric"
