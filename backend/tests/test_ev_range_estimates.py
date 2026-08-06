"""EPA original range resolution for EV battery intelligence.

``factory_range`` anchors the battery state-of-health readout a shopper reads as
the car's real-world range, so the contract pinned here is: EPA's own published
figure or nothing. Three paths were removed on 2026-07-31:

1. a regex over uncited brochure-overlay bullet text (all 6 overlay files that
   yield a range have an inadmissible source; the 2021 Mustang Mach-E file
   claimed 270 mi where EPA publishes 211, the 2021 Taycan file 274 where EPA
   publishes 199);
2. a regex over the dealer's own free-text description;
3. ``epa_extended_specs.ev_range_miles``. That one is a scrape, and it still had
   to go: what those nameplate review pages print is a TOTAL driving range, so
   the column handed a Jeep Wrangler 4xe 400 miles of "all-electric range", a
   BMW 530e 550, a Mercedes-AMG C 63 S E Performance 600, and a 2018 Toyota
   Mirai 402 — a hydrogen car with no plug at all. Serializing all 72,504 active
   listings before and after, 67 cars lose a range and none changes value; all
   67 are plug-in hybrids, Mirais, or one Audi Q4 e-tron. On the 3,500 active
   electrified listings that DO have an EPA row, this column disagreed with EPA
   by more than 5 miles on 2,681 of them — invisible only because EPA won.

Plug-in hybrids are not in EPA's ``vehicles.csv`` (the cache holds 1,451 rows,
all ``atvType=EV``), so a PHEV now shows no factory range. That is the correct
answer to "we do not know it" for a number a health readout is measured against.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.intelligence import ev_range_estimates as ev_range


@pytest.fixture()
def stub_epa_cache(monkeypatch: pytest.MonkeyPatch):
    """Replace the fueleconomy.gov cache lookup with a fixed answer."""

    def _install(miles: int | None):
        monkeypatch.setattr(ev_range, "lookup_epa_range_miles", lambda *a, **k: miles)

    return _install


def test_score_epa_model_match() -> None:
    score = ev_range._score_epa_model_match(
        listing_model="Model Y",
        listing_trim="Long Range",
        epa_model="Model Y Long Range AWD",
    )
    assert score >= 30


def test_lookup_epa_range_miles_from_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cache = {
        "source": "test",
        "rows": [
            {
                "year": 2023,
                "make": "Tesla",
                "make_n": "tesla",
                "model": "Model Y Long Range AWD",
                "range_miles": 330,
            }
        ],
    }
    path = tmp_path / "epa_ev_range_miles.json"
    path.write_text(json.dumps(cache), encoding="utf-8")
    monkeypatch.setattr(ev_range, "_EPA_RANGE_CACHE_PATH", path)
    ev_range._load_epa_ev_range_rows.cache_clear()
    assert ev_range.lookup_epa_range_miles(2023, "Tesla", "Model Y", "Long Range") == 330


def test_lookup_epa_range_miles_token_match(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cache = {
        "source": "test",
        "rows": [
            {
                "year": 2024,
                "make": "Audi",
                "make_n": "audi",
                "model": "Q4 Sportback 55 e-tron quattro",
                "range_miles": 258,
            }
        ],
    }
    path = tmp_path / "epa_ev_range_miles.json"
    path.write_text(json.dumps(cache), encoding="utf-8")
    monkeypatch.setattr(ev_range, "_EPA_RANGE_CACHE_PATH", path)
    ev_range._load_epa_ev_range_rows.cache_clear()
    assert (
        ev_range.lookup_epa_range_miles(2024, "Audi", "Q4 e-tron Sportback", "Premium 55 quattro")
        == 258
    )


# ---------------------------------------------------------------------------
# resolve_factory_epa_range: EPA's published figure or nothing
# ---------------------------------------------------------------------------

_MACH_E = {
    "fuel_type": "Electric",
    "year": 2021,
    "make": "Ford",
    "model": "Mustang Mach-E",
    "trim": "Select",
}


def test_range_comes_from_epa_published_file(stub_epa_cache) -> None:
    stub_epa_cache(211)
    assert ev_range.resolve_factory_epa_range(dict(_MACH_E)) == 211


def test_hidden_when_epa_has_no_row(stub_epa_cache) -> None:
    """The whole point: nothing to cite -> render nothing."""
    stub_epa_cache(None)
    assert ev_range.resolve_factory_epa_range(dict(_MACH_E)) is None


def test_extended_specs_range_is_not_consulted(
    stub_epa_cache, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    A PHEV with no EPA row. ``epa_extended_specs`` has a quotable 400 for every
    Jeep Wrangler — the review page's total driving range, not a battery range —
    and that is what this car used to be given. The extended-specs lookup must
    not be reached at all now, so the stub raises if anything calls it.
    """
    stub_epa_cache(None)

    from backend.intelligence import tco_fuel_estimates as tco

    def _boom(*_a, **_k):  # pragma: no cover - only runs if the source came back
        raise AssertionError("epa_extended_specs must not be consulted for EV range")

    monkeypatch.setattr(tco, "quoted_extended_spec", _boom)
    car = {
        "fuel_type": "Plug-In Hybrid",
        "year": 2024,
        "make": "JEEP",
        "model": "WRANGLER",
        "trim": "Sahara 4xe",
    }
    assert ev_range.resolve_factory_epa_range(car) is None


def test_listing_description_is_not_a_source(stub_epa_cache) -> None:
    """
    Dealer marketing copy is not an admissible source, and a regex over it
    cannot tell a sentence about THIS car from one about the trim above it.
    Measured over all 72,504 active listings: this path resolved a range for 0
    cars, so removing it costs nothing.
    """
    stub_epa_cache(None)
    car = dict(_MACH_E)
    car["description"] = "Dual Motor AWD. 330 miles EPA estimated range. Loaded!"
    car["title"] = "2021 Ford Mustang Mach-E 300 mile range"
    assert ev_range.resolve_factory_epa_range(car) is None


def test_hidden_for_non_electrified_car(stub_epa_cache) -> None:
    """A gas Kona must never inherit the Kona Electric's row."""
    stub_epa_cache(258)
    car = {"fuel_type": "Gasoline", "year": 2023, "make": "Hyundai", "model": "Kona"}
    assert ev_range.resolve_factory_epa_range(car) is None


@pytest.mark.parametrize("miles", [0, -10, 39, 601, 5000, "n/a", None])
def test_hidden_when_outside_physical_band(stub_epa_cache, miles) -> None:
    stub_epa_cache(miles)
    assert ev_range.resolve_factory_epa_range(dict(_MACH_E)) is None


def test_hidden_for_empty_car() -> None:
    assert ev_range.resolve_factory_epa_range(None) is None
    assert ev_range.resolve_factory_epa_range({}) is None


def test_module_no_longer_reads_the_brochure_overlay_json() -> None:
    """
    Regression guard. ``adds_by_trim`` was read straight off
    ``trim_adds_by_year/*.json``, bypassing ``load_brochure_trim_overlay`` and
    the provenance gate behind it. If an overlay path is ever wanted back it
    must go through ``admissible_overlay_adds``, so any direct reference to the
    raw JSON is a bug by construction.

    Checked on the parsed module, not the file's text, so the prose above and in
    the module's own comments does not trip it.
    """
    from backend.tests.test_tco_fuel_estimates import (
        assert_does_not_read_overlay_json,
        imported_names_in,
    )

    assert_does_not_read_overlay_json(ev_range)
    assert not hasattr(ev_range, "_range_miles_from_trim_adds")
    assert not hasattr(ev_range, "parse_epa_range_from_text")
    assert not hasattr(ev_range, "_listing_text_blobs")
    # And no route back to epa_extended_specs, by either of its two names.
    names = imported_names_in(ev_range)
    assert "sourced_extended_specs" not in names
    assert "quoted_extended_spec" not in names
    assert "lookup_epa_extended_specs" not in names


# ---------------------------------------------------------------------------
# Serializer — BOTH shopper-facing range keys
#
# ``factory_range`` feeds the battery-health block; ``ev_range_miles`` feeds the
# spec list at car.html:621 and came from ``merge_verified_specs``, i.e. straight
# off ``epa_extended_specs``, until 2026-07-31. Same fact, one number.
# ---------------------------------------------------------------------------

_MODEL_Y_ROW = {
    "vin": "5YJ3E1EA1KF123456",
    "year": 2023,
    "make": "Tesla",
    "model": "Model Y",
    "trim": "Long Range",
    "fuel_type": "Electric",
}


def test_serialize_ev_includes_factory_range(monkeypatch: pytest.MonkeyPatch) -> None:
    from backend.utils.car_serialize import serialize_car_for_api

    monkeypatch.setattr(ev_range, "lookup_epa_range_miles", lambda *a, **k: 330)
    out = serialize_car_for_api(dict(_MODEL_Y_ROW), include_verified=False)
    assert out["factory_range"] == 330
    assert out["ev_range_miles"] == 330


def test_serialize_hides_factory_range_when_unsourced(monkeypatch: pytest.MonkeyPatch) -> None:
    from backend.utils.car_serialize import serialize_car_for_api

    monkeypatch.setattr(ev_range, "lookup_epa_range_miles", lambda *a, **k: None)
    out = serialize_car_for_api(dict(_MODEL_Y_ROW), include_verified=False)
    assert out["factory_range"] is None
    assert out["ev_range_miles"] is None


def test_serialize_spec_list_range_ignores_merge_verified_specs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    ``verified_specs`` carries the ``epa_extended_specs`` range — 305 for a car
    EPA has no row for. The spec list must not show it.
    """
    from backend.utils.car_serialize import serialize_car_for_api

    monkeypatch.setattr(ev_range, "lookup_epa_range_miles", lambda *a, **k: None)
    out = serialize_car_for_api(
        dict(_MODEL_Y_ROW),
        verified_specs={"ev_range_miles": 305, "battery_kwh": 75.0},
    )
    assert out["ev_range_miles"] is None
    assert out["factory_range"] is None


# ---------------------------------------------------------------------------
# THIRD render path: the generated build sheet
#
# ``generated_spec_sheet.py`` emitted "Electric range" from
# ``verified_specs["ev_range_miles"]`` behind a gate that accepted the token
# "hybrid" anywhere in the fuel string — so a Camry Hybrid, which has no
# battery-only range to quote, got a row. Measured over all 72,504 active
# listings on 2026-07-31 by calling the sheet builder on every one: 11,611
# Electric range rows before, 8,473 of them on a car that cannot be plugged in,
# and the commonest values were 598, 520, 560 and 600 mi — total driving ranges
# scraped off nameplate review pages. After, 3,384, every one EPA's own
# published all-electric range. Of the 2,962 cars that keep a row, the number
# printed CHANGED on 2,801.
# ---------------------------------------------------------------------------

_HYBRID_CAMRY = {
    "vin": "4T1B11HK5KU123456",
    "year": 2022,
    "make": "Toyota",
    "model": "Camry",
    "trim": "Hybrid LE",
    "fuel_type": "Hybrid",
}

_BEV_MODEL_Y = {
    "vin": "5YJ3E1EA1KF123456",
    "year": 2023,
    "make": "Tesla",
    "model": "Model Y",
    "trim": "Long Range",
    "fuel_type": "Electric",
}


def _sheet_rows(car, verified_specs):
    from backend.enrichment.generated_spec_sheet import build_generated_spec_sheet

    sheet = build_generated_spec_sheet(dict(car), dict(verified_specs))
    return {r["label"]: r["value"] for s in sheet["sections"] for r in s["rows"]}


def test_build_sheet_no_electric_range_for_a_conventional_hybrid() -> None:
    """
    A Camry Hybrid cannot be driven on the battery alone, so there is no
    all-electric range to show. ``verified_specs`` offers 598 mi — the car's
    total driving range, mislabelled — and the sheet must ignore it.
    """
    rows = _sheet_rows(_HYBRID_CAMRY, {"ev_range_miles": 598, "battery_kwh": 1.6})
    assert "Electric range" not in rows
    assert "Battery" not in rows


def test_build_sheet_no_electric_range_for_a_gas_car() -> None:
    car = dict(_HYBRID_CAMRY, trim="LE", fuel_type="Gasoline")
    assert "Electric range" not in _sheet_rows(car, {"ev_range_miles": 400})


def test_build_sheet_electric_range_for_a_bev(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ev_range, "lookup_epa_range_miles", lambda *a, **k: 330)
    assert _sheet_rows(_BEV_MODEL_Y, {})["Electric range"] == "330 mi"


def test_build_sheet_electric_range_for_a_plug_in_hybrid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A PHEV does have a battery-only range — it is the row's whole point."""
    monkeypatch.setattr(ev_range, "lookup_epa_range_miles", lambda *a, **k: 42)
    car = dict(_HYBRID_CAMRY, model="RAV4", trim="Prime XSE", fuel_type="Plug-In Hybrid")
    assert _sheet_rows(car, {})["Electric range"] == "42 mi"


def test_build_sheet_hides_electric_range_when_epa_has_no_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No sourced value -> no row, even for a car that certainly has a range."""
    monkeypatch.setattr(ev_range, "lookup_epa_range_miles", lambda *a, **k: None)
    assert "Electric range" not in _sheet_rows(_BEV_MODEL_Y, {})


def test_build_sheet_range_ignores_merge_verified_specs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Even on a BEV, the number must come from EPA and not from the
    ``epa_extended_specs`` value ``merge_verified_specs`` hands over.
    """
    monkeypatch.setattr(ev_range, "lookup_epa_range_miles", lambda *a, **k: None)
    assert "Electric range" not in _sheet_rows(_BEV_MODEL_Y, {"ev_range_miles": 400})


def test_build_sheet_and_serializer_agree_on_the_range(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from backend.utils.car_serialize import serialize_car_for_api

    monkeypatch.setattr(ev_range, "lookup_epa_range_miles", lambda *a, **k: 303)
    out = serialize_car_for_api(dict(_BEV_MODEL_Y), include_verified=False)
    assert out["factory_range"] == 303
    assert out["ev_range_miles"] == 303
    assert _sheet_rows(_BEV_MODEL_Y, {})["Electric range"] == "303 mi"


# ---------------------------------------------------------------------------
# The EPA row has to be about the same car
#
# ``_score_epa_model_match`` scores model and trim together, so a trim string
# shared across a maker's EV lineup ("Long Range AWD", "GT-Line") could outrank
# a model that does not match at all. Every case below was live on 2026-07-31.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("listing_model", "listing_trim", "epa_model", "same"),
    [
        # Rejected — a different nameplate from the same maker.
        ("Model S", "Long Range AWD", "Model 3 Long Range AWD", False),
        ("Model S", "Long Range AWD", "Model Y Long Range AWD", False),
        ("EV6", "GT-Line", "EV9 Long Range AWD GT-Line", False),
        ("Q5", "S line Premium Plus 55 TFSI e quattro", "e-tron quattro", False),
        ("Q5", "S line Premium Plus 55 TFSI e quattro", "Q4 e-tron quattro", False),
        ("EQE", "EQE 350 4MATIC+ Sedan", "EQS 450 4matic", False),
        ("i4", "M60", "i5 eDrive40 Sedan (20 inch Wheels)", False),
        # The head token has to match in BOTH directions, or an Audi e-tron
        # takes a Q4 e-tron's row: "e-tron" is a subset of "Q4 e-tron quattro".
        ("e-tron", "Premium quattro", "Q4 e-tron quattro", False),
        ("Ioniq 5", "SEL", "Ioniq Electric", False),
        # Accepted — the EPA name extends the listing's model name.
        ("Model S", "Long Range AWD", "Model S Long Range", True),
        ("EV6", "GT-Line", "EV6 Long Range AWD (20 inch Wheels)", True),
        ("EQE", "EQE 350 4MATIC+ Sedan", "EQE 350 4matic", True),
        ("i4", "M60", "i4 M60 xDrive Gran Coupe (19 inch Wheels)", True),
        ("Mustang Mach-E", "Premium", "Mustang Mach-E RWD", True),
        ("e-tron", "Premium quattro", "e-tron quattro", True),
        ("Ioniq 5", "SEL", "Ioniq 5 Long Range RWD", True),
        ("ID.4", "S", "ID.4 S", True),
        ("C-HR", "XSE", "C-HR AWD 18inch", True),
        # Accepted the other way round — the LISTING carries the longer name.
        ("C40 Recharge Pure Electric", "Plus", "C40 Recharge", True),
        ("Q4 e-tron Sportback", "Premium 50 quattro", "Q4 e-tron Sportback", True),
        # Accepted despite EPA reordering the words, which is why this is a token
        # test and not a substring test.
        ("Q4 e-tron Sportback", "Premium 55 quattro", "Q4 Sportback 55 e-tron quattro", True),
    ],
)
def test_same_nameplate(listing_model, listing_trim, epa_model, same) -> None:
    assert ev_range._same_nameplate(listing_model, listing_trim, epa_model) is same


def test_same_nameplate_needs_both_names() -> None:
    assert ev_range._same_nameplate("", "Long Range", "Model S") is False
    assert ev_range._same_nameplate("Model S", "Long Range", "") is False
    assert ev_range._same_nameplate(None, None, None) is False


def _install_epa_rows(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, rows) -> None:
    """Point the EPA range cache at a fixed row set (real matcher still runs)."""
    path = tmp_path / "epa_ev_range_miles.json"
    path.write_text(json.dumps({"source": "test", "rows": rows}), encoding="utf-8")
    monkeypatch.setattr(ev_range, "_EPA_RANGE_CACHE_PATH", path)
    ev_range._load_epa_ev_range_rows.cache_clear()


_TESLA_2021_ROWS = [
    {"year": 2021, "make": "Tesla", "make_n": "tesla",
     "model": "Model 3 Long Range AWD", "range_miles": 353},
    {"year": 2021, "make": "Tesla", "make_n": "tesla",
     "model": "Model Y Long Range AWD", "range_miles": 326},
    {"year": 2021, "make": "Tesla", "make_n": "tesla",
     "model": "Model S Long Range", "range_miles": 405},
]


def test_lookup_rejects_a_sibling_models_range(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    A 2021 Model S must not take the Model 3's 353 mi, which is what the score
    alone did: Model 3 and Model Y both scored 95 against the real Model S row's
    70, because the listing trim "Long Range AWD" matches them word for word.
    """
    _install_epa_rows(tmp_path, monkeypatch, _TESLA_2021_ROWS)
    assert ev_range.lookup_epa_range_miles(2021, "Tesla", "Model S", "Long Range AWD") == 405


def test_lookup_shows_nothing_when_only_other_nameplates_match(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    A Q5 55 TFSI e is a plug-in hybrid with roughly 23 miles of electric range.
    EPA's file has no row for it, and the answer to that is a blank — not the
    e-tron's 226, which is what it was showing.
    """
    _install_epa_rows(
        tmp_path,
        monkeypatch,
        [
            {"year": 2023, "make": "Audi", "make_n": "audi",
             "model": "e-tron quattro", "range_miles": 226},
            {"year": 2023, "make": "Audi", "make_n": "audi",
             "model": "Q4 e-tron quattro", "range_miles": 236},
        ],
    )
    assert (
        ev_range.lookup_epa_range_miles(
            2023, "Audi", "Q5", "S line Premium Plus 55 TFSI e quattro"
        )
        is None
    )


def test_lookup_does_not_reach_sideways_across_model_years(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    The adjacent-year window may only reach another year of the SAME nameplate.
    A 2022 Model S with no 2022 row must not fall back to a 2021 Model 3.
    """
    _install_epa_rows(tmp_path, monkeypatch, _TESLA_2021_ROWS[:2])
    assert ev_range.lookup_epa_range_miles(2022, "Tesla", "Model S", "Long Range AWD") is None
