"""TCO MPG average and fuel-tank resolution.

The tank size is multiplied by a live fuel price and shown to a shopper as the
cost of a fill-up, so the contract these tests pin is: a number QUOTED FROM A
PAGE WE CAN NAME, or nothing.

Two rounds of that. Until 2026-07-31 ``resolve_fuel_tank_gallons`` could never
return ``None`` — it fell through a regex over uncited brochure-overlay bullet
text, a 54-entry curated table, a body-style keyword guess and finally a flat
15.5 gal. The first rewrite pointed it at ``epa_extended_specs`` and called the
result sourced. It was not:

* the lookup it used, ``knowledge_engine.lookup_epa_extended_specs``, ends in
  ``_merge_ai_model_specs``, and ``fuel_tank_gal`` is one of the columns that
  merge supplies from the AI-written ``ai_model_specs`` table;
* and the ``epa_extended_specs.fuel_tank_gal`` column is itself 99.9% our own
  derivation — of the 48,658 rows carrying a tank, 31,977 say
  ``fuel_tank_source: body_style`` (8 distinct values across all of them), 9,025
  say ``model_defaults``, 7,023 say ``class_default`` (one number, 15.5, for
  every one), 383 say ``trim_adds`` (parsed out of the LLM bullets), and 250
  carry no flag. Only 59 rows have the number in a page extraction.

So the admission rule is now the page extraction itself, and these tests pin it
on both sides: a value present in ``specs_json['pages'][*]['extracted']`` is
shown with its URL; everything else is silence.

Serializing all 72,504 active listings before and after, the spec-list key
``fuel_tank_gal`` goes 54,387 -> 0 and the TCO key ``fuel_tank_gallons`` goes
53,801 -> 0. Nothing survives because nothing qualifies: the only 59 rows in the
table with a page-extracted tank are 1984-93 Dodge Daytonas, and no active
listing is one. Attributing those 54,387 by where the number came from: 24,446
``model_defaults``, 19,047 ``body_style``, 8,745 ``ai_model_specs``, 1,144
``class_default``, 304 ``ai_engine_specs``, 272 ``trim_adds``, 5 unflagged and
424 the replay could not attribute. Zero sourced. Showing no tank at all is the
honest state until a real per-trim source is acquired.
"""

from __future__ import annotations

import pytest

from backend.intelligence import tco_fuel_estimates as tco
from backend.intelligence.tco_fuel_estimates import (
    average_mpg_city_highway,
    compute_fill_up_cost_usd,
    miles_per_tank,
    quoted_extended_spec,
    resolve_fuel_tank_gallons,
    resolve_tco_avg_mpg,
    resolve_tco_ev_efficiency,
)
from backend.utils.car_serialize import serialize_car_for_api


@pytest.fixture(autouse=True)
def _no_cached_rows():
    """
    The row lookups are memoized, so a real row read by one test must not answer
    for another. Cleared on the way in and out.
    """
    tco.clear_quoted_extended_spec_cache()
    yield
    tco.clear_quoted_extended_spec_cache()


@pytest.fixture()
def stub_quoted(monkeypatch: pytest.MonkeyPatch):
    """
    Replace the ``epa_extended_specs`` row lookup with a fixed set of quotes.

    Stubs one level BELOW :func:`quoted_extended_spec` so the tests that use it
    still run the real admission logic in that function.
    """

    def _install(quotes: dict[str, tuple[float, str]] | None):
        monkeypatch.setattr(
            tco, "_quoted_specs_for_car", lambda car: dict(quotes or {})
        )

    return _install


@pytest.fixture()
def tank_quote(stub_quoted):
    """Shorthand: this car's tank is quoted at *gallons* from a real page."""

    def _install(gallons):
        stub_quoted({"fuel_tank_gal": (gallons, "https://www.motortrend.com/cars/dodge/daytona/")})

    return _install


def assert_does_not_read_overlay_json(module) -> None:
    """
    Fail if *module* touches ``trim_adds_by_year/*.json`` again.

    Inspects the parsed module (imports, attributes, exact string constants) so
    that prose in docstrings and comments describing the removed path does not
    trip it.
    """
    import ast
    from pathlib import Path

    tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"))
    imported_modules = {
        node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module
    }
    imported_names = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        for alias in node.names
    }
    literals = {
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    }
    assert "backend.enrichment.dictionary_paths" not in imported_modules
    assert "backend.enrichment.dictionary_catalog" not in imported_modules
    assert "TRIM_ADDS_BY_YEAR_DIR" not in imported_names
    assert "catalog_key" not in imported_names
    assert "adds_by_trim" not in literals
    assert not hasattr(module, "TRIM_ADDS_BY_YEAR_DIR")
    assert not hasattr(module, "catalog_key")


def imported_names_in(module) -> set[str]:
    """Names *module* imports, from the parsed source (comments cannot trip it)."""
    import ast
    from pathlib import Path

    tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"))
    return {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        for alias in node.names
    }


# ---------------------------------------------------------------------------
# MPG helpers (unchanged behaviour)
# ---------------------------------------------------------------------------


def test_average_mpg_city_highway_mean() -> None:
    assert average_mpg_city_highway(20, 30) == 25.0


def test_average_mpg_city_highway_single_value() -> None:
    assert average_mpg_city_highway(22, None) == 22.0
    assert average_mpg_city_highway(None, 28) == 28.0


def test_resolve_tco_avg_mpg_defaults() -> None:
    assert resolve_tco_avg_mpg({"mpg_city": 24, "mpg_highway": 32}) == 28.0
    assert resolve_tco_avg_mpg({}) == 25.0


def test_resolve_tco_ev_efficiency_from_mpge() -> None:
    assert resolve_tco_ev_efficiency({"mpg_city": 120, "mpg_highway": 100}) == 30.6


# ---------------------------------------------------------------------------
# The admission rule: only what a named page reported
# ---------------------------------------------------------------------------

#: Shape of a row whose tank the scrape actually read off the page — the 250
#: unflagged rows. Trimmed from the live row for the 1984 Dodge Daytona.
_QUOTED_ROW_SPECS_JSON = {
    "pages": [
        {
            "url": "https://www.motortrend.com/cars/dodge/daytona/",
            "title": "Motor Trend: Dodge at Daytona",
            "extracted": {"fuel_tank_gal": 22.0, "body_style_detail": "truck"},
        }
    ],
    "fuel_tank_gal": 22.0,
    "body_style_detail": "truck",
}

#: Shape of a row whose tank one of our own scripts wrote. The page WAS scraped
#: — for horsepower — and the tank was written over the top of it afterwards,
#: which is why a ``source_url`` on the row proves nothing about the tank.
#: Trimmed from the live row for the 2004 Volvo V70 AWD.
_BODY_STYLE_ROW_SPECS_JSON = {
    "pages": [
        {
            "url": "https://www.caranddriver.com/volvo/v70",
            "title": "2010 Volvo V70 Review, Pricing, and Specs",
            "extracted": {"horsepower": 282, "body_style_detail": "wagon"},
        }
    ],
    "fuel_tank_gal": 16.0,
    "fuel_tank_source": "body_style",
}


def test_page_extracted_value_is_quoted_with_its_url() -> None:
    quoted = tco._quotable_from_specs_json(
        {"fuel_tank_gal": 22.0}, _QUOTED_ROW_SPECS_JSON
    )
    assert dict(quoted) == {
        "fuel_tank_gal": (22.0, "https://www.motortrend.com/cars/dodge/daytona/")
    }


def test_body_style_default_is_not_quotable() -> None:
    """
    The row has a stored tank, a scraped page and a ``source_url``. None of the
    pages reported a tank, so there is nothing to quote — this is the 31,977-row
    case and the single biggest reason the figure disappears.
    """
    assert tco._quotable_from_specs_json({"fuel_tank_gal": 16.0}, _BODY_STYLE_ROW_SPECS_JSON) == ()


def test_top_level_copy_alone_is_not_a_quote() -> None:
    """``specs_json['fuel_tank_gal']`` is where the default-fillers wrote."""
    payload = {"pages": [], "fuel_tank_gal": 15.5, "fuel_tank_source": "class_default"}
    assert tco._quotable_from_specs_json({"fuel_tank_gal": 15.5}, payload) == ()


def test_quote_must_match_the_stored_value() -> None:
    """A page that reported a DIFFERENT number does not vouch for this one."""
    payload = {
        "pages": [
            {
                "url": "https://example.invalid/x",
                "extracted": {"fuel_tank_gal": 26.0},
            }
        ]
    }
    assert tco._quotable_from_specs_json({"fuel_tank_gal": 15.5}, payload) == ()


def test_quote_tolerates_json_float_round_trip() -> None:
    payload = {
        "pages": [
            {"url": "https://example.invalid/x", "extracted": {"fuel_tank_gal": 22.02}}
        ]
    }
    assert dict(tco._quotable_from_specs_json({"fuel_tank_gal": 22.0}, payload)) == {
        "fuel_tank_gal": (22.0, "https://example.invalid/x")
    }


def test_quote_needs_a_url_to_name() -> None:
    payload = {"pages": [{"extracted": {"fuel_tank_gal": 22.0}}]}
    assert tco._quotable_from_specs_json({"fuel_tank_gal": 22.0}, payload) == ()


def test_specs_json_accepted_as_text() -> None:
    """psycopg hands back a dict; a SQLite/text column hands back a string."""
    import json

    quoted = tco._quotable_from_specs_json(
        {"fuel_tank_gal": 22.0}, json.dumps(_QUOTED_ROW_SPECS_JSON)
    )
    assert dict(quoted)["fuel_tank_gal"][0] == 22.0


def test_unparseable_specs_json_quotes_nothing() -> None:
    assert tco._quotable_from_specs_json({"fuel_tank_gal": 22.0}, "not json") == ()
    assert tco._quotable_from_specs_json({"fuel_tank_gal": 22.0}, None) == ()


def test_ev_range_is_not_a_quotable_field(stub_quoted) -> None:
    """
    It passes the quote test and is still refused: the number those nameplate
    pages print is a TOTAL driving range (400 miles of "all-electric range" for
    a Jeep Wrangler 4xe). Being quotable is necessary, not sufficient.
    """
    stub_quoted({"ev_range_miles": (400, "https://www.caranddriver.com/jeep/wrangler")})
    car = {"year": 2024, "make": "JEEP", "model": "WRANGLER", "fuel_type": "Plug-In Hybrid"}
    assert quoted_extended_spec(car, "ev_range_miles") is None


# ---------------------------------------------------------------------------
# Fuel tank: quoted or nothing
# ---------------------------------------------------------------------------


def test_tank_comes_from_a_quoted_page(tank_quote) -> None:
    tank_quote(15.8)
    car = {"year": 2020, "make": "Toyota", "model": "Camry", "body_style": "Sedan"}
    assert resolve_fuel_tank_gallons(car) == 15.8


def test_tank_hidden_when_nothing_is_quoted(stub_quoted) -> None:
    """The whole point: nothing to cite -> render nothing, not a class average."""
    stub_quoted({})
    car = {"year": 2015, "make": "Unknown", "model": "Truck", "body_style": "Pickup"}
    assert resolve_fuel_tank_gallons(car) is None


def test_tank_hidden_when_row_quotes_only_other_fields(stub_quoted) -> None:
    stub_quoted({"ev_range_miles": (250, "https://example.invalid/x")})
    car = {"year": 2019, "make": "Honda", "model": "Accord", "body_style": "Sedan"}
    assert resolve_fuel_tank_gallons(car) is None


def test_tank_hidden_for_pure_battery_electric(tank_quote) -> None:
    """
    A BEV has no fuel tank. epa_extended_specs matches at MODEL level, so a
    Kona Electric can be handed the gas Kona's row and vice versa.
    """
    tank_quote(13.2)
    car = {"year": 2023, "make": "Hyundai", "model": "Kona", "fuel_type": "Electric"}
    assert resolve_fuel_tank_gallons(car) is None


def test_tank_kept_for_plug_in_hybrid(tank_quote) -> None:
    """A PHEV is an ordinary engine with an assist motor — it does have a tank."""
    tank_quote(11.3)
    car = {"year": 2023, "make": "Toyota", "model": "RAV4", "fuel_type": "Plug-In Hybrid"}
    assert resolve_fuel_tank_gallons(car) == 11.3


@pytest.mark.parametrize("gallons", [0, -5, 2.0, 61.0, 400.0])
def test_tank_hidden_when_outside_physical_band(tank_quote, gallons) -> None:
    tank_quote(gallons)
    car = {"year": 2020, "make": "Ford", "model": "F-150"}
    assert resolve_fuel_tank_gallons(car) is None


def test_tank_hidden_for_empty_car() -> None:
    assert resolve_fuel_tank_gallons(None) is None
    assert resolve_fuel_tank_gallons({}) is None


def test_module_no_longer_reads_the_brochure_overlay_json() -> None:
    """
    Regression guard for the defect this module was rewritten to close.

    ``adds_by_trim`` was read straight off ``trim_adds_by_year/*.json``, which
    bypasses ``load_brochure_trim_overlay`` and the provenance gate behind it.
    All 97 overlay files that yield a tank number have an inadmissible source.
    If an overlay path is ever wanted back it must go through
    ``admissible_overlay_adds``, so any direct reference to the raw JSON is a
    bug by construction.

    Checked on the parsed module, not on the file's text, so the prose above and
    in the module's own comments does not trip it.
    """
    assert_does_not_read_overlay_json(tco)


def test_module_does_not_use_the_ai_merging_lookups() -> None:
    """
    Regression guard for the second defect: ``lookup_epa_extended_specs`` and
    ``lookup_epa_extended_specs_by_master_id`` both finish by merging
    ``ai_model_specs``, which supplies ``fuel_tank_gal``. Reading the table
    directly is the whole fix, so importing either of them here puts the AI
    numbers straight back into the fill-up cost.
    """
    names = imported_names_in(tco)
    assert "lookup_epa_extended_specs" not in names
    assert "lookup_epa_extended_specs_by_master_id" not in names
    assert not hasattr(tco, "sourced_extended_specs")


# ---------------------------------------------------------------------------
# Downstream arithmetic tolerates the missing value
# ---------------------------------------------------------------------------


def test_compute_fill_up_cost() -> None:
    assert compute_fill_up_cost_usd(3.5, 15.8) == 55.3


def test_compute_fill_up_cost_none_when_tank_unknown() -> None:
    assert compute_fill_up_cost_usd(3.5, None) is None
    assert compute_fill_up_cost_usd(None, 15.8) is None


def test_miles_per_tank_none_when_tank_unknown() -> None:
    assert miles_per_tank(30.0, 15.0) == 450
    assert miles_per_tank(30.0, None) is None
    assert miles_per_tank(None, 15.0) is None


# ---------------------------------------------------------------------------
# Serializer — BOTH shopper-facing tank keys
#
# ``fuel_tank_gallons`` feeds the TCO fill-up block; ``fuel_tank_gal`` feeds the
# spec list at car.html:630 and was fed from ``merge_verified_specs`` until
# 2026-07-31, i.e. from the AI-merging lookup. They are the same fact, so they
# have to carry the same number and disappear together.
# ---------------------------------------------------------------------------

_CAMRY_ROW = {
    "vin": "1HGBH41JXMN109186",
    "year": 2019,
    "make": "Toyota",
    "model": "Camry",
    "mpg_city": 28,
    "mpg_highway": 39,
    "zip_code": "27513",
}


def test_serialize_car_includes_tco_fuel_fields(tank_quote) -> None:
    tank_quote(15.8)
    out = serialize_car_for_api(dict(_CAMRY_ROW), include_verified=False)
    assert out["tco_avg_mpg"] == 33.5
    assert out["fuel_tank_gallons"] == 15.8


def test_serialize_sets_both_tank_keys_from_the_quote(tank_quote) -> None:
    tank_quote(15.8)
    out = serialize_car_for_api(dict(_CAMRY_ROW), include_verified=False)
    assert out["fuel_tank_gal"] == 15.8
    assert out["fuel_tank_gallons"] == out["fuel_tank_gal"]


def test_serialize_hides_tank_when_unquoted(stub_quoted) -> None:
    """
    The path that used to raise ``TypeError`` on ``round(None, 1)``, and the
    reason the spec-list key was a separate bug: it kept rendering a tank from
    ``merge_verified_specs`` after the TCO key had gone blank.
    """
    stub_quoted({})
    out = serialize_car_for_api(dict(_CAMRY_ROW), include_verified=False)
    assert out["fuel_tank_gallons"] is None
    assert out["fuel_tank_gal"] is None


def test_serialize_spec_list_tank_ignores_merge_verified_specs(stub_quoted) -> None:
    """
    ``verified_specs`` is passed in with a tank — the shape ``merge_verified_specs``
    returns after ``_merge_ai_model_specs`` has filled it from ``ai_model_specs``.
    The spec list must not show it.
    """
    stub_quoted({})
    out = serialize_car_for_api(
        dict(_CAMRY_ROW),
        verified_specs={"fuel_tank_gal": 15.8, "horsepower": 203},
    )
    assert out["fuel_tank_gal"] is None
    assert out["horsepower"] == 203


def test_serialize_does_not_fill_tank_from_ai_engine_specs(
    stub_quoted, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    ``ai_engine_specs`` is AI-researched end to end (every row carries
    ``source_host='ai-engine-research'``; 847 of its 888 rows have a tank). It
    used to fill ``fuel_tank_gal`` whenever the model-level value was missing,
    which is precisely the case the quote rule now creates.

    2026-08-02: the whole override was retired, not just the tank field (see
    ``car_serialize/serialize.py`` "THE ai_engine_specs OVERRIDE IS GONE" and
    ``test_spec_guardrails.test_ai_engine_specs_has_no_production_caller``), so
    ``lookup_engine_specs`` has no caller left to monkeypatch through — this
    stub is now inert and horsepower must come back blank like the tank does.
    """
    stub_quoted({})
    import backend.enrichment.knowledge_engine as ke

    monkeypatch.setattr(
        ke,
        "lookup_engine_specs",
        lambda *a, **k: {"fuel_tank_gal": 26.0, "horsepower": 395, "curb_weight_lb": 5100},
    )
    row = dict(_CAMRY_ROW)
    row.update({"make": "RAM", "model": "1500", "engine_description": "5.7L V8"})
    out = serialize_car_for_api(row, include_verified=False)
    assert out["fuel_tank_gal"] is None
    assert out["fuel_tank_gallons"] is None
    assert out["horsepower"] is None


def test_bev_tank_is_suppressed_through_the_serializer(tank_quote) -> None:
    tank_quote(15.0)
    row = {
        "vin": "5YJ3E1EA1KF123456",
        "year": 2023,
        "make": "Tesla",
        "model": "Model 3",
        "fuel_type": "Electric",
    }
    out = serialize_car_for_api(row, include_verified=False)
    assert out["fuel_tank_gal"] is None
    assert out["fuel_tank_gallons"] is None


def test_serialize_ev_includes_tco_ev_efficiency() -> None:
    row = {
        "vin": "5YJ3E1EA1KF123456",
        "year": 2023,
        "make": "Tesla",
        "model": "Model 3",
        "fuel_type": "Electric",
        "mpg_city": 134,
        "mpg_highway": 126,
    }
    out = serialize_car_for_api(row, include_verified=False)
    assert out["tco_ev_efficiency"] == 25.9


# ---------------------------------------------------------------------------
# THIRD render path: the generated build sheet
#
# ``backend/enrichment/generated_spec_sheet.py`` emits its own "Fuel tank" row
# and is reached from a different route branch than the serializer above — the
# build-sheet panel on car.html, shown whenever a car has no real window
# sticker. It read ``verified_specs["fuel_tank_gal"]`` straight, which is the
# output of ``merge_verified_specs`` -> ``lookup_epa_extended_specs`` ->
# ``_merge_ai_model_specs``, so an AI-researched tank landed on the page under
# the heading "Fuel tank" long after both serializer keys were fixed.
#
# Measured over all 72,504 active listings on 2026-07-31, calling the sheet
# builder on every one: 53,818 sheets carried a Fuel tank row before this
# change. Disabling the ``ai_model_specs`` merge changed 8,920 of them (they
# vanished) and altered the value on 248 more, i.e. those rows were AI-derived
# or existed only because the AI merge changed which catalog row was chosen.
# After the change the row renders on 0 of the 72,504, because the whole
# ``epa_extended_specs`` table has just 59 rows whose tank appears in a page
# extraction and all 59 are 1984-1993 Dodge Daytonas, which nobody is selling.
# ---------------------------------------------------------------------------

_SHEET_CAR = {
    "vin": "1HGBH41JXMN109186",
    "year": 2019,
    "make": "Toyota",
    "model": "Camry",
    "trim": "LE",
    "fuel_type": "Gasoline",
}


def _sheet_rows(car, verified_specs):
    from backend.enrichment.generated_spec_sheet import build_generated_spec_sheet

    sheet = build_generated_spec_sheet(dict(car), dict(verified_specs))
    return {r["label"]: r["value"] for s in sheet["sections"] for r in s["rows"]}


def test_build_sheet_shows_a_quoted_tank(tank_quote) -> None:
    tank_quote(15.8)
    assert _sheet_rows(_SHEET_CAR, {})["Fuel tank"] == "15.8 gal"


def test_build_sheet_hides_tank_when_nothing_is_quoted(stub_quoted) -> None:
    """No sourced value -> no row. Not a class average, not a body-style guess."""
    stub_quoted({})
    assert "Fuel tank" not in _sheet_rows(_SHEET_CAR, {})


def test_build_sheet_tank_ignores_merge_verified_specs(stub_quoted) -> None:
    """
    The defect itself. ``verified_specs`` offers a tank — as it does on ~74% of
    active listings — and no page quotes one, so the sheet must stay silent.
    """
    stub_quoted({})
    rows = _sheet_rows(_SHEET_CAR, {"fuel_tank_gal": 15.5, "battery_kwh": 1.6})
    assert "Fuel tank" not in rows


def test_build_sheet_tank_prefers_the_quote_over_verified_specs(tank_quote) -> None:
    """When both exist the quoted number wins; the two must never disagree."""
    tank_quote(22.0)
    assert _sheet_rows(_SHEET_CAR, {"fuel_tank_gal": 15.5})["Fuel tank"] == "22 gal"


def test_build_sheet_hides_tank_for_pure_battery_electric(tank_quote) -> None:
    tank_quote(13.2)
    car = dict(_SHEET_CAR, make="Hyundai", model="Kona", fuel_type="Electric")
    assert "Fuel tank" not in _sheet_rows(car, {"fuel_tank_gal": 13.2})


def test_build_sheet_and_serializer_agree_on_the_tank(tank_quote) -> None:
    """
    Same car, same fact, two render paths — they read the same resolver, so they
    cannot print different numbers. This is what having three paths cost.
    """
    from backend.utils.car_serialize import serialize_car_for_api

    tank_quote(17.4)
    out = serialize_car_for_api(dict(_SHEET_CAR), include_verified=False)
    assert out["fuel_tank_gal"] == 17.4
    assert out["fuel_tank_gallons"] == 17.4
    assert _sheet_rows(_SHEET_CAR, {})["Fuel tank"] == "17.4 gal"
