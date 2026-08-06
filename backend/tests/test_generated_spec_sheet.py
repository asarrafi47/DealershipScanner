"""Generated build sheet: a figure renders only when it can be attributed.

The rule these tests hold the sheet to is the one already proven on trim
bullets: quote it from a document about THIS car, or show nothing. The
regression they exist to prevent is the sheet reading horsepower / torque /
0-60 / curb weight / tow capacity / battery off ``verified_specs``, which
``merge_verified_specs`` fills from ``epa_extended_specs`` and then from
``ai_model_specs`` — AI-generated numbers with no way to tell afterwards which
key they supplied.
"""

from __future__ import annotations

import json

import pytest

from backend.enrichment import generated_spec_sheet as gss
from backend.enrichment.generated_spec_sheet import build_generated_spec_sheet

# The column order ``_extended_row_for_car`` returns.
_ROW_FIELDS = gss._ATTRIBUTABLE_SPEC_FIELDS

# Captured before any fixture swaps them out, so a test can put the real
# implementation back rather than re-installing its own stub by accident.
_REAL_ROW_FOR_CAR = gss._extended_row_for_car
_REAL_SHARED = gss._fields_shared_across_trims


def _rows(sheet: dict, key: str) -> dict[str, str]:
    for sec in sheet["sections"]:
        if sec["key"] == key:
            return {r["label"]: r["value"] for r in sec["rows"]}
    return {}


def _row_objs(sheet: dict, key: str) -> dict[str, dict]:
    for sec in sheet["sections"]:
        if sec["key"] == key:
            return {r["label"]: r for r in sec["rows"]}
    return {}


def _extended_row(*, year=2021, make="Honda", model="Accord", pages=None, **values):
    """Build an ``epa_extended_specs`` row tuple in the resolver's column order."""
    stored = tuple(values.get(f) for f in _ROW_FIELDS)
    return (*stored, year, make, model, json.dumps({"pages": pages or []}))


def _page(title, url="https://example.test/accord", **extracted):
    return {"url": url, "title": title, "extracted": extracted}


@pytest.fixture(autouse=True)
def _no_db(monkeypatch):
    """Every catalog read is stubbed; a test that wants data opts in explicitly.

    Default: no extended-specs row, no quoted tank, no quoted EV range. So the
    baseline for every test below is "we know nothing", which is exactly the
    state the sheet has to survive without inventing a row.
    """
    gss.clear_attributable_spec_cache()
    monkeypatch.setattr(gss, "_extended_row_for_car", lambda car: None)
    monkeypatch.setattr(gss, "_fields_shared_across_trims", lambda y, mk, md: frozenset())
    monkeypatch.setattr(gss, "_sourced_fuel_tank_gallons", lambda car: None)
    monkeypatch.setattr(gss, "_sourced_ev_range_miles", lambda car: None)
    # Cleared on the way IN, not out: at teardown the stubs above are still
    # installed and ``clear_attributable_spec_cache`` would be calling
    # ``cache_clear`` on a lambda.


# --- identity / degradation ------------------------------------------------


def test_returns_none_without_make_model() -> None:
    assert build_generated_spec_sheet({"vin": "X", "year": 2020}) is None
    assert build_generated_spec_sheet({}) is None
    assert build_generated_spec_sheet(None) is None  # type: ignore[arg-type]


def test_basic_sheet_from_listing_only() -> None:
    car = {
        "vin": "1HGROBUST0000001",
        "year": 2021,
        "make": "Honda",
        "model": "Accord",
        "body_style": "Sedan",
        "cylinders": 4,
        "transmission": "CVT",
        "drivetrain": "FWD",
        "fuel_type": "Gasoline",
        "exterior_color": "Modern Steel",
        "interior_color": "Black",
        "mpg_city": 30,
        "mpg_highway": 38,
        "price": 24990,
        "msrp": 27100,
    }
    sheet = build_generated_spec_sheet(car, verified_specs={})
    assert sheet is not None
    assert sheet["title"] == "2021 Honda Accord"
    assert sheet["vin"] == "1HGROBUST0000001"
    assert _rows(sheet, "identity")["Model"] == "Accord"
    assert sheet["has_catalog"] is False


def test_placeholder_values_are_dropped() -> None:
    car = {
        "make": "Toyota",
        "model": "Camry",
        "exterior_color": "—",
        "interior_color": "N/A",
        "transmission": "  ",
    }
    sheet = build_generated_spec_sheet(car, verified_specs={})
    assert _rows(sheet, "color") == {}
    assert _rows(sheet, "powertrain") == {}


# --- the core rule: no sourced value -> show nothing ------------------------


def test_no_extended_row_means_no_performance_section() -> None:
    car = {"make": "Jeep", "model": "Grand Cherokee", "year": 2011, "trim": "Laredo"}
    sheet = build_generated_spec_sheet(car, verified_specs={})
    assert _rows(sheet, "performance") == {}
    assert all(s["key"] != "performance" for s in sheet["sections"])


def test_verified_specs_performance_values_are_ignored() -> None:
    """The 2011 Grand Cherokee Laredo case: ``vs`` says 360 hp, the sheet says nothing.

    ``verified_specs`` is where the AI table's numbers arrive. No amount of it
    can put a row on this sheet — only a quoted, year-matched page can.
    """
    car = {"make": "Jeep", "model": "Grand Cherokee", "year": 2011, "trim": "Laredo"}
    vs = {
        "horsepower": 360,
        "torque_lb_ft": 390,
        "zero_to_60_sec": 6.0,
        "curb_weight_lb": 5100,
        "tow_capacity_lb": 7400,
        "battery_kwh": 17.0,
    }
    sheet = build_generated_spec_sheet(car, vs)
    assert _rows(sheet, "performance") == {}
    assert "Battery" not in _rows(sheet, "economy")


def test_quoted_year_matched_value_renders_with_its_source_url(monkeypatch) -> None:
    car = {"make": "Honda", "model": "Accord", "year": 2021, "trim": "Sport"}
    url = "https://www.caranddriver.com/honda/accord"
    monkeypatch.setattr(
        gss,
        "_extended_row_for_car",
        lambda c: _extended_row(
            horsepower=252,
            pages=[_page("2021 Honda Accord Review, Pricing, and Specs", url, horsepower=252)],
        ),
    )
    sheet = build_generated_spec_sheet(car, verified_specs={})
    row = _row_objs(sheet, "performance")["Horsepower"]
    assert row["value"] == "252 hp"
    assert row["source_url"] == url
    assert row["source"] == "epa_extended_specs page extraction"
    assert row["derived"] is False


def test_page_about_a_different_model_year_renders_nothing(monkeypatch) -> None:
    """The 2005 TrailBlazer / 2026 Trailblazer case, live in the table today.

    A body-on-frame 2005 SUV row carries 137 hp extracted from a page titled
    "2026 Chevrolet Trailblazer Review" — a different vehicle with the same
    name. Same nameplate is not same car.
    """
    car = {"make": "Chevrolet", "model": "TrailBlazer", "year": 2005, "trim": "2WD"}
    monkeypatch.setattr(
        gss,
        "_extended_row_for_car",
        lambda c: _extended_row(
            year=2005,
            make="Chevrolet",
            model="TrailBlazer",
            horsepower=137,
            tow_capacity_lb=1000,
            pages=[
                _page(
                    "2026 Chevrolet Trailblazer Review, Pricing, and Specs",
                    "https://www.caranddriver.com/chevrolet/trailblazer",
                    horsepower=137,
                    tow_capacity_lb=1000,
                )
            ],
        ),
    )
    assert _rows(build_generated_spec_sheet(car, verified_specs={}), "performance") == {}


def test_year_in_the_url_is_not_evidence(monkeypatch) -> None:
    """We built those URLs from our own key, so reading the year back is circular.

    ``carwow.co.uk/ford/f150/1985/specifications`` has our year in the path and a
    brand home page's title. 14,392 of those fetches landed on a brand page and
    were parsed anyway.
    """
    car = {"make": "Ford", "model": "F150", "year": 1985, "trim": ""}
    monkeypatch.setattr(
        gss,
        "_extended_row_for_car",
        lambda c: _extended_row(
            year=1985,
            make="Ford",
            model="F150",
            horsepower=210,
            pages=[
                _page(
                    "Ford UK | New Ford Cars, Prices & Reviews | Carwow",
                    "https://www.carwow.co.uk/ford/f150/1985/specifications",
                    horsepower=210,
                )
            ],
        ),
    )
    assert _rows(build_generated_spec_sheet(car, verified_specs={}), "performance") == {}


def test_value_shared_across_trims_renders_nothing(monkeypatch) -> None:
    """A number that cannot tell a Tradesman from a TRX describes at most one."""
    car = {"make": "Ram", "model": "1500", "year": 2021, "trim": "Tradesman"}
    monkeypatch.setattr(
        gss,
        "_extended_row_for_car",
        lambda c: _extended_row(
            year=2021,
            make="Ram",
            model="1500",
            horsepower=702,
            pages=[
                _page(
                    "2021 Ram 1500 Review, Pricing, and Specs",
                    "https://www.caranddriver.com/ram/1500",
                    horsepower=702,
                )
            ],
        ),
    )
    monkeypatch.setattr(
        gss, "_fields_shared_across_trims", lambda y, mk, md: frozenset({"horsepower"})
    )
    assert _rows(build_generated_spec_sheet(car, verified_specs={}), "performance") == {}


def test_unknown_trim_spread_is_not_permission_to_render(monkeypatch) -> None:
    """A failed spread query must suppress, not wave the value through."""
    monkeypatch.setattr(gss, "_fields_shared_across_trims", _REAL_SHARED)
    _REAL_SHARED.cache_clear()

    def _boom(*a, **k):
        raise RuntimeError("catalog unavailable")

    monkeypatch.setattr("backend.db.inventory_db.get_conn", _boom)
    assert gss._fields_shared_across_trims(2021, "Honda", "Accord") == frozenset(_ROW_FIELDS)
    _REAL_SHARED.cache_clear()


def test_out_of_band_value_renders_nothing(monkeypatch) -> None:
    """A 40 hp Porsche is a parse error; it is dropped, never clamped."""
    car = {"make": "Porsche", "model": "911", "year": 2020, "trim": "Carrera"}
    monkeypatch.setattr(
        gss,
        "_extended_row_for_car",
        lambda c: _extended_row(
            year=2020,
            make="Porsche",
            model="911",
            horsepower=40,
            pages=[
                _page(
                    "2020 Porsche 911 Review",
                    "https://www.caranddriver.com/porsche/911",
                    horsepower=40,
                )
            ],
        ),
    )
    assert _rows(build_generated_spec_sheet(car, verified_specs={}), "performance") == {}


def test_stored_column_must_match_the_page_extraction(monkeypatch) -> None:
    """A page that reported a different number does not vouch for the column."""
    car = {"make": "Honda", "model": "Accord", "year": 2021, "trim": "Sport"}
    monkeypatch.setattr(
        gss,
        "_extended_row_for_car",
        lambda c: _extended_row(
            horsepower=252,
            pages=[
                _page(
                    "2021 Honda Accord Review",
                    "https://www.caranddriver.com/honda/accord",
                    horsepower=192,
                )
            ],
        ),
    )
    assert _rows(build_generated_spec_sheet(car, verified_specs={}), "performance") == {}


def test_resolver_never_queries_the_ai_spec_tables(monkeypatch) -> None:
    """Every statement the gate issues names ``epa_extended_specs`` and nothing else."""
    seen: list[str] = []

    class _Cur:
        def execute(self, sql, params=None):
            seen.append(sql)

        def fetchone(self):
            return None

    class _Conn:
        def cursor(self):
            return _Cur()

        def close(self):
            pass

    monkeypatch.setattr("backend.db.inventory_db.get_conn", lambda: _Conn())
    monkeypatch.setattr(gss, "_extended_row_for_car", _REAL_ROW_FOR_CAR)
    monkeypatch.setattr(gss, "_fields_shared_across_trims", _REAL_SHARED)
    gss.clear_attributable_spec_cache()
    gss._attributable_extended_specs(
        {"make": "Honda", "model": "Accord", "year": 2021, "trim": "Sport"}
    )
    gss.clear_attributable_spec_cache()
    assert seen, "expected the resolver to query the catalog"
    for sql in seen:
        assert "epa_extended_specs" in sql
        assert "ai_model_specs" not in sql
        assert "ai_engine_specs" not in sql


def test_curb_weight_and_tow_rating_are_never_rendered(monkeypatch) -> None:
    """Both columns hold each other's numbers; neither is on the sheet at all.

    The 2026 Jeep Gladiator row stores 7,700 lb for weight AND tow — its tow
    rating; it weighs about 5,050. Even a perfectly quoted, year-matched page
    cannot put either row on the panel.
    """
    car = {"make": "Jeep", "model": "Gladiator", "year": 2026, "trim": "Willys"}
    page = _page(
        "2026 Jeep Gladiator Review",
        "https://www.caranddriver.com/jeep/gladiator",
        curb_weight_lb=7700,
        tow_capacity_lb=7700,
        horsepower=285,
    )
    monkeypatch.setattr(
        gss,
        "_extended_row_for_car",
        lambda c: _extended_row(
            year=2026, make="Jeep", model="Gladiator", horsepower=285, pages=[page]
        ),
    )
    perf = _rows(build_generated_spec_sheet(car, verified_specs={}), "performance")
    assert "Curb weight" not in perf
    assert "Towing capacity" not in perf
    # the other figures on the same row are not condemned with them
    assert perf["Horsepower"] == "285 hp"
    assert "curb_weight_lb" not in gss._ATTRIBUTABLE_SPEC_FIELDS
    assert "tow_capacity_lb" not in gss._ATTRIBUTABLE_SPEC_FIELDS


# --- the panel may not contradict itself -------------------------------------


def test_cylinder_count_contradicting_the_engine_is_dropped() -> None:
    """Live case: 2017 Grand Cherokee Limited, "3.6L V6" one line above "8"."""
    car = {
        "make": "Jeep", "model": "Grand Cherokee", "year": 2017, "trim": "Limited 4x4",
        "engine_description": "3.6L V6", "cylinders": 8, "fuel_type": "Midgrade Gasoline",
    }
    pt = _rows(build_generated_spec_sheet(car, {}), "powertrain")
    assert pt["Engine"] == "3.6L V6"
    assert "Cylinders" not in pt


def test_cylinder_count_agreeing_with_the_engine_is_kept() -> None:
    car = {
        "make": "Jeep", "model": "Grand Cherokee", "year": 2017, "trim": "Limited",
        "engine_description": "3.6L V6", "cylinders": 6, "fuel_type": "Gasoline",
    }
    assert _rows(build_generated_spec_sheet(car, {}), "powertrain")["Cylinders"] == "6"


def test_cylinder_count_on_a_bev_is_dropped() -> None:
    car = {
        "make": "Tesla", "model": "Model 3", "year": 2021,
        "cylinders": 4, "fuel_type": "Electric",
    }
    assert "Cylinders" not in _rows(build_generated_spec_sheet(car, {}), "powertrain")


# --- fuel economy: the label follows the source ------------------------------


def test_epa_mpg_row_requires_an_epa_figure() -> None:
    car = {"make": "Honda", "model": "Accord", "year": 2021}
    vs = {"epa_city08": 30, "epa_highway08": 38, "fuel_economy_display": "30 city / 38 hwy mpg"}
    econ = _row_objs(build_generated_spec_sheet(car, vs), "economy")
    assert econ["Fuel economy (EPA)"]["value"] == "30 city / 38 hwy mpg"
    assert econ["Fuel economy (EPA)"]["source"] == "epa_master (EPA dataset)"
    assert "Fuel economy (as listed)" not in econ


def test_dealer_mpg_is_not_labelled_epa() -> None:
    """``merge_verified_specs`` falls back to the dealer's own mpg columns and
    hands them over in ``fuel_economy_display``; that must not wear EPA's name."""
    car = {"make": "Honda", "model": "Accord", "year": 2021, "mpg_city": 30, "mpg_highway": 38}
    vs = {"fuel_economy_display": "30 city / 38 hwy mpg"}  # no epa_city08/epa_highway08
    econ = _row_objs(build_generated_spec_sheet(car, vs), "economy")
    assert "Fuel economy (EPA)" not in econ
    assert econ["Fuel economy (as listed)"]["value"] == "30 city / 38 hwy mpg"
    assert "dealer listing" in econ["Fuel economy (as listed)"]["source"]


def test_no_mpg_anywhere_shows_no_mpg_row() -> None:
    car = {"make": "Honda", "model": "Accord", "year": 2021}
    econ = _rows(build_generated_spec_sheet(car, {}), "economy")
    assert "Fuel economy (EPA)" not in econ
    assert "Fuel economy (as listed)" not in econ


# --- electrified rows --------------------------------------------------------


def test_ev_fields_suppressed_on_gas_car(monkeypatch) -> None:
    monkeypatch.setattr(gss, "_sourced_ev_range_miles", lambda car: 25)
    monkeypatch.setattr(
        gss,
        "_extended_row_for_car",
        lambda c: _extended_row(
            year=2021,
            make="Jeep",
            model="Grand Cherokee",
            battery_kwh=17.0,
            pages=[
                _page(
                    "2021 Jeep Grand Cherokee Review",
                    "https://www.caranddriver.com/jeep/grand-cherokee",
                    battery_kwh=17.0,
                )
            ],
        ),
    )
    car = {"make": "Jeep", "model": "Grand Cherokee", "year": 2021, "fuel_type": "Gasoline"}
    econ = _rows(build_generated_spec_sheet(car, {"epa_fuel_type": "Regular Gasoline"}), "economy")
    assert "Electric range" not in econ
    assert "Battery" not in econ


def test_battery_row_needs_a_quote_not_just_an_electric_car(monkeypatch) -> None:
    car = {"make": "Tesla", "model": "Model 3", "year": 2021, "fuel_type": "Electric"}
    econ = _rows(build_generated_spec_sheet(car, {"battery_kwh": 60.0}), "economy")
    assert "Battery" not in econ


def test_battery_row_renders_when_quoted_for_this_year(monkeypatch) -> None:
    url = "https://www.caranddriver.com/tesla/model-3"
    monkeypatch.setattr(
        gss,
        "_extended_row_for_car",
        lambda c: _extended_row(
            year=2021,
            make="Tesla",
            model="Model 3",
            battery_kwh=60.0,
            pages=[_page("2021 Tesla Model 3 Review", url, battery_kwh=60.0)],
        ),
    )
    monkeypatch.setattr(gss, "_sourced_ev_range_miles", lambda car: 272)
    car = {"make": "Tesla", "model": "Model 3", "year": 2021, "fuel_type": "Electric"}
    econ = _row_objs(build_generated_spec_sheet(car, {}), "economy")
    assert econ["Battery"]["value"] == "60 kWh"
    assert econ["Battery"]["source_url"] == url
    assert econ["Electric range"]["value"] == "272 mi"


# --- package prices ----------------------------------------------------------


def _stub_prices(monkeypatch, table):
    import backend.enrichment.package_registry as reg

    miss = {
        "price": None,
        "source": None,
        "from_sticker": False,
        "observed_year": None,
        "observed_trim": None,
        "exact_config": False,
    }
    monkeypatch.setattr(
        reg,
        "price_for_package",
        lambda make, model, year, trim, name: table.get(reg.normalize_name(name), miss),
    )


def test_exact_config_sticker_price_is_printed(monkeypatch) -> None:
    _stub_prices(
        monkeypatch,
        {
            "sport": {
                "price": 1795,
                "source": "oem_sticker",
                "from_sticker": True,
                "observed_year": 2019,
                "observed_trim": "GT",
                "exact_config": True,
            }
        },
    )
    car = {
        "make": "Ford", "model": "Mustang", "trim": "GT", "year": 2019,
        "packages": json.dumps({"factory_packages": ["Sport Package"]}),
    }
    sheet = build_generated_spec_sheet(car, verified_specs={})
    pkg = sheet["catalog"]["packages"][0]
    assert pkg["price_display"] == "$1,795"
    assert pkg["from_sticker"] is True
    assert pkg["price_basis"] == "exact_config"
    assert sheet["catalog_from_sticker"] is True
    assert sheet["catalog"]["priced_total_display"] == "$1,795"
    assert sheet["catalog"]["priced_total_derived"] is True


def test_price_observed_on_another_year_is_not_shown_as_this_cars(monkeypatch) -> None:
    """The observation is real and is kept in the data; the attribution is not made."""
    _stub_prices(
        monkeypatch,
        {
            "sport": {
                "price": 1795,
                "source": "oem_sticker",
                "from_sticker": True,
                "observed_year": 2014,
                "observed_trim": "V6",
                "exact_config": False,
            }
        },
    )
    car = {
        "make": "Ford", "model": "Mustang", "trim": "GT", "year": 2019,
        "packages": json.dumps({"factory_packages": ["Sport Package"]}),
    }
    sheet = build_generated_spec_sheet(car, verified_specs={})
    pkg = sheet["catalog"]["packages"][0]
    assert pkg["price"] is None
    assert pkg["price_display"] is None
    assert pkg["from_sticker"] is False
    assert pkg["price_basis"] == "other_config"
    assert pkg["observed_price"] == 1795
    assert pkg["observed_year"] == 2014
    assert pkg["observed_trim"] == "V6"
    assert sheet["catalog_from_sticker"] is False
    assert sheet["catalog"]["priced_total_display"] is None


def test_unpriced_package_is_still_listed(monkeypatch) -> None:
    _stub_prices(monkeypatch, {})
    car = {
        "make": "Ford", "model": "Mustang", "trim": "GT", "year": 2019,
        "packages": json.dumps({"factory_packages": ["Mystery Group"]}),
    }
    sheet = build_generated_spec_sheet(car, verified_specs={})
    pkg = sheet["catalog"]["packages"][0]
    assert pkg["name"] == "Mystery Group"
    assert pkg["price_display"] is None
    assert pkg["price_basis"] is None


def test_no_described_packages_means_no_catalog() -> None:
    car = {"make": "Honda", "model": "Accord", "trim": "EX", "year": 2021}
    assert build_generated_spec_sheet(car, verified_specs={})["has_catalog"] is False


def test_normalize_and_classify() -> None:
    from backend.enrichment.package_registry import classify_kind, normalize_name

    assert normalize_name("Premium Package") == normalize_name("Premium Pkg") == "premium"
    assert normalize_name("  Tech Group ") == "tech"
    assert classify_kind("M Sport Package") == "package"
    assert classify_kind("Convenience Group") == "package"
    assert classify_kind("Heated Seats") == "option"


# --- derived figures are labelled derived ------------------------------------


def test_savings_is_flagged_as_derived() -> None:
    # ``condition: New`` is load-bearing since the MSRP trust gate landed: a feed
    # msrp is only believed on new, non-CPO inventory, and a saving is only
    # printed there. See ``backend/tests/test_msrp_trust.py`` for the rest.
    car = {
        "make": "Honda",
        "model": "Accord",
        "year": 2021,
        "condition": "New",
        "price": 24990,
        "msrp": 27100,
    }
    pricing = build_generated_spec_sheet(car, verified_specs={})["pricing"]
    assert pricing["savings"] == 2110
    assert pricing["savings_derived"] is True
    assert pricing["savings_basis"] == "cars.msrp - cars.price"
    assert pricing["price_source"] == "dealer listing (cars.price)"


def test_no_savings_means_no_derived_flag() -> None:
    car = {"make": "Honda", "model": "Accord", "year": 2021, "price": 24990}
    pricing = build_generated_spec_sheet(car, verified_specs={})["pricing"]
    assert pricing["savings"] is None
    assert pricing["savings_derived"] is False


def test_negative_option_price_renders_as_credit() -> None:
    assert gss._fmt_price(-500) == "$500 credit"
    assert gss._fmt_price(1795) == "$1,795"
    assert gss._fmt_price(None) is None


def test_every_rendered_row_says_where_it_came_from(monkeypatch) -> None:
    """No row on a spec section may ship without a source or a derived flag."""
    monkeypatch.setattr(
        gss,
        "_extended_row_for_car",
        lambda c: _extended_row(
            horsepower=252,
            pages=[
                _page(
                    "2021 Honda Accord Review",
                    "https://www.caranddriver.com/honda/accord",
                    horsepower=252,
                )
            ],
        ),
    )
    car = {
        "make": "Honda", "model": "Accord", "year": 2021, "trim": "Sport",
        "mpg_city": 30, "mpg_highway": 38, "exterior_color": "Black",
    }
    sheet = build_generated_spec_sheet(car, {"epa_city08": 29, "epa_highway08": 37})
    for sec in sheet["sections"]:
        if sec["key"] not in ("economy", "performance"):
            continue
        for row in sec["rows"]:
            assert row.get("source"), f"{sec['key']}/{row['label']} has no source"
            assert row["derived"] is False
