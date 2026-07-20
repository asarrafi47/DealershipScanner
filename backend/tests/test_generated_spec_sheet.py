"""Generated build sheet: every car gets a sheet; catalog stays best-effort."""

from __future__ import annotations

from backend.enrichment import generated_spec_sheet as gss
from backend.enrichment.generated_spec_sheet import build_generated_spec_sheet


def _rows(sheet: dict, key: str) -> dict[str, str]:
    for sec in sheet["sections"]:
        if sec["key"] == key:
            return {r["label"]: r["value"] for r in sec["rows"]}
    return {}


def test_returns_none_without_make_model() -> None:
    assert build_generated_spec_sheet({"vin": "X", "year": 2020}) is None
    assert build_generated_spec_sheet({}) is None
    assert build_generated_spec_sheet(None) is None  # type: ignore[arg-type]


def test_basic_sheet_from_listing_only(monkeypatch) -> None:
    # No trim → catalog is skipped entirely, so no DB is touched.
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

    ident = _rows(sheet, "identity")
    assert ident["Make"] == "Honda" and ident["Model"] == "Accord"

    econ = _rows(sheet, "economy")
    assert econ["Fuel economy (EPA)"] == "30 city / 38 hwy mpg"

    # savings = msrp - price
    assert sheet["has_pricing"] is True
    assert sheet["pricing"]["savings"] == 2110
    assert sheet["pricing"]["savings_display"] == "$2,110"

    # no trim → no catalog, no crash
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
    assert sheet is not None
    # A color section with only placeholder values should not appear.
    assert _rows(sheet, "color") == {}
    assert _rows(sheet, "powertrain") == {}


def test_verified_specs_fill_performance_and_economy() -> None:
    car = {"make": "Tesla", "model": "Model 3", "trim": "", "fuel_type": "Electric"}
    vs = {
        "horsepower": 283,
        "torque_lb_ft": 307,
        "zero_to_60_sec": 5.8,
        "ev_range_miles": 272,
        "battery_kwh": 60.0,
        "epa_city08": 138,
        "epa_highway08": 126,
    }
    sheet = build_generated_spec_sheet(car, vs)
    perf = _rows(sheet, "performance")
    assert perf["Horsepower"] == "283 hp"
    assert perf["Torque"] == "307 lb-ft"
    assert perf["0–60 mph"] == "5.8 sec"
    econ = _rows(sheet, "economy")
    assert econ["Electric range"] == "272 mi"
    assert econ["Battery"] == "60 kWh"


def test_described_packages_priced_by_registry(monkeypatch) -> None:
    # The build sheet lists the packages THIS listing names (factory_packages)
    # and prices each via the registry. Stub the price lookup so no DB is needed.
    import json

    import backend.enrichment.package_registry as reg

    # keys are NORMALIZED names ("Sport Package" -> "sport")
    prices = {
        "sport": {"price": 1795, "source": "oem_sticker", "from_sticker": True},
        "premium": {"price": 900, "source": "dealer_listing", "from_sticker": False},
    }
    monkeypatch.setattr(
        reg,
        "price_for_package",
        lambda make, model, year, trim, name: prices.get(
            reg.normalize_name(name), {"price": None, "source": None, "from_sticker": False}
        ),
    )
    car = {
        "make": "Ford", "model": "Mustang", "trim": "GT", "year": 2019,
        "packages": json.dumps(
            {"factory_packages": ["Sport Package", "Premium Package", "Mystery Group"]}
        ),
    }
    sheet = build_generated_spec_sheet(car, verified_specs={})
    assert sheet["has_catalog"] is True
    assert sheet["catalog_from_sticker"] is True  # Sport Package came from a sticker
    pkgs = {p["name"]: p for p in sheet["catalog"]["packages"]}
    assert pkgs["Sport Package"]["price_display"] == "$1,795"
    assert pkgs["Premium Package"]["price_display"] == "$900"
    assert pkgs["Mystery Group"]["price_display"] is None  # unpriced still listed
    # subtotal = only the priced ones (1795 + 900)
    assert sheet["catalog"]["priced_total_display"] == "$2,695"


def test_no_described_packages_means_no_catalog() -> None:
    car = {"make": "Honda", "model": "Accord", "trim": "EX", "year": 2021}
    sheet = build_generated_spec_sheet(car, verified_specs={})
    assert sheet["has_catalog"] is False


def test_normalize_and_classify() -> None:
    from backend.enrichment.package_registry import classify_kind, normalize_name

    assert normalize_name("Premium Package") == normalize_name("Premium Pkg") == "premium"
    assert normalize_name("  Tech Group ") == "tech"
    assert classify_kind("M Sport Package") == "package"
    assert classify_kind("Convenience Group") == "package"
    assert classify_kind("Heated Seats") == "option"


def test_ev_fields_suppressed_on_gas_car() -> None:
    # A wrong/hybrid trim match can leave ev_range/battery on verified_specs for
    # a gas car — those must never surface unless the fuel type is electrified.
    car = {"make": "Jeep", "model": "Grand Cherokee", "fuel_type": "Gasoline"}
    vs = {"ev_range_miles": 25, "battery_kwh": 17.0, "epa_fuel_type": "Regular Gasoline"}
    econ = _rows(build_generated_spec_sheet(car, vs), "economy")
    assert "Electric range" not in econ
    assert "Battery" not in econ


def test_ev_fields_shown_on_electric_car() -> None:
    car = {"make": "Tesla", "model": "Model 3", "fuel_type": "Electric"}
    vs = {"ev_range_miles": 272, "battery_kwh": 60.0}
    econ = _rows(build_generated_spec_sheet(car, vs), "economy")
    assert econ["Electric range"] == "272 mi"
    assert econ["Battery"] == "60 kWh"


def test_negative_option_price_renders_as_credit() -> None:
    assert gss._fmt_price(-500) == "$500 credit"
    assert gss._fmt_price(1795) == "$1,795"
    assert gss._fmt_price(None) is None
