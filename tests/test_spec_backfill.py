"""Pure helpers for spec backfill (no live network / Playwright)."""

from __future__ import annotations

import json

import pytest

from backend import spec_backfill as sb
from backend.spec_search_client import (
    build_spec_search_query,
    is_allowed_spec_result_url,
    parse_fueleconomy_gov_html,
)
from backend.utils import spec_provenance as sp
from backend.utils.field_clean import format_mpg_city_highway_display
from backend.utils.vdp_spec_parse import parse_html_for_vehicle_specs


def test_format_mpg_city_highway_display() -> None:
    assert format_mpg_city_highway_display(22, 31) == "22 City / 31 Hwy"
    assert format_mpg_city_highway_display(22, None) == "22 City MPG"
    assert format_mpg_city_highway_display(None, None) is None


def test_build_spec_search_query_site_bias() -> None:
    q = build_spec_search_query(2021, "Honda", "Accord", "Sport", intent="mpg")
    assert "2021" in q and "Honda" in q and "Accord" in q and "Sport" in q
    assert "site:fueleconomy.gov" in q


def test_is_allowed_spec_result_url() -> None:
    assert is_allowed_spec_result_url("https://www.fueleconomy.gov/find.do?foo=1")
    assert is_allowed_spec_result_url("https://www.epa.gov/")
    assert not is_allowed_spec_result_url("https://random-blog.example/page")


def test_parse_fueleconomy_gov_html_city_hwy() -> None:
    html = """
    <html><body>
    <div>EPA Fuel Economy</div>
    <span>City MPG: 23</span>
    <span>Highway MPG: 34</span>
    </body></html>
    """
    d = parse_fueleconomy_gov_html(html)
    assert d.get("mpg_city") == 23
    assert d.get("mpg_highway") == 34


def test_parse_html_for_vehicle_specs_ld_json() -> None:
    html = r"""
    <html><head>
    <script type="application/ld+json">
    {"@context":"https://schema.org","@type":"Car","vehicleEngine":{"name":"2.0L I4 DOHC"}}
    </script>
    </head><body></body></html>
    """
    d = parse_html_for_vehicle_specs(html)
    assert d.get("cylinders") == 4


def test_parse_html_for_vehicle_specs_mpg_slash() -> None:
    html = "<html><body><div>Fuel Economy 28/36 MPG combined</div></body></html>"
    d = parse_html_for_vehicle_specs(html)
    assert d.get("mpg_city") == 28
    assert d.get("mpg_highway") == 36


def test_merge_spec_source_json() -> None:
    base = '{"cylinders":{"source":"epa"}}'
    out = sp.merge_spec_source_json(
        base,
        {"mpg_city": {"source": "vdp", "url": "https://dealer.example/vdp"}},
    )
    data = json.loads(out)
    assert data["cylinders"]["source"] == "epa"
    assert data["mpg_city"]["source"] == "vdp"


def test_car_needs_spec_backfill() -> None:
    assert sb.car_needs_spec_backfill({"id": 1, "cylinders": None, "mpg_city": 1, "mpg_highway": 2})
    assert not sb.car_needs_spec_backfill(
        {"id": 1, "cylinders": 4, "mpg_city": 20, "mpg_highway": 28, "engine_l": "2.0"}
    )


@pytest.mark.parametrize(
    "car,found,expected_keys",
    [
        (
            {"cylinders": None, "mpg_city": None, "mpg_highway": None},
            {"cylinders": 6, "mpg_city": 18, "mpg_highway": 25},
            {"cylinders", "mpg_city", "mpg_highway"},
        ),
        (
            {"cylinders": 4, "mpg_city": None, "mpg_highway": None},
            {"cylinders": 6, "mpg_city": 18, "mpg_highway": 25},
            {"mpg_city", "mpg_highway"},
        ),
    ],
)
def test_conservative_updates(car: dict, found: dict, expected_keys: set) -> None:
    prov = {k: {"source": "test"} for k in found}
    upd, pout = sb._conservative_updates(car, found, prov)
    assert set(upd.keys()) == expected_keys
    assert set(pout.keys()) == set(upd.keys())
