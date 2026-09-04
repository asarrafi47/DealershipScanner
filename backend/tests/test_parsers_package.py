"""Package-level parser behavior (unknown provider, etc.)."""

from __future__ import annotations

import pytest

import backend.parsers as parsers


@pytest.fixture(autouse=True)
def clear_unknown_provider_warnings() -> None:
    parsers._warned_unknown_providers.clear()
    yield
    parsers._warned_unknown_providers.clear()


def test_unknown_provider_returns_empty() -> None:
    """Unknown provider tries auto-detect; empty raw data yields no vehicles."""
    assert parsers.parse("not_a_provider", {}, base_url="https://x.com/", dealer_id="d1") == []
    assert parsers.parse("not_a_provider", {}, base_url="https://x.com/", dealer_id="d1") == []


def test_chapman_shape_rescues_mistagged_dealer_dot_com_provider() -> None:
    """A Chapman recipe mis-tagged provider_hint='dealer_dot_com' (the real
    production bug on chapmanbmwchandler-com/chapmanfordaz-com) must still get
    routed to the Chapman parser by shape, so exterior_color etc. are populated
    instead of silently coming back None via the generic key-guessing parser.
    """
    raw_data = [
        {
            "vin": "1FA6P8TH0K5111111",
            "year": 2023,
            "make": "Ford",
            "model": "Mustang",
            "trim": "GT",
            "colorExt": "Race Red",
            "colorInt": "Ebony",
            "arkona": "A12345",
            "uniqueArkona": "U12345",
            "valueArkona": "V12345",
            "drive": "RWD",
            "fuel": "Gasoline",
            "body": "Coupe",
            "isCertified": False,
            "isFleet": False,
            "pricing": {"msrp": 45000},
            "msrp": 45000,
        },
        {
            "vin": "1FA6P8TH0K5222222",
            "year": 2022,
            "make": "Ford",
            "model": "Explorer",
            "trim": "XLT",
            "colorExt": "Oxford White",
            "colorInt": "Sandstone",
            "arkona": "A67890",
            "drive": "AWD",
            "fuel": "Gasoline",
            "body": "SUV",
            "isCertified": True,
            "pricing": {"msrp": 38000},
            "msrp": 38000,
        },
    ]

    rows = parsers.parse(
        "dealer_dot_com",  # wrong declared provider hint, simulating the real bug
        raw_data,
        base_url="https://www.chapmanfordaz.com/",
        dealer_id="chapmanfordaz-com",
        dealer_name="Chapman Ford",
        dealer_url="https://www.chapmanfordaz.com/",
    )

    assert len(rows) == 2
    by_vin = {r["vin"]: r for r in rows}
    assert by_vin["1FA6P8TH0K5111111"]["exterior_color"] == "Race Red"
    assert by_vin["1FA6P8TH0K5111111"]["interior_color"] == "Ebony"
    assert by_vin["1FA6P8TH0K5111111"]["drivetrain"] == "RWD"
    assert by_vin["1FA6P8TH0K5111111"]["fuel_type"] == "Gasoline"
    assert by_vin["1FA6P8TH0K5111111"]["body_style"] == "Coupe"
    assert by_vin["1FA6P8TH0K5222222"]["exterior_color"] == "Oxford White"
    assert by_vin["1FA6P8TH0K5222222"]["drivetrain"] == "AWD"
