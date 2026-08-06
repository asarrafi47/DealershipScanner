"""An MSRP renders only when it is credible, and never as a fabricated anchor.

``cars.msrp`` is the dealer feed's verbatim value. On used inventory it is a
marketing "was" price: of the 18,350 active listings carrying one, 9,621 are
EXACTLY the asking price and 2,439 are below it. The regression these tests
exist to prevent is any of that reaching a shopper — as an "MSRP" identical to
the price it sits next to, as an MSRP under the price, or (worst) subtracted
from the price and printed as "BELOW MSRP" savings.

The two cars this was found on are pinned by name below:
  * 5UX83DP05R9U16883, a 2024 BMW X3 M40i — "MSRP $48,990 / LISTED $48,990".
  * WBS3U9C58GP969438, a 2016 BMW M4 — "MSRP $38,990 / LISTED $36,788 /
    BELOW MSRP $2,202", against a real window sticker reading $86,600.
"""

from __future__ import annotations

import pytest

from backend.enrichment.generated_spec_sheet import build_generated_spec_sheet
from backend.utils import msrp_trust
from backend.utils.car_serialize import serialize_car_for_api
from backend.utils.msrp_trust import resolve_display_msrp


@pytest.fixture(autouse=True)
def _no_disk_stickers(monkeypatch: pytest.MonkeyPatch):
    """Default every test to "we hold no sticker for this VIN".

    Tests that want one call ``_with_sticker``. Without this the resolver would
    reach the real ``car_window_stickers/`` tree and a developer's checkout
    would decide the outcome.
    """
    monkeypatch.setattr(msrp_trust, "sticker_msrp_for_car", lambda car: None)


def _with_sticker(monkeypatch: pytest.MonkeyPatch, value: int | None) -> None:
    monkeypatch.setattr(msrp_trust, "sticker_msrp_for_car", lambda car: value)


def _new(**over):
    car = {
        "vin": "1HGROBUST0000001",
        "year": 2025,
        "make": "Honda",
        "model": "Accord",
        "condition": "New",
        "is_cpo": 0,
        "price": 34_000,
    }
    car.update(over)
    return car


def _used(**over):
    car = _new(condition="Used", year=2016, mileage=52_000)
    car.update(over)
    return car


# --- rule 1: an MSRP equal to the price is not an MSRP ------------------------


def test_msrp_equal_to_price_is_suppressed() -> None:
    out = resolve_display_msrp(_new(price=34_000, msrp=34_000))
    assert out["msrp"] is None
    assert out["savings"] is None
    assert out["rejected"] == "not_above_price"


def test_x3_m40i_feed_msrp_equal_to_price_never_renders() -> None:
    """The 2024 X3 M40i as it sat in the database: 48,990 in both columns."""
    car = _used(vin="5UX83DP05R9U16883", make="BMW", model="X3", trim="M40i",
                year=2024, price=48_990.0, msrp=48_990.0)
    assert resolve_display_msrp(car)["msrp"] is None


# --- rule 2: an MSRP below the price is not an MSRP, and savings never invert -


def test_msrp_below_price_is_suppressed() -> None:
    out = resolve_display_msrp(_new(price=34_000, msrp=31_500))
    assert out["msrp"] is None
    assert out["savings"] is None
    assert out["rejected"] == "not_above_price"


def test_sticker_msrp_below_price_is_suppressed_too(monkeypatch) -> None:
    """The document source gets no exemption from the arithmetic rule."""
    _with_sticker(monkeypatch, 30_000)
    assert resolve_display_msrp(_new(price=34_000))["msrp"] is None


def test_savings_is_never_zero_or_negative(monkeypatch) -> None:
    for msrp in (34_000, 33_999, 1):
        out = resolve_display_msrp(_new(price=34_000, msrp=msrp))
        assert out["savings"] is None, msrp
    _with_sticker(monkeypatch, 34_000)
    assert resolve_display_msrp(_new(price=34_000))["savings"] is None


# --- rule 3: feed msrp is untrustworthy on used / CPO inventory ---------------


def test_feed_msrp_on_used_car_is_rejected() -> None:
    out = resolve_display_msrp(_used(price=36_788, msrp=38_990))
    assert out["msrp"] is None
    assert out["savings"] is None
    assert out["rejected"] == "feed_msrp_on_pre_owned"


def test_m4_fabricated_discount_is_gone() -> None:
    """No sticker in reach: the $2,202 "BELOW MSRP" must not survive."""
    car = _used(vin="WBS3U9C58GP969438", make="BMW", model="M4",
                price=36_788.0, msrp=38_990.0)
    out = resolve_display_msrp(car)
    assert out["msrp"] is None
    assert out["savings"] is None


def test_feed_msrp_on_cpo_car_is_rejected() -> None:
    car = _new(condition="Certified Pre-Owned", is_cpo=1, price=30_000, msrp=41_000)
    assert resolve_display_msrp(car)["msrp"] is None


def test_feed_msrp_with_cpo_flag_but_new_condition_is_rejected() -> None:
    """``is_cpo`` alone disqualifies — the two columns disagree, so neither wins."""
    assert resolve_display_msrp(_new(is_cpo=1, msrp=41_000))["msrp"] is None


def test_unknown_condition_is_treated_as_pre_owned() -> None:
    assert resolve_display_msrp(_new(condition=None, msrp=41_000))["msrp"] is None
    assert resolve_display_msrp(_new(condition="", msrp=41_000))["msrp"] is None


def test_feed_msrp_on_new_car_is_trusted() -> None:
    out = resolve_display_msrp(_new(price=34_000, msrp=36_500))
    assert out["msrp"] == 36_500
    assert out["label"] == "MSRP"
    assert out["from_sticker"] is False
    assert out["source"] == "dealer listing (cars.msrp)"
    assert out["savings"] == 2_500
    assert out["savings_basis"] == "cars.msrp - cars.price"


def test_out_of_band_feed_msrp_is_rejected() -> None:
    assert resolve_display_msrp(_new(price=100, msrp=999))["msrp"] is None
    assert resolve_display_msrp(_new(price=100, msrp=9_000_000))["msrp"] is None


# --- rule 3: a parsed window sticker is preferred and labelled ----------------


def test_sticker_msrp_beats_the_feed_on_a_used_car(monkeypatch) -> None:
    """The M4 with its own sticker in reach: $86,600, not $38,990."""
    _with_sticker(monkeypatch, 86_600)
    car = _used(vin="WBS3U9C58GP969438", make="BMW", model="M4",
                price=36_788.0, msrp=38_990.0)
    out = resolve_display_msrp(car)
    assert out["msrp"] == 86_600
    assert out["from_sticker"] is True
    assert out["source"] == "window sticker (parsed)"
    # ...and the gap to today's price is depreciation, not a dealer discount.
    assert out["savings"] is None
    assert out["label"] == "Original MSRP"


def test_sticker_msrp_beats_the_feed_on_a_new_car(monkeypatch) -> None:
    _with_sticker(monkeypatch, 36_500)
    out = resolve_display_msrp(_new(price=34_000, msrp=35_000))
    assert out["msrp"] == 36_500
    assert out["from_sticker"] is True
    assert out["label"] == "MSRP"
    assert out["savings"] == 2_500
    assert out["savings_basis"] == "window sticker MSRP - cars.price"


def test_sticker_msrp_renders_on_a_car_with_no_price(monkeypatch) -> None:
    """With no asking price the MSRP is the only figure the page has."""
    _with_sticker(monkeypatch, 66_880)
    out = resolve_display_msrp(_used(price=None))
    assert out["msrp"] == 66_880
    assert out["savings"] is None


def test_allow_sticker_false_skips_the_disk_read(monkeypatch) -> None:
    def _boom(car):
        raise AssertionError("bulk paths must not touch the sticker store")

    monkeypatch.setattr(msrp_trust, "sticker_msrp_for_car", _boom)
    assert resolve_display_msrp(_used(msrp=38_990), allow_sticker=False)["msrp"] is None


# --- the file parser behind the sticker source --------------------------------


def test_sticker_total_is_read_out_of_a_stored_sticker(tmp_path) -> None:
    p = tmp_path / "window_sticker.txt"
    p.write_text(
        "2016 BMW M4 Coupe\nAdded Options ...\nDestination Charge $995.00\n"
        "Net Total $86,600.00\n"
    )
    st = p.stat()
    assert msrp_trust._sticker_msrp_from_file(str(p), st.st_mtime_ns, st.st_size) == 86_600


def test_unparseable_sticker_yields_nothing(tmp_path) -> None:
    p = tmp_path / "window_sticker.txt"
    p.write_text("This document has no price on it at all.\n")
    st = p.stat()
    assert msrp_trust._sticker_msrp_from_file(str(p), st.st_mtime_ns, st.st_size) is None


def test_image_stickers_are_not_guessed_at(tmp_path) -> None:
    p = tmp_path / "window_sticker.png"
    p.write_bytes(b"\x89PNG\r\n\x1a\n" + b"0" * 40_000)
    st = p.stat()
    assert msrp_trust._sticker_msrp_from_file(str(p), st.st_mtime_ns, st.st_size) is None


# --- what the car page and the build sheet actually receive -------------------


def test_serializer_suppresses_the_feed_msrp_on_a_used_car() -> None:
    car = _used(vin="5UX83DP05R9U16883", make="BMW", model="X3",
                year=2024, price=48_990.0, msrp=48_990.0)
    out = serialize_car_for_api(car, include_verified=False)
    # Rule 4: no MSRP slot at all — not an em-dash, not the price echoed back.
    assert out["msrp"] is None
    assert out["msrp_label"] is None
    assert out["below_msrp"] is None
    assert out["price"] == 48_990.0


def test_serializer_passes_a_trustworthy_msrp_through() -> None:
    out = serialize_car_for_api(_new(price=34_000, msrp=36_500), include_verified=False)
    assert out["msrp"] == 36_500
    assert out["msrp_label"] == "MSRP"
    assert out["msrp_from_sticker"] is False
    assert out["below_msrp"] == 2_500


def test_serializer_labels_a_sticker_derived_msrp(monkeypatch) -> None:
    _with_sticker(monkeypatch, 66_880)
    car = _used(vin="5UX83DP05R9U16883", make="BMW", model="X3",
                year=2024, price=48_990.0, msrp=48_990.0)
    out = serialize_car_for_api(car, include_verified=False)
    assert out["msrp"] == 66_880
    assert out["msrp_from_sticker"] is True
    assert out["msrp_label"] == "Original MSRP"
    assert out["msrp_source"] == "window sticker (parsed)"
    assert out["below_msrp"] is None


def test_build_sheet_prints_no_msrp_and_no_savings_for_the_m4() -> None:
    car = _used(vin="WBS3U9C58GP969438", make="BMW", model="M4",
                price=36_788.0, msrp=38_990.0)
    pricing = build_generated_spec_sheet(car, verified_specs={})["pricing"]
    assert pricing["msrp_display"] is None
    assert pricing["savings"] is None
    assert pricing["savings_display"] is None
    assert pricing["savings_derived"] is False
    # The listed price still stands alone.
    assert pricing["price_display"] == "$36,788"


def test_build_sheet_shows_the_sticker_msrp_labelled(monkeypatch) -> None:
    _with_sticker(monkeypatch, 86_600)
    car = _used(vin="WBS3U9C58GP969438", make="BMW", model="M4",
                price=36_788.0, msrp=38_990.0)
    pricing = build_generated_spec_sheet(car, verified_specs={})["pricing"]
    assert pricing["msrp_display"] == "$86,600"
    assert pricing["msrp_label"] == "Original MSRP"
    assert pricing["msrp_from_sticker"] is True
    assert pricing["savings"] is None


def test_build_sheet_savings_survive_on_new_inventory() -> None:
    pricing = build_generated_spec_sheet(
        _new(price=24_990, msrp=27_100), verified_specs={}
    )["pricing"]
    assert pricing["msrp_display"] == "$27,100"
    assert pricing["savings"] == 2_110
    assert pricing["savings_derived"] is True
    assert pricing["savings_basis"] == "cars.msrp - cars.price"
