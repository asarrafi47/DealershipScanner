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

    Likewise the trim-band plausibility cache defaults to loaded-and-empty so
    no test reaches the live ``trim_msrp_bands`` table; tests that want a band
    seed ``_BAND_CACHE`` themselves.
    """
    monkeypatch.setattr(msrp_trust, "sticker_msrp_for_car", lambda car: None)
    monkeypatch.setattr(msrp_trust, "_BAND_LOADED", True)
    monkeypatch.setattr(msrp_trust, "_BAND_CACHE", {})


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


# --- rule 4: an MSRP far outside the trim's observed band is a data error -----


def test_implausible_for_trim_msrp_is_suppressed_with_reason(monkeypatch) -> None:
    reason = "365,000 is far above the 42 observed stickers for this trim (max 27,100)"
    monkeypatch.setattr(
        msrp_trust,
        "implausible_for_trim",
        lambda car, msrp: reason if msrp == 365_000 else None,
    )
    out = resolve_display_msrp(_new(price=34_000, msrp=365_000))
    assert out["msrp"] is None
    assert out["label"] is None
    assert out["source"] is None
    assert out["savings"] is None
    assert out["rejected"] == reason


def test_implausible_sticker_msrp_is_suppressed_too(monkeypatch) -> None:
    """The document source gets no exemption from the band check either."""
    _with_sticker(monkeypatch, 366_500)
    monkeypatch.setattr(
        msrp_trust, "implausible_for_trim", lambda car, msrp: "far above the band"
    )
    out = resolve_display_msrp(_new(price=34_000))
    assert out["msrp"] is None
    assert out["savings"] is None
    assert out["rejected"] == "far above the band"


def test_plausible_msrp_passes_the_trim_guard_untouched(monkeypatch) -> None:
    calls: list[float] = []

    def _guard(car, msrp):
        calls.append(msrp)
        return None

    monkeypatch.setattr(msrp_trust, "implausible_for_trim", _guard)
    out = resolve_display_msrp(_new(price=34_000, msrp=36_500))
    assert out["msrp"] == 36_500
    assert out["savings"] == 2_500
    assert out["rejected"] is None
    assert calls == [36_500]  # the guard saw exactly the value being shown


def test_trim_band_guard_end_to_end(monkeypatch) -> None:
    """Through the real ``implausible_for_trim`` off a seeded observed band."""
    monkeypatch.setattr(
        msrp_trust,
        "_BAND_CACHE",
        {(2025, "honda", "accord", "touring"): (32_000, 42_000, 57)},
    )
    out = resolve_display_msrp(_new(trim="Touring", price=34_000, msrp=95_000))
    assert out["msrp"] is None
    assert out["savings"] is None
    assert out["rejected"] == (
        "95,000 is far above the 57 observed stickers for this trim (max 42,000)"
    )
    # The same trim at a believable figure sails through.
    ok = resolve_display_msrp(_new(trim="Touring", price=34_000, msrp=36_500))
    assert ok["msrp"] == 36_500
    assert ok["savings"] == 2_500
    assert ok["rejected"] is None


def test_serializer_suppresses_a_trim_implausible_msrp(monkeypatch) -> None:
    monkeypatch.setattr(
        msrp_trust, "implausible_for_trim", lambda car, msrp: "far above the band"
    )
    out = serialize_car_for_api(_new(price=34_000, msrp=365_000), include_verified=False)
    assert out["msrp"] is None
    assert out["msrp_label"] is None
    assert out["below_msrp"] is None
    assert out["price"] == 34_000


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


# --- the sidecar overlay: photographed stickers and observed trim bands -------
#
# ``msrp_overlay_public_fields`` (backend/utils/car_serialize/msrp_overlay.py)
# only ever ADDS to a car the resolver left without an MSRP, and its two fields
# are distinct from ``msrp`` by construction — a sticker photographed in the
# listing's gallery as ``sticker_msrp``, the trim's observed range as
# ``estimated_msrp_band``. Neither may ever masquerade as the feed figure.


def _overlay(car, sticker=None):
    from backend.utils.car_serialize.msrp_overlay import msrp_overlay_public_fields

    return msrp_overlay_public_fields(car, sticker)


def _accord_band(monkeypatch, lo=32_000, hi=42_000, n=57):
    monkeypatch.setattr(
        msrp_trust, "_BAND_CACHE", {(2025, "honda", "accord", "touring"): (lo, hi, n)}
    )


def test_overlay_adds_nothing_when_a_trusted_msrp_already_renders(monkeypatch) -> None:
    """The resolver's figure stands alone — even with a sticker photo and a band."""
    _accord_band(monkeypatch)
    out = serialize_car_for_api(
        _new(trim="Touring", price=34_000, msrp=36_500), include_verified=False
    )
    assert out["msrp"] == 36_500
    assert _overlay(out, sticker=37_200) == {}


def test_overlay_shows_a_photographed_sticker_as_a_distinct_field() -> None:
    out = serialize_car_for_api(_new(price=34_000), include_verified=False)
    assert out["msrp"] is None
    fields = _overlay(out, sticker=37_200)
    assert fields["sticker_msrp"] == 37_200
    assert fields["sticker_msrp_source"] == "window_sticker_photo"
    assert fields["sticker_msrp_label"] == "MSRP"
    # The trusted slot stays empty: nothing here rewrites ``msrp`` — and nothing
    # here may stamp the resolver-owned ``msrp_source`` either, or provenance
    # renders for an MSRP that does not exist.
    assert "msrp" not in fields
    assert "msrp_source" not in fields


def test_overlay_serves_a_dealer_build_sheet_with_its_own_provenance() -> None:
    """2026-08-18 policy: a build-sheet total is stored and served, but worded as
    exactly what it is — never dressed up as the factory Monroney."""
    out = serialize_car_for_api(_new(price=34_000), include_verified=False)
    fields = _overlay(
        out,
        sticker={
            "value": 37_200,
            "sticker_is_original": False,
            "msrp_document": "dealer_build_sheet",
        },
    )
    assert fields["sticker_msrp"] == 37_200
    assert fields["sticker_msrp_source"] == "dealer_build_sheet_photo"
    assert fields["sticker_msrp_label"] == "MSRP"
    assert "dealer build sheet" in fields["sticker_msrp_note"]
    assert "not the factory Monroney" in fields["sticker_msrp_note"]


def test_overlay_mapping_entry_keeps_the_monroney_wording() -> None:
    """The dict shape from car_sticker_msrp_values words an original document
    exactly like the legacy bare-number path."""
    out = serialize_car_for_api(_new(price=34_000), include_verified=False)
    fields = _overlay(
        out,
        sticker={
            "value": 37_200,
            "sticker_is_original": True,
            "msrp_document": "monroney",
        },
    )
    assert fields["sticker_msrp"] == 37_200
    assert fields["sticker_msrp_source"] == "window_sticker_photo"
    assert "window sticker" in fields["sticker_msrp_note"]


def test_overlay_build_sheet_below_the_price_is_still_not_an_msrp() -> None:
    """The price/band guards apply to build-sheet totals exactly as to Monroneys."""
    out = serialize_car_for_api(_new(price=34_000), include_verified=False)
    fields = _overlay(
        out,
        sticker={
            "value": 34_000,
            "sticker_is_original": False,
            "msrp_document": "dealer_build_sheet",
        },
    )
    assert "sticker_msrp" not in fields


def test_overlay_labels_a_used_cars_photographed_sticker_original() -> None:
    out = serialize_car_for_api(
        _used(price=36_788.0, msrp=38_990.0), include_verified=False
    )
    assert out["msrp"] is None  # the feed "was" price was rejected
    fields = _overlay(out, sticker=86_600)
    assert fields["sticker_msrp"] == 86_600
    assert fields["sticker_msrp_label"] == "Original MSRP"


def test_overlay_sticker_at_or_below_the_price_is_not_an_msrp(monkeypatch) -> None:
    """Rule (b) applies to the photographed source too; the band steps in instead."""
    _accord_band(monkeypatch)
    out = serialize_car_for_api(_new(trim="Touring", price=34_000), include_verified=False)
    fields = _overlay(out, sticker=34_000)
    assert "sticker_msrp" not in fields
    assert fields["estimated_msrp_band"] == {
        "low": 32_000, "high": 42_000, "observations": 57
    }


def test_overlay_band_disproves_a_photographed_sticker(monkeypatch) -> None:
    """A total far outside everything this trim has stickered at is a misread."""
    _accord_band(monkeypatch)
    out = serialize_car_for_api(_new(trim="Touring", price=34_000), include_verified=False)
    fields = _overlay(out, sticker=95_000)
    assert "sticker_msrp" not in fields
    assert fields["estimated_msrp_band"]["high"] == 42_000


def test_overlay_band_only_is_labelled_an_estimate(monkeypatch) -> None:
    _accord_band(monkeypatch)
    out = serialize_car_for_api(_new(trim="Touring", price=34_000), include_verified=False)
    fields = _overlay(out)
    assert fields["estimated_msrp_band"] == {
        "low": 32_000, "high": 42_000, "observations": 57
    }
    assert "not this car's sticker price" in fields["estimated_msrp_band_note"]
    assert "sticker_msrp" not in fields


def test_overlay_with_no_sticker_and_no_band_says_nothing() -> None:
    out = serialize_car_for_api(_new(price=34_000), include_verified=False)
    assert _overlay(out) == {}


def test_overlay_band_lookup_uses_the_raw_identity_not_the_display_one(monkeypatch) -> None:
    """BMW model/trim are display-normalized in the serialized dict; the band keys
    are built from ``cars.model`` / ``cars.trim`` verbatim, so the raw row decides."""
    from backend.utils.car_serialize.msrp_overlay import msrp_overlay_public_fields

    monkeypatch.setattr(
        msrp_trust, "_BAND_CACHE", {(2025, "bmw", "x5 xdrive40i", ""): (76_375, 93_125, 59)}
    )
    raw = _new(make="BMW", model="X5 xDrive40i", trim=None, price=71_000)
    ser = serialize_car_for_api(dict(raw), include_verified=False)
    fields = msrp_overlay_public_fields(ser, None, identity=raw)
    assert fields["estimated_msrp_band"]["observations"] == 59


# --- the batched car_image_text read behind the overlay -----------------------


_CAR_IMAGE_TEXT_DDL = """
CREATE TABLE car_image_text (
    car_id       INTEGER PRIMARY KEY,
    vin          TEXT,
    version      INTEGER NOT NULL DEFAULT 0,
    summary      TEXT,
    has_sticker  INTEGER NOT NULL DEFAULT 0,
    sticker_msrp REAL,
    equipment_count INTEGER NOT NULL DEFAULT 0
);
"""


def _seed_image_text(path, rows):
    import json
    import sqlite3

    conn = sqlite3.connect(str(path))
    try:
        conn.executescript(_CAR_IMAGE_TEXT_DDL)
        conn.executemany(
            "INSERT INTO car_image_text (car_id, version, summary, sticker_msrp) "
            "VALUES (?,?,?,?)",
            [(cid, ver, json.dumps(summary), msrp) for cid, ver, summary, msrp in rows],
        )
        conn.commit()
    finally:
        conn.close()


def test_sticker_msrp_read_keeps_only_provable_agent_vision_rows(sqlite_inventory) -> None:
    """Same provenance bar as build_trim_msrp_bands: agent-vision version, total
    READ off the document, nothing recorded before the guards existed."""
    from backend.db.repositories.cars_repo import car_sticker_msrp_values

    _seed_image_text(sqlite_inventory.path, [
        (1, 100, {"msrp_read_directly": True}, 37_200.0),
        # Local-OCR era row (version 1-99): never served, whatever it claims.
        (2, 1, {"msrp_read_directly": True}, 41_000.0),
        # Recorded before the provenance guards; cannot be shown to be right.
        (3, 100, {"msrp_read_directly": True, "msrp_provenance": "unverified_pre_guard"}, 52_000.0),
        # Total was reconstructed, not read -- where a missed line hides.
        (4, 100, {"msrp_read_directly": False}, 44_500.0),
        (5, 100, {"msrp_read_directly": True}, None),
        # recheck_msrp parked this one for human review: an agent proposed
        # withdrawing it, and a number under active dispute must not be served.
        (6, 100, {"msrp_read_directly": True, "msrp_recheck": "withdraw_proposed"}, 48_000.0),
        # A dealer build sheet (2026-08-18 policy): stored AND served, with its
        # document type riding along so the overlay can word it honestly.
        (7, 100, {
            "msrp_read_directly": True,
            "sticker_is_original": False,
            "msrp_document": "dealer_build_sheet",
        }, 57_515.0),
    ])

    # Only the MSRP-relevant projection is asserted: the same entries also carry
    # the sticker's printed-color pass-through, which has its own tests.
    def _msrp_view(entries):
        return {
            cid: {
                k: e[k] for k in ("value", "sticker_is_original", "msrp_document")
            }
            for cid, e in entries.items()
            if e.get("value") is not None
        }

    monroney = {
        "value": 37_200, "sticker_is_original": True, "msrp_document": "monroney",
    }
    build_sheet = {
        "value": 57_515,
        "sticker_is_original": False,
        "msrp_document": "dealer_build_sheet",
    }
    assert _msrp_view(car_sticker_msrp_values([1, 2, 3, 4, 5, 6, 7])) == {
        1: monroney, 7: build_sheet,
    }
    assert _msrp_view(car_sticker_msrp_values([2])) == {}
    assert _msrp_view(car_sticker_msrp_values()) == {1: monroney, 7: build_sheet}


def test_sticker_msrp_read_degrades_to_nothing_without_the_table(sqlite_inventory) -> None:
    """A dev/test database without car_image_text must still serve the page."""
    from backend.db.repositories.cars_repo import car_sticker_msrp_values

    assert car_sticker_msrp_values([1]) == {}
    assert car_sticker_msrp_values() == {}
