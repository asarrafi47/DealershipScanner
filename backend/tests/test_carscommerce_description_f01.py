"""F01 (docs/DATA_COMPLETENESS_FINDINGS_2026_09_28.md): Dealer Inspire / CarsCommerce
search feeds carry the listing copy in ``extra_fields.description_text`` and
``description_text_vdp``; the parser and the harvest mapper must emit it because
the VDP prefetch on those hosts answers 403. Fixture shape is the saved
billluke-com feed listing (h6/billluke-com.feed_listing.json)."""
from __future__ import annotations

from backend.parsers.carscommerce import parse as parse_carscommerce
from backend.scanner.carscommerce_harvest import HARVEST_FIELDS, _map_listing

_BILLLUKE_TEXT = (
    "Bright White Clearcoat 2025 Jeep Grand Cherokee Limited 4WD 8-Speed Automatic "
    "3.6L V6 24V VVT<br><br>19/26 City/Highway MPG"
)


def _listing(extra: dict | None) -> dict:
    return {
        "vin": "1C4RJHBG5S8712345", "stock": "S8712345", "type": "New", "year": 2025,
        "make": "Jeep", "model": "Grand Cherokee", "trim": "Limited", "mileage": 5,
        "vdp_url": "https://www.billluke.com/inventory/new-2025-jeep-grand-cherokee-limited-4wd-1C4RJHBG5S8712345/",
        "styles": [{"exterior_color": "Bright White Clearcoat", "interior_color": "Global Black"}],
        "mechanical": {"engine": "3.6L V6 24V VVT", "drivetrain": "4WD", "fuel_type": "Gasoline Fuel",
                       "transmission": "8-Speed Automatic"},
        "body_details": {"type": "SUV"}, "media": {"images": ["https://img.example/1.jpg"]},
        "pricing": {"msrp": 52000, "price": 49500}, "dealer": {"location": "Phoenix, AZ 85014"},
        "extra_fields": extra if extra is not None else {},
    }


def test_harvest_mapper_emits_description_from_feed_copy():
    m = _map_listing(_listing({"description_text": _BILLLUKE_TEXT, "description_text_vdp": _BILLLUKE_TEXT}))
    assert m is not None
    assert m["description"].startswith("Bright White Clearcoat 2025 Jeep Grand Cherokee Limited 4WD")
    assert "<br>" not in m["description"]
    assert "19/26 City/Highway MPG" in m["description"]
    assert "description" in HARVEST_FIELDS  # harvest_carscommerce.py fills it without a rescan


def test_parser_row_carries_description():
    rows = parse_carscommerce({"data": {"listings": [_listing({"description_text": _BILLLUKE_TEXT})]}},
                              "https://www.billluke.com", "billluke-com", "Bill Luke", "https://www.billluke.com")
    assert len(rows) == 1
    assert rows[0]["description"].startswith("Bright White Clearcoat 2025 Jeep")


def test_longer_variant_wins_and_entities_unescape():
    short = "Clean CARFAX. One owner. Great condition all around, come see it today."
    long_vdp = (
        "Clean CARFAX. One owner. Great condition all around, come see it today. "
        "Heated &amp; ventilated seats, panoramic roof, adaptive cruise.<br>Call now."
    )
    m = _map_listing(_listing({"description_text": short, "description_text_vdp": long_vdp}))
    assert m["description"].startswith("Clean CARFAX. One owner.")
    assert "Heated & ventilated seats" in m["description"]
    assert "&amp;" not in m["description"]


def test_short_or_missing_copy_stays_none():
    assert _map_listing(_listing({}))["description"] is None
    assert _map_listing(_listing({"description_text": "", "description_text_vdp": None}))["description"] is None
    assert _map_listing(_listing({"description_text": "In transit."}))["description"] is None  # < 40 chars
    rows = parse_carscommerce({"data": {"listings": [_listing({})]}}, "https://www.billluke.com", "billluke-com",
                              "Bill Luke", "https://www.billluke.com")
    assert rows[0].get("description") is None


def test_description_capped_at_4000():
    m = _map_listing(_listing({"description_text": "word " * 2000}))
    assert len(m["description"]) <= 4000
