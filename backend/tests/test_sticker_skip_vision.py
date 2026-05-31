"""Skip photo/vision analysis when sticker or listing description already has equipment."""

from __future__ import annotations

from backend.enrichment.window_sticker_service import (
    packages_has_parsed_description,
    packages_has_sticker_options,
    should_skip_photo_package_analysis,
    window_sticker_available,
)
from backend.scanner.window_sticker import (
    build_sticker_option_dedupe_keys,
    normalize_option_dedupe_key,
    option_name_overlaps_sticker,
)


def test_should_skip_photo_when_sticker_file_exists() -> None:
    car = {"vin": "W1K6G7GB6NA126084", "packages": "{}"}
    assert should_skip_photo_package_analysis(car) is False or window_sticker_available(car)


def test_should_skip_photo_when_sticker_options_in_packages() -> None:
    car = {
        "vin": "W1K6G7GB6NA126084",
        "packages": '{"sticker_options_priced": [{"name": "AMG Line", "price": 4300}]}',
    }
    assert should_skip_photo_package_analysis(car) is True


def test_should_skip_photo_when_description_parsed() -> None:
    car = {
        "vin": "W1K6G7GB6NA126084",
        "packages": (
            '{"packages_normalized": [{"name": "Premium Package", "features": ["Sunroof"]}], '
            '"listing_description_parser_version": "3"}'
        ),
    }
    assert packages_has_parsed_description(
        __import__("json").loads(car["packages"])
    )
    assert should_skip_photo_package_analysis(car) is True


def test_option_dedupe_overlaps() -> None:
    keys = build_sticker_option_dedupe_keys(
        [{"name": "DG3 AMG Line", "price": 4300}, {"name": "Night Package", "price": 400}]
    )
    assert option_name_overlaps_sticker("AMG Line", keys)
    assert option_name_overlaps_sticker("DC1 Night Package", keys)
    assert not option_name_overlaps_sticker("Heated Seats", keys)


def test_normalize_option_dedupe_key_strips_code() -> None:
    assert normalize_option_dedupe_key("DG3 AMG Line") == normalize_option_dedupe_key("AMG Line")
