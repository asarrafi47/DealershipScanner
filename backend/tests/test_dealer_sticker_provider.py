"""Dealer sticker provider detection and sticker section grouping."""

from __future__ import annotations

from backend.scanner.dealer_sticker_provider import (
    PROVIDER_IPACKET,
    PROVIDER_NONE,
    detect_sticker_provider_from_html,
    detect_sticker_provider_from_urls,
    should_try_ipacket_website_plugin,
)
from backend.scanner.window_sticker import (
    _repair_sticker_credit_name,
    is_cdjr_stellantis_car,
    organize_sticker_option_sections,
)


def test_detect_ipacket_from_html() -> None:
    html = '<script src="https://cdn.autoipacket.com/widget.js"></script>'
    assert detect_sticker_provider_from_html(html) == PROVIDER_IPACKET


def test_detect_ipacket_from_urls() -> None:
    urls = ["https://djapi.autoipacket.com/v2/sticker-puller/download/VIN?token=x"]
    assert detect_sticker_provider_from_urls(urls) == PROVIDER_IPACKET


def test_should_not_try_ipacket_for_cdjr() -> None:
    car = {"make": "Jeep", "vin": "1C4RJFBG0LC123456"}
    assert is_cdjr_stellantis_car(car) is True
    assert should_try_ipacket_website_plugin(car, "<html></html>") is False


def test_should_skip_ipacket_when_dealer_none(monkeypatch) -> None:
    monkeypatch.setattr(
        "backend.scanner.dealer_sticker_provider.get_car_dealer_sticker_provider",
        lambda _car: PROVIDER_NONE,
    )
    car = {"make": "Mercedes-Benz", "vin": "W1K6G7GB6NA126084", "dealership_registry_id": 99}
    assert should_try_ipacket_website_plugin(car, "") is False


def test_repair_credit_name() -> None:
    assert _repair_sticker_credit_name("Hands-Free Access Features Not O CREDITnal") == (
        "Hands-Free Access Features Not Original"
    )


def test_organize_sticker_sections_mercedes() -> None:
    items = [
        {"name": "DC1 Night Package", "price": 400, "code": "DC1"},
        {"name": "P55 Night Package", "price": None, "code": "P55"},
        {"name": "Wheel Locking Bolts", "price": 150, "code": "RLB"},
        {"name": "Hands-Free Access Features Not O CREDITnal", "price": -100, "code": "D6M"},
        {"name": "Base", "price": 117700, "code": None},
        {"name": "Black Nappa Leather", "price": None, "code": None},
        {"name": "Dash Cam", "price": None, "code": None},
    ]
    sections = organize_sticker_option_sections(items)
    assert len(sections["options"]) == 1
    assert sections["options"][0]["name"] == "Wheel Locking Bolts"
    assert len(sections["packages"]) == 1
    assert sections["packages"][0]["name"] == "DC1 Night Package"
    assert any(f.get("name") == "P55 Night Package" for f in sections["packages"][0]["features"])
    assert sections["base"]["price"] == 117700
    assert len(sections["base"]["features"]) == 2
    assert "credits" not in sections
    assert "accessories" not in sections


def test_organize_sticker_sections_flat_optional_order() -> None:
    items = [
        {"name": "DC1 Night Package", "price": 400, "code": "DC1"},
        {"name": "P55 Night Package", "price": None, "code": "P55"},
        {"name": "Wheel Locking Bolts", "price": 150, "code": "RLB"},
        {"name": "Rear Spoiler, Body Color", "price": 500, "code": None},
        {"name": "First-Aid Kit", "price": 35, "code": None},
        {"name": "DG3 AMG Line", "price": 4300, "code": "DG3"},
        {"name": "DU3 Warmth and Comfort Package", "price": 3150, "code": "DU3"},
        {"name": "Hands-Free Access Features Not O CREDITnal", "price": -100, "code": "D6M"},
    ]
    sections = organize_sticker_option_sections(items)
    assert [g["name"] for g in sections["packages"]] == ["DC1 Night Package"]
    assert [f["name"] for f in sections["packages"][0]["features"]] == ["P55 Night Package"]
    assert [g["name"] for g in sections["options"]] == [
        "Wheel Locking Bolts",
        "Rear Spoiler, Body Color",
        "First-Aid Kit",
        "DG3 AMG Line",
        "DU3 Warmth and Comfort Package",
    ]
