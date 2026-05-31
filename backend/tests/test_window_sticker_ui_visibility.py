"""Window sticker UI visibility: CDJR/Stellantis vs listing-provided sticker."""

from __future__ import annotations

from backend.scanner.window_sticker import (
    car_has_listing_sticker_signal,
    car_listing_sticker_urls,
    is_cdjr_stellantis_car,
    is_sticker_media_url,
    should_auto_fetch_oem_window_sticker,
    show_window_sticker_panel,
    show_window_sticker_ui,
)


def test_is_sticker_media_url_detects_monroney_gallery_path() -> None:
    assert is_sticker_media_url("https://cdn.dealer.com/photos/monroney/123.jpg") is True
    assert is_sticker_media_url("https://cdn.dealer.com/exterior/front.jpg") is False


def test_show_window_sticker_ui_cdjr_without_listing_signal(monkeypatch) -> None:
    car = {"vin": "1C6RRFFG0ZZ999999", "make": "Ram", "gallery": []}
    assert is_cdjr_stellantis_car(car) is True
    monkeypatch.setattr(
        "backend.enrichment.window_sticker_service.window_sticker_has_visual",
        lambda _c: False,
    )
    assert show_window_sticker_panel(car) is False
    assert should_auto_fetch_oem_window_sticker(car) is True


def test_show_window_sticker_ui_bmw_hidden_without_listing_sticker() -> None:
    car = {
        "vin": "WBA3A5C50FD123456",
        "make": "BMW",
        "gallery": ["https://cdn.dealer.com/exterior/1.jpg"],
    }
    assert is_cdjr_stellantis_car(car) is False
    assert car_has_listing_sticker_signal(car) is False
    assert show_window_sticker_ui(car) is False
    assert should_auto_fetch_oem_window_sticker(car) is False


def test_show_window_sticker_ui_bmw_with_gallery_sticker() -> None:
    car = {
        "vin": "WBA3A5C50FD123456",
        "make": "BMW",
        "gallery": [
            "https://cdn.dealer.com/exterior/1.jpg",
            "https://cdn.dealer.com/monroney/window-sticker.jpg",
        ],
    }
    assert show_window_sticker_ui(car) is True
    assert car_listing_sticker_urls(car) == [
        "https://cdn.dealer.com/monroney/window-sticker.jpg",
    ]


def test_show_window_sticker_ui_ford_requires_listing_sticker() -> None:
    car = {"vin": "1FAFP45F62F123456", "make": "Ford", "gallery": []}
    assert is_cdjr_stellantis_car(car) is False
    assert show_window_sticker_ui(car) is False
    assert should_auto_fetch_oem_window_sticker(car) is False

    car["window_sticker_url"] = "https://cdn.dealer.com/inventory/monroney/sticker.pdf"
    assert show_window_sticker_ui(car) is False
    car["window_sticker_url"] = "https://cdn.dealer.com/inventory/monroney/sticker.jpg"
    assert show_window_sticker_ui(car) is True


def test_show_window_sticker_ui_hidden_for_unreadable_vision_only() -> None:
    car = {"vin": "4T1DAACK0SU123456", "make": "Toyota", "gallery": []}
    ctx = {
        "listing_sticker_options": [
            "Window sticker visible on windshield - unreadable in this image."
        ],
    }
    assert show_window_sticker_panel(car, ctx) is False


def test_listing_sticker_direct_image_url_rejects_html_landing_pages() -> None:
    from backend.scanner.window_sticker import (
        listing_sticker_direct_image_url,
        sticker_embed_preview_url,
    )

    mbusa = "https://www.mbusa.com/en/vehicles/vin/W1NKM4GB5SF251603/window-sticker"
    assert listing_sticker_direct_image_url(mbusa) is None
    jpg = "https://cdn.dealer.com/inventory/monroney/sticker.jpg"
    assert listing_sticker_direct_image_url(jpg) == jpg
    car = {
        "vin": "W1NKM4GB5SF251603",
        "window_sticker_url": mbusa,
    }
    assert sticker_embed_preview_url(car, preview_api_url="/car/1/window-sticker-preview.png", has_paid_access=True) is None
    car["gallery"] = [jpg]
    assert sticker_embed_preview_url(car, preview_api_url="/car/1/window-sticker-preview.png", has_paid_access=True) == jpg
