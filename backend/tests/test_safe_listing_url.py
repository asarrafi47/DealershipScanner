"""Listing image URL sanitization for mobile + web grid JSON."""

from __future__ import annotations

import pytest

from backend.utils.safe_listing_url import normalize_listing_image_url


def test_blocks_javascript_and_data_urls() -> None:
    assert normalize_listing_image_url("javascript:alert(1)") is None
    assert normalize_listing_image_url("data:image/png;base64,abc") is None


def test_allows_car_images_path() -> None:
    assert normalize_listing_image_url("/car-images/dealer/vin/1.jpg") == "/car-images/dealer/vin/1.jpg"


def test_allows_https_dealer_cdn() -> None:
    u = normalize_listing_image_url("https://images.dealer.com/photo.jpg")
    assert u == "https://images.dealer.com/photo.jpg"


def test_blocks_private_ip_image_url() -> None:
    assert normalize_listing_image_url("https://192.168.1.10/photo.jpg") is None


def test_blocks_localhost_in_production(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLASK_ENV", "production")
    assert normalize_listing_image_url("https://127.0.0.1/photo.jpg") is None
    assert normalize_listing_image_url("https://localhost/photo.jpg") is None


def test_normalize_safe_http_url_blocks_javascript() -> None:
    from backend.utils.safe_listing_url import normalize_safe_http_url

    assert normalize_safe_http_url("javascript:alert(1)") is None
    assert normalize_safe_http_url("https://dealer.example.com") == "https://dealer.example.com"


def test_blocks_credentials_in_url() -> None:
    assert normalize_listing_image_url("https://user:pass@example.com/x.jpg") is None
