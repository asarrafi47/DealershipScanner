"""Dealer manifest URL → inventory scrape base (origin when path is .htm/.html)."""

from __future__ import annotations

from backend.scanner.dealer_site_url import dealer_inventory_base_url


def test_document_url_collapses_to_origin() -> None:
    base, norm = dealer_inventory_base_url("https://deal.example.com/service/schedule-service.htm")
    assert norm is True
    assert base == "https://deal.example.com"


def test_html_document_collapses_to_origin() -> None:
    base, norm = dealer_inventory_base_url("http://deal.example.com/promo/landing.HTML")
    assert norm is True
    assert base == "http://deal.example.com"


def test_inventory_directory_preserved() -> None:
    base, norm = dealer_inventory_base_url("https://deal.example.com/new-inventory/")
    assert norm is False
    assert base == "https://deal.example.com/new-inventory"


def test_plain_homepage_untouched() -> None:
    base, norm = dealer_inventory_base_url("https://deal.example.com")
    assert norm is False
    assert base == "https://deal.example.com"


def test_prepends_https_when_missing_scheme() -> None:
    base, norm = dealer_inventory_base_url("deal.example.com/foo.htm")
    assert norm is True
    assert base == "https://deal.example.com"
