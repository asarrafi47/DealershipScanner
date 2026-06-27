"""Tests for dealership URL normalization helpers."""

from __future__ import annotations

from backend.db.dealership_url import effective_website_url, normalized_dealership_host


def test_effective_website_url_prefers_website_url() -> None:
    row = {"website_url": "https://www.example.com/", "dealer_website_url": "https://other.com/"}
    assert effective_website_url(row) == "https://www.example.com/"


def test_effective_website_url_falls_back_to_dealer_website_url() -> None:
    row = {"website_url": "", "dealer_website_url": "https://dealer.example.com"}
    assert effective_website_url(row) == "https://dealer.example.com"


def test_normalized_dealership_host_strips_www() -> None:
    host = normalized_dealership_host(url="https://WWW.Dealer-Example.COM/inventory")
    assert host == "dealer-example.com"
