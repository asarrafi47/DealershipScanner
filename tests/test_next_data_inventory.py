"""__NEXT_DATA__ extraction for inventory HTML fallback."""

from __future__ import annotations

from backend.scrapers.next_data_inventory import parse_next_data_json_from_html


def test_parse_next_data_json_from_html_basic() -> None:
    html = (
        '<!DOCTYPE html><html><body>'
        '<script id="__NEXT_DATA__" type="application/json">'
        '{"props":{"pageProps":{"vehicles":[{"vin":"1HGBH41JXMN109185"}]}}}'
        "</script></body></html>"
    )
    data = parse_next_data_json_from_html(html)
    assert data is not None
    assert data["props"]["pageProps"]["vehicles"][0]["vin"] == "1HGBH41JXMN109185"


def test_parse_next_data_missing_returns_none() -> None:
    assert parse_next_data_json_from_html("<html></html>") is None
