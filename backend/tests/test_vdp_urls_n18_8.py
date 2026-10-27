"""N18.8: resolver reads the payload's _source_url; guessed URLs on non-dealer.com platforms
are not fetched; page fields apply only when the VIN is on the page; F5 counter."""
from __future__ import annotations

import asyncio

from backend.parsers.vdp_urls import (
    GUESSED_KEY,
    apply_vehicle_source_url,
    guessed_url_fetchable,
    mark_guessed,
    resolve_vehicle_source_url,
)
from backend.scanner.vdp import prefetch as pf

VIN = "1HGBH41JXMN109186"
REAL = "https://www.dealer.test/inventory/used-2022-honda-accord-1hgbh41jxmn109186/"


def test_resolver_reads_payload_source_url_over_a_guess():
    row = {"vin": VIN, "dealer_url": "https://www.dealer.test", "_source_url": REAL}
    assert resolve_vehicle_source_url(row) == REAL
    apply_vehicle_source_url(row)
    assert row["source_url"] == REAL and GUESSED_KEY not in row


def test_payload_source_url_clears_an_earlier_guess_flag():
    row = {"vin": VIN, "dealer_url": "https://www.dealer.test", "_source_url": REAL}
    mark_guessed(row, "dealer_on")
    apply_vehicle_source_url(row)
    assert GUESSED_KEY not in row and "_source_url_guess_platform" not in row


def test_constructed_url_is_still_marked_guessed_without_a_platform():
    row = {"vin": VIN, "dealer_url": "https://www.dealer.test", "condition": "Used"}
    apply_vehicle_source_url(row)
    assert row[GUESSED_KEY] is True and not guessed_url_fetchable(row)


def test_guess_fetchable_only_for_dealer_dot_com():
    a, b, c = {}, {}, {}
    mark_guessed(a, "dealer_dot_com")
    mark_guessed(b, "dealer_on")
    mark_guessed(c)
    assert guessed_url_fetchable(a) and not guessed_url_fetchable(b) and not guessed_url_fetchable(c)
    assert guessed_url_fetchable({})


def test_prefetch_does_not_fetch_guessed_urls_off_dealer_dot_com(monkeypatch):
    fetched: list[str] = []
    monkeypatch.setattr(pf, "_fetch_html", lambda url: fetched.append(url))
    dealer_on = {"vin": VIN, "_detail_url": "https://m.test/used-inventory/vin-X.htm", "price": None}
    mark_guessed(dealer_on, "dealer_on")
    stats = asyncio.run(pf.http_prefetch_missing_fields([dealer_on]))
    assert fetched == [] and stats["guessed_url_skipped"] == 1 and stats["candidates"] == 0


def test_prefetch_fetches_a_guessed_dealer_dot_com_url_and_the_payload_url(monkeypatch):
    fetched: list[str] = []
    monkeypatch.setattr(pf, "_fetch_html", lambda url: fetched.append(url) or None)
    a = {"vin": VIN, "_detail_url": "https://d.test/used-inventory/vin-X.htm", "price": None}
    mark_guessed(a, "dealer_dot_com")
    b = {"vin": "2HGBH41JXMN109186", "_detail_url": "https://g.test/guess.htm", "_source_url": REAL, "price": None}
    mark_guessed(b, "dealer_on")
    asyncio.run(pf.http_prefetch_missing_fields([a, b]))
    assert sorted(fetched) == sorted([a["_detail_url"], REAL])


def test_page_without_the_vin_applies_no_fields(monkeypatch):
    page = '<script type="application/ld+json">{"@type":"Vehicle","offers":{"price":"19995"}}</script>' + "x" * 600
    monkeypatch.setattr(pf, "_fetch_html", lambda url: page)
    v = {"vin": VIN, "_detail_url": "https://d.test/car", "price": None, "exterior_color": ""}
    stats = asyncio.run(pf.http_prefetch_missing_fields([v]))
    assert stats["fetched"] == 0 and stats["vin_not_on_page"] == 1 and stats["fields_filled"] == 0
    assert v["price"] is None and v["exterior_color"] == ""


def test_vin_match_is_case_insensitive():
    assert pf._vin_on_page(VIN, f"<a href='/vin-{VIN.lower()}'>")
    assert not pf._vin_on_page("", "anything")


def test_f5_old_or_high_mileage_unpriced():
    assert pf._f5_old_unpriced({"year": 2008, "mileage": 180000, "price": None, "condition": "Used"})
    assert pf._f5_old_unpriced({"year": 2021, "mileage": 130000, "price": 0})
    assert not pf._f5_old_unpriced({"year": 2008, "price": 5000})
    assert not pf._f5_old_unpriced({"year": 2024, "mileage": 9000, "price": None})
    assert not pf._f5_old_unpriced({"year": 2010, "price": None, "condition": "New"})
