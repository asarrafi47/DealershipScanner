"""Dealer eProcess group sites: scope the SRP walk to this store with lc=<id>
(lexusofknoxville-com filed 1,641 group cars under one store, 2026-09-26)."""
from __future__ import annotations

from backend.scanner import recipe_synth as rs

_KNOX = ('<a href="https://www.lexusofknoxville.com/search/new-lexus-nx-450h+-lexus-of-knoxville/?cy=37922&lc=15578&md=12629&mk=33&tp=new">NX</a>'
         '<a href="/search/pre-owned/?tp=pre_owned&ct=48">Pre-Owned</a>')
_CAPITAL = '<a href="/search/new-toyota-camry/?lc=8243&md=1&tp=new">Camry</a><a href="/search/new-toyota-rav4/?lc=8243&md=2&tp=new">RAV4</a>'
_MULTI = '<a href="/search/new-a/?lc=1&tp=new">a</a><a href="/search/new-b/?lc=2&tp=new">b</a>'


def test_store_lc_from_named_slug_or_lone_value():
    assert rs._dep_store_lc(_KNOX, "lexusofknoxville-com") == "15578"
    assert rs._dep_store_lc(_CAPITAL, "capitaltoyota-com") == "8243"
    assert rs._dep_store_lc(_MULTI, "somestore-com") is None
    assert rs._dep_store_lc("<a href='/search/used/?tp=used'>x</a>", "x-com") is None


def _vehicle_ld(vin: str) -> str:
    return ('<script type="application/ld+json">{"@context":"https://schema.org","@type":"Vehicle","name":"2024 Lexus RX",'
            f'"vehicleIdentificationNumber":"{vin}","brand":{{"@type":"Brand","name":"Lexus"}},"offers":{{"@type":"Offer","price":"41000","priceCurrency":"USD","url":"/auto/x/{vin}/"}}}}</script>')


def test_dep_recipe_walks_the_store_scoped_url(monkeypatch):
    group = "<html>dealereprocess" + _KNOX + '<div data-vehicle_count="1641"></div>' + "".join(_vehicle_ld(f"2T2GGCEZ0TC10{i:04d}") for i in range(12)) + "</html>"
    scoped = "<html>dealereprocess" + _KNOX + '<div data-vehicle_count="212"></div>' + "".join(_vehicle_ld(f"2T2GGCEZ0TC20{i:04d}") for i in range(12)) + "</html>"

    def fake_fetch(url):
        return (scoped if "lc=15578" in url else group), url

    monkeypatch.setattr(rs, "_dep_fetch_page", fake_fetch)
    monkeypatch.setattr(rs, "_dep_page_size", lambda html: None)
    out = rs._dep_srp_recipe("lexusofknoxville-com", "https://www.lexusofknoxville.com", "/search/pre-owned/?tp=pre_owned&ct=48")
    assert out is not None
    recipe, vins = out
    assert "lc=15578" in recipe.url and recipe.total_count == 212 and len(vins) == 12
    assert all(v.startswith("2T2GGCEZ0TC20") for v in vins)
