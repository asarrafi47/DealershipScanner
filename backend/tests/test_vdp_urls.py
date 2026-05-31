"""VDP URL resolution for scanner rows and car detail links."""

from __future__ import annotations

from backend.parsers.vdp_urls import (
    apply_vehicle_source_url,
    is_dealer_site_homepage,
    resolve_vehicle_source_url,
)
from backend.utils.car_serialize import serialize_car_for_api


def test_is_dealer_site_homepage() -> None:
    base = "https://www.hendrickhonda.com"
    assert is_dealer_site_homepage("https://www.hendrickhonda.com/", base) is True
    assert is_dealer_site_homepage("https://www.hendrickhonda.com", base) is True
    assert (
        is_dealer_site_homepage(
            "https://www.hendrickhonda.com/inventory/used-2022-chevrolet-silverado-1500-4wd-pickup-3gcpdkek7ng506582/",
            base,
        )
        is False
    )


def test_resolve_from_detail_url_when_source_missing() -> None:
    row = {
        "vin": "3GCPDKEK7NG506582",
        "dealer_url": "https://www.hendrickhonda.com",
        "_detail_url": "https://www.hendrickhonda.com/inventory/used-2022-chevrolet-silverado-1500-4wd-pickup-3gcpdkek7ng506582/",
    }
    u = resolve_vehicle_source_url(row)
    assert u is not None
    assert "/inventory/" in u
    assert "3gcpdkek7ng506582" in u.lower()


def test_resolve_suggests_dealer_com_style_vdp() -> None:
    row = {
        "vin": "3GCPDKEK7NG506582",
        "dealer_url": "https://www.hendrickhonda.com",
        "condition": "Used",
    }
    u = resolve_vehicle_source_url(row)
    assert u is not None
    assert u.startswith("https://www.hendrickhonda.com/used-inventory/vin-3GCPDKEK7NG506582.htm")


def test_apply_vehicle_source_url_mutates_row() -> None:
    row = {
        "vin": "3GCPDKEK7NG506582",
        "dealer_url": "https://www.hendrickhonda.com",
        "_detail_url": "https://www.hendrickhonda.com/inventory/used-2022-chevrolet-silverado-1500-4wd-pickup-3gcpdkek7ng506582/",
    }
    apply_vehicle_source_url(row)
    assert row["source_url"] == row["_detail_url"]


def test_serialize_car_exposes_listing_vdp_url_for_stale_rows() -> None:
    row = {
        "vin": "3GCPDKEK7NG506582",
        "year": 2022,
        "make": "Chevrolet",
        "model": "Silverado 1500",
        "dealer_url": "https://www.hendrickhonda.com",
    }
    out = serialize_car_for_api(row, include_verified=False)
    assert out.get("listing_vdp_url")
    assert "/used-inventory/vin-" in out["listing_vdp_url"]
