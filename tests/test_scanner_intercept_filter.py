"""Pure helpers for ``scanner.py`` inventory intercept gating and totalCount selection."""

from __future__ import annotations

import pytest

from backend.scrapers.scanner_intercept_filter import (
    intercept_url_allowed,
    pick_total_count_from_intercepts,
)


def _vin_list(n: int = 5) -> list[dict]:
    # 17-char VIN-shaped strings (not necessarily check-digit valid).
    return [{"vin": ("1HGCM82633%07d" % i)[:17]} for i in range(n)]


def _listing_body(*, total: int | None, vehicles: list[dict]) -> dict:
    return {"pageInfo": {"totalCount": total}, "inventory": vehicles}


@pytest.mark.parametrize(
    ("resp_url", "dealer_url", "expect"),
    [
        (
            "https://shop.bmwdealer.com/ws-inv-data/getInventory",
            "https://bmwdealer.com",
            True,
        ),
        (
            "https://shop.bmwdealer.com/api/getInventoryAndFacets",
            "https://bmwdealer.com",
            True,
        ),
        (
            "https://shop.bmwdealer.com/foo",
            "https://shop.bmwdealer.com",
            True,
        ),
        (
            "https://dealer.algolia.net/1/indexes/*/queries",
            "https://shop.bmwdealer.com",
            True,
        ),
    ],
)
def test_intercept_url_allowed_dealer_com_style(resp_url: str, dealer_url: str, expect: bool) -> None:
    assert intercept_url_allowed(resp_url, dealer_url) is expect


def test_intercept_url_denied_carnow(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SCANNER_INTERCEPT_URL_DENY", "carnow.com,payments")
    dealer = "https://legitbmw.com"
    assert not intercept_url_allowed("https://payments.carnow.com/api/v2/session", dealer)
    assert intercept_url_allowed("https://legitbmw.com/ws-inv-data/getInventory", dealer)


def test_intercept_url_explicit_allow_cross_host(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SCANNER_INTERCEPT_URL_ALLOW", "my-cdn-inventory")
    dealer = "https://dealer.com"
    assert intercept_url_allowed("https://edge.vendor.com/my-cdn-inventory/x", dealer)


def test_pick_total_count_prefers_payload_with_total_and_more_rows() -> None:
    dealer = "https://dealer.example.com"
    small = _listing_body(total=12, vehicles=_vin_list(3))
    big = _listing_body(total=240, vehicles=_vin_list(20))
    records = [
        (f"{dealer}/noise.json", small),
        (f"{dealer}/ws-inv-data/getInventory", big),
    ]
    assert pick_total_count_from_intercepts(records, dealer) == 240


def test_pick_total_count_falls_back_to_tail_when_no_total() -> None:
    dealer = "https://dealer.example.com"
    vehicles = _vin_list(4)
    records = [
        (f"{dealer}/a", {"inventory": vehicles}),
        (f"{dealer}/b", {"totalCount": 99, "inventory": vehicles}),
    ]
    assert pick_total_count_from_intercepts(records, dealer) == 99


def test_pick_total_count_ignores_denied_hosts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SCANNER_INTERCEPT_URL_DENY", "payments,carnow")
    dealer = "https://dealer.example.com"
    vehicles = _vin_list(5)
    bad = {"totalCount": 9999, "inventory": vehicles}
    records = [
        ("https://payments.carnow.com/api/session", bad),
    ]
    assert pick_total_count_from_intercepts(records, dealer) is None
