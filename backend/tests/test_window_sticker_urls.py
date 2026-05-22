"""OEM window sticker URL tiers by make (WMI)."""

from __future__ import annotations

import pytest

from backend.scanner.window_sticker import (
    get_window_sticker_candidate_urls,
    get_window_sticker_url,
    window_sticker_oem_family,
)


@pytest.mark.parametrize(
    "vin,host_fragment",
    [
        ("1C4RJHBG9SC340097", "jeep.com/hostd/windowsticker"),
        ("1C6RR7PT0NS123456", "ramtrucks.com/hostd/windowsticker"),
        ("2C3CDXHG9LH123456", "dodge.com/hostd/windowsticker"),
        ("2C4RC1BG0LR123456", "chrysler.com/hostd/windowsticker"),
    ],
)
def test_stellantis_urls(vin: str, host_fragment: str) -> None:
    url = get_window_sticker_url(vin)
    assert url is not None
    assert host_fragment in url
    assert vin in url


def test_ford_url() -> None:
    # Sample Ford WMI
    vin = "1FTEW1EP5MFA12345"
    url = get_window_sticker_url(vin)
    assert url is not None
    assert "windowsticker.forddirect.com" in url


def test_lincoln_tries_ford_then_lincoln() -> None:
    vin = "5LM5J7XC6LGL12345"
    urls = [u for u, _t in get_window_sticker_candidate_urls(vin)]
    assert any("forddirect.com" in u for u in urls)
    assert any("lincolnvehicles.com" in u for u in urls)


def test_gm_off_by_default() -> None:
    vin = "1G1FY6D70N5123456"
    assert get_window_sticker_url(vin) is None
    assert window_sticker_oem_family(vin) is None


def test_gm_experimental_when_enabled(monkeypatch) -> None:
    monkeypatch.setenv("WINDOW_STICKER_GM_EXPERIMENTAL", "1")
    vin = "1G1FY6D70N5123456"
    urls = [u for u, _t in get_window_sticker_candidate_urls(vin)]
    assert any("cws.gm.com" in u for u in urls)
    assert window_sticker_oem_family(vin) == "gm-experimental"


def test_bmw_not_supported() -> None:
    vin = "WBA8E9G50LNT12345"
    assert get_window_sticker_url(vin) is None


def test_fetch_blocks_url_not_in_oem_candidates(monkeypatch) -> None:
    from backend.scanner import window_sticker as ws

    called: list[str] = []

    def _fake_get(*_a, **_k):
        called.append("get")
        raise AssertionError("requests.get should not run for disallowed URL")

    monkeypatch.setattr("requests.get", _fake_get)
    vin = "1C4RJHBG9SC340097"
    out = ws._fetch_url_as_sticker(
        "http://127.0.0.1/internal.pdf",
        vin,
        timeout=5.0,
        tier="reliable_pdf",
    )
    assert out is None
    assert called == []
