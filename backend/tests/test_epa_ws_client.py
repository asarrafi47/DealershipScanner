"""epa_ws_client parses fueleconomy.gov XML; HTTP is stubbed, no network (P17B.10)."""
import urllib.error

import pytest

from backend.enrichment import epa_ws_client as c

MENU = (
    "<menuItems><menuItem><text>X5 xDrive40i</text><value>X5 xDrive40i</value></menuItem>"
    "<menuItem><text></text><value>skip</value></menuItem></menuItems>"
)
VEH = "<vehicle><id>123</id><make>BMW</make><cylinders>6</cylinders></vehicle>"


def test_parse_menu_items_skips_blank_text():
    assert c.parse_menu_items(MENU) == [("X5 xDrive40i", "X5 xDrive40i")]


def test_menu_years_sorted_unique(monkeypatch):
    xml = "<menuItems>" + "".join(
        f"<menuItem><text>{y}</text><value>{y}</value></menuItem>" for y in (2020, 2019, 2020, "x")
    ) + "</menuItems>"
    monkeypatch.setattr(c, "_fetch", lambda url, **kw: xml)
    assert c.menu_years() == [2019, 2020]


def test_menu_models_builds_query(monkeypatch):
    seen = []
    monkeypatch.setattr(c, "_fetch", lambda url, **kw: seen.append(url) or MENU)
    assert c.menu_models(2019, "BMW") == [("X5 xDrive40i", "X5 xDrive40i")]
    assert seen == [f"{c.BASE}/menu/model?year=2019&make=BMW"]


def test_vehicle_record_flattens_children(monkeypatch):
    monkeypatch.setattr(c, "_fetch", lambda url, **kw: VEH)
    assert c.vehicle_record(123) == {"id": "123", "make": "BMW", "cylinders": "6"}


def test_fetch_with_retries_retries_then_raises(monkeypatch):
    monkeypatch.setattr(c.time, "sleep", lambda s: None)
    calls = []

    def flaky():
        calls.append(1)
        if len(calls) < 3:
            raise urllib.error.URLError("boom")
        return "ok"

    assert c.fetch_with_retries(flaky) == "ok"

    def dead():
        raise TimeoutError()

    with pytest.raises(TimeoutError):
        c.fetch_with_retries(dead, retries=2)
