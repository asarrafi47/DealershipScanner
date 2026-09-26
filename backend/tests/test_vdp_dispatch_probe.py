"""Description visits are probed before the full cap is spent, and the pool's
counters survive a phase-cap cancellation (stats_out).

2026-09-22 lab: 120 description visits per dealer produced 0 descriptions at
Freeway Honda; the phase cap then dropped the visit counter entirely.
"""
from __future__ import annotations

import asyncio

import pytest

from backend.scanner.vdp import dispatch


class _Page:
    def __init__(self) -> None:
        self.context = self

    async def new_page(self):
        return _Page()

    async def close(self) -> None:
        return None


def _vehicles(n: int) -> list[dict]:
    out = []
    for i in range(n):
        out.append(
            {
                "vin": f"1HGBH41JXMN10{i:04d}"[:17].ljust(17, "0"),
                "_detail_url": f"https://d.example/car/{i}",
                "price": 20000 + i,
                "exterior_color": "Blue",
                "interior_color": "Black",
                "engine_description": "2.0L I4",
                "transmission": "CVT",
                "drivetrain": "FWD",
                "fuel_type": "Gasoline",
                "body_style": "Sedan",
                "description": "",
                "gallery": [f"https://img.example/{i}/{k}.jpg" for k in range(25)],
            }
        )
    return out


def _run(monkeypatch, *, fills_description: bool, n: int = 40, conc: int = 2):
    visited: list[str] = []

    async def fake_visit(wp, dealer_name, v, u, vin, *a, **k):
        visited.append(vin)
        filled = ["description"] if fills_description else []
        if fills_description:
            v["description"] = "x" * 80
        return {"visited": 1, "enriched": bool(filled), "filled": filled, "gallery_added": 0}

    monkeypatch.setattr(dispatch, "_vdp_visit_one", fake_visit)
    monkeypatch.setattr(dispatch, "_max_vdp_concurrency", lambda: conc)
    monkeypatch.setenv("SCANNER_VDP_EP_MAX", "0")
    monkeypatch.setenv("SCANNER_VDP_PRICE_MAX", "0")
    monkeypatch.setenv("SCANNER_VDP_SPEC_GAP_MAX", "0")
    monkeypatch.setenv("SCANNER_VDP_DESCRIPTION_MAX", "30")
    monkeypatch.setenv("SCANNER_VDP_DESCRIPTION_PROBE", "5")
    monkeypatch.setenv("SCANNER_VDP_DESCRIPTION_MIN_YIELD", "0.3")
    monkeypatch.setenv("SCANNER_VDP_ROTATION", "0")
    stats_out: dict = {}
    stats = asyncio.run(
        dispatch.enrich_vehicles_vdp(
            _Page(), _vehicles(n), "Test Motors", dealer_id="test-motors", stats_out=stats_out
        )
    )
    return stats, stats_out, visited


@pytest.mark.parametrize("conc", [1, 2])
def test_description_probe_stops_when_nothing_fills(monkeypatch, conc):
    stats, stats_out, visited = _run(monkeypatch, fills_description=False, conc=conc)
    assert len(visited) == 5, "only the probe visits should have run"
    assert stats["description_probe"] == {"probe": 5, "deferred": 25, "filled": 0, "extended": False}
    assert stats["vdps_visited"] == 5


def test_description_probe_extends_on_yield(monkeypatch):
    stats, stats_out, visited = _run(monkeypatch, fills_description=True)
    assert len(visited) == 30
    assert stats["description_probe"]["extended"] is True
    assert stats["description_probe"]["filled"] == 5


def test_stats_out_is_the_live_stats_dict(monkeypatch):
    stats, stats_out, visited = _run(monkeypatch, fills_description=True)
    assert stats is stats_out
    assert stats_out["vdps_visited"] == 30
