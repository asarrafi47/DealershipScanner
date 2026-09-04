"""Sticker-corpus trim diffs: what each rung adds, per photographed documents."""
from __future__ import annotations

from backend.enrichment.trim_ladder import sticker_diffs as sd


def _groups(monkeypatch, groups):
    monkeypatch.setattr(sd, "_groups", lambda: groups)


def _ev(display, items, count=2, car_ids=(1, 2)):
    return {
        "display_trim": display,
        "items": {sd._norm_equip(i): i for i in items},
        "sticker_count": count,
        "car_ids": list(car_ids),
        "median_base_msrp": None,
    }


def test_adds_are_diffs_against_all_lower_rungs(monkeypatch):
    cfg = (2026, "honda", "crvhybrid")
    _groups(monkeypatch, {cfg: {
        "sporttouring": _ev("Sport Touring", ["Heated Seats", "Bose Audio", "Navigation", "Sunroof"]),
        "sportl": _ev("Sport-L", ["Heated Seats", "Sunroof"]),
        "sport": _ev("Sport", ["Heated Seats"]),
    }})
    steps = [{"name": "Sport Touring"}, {"name": "Sport-L"}, {"name": "Sport"}]
    n = sd.attach_sticker_adds(steps, make="Honda", model="CR-V Hybrid", year=2026)
    assert n == 3
    assert set(steps[0]["sticker_adds"]) == {"Bose Audio", "Navigation"}
    assert steps[1]["sticker_adds"] == ["Sunroof"]
    # Base rung gets no adds (nothing below to diff against), but does get
    # a confirmed-equipment annotation from its own window sticker.
    assert "sticker_adds" not in steps[2]
    assert steps[2]["sticker_equipment"]
    assert "window sticker" in steps[0]["sticker_adds_note"]


def test_never_overrides_brochure_adds(monkeypatch):
    cfg = (2026, "honda", "civic")
    _groups(monkeypatch, {cfg: {
        "sport": _ev("Sport", ["Alloy Wheels"]),
        "lx": _ev("LX", []),
    }})
    steps = [{"name": "Sport", "adds": ["Brochure-cited item"]}, {"name": "LX"}]
    sd.attach_sticker_adds(steps, make="Honda", model="Civic", year=2026)
    assert "sticker_adds" not in steps[0]  # photographed page beats a diff


def test_depluralized_diff_key():
    assert sd._norm_equip("Heated Front Seats") == sd._norm_equip("heated front seat")
    assert sd._norm_equip("18-in. Alloy Wheels") == sd._norm_equip("18 in alloy wheel")


def test_no_evidence_no_annotation(monkeypatch):
    _groups(monkeypatch, {})
    steps = [{"name": "XSE"}, {"name": "LE"}]
    assert sd.attach_sticker_adds(steps, make="Toyota", model="Camry", year=2026) == 0
    assert "sticker_adds" not in steps[0]
