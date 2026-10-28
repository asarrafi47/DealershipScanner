"""P17B.7 (D-DC9 c): requires_browser is informational; delta takes the standard no-recipe path."""
from __future__ import annotations

import asyncio

import backend.scanner.delta_scan as ds


def _dealer():
    return {"dealer_id": "d-com", "url": "https://d.example", "name": "D", "provider": "dealer_dot_com"}


def _run(monkeypatch, hints):
    notes = []

    async def _none(*a, **k):
        return None

    monkeypatch.setattr("backend.scanner.recipes.try_fetch_via_recipes", _none)
    monkeypatch.setattr("backend.scanner.recipe_store.get_scan_hints", lambda dealer_id: dict(hints))
    monkeypatch.setattr(ds, "_write_hint_note", lambda dealer_id, note: notes.append(note))
    return asyncio.run(ds.delta_scan_dealer(_dealer())), notes


def test_requires_browser_gets_the_standard_no_recipe_handling(monkeypatch):
    out, notes = _run(monkeypatch, {"requires_browser": True})
    assert out["skipped"] == "no_recipe_yield"
    assert len(notes) == 1 and "browser" not in notes[0]


def test_no_recipe_note_is_the_same_with_and_without_the_hint(monkeypatch):
    _, with_hint = _run(monkeypatch, {"requires_browser": True})
    _, without = _run(monkeypatch, {})
    assert with_hint == without


def test_needs_http_proxy_and_skip_reason_still_route(monkeypatch):
    out, notes = _run(monkeypatch, {"needs_http_proxy": True})
    assert "needs_http_proxy" in out["skipped"] and notes == []
    out, _ = _run(monkeypatch, {"skip_reason": "not a car dealer"})
    assert out["skipped"].startswith("hinted_skip")
