"""B-029: _resolve must compute aux_key with the pending flag, like the writer."""

from __future__ import annotations

from types import SimpleNamespace

from backend.db.repositories import grid_cards_repo as repo


def test_pending_car_matches_stored_aux_key(monkeypatch):
    ctx = repo._Ctx.__new__(repo._Ctx)
    ctx.include_incomplete = True
    ctx.snapshot = SimpleNamespace(
        per_row_fallback=False, ids=frozenset(), pending_ids=frozenset({5})
    )
    ctx.attribution = {}
    ctx.attribution_ok = True
    ctx.market_gen = 1
    ctx.today = "2026-10-26"
    stored = repo._aux_key(ctx, False, None, True)
    cand = {"id": 5, "ver": "v1", "row_ver": "v1", "card_json": "{}", "aux_key": stored,
            "built_day": "2026-10-26"}

    def boom(ids):
        raise AssertionError("pending car with matching stored aux_key was rebuilt")

    monkeypatch.setattr(repo, "_fetch_full_rows", boom)
    res = repo._resolve([(cand, None)], ctx)
    assert cand["_aux"] == stored
    assert res is not None
