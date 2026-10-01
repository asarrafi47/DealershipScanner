"""Delta scan must settle rooftop attribution the way the full scan does (audit B3).

Two fixes landed in ``phases/dealer_run.run_dealer`` and not in
``delta_scan.delta_scan_dealer``:

1. the store place comes from ``dealer_place.roster_place_with_hints`` so the
   street learned from the dealer page reaches the gate;
2. rows a store-scoped recipe returned (``_feed_scoped``) skip the union
   re-gate, so they are never handed to ``_disown_foreign_rooftop_vins``
   (Tutton CDJR kept 5 of 346 cars on 2026-09-26 without it).
"""
from __future__ import annotations

import asyncio

import backend.scanner.delta_scan as ds


def _dealer():
    return {"dealer_id": "tutton-com", "url": "https://tutton.example", "name": "Tutton", "provider": "carscommerce"}


def _vin(i: int) -> str:
    return f"1C4RJFAG0MC{i:06d}"


def _patch(monkeypatch, *, rows, gate, upserts, disowned, parse_calls=None):
    async def _fake_fetch(dealer_id, provider, base_url, dealer_name, **kw):
        return ([("https://tutton.example/api", {"x": 1})], len(rows))

    def _parse(*a, **k):
        if parse_calls is not None:
            parse_calls.append(k)
        return [dict(r) for r in rows]

    monkeypatch.setattr("backend.scanner.recipes.try_fetch_via_recipes", _fake_fetch)
    monkeypatch.setattr("backend.parsers.parse", _parse)
    monkeypatch.setattr("backend.parsers.resolve_rooftop_attribution", gate)
    monkeypatch.setattr(ds, "_active_count", lambda dealer_id: len(rows))
    monkeypatch.setattr(
        ds, "_disown_foreign_rooftop_vins",
        lambda dealer_id, vins: disowned.append(set(vins)) or len(vins),
    )
    monkeypatch.setattr(
        "backend.scanner.inventory_reconcile.reconcile_dealer_inventory_after_scan",
        lambda *a, **k: {"ran": False},
    )
    monkeypatch.setenv("SCANNER_VDP_DB_MERGE", "0")
    monkeypatch.setenv("SCANNER_POST_LISTING_GAP_FILL", "0")

    class _FakeCoordinator:
        async def upsert_vehicles(self, vehicles, stats=None):
            upserts.append(list(vehicles))
            return len(vehicles)

    monkeypatch.setattr("backend.scanner.inventory_write.InventoryWriteCoordinator", _FakeCoordinator)


def _refuse_all_gate(seen):
    """A union gate that calls every row a sibling's (the Tutton failure)."""
    def _gate(vehicles, **kw):
        seen.append({"vins": [v["vin"] for v in vehicles], "kw": kw})
        refused = [dict(v, _rooftop_reject="sibling_rooftop") for v in vehicles]
        return [], refused
    return _gate


def test_feed_scoped_rows_skip_the_union_gate_and_are_not_disowned(monkeypatch):
    scoped = [{"vin": _vin(i), "price": 30000 + i, "_feed_scoped": True} for i in range(10)]
    open_rows = [{"vin": _vin(100 + i), "price": 40000 + i} for i in range(2)]
    seen, upserts, disowned = [], [], []
    _patch(monkeypatch, rows=scoped + open_rows, gate=_refuse_all_gate(seen),
           upserts=upserts, disowned=disowned)

    out = asyncio.run(ds.delta_scan_dealer(_dealer()))

    # Only the unscoped rows went through the union gate.
    assert len(seen) == 1
    assert sorted(seen[0]["vins"]) == sorted(r["vin"] for r in open_rows)
    # The store-scoped rows were kept and written; only the open rows were disowned.
    assert out["upserted"] == len(scoped)
    assert {v["vin"] for v in upserts[0]} == {r["vin"] for r in scoped}
    assert disowned == [{r["vin"] for r in open_rows}]


def test_all_feed_scoped_rows_never_reach_the_gate(monkeypatch):
    scoped = [{"vin": _vin(i), "price": 30000 + i, "_feed_scoped": True} for i in range(8)]
    seen, upserts, disowned = [], [], []
    _patch(monkeypatch, rows=scoped, gate=_refuse_all_gate(seen), upserts=upserts, disowned=disowned)

    out = asyncio.run(ds.delta_scan_dealer(_dealer()))

    assert seen == []
    assert disowned == []
    assert out["upserted"] == len(scoped)
    assert "rooftop_disowned" not in out


def test_delta_uses_the_page_learned_street(monkeypatch):
    """Roster has the town but no street; the street learned from the dealer
    page (scan hints) must reach both the per-page parse and the union gate."""
    monkeypatch.setattr(
        "backend.scanner.rooftop_disown.roster_place",
        lambda url: {"dealer_city": "Blairsville", "dealer_state": "GA"},
    )
    monkeypatch.setattr(
        "backend.scanner.dealer_place.place_from_hints",
        lambda dealer_id: {"dealer_address": "1050 Highway 515 South", "dealer_city": "Blairsville",
                           "dealer_state": "GA"} if dealer_id == "tutton-com" else {},
    )
    rows = [{"vin": _vin(i), "price": 30000 + i} for i in range(5)]
    gate_calls, parse_calls, upserts, disowned = [], [], [], []

    def _keep_all(vehicles, **kw):
        gate_calls.append(kw)
        return vehicles, []

    _patch(monkeypatch, rows=rows, gate=_keep_all, upserts=upserts, disowned=disowned,
           parse_calls=parse_calls)

    out = asyncio.run(ds.delta_scan_dealer(_dealer()))

    assert out["upserted"] == len(rows)
    for kw in parse_calls + gate_calls:
        assert kw["dealer_address"] == "1050 Highway 515 South"
        assert kw["dealer_address_source"] == "site_jsonld"
        assert kw["dealer_city"] == "Blairsville"
    assert parse_calls and gate_calls


def test_delta_and_full_scan_share_one_place_function():
    from backend.scanner.dealer_place import roster_place_with_hints

    assert ds._roster_place is roster_place_with_hints
