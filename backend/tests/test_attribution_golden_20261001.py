"""Golden parity for the move of rooftop attribution into ``backend/attribution/``.

``fixtures/attribution_golden_20261001.json`` was recorded on b1578241d, BEFORE the
move, by running the code as it stood then:

* ``corpus``: every rooftop-corpus scenario (42) x {legacy, scorer} through
  ``backend.parsers.parse`` (or the gate, for platforms without a parser shape) —
  the exact kept / refused VIN sets, each refusal's reason and each row's tier;
* ``flows``: per fixture, each scenario as a one-page scan with its own place,
  plus every scenario of the fixture as the pages of ONE scan with the roster
  place, through the two all-rows passes as they were written then —
  ``dealer_run`` (phases/dealer_run.py: union over per-page kept + refused, store-
  scoped rows exempt) and ``delta_old`` (delta_scan.py: union over per-page KEPT
  rows only, per-page refusals appended).

After the move ``dealer_run`` and the delta scan share ``backend.attribution.decide``.
The parse-time answers and the full-scan answers must be identical; the delta
scan now answers exactly what the full scan answers. Where that differs from
``delta_old`` the change is listed in DELTA_CHANGES below (kept, evidence-backed
refusals and the un-list set are identical in every case; only refusal LABELS of
rows that stay refused, and the "unidentified" count, change).
"""
from __future__ import annotations

import asyncio
import json
import os

import pytest

import backend.tests.test_rooftop_corpus as C
from backend.attribution import DealerCtx, decide, parse_page, split_refusals

GOLDEN = json.load(open(os.path.join(os.path.dirname(__file__), "fixtures", "attribution_golden_20261001.json")))

# Every flow where the pre-move delta answer differed from the full scan's. Each is
# the multi-page scan of one fixture; in each the per-page gate refused rows the
# union pass (which sees all pages) labels differently or keeps.
DELTA_CHANGES = {
    f"{mode}|{dealer}|__all_pages__"
    for mode in ("legacy", "scorer")
    for dealer in ("group1toyotanorthaustin-com", "hendrickhonda-com", "shottenkirkchrysler-com")
}


def _vinset(rows):
    return sorted({r["vin"] for r in rows})


def _reasons(rows):
    return {r["vin"]: r.get("_rooftop_reject") for r in rows}


def _dealer_url(fx):
    return "https://www." + fx["dealer_id"].replace("-com", ".com").replace("-net", ".net")


def _setup(monkeypatch, fx, sc, mode):
    monkeypatch.setenv("SCANNER_ROOFTOP_SCORER", "0" if mode == "legacy" else "1")
    aliases = tuple(sc.get("aliases") if (sc is not None and "aliases" in sc) else fx["roster"].get("aliases") or ())
    monkeypatch.setattr(C.parsers_mod, "roster_name_aliases", lambda d: aliases)
    monkeypatch.setattr("backend.scanner.rooftop_ledger.note_refusal", lambda *a, **k: None, raising=False)
    monkeypatch.setattr("backend.scanner.rooftop_ledger.flush", lambda *a, **k: None, raising=False)


def _page(fx, sc, dealer_url):
    if fx["platform"] == "carscommerce":
        p = {"data": {"listings": [C._cc_listing(r["vin"], r["type"], r["stamp"], fx["feed"]["account"]) for r in sc["rows"]]},
             "meta": {"pagination": {"total": len(sc["rows"])}}}
        if sc["gate"] == "feed_scoped":
            p["_feed_scoped"] = True
        return "carscommerce", p
    if fx["platform"] == "typesense":
        return "typesense", {"results": [{"found": len(sc["rows"]), "hits": [C._ts_doc(r["vin"], r["type"], r["stamp"], dealer_url) for r in sc["rows"]]}]}
    return None, None


def _runs(fx):
    runs = [(sc["name"], [sc], C._place_kwargs(fx, sc), sc) for sc in fx["scenarios"]]
    runs.append(("__all_pages__", list(fx["scenarios"]), C._place_kwargs(fx, {"place": None}), None))
    return runs


def _flow_params():
    out = []
    for mode in C.MODES:
        for fx in C.CORPUS:
            for label, scs, place, sc0 in _runs(fx):
                key = f"{mode}|{fx['dealer_id']}|{label}"
                if key in GOLDEN["flows"]:
                    out.append(pytest.param(mode, fx, scs, place, sc0, key, id=key))
    return out


def _summary(kept, refused):
    ev, unid = split_refusals(refused)
    return {"kept": _vinset(kept), "refused": _reasons(refused), "evidenced": _vinset(ev),
            "disown": sorted({v["vin"] for v in ev} - {v["vin"] for v in kept}), "unidentified": unid}


# ── A. parse-time gate: identical per scenario and mode ─────────────────────

def test_golden_covers_the_whole_corpus():
    assert len(GOLDEN["corpus"]) == 2 * len(C.SCENARIOS) == 84
    assert len(GOLDEN["flows"]) == 108


@pytest.mark.parametrize("mode", C.MODES)
@pytest.mark.parametrize("fx,sc", C.SCENARIOS, ids=C.IDS)
def test_parse_time_gate_matches_pre_move_golden(mode, fx, sc, monkeypatch):
    by_vin, kept, rejected = C.run_scenario(fx, sc, monkeypatch, mode)
    want = GOLDEN["corpus"][f"{mode}|{fx['dealer_id']}|{sc['name']}"]
    assert _vinset(kept) == want["kept"]
    assert _reasons(rejected) == want["refused"]
    assert {v: r.get("_rooftop_tier") for v, r in sorted(by_vin.items())} == want["tiers"]


# ── B. the all-rows pass: full scan identical, delta == full scan ───────────

@pytest.mark.parametrize("mode,fx,scs,place,sc0,key", _flow_params())
def test_all_rows_pass_matches_pre_move_full_scan(mode, fx, scs, place, sc0, key, monkeypatch):
    _setup(monkeypatch, fx, sc0, mode)
    url = _dealer_url(fx)
    ctx = DealerCtx(fx["dealer_id"], fx["roster"]["name"], url, dict(place))
    page_kept: list[dict] = []
    page_refused: list[dict] = []
    for sc in scs:
        provider, body = _page(fx, sc, url)
        page_kept.extend(parse_page(provider, body, ctx, page_refused))
    decision = decide(page_kept + page_refused, ctx)
    got = _summary(decision.kept, decision.refused)
    golden = GOLDEN["flows"][key]
    assert got == golden["dealer_run"]
    if key in DELTA_CHANGES:
        old = golden["delta_old"]
        # what the delta scan answered before: same cars kept, same cars un-listed
        assert old["kept"] == got["kept"] and old["evidenced"] == got["evidenced"] and old["disown"] == got["disown"]
        assert old["refused"] != got["refused"]
    else:
        assert golden["delta_old"] == got


def test_delta_changes_list_is_exact():
    differing = {k for k, v in GOLDEN["flows"].items() if v["dealer_run"] != v["delta_old"]}
    assert differing == DELTA_CHANGES


# ── C. delta_scan_dealer end to end over the multi-page fixtures ────────────

@pytest.mark.parametrize("mode", C.MODES)
@pytest.mark.parametrize("fx", [fx for fx in C.CORPUS if fx["platform"] in ("carscommerce", "typesense")],
                         ids=lambda fx: fx["dealer_id"])
def test_delta_scan_dealer_settles_like_the_full_scan(mode, fx, monkeypatch):
    import backend.scanner.delta_scan as ds

    _setup(monkeypatch, fx, None, mode)
    url = _dealer_url(fx)
    place = C._place_kwargs(fx, {"place": None})
    pages = []
    provider = None
    for sc in fx["scenarios"]:
        provider, body = _page(fx, sc, url)
        pages.append((url + "/api", body))

    async def _fetch(dealer_id, prov, base_url, dealer_name, **kw):
        return list(pages), 0

    decisions, disowned = [], []
    real_decide = ds.decide
    monkeypatch.setattr(ds, "decide", lambda rows, ctx: decisions.append((real_decide(rows, ctx), ctx)) or decisions[-1][0])
    monkeypatch.setattr("backend.scanner.recipes.try_fetch_via_recipes", _fetch)
    monkeypatch.setattr("backend.scanner.recipe_store.get_scan_hints", lambda did: {})
    monkeypatch.setattr("backend.scanner.rooftop_disown.roster_place", lambda u: dict(place))
    monkeypatch.setattr(ds, "_write_hint_note", lambda *a, **k: None)
    monkeypatch.setattr(ds, "_active_count", lambda dealer_id: 0)
    monkeypatch.setattr(ds, "_disown_foreign_rooftop_vins", lambda dealer_id, vins: disowned.append(set(vins)) or len(vins))
    monkeypatch.setattr("backend.scanner.inventory_reconcile.reconcile_dealer_inventory_after_scan", lambda *a, **k: {"ran": False})
    monkeypatch.setattr("backend.parsers.team_velocity.is_team_velocity_dealer", lambda d: False)
    monkeypatch.setenv("SCANNER_VDP_DB_MERGE", "0")
    monkeypatch.setenv("SCANNER_POST_LISTING_GAP_FILL", "0")

    class _FakeCoordinator:
        async def upsert_vehicles(self, vehicles, stats=None):
            return len(vehicles)

    monkeypatch.setattr("backend.scanner.inventory_write.InventoryWriteCoordinator", _FakeCoordinator)

    out = asyncio.run(ds.delta_scan_dealer({"dealer_id": fx["dealer_id"], "url": url, "name": fx["roster"]["name"], "provider": provider}))

    assert len(decisions) == 1, "exactly one all-rows pass"
    decision, ctx = decisions[0]
    assert ctx.place == place  # the one place lookup (registry stub, no hints)
    want = GOLDEN["flows"][f"{mode}|{fx['dealer_id']}|__all_pages__"]["dealer_run"]
    assert _summary(decision.kept, decision.refused) == want
    assert (disowned[0] if disowned else set()) == set(want["disown"])
    assert out.get("rooftop_refused_rows", 0) == len(decision.refused)  # rows (a VIN can repeat across pages)


# ── D. one place lookup; old import paths are the same objects ──────────────

@pytest.mark.parametrize("registry,hints,want", [
    # roster with a street: unchanged
    ({"dealer_address": "1 Main St", "dealer_city": "Rome", "dealer_state": "GA", "dealer_zip": "30161", "dealer_address_source": ""},
     {"dealer_address": "9 Other Rd"},
     {"dealer_address": "1 Main St", "dealer_city": "Rome", "dealer_state": "GA", "dealer_zip": "30161", "dealer_address_source": ""}),
    # roster town without a street: the page-learned street completes it
    ({"dealer_address": "", "dealer_city": "Charlotte", "dealer_state": "NC", "dealer_zip": "28273", "dealer_address_source": ""},
     {"dealer_address": "8901 South Blvd", "dealer_city": "X", "place_source": "jsonld"},
     {"dealer_address": "8901 South Blvd", "dealer_city": "Charlotte", "dealer_state": "NC", "dealer_zip": "28273", "dealer_address_source": "site_jsonld"}),
    # not in the registry: the hinted place alone (gate keys only)
    ({}, {"dealer_address": "1050 Highway 515 South", "dealer_city": "Jasper", "dealer_state": "GA", "place_source": "title"},
     {"dealer_address": "1050 Highway 515 South", "dealer_city": "Jasper", "dealer_state": "GA"}),
    # nothing anywhere
    ({}, {}, {}),
])
def test_store_place_is_the_old_roster_place_with_hints(registry, hints, want, monkeypatch):
    from backend.attribution import store_place

    monkeypatch.setattr("backend.scanner.rooftop_disown.roster_place", lambda u: dict(registry))
    monkeypatch.setattr("backend.scanner.dealer_place.place_from_hints", lambda d: dict(hints))
    assert store_place("https://x.example", "x-com") == want
    assert DealerCtx.for_store("x-com", "X", "https://x.example").place == want


def test_old_import_paths_are_the_package_objects():
    import backend.attribution as A
    import backend.attribution.match as match
    import backend.attribution.rooftop as rooftop
    import backend.parsers as parsers
    from backend.scanner import dealer_place, rooftop_disown, rooftop_match

    assert rooftop_match is match
    assert parsers.resolve_rooftop_attribution is rooftop.resolve_rooftop_attribution is A.resolve_rooftop_attribution
    for name in ("_pick_target", "_Rooftop", "_nrm", "_street_key", "_rooftop_slug", "_expand_brand_initials",
                 "_looks_like_address", "_looks_like_store_name", "_merge_department_rooftops", "_scorer_enabled"):
        assert getattr(parsers, name) is getattr(rooftop, name), name
    assert dealer_place.roster_place_with_hints is A.store_place
    assert rooftop_disown.roster_place is A.registry_place
    assert rooftop_disown.split_refusals is A.split_refusals
    assert rooftop_disown.EVIDENCE_BACKED_REJECTS is A.EVIDENCE_BACKED_REJECTS
    assert rooftop_disown.disown_foreign_rooftop_vins is A.disown_foreign_rooftop_vins
