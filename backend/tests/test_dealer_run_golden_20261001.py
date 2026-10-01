"""Golden for ``backend.scanner.phases.dealer_run.run_dealer`` (pure-refactor guard).

``fixtures/dealer_run_golden_20261001.json`` was recorded on 75671a38f, BEFORE
run_dealer was split into named steps, by driving the function as it stood then
through every branch the scenarios below reach. After the split the recorded
outputs must be identical:

* the returned result dict (timings masked),
* every payload handed to the write coordinator (``upsert_vehicles``) and its
  stats dict,
* the side-effect calls and their arguments: recipe fetch, recovery context,
  ``parse_fn`` output, prefetch, gallery vision, VIN facts, registry resolve /
  link, auto-heal, rooftop disown, reconcile (with a snapshot of the result dict
  it receives), browser context open / close,
* the scan_log JSONL records and every INFO+ line on the ``scanner`` logger.

Inputs: the rooftop corpus (``fixtures/rooftop_corpus``, real feed shapes) and 14
real listing rows read from local Postgres (``fixtures/dealer_run_golden_rows_20261001.json``,
read-only) as a recovery strategy's output.

Re-record (only on purpose): ``DEALER_RUN_GOLDEN_RECORD=1 pytest <this file>``.
"""
from __future__ import annotations

import asyncio
import copy
import json
import logging
import os
import re
import sys
from types import SimpleNamespace
from typing import Any

import pytest

import backend.tests.test_rooftop_corpus as C

HERE = os.path.dirname(__file__)
GOLDEN_PATH = os.path.join(HERE, "fixtures", "dealer_run_golden_20261001.json")
ROWS = json.load(open(os.path.join(HERE, "fixtures", "dealer_run_golden_rows_20261001.json"), encoding="utf-8"))["rows"]
RECORD = os.environ.get("DEALER_RUN_GOLDEN_RECORD") == "1"


def _golden() -> dict:
    try:
        with open(GOLDEN_PATH, encoding="utf-8") as fh:
            return json.load(fh)
    except FileNotFoundError:
        return {}


# ── helpers ──────────────────────────────────────────────────────────────────

def _default(o: Any) -> Any:
    if isinstance(o, (set, frozenset)):
        return sorted(o, key=str)
    return str(o)


def _jsonable(obj: Any) -> Any:
    return json.loads(json.dumps(obj, default=_default, sort_keys=True))


def _patch_bound(monkeypatch, name: str, fn: Any) -> None:
    """Replace ``name`` wherever run_dealer's modules bound it at import time
    (dealer_run itself before the split; its step modules after it)."""
    import backend.scanner.phases.dealer_run  # noqa: F401 - loads its step modules too

    hit = False
    for modname, mod in list(sys.modules.items()):
        if mod is None or not modname.startswith("backend.scanner.phases.dealer_run"):
            continue
        if hasattr(mod, name):
            monkeypatch.setattr(mod, name, fn)
            hit = True
    assert hit, f"{name} is not bound in any dealer_run module"


_T_PATTERNS = [
    (re.compile(r'"seconds":[0-9.eE+-]+'), '"seconds":"<t>"'),
    (re.compile(r'"upsert":[0-9.eE+-]+'), '"upsert":"<t>"'),
    (re.compile(r"[0-9]+\.[0-9]s\)"), "<t>s)"),
]


def _mask(s: str) -> str:
    for pat, rep in _T_PATTERNS:
        s = pat.sub(rep, s)
    return s


def _mask_result(res: dict) -> dict:
    out = _jsonable(res)
    if "seconds" in out:
        out["seconds"] = "<t>"
    if isinstance(out.get("phase_secs"), dict):
        out["phase_secs"] = {k: "<t>" for k in out["phase_secs"]}
    return out


def _cc_page(fx: dict, sc: dict) -> dict:
    p = {"data": {"listings": [C._cc_listing(r["vin"], r["type"], r["stamp"], fx["feed"]["account"]) for r in sc["rows"]]},
         "meta": {"pagination": {"total": len(sc["rows"])}}}
    if sc["gate"] == "feed_scoped":
        p["_feed_scoped"] = True
    return p


def _ts_page(fx: dict, sc: dict, url: str) -> dict:
    return {"results": [{"found": len(sc["rows"]), "hits": [C._ts_doc(r["vin"], r["type"], r["stamp"], url) for r in sc["rows"]]}]}


def _dealer_url(dealer_id: str) -> str:
    return "https://www." + dealer_id.replace("-com", ".com").replace("-net", ".net")


def _fixture(dealer_id: str) -> dict:
    return next(fx for fx in C.CORPUS if fx["dealer_id"] == dealer_id)


# ── the harness ──────────────────────────────────────────────────────────────

class Harness:
    def __init__(self, monkeypatch, caplog):
        self.mp = monkeypatch
        self.caplog = caplog
        self.events: list[list[Any]] = []
        self.upserts: list[dict] = []
        self.scanlog: list[dict] = []

    def ev(self, name: str, payload: Any) -> None:
        self.events.append([name, _jsonable(payload)])

    def base_env(self, mode: str = "scorer") -> None:
        mp = self.mp
        mp.setenv("SCANNER_ROOFTOP_SCORER", "0" if mode == "legacy" else "1")
        mp.setenv("SCANNER_USER_AGENT", "golden-UA/1.0")
        for k in ("SCANNER_ALLOW_BROWSER", "SCANNER_HTTP_ONLY", "SCANNER_GALLERY_VISION_INLINE", "SCANNER_FAST_MODE",
                  "SCANNER_SISTER_STORE_FILTER", "SCANNER_SISTER_STORE_SAFE", "SCANNER_SISTER_STORE_MIN_KEEP",
                  "SCANNER_RECIPE_MIN_PRICE_COVERAGE", "SCANNER_RECIPE_MIN_FIELD_COVERAGE",
                  "SCANNER_INTERCEPT_COVERAGE_RATIO", "SCANNER_RECOVERY_MIN_ROWS"):
            mp.setenv(k, "")
        mp.setenv("SCANNER_INVENTORY_RECOVERY", "0")

    def stubs(self, *, aliases=(), place=None, known=0, recipe=None, recipe_cov=None, hints=None,
              recover=None, prefetch=None, gallery_vision=None, vin_facts=None, registry=None,
              upsert=None, auto_heal=None, link=None, disown=None, reconcile=None) -> None:
        mp, h = self.mp, self
        import backend.parsers as parsers_mod

        mp.setattr(parsers_mod, "roster_name_aliases", lambda d: tuple(aliases))
        mp.setattr("backend.scanner.rooftop_ledger.note_refusal", lambda *a, **k: None, raising=False)
        mp.setattr("backend.scanner.rooftop_ledger.flush", lambda *a, **k: None, raising=False)
        mp.setattr("backend.parsers.team_velocity.is_team_velocity_dealer", lambda d: False, raising=False)
        mp.setattr("backend.attribution.gate.store_place", lambda u, d: (h.ev("store_place", [u, d]), dict(place or {}))[1])

        # recipes
        async def _fetch(dealer_id, provider, base_url, dealer_name, *, union=False, coverage_out=None):
            h.ev("try_fetch_via_recipes", [dealer_id, provider, base_url, dealer_name, union])
            if isinstance(recipe, BaseException):
                raise recipe
            if recipe is None:
                return None
            if coverage_out is not None and recipe_cov:
                coverage_out.update(recipe_cov)
            records = recipe
            return list(records), len({id(b) for _, b in records})

        mp.setattr("backend.scanner.recipes.try_fetch_via_recipes", _fetch)
        mp.setattr("backend.scanner.recipes.last_known_vin_count", lambda d: (h.ev("last_known_vin_count", d), known)[1])

        def _load(d):
            h.ev("load_recipes", d)
            if isinstance(hints, BaseException):
                raise hints
            return [SimpleNamespace(stale=s, provider_hint=p) for s, p in (hints or [])]

        mp.setattr("backend.scanner.recipes.load_recipes", _load)

        # recovery
        import backend.scanner.inventory_recovery as rec_mod

        real_recover = rec_mod.recover_inventory

        async def _recover(ctx):
            h.ev("recover_inventory.ctx", {
                "page": None if ctx.page is None else type(ctx.page).__name__,
                "base_url": ctx.base_url, "dealer_id": ctx.dealer_id, "dealer_name": ctx.dealer_name,
                "dealer_url": ctx.dealer_url, "provider": ctx.provider,
                "intercepts": [u for u, _ in ctx.intercept_records], "path_htmls": ctx.path_htmls,
                "vins": [v.get("vin") for v in ctx.vehicles],
                "dealer_keys": sorted((ctx.dealer or {}).keys()),
            })
            if recover is None:
                out = await real_recover(ctx)
            else:
                out = await recover(ctx, h)
            h.ev("recover_inventory.out", {"vins": [v.get("vin") for v in out.vehicles], "tried": out.strategies_tried,
                                           "winner": out.winning_strategy, "replaced": out.replaced})
            return out

        mp.setattr(rec_mod, "recover_inventory", _recover)

        # enrich
        async def _prefetch(vehicles, dealer_id, name):
            h.ev("prefetch_before_vdp", [[v.get("vin") for v in vehicles], dealer_id, name])
            if prefetch is None:
                return None
            return prefetch(vehicles)

        mp.setattr("backend.scanner.vdp.prefetch.prefetch_before_vdp", _prefetch)

        def _gv(vehicles):
            h.ev("gallery_vision", [v.get("vin") for v in vehicles])
            if gallery_vision is None:
                return {"gallery_vision_unique_dropped": 0, "gallery_vision_unique_before": 0}
            return gallery_vision(vehicles)

        _patch_bound(mp, "apply_gallery_vision_filter_to_vehicles", _gv)

        def _vf(vehicles):
            h.ev("override_vehicles", [v.get("vin") for v in vehicles])
            if vin_facts is None:
                return {"cached": 0, "drivetrain": 0, "fuel_type": 0}
            return vin_facts(vehicles)

        mp.setattr("backend.enrichment.vpic_facts.override_vehicles", _vf)

        def _reg(d):
            h.ev("resolve_car_dealership_registry_id", d)
            if isinstance(registry, BaseException):
                raise registry
            return registry

        mp.setattr("backend.listings.dealer_registry_match.resolve_car_dealership_registry_id", _reg)

        # persist
        class _Coord:
            async def upsert_vehicles(self, vehicles, stats=None):
                h.upserts.append({"vehicles": _jsonable(vehicles), "stats_in": _jsonable(stats)})
                if upsert is not None:
                    return upsert(vehicles, stats)
                return len(vehicles)

        self.coord = _Coord()
        mp.setattr("backend.scanner.scan_log._log_file", "golden.jsonl")
        mp.setattr("backend.scanner.scan_log._scan_ts", "<scan_ts>")

        def _write(record):
            r = _jsonable(record)
            if "scan_ts" in r:
                r["scan_ts"] = "<scan_ts>"
            if "seconds" in r:
                r["seconds"] = "<t>"
            if isinstance(r.get("phase_secs"), dict):
                r["phase_secs"] = {k: "<t>" for k in r["phase_secs"]}
            h.scanlog.append(r)

        mp.setattr("backend.scanner.scan_log._write", _write)

        # after write
        def _heal(dealer_id, name, cov, vehicles):
            h.ev("auto_heal", [dealer_id, name, cov, [v.get("vin") for v in vehicles]])
            if isinstance(auto_heal, BaseException):
                raise auto_heal
            return auto_heal

        mp.setattr("backend.scanner.post_scan.auto_heal.run_auto_heal_for_dealer", _heal)

        def _link(reg_id, url, dealer_id_slug=None):
            h.ev("link_cars_to_dealership_registry", [reg_id, url, dealer_id_slug])
            if isinstance(link, BaseException):
                raise link
            return link

        mp.setattr("backend.db.inventory_db.link_cars_to_dealership_registry", _link)

        def _disown(dealer_id, vins):
            h.ev("disown_foreign_rooftop_vins", [dealer_id, sorted(vins)])
            if isinstance(disown, BaseException):
                raise disown
            return len(vins)

        _patch_bound(mp, "disown_foreign_rooftop_vins", _disown)

        def _reconcile(dealer_id, url, scraped_norm, result, scraped_conditions=None):
            h.ev("reconcile", [dealer_id, url, sorted(scraped_norm), _mask_result(result), scraped_conditions])
            if isinstance(reconcile, BaseException):
                raise reconcile
            return reconcile or {"ran": True, "scraped_candidates": len(scraped_norm), "marked_inactive": 0, "skipped_reason": None}

        mp.setattr("backend.scanner.inventory_reconcile.reconcile_dealer_inventory_after_scan", _reconcile)

    def run(self, dealer: dict, browser: Any = None, **kw) -> dict:
        from backend.scanner.phases.dealer_run import run_dealer

        self.caplog.set_level(logging.INFO, logger="scanner")
        self.caplog.clear()
        error = None
        result = None
        try:
            result = asyncio.run(run_dealer(browser, dealer, self.coord, **kw))
        except BaseException as e:  # noqa: BLE001 - CancelledError is part of the contract
            error = type(e).__name__
        logs = [[r.levelname, _mask(r.getMessage())] for r in self.caplog.records if r.name == "scanner" and r.levelno >= logging.INFO]
        return {
            "result": _mask_result(result) if result is not None else None,
            "raised": error,
            "upserts": self.upserts,
            "events": self.events,
            "scan_log": self.scanlog,
            "logs": logs,
        }


# ── scenarios ────────────────────────────────────────────────────────────────

def _corpus_dealer(fx: dict, provider: str) -> dict:
    return {"dealer_id": fx["dealer_id"], "url": _dealer_url(fx["dealer_id"]), "name": fx["roster"]["name"], "provider": provider}


def _corpus_scenario(dealer_id: str, mode: str):
    def run(h: Harness):
        fx = _fixture(dealer_id)
        h.base_env(mode)
        url = _dealer_url(dealer_id)
        pages = []
        provider = fx["platform"]
        for sc in fx["scenarios"]:
            body = _cc_page(fx, sc) if provider == "carscommerce" else _ts_page(fx, sc, url)
            pages.append((url + "/api/" + sc["name"], body))
        # the same payload object twice: run_dealer parses one body once
        pages.append((url + "/api/dup", pages[0][1]))
        n = sum(len(sc["rows"]) for sc in fx["scenarios"])
        h.stubs(aliases=fx["roster"].get("aliases") or (), place=C._place_kwargs(fx, {"place": None}),
                known=n, recipe=pages, recipe_cov={"price": 1.0, "trim": 1.0, "exterior_color": 0.0, "n": n},
                hints=[(True, "stale_hint"), (False, provider + "_hinted")])
        return h.run(_corpus_dealer(fx, provider))
    return run


def _rows(n: int | None = None) -> list[dict]:
    rows = copy.deepcopy(ROWS[: n or len(ROWS)])
    # what recovery strategies hand back: a repeated VIN with partial fields, a
    # row with no gallery (hero fallback), a row that names another lot
    dup = {"vin": rows[0]["vin"], "price": None, "stock_number": "DUP-" + str(rows[0].get("stock_number") or ""),
           "gallery": ["https://img.example/dup-1.jpg"]}
    rows.append(dup)
    rows[2]["gallery"] = None
    if len(rows) > 5:
        rows[5]["_lot_location"] = "Nashville, TN"
    return rows


def _replacing_recovery(rows_fn, *, tried=("jsonld_listing_html",), replaced=True, parse_body=None):
    async def _rec(ctx, h):
        from backend.scanner.inventory_recovery import RecoveryResult

        if parse_body is not None:
            h.ev("parse_fn", [v.get("vin") for v in ctx.parse_fn(parse_body)])
        rows = rows_fn() if replaced else list(ctx.vehicles)
        return RecoveryResult(vehicles=rows, strategies_tried=list(tried), winning_strategy=tried[0] if replaced else None, replaced=replaced)
    return _rec


def _prefetch_fill(exclude=(), all_exclude=False):
    def _pf(vehicles):
        filled = 0
        ext = 0
        for i, v in enumerate(vehicles):
            if not v.get("price"):
                v["price"] = 41000 + i
                filled += 1
            if i % 3 == 0 and isinstance(v.get("gallery"), list):
                v["gallery"] = list(v["gallery"]) + [f"https://img.example/{v['vin']}/{k}.jpg" for k in range(4)]
                ext += 1
            if all_exclude or i in exclude:
                v["_sister_store_exclude"] = True
        return {"http_first": {"fields_filled": filled, "galleries_extended": ext}, "db_merge": {"rows": len(vehicles)}}
    return _pf


def _gv_drop_last(vehicles):
    dropped = 0
    for v in vehicles:
        g = v.get("gallery")
        if isinstance(g, list) and len(g) > 1:
            v["gallery"] = g[:-1]
            dropped += 1
    return {"gallery_vision_unique_dropped": dropped, "gallery_vision_unique_before": 99}


def _vf_awd(vehicles):
    n = 0
    for v in vehicles:
        if (v.get("vin") or "").startswith("2T2"):
            v["drivetrain"] = "AWD"
            n += 1
    return {"cached": len(vehicles), "drivetrain": n, "fuel_type": 0}


def _upsert_with_conflict(vehicles, stats):
    vin = vehicles[1]["vin"]
    stats["vin_owner_conflict_vins"] = [vin]
    stats["vin_owner_conflict_owners"] = {vin: "some-other-dealer-com"}
    return len(vehicles) - 1


LEXUS = {"dealer_id": "lexusofknoxville-com", "url": "https://www.lexusofknoxville.com/new-inventory/index.htm",
         "name": "Lexus of Knoxville", "provider": "dealer_eprocess", "dealership_registry_id": 4242,
         "city": "Knoxville", "state": "TN"}
LEXUS_PLACE = {"dealer_address": "", "dealer_city": "Knoxville", "dealer_state": "TN", "dealer_zip": "", "dealer_address_source": ""}


def _tutton_page():
    fx = _fixture("tuttoncdjr-com")
    return fx, _cc_page(fx, fx["scenarios"][0])


def s_missing_url(h):
    h.base_env()
    h.stubs()
    return h.run({"dealer_id": "x-com", "name": "No URL"})


def s_no_recipe_hit(h):
    h.base_env()
    h.stubs(recipe=None)
    return h.run({"dealer_id": "empty-com", "url": "https://www.empty.com/", "name": "Empty", "provider": "dealer_dot_com"})


def s_recipe_raises(h):
    h.base_env()
    h.stubs(recipe=RuntimeError("replay boom"))
    return h.run({"dealer_id": "empty-com", "url": "https://www.empty.com", "name": "Empty"})


def s_realrows_full(h):
    h.base_env()
    h.mp.setenv("SCANNER_GALLERY_VISION_INLINE", "1")
    fx, page = _tutton_page()
    h.stubs(place=LEXUS_PLACE, known=500, recipe=[("https://www.lexusofknoxville.com/api/inv", {"vehicles": []})],
            recipe_cov={"price": 0.0, "n": 0}, hints=[(False, "dealer_eprocess")],
            recover=_replacing_recovery(_rows, parse_body=page), prefetch=_prefetch_fill(exclude=(4,)),
            gallery_vision=_gv_drop_last, vin_facts=_vf_awd, upsert=_upsert_with_conflict,
            auto_heal={"healed": 2}, link=5,
            reconcile={"ran": True, "scraped_candidates": 13, "marked_inactive": 1, "skipped_reason": None})
    return h.run(dict(LEXUS), gallery_vision_filter=True, monroney_vision=True)


def s_realrows_vdp_abort_empty(h):
    h.base_env()
    dealer = dict(LEXUS)
    dealer.pop("dealership_registry_id")
    h.stubs(place=LEXUS_PLACE, known=0, recipe=None, recover=_replacing_recovery(_rows),
            prefetch=_prefetch_fill(all_exclude=True), registry=77, link=0)
    return h.run(dealer, gallery_vision_filter=False, monroney_vision=False)


def s_realrows_vdp_exclude_all_unsafe(h):
    h.base_env()
    h.mp.setenv("SCANNER_SISTER_STORE_SAFE", "0")
    dealer = dict(LEXUS)
    dealer.pop("dealership_registry_id")
    h.stubs(place=LEXUS_PLACE, recipe=None, recover=_replacing_recovery(_rows),
            prefetch=_prefetch_fill(all_exclude=True), registry=None)
    return h.run(dealer)


def s_realrows_every_side_pass_fails(h):
    h.base_env()
    h.mp.setenv("SCANNER_GALLERY_VISION_INLINE", "1")
    dealer = dict(LEXUS)
    dealer.pop("dealership_registry_id")

    def _boom(_):
        raise RuntimeError("side pass boom")

    def _pf_boom(_):
        raise RuntimeError("prefetch boom")

    fx, page = _tutton_page()
    sibling = copy.deepcopy(page)
    h.stubs(aliases=fx["roster"].get("aliases") or (), place=C._place_kwargs(fx, {"place": None}), recipe=[("https://t/api", sibling)],
            recover=_replacing_recovery(lambda: _rows(8), tried=("html_next_data", "jsonld_listing_html")),
            prefetch=_pf_boom, gallery_vision=_boom, vin_facts=_boom, registry=RuntimeError("registry boom"),
            auto_heal=RuntimeError("heal boom"), link=RuntimeError("never reached"), disown=RuntimeError("disown boom"),
            reconcile=RuntimeError("reconcile boom"))
    return h.run(dealer)


def s_tutton_feed_with_conflict_and_disown(h):
    # corpus feed, kept rows upserted, sibling refusals disowned, recovery tried but kept the feed
    h.base_env("scorer")
    fx = _fixture("tuttoncdjr-com")
    url = _dealer_url(fx["dealer_id"])
    pages = [(url + "/api/" + sc["name"], _cc_page(fx, sc)) for sc in fx["scenarios"]]
    h.stubs(aliases=fx["roster"].get("aliases") or (), place=C._place_kwargs(fx, {"place": None}), known=3,
            recipe=pages, recipe_cov={"price": 1.0, "trim": 1.0, "n": 15}, hints=RuntimeError("load boom"),
            recover=_replacing_recovery(None, tried=("shopperexpress_api",), replaced=False),
            prefetch=_prefetch_fill(), upsert=_upsert_with_conflict, auto_heal=None)
    d = _corpus_dealer(fx, "carscommerce")
    d["dealership_registry_id"] = "88"
    d["city"], d["state"] = "Jasper", "GA"
    return h.run(d)


def s_upsert_raises(h):
    h.base_env()
    h.stubs(place=LEXUS_PLACE, recipe=None, recover=_replacing_recovery(lambda: _rows(4)),
            upsert=lambda v, s: (_ for _ in ()).throw(RuntimeError("db down")))
    return h.run(dict(LEXUS))


def s_recovery_cancelled(h):
    h.base_env()

    async def _cancel(ctx, h):
        raise asyncio.CancelledError()

    h.stubs(place=LEXUS_PLACE, recipe=None, recover=_cancel)
    return h.run(dict(LEXUS))


def s_browser_allowed(h):
    h.base_env()
    h.mp.setenv("SCANNER_ALLOW_BROWSER", "1")

    class FakePage:
        pass

    class FakeContext:
        async def new_page(self):
            h.ev("context.new_page", None)
            return FakePage()

        async def close(self):
            h.ev("context.close", None)

    class FakeBrowser:
        async def new_context(self, **opts):
            h.ev("browser.new_context", opts)
            return FakeContext()

    h.stubs(place=LEXUS_PLACE, recipe=None, recover=_replacing_recovery(lambda: _rows(3)))
    return h.run(dict(LEXUS), browser=FakeBrowser())


def s_browser_given_but_http_only(h):
    h.base_env()

    class FakeBrowser:
        async def new_context(self, **opts):
            h.ev("browser.new_context", opts)
            raise AssertionError("must not open a context under HTTP-only")

    h.stubs(place=LEXUS_PLACE, recipe=None, recover=_replacing_recovery(lambda: _rows(3)))
    return h.run(dict(LEXUS), browser=FakeBrowser())


SCENARIOS: dict[str, Any] = {
    "missing_url": s_missing_url,
    "no_recipe_hit": s_no_recipe_hit,
    "recipe_raises": s_recipe_raises,
    "realrows_full": s_realrows_full,
    "realrows_vdp_abort_empty": s_realrows_vdp_abort_empty,
    "realrows_vdp_exclude_all_unsafe": s_realrows_vdp_exclude_all_unsafe,
    "realrows_every_side_pass_fails": s_realrows_every_side_pass_fails,
    "tutton_feed_with_conflict_and_disown": s_tutton_feed_with_conflict_and_disown,
    "upsert_raises": s_upsert_raises,
    "recovery_cancelled": s_recovery_cancelled,
    "browser_allowed": s_browser_allowed,
    "browser_given_but_http_only": s_browser_given_but_http_only,
}
for _fx in C.CORPUS:
    if _fx["platform"] in ("carscommerce", "typesense"):
        for _mode in C.MODES:
            SCENARIOS[f"corpus|{_mode}|{_fx['dealer_id']}"] = _corpus_scenario(_fx["dealer_id"], _mode)


@pytest.mark.parametrize("name", sorted(SCENARIOS))
def test_run_dealer_matches_pre_split_golden(name, monkeypatch, caplog):
    got = SCENARIOS[name](Harness(monkeypatch, caplog))
    if RECORD:
        g = _golden()
        g[name] = got
        with open(GOLDEN_PATH, "w", encoding="utf-8") as fh:
            json.dump(dict(sorted(g.items())), fh, indent=1, ensure_ascii=False, sort_keys=True)
            fh.write("\n")
        return
    want = _golden().get(name)
    assert want is not None, f"no golden for {name}; record it on the pre-split code"
    for key in ("raised", "result", "upserts", "events", "scan_log", "logs"):
        assert got[key] == want[key], f"{name}: {key} differs"


def test_golden_covers_every_scenario():
    if RECORD:
        pytest.skip("recording")
    assert set(_golden()) == set(SCENARIOS)
