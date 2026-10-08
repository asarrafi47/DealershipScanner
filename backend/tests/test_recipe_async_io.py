"""Recipe-store I/O never runs on the event loop (P1B.4).

The recipe store is a cache file per dealer plus the shared ``dealer_recipes`` table
(``load_recipes`` / ``save_recipes`` / ``mark_stale``, ``promote_from_ledger``, and the
``scan_hints`` writers). Every call does blocking file and DB I/O. Run on the event
loop, one slow Postgres round trip stalls every other dealer the process is scanning.
Before P1B.4 four paths did that:

- ``try_fetch_via_recipes``: the success write-back (last_ok_at, field coverage, un-stale);
- ``fetch_recipe_feed``: the provider hint read in an HTTP-only scan;
- ``discovery_capture.capture_endpoints``: the before/after counts and the promote;
- ``delta_scan_dealer``: the four ``_write_hint_note`` calls.

Each test spies on the store calls, records ``threading.current_thread()`` in every
one, and proves none ran on the loop's thread. The spies call through to the real
functions where the test needs the real behaviour (the replay), so the results are
checked too: moving the I/O must not change what is written.

Nothing here touches the real workspace or the session DB: the replay tests use the
per-test SQLite store from ``recipe_store_harness``; the others stub the store.
"""
from __future__ import annotations

import asyncio
import sys
import threading
import traceback
import types
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any

import pytest

import backend.scanner.delta_scan as ds
import backend.scanner.recipes as rec
from backend.scanner import recipe_store
from backend.scanner.recipes import PAGINATION_CARSCOMMERCE, EndpointRecipe, save_recipes
from backend.tests.recipe_store_harness import TwoHostRecipeStore

D = "async-io-dealer-com"
CC_URL = "https://websites-search.api.carscommerce.inc/api/v1/listings/1/search"
OTHER_URL = "https://websites-search.api.carscommerce.inc/api/v1/listings/1/legacy-search"


# ── the spy ───────────────────────────────────────────────────────────────────

class ThreadSpy:
    """Wraps module attributes and records the thread each call ran on."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self._mp = monkeypatch
        self.calls: list[tuple[str, threading.Thread]] = []
        self.stacks: list[str] = []

    def wrap(self, owner: Any, attr: str, replace: Callable[..., Any] | None = None) -> None:
        """Spy on ``owner.attr``; call ``replace`` instead of the original when given."""
        impl = replace if replace is not None else getattr(owner, attr)
        label = f"{getattr(owner, '__name__', type(owner).__name__).rsplit('.', 1)[-1]}.{attr}"

        def spy(*args: Any, **kwargs: Any) -> Any:
            self.calls.append((label, threading.current_thread()))
            self.stacks.append("".join(traceback.format_stack(limit=8)[:-1]))
            return impl(*args, **kwargs)

        self._mp.setattr(owner, attr, spy)

    def names(self) -> set[str]:
        return {name for name, _ in self.calls}

    def on(self, thread: threading.Thread) -> list[str]:
        return [name for name, t in self.calls if t is thread]

    def where(self, thread: threading.Thread) -> str:
        """The call sites of the calls made on ``thread`` (for the failure message)."""
        return "\n".join(
            f"--- {name}\n{stack}" for (name, t), stack in zip(self.calls, self.stacks) if t is thread
        )


def run_on_loop(make_coro: Callable[[], Any]) -> tuple[threading.Thread, Any]:
    """``asyncio.run`` the coroutine; return the loop's thread and the result."""
    box: dict[str, Any] = {}

    async def main() -> None:
        box["loop_thread"] = threading.current_thread()
        box["result"] = await make_coro()

    asyncio.run(main())
    return box["loop_thread"], box["result"]


def assert_off_loop(spy: ThreadSpy, loop_thread: threading.Thread, expected: set[str]) -> None:
    missing = expected - spy.names()
    assert not missing, f"store calls never made: {sorted(missing)} (made: {sorted(spy.names())})"
    on_loop = spy.on(loop_thread)
    assert on_loop == [], f"store I/O ran on the event loop thread: {on_loop}\n{spy.where(loop_thread)}"


# ── replay: try_fetch_via_recipes ─────────────────────────────────────────────

@pytest.fixture
def store(tmp_path, monkeypatch):
    monkeypatch.setenv("SCANNER_EGRESS_TAG", "")
    monkeypatch.setenv("SCANNER_RECIPE_FETCH", "1")
    monkeypatch.setenv("SCANNER_RECIPE_MIN_VEHICLES", "10")
    # The store place lookup (registry + scan hints) is not under test; keep it off the DB.
    monkeypatch.setattr("backend.attribution.gate.store_place", lambda url, dealer_id: {})
    # Nor is the rooftop gate's learned-alias read. parse_page reaches it on the loop
    # (rooftop_aliases._hinted_aliases -> get_scan_hints, cached per process and
    # dealer); that read belongs to the attribution layer, outside this unit.
    monkeypatch.setattr("backend.parsers.roster_name_aliases", lambda dealer_id: ())
    return TwoHostRecipeStore(tmp_path, monkeypatch)


def _recipe(**kw: Any) -> EndpointRecipe:
    fields: dict[str, Any] = dict(
        dealer_id=D, url=CC_URL, method="POST", content_type="application/json",
        post_template='{"page":1,"perPage":20}', pagination=PAGINATION_CARSCOMMERCE,
        provider_hint="dealer_dot_com", vehicle_rows=20, total_count=45,
    )
    fields.update(kw)
    return EndpointRecipe(**fields)


def _vehicle(i: int) -> dict:
    return {"vin": f"1HGBH41JXMN20{i:04d}", "year": 2024, "make": "Honda", "model": "Civic",
            "price": 30000 + i, "exterior_color": "Red"}


def _serve(monkeypatch, status: int = 200) -> None:
    """The recipe answers 45 cars over three pages (or ``status`` with no body)."""
    pages = {
        1: [_vehicle(i) for i in range(20)],
        2: [_vehicle(20 + i) for i in range(20)],
        3: [_vehicle(40 + i) for i in range(5)],
    }

    def _request(recipe, body, base_url, url=None):
        if status != 200:
            return status, None
        return 200, {"inventory": pages.get((body or {}).get("page", 1), [])}

    monkeypatch.setattr(rec, "_replay_request", _request)


def _store_spy(store: TwoHostRecipeStore, monkeypatch) -> ThreadSpy:
    spy = ThreadSpy(monkeypatch)
    for attr in ("load_recipes", "save_recipes", "mark_stale", "_atomic_write_json",
                 "record_stale_status", "clear_stale_status"):
        spy.wrap(rec, attr)
    # Every dealer_recipes / scan_hints round trip opens a connection here.
    spy.wrap(recipe_store, "_conn")
    return spy


_WRITE_BACK = {"recipes.load_recipes", "recipes.save_recipes", "recipes._atomic_write_json", "recipe_store._conn"}

REPLAY_CASES = {
    # name: (seeded recipe kwargs, replay status, union, store calls that must happen,
    #        "hit" or the stale_reason the replay leaves on file)
    "first_hit_success": ({}, 200, False, _WRITE_BACK | {"recipes.clear_stale_status"}, "hit"),
    "union_success": ({}, 200, True, _WRITE_BACK | {"recipes.clear_stale_status"}, "hit"),
    "stale_recipe_answers_again": (
        {"stale": True, "stale_reason": "http_401"}, 200, False,
        _WRITE_BACK | {"recipes.clear_stale_status"}, "hit",
    ),
    "auth_dead_marks_stale": (
        {}, 401, False, _WRITE_BACK | {"recipes.mark_stale", "recipes.record_stale_status"}, "http_401",
    ),
    "unparseable_post_template": (
        {"post_template": '{"page":1,"perPage":'}, 200, False, _WRITE_BACK | {"recipes.mark_stale"},
        "unreplayable_post_template",
    ),
}


@pytest.mark.parametrize("case", sorted(REPLAY_CASES))
def test_replay_store_io_runs_off_the_loop(case, store, monkeypatch):
    seed, status, union, expected, outcome = REPLAY_CASES[case]
    save_recipes(D, [_recipe(**seed)])
    _serve(monkeypatch, status)
    spy = _store_spy(store, monkeypatch)

    loop_thread, hit = run_on_loop(
        lambda: rec.try_fetch_via_recipes(D, "dealer_dot_com", "https://dealer.example", "Async Dealer", union=union)
    )

    assert_off_loop(spy, loop_thread, expected)
    (on_file,) = store.file_rows("mbp", D)
    if outcome == "hit":
        assert hit is not None and hit[1] == 45
        assert on_file["last_ok_at"] > 0, "the replay's write-back reached the cache file"
        assert on_file["field_coverage"] == {"price": 1.0, "trim": 0.0, "exterior_color": 1.0}
        assert on_file["stale"] is False and on_file["stale_reason"] == ""
        assert store.db_row(D)["rows"] == store.file_rows("mbp", D), "and the shared store"
    else:
        assert hit is None
        assert on_file["stale"] is True and on_file["stale_reason"] == outcome


def test_persist_replay_success_is_the_success_write_back(store):
    """The helper the replay hands to ``to_thread`` writes last_ok_at and coverage onto
    the matching recipe, un-stales it only when asked, and leaves siblings alone."""
    used = _recipe(url=OTHER_URL, stale=True, stale_reason="http_403")
    new = _recipe(stale=True, stale_reason="http_403")
    save_recipes(D, [new, used])
    answered = _recipe(stale=True, stale_reason="http_403", last_ok_at=123.0,
                       field_coverage={"price": 0.9, "trim": 0.5, "exterior_color": 0.1})

    rec._persist_replay_success(D, answered, False)
    first, second = store.file_rows("mbp", D)
    assert first["last_ok_at"] == 123.0 and first["field_coverage"]["price"] == 0.9
    assert first["stale"] is True, "not a stale retry: the flag stays"
    assert second["last_ok_at"] == 0.0 and second["field_coverage"] == {} and second["stale"] is True

    rec._persist_replay_success(D, answered, True)
    first, second = store.file_rows("mbp", D)
    assert first["stale"] is False and first["stale_reason"] == ""
    assert second["stale"] is True, "a sibling recipe is not un-staled"


# ── full scan: fetch_recipe_feed's provider hint ──────────────────────────────

def test_feed_provider_hint_runs_off_the_loop(monkeypatch):
    from backend.scanner.phases.dealer_run_steps.feed import fetch_recipe_feed
    from backend.scanner.phases.dealer_run_steps.state import DealerRun, new_result

    monkeypatch.setenv("SCANNER_ALLOW_BROWSER", "")  # HTTP-only: the hint is read
    spy = ThreadSpy(monkeypatch)

    async def _fetch(dealer_id, provider, base_url, dealer_name, *, union=False, coverage_out=None):
        if coverage_out is not None:
            coverage_out.update({"price": 1.0, "trim": 1.0, "exterior_color": 1.0, "n": 10})
        return [("https://dealer.example/api", {"inventory": []})], 10

    monkeypatch.setattr(rec, "try_fetch_via_recipes", _fetch)
    spy.wrap(rec, "load_recipes", lambda d: [
        SimpleNamespace(stale=True, provider_hint="stale_provider"),
        SimpleNamespace(stale=False, provider_hint="dealer_inspire"),
    ])
    spy.wrap(rec, "last_known_vin_count", lambda d: 10)
    run = DealerRun(
        dealer={"dealer_id": D, "url": "https://dealer.example", "name": "Async Dealer"},
        name="Async Dealer", url="https://dealer.example", provider="dealer_dot_com", dealer_id=D,
        result=new_result(D, "Async Dealer", "dealer_dot_com"), t0=0.0,
    )

    loop_thread, _ = run_on_loop(lambda: fetch_recipe_feed(run))

    assert_off_loop(spy, loop_thread, {"recipes.load_recipes", "recipes.last_known_vin_count"})
    assert run.result["provider"] == "dealer_inspire", "the live recipe's hint still wins"
    assert run.result["recipe_fetch"] == "full+http_only"
    assert run.intercept_records == [("https://dealer.example/api", {"inventory": []})]


# ── discovery capture: before/after counts and the promote ────────────────────

class _FakeContext:
    async def close(self) -> None:
        return None


class _FakeBrowser:
    async def new_context(self, **_kw: Any) -> _FakeContext:
        return _FakeContext()

    async def close(self) -> None:
        return None


class _FakeChromium:
    async def launch(self, **_kw: Any) -> _FakeBrowser:
        return _FakeBrowser()


class _FakePlaywright:
    chromium = _FakeChromium()


class _FakePlaywrightCM:
    async def __aenter__(self) -> _FakePlaywright:
        return _FakePlaywright()

    async def __aexit__(self, *_exc: Any) -> bool:
        return False


def test_discovery_capture_store_io_runs_off_the_loop(monkeypatch):
    import backend.scanner.discovery_capture as dc
    import backend.scanner.http_fetch as http_fetch
    import backend.scanner.phases.inventory_scrape as inventory_scrape

    # A stub browser: no Chromium, no network.
    monkeypatch.setenv("SCANNER_ALLOW_BROWSER", "1")  # capture_endpoints sets it; monkeypatch restores it
    fake_api = types.ModuleType("playwright.async_api")
    fake_api.async_playwright = lambda: _FakePlaywrightCM()  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "playwright.async_api", fake_api)
    monkeypatch.setitem(sys.modules, "playwright_stealth", None)  # import fails -> plain async_playwright()
    monkeypatch.setattr(http_fetch, "playwright_proxy_kwargs", lambda: {})
    endpoint = SimpleNamespace(
        method="POST", url=CC_URL, content_type="application/json", vehicle_rows=20, total_count=45,
        provider_hint="dealer_dot_com", post_data_sample='{"page":1,"perPage":20}', reason="legacy",
    )

    async def _scrape(context, path, origin, provider, dealer_id, name, wait_ms, page_wait_ms, site_profile=None):
        assert isinstance(context, _FakeContext)
        return [("https://dealer.example/api", {"inventory": []})], "", 0, [], [endpoint]

    monkeypatch.setattr(inventory_scrape, "scrape_inventory_path", _scrape)

    spy = ThreadSpy(monkeypatch)
    saved: list[Any] = []
    promoted: list[dict[str, Any]] = []

    def _promote(dealer_id, provider, endpoints, **kw):
        promoted.append({"dealer_id": dealer_id, "endpoints": list(endpoints), **kw})
        kw["validation_out"].update({"verdict": "accept", "reasons": []})
        saved.append(SimpleNamespace(stale=False))
        return 1

    spy.wrap(rec, "load_recipes", lambda d: list(saved))
    spy.wrap(rec, "promote_from_ledger", _promote)
    spy.wrap(dc, "capture_place", lambda dealer, origin: {"city": "Charlotte", "state": "NC"})

    loop_thread, out = run_on_loop(lambda: dc.capture_endpoints(
        D, "https://www.async-io-dealer.com/", "Async Dealer", "dealer_dot_com",
        paths=["/new-inventory/index.htm"], profile=False,
    ))

    assert_off_loop(spy, loop_thread, {"recipes.load_recipes", "recipes.promote_from_ledger",
                                       "discovery_capture.capture_place"})
    assert [n for n, _ in spy.calls].count("recipes.load_recipes") == 2, "before and after counts"
    assert out["errors"] == []
    assert (out["recipes_before"], out["recipes_written"], out["recipes_after"]) == (0, 1, 1)
    assert out["validation"] == {"verdict": "accept", "reasons": []}
    (call,) = promoted
    assert call["endpoints"] == [endpoint]
    assert call["place"] == {"city": "Charlotte", "state": "NC"}
    assert call["base_url"] == "https://www.async-io-dealer.com" and call["validate"] is True


def test_discovery_capture_promote_failure_is_still_reported(monkeypatch):
    """A promote that raises in the worker thread lands in ``errors`` as before."""
    import backend.scanner.discovery_capture as dc
    import backend.scanner.http_fetch as http_fetch
    import backend.scanner.phases.inventory_scrape as inventory_scrape

    monkeypatch.setenv("SCANNER_ALLOW_BROWSER", "1")
    fake_api = types.ModuleType("playwright.async_api")
    fake_api.async_playwright = lambda: _FakePlaywrightCM()  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "playwright.async_api", fake_api)
    monkeypatch.setitem(sys.modules, "playwright_stealth", None)
    monkeypatch.setattr(http_fetch, "playwright_proxy_kwargs", lambda: {})
    endpoint = SimpleNamespace(method="GET", url=CC_URL, vehicle_rows=20, reason="legacy")

    async def _scrape(*_a, **_k):
        return [], "", 0, [], [endpoint]

    def _boom(*_a, **_k):
        raise RuntimeError("store down")

    monkeypatch.setattr(inventory_scrape, "scrape_inventory_path", _scrape)
    monkeypatch.setattr(rec, "load_recipes", lambda d: [])
    monkeypatch.setattr(rec, "promote_from_ledger", _boom)
    monkeypatch.setattr(dc, "capture_place", lambda dealer, origin: None)

    out = asyncio.run(dc.capture_endpoints(D, "https://www.async-io-dealer.com/", paths=["/x"], profile=False))
    assert out["recipes_written"] == 0
    assert out["errors"] == ["promote: store down"]


# ── delta scan: the four hint notes ───────────────────────────────────────────

def _rows(n: int, priced: bool = True) -> list[dict]:
    return [{"vin": f"1HGBH41JXMN3{i:05d}", "price": 20000 + i if priced else None} for i in range(n)]


DELTA_CASES = {
    # name: (replay rows or None for no yield, unidentified refusals, known active, hints,
    #        expected note fragment, expected extra keys, expected skip)
    "no_recipe_yield": (None, 0, 0, {}, "no recipe yield on delta replay", {}, "no_recipe_yield"),
    "rooftop_unidentified": (
        _rows(10), 3, 100, {}, "rooftop gate could not identify this store",
        {"rooftop_unidentified_rows": 3}, "partial_feed",
    ),
    "priceless_needs_full_scan": (
        _rows(60, priced=False), 0, 60, {}, "60 priceless rows on delta replay",
        {"price_requires_full_scan": True, "price_source": None}, "low_price_coverage",
    ),
    "price_lives_on_vdp": (
        _rows(20, priced=False), 0, 20, {}, "feed priced 0% on delta replay",
        {"price_source": "vdp"}, "low_price_coverage",
    ),
}


@pytest.mark.parametrize("case", sorted(DELTA_CASES))
def test_delta_hint_notes_run_off_the_loop(case, monkeypatch):
    rows, unidentified, known, hints, fragment, extra, skip = DELTA_CASES[case]
    spy = ThreadSpy(monkeypatch)
    written: list[tuple[str, dict]] = []

    def _set_scan_hints(dealer_id, new_hints, *, merge=True):
        written.append((dealer_id, dict(new_hints)))
        return True

    spy.wrap(recipe_store, "get_scan_hints", lambda dealer_id: dict(hints))
    spy.wrap(recipe_store, "set_scan_hints", _set_scan_hints)

    async def _fetch(dealer_id, provider, base_url, dealer_name, **_kw):
        if rows is None:
            return None
        return [("https://dealer.example/api", {"x": 1})], len(rows)

    monkeypatch.setattr(rec, "try_fetch_via_recipes", _fetch)
    monkeypatch.setattr(ds, "DealerCtx", SimpleNamespace(for_store=lambda *a: None))
    monkeypatch.setattr(ds, "parse_page", lambda provider, body, ctx, refused: [dict(v) for v in rows or []])
    monkeypatch.setattr(ds, "decide", lambda found, ctx: SimpleNamespace(kept=list(found), refused=[]))
    monkeypatch.setattr(ds, "split_refusals", lambda refused: ([], unidentified))
    monkeypatch.setattr(ds, "_active_count", lambda dealer_id: known)
    monkeypatch.setattr(ds, "_complete_prices_from_vdp", lambda vehicles, name: 0)

    loop_thread, out = run_on_loop(lambda: ds.delta_scan_dealer(
        {"dealer_id": D, "url": "https://dealer.example", "name": "Async Dealer", "provider": "dealer_dot_com"}
    ))

    assert_off_loop(spy, loop_thread, {"recipe_store.get_scan_hints", "recipe_store.set_scan_hints"})
    assert out["skipped"] and out["skipped"].startswith(skip)
    ((dealer_id, note),) = written
    assert dealer_id == D
    assert fragment in note["notes"] and note["hint_source"] == "delta_scan_auto"
    assert {k: note[k] for k in extra} == extra
