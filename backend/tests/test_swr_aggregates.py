"""Web speed P6: fleet aggregates are refreshed off the request (docs/web_speed_search_2026_11/README.md).

README acceptance, one test group each:

* 4 concurrent callers at expiry -> the loader runs once;
* the first call after expiry returns in under 50 ms (a loader that takes 0.5 s);
* a failing refresh keeps serving the old value (and does not retry on every request);
* inline compute stays as the cold fallback (and concurrent cold callers share one load);
* the rarity Counters equal the shipped per-car loader;
* warm at start, capped, never fatal, before the worker accepts a connection.

The six aggregates the README groups as "the five": rarity counts, price plausibility,
deal-score bands, public listings count, landing featured cars, dealer fallback URLs.
Fakes stand in for the production database (this suite has none).
"""

from __future__ import annotations

import ast
import threading
import time
from collections import Counter
from pathlib import Path

import pytest

from backend.db.repositories import grid_cards_repo as gc
from backend.db.repositories import listings_repo as lr
from backend.intelligence import deal_score_cache as dsc
from backend.utils import aggregate_warmup as warmup
from backend.utils import price_plausibility as pp
from backend.utils import rarity_score as rs
from backend.utils.swr_refresh import BackgroundRefresher

REPO = Path(__file__).resolve().parents[2]
SLOW_S = 0.5
FAST_LIMIT_S = 0.05


class FakeLoader:
    """A loader that counts its runs, takes ``delay`` seconds and can be made to fail."""

    def __init__(self, value, delay: float = 0.0):
        self.value = value
        self.delay = delay
        self.runs = 0
        self.fail = False
        self._lock = threading.Lock()

    def __call__(self, *a, **k):
        with self._lock:
            self.runs += 1
            n = self.runs
        time.sleep(self.delay)
        if self.fail:
            raise RuntimeError("database unavailable")
        return self.value(n) if callable(self.value) else self.value


def _concurrently(fn, n: int = 4):
    """Run ``fn`` on n threads released together; returns their results."""
    barrier = threading.Barrier(n)
    results: list = [None] * n

    def run(i):
        barrier.wait()
        results[i] = fn()

    threads = [threading.Thread(target=run, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(10)
    return results


def _timed(fn):
    t0 = time.perf_counter()
    out = fn()
    return out, time.perf_counter() - t0


# Each aggregate is driven through the same five facts. A case knows how to put the aggregate
# in "loaded long ago" state with a fake loader, how to read it, and how to wait for its refresher.


class Case:
    name = ""

    def install(self, monkeypatch, loader):  # patch the loader, leave the aggregate cold
        raise NotImplementedError

    def make_stale(self, monkeypatch):  # loaded earlier, past its TTL
        raise NotImplementedError

    def read(self):
        raise NotImplementedError

    def refresher(self) -> BackgroundRefresher:
        raise NotImplementedError


class RarityCase(Case):
    name = "rarity"

    def install(self, monkeypatch, loader):
        monkeypatch.setattr(rs, "_load_inventory_counts", loader)
        monkeypatch.setattr(rs, "_counts_cache", None)

    def make_stale(self, monkeypatch):
        old = {"model": Counter({("a", "b"): 1}), "trim": Counter(), "color": Counter(), "color_known": Counter()}
        monkeypatch.setattr(rs, "_counts_cache", (time.monotonic() - rs._CACHE_TTL_S - 1, old))
        return old

    def read(self):
        return rs.get_inventory_counts()

    def refresher(self):
        return rs._counts_refresher

    def fresh_value(self, n):
        return {"model": Counter({("a", "b"): 100 + n}), "trim": Counter(), "color": Counter(), "color_known": Counter()}


class PlausibilityCase(Case):
    name = "price_plausibility"

    def install(self, monkeypatch, loader):
        monkeypatch.setattr(pp, "_load", loader)
        monkeypatch.setattr(pp, "_trim_year_medians", None)
        monkeypatch.setattr(pp, "_model_medians", None)
        monkeypatch.setattr(pp, "_loaded_at", 0.0)

    def make_stale(self, monkeypatch):
        old = ({("ford", "bronco", "base", 2026): (1.0, 9)}, {})
        monkeypatch.setattr(pp, "_trim_year_medians", old[0])
        monkeypatch.setattr(pp, "_model_medians", old[1])
        monkeypatch.setattr(pp, "_loaded_at", time.monotonic() - pp._CACHE_TTL_SEC - 1)
        return old[0]

    def read(self):
        pp._ensure_loaded()
        return pp._trim_year_medians

    def refresher(self):
        return pp._refresher

    def fresh_value(self, n):
        return ({("ford", "bronco", "base", 2026): (float(100 + n), 9)}, {})


class BandsCase(Case):
    name = "deal_bands"

    def install(self, monkeypatch, loader):
        monkeypatch.setattr(dsc, "_load_bands", loader)
        monkeypatch.setattr(dsc, "_bands", None)
        monkeypatch.setattr(dsc, "_loaded_at", 0.0)

    def make_stale(self, monkeypatch):
        old = {(2022, "a", "b", "c", "used", 0): {"band_median": 1.0}}
        monkeypatch.setattr(dsc, "_bands", old)
        monkeypatch.setattr(dsc, "_loaded_at", time.monotonic() - dsc._CACHE_TTL_SEC - 1)
        return old

    def read(self):
        return dsc._bands_map()

    def refresher(self):
        return dsc._refresher

    def fresh_value(self, n):
        return {(2022, "a", "b", "c", "used", 0): {"band_median": float(100 + n)}}


class CountCase(Case):
    name = "public_count"

    def install(self, monkeypatch, loader):
        monkeypatch.setattr(lr, "_count_public_listings_uncached", loader)
        monkeypatch.setattr(lr, "_public_count_cache", None)
        monkeypatch.setattr(lr, "_pg_grid_write_fingerprint", lambda: (("cars", 1),))

    def make_stale(self, monkeypatch):
        monkeypatch.setattr(lr, "_public_count_cache", ((("cars", 1),), time.time() - lr._PUBLIC_COUNT_TTL_S - 1, 7))
        return 7

    def read(self):
        return lr.public_listings_count()

    def refresher(self):
        return lr._count_refresher

    def fresh_value(self, n):
        return 1000 + n


class FeaturedCase(Case):
    name = "featured_cars"

    def install(self, monkeypatch, loader):
        monkeypatch.setattr(lr, "_build_featured_cars", loader)
        monkeypatch.setattr(lr, "_featured_cars_cache", None)

    def make_stale(self, monkeypatch):
        old = [{"id": 1}]
        monkeypatch.setattr(lr, "_featured_cars_cache", (time.time() - lr._FEATURED_CARS_TTL_S - 1, old))
        return old

    def read(self):
        return lr.landing_featured_cars(limit=4)

    def refresher(self):
        return lr._featured_refresher

    def fresh_value(self, n):
        return [{"id": 100 + n}]

    def after_load(self, monkeypatch, loader):
        """The real _build_featured_cars stores into the cache; the fake must as well."""
        real = loader

        def storing():
            out = real()
            monkeypatch.setattr(lr, "_featured_cars_cache", (time.time(), out))
            return out

        monkeypatch.setattr(lr, "_build_featured_cars", storing)


class FallbackCase(Case):
    name = "fallback_urls"

    def install(self, monkeypatch, loader):
        monkeypatch.setattr(gc, "_load_fallback_dealer_urls", loader)
        monkeypatch.setattr(gc, "_fallback_urls_cache", None)

    def make_stale(self, monkeypatch):
        old = [("https://a.test", "a", 1, None)]
        monkeypatch.setattr(gc, "_fallback_urls_cache", (time.monotonic() - gc._FALLBACK_TTL_S - 1, old))
        return old

    def read(self):
        return gc._fallback_dealer_urls()

    def refresher(self):
        return gc._fallback_refresher

    def fresh_value(self, n):
        return [("https://a.test", "a", 100 + n, None)]


CASES = [RarityCase(), PlausibilityCase(), BandsCase(), CountCase(), FeaturedCase(), FallbackCase()]


def _prepare(case: Case, monkeypatch, loader):
    case.install(monkeypatch, loader)
    if hasattr(case, "after_load"):
        case.after_load(monkeypatch, loader)


@pytest.fixture(autouse=True)
def _quiet_refreshers():
    """No background run from one test may outlive it."""
    yield
    for case in CASES:
        case.refresher().join(5)


@pytest.mark.parametrize("case", CASES, ids=lambda c: c.name)
def test_four_concurrent_callers_at_expiry_run_the_loader_once(case, monkeypatch):
    loader = FakeLoader(case.fresh_value, delay=SLOW_S)
    _prepare(case, monkeypatch, loader)
    case.make_stale(monkeypatch)
    results = _concurrently(case.read, 4)
    assert all(r is not None for r in results)
    assert case.refresher().join(10)
    assert loader.runs == 1, "one background reload for four callers, not four"


@pytest.mark.parametrize("case", CASES, ids=lambda c: c.name)
def test_first_call_after_expiry_returns_under_50_ms_with_the_old_value(case, monkeypatch):
    loader = FakeLoader(case.fresh_value, delay=SLOW_S)
    _prepare(case, monkeypatch, loader)
    old = case.make_stale(monkeypatch)
    value, elapsed = _timed(case.read)
    assert elapsed < FAST_LIMIT_S, f"{case.name} waited {elapsed:.3f}s for its own reload"
    if case.name == "featured_cars":
        assert value == old
    else:
        assert value == old
    assert case.refresher().join(10)
    # ...and the next call sees the reloaded value.
    assert case.read() != old


@pytest.mark.parametrize("case", CASES, ids=lambda c: c.name)
def test_a_failing_refresh_keeps_the_old_value_and_does_not_retry_per_request(case, monkeypatch):
    loader = FakeLoader(case.fresh_value, delay=0.0)
    loader.fail = True
    _prepare(case, monkeypatch, loader)
    old = case.make_stale(monkeypatch)
    assert case.read() == old
    assert case.refresher().join(10)
    assert loader.runs == 1
    for _ in range(5):
        assert case.read() == old, "the old value keeps being served"
    assert case.refresher().join(10)
    assert loader.runs == 1, "a failed refresh is retried after a pause, not on every request"
    # Once the loader works again and the retry pause has passed, the value is replaced.
    loader.fail = False
    case.make_stale(monkeypatch)
    case.read()
    assert case.refresher().join(10)
    assert loader.runs == 2
    assert case.read() != old


@pytest.mark.parametrize("case", CASES, ids=lambda c: c.name)
def test_cold_start_computes_inline_once_for_concurrent_callers(case, monkeypatch):
    loader = FakeLoader(case.fresh_value, delay=0.2)
    _prepare(case, monkeypatch, loader)
    results = _concurrently(case.read, 4)
    assert loader.runs == 1, "concurrent cold callers share one inline load"
    assert all(r is not None for r in results)
    assert case.read() is not None
    assert loader.runs == 1


def test_a_cold_failure_still_answers_and_looks_again_soon(monkeypatch):
    """Cold fallbacks that returned an empty value before the change still do."""
    loader = FakeLoader({}, delay=0.0)
    loader.fail = True
    dsc_case = BandsCase()
    dsc_case.install(monkeypatch, loader)
    assert dsc._bands_map() == {}
    # The empty value is retried after a short pause, not after a whole 300 s TTL.
    age = time.monotonic() - dsc._loaded_at
    assert age > dsc._CACHE_TTL_SEC - 60


def test_plausibility_reads_a_consistent_pair_while_a_reload_swaps(monkeypatch):
    seeded = ({("ford", "bronco", "base", 2026): (46039.0, 44)}, {})
    monkeypatch.setattr(pp, "_trim_year_medians", seeded[0])
    monkeypatch.setattr(pp, "_model_medians", seeded[1])
    monkeypatch.setattr(pp, "_loaded_at", time.monotonic())
    car = {"make": "Ford", "model": "Bronco", "trim": "Base", "year": 2026}
    assert pp.implausible_price(car, 449150.0) is True
    assert pp.implausible_price(car, 40000.0) is False


# ── rarity: counted in SQL, Counters equal to the shipped loader ─────────────────────


def _shipped_loader(conn):
    """The per-car loop rarity_score shipped before P6, verbatim (the reference)."""
    model_c: Counter = Counter()
    trim_c: Counter = Counter()
    color_c: Counter = Counter()
    color_known_c: Counter = Counter()
    cur = conn.execute(
        """
        SELECT make, model, trim, exterior_color
        FROM cars
        WHERE COALESCE(listing_active, 1) = 1
          AND TRIM(COALESCE(make, '')) != ''
          AND TRIM(COALESCE(model, '')) != ''
        """
    )
    for make, model, trim, color in cur.fetchall():
        mk_md = (rs._norm(make), rs._norm(model))
        model_c[mk_md] += 1
        t = rs._norm(trim)
        if t:
            trim_c[(*mk_md, t)] += 1
        fam = rs._color_family(color, make)
        if fam:
            color_known_c[mk_md] += 1
            color_c[(*mk_md, fam)] += 1
    return {"model": model_c, "trim": trim_c, "color": color_c, "color_known": color_known_c}


def _fleet_rows():
    rows = []
    spec = [
        ("BMW", "M3", "Competition", "Isle of Man Green", 3),
        ("bmw", " m3 ", "competition", "ISLE OF MAN GREEN", 2),  # spelling variants share a pool
        ("BMW", "M3", "", "Black Sapphire Metallic", 4),         # no trim
        ("Toyota", "Camry", "LE", "Midnight Black Metallic", 6),
        ("Toyota", "Camry", "LE", None, 2),                      # unknown colour
        ("Toyota", "Camry", "SE", "Wind Chill Pearl", 5),
        ("Ford", "Bronco", None, "Area 51", 1),
        ("McLaren", "765LT", "Base", "Papaya Spark", 1),
        ("", "Ghost", "X", "Black", 2),                          # no make: never counted
        ("Ford", "   ", "X", "Black", 2),                        # blank model: never counted
    ]
    for make, model, trim, color, n in spec:
        for _ in range(n):
            rows.append({"make": make, "model": model, "trim": trim, "exterior_color": color})
    rows += [
        {"make": "Toyota", "model": "Camry", "trim": "LE", "exterior_color": "Midnight Black Metallic", "listing_active": 0},
        {"make": "Honda", "model": "Civic", "trim": "EX", "exterior_color": "Rallye Red", "listing_active": None},
    ]
    return rows


def test_rarity_counters_equal_the_shipped_loader(sqlite_inventory):
    sqlite_inventory.add_cars(_fleet_rows())
    from backend.db.inventory_db import db_conn

    with db_conn() as conn:
        expected = _shipped_loader(conn)
    got = rs._load_inventory_counts()
    assert got == expected
    assert got["model"][("toyota", "camry")] == 13  # inactive row excluded
    assert got["model"][("bmw", "m3")] == 9         # three spellings, one pool
    assert got["trim"][("bmw", "m3", "competition")] == 5
    assert sum(got["model"].values()) == sum(1 for r in _fleet_rows() if r["make"].strip() and r["model"].strip() and r.get("listing_active", 1) in (1, None))


def test_rarity_loader_asks_the_database_to_count(sqlite_inventory, monkeypatch):
    """One GROUP BY statement, not a row per car in Python."""
    sqlite_inventory.add_cars(_fleet_rows())
    from backend.db import inventory_db

    seen = []
    real = inventory_db.db_conn

    class Spy:
        def __init__(self, ctx):
            self.ctx = ctx

        def __enter__(self):
            conn = self.ctx.__enter__()

            class C:
                def execute(self_inner, sql, *a):
                    seen.append(" ".join(sql.split()))
                    return conn.execute(sql, *a)

            return C()

        def __exit__(self, *exc):
            return self.ctx.__exit__(*exc)

    monkeypatch.setattr(inventory_db, "db_conn", lambda *a, **k: Spy(real(*a, **k)))
    rs._load_inventory_counts()
    assert len(seen) == 1 and "GROUP BY make, model, trim, exterior_color" in seen[0]
    assert "COUNT(*)" in seen[0]


def test_rarity_scores_do_not_change(monkeypatch):
    """The score the shopper sees is a function of the Counters, which are equal."""
    counts = rs._counts_from_groups([("Toyota", "Camry", "LE", "Midnight Black Metallic", 40), ("Ford", "Bronco", "", "Area 51", 1)])
    car = {"make": "Ford", "model": "Bronco", "trim": ""}
    out = rs.rarity_for_car(car, counts=counts)
    assert out is not None and out["model_count"] == 1


# ── warm at start ───────────────────────────────────────────────────────────────────


def test_warm_loads_every_aggregate_and_a_second_warm_is_free(monkeypatch):
    loaders = {}
    for case in CASES:
        loaders[case.name] = FakeLoader(case.fresh_value)
        _prepare(case, monkeypatch, loaders[case.name])
    result = warmup.warm_aggregates()
    assert result == {label: "ok" for label, _m, _f in warmup.WARM_STEPS}
    assert all(l.runs == 1 for l in loaders.values())
    warmup.warm_aggregates()
    assert all(l.runs == 1 for l in loaders.values()), "already loaded: nothing runs again"


def test_warm_steps_cover_the_six_aggregates():
    assert [m for _l, m, _f in warmup.WARM_STEPS] == [
        "backend.utils.rarity_score",
        "backend.utils.price_plausibility",
        "backend.intelligence.deal_score_cache",
        "backend.db.repositories.listings_repo",
        "backend.db.repositories.listings_repo",
        "backend.db.repositories.grid_cards_repo",
    ]
    import importlib

    for _label, module, func in warmup.WARM_STEPS:
        assert callable(getattr(importlib.import_module(module), func))


def test_a_failing_warm_step_does_not_stop_the_others(monkeypatch):
    steps = (
        ("one", "backend.utils.swr_refresh", "does_not_exist"),
        ("two", "backend.utils.aggregate_warmup", "warm_enabled"),
    )
    result = warmup.warm_aggregates(steps)
    assert result["one"].startswith("failed")
    assert result["two"] == "ok"


def test_warm_stops_starting_steps_after_the_deadline():
    steps = (("a", "backend.utils.aggregate_warmup", "warm_enabled"),) * 2
    result = warmup.warm_aggregates(steps, deadline=time.monotonic() - 1)
    assert set(result.values()) == {"skipped"}


def test_warm_blocking_is_capped_so_a_slow_boot_cannot_fail_the_deploy(monkeypatch):
    release = threading.Event()
    monkeypatch.setattr(warmup, "warm_aggregates", lambda steps=None, deadline=None: release.wait(30) and {})
    t0 = time.perf_counter()
    assert warmup.warm_blocking(cap_s=0.3) is None
    assert time.perf_counter() - t0 < 2.0
    release.set()


def test_warm_blocking_returns_the_results_when_it_finishes_in_time(monkeypatch):
    monkeypatch.setattr(warmup, "warm_aggregates", lambda steps=None, deadline=None: {"x": "ok"})
    assert warmup.warm_blocking(cap_s=5) == {"x": "ok"}


def test_warm_can_be_switched_off(monkeypatch):
    monkeypatch.setenv("AGGREGATE_WARM", "0")
    called = []
    monkeypatch.setattr(warmup, "warm_aggregates", lambda *a, **k: called.append(1) or {})
    assert warmup.warm_blocking() is None and not called


def test_warm_cap_stays_under_the_worker_timeout(monkeypatch):
    entrypoint = (REPO / "scripts" / "docker-entrypoint-web.sh").read_text(encoding="utf-8")
    import re

    timeout = int(re.search(r"--timeout (\d+)", entrypoint).group(1))
    assert warmup.MAX_CAP_S < timeout and warmup.DEFAULT_CAP_S < warmup.MAX_CAP_S
    monkeypatch.setenv("AGGREGATE_WARM_CAP_S", "100000")
    assert warmup.warm_cap_seconds() == warmup.MAX_CAP_S
    monkeypatch.setenv("AGGREGATE_WARM_CAP_S", "junk")
    assert warmup.warm_cap_seconds() == warmup.DEFAULT_CAP_S


def test_gunicorn_post_fork_warms_before_the_worker_serves(monkeypatch):
    """post_fork runs in the worker before it accepts a connection, so warming there holds
    /health back until the aggregates are loaded (or the cap passes)."""
    import importlib.util

    spec = importlib.util.spec_from_file_location("gunicorn_conf_under_test", REPO / "gunicorn.conf.py")
    conf = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(conf)
    calls = []
    import backend.main as main

    monkeypatch.setattr(main, "_prewarm_listings_inventory_cache", lambda: calls.append("prewarm"))
    monkeypatch.setattr(warmup, "warm_blocking", lambda *a, **k: calls.append("warm"))
    conf.post_fork(None, None)
    assert calls == ["prewarm", "warm"]


def test_gunicorn_post_fork_survives_a_warm_up_crash(monkeypatch):
    import importlib.util

    spec = importlib.util.spec_from_file_location("gunicorn_conf_under_test2", REPO / "gunicorn.conf.py")
    conf = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(conf)
    import backend.main as main

    monkeypatch.setattr(main, "_prewarm_listings_inventory_cache", lambda: None)

    def boom(*a, **k):
        raise RuntimeError("warm-up bug")

    monkeypatch.setattr(warmup, "warm_blocking", boom)
    conf.post_fork(None, None)  # must not raise: a boot is never failed by the warm-up


def test_warm_up_is_not_started_at_import_or_under_the_arbiter():
    """backend/main.py must not warm the aggregates itself: the arbiter would load them for
    a process that serves nothing (the reason gunicorn.conf.py owns the prewarm)."""
    tree = ast.parse((REPO / "backend" / "main.py").read_text(encoding="utf-8"))
    names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)} | {
        n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)
    }
    assert "warm_blocking" not in names and "warm_aggregates" not in names


def test_the_old_request_path_loaders_are_gone():
    """Nothing in the five aggregates reloads inline past its TTL any more."""
    for path, needle in (
        ("backend/utils/rarity_score.py", "lru_cache"),
        ("backend/utils/price_plausibility.py", "stale = _trim_year_medians is None or"),
        ("backend/intelligence/deal_score_cache.py", "stale = _bands is None or"),
    ):
        assert needle not in (REPO / path).read_text(encoding="utf-8"), path


def test_refresher_runs_one_job_at_a_time_and_swallows_failures():
    r = BackgroundRefresher("t")
    gate = threading.Event()
    started = []

    def job():
        started.append(1)
        gate.wait(5)
        raise RuntimeError("boom")

    assert r.kick(job) is True
    assert r.kick(job) is False and r.running()
    gate.set()
    assert r.join(5)
    assert len(started) == 1 and isinstance(r.last_error, RuntimeError)
    assert r.kick(lambda: None) is True
    assert r.join(5) and r.last_error is None
