"""saved_at is the last-write stamp, so every recipe write reaches the other hosts (P1B.3).

``load_recipes`` reconciles a host's recipe file with the shared ``dealer_recipes`` row
by max ``saved_at``. Before P1B.3 only a capture (promote) set ``saved_at``: mark_stale,
a replay's un-stale and its last_ok / coverage update kept the old stamp, so another
host with the same stamp kept its own copy; and a re-synthesized or cascaded set was
saved with ``saved_at=0``, so any older cache won and pushed its old set back up.
``save_recipes`` now stamps every row with one ``time.time()`` per call. A push-up
(file newer than the DB) does not stamp, and logs a WARNING.

Every test runs two hosts (``mbp``, ``mini``) over one per-test SQLite store from
``recipe_store_harness``; nothing touches the session DB or the real workspace.
Replays go through the real ``try_fetch_via_recipes`` with ``_replay_request`` stubbed.
"""
from __future__ import annotations

import asyncio
import logging
import sqlite3
from contextlib import closing
from dataclasses import asdict
from types import SimpleNamespace

import pytest

import backend.scanner.recipes as rec
from backend.scanner import recipe_store
from backend.scanner.recipe_cascade import adapt_recipe
from backend.scanner.recipes import (
    PAGINATION_CARSCOMMERCE,
    EndpointRecipe,
    load_recipes,
    mark_stale,
    save_recipes,
)
from backend.tests.recipe_store_harness import TwoHostRecipeStore, _is_under, session_inventory_db_path

D = "freshness-dealer-com"
SITE = "https://www.freshness-dealer.com"
CC_URL = "https://websites-search.api.carscommerce.inc/api/v1/listings/1/search"
OLD_URL = "https://websites-search.api.carscommerce.inc/api/v1/listings/1/legacy-search"
T0 = 1_800_000_000.0   # "now" in these tests (2027)
OLD = 1_700_000_000.0  # an older cache (2023)


@pytest.fixture
def store(tmp_path, monkeypatch):
    monkeypatch.setenv("SCANNER_EGRESS_TAG", "")
    monkeypatch.setenv("SCANNER_RECIPE_FETCH", "1")
    monkeypatch.setenv("SCANNER_RECIPE_MIN_VEHICLES", "10")
    return TwoHostRecipeStore(tmp_path, monkeypatch)


def _recipe(**kw) -> EndpointRecipe:
    fields = dict(
        dealer_id=D, url=CC_URL, method="POST", content_type="application/json",
        post_template='{"page":1,"perPage":20}', pagination=PAGINATION_CARSCOMMERCE,
        provider_hint="dealer_dot_com", vehicle_rows=20, total_count=45,
    )
    fields.update(kw)
    return EndpointRecipe(**fields)


def _vehicle(i: int) -> dict:
    return {"vin": f"1HGBH41JXMN10{i:04d}", "year": 2024, "make": "Honda", "model": "Civic", "price": 30000 + i}


def _serve_pages(monkeypatch) -> None:
    """The recipe answers 45 cars over three pages."""
    pages = {
        1: [_vehicle(i) for i in range(20)],
        2: [_vehicle(20 + i) for i in range(20)],
        3: [_vehicle(40 + i) for i in range(5)],
    }
    monkeypatch.setattr(
        rec, "_replay_request",
        lambda recipe, body, base_url, url=None: (200, {"inventory": pages.get(body["page"], [])}),
    )


def _replay():
    return asyncio.run(rec.try_fetch_via_recipes(D, "dealer_dot_com", "https://dealer.example", "Freshness Dealer"))


def _push_up_warnings(caplog) -> list[logging.LogRecord]:
    return [
        r for r in caplog.records
        if r.levelno == logging.WARNING and "newer than dealer_recipes" in r.getMessage()
    ]


# ── the stamp ──────────────────────────────────────────────────────────────────

def test_save_stamps_every_row_with_one_clock_reading(store):
    clock = store.freeze_clock(T0 + 0.25)
    recipes = [
        _recipe(saved_at=0.0),
        _recipe(post_template='{"page":1,"perPage":20,"type":"used"}', saved_at=5.0),
        _recipe(post_template='{"page":1,"perPage":20,"type":"cpo"}', saved_at=9e9),
    ]
    save_recipes(D, recipes)

    assert clock.calls == 1, "one time.time() per save_recipes call"
    on_file = store.file_rows("mbp", D)
    assert [r["saved_at"] for r in on_file] == [T0 + 0.25] * 3
    row = store.db_row(D)
    assert row["rows"] == on_file, "the file and the DB row carry the same stamp"
    assert row["max_saved_at"] == T0 + 0.25
    assert [r.saved_at for r in recipes] == [0.0, 5.0, 9e9], "the caller's objects are not modified"


def test_each_save_takes_a_new_stamp(store):
    clock = store.freeze_clock(T0)
    save_recipes(D, [_recipe()])
    clock.now = T0 + 1
    save_recipes(D, [_recipe(vehicle_rows=21)])
    assert store.file_rows("mbp", D)[0]["saved_at"] == T0 + 1
    assert store.db_row(D)["max_saved_at"] == T0 + 1


# ── every write reaches the other host ─────────────────────────────────────────

def test_a_stale_flag_reaches_the_other_host(store):
    clock = store.freeze_clock(T0)
    with store.on("mbp"):
        save_recipes(D, [_recipe(last_ok_at=T0)])
    with store.on("mini"):
        (r,) = load_recipes(D)
        assert not r.stale
    assert store.file_rows("mini", D) == store.file_rows("mbp", D), "both caches hold the same copy"

    clock.now = T0 + 60
    with store.on("mbp"):
        mark_stale(D, r, "http_401")
    with store.on("mini"):
        (seen,) = load_recipes(D)

    assert seen.stale and seen.stale_reason == "http_401"
    assert store.file_rows("mini", D)[0]["stale"] is True, "the mini's cache was re-materialized"
    assert store.db_row(D)["stale_count"] == 1


def test_an_unstale_from_a_replay_reaches_the_other_host(store, monkeypatch):
    clock = store.freeze_clock(T0)
    with store.on("mbp"):
        save_recipes(D, [_recipe(stale=True, stale_reason="http_401")])
    recipe_store.set_scan_hints(D, {"recipe_status": "stale:401:2027-01-15T08:00:00+00:00"})
    with store.on("mini"):
        assert load_recipes(D)[0].stale

    clock.now = T0 + 60
    _serve_pages(monkeypatch)
    with store.on("mbp"):
        hit = _replay()
    assert hit is not None and hit[1] == 45

    with store.on("mini"):
        (seen,) = load_recipes(D)
    assert not seen.stale and seen.stale_reason == ""
    assert seen.last_ok_at == T0 + 60
    assert store.file_rows("mini", D)[0]["stale"] is False
    assert recipe_store.get_scan_hints(D)["recipe_status"] == "ok"


def test_a_coverage_update_from_a_replay_reaches_the_other_host(store, monkeypatch):
    clock = store.freeze_clock(T0)
    thin = {"price": 0.1, "trim": 0.0, "exterior_color": 0.0}
    with store.on("mbp"):
        save_recipes(D, [_recipe(last_ok_at=T0 - 86_400, field_coverage=thin)])
    with store.on("mini"):
        assert load_recipes(D)[0].field_coverage == thin

    clock.now = T0 + 60
    _serve_pages(monkeypatch)
    with store.on("mbp"):
        assert _replay() is not None
        (mine,) = load_recipes(D)
    assert mine.field_coverage != thin and mine.field_coverage["price"] == 1.0

    with store.on("mini"):
        (seen,) = load_recipes(D)
    assert seen.field_coverage == mine.field_coverage
    assert seen.last_ok_at == T0 + 60
    assert store.file_rows("mini", D) == store.file_rows("mbp", D)


def _seed_old_set_everywhere(store) -> list[dict]:
    old = [asdict(_recipe(url=OLD_URL, saved_at=OLD, last_ok_at=OLD, vehicle_rows=12))]
    store.write_db(D, old)
    for host in store.hosts:
        store.write_file(host, D, old)
    return old


def _stub_synthesis(monkeypatch, recipes: list[EndpointRecipe]) -> None:
    """ensure_recipe's network half: homepage, fingerprint, synthesis, validation, gate."""
    from backend.scanner import dealer_place, recipe_synth, recipe_validation

    monkeypatch.setattr(recipe_synth, "fetch_dealer_html", lambda url: "<html>carscommerce</html>")
    monkeypatch.setattr(recipe_synth, "fingerprint_platform", lambda html, url: "carscommerce")
    monkeypatch.setattr(recipe_synth, "synthesize_recipes", lambda did, url, html, platform: list(recipes))
    monkeypatch.setattr(recipe_synth, "validate_recipe", lambda *a, **k: 45)

    def no_place(*a, **k):
        raise RuntimeError("no place lookup in this test")

    monkeypatch.setattr(dealer_place, "learn_place", no_place)
    monkeypatch.setattr(
        recipe_validation, "gate_recipes",
        lambda did, kept, **k: (kept, SimpleNamespace(status="ok", summary=lambda: {"verdict": "ok"})),
    )


def test_a_synthesized_set_with_zero_saved_at_is_not_reverted_by_an_older_cache(store, monkeypatch):
    """ensure_recipe(force) saves synthesized recipes built with saved_at=0. The mini's
    older cache used to win that comparison and push its old set back up."""
    from backend.scanner.pipeline.recipes import ensure_recipe

    _seed_old_set_everywhere(store)
    synthesized = _recipe(saved_at=0.0, provider_hint="carscommerce")
    _stub_synthesis(monkeypatch, [synthesized])
    store.freeze_clock(T0)

    with store.on("mbp"):
        info = ensure_recipe({"dealer_id": D, "url": SITE, "name": "Freshness Dealer"}, force=True)
    assert info["synth"] == "saved_1"

    with store.on("mini"):
        (seen,) = load_recipes(D)
    assert seen.url == CC_URL, "the mini adopted the synthesized set"
    assert store.file_rows("mini", D)[0]["url"] == CC_URL
    row = store.db_row(D)
    assert [r["url"] for r in row["rows"]] == [CC_URL], "the older cache did not push its set back up"
    assert row["max_saved_at"] == T0


def test_a_cascaded_recipe_with_zero_saved_at_is_not_reverted_by_an_older_cache(store):
    """cascade_recipes saves ``adapt_recipe`` output, which carries saved_at=0."""
    _seed_old_set_everywhere(store)
    donor = _recipe(dealer_id="donor-dealer-com", url="https://www.donor-dealer.com/api/inventory/search",
                    saved_at=OLD, last_ok_at=OLD)
    cascaded = adapt_recipe(donor, D, SITE)
    assert cascaded is not None and cascaded.saved_at == 0.0
    store.freeze_clock(T0)

    with store.on("mbp"):
        save_recipes(D, [cascaded])
    with store.on("mini"):
        (seen,) = load_recipes(D)

    assert seen.url == "https://www.freshness-dealer.com/api/inventory/search"
    assert store.db_row(D)["rows"][0]["url"] == seen.url
    assert store.db_row(D)["max_saved_at"] == T0


# ── reconcile directions: neither re-stamps; only the push-up warns ─────────────

def test_push_up_logs_a_warning_and_keeps_the_file_stamp(store, caplog):
    db_copy = [
        asdict(_recipe(url=OLD_URL, saved_at=1000.0)),
        asdict(_recipe(url=OLD_URL, post_template='{"page":1,"type":"used"}', saved_at=900.0)),
    ]
    store.write_db(D, db_copy)
    file_copy = [asdict(_recipe(saved_at=2000.0))]
    path = store.write_file("mbp", D, file_copy)
    before = path.read_bytes()
    clock = store.freeze_clock(T0)

    with caplog.at_level(logging.WARNING, logger="scanner"):
        (loaded,) = load_recipes(D)

    assert loaded.url == CC_URL
    (warning,) = _push_up_warnings(caplog)
    msg = warning.getMessage()
    assert D in msg
    assert "overwrote the DB copy of 2 recipe(s)" in msg
    assert "pushed 1 file recipe(s)" in msg
    assert clock.calls == 0, "a push-up never stamps"
    assert path.read_bytes() == before, "the file is not rewritten"
    row = store.db_row(D)
    assert row["rows"] == file_copy
    assert row["max_saved_at"] == 2000.0


def test_a_failed_write_through_is_pushed_up_on_the_next_load(store, monkeypatch, caplog):
    """The realistic push-up: a save whose write-through failed leaves the file newer
    than the DB; the next load pushes it, and the other host then sees the write."""
    clock = store.freeze_clock(T0)
    save_recipes(D, [_recipe()])
    (r,) = load_recipes(D)

    clock.now = T0 + 60

    def unreachable():
        raise sqlite3.OperationalError("store unreachable")

    monkeypatch.setattr(recipe_store, "_save_failure_warned", set())
    monkeypatch.setattr(recipe_store, "_conn", unreachable)
    mark_stale(D, r, "http_403")
    monkeypatch.setattr(recipe_store, "_conn", store._connect)
    assert store.db_row(D)["stale_count"] == 0, "the write-through failed"

    with caplog.at_level(logging.WARNING, logger="scanner"):
        assert load_recipes(D)[0].stale
    (warning,) = _push_up_warnings(caplog)
    assert D in warning.getMessage() and "overwrote the DB copy of 1 recipe(s)" in warning.getMessage()
    row = store.db_row(D)
    assert row["stale_count"] == 1 and row["max_saved_at"] == T0 + 60

    with store.on("mini"):
        assert load_recipes(D)[0].stale


def test_push_up_of_a_dealer_with_no_db_row_says_so(store, caplog):
    store.write_file("mbp", D, [asdict(_recipe(saved_at=2000.0))])
    with caplog.at_level(logging.WARNING, logger="scanner"):
        load_recipes(D)
    (warning,) = _push_up_warnings(caplog)
    assert D in warning.getMessage() and "no DB copy was read" in warning.getMessage()
    assert store.db_row(D)["max_saved_at"] == 2000.0


def test_adopting_the_db_copy_keeps_its_stamp_and_does_not_warn(store, caplog):
    store.write_file("mini", D, [asdict(_recipe(url=OLD_URL, saved_at=1000.0))])
    db_copy = [asdict(_recipe(saved_at=2000.0))]
    store.write_db(D, db_copy)
    clock = store.freeze_clock(T0)

    with store.on("mini"), caplog.at_level(logging.WARNING, logger="scanner"):
        (seen,) = load_recipes(D)

    assert seen.url == CC_URL
    assert store.file_rows("mini", D) == db_copy, "adopted verbatim, saved_at 2000 kept"
    assert clock.calls == 0
    assert not _push_up_warnings(caplog)


def test_equal_stamps_write_nothing_and_do_not_warn(store, caplog):
    store.freeze_clock(T0)
    save_recipes(D, [_recipe()])
    path = store.file_path("mbp", D)
    mtime = path.stat().st_mtime_ns
    updated_at = store.db_row(D)["updated_at"]

    with caplog.at_level(logging.WARNING, logger="scanner"):
        assert load_recipes(D)[0].url == CC_URL

    assert not _push_up_warnings(caplog)
    assert path.stat().st_mtime_ns == mtime
    assert store.db_row(D)["updated_at"] == updated_at


def test_no_push_up_warning_when_the_store_is_switched_off(store, monkeypatch, caplog):
    """RECIPES_DB_DISABLED=1: every load sees a file newer than the (absent) DB copy,
    but nothing is pushed, so nothing is logged."""
    store.write_file("mbp", D, [asdict(_recipe(saved_at=2000.0))])
    monkeypatch.setenv("RECIPES_DB_DISABLED", "1")
    with caplog.at_level(logging.WARNING, logger="scanner"):
        assert load_recipes(D)[0].url == CC_URL
    assert not _push_up_warnings(caplog)
    assert store.db_row(D) is None


# ── the harness itself ─────────────────────────────────────────────────────────

def test_harness_store_is_private_to_the_test(store, tmp_path):
    save_recipes(D, [_recipe()])
    assert store.connections > 0, "the store was really used"
    assert _is_under(store.db_path, tmp_path)
    assert all(_is_under(d, tmp_path) for d in store.dirs.values())
    assert len(set(store.dirs.values())) == 2
    assert rec.RECIPES_DIR == store.dirs["mbp"]

    session_db = session_inventory_db_path()
    assert session_db is not None and not _is_under(session_db, tmp_path)
    if session_db.exists():
        with closing(sqlite3.connect(f"file:{session_db}?mode=ro", uri=True)) as conn:
            try:
                n = conn.execute("SELECT count(*) FROM dealer_recipes WHERE dealer_id = ?", (D,)).fetchone()[0]
            except sqlite3.OperationalError:  # no dealer_recipes table in the session DB
                n = 0
        assert n == 0, "the harness store never writes the session DB"


def test_harness_refuses_the_session_db(store):
    session_db = session_inventory_db_path()
    assert session_db is not None
    store.db_path = session_db
    with pytest.raises(AssertionError):
        store._connect()
    assert store.connections == 0
