"""N13 P7.2 / P7.3: store-authoritative ``load_recipes``, the ``recipe_cache`` mechanics and the
key-scoped mutation API with its offline op-log, ``pg`` tier.

The ``dealer_recipes`` row is the authority; the cache file mirrors it and is rewritten only
when its content differs. Real Postgres through ``pg_db`` (no sqlite3), RECIPES_DIR under
``tmp_path``.
"""

from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import asdict
from pathlib import Path

import pytest

import backend.scanner.recipe_cache as rcache
import backend.scanner.recipe_store as rs
import backend.scanner.recipes as rec
from backend.scanner.recipes import EndpointRecipe, load_recipes

pytestmark = pytest.mark.pg

D = "sync-dealer-com"


@pytest.fixture()
def env(pg_db, tmp_path, monkeypatch):
    monkeypatch.setenv("RECIPES_DB_DISABLED", "")
    monkeypatch.setattr(rec, "RECIPES_DIR", tmp_path / "ws" / "recipes")
    monkeypatch.setattr(rec, "_foreign_unread", set())
    monkeypatch.setattr(rec, "_foreign_warned", set())
    (tmp_path / "ws" / "recipes").mkdir(parents=True)
    return tmp_path / "ws" / "recipes"


def _row(url: str, saved_at: float = 100.0, dealer: str = D, **kw) -> dict:
    return asdict(EndpointRecipe(dealer_id=dealer, url=url, method="GET", content_type="application/json",
                                 post_template=None, saved_at=saved_at, **kw))


def _file(cache: Path, dealer: str = D) -> Path:
    return cache / f"{rec._recipe_slug(dealer)}.json"


def _backups(cache: Path) -> list[Path]:
    return sorted((cache.parent / "backups" / rcache.BACKUP_SUBDIR).glob("*.json"))


def test_authoritative_row_wins_over_a_newer_file_and_refreshes_the_cache(env) -> None:
    assert rs.db_save_recipes(D, [_row("https://a.example/db", 100.0)])
    _file(env).write_text(json.dumps([_row("https://a.example/file", 999.0)]))
    (r,) = load_recipes(D)
    assert r.url == "https://a.example/db"
    assert json.loads(_file(env).read_text()) == [_row("https://a.example/db", 100.0)]
    assert rs.db_read(D).rows[0]["url"] == "https://a.example/db", "the store was not overwritten"


@pytest.mark.parametrize("content", ["{not json", '[{"url": "https://a.ex', "", '{"a": 1}'])
def test_corrupt_or_truncated_cache_yields_the_db_rows_and_is_rewritten(env, content) -> None:
    rows = [_row("https://a.example/db")]
    assert rs.db_save_recipes(D, rows)
    _file(env).write_text(content)
    assert [r.url for r in load_recipes(D)] == ["https://a.example/db"]
    assert json.loads(_file(env).read_text()) == rows


def test_missing_cache_is_created_without_a_backup(env) -> None:
    rows = [_row("https://a.example/db")]
    assert rs.db_save_recipes(D, rows)
    load_recipes(D)
    assert json.loads(_file(env).read_text()) == rows and _backups(env) == []


def test_equal_cache_is_not_rewritten(env, monkeypatch) -> None:
    rows = [_row("https://a.example/db")]
    assert rs.db_save_recipes(D, rows)
    load_recipes(D)
    calls: list[Path] = []
    monkeypatch.setattr(rec, "_atomic_write_json", lambda p, o: calls.append(p))
    load_recipes(D)
    load_recipes(D)
    assert calls == []


def test_differing_cache_is_backed_up_until_the_store_is_reconciled(env) -> None:
    assert rs.db_save_recipes(D, [_row("https://a.example/db")])
    old = json.dumps([_row("https://a.example/file", 5.0)])
    _file(env).write_text(old)
    load_recipes(D)
    (backup,) = _backups(env)
    assert backup.read_text() == old
    load_recipes(D)
    assert len(_backups(env)) == 1, "once rewritten the cache matches: no second backup"

    # With the marker for this store's fingerprint the differing cache is simply replaced.
    (env / rcache.RECONCILED_MARKER).write_text(json.dumps({"fingerprint": rec.store_fingerprint()}))
    _file(env).write_text(old)
    load_recipes(D)
    assert len(_backups(env)) == 1
    assert json.loads(_file(env).read_text())[0]["url"] == "https://a.example/db"


def test_marker_for_another_store_does_not_count_as_reconciled(env) -> None:
    assert rs.db_save_recipes(D, [_row("https://a.example/db")])
    (env / rcache.RECONCILED_MARKER).write_text(json.dumps({"fingerprint": "someone-else"}))
    _file(env).write_text(json.dumps([_row("https://a.example/file", 5.0)]))
    load_recipes(D)
    assert len(_backups(env)) == 1


def test_failed_backup_leaves_the_cache_alone_but_serves_the_store(env, monkeypatch) -> None:
    assert rs.db_save_recipes(D, [_row("https://a.example/db")])
    old = json.dumps([_row("https://a.example/file", 5.0)])
    _file(env).write_text(old)

    def boom(_p):
        raise OSError("disk full")

    monkeypatch.setattr(rcache, "backup_cache_file", boom)
    assert [r.url for r in load_recipes(D)] == ["https://a.example/db"]
    assert _file(env).read_text() == old


def test_hints_only_row_does_not_hide_alias_recipes(env) -> None:
    old_slug = "old-dealer-com"
    (env / rec.ALIASES_FILENAME).write_text(json.dumps({old_slug: D}))
    assert rs.db_save_recipes(old_slug, [_row("https://a.example/old", dealer=old_slug)])
    assert rs.set_scan_hints(D, {"notes": "hints first"})
    got = rs.db_read(D)
    assert got.authoritative and got.rows[0]["url"] == "https://a.example/old"
    assert rs.db_load_recipes(D)[0][0]["url"] == "https://a.example/old"
    (r,) = load_recipes(D)
    assert r.url == "https://a.example/old"
    assert json.loads(_file(env).read_text())[0]["url"] == "https://a.example/old", "cached under the current slug"
    assert rs.get_scan_hints(D) == {"notes": "hints first"}


def test_hints_only_row_without_alias_is_still_returned_unauthoritative(env) -> None:
    assert rs.set_scan_hints(D, {"notes": "x"})
    got = rs.db_read(D)
    assert got is not None and not got.authoritative
    assert rs.db_load_recipes(D) == ([], 0.0)


def test_cache_file_seeds_a_store_with_no_row_and_keeps_its_stamp(env) -> None:
    rows = [_row("https://a.example/file", 77.0)]
    _file(env).write_text(json.dumps(rows))
    (r,) = load_recipes(D)
    assert r.url == "https://a.example/file"
    got = rs.db_read(D)
    assert got.authoritative and got.rows == rows and got.max_saved_at == 77.0


def test_cache_file_seeds_a_hints_only_row_and_keeps_its_hints(env) -> None:
    assert rs.set_scan_hints(D, {"notes": "keep"})
    rows = [_row("https://a.example/file", 77.0)]
    _file(env).write_text(json.dumps(rows))
    load_recipes(D)
    got = rs.db_read(D)
    assert got.authoritative and got.rows == rows and got.hints == {"notes": "keep"}


def test_seed_never_replaces_a_row_that_became_authoritative_meanwhile(env, monkeypatch) -> None:
    rows = [_row("https://a.example/file", 77.0)]
    _file(env).write_text(json.dumps(rows))
    real = rs.db_read
    monkeypatch.setattr(rs, "db_read", lambda *a, **k: None)  # this load saw no row ...
    assert rs.db_save_recipes(D, [_row("https://a.example/other-host", 50.0)])  # ... but one landed
    (r,) = load_recipes(D)
    assert r.url == "https://a.example/file", "this load serves the file it read"
    assert real(D).rows[0]["url"] == "https://a.example/other-host", "the seed did not overwrite it"


def test_unreachable_store_serves_the_file_rows(env, monkeypatch) -> None:
    rows = [_row("https://a.example/file", 77.0)]
    _file(env).write_text(json.dumps(rows))

    def unreachable():
        raise OSError("store down")

    monkeypatch.setattr(rs, "_conn", unreachable)
    assert [r.url for r in load_recipes(D)] == ["https://a.example/file"]
    assert json.loads(_file(env).read_text()) == rows, "the cache is untouched"


def test_store_disabled_serves_the_file_rows(env, monkeypatch) -> None:
    rows = [_row("https://a.example/file", 77.0)]
    _file(env).write_text(json.dumps(rows))
    monkeypatch.setenv("RECIPES_DB_DISABLED", "1")
    assert [r.url for r in load_recipes(D)] == ["https://a.example/file"]


def test_save_recipes_holds_the_file_lock_and_leaves_only_a_lock_beside_the_cache(env) -> None:
    rec.save_recipes(D, [EndpointRecipe(dealer_id=D, url="https://a.example/x", method="GET",
                                        content_type="application/json", post_template=None)])
    names = sorted(p.name for p in env.iterdir())
    assert names == [f"{D}.json", f"{D}.json.lock"]
    assert [p.name for p in env.glob("*.json")] == [f"{D}.json"], "the lock is invisible to *.json globs"


def test_cache_lock_serializes_threads(env) -> None:
    path = _file(env)
    inside = 0
    worst = 0
    guard = threading.Lock()

    def work() -> None:
        nonlocal inside, worst
        for _ in range(20):
            with rcache.cache_lock(path):
                with guard:
                    inside += 1
                    worst = max(worst, inside)
                with guard:
                    inside -= 1

    ts = [threading.Thread(target=work) for _ in range(6)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert worst == 1


def test_atomic_write_json_failure_leaves_the_old_bytes(env, monkeypatch) -> None:
    path = _file(env)
    rcache.atomic_write_json(path, [{"a": 1}])
    before = path.read_bytes()

    def boom(*_a):
        raise OSError("replace failed")

    monkeypatch.setattr(os, "replace", boom)
    with pytest.raises(OSError):
        rcache.atomic_write_json(path, [{"a": 2}])
    assert path.read_bytes() == before
    assert [p.name for p in env.iterdir() if ".tmp." in p.name] == []


# ── P7.3: key-scoped mutation API and offline op-log ─────────────────────────


def _rec(url: str, dealer: str = D, **kw) -> EndpointRecipe:
    return EndpointRecipe(dealer_id=dealer, url=url, method="GET", content_type="application/json",
                          post_template=None, **kw)


def _by_url(rows: list[dict]) -> dict[str, dict]:
    return {r["url"]: r for r in rows}


def _host(monkeypatch, tmp_path: Path, name: str) -> Path:
    """Point this process at another host's cache dir (the store stays shared)."""
    d = tmp_path / name / "recipes"
    d.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(rec, "RECIPES_DIR", d)
    return d


def _log_file(cache: Path, dealer: str = D) -> Path:
    return rcache.oplog_path(cache, rec.store_fingerprint(), rec._recipe_slug(dealer))


def _store_down(monkeypatch):
    def unreachable():
        raise OSError("store down")

    real = rs._conn
    monkeypatch.setattr(rs, "_conn", unreachable)
    return lambda: monkeypatch.setattr(rs, "_conn", real)


def test_mark_stale_by_one_host_is_seen_by_another(env, monkeypatch, tmp_path) -> None:
    rec.save_recipes(D, [_rec("https://a.example/x"), _rec("https://a.example/y")])
    rx = [r for r in load_recipes(D) if r.url.endswith("/x")][0]
    rec.mark_stale(D, rx, "403 seen")
    _host(monkeypatch, tmp_path, "hostb")
    got = {r.url: r for r in load_recipes(D)}
    assert got["https://a.example/x"].stale and got["https://a.example/x"].stale_reason == "403 seen"
    assert not got["https://a.example/y"].stale


def test_ops_stamp_mutated_at_and_saved_at_on_their_key_only(env) -> None:
    rec.save_recipes(D, [_rec("https://a.example/x"), _rec("https://a.example/y")])
    before = _by_url(rs.db_read(D).rows)
    rec.mark_stale(D, _rec("https://a.example/x"), "r")
    after = _by_url(rs.db_read(D).rows)
    assert after["https://a.example/x"]["mutated_at"] > before["https://a.example/x"]["mutated_at"]
    assert after["https://a.example/x"]["saved_at"] == after["https://a.example/x"]["mutated_at"]
    assert after["https://a.example/y"] == before["https://a.example/y"]
    assert json.loads(_file(env).read_text()) == rs.db_read(D).rows, "the cache follows the store"


def test_mark_stale_of_a_removed_key_does_nothing(env) -> None:
    rec.save_recipes(D, [_rec("https://a.example/y")])
    rev = rs.db_read(D).rev
    rec.mark_stale(D, _rec("https://a.example/gone"), "r")
    got = rs.db_read(D)
    assert got.rev == rev and [r["url"] for r in got.rows] == ["https://a.example/y"]


def test_mark_stale_older_than_another_hosts_success_is_ignored(env) -> None:
    rec.save_recipes(D, [_rec("https://a.example/x", last_ok_at=200.0)])
    rec.mark_stale(D, _rec("https://a.example/x"), "403", observed_at=100.0)
    assert not rs.db_read(D).rows[0]["stale"]
    rec.mark_stale(D, _rec("https://a.example/x"), "403", observed_at=300.0)
    assert rs.db_read(D).rows[0]["stale"]


def test_record_replay_ok_updates_its_key_and_the_learned_total_only(env) -> None:
    x, y = _rec("https://a.example/x", stale=True, stale_reason="403", total_count=10), _rec("https://a.example/y", total_count=99)
    rec.save_recipes(D, [x, y])
    rec.record_replay_ok(D, x.key(), last_ok_at=500.0, field_coverage={"price": 0.9}, unstale=True, total_count=45)
    got = _by_url(rs.db_read(D).rows)
    gx, gy = got["https://a.example/x"], got["https://a.example/y"]
    assert (gx["last_ok_at"], gx["field_coverage"], gx["stale"], gx["stale_reason"], gx["total_count"]) == (
        500.0, {"price": 0.9}, False, "", 45)
    assert gy["total_count"] == 99 and gy["last_ok_at"] == 0.0
    rec.record_replay_ok(D, x.key(), last_ok_at=400.0, field_coverage={}, unstale=False)
    assert _by_url(rs.db_read(D).rows)["https://a.example/x"]["last_ok_at"] == 500.0, "never moved back"


def test_concurrent_mutations_on_different_keys_both_persist(env) -> None:
    x, y = _rec("https://a.example/x"), _rec("https://a.example/y")
    rec.save_recipes(D, [x, y])
    barrier = threading.Barrier(2)

    def stale_x():
        barrier.wait()
        for _ in range(10):
            rec.mark_stale(D, x, "403")

    def ok_y():
        barrier.wait()
        for i in range(10):
            rec.record_replay_ok(D, y.key(), last_ok_at=1000.0 + i, field_coverage={"price": 1.0}, unstale=False)

    ts = [threading.Thread(target=stale_x), threading.Thread(target=ok_y)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    got = _by_url(rs.db_read(D).rows)
    assert got["https://a.example/x"]["stale"] is True
    assert got["https://a.example/y"]["last_ok_at"] == 1009.0 and not got["https://a.example/y"]["stale"]


def test_replaced_keys_are_not_brought_back_by_an_older_cache(env) -> None:
    rec.save_recipes(D, [_rec("https://a.example/x"), _rec("https://a.example/old")])
    stale_cache = _file(env).read_text()
    rec.save_recipes(D, [_rec("https://a.example/x")])  # another host replaced the set
    _file(env).write_text(stale_cache)  # this host still has the older cache
    assert [r.url for r in load_recipes(D)] == ["https://a.example/x"]
    rec.mark_stale(D, _rec("https://a.example/x"), "r")
    assert [r["url"] for r in rs.db_read(D).rows] == ["https://a.example/x"]
    assert [r["url"] for r in json.loads(_file(env).read_text())] == ["https://a.example/x"]


def test_offline_mutation_goes_to_the_op_log_and_replays_when_the_store_is_back(env, monkeypatch, tmp_path) -> None:
    rec.save_recipes(D, [_rec("https://a.example/x"), _rec("https://a.example/y")])
    up = _store_down(monkeypatch)
    rec.mark_stale(D, _rec("https://a.example/x"), "403 offline", observed_at=50.0)
    log = _log_file(env)
    (entry,) = rcache.oplog_read(log)
    assert (entry["op"], entry["key"], entry["observed_at"], entry["payload"]) == (
        "mark_stale", ["GET", "a.example/x"], 50.0, {"reason": "403 offline"})
    assert "base_rev" in entry
    cached = _by_url(json.loads(_file(env).read_text()))
    assert cached["https://a.example/x"]["stale"], "applied to the cache file meanwhile"
    assert [r.stale for r in load_recipes(D) if r.url.endswith("/x")] == [True], "served from the file"
    assert log.exists(), "still pending while the store is down"

    # another host changes y while this one is offline; the replay must not undo it
    up()
    rec.record_replay_ok(D, _rec("https://a.example/y").key(), last_ok_at=700.0, field_coverage={}, unstale=False)
    got = {r.url: r for r in load_recipes(D)}
    assert got["https://a.example/x"].stale and got["https://a.example/y"].last_ok_at == 700.0
    assert _by_url(rs.db_read(D).rows)["https://a.example/x"]["stale"]
    assert not log.exists(), "the log is removed once replayed"


def test_offline_replay_keeps_order_and_stops_at_the_first_unreachable_entry(env, monkeypatch) -> None:
    rec.save_recipes(D, [_rec("https://a.example/x")])
    key = _rec("https://a.example/x").key()
    up = _store_down(monkeypatch)
    rec.mark_stale(D, _rec("https://a.example/x"), "403")
    rec.record_replay_ok(D, key, last_ok_at=900.0, field_coverage={"price": 1.0}, unstale=True)
    assert [e["op"] for e in rcache.oplog_read(_log_file(env))] == ["mark_stale", "record_replay_ok"]
    load_recipes(D)  # still down: nothing is consumed
    assert len(rcache.oplog_read(_log_file(env))) == 2
    up()
    load_recipes(D)
    row = rs.db_read(D).rows[0]
    assert row["stale"] is False and row["last_ok_at"] == 900.0, "replayed in order: stale, then un-stale"


def test_a_pending_op_never_re_adds_a_key_removed_since_it_was_recorded(env, monkeypatch) -> None:
    rec.save_recipes(D, [_rec("https://a.example/x"), _rec("https://a.example/y")])
    load_recipes(D)  # the cache holds x and y
    up = _store_down(monkeypatch)
    rec.merge_recipes(D, [_rec("https://a.example/y", vehicle_rows=50), _rec("https://a.example/new", vehicle_rows=9)])
    (entry,) = rcache.oplog_read(_log_file(env))
    assert sorted(map(tuple, entry["base_keys"])) == [("GET", "a.example/x"), ("GET", "a.example/y")]
    up()
    rec.save_recipes(D, [_rec("https://a.example/x")])  # another host replaced the set without y
    got = sorted(r.url for r in load_recipes(D))
    assert got == ["https://a.example/new", "https://a.example/x"], "y stays gone; the never-seen key is added"


def test_a_pending_op_never_un_supersedes(env, monkeypatch) -> None:
    rec.save_recipes(D, [_rec("https://a.example/y")])
    _file(env).unlink()  # no base keys: only the supersede guard can stop the re-add
    up = _store_down(monkeypatch)
    rec.merge_recipes(D, [_rec("https://a.example/y", vehicle_rows=50)])
    assert rcache.oplog_read(_log_file(env))[0]["base_keys"] == []
    up()
    conn = rs._conn()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE dealer_recipes SET recipes_json='[]', rev=rev+1, superseded_json=? WHERE dealer_id=?",
                    (json.dumps([{"key": ["GET", "a.example/y"], "recipe": {}, "superseded_at": time.time() + 5,
                                  "reason": "synth:force"}]), D))
        conn.commit()
    finally:
        conn.close()
    assert load_recipes(D) == []
    assert rs.db_read(D).rows == []
    assert not _log_file(env).exists()


def test_an_op_log_recorded_against_another_store_is_never_replayed_here(env) -> None:
    rec.save_recipes(D, [_rec("https://a.example/x")])
    other = rcache.oplog_path(env, "f" * 64, rec._recipe_slug(D))
    rcache.oplog_append(other, {"op": "mark_stale", "key": ["GET", "a.example/x"], "observed_at": None,
                                "payload": {"reason": "from elsewhere"}, "at": time.time(), "base_rev": 0})
    load_recipes(D)
    assert not rs.db_read(D).rows[0]["stale"]
    assert other.exists() and len(rcache.oplog_read(other)) == 1


def test_save_recipes_offline_is_logged_as_a_replace_and_skipped_when_the_store_moved_on(env, monkeypatch) -> None:
    rec.save_recipes(D, [_rec("https://a.example/x")])
    up = _store_down(monkeypatch)
    rec.save_recipes(D, [_rec("https://a.example/offline")])
    (entry,) = rcache.oplog_read(_log_file(env))
    assert entry["op"] == "replace_recipes" and entry["payload"]["rows"][0]["url"] == "https://a.example/offline"
    up()
    rec.mark_stale(D, _rec("https://a.example/x"), "other host moved the store on")
    load_recipes(D)
    assert [r["url"] for r in rs.db_read(D).rows] == ["https://a.example/x"], "a snapshot is never merged over it"
    assert not _log_file(env).exists()


def test_merge_recipes_applies_promotions_rules_inside_the_cas(env) -> None:
    rec.save_recipes(D, [_rec("https://a.example/a", vehicle_rows=30, last_ok_at=5.0),
                         _rec("https://a.example/b", vehicle_rows=5, stale=True)])
    out = rec.merge_recipes(D, [_rec("https://a.example/a", vehicle_rows=10),   # smaller than a live recipe: loses
                                _rec("https://a.example/b", vehicle_rows=2),    # replaces the stale one
                                _rec("https://a.example/c", vehicle_rows=7)], max_recipes=2)
    got = _by_url(rs.db_read(D).rows)
    assert [r.url for r in out] == [r["url"] for r in rs.db_read(D).rows]
    assert len(got) == 2 and "https://a.example/a" in got
    assert got["https://a.example/a"]["vehicle_rows"] == 30
    assert all(r["mutated_at"] and r["saved_at"] for r in got.values())


def test_a_recipe_another_host_adds_during_a_merge_survives(env, monkeypatch) -> None:
    rec.save_recipes(D, [_rec("https://a.example/a")])
    real = rs._cas_write
    fired: list[int] = []

    def racing(cur, dealer_id, current, cols):
        if not fired:
            fired.append(1)
            rs.db_mutate(D, lambda c: c.rows + [asdict(_rec("https://a.example/theirs", saved_at=9.0))])
        return real(cur, dealer_id, current, cols)

    monkeypatch.setattr(rs, "_cas_write", racing)
    rec.merge_recipes(D, [_rec("https://a.example/mine", vehicle_rows=4)])
    assert sorted(r["url"] for r in rs.db_read(D).rows) == [
        "https://a.example/a", "https://a.example/mine", "https://a.example/theirs"]


def test_store_disabled_ops_edit_the_cache_file_and_log_nothing(env, monkeypatch) -> None:
    monkeypatch.setenv("RECIPES_DB_DISABLED", "1")
    rec.save_recipes(D, [_rec("https://a.example/x")])
    rec.mark_stale(D, _rec("https://a.example/x"), "403")
    assert json.loads(_file(env).read_text())[0]["stale"] is True
    assert not (env / rcache.PENDING_DIR).exists()
