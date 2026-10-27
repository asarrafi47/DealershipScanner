"""N13 P7.2: store-authoritative ``load_recipes`` and the ``recipe_cache`` mechanics, ``pg`` tier.

The ``dealer_recipes`` row is the authority; the cache file mirrors it and is rewritten only
when its content differs. Real Postgres through ``pg_db`` (no sqlite3), RECIPES_DIR under
``tmp_path``.
"""

from __future__ import annotations

import json
import os
import threading
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
