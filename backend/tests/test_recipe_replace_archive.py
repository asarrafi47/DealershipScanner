"""N13 P7.5: replace-with-archive (``replace_recipes`` into ``superseded_json``), the restore
operator command, ensure_recipe's reasons, cascade/synth merging, and discovery.md carrying
no URL query strings. Real Postgres through ``pg_db`` (no sqlite3), RECIPES_DIR under tmp_path.
"""

from __future__ import annotations

import time
from types import SimpleNamespace

import pytest

import backend.scanner.recipe_store as rs
import backend.scanner.recipes as rec
from backend.scanner.recipes import EndpointRecipe, load_recipes

pytestmark = pytest.mark.pg

D = "archive-dealer-com"


@pytest.fixture()
def env(pg_db, tmp_path, monkeypatch):
    monkeypatch.setenv("RECIPES_DB_DISABLED", "")
    monkeypatch.setattr(rec, "RECIPES_DIR", tmp_path / "ws" / "recipes")
    monkeypatch.setattr(rec, "_foreign_unread", set())
    monkeypatch.setattr(rec, "_foreign_warned", set())
    (tmp_path / "ws" / "recipes").mkdir(parents=True)
    return tmp_path


def _rec(url: str, **kw) -> EndpointRecipe:
    return EndpointRecipe(dealer_id=D, url=url, method="GET", content_type="application/json",
                          post_template=None, **kw)


def test_forced_resynthesis_archives_the_captured_recipe_with_the_same_key(env) -> None:
    rec.save_recipes(D, [_rec("https://a.example/inv", vehicle_rows=40, last_ok_at=5.0)])
    live = rec.replace_recipes(D, [_rec("https://a.example/inv", vehicle_rows=90)], reason="synth:force")
    assert [r.vehicle_rows for r in live] == [90]
    row = rs.db_read(D)
    assert [r["vehicle_rows"] for r in row.rows] == [90]
    (arch,) = row.superseded
    assert arch["key"] == ["GET", "a.example/inv"] and arch["reason"] == "synth:force"
    assert arch["recipe"]["vehicle_rows"] == 40 and arch["superseded_at"] > 0
    assert [r.vehicle_rows for r in load_recipes(D)] == [90]


def test_archived_recipes_are_invisible_to_loaders_that_read_recipes_json(env) -> None:
    rec.save_recipes(D, [_rec("https://a.example/old", vehicle_rows=40)])
    rec.replace_recipes(D, [_rec("https://a.example/new", vehicle_rows=50)], reason="synth:force")
    # A simulated old-code loader: the store's recipe list and the cache file only.
    assert [r["url"] for r in rs.db_load_recipes(D)[0]] == ["https://a.example/new"]
    assert all(r.url == "https://a.example/new" for r in load_recipes(D))
    assert rs.db_read(D).superseded[0]["recipe"]["url"] == "https://a.example/old"


def test_recipes_carried_over_unchanged_are_not_archived(env) -> None:
    rec.save_recipes(D, [_rec("https://a.example/keep", vehicle_rows=7), _rec("https://a.example/old", vehicle_rows=3)])
    kept = load_recipes(D)[0]
    rec.replace_recipes(D, [kept, _rec("https://a.example/new", vehicle_rows=9)], reason="synth:cc_section_scoped")
    assert {a["recipe"]["url"] for a in rs.db_read(D).superseded} == {"https://a.example/old"}


def test_restore_brings_it_back_and_archives_the_live_copy(env) -> None:
    rec.save_recipes(D, [_rec("https://a.example/inv", vehicle_rows=40)])
    rec.replace_recipes(D, [_rec("https://a.example/inv", vehicle_rows=90)], reason="synth:force")
    assert rec.restore_superseded(D, ("GET", "a.example/inv"))
    assert [r.vehicle_rows for r in load_recipes(D)] == [40]
    (arch,) = rs.db_read(D).superseded
    assert arch["recipe"]["vehicle_rows"] == 90 and arch["reason"] == "restore"
    assert not rec.restore_superseded(D, ("GET", "a.example/none"))


def test_entries_older_than_the_keep_window_are_pruned_on_the_next_replace(env) -> None:
    rec.save_recipes(D, [_rec("https://a.example/a")])
    rec.replace_recipes(D, [_rec("https://a.example/b")], reason="r1")
    old = rs.db_read(D).superseded
    old[0]["superseded_at"] = time.time() - (rec.SUPERSEDED_KEEP_DAYS + 1) * 86400
    assert rs.db_mutate(D, lambda cur: (cur.rows, old)) is not None
    rec.replace_recipes(D, [_rec("https://a.example/c")], reason="r2")
    assert [a["recipe"]["url"] for a in rs.db_read(D).superseded] == ["https://a.example/b"]


def test_save_recipes_stays_a_plain_replace_without_an_archive(env) -> None:
    rec.save_recipes(D, [_rec("https://a.example/a")])
    rec.save_recipes(D, [_rec("https://a.example/b")])
    assert rs.db_read(D).superseded == []


def _fake_synth(monkeypatch, cands, *, platform="carscommerce"):
    from backend.scanner import dealer_place, recipe_synth, recipe_validation

    monkeypatch.setattr(recipe_synth, "fetch_dealer_html", lambda url: "<html/>")
    monkeypatch.setattr(recipe_synth, "fingerprint_platform", lambda html, url: platform)
    monkeypatch.setattr(recipe_synth, "synthesize_recipes", lambda did, url, html, p: list(cands))
    monkeypatch.setattr(recipe_synth, "validate_recipe", lambda *a, **k: 50)
    monkeypatch.setattr(dealer_place, "learn_place", lambda *a, **k: {})
    report = SimpleNamespace(status="ok", summary=lambda: "ok")
    monkeypatch.setattr(recipe_validation, "gate_recipes", lambda did, kept, **k: (kept, report))


def test_ensure_recipe_force_supersedes_the_whole_set_with_reason_synth_force(env, monkeypatch) -> None:
    from backend.scanner.pipeline.recipes import ensure_recipe

    rec.save_recipes(D, [_rec("https://a.example/inv", vehicle_rows=40, last_ok_at=5.0),
                         _rec("https://a.example/other", vehicle_rows=20, last_ok_at=5.0)])
    _fake_synth(monkeypatch, [_rec("https://a.example/inv")])
    info = ensure_recipe({"dealer_id": D, "url": "https://a.example", "name": "A"}, force=True)
    assert info["synth"] == "saved_1"
    assert [r.url for r in load_recipes(D)] == ["https://a.example/inv"]
    assert {a["recipe"]["url"] for a in rs.db_read(D).superseded} == {"https://a.example/inv", "https://a.example/other"}
    assert {a["reason"] for a in rs.db_read(D).superseded} == {"synth:force"}


def test_non_force_section_scoped_resynthesis_supersedes_only_the_scoped_recipes(env, monkeypatch) -> None:
    from backend.scanner.pipeline.recipes import ensure_recipe

    scoped = _rec("https://a.example/inv?type_slug=used", vehicle_rows=10, last_ok_at=5.0)
    monkeypatch.setattr(rec, "recipe_is_section_scoped", lambda r: "type_slug" in r.url)
    rec.save_recipes(D, [scoped])
    # A second, unscoped recipe cannot coexist with "all live are scoped"; so the trigger
    # class is exactly the scoped one and an unrelated stale recipe stays live-side.
    rec.save_recipes(D, [scoped, _rec("https://a.example/stale", vehicle_rows=1, stale=True)])
    _fake_synth(monkeypatch, [_rec("https://a.example/whole", vehicle_rows=80)])
    ensure_recipe({"dealer_id": D, "url": "https://a.example", "name": "A"})
    assert {r.url for r in load_recipes(D)} == {"https://a.example/whole", "https://a.example/stale"}
    (arch,) = rs.db_read(D).superseded
    assert arch["recipe"]["url"] == scoped.url and arch["reason"] == "synth:cc_section_scoped"


def test_cascade_and_synth_upsert_keep_the_existing_recipes(env) -> None:
    from backend.scripts.synthesize_recipes import _upsert_recipe

    rec.save_recipes(D, [_rec("https://a.example/x", vehicle_rows=30, last_ok_at=5.0)])
    _upsert_recipe(D, _rec("https://a.example/y", vehicle_rows=10))
    assert {r.url for r in load_recipes(D)} == {"https://a.example/x", "https://a.example/y"}
    assert rs.db_read(D).superseded == []


def test_discovery_md_carries_no_url_query_strings(tmp_path, monkeypatch) -> None:
    from backend.scanner.pipeline import dealer_logs
    from backend.scanner.recipe_validation import strip_url_queries, write_discovery_log

    assert strip_url_queries("see https://a.example/feed?apikey=SECRET&p=1 (and https://b.example/x).") == \
        "see https://a.example/feed (and https://b.example/x)."
    monkeypatch.setattr(dealer_logs, "LOG_ROOT", tmp_path)
    dealer_logs._log_append(D, "discovery.md", "- recipe https://a.example/feed?apikey=SECRET")
    text = (tmp_path / D / "discovery.md").read_text(encoding="utf-8")
    assert "SECRET" not in text and "https://a.example/feed" in text

    report = SimpleNamespace(verdict="ok", stamp="t", reason_code="", to_markdown=lambda ctx: "x https://a.example/f?k=SECRET")
    write_discovery_log(D, report, log_root=tmp_path / "v")
    assert "SECRET" not in (tmp_path / "v" / D / "discovery.md").read_text(encoding="utf-8")
