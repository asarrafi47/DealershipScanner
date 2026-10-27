"""N13 P7.7: rows written by the authority-model code (rev CAS, mutation API, archive,
per-host blocked status) stay readable by a Phase-0 reader that knows only ``recipes_json``
and ``max_saved_at`` (the code a rolled-back fleet or a not-yet-upgraded host still runs).
Real Postgres through ``pg_db`` (no sqlite3), RECIPES_DIR under tmp_path.
"""

from __future__ import annotations

import json
from dataclasses import fields

import pytest

import backend.scanner.recipe_store as rs
import backend.scanner.recipes as rec
from backend.scanner.recipes import EndpointRecipe

pytestmark = pytest.mark.pg

D = "backcompat-dealer-com"

# The recipe row schema Phase-0 code wrote: today's fields minus the P7.3 stamp. Phase-0
# loaders drop keys they do not know, so ``mutated_at`` is additive, nothing else may appear.
PHASE0_FIELDS = {f.name for f in fields(EndpointRecipe)} - {"mutated_at"}


@pytest.fixture()
def env(pg_db, tmp_path, monkeypatch):
    monkeypatch.setenv("RECIPES_DB_DISABLED", "")
    monkeypatch.delenv("SCANNER_EGRESS_TAG", raising=False)
    monkeypatch.setattr(rec, "RECIPES_DIR", tmp_path / "ws" / "recipes")
    monkeypatch.setattr(rec, "_foreign_unread", set())
    monkeypatch.setattr(rec, "_foreign_warned", set())
    (tmp_path / "ws" / "recipes").mkdir(parents=True)
    return pg_db


def _rec(url: str, **kw) -> EndpointRecipe:
    return EndpointRecipe(dealer_id=D, url=url, method="GET", content_type="application/json",
                          post_template=None, **kw)


def _phase0_read(pg, dealer_id: str = D) -> tuple[list[dict], float]:
    """What Phase-0 ``db_load_recipes`` did: two columns, ``recipes_json`` parsed."""
    with pg.connect() as conn:
        row = conn.execute("SELECT recipes_json, max_saved_at FROM dealer_recipes WHERE dealer_id = %s",
                           (dealer_id,)).fetchone()
    return json.loads(row[0]), float(row[1])


def _phase0_recipes(rows: list[dict]) -> list[EndpointRecipe]:
    """Phase-0 ``_rows_to_recipes``: keep known keys, build the dataclass."""
    return [EndpointRecipe(**{k: v for k, v in r.items() if k in PHASE0_FIELDS}) for r in rows]


def _assert_phase0_readable(pg, live_urls: set[str]) -> None:
    rows, max_saved = _phase0_read(pg)
    assert isinstance(rows, list) and {r["url"] for r in rows} == live_urls
    for r in rows:
        assert set(r) <= PHASE0_FIELDS | {"mutated_at"}
    assert max_saved == max(r["saved_at"] for r in rows) > 0
    assert {r.url for r in _phase0_recipes(rows)} == live_urls


def test_every_write_path_keeps_max_saved_at_and_the_recipes_json_schema(env) -> None:
    rec.save_recipes(D, [_rec("https://a.example/one", vehicle_rows=5),
                         _rec("https://a.example/two", vehicle_rows=6)])
    _assert_phase0_readable(env, {"https://a.example/one", "https://a.example/two"})
    first = _phase0_read(env)[1]

    rec.mark_stale(D, _rec("https://a.example/one"), "http 410")
    rows, stamp = _phase0_read(env)
    assert stamp >= first and [r["stale"] for r in rows if r["url"].endswith("/one")] == [True]

    rec.record_replay_ok(D, _rec("https://a.example/one").key(), last_ok_at=99.0,
                         field_coverage={"price": 0.9}, unstale=True, total_count=40)
    rows, stamp2 = _phase0_read(env)
    assert stamp2 >= stamp
    one = next(r for r in rows if r["url"].endswith("/one"))
    assert one["stale"] is False and one["last_ok_at"] == 99.0 and one["total_count"] == 40

    rec.merge_recipes(D, [_rec("https://a.example/three", vehicle_rows=9)])
    _assert_phase0_readable(env, {"https://a.example/one", "https://a.example/two", "https://a.example/three"})

    rec.replace_recipes(D, [_rec("https://a.example/new", vehicle_rows=1)], reason="synth:force")
    _assert_phase0_readable(env, {"https://a.example/new"})


def test_phase0_reader_never_sees_the_archive_or_the_hints(env) -> None:
    rec.save_recipes(D, [_rec("https://a.example/old", vehicle_rows=4)])
    rec.replace_recipes(D, [_rec("https://a.example/new", vehicle_rows=8)], reason="synth:force")
    rs.mutate_scan_hints(D, lambda h: {**h, "recipe_blocked": {"railway": "blocked:railway:403:x"}})
    rows, _ = _phase0_read(env)
    assert [r["url"] for r in rows] == ["https://a.example/new"]
    assert "https://a.example/old" not in json.dumps(rows) and "superseded" not in json.dumps(rows)
    # the archive is in its own column, which a Phase-0 SELECT never names
    assert rs.db_read(D).superseded[0]["recipe"]["url"] == "https://a.example/old"
    # Phase-0 db_load_recipes (the live API) returns the same two things
    live, stamp = rs.db_load_recipes(D)
    assert [r["url"] for r in live] == ["https://a.example/new"] and stamp > 0


def test_hints_only_writes_do_not_move_max_saved_at(env) -> None:
    rec.save_recipes(D, [_rec("https://a.example/one")])
    before = _phase0_read(env)
    assert rs.set_scan_hints(D, {"note": "x"})
    assert _phase0_read(env) == before


def test_phase0_save_over_a_new_row_bumps_nothing_it_does_not_know(env) -> None:
    """A Phase-0 writer UPDATEs only the columns it knows (recipes_json, max_saved_at, ...).
    The V034 defaults let it insert and overwrite without naming rev, recipes_written_at or
    superseded_json, and the new code reads the result."""
    rec.save_recipes(D, [_rec("https://a.example/one")])
    with env.connect() as conn:
        conn.execute("UPDATE dealer_recipes SET recipes_json = %s, max_saved_at = %s WHERE dealer_id = %s",
                     (json.dumps([{"dealer_id": D, "url": "https://a.example/p0", "method": "GET",
                                   "content_type": "application/json", "post_template": None,
                                   "saved_at": 1e12}]), 1e12, D))
    cur = rs.db_read(D)
    assert [r["url"] for r in cur.rows] == ["https://a.example/p0"]
    assert cur.superseded == []
