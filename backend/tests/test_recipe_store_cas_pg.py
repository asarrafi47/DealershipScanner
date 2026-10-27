"""N13 P7.1 / P14A.9: compare-and-set on ``dealer_recipes`` (V034), ``pg`` tier.

Real Postgres (READ COMMITTED re-evaluation and rowcount through the inventory cursor),
on the throwaway database ``pg_db`` clones from the migration-built template.
"""

from __future__ import annotations

import json
import threading
from pathlib import Path

import pytest

from backend.scanner import recipe_store as rs

pytestmark = pytest.mark.pg

REPO = Path(__file__).resolve().parents[2]


@pytest.fixture()
def pg(pg_db, monkeypatch):
    monkeypatch.setenv("RECIPES_DB_DISABLED", "")
    monkeypatch.setattr(rs, "_alias_read_slug", lambda _d: None)
    with rs._stats_lock:
        for k in rs._stats:
            rs._stats[k] = 0
    return pg_db


def _row(pg, dealer_id):
    with pg.connect() as conn:
        return conn.execute(
            "SELECT rev, recipes_written_at, superseded_json, recipes_json, scan_hints "
            "FROM dealer_recipes WHERE dealer_id = %s", (dealer_id,)).fetchone()


def _counter(rows):
    return (rows[0]["n"] if rows else 0)


def _bump_counter(current):
    rows = list(current.rows) if current else []
    rows = [{"n": _counter(rows) + 1}]
    return rows


def test_v034_columns_and_defaults(pg) -> None:
    assert rs.db_save_recipes("d1", [{"saved_at": 5.0}])
    rev, written, superseded, _rj, _h = _row(pg, "d1")
    assert rev == 1 and written > 0 and json.loads(superseded) == []


def test_first_insert_race_exactly_one_inserter_wins_the_rest_retry(pg) -> None:
    errs: list[BaseException] = []

    def work() -> None:
        try:
            assert rs.db_mutate("race", _bump_counter) is not None
        except BaseException as exc:  # noqa: BLE001
            errs.append(exc)

    ts = [threading.Thread(target=work) for _ in range(8)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert not errs
    rev, _w, _s, rj, _h = _row(pg, "race")
    assert json.loads(rj) == [{"n": 8}] and rev == 8


def test_8_threads_x_50_increments_lose_nothing(pg) -> None:
    errs: list[BaseException] = []

    def work() -> None:
        try:
            for _ in range(50):
                assert rs.db_mutate("inc", _bump_counter, retries=10_000) is not None
        except BaseException as exc:  # noqa: BLE001
            errs.append(exc)

    ts = [threading.Thread(target=work) for _ in range(8)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert not errs
    rev, _w, _s, rj, _h = _row(pg, "inc")
    assert json.loads(rj) == [{"n": 400}] and rev == 400
    assert rs.store_stats()["writes"] == 400


def test_rowcount_zero_is_never_success(pg, monkeypatch) -> None:
    assert rs.db_save_recipes("stale", [{"n": 0}])

    real = rs._snapshot

    def stale_read(cur, dealer_id, *args, **kwargs):
        out = real(cur, dealer_id, *args, **kwargs)
        return out if out is None else type(out)(**{**out.__dict__, "rev": out.rev - 1})

    monkeypatch.setattr(rs, "_snapshot", stale_read)
    assert rs.db_mutate("stale", _bump_counter, retries=3) is None
    monkeypatch.undo()
    stats = rs.store_stats()
    assert stats["exhausted"] == 1 and stats["conflicts"] == 3
    assert json.loads(_row(pg, "stale")[3]) == [{"n": 0}]


def test_concurrent_hint_keys_both_survive(pg) -> None:
    def work(i: int) -> None:
        for j in range(10):
            assert rs.set_scan_hints("h", {f"k{i}_{j}": j})

    ts = [threading.Thread(target=work, args=(i,)) for i in range(4)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    hints = rs.get_scan_hints("h")
    assert len(hints) == 40
    rev, written, *_ = _row(pg, "h")
    assert rev == 40 and written == 0  # hint writes bump rev, never recipes_written_at


def test_hints_only_row_is_not_authoritative_until_a_recipe_write(pg) -> None:
    assert rs.set_scan_hints("hx", {"notes": "x"})
    got = rs.db_read("hx")
    assert got is not None and not got.authoritative and got.rows == [] and got.hints == {"notes": "x"}
    assert rs.db_save_recipes("hx", [{"saved_at": 1.0}])
    got = rs.db_read("hx")
    assert got.authoritative and got.rev == 2 and got.hints == {"notes": "x"}


def test_mutate_scan_hints_nested_merge_and_noop(pg) -> None:
    def add(h):
        h.setdefault("blocked", {})["railway"] = {"status": 403}
        return h

    assert rs.mutate_scan_hints("n", add)
    assert rs.mutate_scan_hints("n", lambda h: {**h, "blocked": {**h["blocked"], "mbp": {"status": 200}}})
    assert set(rs.get_scan_hints("n")["blocked"]) == {"railway", "mbp"}
    rev = rs.db_read("n").rev
    assert rs.mutate_scan_hints("n", lambda h: None)
    assert rs.db_read("n").rev == rev


def test_create_false_does_not_insert(pg) -> None:
    assert rs.db_mutate("nope", lambda c: [{"x": 1}], create=False) is None
    assert _row(pg, "nope") is None


def test_set_scan_hints_semantics_preserved(pg) -> None:
    rs.set_scan_hints("s", {"a": 1, "b": 2})
    rs.set_scan_hints("s", {"a": None, "c": 3})
    assert rs.get_scan_hints("s") == {"b": 2, "c": 3}
    rs.set_scan_hints("s", {"z": 1}, merge=False)
    assert rs.get_scan_hints("s") == {"z": 1}
    rs.set_scan_hints("s", {}, merge=False)
    assert rs.get_scan_hints("s") == {}


def test_cas_statements_adapt_for_postgres() -> None:
    from backend.db.inventory_pg import adapt_sql_for_postgres_execute as adapt

    ins = adapt("INSERT INTO dealer_recipes (dealer_id, rev) VALUES (?, ?) ON CONFLICT (dealer_id) DO NOTHING")
    upd = adapt("UPDATE dealer_recipes SET recipes_json=?, rev=rev+1 WHERE dealer_id=? AND rev=?")
    assert ins is not None and "%s" in ins and "ON CONFLICT (dealer_id) DO NOTHING" in ins and "?" not in ins
    assert upd is not None and upd.count("%s") == 3 and "rev=rev+1" in upd


def test_migration_file_is_additive_and_guarded() -> None:
    text = (REPO / "migrations" / "V034__dealer_recipes_rev.sql").read_text()
    for col in ("rev", "recipes_written_at", "superseded_json"):
        assert f"ADD COLUMN IF NOT EXISTS {col}" in text
    assert "DROP" not in text.upper().replace("DROPPED", "")
