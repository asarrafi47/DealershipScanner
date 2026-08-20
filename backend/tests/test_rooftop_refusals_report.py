"""
Tests for the rooftop_refusals readers (backend/scripts/report_rooftop_refusals.py).

The write path (backend/scanner/rooftop_ledger.py) shipped with zero readers; these
tests exercise the three readings V009 promised, end to end through the ledger's OWN
writer where possible: refusals are seeded via ``note_refusal`` + ``flush`` against an
in-memory SQLite stand-in for Postgres (the same monkeypatch-the-connection pattern as
test_job_queue.py), then read back with the report functions. That way a drift between
what the writer inserts and what the readers expect fails here instead of in production.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from backend.scanner import rooftop_ledger
from backend.scripts import report_rooftop_refusals as rr


# ---------------------------------------------------------------------------
# Harness: an in-memory SQLite database dressed up as the inventory Postgres.
# The ledger writes with %s placeholders; the adapter translates. ``close`` is
# a no-op so the ledger's own conn.close() cannot destroy the shared database.
# ---------------------------------------------------------------------------


class _Cursor:
    def __init__(self, cur: sqlite3.Cursor) -> None:
        self._cur = cur

    def execute(self, sql: str, params: tuple = ()) -> "_Cursor":
        self._cur.execute(sql.replace("%s", "?"), params)
        return self

    def fetchone(self):
        return self._cur.fetchone()

    def fetchall(self):
        return self._cur.fetchall()

    def close(self) -> None:
        self._cur.close()


class _Conn:
    def __init__(self, db: sqlite3.Connection) -> None:
        self._db = db

    def cursor(self) -> _Cursor:
        return _Cursor(self._db.cursor())

    def commit(self) -> None:
        self._db.commit()

    def rollback(self) -> None:
        self._db.rollback()

    def close(self) -> None:  # deliberate no-op: the db outlives the ledger's close()
        pass


_SCHEMA = """
CREATE TABLE rooftop_refusals (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    dealer_id     TEXT NOT NULL,
    reason        TEXT NOT NULL,
    rows_refused  INTEGER NOT NULL DEFAULT 0,
    rooftops_seen TEXT,
    rooftop_count INTEGER,
    scanned_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%S+00:00', 'now'))
);
CREATE TABLE dealerships (
    name TEXT, website_url TEXT, dealer_website_url TEXT,
    is_active INTEGER DEFAULT 1, duplicate_of_id INTEGER
);
CREATE TABLE cars (scraped_at TEXT);
"""


@pytest.fixture()
def conn(monkeypatch) -> _Conn:
    db = sqlite3.connect(":memory:")
    db.executescript(_SCHEMA)
    wrapped = _Conn(db)
    # The ledger's flush() imports these two from backend.db.inventory_pg.
    monkeypatch.setattr("backend.db.inventory_pg.is_inventory_postgres", lambda: True)
    monkeypatch.setattr("backend.db.inventory_pg.pg_connect", lambda: wrapped)
    monkeypatch.setenv("ROOFTOP_LEDGER", "1")
    # A previous test's unflushed buffer must not leak into this one.
    with rooftop_ledger._lock:
        rooftop_ledger._pending.clear()
    yield wrapped
    db.close()


def _seed_via_ledger(dealer_id: str, rows: int, rooftops: list[str],
                     reason: str = "group_feed_no_matching_rooftop") -> None:
    """Seed exactly the way production does: through the ledger's own writer."""
    rooftop_ledger.note_refusal(dealer_id, reason, rows, rooftops)
    assert rooftop_ledger.flush(dealer_id) == 1


def _seed_car_scraped(conn: _Conn, when: datetime) -> None:
    conn.cursor().execute(
        "INSERT INTO cars (scraped_at) VALUES (%s)",
        (when.strftime("%Y-%m-%dT%H:%M:%SZ"),),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Reading 1: the census orders worst-first
# ---------------------------------------------------------------------------


def test_census_orders_worst_first(conn) -> None:
    _seed_via_ledger("small-dealer-com", 50, ["Rooftop One"])
    _seed_via_ledger("worst-dealer-com", 700, ["Audi Huntsville", "BMW of Silver Spring"])
    _seed_via_ledger("worst-dealer-com", 500, ["Audi Huntsville", "Crown Lexus"])
    _seed_via_ledger("middle-dealer-com", 400, ["Rooftop Two"])

    census = rr.refusal_census(conn)

    assert [e["dealer_id"] for e in census] == [
        "worst-dealer-com", "middle-dealer-com", "small-dealer-com",
    ]
    worst = census[0]
    assert worst["rows_refused"] == 1200
    assert worst["events"] == 2
    # Distinct across BOTH events: Audi Huntsville counted once.
    assert worst["distinct_rooftops"] == 3
    assert worst["rooftops"] == ["Audi Huntsville", "BMW of Silver Spring", "Crown Lexus"]
    assert worst["last_scanned_at"] is not None


def test_census_empty_table(conn) -> None:
    assert rr.refusal_census(conn) == []


# ---------------------------------------------------------------------------
# Reading 2: the gap list finds unregistered rooftops and skips registered ones
# ---------------------------------------------------------------------------


def _register(conn: _Conn, name: str, url: str) -> None:
    conn.cursor().execute(
        "INSERT INTO dealerships (name, website_url, is_active) VALUES (%s, %s, 1)",
        (name, url),
    )
    conn.commit()


def test_gap_list_finds_unregistered_rooftop(conn) -> None:
    _register(conn, "Crown Lexus", "https://www.crownlexus.com")
    _register(conn, "Audi Ontario", "https://www.audiontario.com")
    _seed_via_ledger(
        "host-dealer-com", 300,
        ["Crown Lexus", "Audi Huntsville", "audiontario.com"],
    )
    _seed_via_ledger("other-dealer-com", 100, ["Audi Huntsville"])

    gaps = rr.unregistered_rooftops(conn)
    named = [g for g in gaps if g["kind"] in ("name", "host")]

    assert [g["identifier"] for g in named] == ["Audi Huntsville"]
    gap = named[0]
    assert gap["events"] == 2
    assert gap["dealers_reporting"] == ["host-dealer-com", "other-dealer-com"]


def test_gap_list_matches_registry_name_case_and_punctuation(conn) -> None:
    _register(conn, "BMW of Silver Spring", "https://www.bmwofsilverspring.com")
    _seed_via_ledger("d-com", 10, ["BMW OF SILVER SPRING", "bmwofsilverspring.com"])
    named = [g for g in rr.unregistered_rooftops(conn) if g["kind"] in ("name", "host")]
    assert named == []


def test_gap_list_reports_address_identifiers_as_capture_gaps_not_registry_gaps(conn) -> None:
    # The live ledger's dominant shape: address blocks, no store name (Hendrick feed).
    _seed_via_ledger(
        "terrylabontechevy-com", 100,
        ["1464 Savannah Highway<br/>Charleston, SC 29407<br/>(843) 763-8403", "Cary, NC"],
    )
    gaps = rr.unregistered_rooftops(conn)
    kinds = {g["identifier"]: g["kind"] for g in gaps}
    assert kinds["1464 Savannah Highway<br/>Charleston, SC 29407<br/>(843) 763-8403"] == "address"
    assert kinds["Cary, NC"] == "locality"
    assert [g for g in gaps if g["kind"] in ("name", "host")] == []


# ---------------------------------------------------------------------------
# Reading 3: the regression alarm
# ---------------------------------------------------------------------------


def test_alarm_fires_on_zero_refusals_while_scans_ran(conn) -> None:
    _seed_car_scraped(conn, datetime.now(timezone.utc))
    verdict = rr.zero_refusal_alarm(conn, days=3)
    assert verdict["alarm"] is True
    assert verdict["refusals_in_window"] == 0
    assert verdict["cars_scraped_in_window"] == 1


def test_alarm_quiet_when_gate_is_alive(conn) -> None:
    _seed_car_scraped(conn, datetime.now(timezone.utc))
    _seed_via_ledger("alive-dealer-com", 42, ["Some Rooftop"])
    verdict = rr.zero_refusal_alarm(conn, days=3)
    assert verdict["alarm"] is False
    assert verdict["refusals_in_window"] == 1


def test_alarm_not_fired_when_nothing_ran(conn) -> None:
    # Zero refusals AND zero scans = idleness, not breakage.
    verdict = rr.zero_refusal_alarm(conn, days=3)
    assert verdict["alarm"] is False
    assert verdict["cars_scraped_in_window"] == 0


def test_alarm_ignores_refusals_outside_the_window(conn) -> None:
    _seed_car_scraped(conn, datetime.now(timezone.utc))
    _seed_via_ledger("stale-dealer-com", 10, ["Old Rooftop"])
    # Age the row beyond the window; the writer stamps NOW() so backdate directly.
    old = (datetime.now(timezone.utc) - timedelta(days=10)).strftime("%Y-%m-%d %H:%M:%S+00:00")
    conn.cursor().execute("UPDATE rooftop_refusals SET scanned_at = %s", (old,))
    conn.commit()
    verdict = rr.zero_refusal_alarm(conn, days=3)
    assert verdict["alarm"] is True


# ---------------------------------------------------------------------------
# CLI exit codes: silence while scanning is a nonzero exit, documented as 3
# ---------------------------------------------------------------------------


def test_main_exits_3_when_alarm_fires(conn, monkeypatch, capsys) -> None:
    _seed_car_scraped(conn, datetime.now(timezone.utc))
    monkeypatch.setattr(rr, "_connect", lambda: conn)
    code = rr.main(["--alarm"])
    assert code == rr.ALARM_EXIT_CODE == 3
    assert "ALARM" in capsys.readouterr().out


def test_main_exits_0_when_gate_alive(conn, monkeypatch, capsys) -> None:
    _seed_car_scraped(conn, datetime.now(timezone.utc))
    _seed_via_ledger("alive-dealer-com", 42, ["Some Rooftop"])
    monkeypatch.setattr(rr, "_connect", lambda: conn)
    code = rr.main(["--alarm"])
    assert code == 0
    assert "gate is alive" in capsys.readouterr().out
