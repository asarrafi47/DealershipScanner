"""``backfill_dealership_registry_ids`` skips hosts no unstamped car names (``pg`` tier).

The backfill ran one ``UPDATE cars ... LIKE '%//host'`` per registry host (~620), each a full
scan of ``cars``, on every worker start. It now reads the hosts the unstamped cars name in one
pass and only runs the UPDATEs that can match. The stamped result must not change.
"""

from __future__ import annotations

import pytest

from backend.db.repositories import dealers_repo

pytestmark = pytest.mark.pg


class _RecordingConn:
    """Delegates to a real connection and remembers every UPDATE it is asked to run."""

    def __init__(self, conn):
        self._conn = conn
        self.updates: list[tuple] = []

    def cursor(self):
        outer = self
        cur = self._conn.cursor()

        class _Cur:
            def execute(self, sql, params=()):
                if sql.lstrip().upper().startswith("UPDATE"):
                    outer.updates.append(tuple(params))
                return cur.execute(sql, params)

            def __getattr__(self, name):
                return getattr(cur, name)

        return _Cur()

    def __getattr__(self, name):
        return getattr(self._conn, name)


def _seed(pg_db):
    pg_db.add_dealerships(
        [
            {"website_url": "https://www.alpha-motors.example.com/"},
            {"website_url": "https://beta-cars.example.com"},
            {"website_url": "https://subaru.com"},  # a brand HQ row: must not claim *subaru.com rooftops
            {"website_url": "https://gamma-nobody.example.com"},  # no car names this host
            {"website_url": "https://under_score.example.com"},  # LIKE wildcard in the host: always tried
        ]
    )
    with pg_db.connect() as conn:
        ids = {
            r[1].split("//")[1].replace("www.", "").strip("/"): r[0]
            for r in conn.execute("SELECT id, website_url FROM dealerships").fetchall()
        }
    pg_db.add_cars(
        [
            {"dealer_url": "https://www.alpha-motors.example.com/inventory"},  # www + path
            {"dealer_url": "HTTP://ALPHA-MOTORS.EXAMPLE.COM"},  # case, bare host
            {"dealer_url": "https://beta-cars.example.com/"},
            {"dealer_url": "https://beta-cars.example.com:8443/x"},  # port: not the same host
            {"dealer_url": "https://sutherlinsubaru.com/used"},  # sibling of subaru.com: no match
            {"dealer_url": "https://underXscore.example.com/"},  # '_' matches any char in LIKE
            {"dealer_url": None},
            {"dealer_url": "https://beta-cars.example.com/", "listing_active": 0},  # inactive
            {"dealer_url": "https://beta-cars.example.com/", "dealership_registry_id": ids["alpha-motors.example.com"]},
        ]
    )
    return ids


def _stamps(pg_db):
    with pg_db.connect() as conn:
        return conn.execute(
            "SELECT dealer_url, listing_active, dealership_registry_id FROM cars ORDER BY id"
        ).fetchall()


def test_prefilter_changes_no_stamp_and_skips_unneeded_updates(pg_db):
    ids = _seed(pg_db)
    alpha, beta, under = (
        ids["alpha-motors.example.com"],
        ids["beta-cars.example.com"],
        ids["under_score.example.com"],
    )
    conn = _RecordingConn(pg_db.get_conn())
    try:
        total = dealers_repo.backfill_dealership_registry_ids(conn=conn)
    finally:
        conn._conn.close()

    rows = _stamps(pg_db)
    assert [r[2] for r in rows] == [
        alpha,  # www.alpha-motors.example.com/inventory
        alpha,  # HTTP://ALPHA-MOTORS.EXAMPLE.COM
        beta,
        None,  # :8443 is a different host string; the LIKE patterns never matched it
        None,  # sutherlinsubaru.com is not subaru.com
        under,  # '_' is a LIKE wildcard, so the host with '_' still reaches SQL and matches
        None,
        None,  # inactive rows are never stamped
        alpha,  # already stamped: left alone
    ]
    assert total == 4

    # Only hosts some unstamped car names (plus the wildcard host) were UPDATEd; the brand HQ
    # and the host nobody uses cost no table scan.
    hosts_run = {p[1].removeprefix("%//").split("/")[0] for p in conn.updates}
    assert hosts_run == {"alpha-motors.example.com", "beta-cars.example.com", "under_score.example.com"}


def test_second_run_with_nothing_left_runs_no_update_at_all(pg_db):
    _seed(pg_db)
    first = _RecordingConn(pg_db.get_conn())
    try:
        dealers_repo.backfill_dealership_registry_ids(conn=first)
    finally:
        first._conn.close()
    again = _RecordingConn(pg_db.get_conn())
    try:
        # The only unstamped active cars left are the port/sibling/NULL ones, which name no
        # registry host, so the per-host UPDATEs (a full scan each) are all skipped.
        assert dealers_repo.backfill_dealership_registry_ids(conn=again) == 0
    finally:
        again._conn.close()
    assert [p for p in again.updates if "under_score" not in str(p)] == []
