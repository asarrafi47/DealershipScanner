"""Card S20: the scanner never deletes, retires, moves or overwrites a user's car.

A car a seller posts through ``/inventory`` lives in ``cars`` with ``dealer_id``
``dealer-portal-<user id>`` (``backend/dealer/portal_sync.py``). Before S20 the nightly
roster took that id like a dealer's, the VIN upsert let a scanned dealer take the row
(owner inactive, 48 h window lapsed, or a "strong" claimant), the dummy-VIN delete went
by VIN alone, and the two attribution scripts could move it.

Offline tests drive the Python guard, the roster and the scripts with fakes (no SQLite,
no network). The ``pg`` tests run the real upsert, the SQL backstop and the reconcile
paths on a migration-built Postgres clone (``scripts/cloud/with_pg.sh``).
"""
from __future__ import annotations

import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from backend.scanner.upsert import guard as _guard
from backend.scanner.upsert import ownership as _own
from backend.utils import user_listings as ul

REPO = Path(__file__).resolve().parents[2]
USER = "dealer-portal-41"
OTHER_USER = "dealer-portal-42"
DEALER = "scanned-motors-com"
VIN = "1HGCM82633A004352"


def _iso(dt: datetime, z: bool = True) -> str:
    text = dt.astimezone(timezone.utc).replace(tzinfo=None).isoformat()
    return text + "Z" if z else text + "+00:00"


def _ago(**kw) -> datetime:
    return datetime.now(timezone.utc) - timedelta(**kw)


# ---------------------------------------------------------------------------
# Offline: the Python guard (ownership.decide via guard.prefetch_existing)
# ---------------------------------------------------------------------------


class _Cursor:
    """Answers the prefetch SELECT from a list of stored rows; records every statement."""

    def __init__(self, rows: list[tuple]):
        self.rows = rows
        self.statements: list[tuple[str, tuple]] = []
        self._out: list[tuple] = []
        self.rowcount = 0

    def execute(self, sql, params=()):
        params = tuple(params)
        self.statements.append((sql, params))
        if "FROM cars WHERE vin IN" in sql:
            self._out = [r for r in self.rows if r[0] in params]
        else:
            self._out = []
        self.rowcount = 1 if sql.lstrip().upper().startswith("UPDATE") else 0

    def fetchall(self):
        return list(self._out)


def _stored(owner=USER, *, active=1, scraped=None, tier="none", removed=None, vin=VIN) -> tuple:
    scraped = scraped if scraped is not None else _iso(_ago(minutes=5))
    return (vin, None, owner, active, scraped, tier, owner, None, removed)


def _prefetch(stored: tuple, claimant=DEALER, *, tier=None, hours=48.0):
    by_vin = {stored[0]: {"vin": stored[0], "dealer_id": claimant, "_rooftop_evidence_tier": tier}}
    from backend.scanner import database as sdb

    moves: list = []
    reasons: dict = {}
    front: list = []
    _, conflicts = _guard.prefetch_existing(
        _Cursor([stored]), by_vin, _guard.guard_window(hours), sdb._vin_owned_elsewhere,
        moves=moves, reasons=reasons, front_run=front,
    )
    return conflicts, reasons, moves, front


def test_strong_claimant_cannot_take_a_user_vin():
    # Before S20 rule 1 (strong claimant over a none/weak owner) let a dealer take it.
    for hours in (48.0, 0.0):
        conflicts, reasons, moves, _ = _prefetch(_stored(), tier="strong", hours=hours)
        assert conflicts == {VIN: (USER, DEALER)}
        assert reasons[VIN] == _own.USER_LISTING
        assert moves == []
    # The rule outranks a strong stored tier, pools and aliases too: decide() says so first.
    pools = _own.Pools(((DEALER, (DEALER, USER)),), (frozenset({DEALER, USER}),))
    verdict = _own.decide(USER, "none", True, False, DEALER, "strong", pools, owner_user_protected=True)
    assert verdict == _own.Verdict(False, _own.USER_LISTING)


def test_lapsed_window_cannot_take_a_user_vin():
    # Scraped 10 days ago: outside the 48 h window, which used to hand the VIN over.
    conflicts, reasons, moves, _ = _prefetch(_stored(scraped=_iso(_ago(days=10))))
    assert conflicts == {VIN: (USER, DEALER)}
    assert reasons[VIN] == _own.USER_LISTING
    assert moves == []


@pytest.mark.parametrize("removed", [None, "", "not a date", "recent"])
def test_inactive_user_row_is_not_taken(removed):
    # listing_active 0 used to mean "owner_inactive: take it". A user row retired 7 days
    # ago or less, or with no readable retirement time, still blocks the VIN.
    if removed == "recent":
        removed = _iso(_ago(days=2), z=False)
    conflicts, reasons, moves, front = _prefetch(_stored(active=0, removed=removed), hours=0.0)
    assert conflicts == {VIN: (USER, DEALER)}
    assert reasons[VIN] == _own.USER_LISTING
    assert moves == [] and front == []  # an already retired row is not hidden again


def test_retired_user_row_older_than_7_days_can_be_taken():
    removed = _iso(_ago(days=ul.USER_ROW_GRACE_DAYS, hours=1))
    conflicts, _, moves, _ = _prefetch(_stored(active=0, removed=removed))
    assert conflicts == {}
    assert [m.reason for m in moves] == ["owner_inactive"]
    assert ul.user_row_blocks_vin(USER, 0, removed) is False
    assert ul.user_row_blocks_vin(USER, 0, _iso(_ago(days=ul.USER_ROW_GRACE_DAYS - 1))) is True


def test_another_user_cannot_take_a_user_vin_and_is_not_front_running():
    conflicts, reasons, _, front = _prefetch(_stored(), claimant=OTHER_USER)
    assert conflicts == {VIN: (USER, OTHER_USER)} and reasons[VIN] == _own.USER_LISTING
    assert front == []  # only a scanned dealer listing the VIN hides the user listing


def test_front_run_pairs_are_live_user_rows_claimed_by_a_scanned_dealer():
    _, _, _, front = _prefetch(_stored(), hours=0.0)
    assert front == [(VIN, USER)]
    cur = _Cursor([])
    assert _guard.hide_front_run_user_listings(cur, [(VIN, USER), ("X" * 17, DEALER)], "now") == 1
    (sql, params), = cur.statements
    assert "listing_active = 0" in sql and "marked_for_review = 1" in sql
    assert params == ("now", USER, VIN)  # a scanned dealer's id is never touched


def test_scanned_dealers_keep_their_rules():
    # No user involved: every N8.6 verdict is what it was (the full table is in
    # test_upsert_vin_owner_guard.py::test_decide_table, which runs unchanged).
    conflicts, reasons, _, front = _prefetch(_stored(owner="owner-com"), tier=None)
    assert conflicts == {VIN: ("owner-com", DEALER)} and reasons[VIN] == "group_feed_unstamped"
    conflicts, _, moves, _ = _prefetch(_stored(owner="owner-com"), tier="strong")
    assert conflicts == {} and [m.reason for m in moves] == ["strong_claimant"]
    conflicts, _, moves, _ = _prefetch(_stored(owner="owner-com"), hours=0.0)
    assert conflicts == {} and front == []


def test_user_listing_helper_matches_the_portal_prefix():
    from backend.dealer import portal_sync

    assert ul.DEALER_PORTAL_DEALER_ID_PREFIX == portal_sync.DEALER_PORTAL_DEALER_ID_PREFIX
    assert ul.is_dealer_portal_dealer_id(portal_sync.dealer_portal_dealer_id(41))
    for value in ("scanned-motors-com", "", None, 41, "xdealer-portal-1"):
        assert ul.is_dealer_portal_dealer_id(value) is portal_sync.is_dealer_portal_dealer_id(value)


def test_scanner_import_path_stays_free_of_flask():
    """The scanner image has no Flask (requirements-scanner.txt): the helper must not pull it in."""
    code = (
        "import sys; import backend.utils.user_listings, backend.scanner.upsert.guard, "
        "backend.scanner.manifest, backend.scripts.fleet_scan; "
        "assert 'flask' not in sys.modules and 'backend.dealer' not in sys.modules"
    )
    subprocess.run([sys.executable, "-c", code], cwd=REPO, check=True, timeout=120)


# ---------------------------------------------------------------------------
# Offline: roster, dummy-VIN delete, attribution scripts
# ---------------------------------------------------------------------------


class _Conn:
    def __init__(self, rows):
        self.cur = _ListCursor(rows)

    def cursor(self):
        return self.cur

    def close(self):
        pass

    def commit(self):
        pass


class _ListCursor:
    def __init__(self, rows):
        self.rows = rows
        self.statements: list[tuple[str, tuple]] = []
        self.rowcount = 0

    def execute(self, sql, params=()):
        self.statements.append((sql, tuple(params)))

    def fetchall(self):
        return list(self.rows)


def test_roster_excludes_user_dealer_ids(monkeypatch):
    from backend.db import inventory_db
    from backend.scanner import manifest
    from backend.scripts import fleet_scan

    monkeypatch.setattr(manifest, "_attach_shared_host_identity", lambda entries: entries)
    scannable = [(DEALER, "Scanned Motors", "https://www.scanned-motors.com/x", "", "", 1),
                 (USER, "A seller", None, "", "", 1)]
    monkeypatch.setattr(inventory_db, "get_conn", lambda: _Conn(scannable))
    assert [d["dealer_id"] for d in manifest._load_scannable_dealers()] == [DEALER]
    monkeypatch.setattr(inventory_db, "get_conn", lambda: _Conn([r[:3] for r in scannable]))
    assert [d["dealer_id"] for d in manifest._load_active_inventory_dealers()] == [DEALER]

    # fleet_scan.load_roster: defence in depth on both paths, even an explicit SCAN_DEALERS.
    monkeypatch.setenv("SCAN_DEALERS", f"{DEALER},{USER}")
    assert fleet_scan.load_roster() == [DEALER]
    monkeypatch.delenv("SCAN_DEALERS")
    monkeypatch.setenv("SCAN_FLEET", "1")
    monkeypatch.setattr(manifest, "_load_scannable_dealers",
                        lambda: [{"dealer_id": DEALER}, {"dealer_id": USER}])
    monkeypatch.setattr(manifest, "load_do_not_scan_ids", lambda *a, **k: set())
    monkeypatch.setattr(manifest, "load_revived_dealer_ids", lambda *a, **k: set())
    assert fleet_scan.load_roster() == [DEALER]


def test_dummy_vin_delete_skips_user_rows(monkeypatch):
    from backend.db import incomplete_listings_db
    from backend.db.repositories import cars_repo

    rows = [(1, "VIN000001", DEALER), (2, "VIN000002", USER), (3, "VINXXX", USER), (4, VIN, DEALER)]
    conn = _Conn(rows)
    monkeypatch.setattr(cars_repo, "get_conn", lambda: conn)
    monkeypatch.setattr(cars_repo, "ensure_nhtsa_vpic_cache_table", lambda c: None)
    monkeypatch.setattr(incomplete_listings_db, "delete_incomplete_record", lambda cid: None)
    out = cars_repo.delete_cars_with_dummy_placeholder_vins()
    assert out["deleted"] == 1 and out["vins"] == ["VIN000001"]
    deletes = [p for s, p in conn.cur.statements if s.startswith("DELETE FROM cars")]
    assert deletes == [(1,)]


def test_photo_attribution_skips_user_rows():
    from backend.scripts import apply_attribution_moves as moves_script
    from backend.scripts import resolve_photo_attribution as photo

    summary = {"dealer_domains": ["scanned-motors.com", "scanned-motors.com"]}
    cur = _ListCursor([(1, VIN, USER, summary), (2, "2HGCM82633A004352", "other-motors-com", summary)])
    index = {"scanned-motors-com": "scanned-motors-com"}
    found = photo.find_candidates(cur, index)
    assert [c["car_id"] for c in found] == [2]  # the user's car is never a candidate

    dealers = [(7, "Scanned Motors", "https://www.scanned-motors.com", 33.1)]
    candidates = [(1, "Scanned Motors", "plate frame", USER, "A seller", None),
                  (2, "Scanned Motors", "plate frame", "other-motors-com", "Other", None)]
    planned, *_ = moves_script.plan_moves(candidates, dealers, set(), set(), {})
    assert [m[0] for m in planned] == [2]
    assert all(not ul.is_dealer_portal_dealer_id(m[4]) for m in planned)


# ---------------------------------------------------------------------------
# Postgres tier: the real upsert, the SQL backstop and the reconcile paths
# ---------------------------------------------------------------------------


@pytest.fixture()
def pg_env(pg_db, monkeypatch):
    from backend.scanner import database as sdb

    monkeypatch.delenv("SCANNER_VIN_OWNER_GUARD_HOURS", raising=False)
    monkeypatch.setattr("backend.utils.project_env.load_project_dotenv", lambda *a, **k: None)
    monkeypatch.setattr(sdb, "_vin_owner_table_ready", False)
    monkeypatch.setattr(
        "backend.enrichment.spec_structured_backfill.apply_structured_spec_backfill_for_car",
        lambda *a, **k: {},
    )
    monkeypatch.setattr("backend.catalog.linker.link_cars_by_vins", lambda vins: 0)
    return pg_db


def _car(dealer_id: str, vin: str = VIN, **fields) -> dict:
    row = {
        "vin": vin, "title": "2020 Honda Accord EX", "year": 2020, "make": "Honda", "model": "Accord",
        "trim": "EX", "price": 21000, "mileage": 31000, "dealer_id": dealer_id, "dealer_name": dealer_id,
    }
    if not ul.is_dealer_portal_dealer_id(dealer_id):
        row["dealer_url"] = f"https://www.{dealer_id.replace('-com', '.com')}"
    row.update(fields)
    return row


def _row(env, vin: str = VIN) -> dict:
    with env.connect() as conn:
        cur = conn.execute(
            "SELECT dealer_id, price, listing_active, listing_removed_at, marked_for_review "
            "FROM cars WHERE vin = %s", (vin,))
        r = cur.fetchone()
    keys = ("dealer_id", "price", "listing_active", "listing_removed_at", "marked_for_review")
    return dict(zip(keys, r)) if r else {}


def _sql(env, sql: str, params=()):
    with env.connect() as conn:
        conn.execute(sql, params)
        conn.commit()


def _conflicts(env) -> list[tuple]:
    with env.connect() as conn:
        return conn.execute(
            "SELECT vin, owner_dealer_id, claimant_dealer_id FROM vin_owner_conflicts ORDER BY 1, 2, 3"
        ).fetchall()


def _bind(dealer_id: str, *, guard_on: bool, tier: str = "none", vin: str = VIN, price: int = 21000):
    from backend.scanner.upsert.rows import normalize_vehicle_row
    from backend.scanner.upsert.sql import prepare_row, upsert_params

    row = prepare_row(normalize_vehicle_row(_car(dealer_id, vin, price=price)), None)
    return upsert_params(
        row, now=_iso(_ago(seconds=0)), price_history_json=None, guard_on=guard_on,
        guard_cutoff_iso=_iso(_ago(hours=48)), rooftop_tier=tier,
    )


@pytest.mark.pg
def test_sql_backstop_refuses_user_vin(pg_env):
    """UPSERT_CARS_SQL alone (no Python guard): every branch that used to let a claim
    through (guard flag 0, strong rank, inactive owner) is refused for a user's car."""
    from backend.scanner.upsert.sql import UPSERT_CARS_SQL

    conn = pg_env.get_conn()
    try:
        cur = conn.cursor()
        cur.execute(UPSERT_CARS_SQL, _bind(USER, guard_on=True))
        assert cur.rowcount == 1
        for guard_on in (True, False):
            cur.execute(UPSERT_CARS_SQL, _bind(DEALER, guard_on=guard_on, tier="strong", price=1))
            assert cur.rowcount == 0, guard_on
        # retired two days ago by the seller: still the user's, whatever listing_active says
        cur.execute("UPDATE cars SET listing_active = 0, listing_removed_at = %s WHERE vin = %s",
                    (_iso(_ago(days=2), z=False), VIN))
        cur.execute(UPSERT_CARS_SQL, _bind(DEALER, guard_on=False, price=1))
        assert cur.rowcount == 0
        cur.execute(UPSERT_CARS_SQL, _bind(OTHER_USER, guard_on=False, price=1))
        assert cur.rowcount == 0
        cur.execute("SELECT dealer_id, price FROM cars WHERE vin = %s", (VIN,))
        assert tuple(cur.fetchone()) == (USER, 21000)
        # retired 8 days ago: the VIN is free again
        cur.execute("UPDATE cars SET listing_removed_at = %s WHERE vin = %s", (_iso(_ago(days=8)), VIN))
        cur.execute(UPSERT_CARS_SQL, _bind(DEALER, guard_on=True, price=2))
        assert cur.rowcount == 1
        cur.execute("SELECT dealer_id, listing_active FROM cars WHERE vin = %s", (VIN,))
        assert tuple(cur.fetchone()) == (DEALER, 1)
    finally:
        conn.rollback()
        conn.close()


@pytest.mark.pg
def test_portal_edit_still_updates_own_row(pg_env, monkeypatch):
    from backend.scanner import database as sdb

    assert sdb.upsert_vehicles([_car(USER, price=21000)]) == 1
    for hours in ("48", "0"):
        monkeypatch.setenv("SCANNER_VIN_OWNER_GUARD_HOURS", hours)
        stats: dict = {}
        assert sdb.upsert_vehicles([_car(USER, price=19500 + int(hours))], stats) == 1
        assert stats["vin_owner_conflicts"] == 0
        assert _row(pg_env)["price"] == 19500 + int(hours)
    # The seller deleted it (portal retire) and adds it back: the same row comes back live.
    _sql(pg_env, "UPDATE cars SET listing_active = 0, listing_removed_at = %s WHERE vin = %s",
         (_iso(_ago(hours=1), z=False), VIN))
    assert sdb.upsert_vehicles([_car(USER, price=18000)]) == 1
    assert _row(pg_env)["listing_active"] == 1 and _row(pg_env)["listing_removed_at"] is None


@pytest.mark.pg
def test_sql_backstop_holds_with_guard_off(pg_env, monkeypatch):
    """vin_owner_guard_hours=0 and the Python user check knocked out: the SQL still refuses."""
    from backend.scanner import database as sdb

    assert sdb.upsert_vehicles([_car(USER)]) == 1
    monkeypatch.setenv("SCANNER_VIN_OWNER_GUARD_HOURS", "0")
    monkeypatch.setattr(_guard, "user_row_blocks_vin", lambda *a, **k: False)
    stats: dict = {}
    assert sdb.upsert_vehicles([_car(DEALER, price=1, _rooftop_evidence_tier="strong")], stats) == 0
    assert _row(pg_env)["dealer_id"] == USER and _row(pg_env)["price"] == 21000
    assert stats["vin_owner_conflicts"] == 1
    assert _conflicts(pg_env) == [(VIN, USER, DEALER)]


@pytest.mark.pg
def test_strong_and_lapsed_claims_on_postgres(pg_env):
    from backend.scanner import database as sdb

    assert sdb.upsert_vehicles([_car(USER)]) == 1
    _sql(pg_env, "UPDATE cars SET scraped_at = %s, listing_active = 0, listing_removed_at = %s WHERE vin = %s",
         (_iso(_ago(days=10)), _iso(_ago(days=3), z=False), VIN))
    stats: dict = {}
    assert sdb.upsert_vehicles([_car(DEALER, price=1, _rooftop_evidence_tier="strong")], stats) == 0
    assert stats["vin_owner_conflict_reasons"] == {_own.USER_LISTING: 1}
    assert _row(pg_env)["dealer_id"] == USER


@pytest.mark.pg
def test_retired_user_row_older_than_7_days_is_taken_on_postgres(pg_env):
    from backend.scanner import database as sdb

    assert sdb.upsert_vehicles([_car(USER)]) == 1
    _sql(pg_env, "UPDATE cars SET listing_active = 0, listing_removed_at = %s, marked_for_review = 1 WHERE vin = %s",
         (_iso(_ago(days=8), z=False), VIN))
    stats: dict = {}
    assert sdb.upsert_vehicles([_car(DEALER, price=23000)], stats) == 1
    row = _row(pg_env)
    assert (row["dealer_id"], row["listing_active"], row["marked_for_review"]) == (DEALER, 1, 0)
    assert stats["vin_owner_conflicts"] == 0


@pytest.mark.pg
def test_front_running_user_listing_is_hidden_for_review(pg_env):
    from backend.scanner import database as sdb

    assert sdb.upsert_vehicles([_car(USER)]) == 1
    stats: dict = {}
    assert sdb.upsert_vehicles([_car(DEALER, price=24000)], stats) == 0
    row = _row(pg_env)
    assert row["dealer_id"] == USER and row["price"] == 21000  # not taken, not overwritten
    assert row["listing_active"] == 0 and row["listing_removed_at"]  # off the car page and search
    assert row["marked_for_review"] == 1
    assert stats["user_listings_hidden_for_review"] == 1
    assert _conflicts(pg_env) == [(VIN, USER, DEALER)]
    # The seller re-syncs (edits) it: it stays hidden until an admin clears the flag.
    assert sdb.upsert_vehicles([_car(USER, price=20000)]) == 1
    row = _row(pg_env)
    assert (row["listing_active"], row["marked_for_review"], row["price"]) == (0, 1, 20000)
    # The next nightly sees the dealer again: still the user's row, nothing hidden twice.
    stats = {}
    assert sdb.upsert_vehicles([_car(DEALER, price=24000)], stats) == 0
    assert stats["user_listings_hidden_for_review"] == 0 and _row(pg_env)["dealer_id"] == USER
    # An admin clears the flag; the seller's next edit publishes it again.
    _sql(pg_env, "UPDATE cars SET marked_for_review = 0 WHERE vin = %s", (VIN,))
    assert sdb.upsert_vehicles([_car(USER, price=20000)]) == 1
    assert _row(pg_env)["listing_active"] == 1


@pytest.mark.pg
def test_roster_excludes_user_dealer_ids_on_postgres(pg_env, monkeypatch):
    from backend.scanner import database as sdb
    from backend.scanner import manifest

    assert sdb.upsert_vehicles([_car(USER), _car(DEALER, vin="2HGCM82633A004352")]) == 2
    monkeypatch.setattr(manifest, "_attach_shared_host_identity", lambda entries: entries)
    assert [d["dealer_id"] for d in manifest._load_scannable_dealers()] == [DEALER]
    assert [d["dealer_id"] for d in manifest._load_active_inventory_dealers()] == [DEALER]


@pytest.mark.pg
def test_reconcile_of_a_scanned_dealer_never_retires_user_rows(pg_env, monkeypatch):
    """The three retire paths are scoped by the scanned dealer_id (DESIGN.md 23.4 step 9)."""
    from backend.attribution.disown import disown_foreign_rooftop_vins
    from backend.scanner import database as sdb
    from backend.scanner.inventory_reconcile import reconcile_dealer_inventory_after_scan
    from backend.scanner.pipeline.reconcile import reconcile_dealer

    dealer_vins = [f"3HGCM82633A0{n:05d}" for n in range(30)]
    user_vins = [f"4HGCM82633A0{n:05d}" for n in range(3)]
    assert sdb.upsert_vehicles([_car(DEALER, v) for v in dealer_vins]) == 30
    assert sdb.upsert_vehicles([_car(USER, v) for v in user_vins]) == 3
    old = _iso(_ago(days=20))
    _sql(pg_env, "UPDATE cars SET scraped_at = %s", (old,))

    monkeypatch.setenv("SCANNER_RECONCILE", "1")
    seen = set(dealer_vins[:24]) | set(user_vins)  # even a feed that names the users' VINs
    out = reconcile_dealer_inventory_after_scan(
        DEALER, "https://www.scanned-motors.com", seen, {"deduped_rows": 24})
    assert out["marked_inactive"] == 6

    conn = pg_env.get_conn()
    try:
        res = reconcile_dealer(conn, DEALER, _iso(_ago(seconds=0)), 24, 24, "ok")
        assert res["retired"] == 24
    finally:
        conn.close()
    assert disown_foreign_rooftop_vins(DEALER, set(user_vins)) == 0

    for v in user_vins:
        row = _row(pg_env, v)
        assert (row["dealer_id"], row["listing_active"], row["listing_removed_at"]) == (USER, 1, None)
    assert all(_row(pg_env, v)["listing_active"] == 0 for v in dealer_vins)


@pytest.mark.pg
def test_dummy_vin_delete_skips_user_rows_on_postgres(pg_env):
    from backend.db.repositories import cars_repo

    _sql(pg_env, "INSERT INTO cars (vin, title, dealer_id, listing_active) VALUES "
         "('VIN000001', 't', %s, 1), ('VIN000002', 't', %s, 1)", (DEALER, USER))
    out = cars_repo.delete_cars_with_dummy_placeholder_vins()
    assert out["vins"] == ["VIN000001"]
    assert _row(pg_env, "VIN000002")["dealer_id"] == USER
