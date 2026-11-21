"""S20: the scanner never deletes or overwrites a user's car (docs/auth_sms/TASKS.md).

A car posted through the dealer portal lives in ``cars`` under ``dealer-portal-<user id>``. Rule 0 of
the VIN ownership policy keeps every scanner write, delete and re-attribution away from such a row
while it is live and for 7 days after it was retired, whatever the guard window, the evidence tiers
and the pools say.

Two groups. The top half needs no database (the helper, the roster, the policy function, the
scripts' filters). The bottom half is the ``pg`` tier: the real upsert statement, the guard, the
repair functions and reconcile against a throwaway Postgres built from the migrations. No SQLite
(``test_no_sqlite.py``), no network, no real dealer, no real person.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from backend.scanner import database as sdb
from backend.scanner.database import upsert_vehicles
from backend.scanner.upsert import ownership as own
from backend.utils.portal_ids import (
    DEALER_PORTAL_DEALER_ID_PREFIX,
    dealer_portal_dealer_id,
    is_dealer_portal_dealer_id,
)

VIN = "1N4BL4DV0PN300001"
USER = dealer_portal_dealer_id(7)  # a person's own listing
STORE = "real-dealer-com"  # a scanned rooftop


def _iso_ago(**delta) -> str:
    return (datetime.utcnow() - timedelta(**delta)).replace(microsecond=0).isoformat() + "Z"


def _car(dealer_id: str, price: int = 30000, vin: str = VIN, **fields) -> dict:
    row = {"vin": vin, "title": "2023 Nissan Altima", "year": 2023, "make": "Nissan", "model": "Altima",
           "price": price, "dealer_id": dealer_id, "dealer_name": dealer_id, "dealer_url": f"https://{dealer_id}"}
    row.update(fields)
    return row


# =========================================================================== no database


def test_portal_id_helper_is_shared_and_re_exported():
    assert is_dealer_portal_dealer_id(USER) and is_dealer_portal_dealer_id(DEALER_PORTAL_DEALER_ID_PREFIX + "1")
    assert not is_dealer_portal_dealer_id(STORE) and not is_dealer_portal_dealer_id(None)
    assert not is_dealer_portal_dealer_id("x-dealer-portal-7")
    from backend.dealer import portal_sync

    assert portal_sync.is_dealer_portal_dealer_id is is_dealer_portal_dealer_id
    assert portal_sync.DEALER_PORTAL_DEALER_ID_PREFIX == DEALER_PORTAL_DEALER_ID_PREFIX


def test_helper_module_imports_without_flask():
    """The scanner image has no Flask: the helper must not pull in ``backend.dealer``."""
    import subprocess
    import sys

    code = ("import sys; import backend.utils.portal_ids, backend.scanner.upsert.ownership; "
            "assert 'flask' not in sys.modules and 'backend.dealer' not in sys.modules")
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert r.returncode == 0, r.stderr


def test_roster_excludes_user_dealer_ids(monkeypatch):
    from backend.db import inventory_db
    from backend.scanner.manifest import _load_scannable_dealers

    rows = [(USER, "", "", "", "", 1), (STORE, "Real Dealer", "", "", "", 1),
            (dealer_portal_dealer_id(8), "", "", "", "", 0)]

    class _Cur:
        def execute(self, *a, **k):
            return None

        def fetchall(self):
            return rows

    class _Conn:
        def cursor(self):
            return _Cur()

        def close(self):
            return None

    monkeypatch.setattr(inventory_db, "get_conn", lambda *a, **k: _Conn())
    assert [d["dealer_id"] for d in _load_scannable_dealers()] == [STORE]


def test_fleet_roster_drops_user_ids_even_when_named_by_hand(monkeypatch, capsys):
    from backend.scripts import fleet_scan

    monkeypatch.setenv("SCAN_DEALERS", f"{STORE}, {USER}")
    monkeypatch.delenv("SCAN_FLEET", raising=False)
    assert fleet_scan.load_roster() == [STORE]
    assert USER in capsys.readouterr().out  # said out loud, not silently


@pytest.mark.parametrize(
    "owner_active, removed_at, allow, reason",
    [
        (True, None, False, "user_listing"),
        (False, None, False, "user_listing_recent"),
        (False, "not a date", False, "user_listing_recent"),
        (False, "2026-11-14T10:00:00+00:00", False, "user_listing_recent"),  # 1 day before the pinned now
        (False, "2026-11-01T10:00:00Z", True, "user_listing_lapsed"),
    ],
)
def test_decide_rule_0(owner_active, removed_at, allow, reason):
    now = datetime(2026, 11, 15, 10, 0, 0, tzinfo=timezone.utc)
    v = own.decide(USER, "none", owner_active, False, STORE, "strong", own.Pools((), ()),
                   owner_removed_at=removed_at, user_cutoff=own.user_listing_cutoff(now))
    assert (v.allow, v.reason) == (allow, reason)


def test_decide_without_the_new_arguments_behaves_as_before():
    pools = own.Pools((), ())
    assert own.decide("a-com", "none", False, False, "b-com", "none", pools) == (True, "owner_inactive")
    assert own.decide("a-com", "strong", True, False, "b-com", "none", pools) == (False, "owner_strong")


def test_user_listing_cutoff_is_seven_days_back():
    now = datetime(2026, 11, 15, 10, 0, 0, tzinfo=timezone.utc)
    assert own.user_listing_cutoff_iso(now) == "2026-11-08T10:00:00"


def test_photo_attribution_skips_user_rows():
    from backend.scripts.resolve_photo_attribution import find_candidates

    summary = json.dumps({"dealer_domains": ["somewhereelsemotors.com"] * 3})
    rows = [(1, "VIN1", STORE, summary), (2, "VIN2", USER, summary)]

    class _Cur:
        def execute(self, *a, **k):
            return None

        def fetchall(self):
            return rows

    found = find_candidates(_Cur(), {"somewhereelsemotors-com": "somewhereelsemotors-com"})
    assert [c["car_id"] for c in found] == [1]  # the rooftop's car is a candidate, the person's is not


def test_attribution_moves_script_skips_user_rows():
    """The move planner reads its candidates in ``main``; pin that the loop asks the one helper."""
    from pathlib import Path

    src = (Path(__file__).resolve().parents[1] / "scripts" / "apply_attribution_moves.py").read_text(encoding="utf-8")
    loop = src.index("for car_id, rooftop, evidence, from_did, from_name, from_rid in candidates:")
    assert "is_dealer_portal_dealer_id(from_did)" in src[loop:loop + 400]


def test_reconcile_refuses_a_user_listing_without_touching_the_database():
    from backend.scanner.pipeline.reconcile import reconcile_dealer

    out = reconcile_dealer(object(), USER, _iso_ago(minutes=5), 0, 99, "ok")
    assert out == {"eligible": False, "stale": 0, "retired": 0, "reason": "user listing"}


# =========================================================================== Postgres tier


pytestmark_pg = pytest.mark.pg


@pytest.fixture()
def pg_env(pg_db, monkeypatch):
    """A fresh migration-built clone as the inventory database; the network post-write steps stubbed."""
    monkeypatch.setattr("backend.utils.project_env.load_project_dotenv", lambda *a, **k: None)
    monkeypatch.setattr(sdb, "_vin_owner_table_ready", False)
    monkeypatch.delenv("SCANNER_VIN_OWNER_GUARD_HOURS", raising=False)
    monkeypatch.setattr(
        "backend.enrichment.spec_structured_backfill.apply_structured_spec_backfill_for_car", lambda *a, **k: {}
    )
    monkeypatch.setattr("backend.catalog.linker.link_cars_by_vins", lambda vins: 0)
    return pg_db


def _row(env, vin: str = VIN):
    with env.connect() as conn:
        r = conn.execute(
            "SELECT dealer_id, price, listing_active, listing_removed_at, marked_for_review FROM cars WHERE vin = %s",
            (vin,),
        ).fetchone()
    return tuple(r) if r else None


def _set(env, sql: str, params=()):
    with env.connect() as conn:
        conn.execute(sql, params)
        conn.commit()


def _conflicts(env):
    with env.connect() as conn:
        return [tuple(r) for r in conn.execute(
            "SELECT vin, owner_dealer_id, claimant_dealer_id FROM vin_owner_conflicts").fetchall()]


def _no_prefetch(monkeypatch):
    """Switch the Python guard off so only the statement's own WHERE can stop a claimant."""
    monkeypatch.setattr(sdb, "prefetch_existing", lambda *a, **k: ({}, {}))


@pytest.fixture()
def user_row(pg_env):
    """The person's own live listing of ``VIN``."""
    assert upsert_vehicles([_car(USER, 31000)]) == 1
    return pg_env


@pytest.mark.pg
def test_strong_claimant_cannot_take_a_user_vin(user_row):
    stats: dict = {}
    n = upsert_vehicles([_car(STORE, 1, _rooftop_evidence_tier="strong")], stats)
    assert n == 0
    assert _row(user_row)[:2] == (USER, 31000)  # never overwritten
    assert stats["vin_owner_conflicts"] == 1
    assert stats["vin_owner_conflict_reasons"] == {"user_listing": 1}
    assert _conflicts(user_row) == [(VIN, USER, STORE)]


@pytest.mark.pg
def test_lapsed_window_cannot_take_a_user_vin(user_row):
    """A scanned owner's row goes to anyone once the 48 h window lapses; a user's row does not."""
    _set(user_row, "UPDATE cars SET scraped_at = %s WHERE vin = %s", (_iso_ago(hours=500), VIN))
    assert upsert_vehicles([_car(STORE, 1)]) == 0
    assert _row(user_row)[:2] == (USER, 31000)


@pytest.mark.pg
def test_inactive_user_row_is_not_taken(user_row):
    """Retired a day ago: still the person's. A missing retirement stamp is protected too."""
    _set(user_row, "UPDATE cars SET listing_active = 0, listing_removed_at = %s WHERE vin = %s",
         (_iso_ago(days=1), VIN))
    assert upsert_vehicles([_car(STORE, 1)]) == 0
    assert _row(user_row)[0] == USER
    _set(user_row, "UPDATE cars SET listing_removed_at = NULL WHERE vin = %s", (VIN,))
    assert upsert_vehicles([_car(STORE, 1)]) == 0
    assert _row(user_row)[0] == USER


@pytest.mark.pg
def test_retired_user_row_older_than_7_days_can_be_taken(user_row):
    _set(user_row, "UPDATE cars SET listing_active = 0, listing_removed_at = %s WHERE vin = %s",
         (_iso_ago(days=6, hours=20), VIN))
    assert upsert_vehicles([_car(STORE, 1)]) == 0  # 6 days 20 h: still inside the 7 days
    assert _row(user_row)[0] == USER
    _set(user_row, "UPDATE cars SET listing_removed_at = %s WHERE vin = %s", (_iso_ago(days=7, hours=2), VIN))
    assert upsert_vehicles([_car(STORE, 29000)]) == 1
    assert _row(user_row)[:3] == (STORE, 29000, 1)


@pytest.mark.pg
def test_front_running_user_listing_is_hidden_for_review(user_row):
    """A rooftop that lists the VIN on its own site is refused the row, and the person's listing
    stops showing (``listing_active`` 0: the car page checks that, not ``marked_for_review``)."""
    assert upsert_vehicles([_car(STORE, 1)]) == 0
    dealer_id, price, active, removed_at, review = _row(user_row)
    assert (dealer_id, price) == (USER, 31000)  # never overwritten, never deleted
    assert active == 0 and review == 1 and removed_at
    # Still the person's during the grace, so a second sighting changes nothing.
    assert upsert_vehicles([_car(STORE, 1)]) == 0
    assert _row(user_row)[0] == USER and len(_conflicts(user_row)) == 1


@pytest.mark.pg
def test_a_user_claimant_does_not_hide_another_users_row(user_row):
    assert upsert_vehicles([_car(dealer_portal_dealer_id(9), 1)]) == 0
    row = _row(user_row)
    assert row[0] == USER and row[2] == 1  # refused, but not hidden: no rooftop lists it


@pytest.mark.pg
def test_sql_backstop_refuses_user_vin(user_row, monkeypatch):
    _no_prefetch(monkeypatch)  # only the statement can stop the claimant
    assert upsert_vehicles([_car(STORE, 1, _rooftop_evidence_tier="strong")], {}) == 0
    assert _row(user_row)[:3] == (USER, 31000, 1)
    assert _conflicts(user_row) == [(VIN, USER, STORE)]


@pytest.mark.pg
def test_sql_backstop_holds_with_guard_off(user_row, monkeypatch):
    """``vin_owner_guard_hours=0`` turns the old guard off. A user's row is still not taken, by the
    Python rule and, with that skipped too, by the statement's own WHERE."""
    monkeypatch.setenv("SCANNER_VIN_OWNER_GUARD_HOURS", "0")
    assert upsert_vehicles([_car(STORE, 1)]) == 0
    assert _row(user_row)[:2] == (USER, 31000)
    _set(user_row, "UPDATE cars SET listing_active = 1, listing_removed_at = NULL, marked_for_review = NULL "
                   "WHERE vin = %s", (VIN,))
    _no_prefetch(monkeypatch)
    assert upsert_vehicles([_car(STORE, 1)]) == 0
    assert _row(user_row)[:3] == (USER, 31000, 1)


@pytest.mark.pg
def test_sql_backstop_ignores_an_inactive_owner_for_recent_user_rows(user_row, monkeypatch):
    """The statement's old ``listing_active != 1`` branch cannot let a recently retired user row go."""
    monkeypatch.setenv("SCANNER_VIN_OWNER_GUARD_HOURS", "0")
    _set(user_row, "UPDATE cars SET listing_active = 0, listing_removed_at = %s WHERE vin = %s",
         (_iso_ago(days=2), VIN))
    _no_prefetch(monkeypatch)
    assert upsert_vehicles([_car(STORE, 1)]) == 0
    assert _row(user_row)[0] == USER
    _set(user_row, "UPDATE cars SET listing_removed_at = %s WHERE vin = %s", (_iso_ago(days=9), VIN))
    assert upsert_vehicles([_car(STORE, 29000)]) == 1  # lapsed: the statement lets it go
    assert _row(user_row)[:2] == (STORE, 29000)


@pytest.mark.pg
def test_portal_edit_still_updates_own_row(user_row):
    assert upsert_vehicles([_car(USER, 28500)]) == 1  # the portal's own upsert, same owner
    assert _row(user_row)[:3] == (USER, 28500, 1)


@pytest.mark.pg
def test_scanned_dealers_still_trade_a_vin_as_before(pg_env):
    """No change between scanned dealers: an inactive owner's row can be taken."""
    upsert_vehicles([_car("a-com")])
    _set(pg_env, "UPDATE cars SET listing_active = 0 WHERE vin = %s", (VIN,))
    assert upsert_vehicles([_car("b-com")]) == 1
    assert _row(pg_env)[0] == "b-com"


@pytest.mark.pg
def test_dummy_vin_delete_skips_user_rows(pg_env):
    from backend.db.repositories.cars_repo import delete_cars_with_dummy_placeholder_vins

    upsert_vehicles([_car(STORE, vin="1N4BL4DV0PN300009")])
    for vin, dealer in (("VIN0000001", USER), ("VIN0000002", STORE)):
        _set(pg_env, "INSERT INTO cars (vin, title, dealer_id, listing_active) VALUES (%s, %s, %s, 1)",
             (vin, "t", dealer))
    res = delete_cars_with_dummy_placeholder_vins()
    assert res["vins"] == ["VIN0000002"] and res["deleted"] == 1  # the rooftop's placeholder went
    assert _row(pg_env, "VIN0000001") is not None  # the person's car stayed, whatever its VIN looks like
    assert _row(pg_env, "VIN0000002") is None


@pytest.mark.pg
def test_reconcile_of_a_scanned_dealer_never_retires_user_rows(pg_env):
    from backend.db.inventory_db import get_conn
    from backend.scanner.pipeline.constants import MIN_ROWS_UNKNOWN
    from backend.scanner.pipeline.reconcile import reconcile_dealer

    cars = [_car(STORE, vin=f"1N4BL4DV0PN3{i:05d}") for i in range(MIN_ROWS_UNKNOWN + 2)]
    user_vin = "1N4BL4DV0PN399999"
    assert upsert_vehicles(cars + [_car(USER, vin=user_vin)]) == len(cars) + 1
    old = _iso_ago(days=30)
    _set(pg_env, "UPDATE cars SET scraped_at = %s WHERE vin IN (%s, %s)", (old, user_vin, "1N4BL4DV0PN300000"))
    conn = get_conn()
    try:
        out = reconcile_dealer(conn, STORE, _iso_ago(minutes=5), 0, len(cars) - 1, "ok", baseline_buckets=None)
        conn.commit()
    finally:
        conn.close()
    assert out["eligible"] and out["retired"] == 1  # the rooftop's own stale car, nothing else
    assert _row(pg_env, user_vin)[2] == 1  # the person's old row is still live
