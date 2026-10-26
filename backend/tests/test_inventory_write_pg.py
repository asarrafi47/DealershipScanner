"""Concurrent upserts through ``InventoryWriteCoordinator`` on a real Postgres (P14A.7, ``pg`` tier).

The offline tests fake the exceptions (test_upsert_deadlock_retry.py,
test_upsert_vin_owner_guard.py). These make Postgres raise them:

* a real 40P01 (two sessions take two row locks in opposite orders) is retried;
* a held row lock with ``PGOPTIONS='-c lock_timeout=100ms'`` raises a real 55P03
  (the southcoasttoyota-com 609-row loss class) and is retried, then gives up
  after three attempts when the lock never clears;
* two dealers claiming one VIN at the same moment end with ONE owner and the
  loser recorded in ``vin_owner_conflicts`` (the SQL backstop in
  ``upsert/sql.py``, which compares ``cars.scraped_at`` TEXT against the cutoff).

Every interleave is synchronized with ``threading.Event`` / ``Barrier``, never
with sleeps that decide the outcome. Each test gets a fresh migration-built
clone (``pg_db``).
"""

from __future__ import annotations

import asyncio
import threading
from datetime import datetime, timedelta, timezone

import pytest

from backend.scanner import database as sdb
from backend.scanner import inventory_write as iw

pytestmark = pytest.mark.pg

VIN_A = "1N4BL4DV0PN300001"
VIN_B = "1N4BL4DV0PN300002"
VIN_C = "1N4BL4DV0PN300003"
STEP = 10  # seconds: upper bound on any single wait, so a broken interleave fails fast


@pytest.fixture()
def write_env(pg_db, monkeypatch):
    """Parallel upsert on, guard at its default, the network post-write steps stubbed."""
    monkeypatch.setenv("SCANNER_PARALLEL_UPSERT", "1")
    monkeypatch.delenv("SCANNER_VIN_OWNER_GUARD_HOURS", raising=False)
    monkeypatch.setattr("backend.utils.project_env.load_project_dotenv", lambda *a, **k: None)
    monkeypatch.setattr(sdb, "_vin_owner_table_ready", False)
    monkeypatch.setattr(
        "backend.enrichment.spec_structured_backfill.apply_structured_spec_backfill_for_car",
        lambda *a, **k: {},
    )
    monkeypatch.setattr("backend.catalog.linker.link_cars_by_vins", lambda vins: 0)
    # A short retry sleep keeps the retry cases fast; the retry logic is unchanged.
    real_sleep = asyncio.sleep
    monkeypatch.setattr(iw.asyncio, "sleep", lambda *_a, **_k: real_sleep(0.05))
    return pg_db


def _car(dealer_id: str, vin: str, **fields) -> dict:
    row = {
        "vin": vin,
        "title": "2023 Nissan Altima SV",
        "year": 2023,
        "make": "Nissan",
        "model": "Altima",
        "trim": "SV",
        "price": 30000,
        "mileage": 12000,
        "image_url": "https://img.example.test/altima.jpg",
        "exterior_color": "Pearl White",
        "interior_color": "Charcoal",
        "transmission": "CVT",
        "drivetrain": "FWD",
        "fuel_type": "Gasoline",
        "dealer_id": dealer_id,
        "dealer_name": dealer_id,
        "dealer_url": f"https://{dealer_id}.example.test",
    }
    row.update(fields)
    return row


def _select(env, sql: str, params=()):
    with env.connect() as conn:
        return conn.execute(sql, params).fetchall()


def _prices(env) -> dict[str, float]:
    return {v: float(p) for v, p in _select(env, "SELECT vin, price FROM cars")}


def _seed(vins: list[str], dealer_id: str = "d-one") -> None:
    assert sdb.upsert_vehicles([_car(dealer_id, v) for v in vins], {}) == len(vins)


def test_a_real_deadlock_is_retried_and_both_writers_land(write_env):
    """Opposite lock orders on one VIN pair: Postgres picks a victim (40P01), the retry lands it."""
    _seed([VIN_A, VIN_B])
    real = sdb.upsert_vehicles
    barrier = threading.Barrier(2, timeout=STEP)
    guard = threading.Lock()
    calls: dict[tuple, int] = {}
    deadlocks: list[str] = []

    def crossing(vehicles, stats=None):
        order = tuple(v["vin"] for v in vehicles)
        with guard:
            calls[order] = calls.get(order, 0) + 1
            first = calls[order] == 1
        if first:
            # The first attempt of each writer takes its two row locks in its own order,
            # holding the first until the other has taken its first: a genuine cycle.
            import psycopg

            try:
                with psycopg.connect(write_env.dsn) as conn:
                    try:
                        conn.execute("SET LOCAL deadlock_timeout = '100ms'")
                    except psycopg.Error:
                        pass  # not allowed for this role: the 1 s default still resolves it
                    conn.execute("UPDATE cars SET price = price WHERE vin = %s", (order[0],))
                    barrier.wait()
                    conn.execute("UPDATE cars SET price = price WHERE vin = %s", (order[1],))
            except psycopg.errors.DeadlockDetected:
                deadlocks.append(order[0])
                raise
        return real(vehicles) if stats is None else real(vehicles, stats)

    async def run():
        co = iw.InventoryWriteCoordinator()
        assert co.parallel
        return await asyncio.gather(
            co.upsert_vehicles([_car("d-one", VIN_A, price=31000), _car("d-one", VIN_B, price=31000)]),
            co.upsert_vehicles([_car("d-one", VIN_B, price=32000), _car("d-one", VIN_A, price=32000)]),
        )

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(sdb, "upsert_vehicles", crossing)
        counts = asyncio.run(asyncio.wait_for(run(), timeout=STEP * 2))

    assert counts == [2, 2]
    assert len(deadlocks) == 1, "Postgres must abort exactly one of the two sessions"
    assert sum(calls.values()) == 3, "two first attempts plus one retry of the victim"
    prices = _prices(write_env)
    assert set(prices) == {VIN_A, VIN_B}
    assert set(prices.values()) <= {31000.0, 32000.0} and len(set(prices.values())) == 1
    assert _select(write_env, "SELECT count(*) FROM cars") == [(2,)]


def _hold_row_lock(env, vin: str):
    """A session that holds the row lock on *vin* until the returned ``release`` is called."""
    conn = env.connect()
    conn.execute("UPDATE cars SET price = price WHERE vin = %s", (vin,))

    def release():
        conn.commit()
        conn.close()

    return release


def test_a_lock_timeout_is_retried_until_the_lock_clears(write_env, monkeypatch):
    """55P03 from ``lock_timeout=100ms`` (the 609-row loss class) retries and the row is written."""
    _seed([VIN_A, VIN_B, VIN_C])
    monkeypatch.setenv("PGOPTIONS", "-c lock_timeout=100ms")
    release = _hold_row_lock(write_env, VIN_B)
    real = sdb.upsert_vehicles
    timed_out = threading.Event()
    states: list[str] = []

    def watching(vehicles, stats=None):
        import psycopg

        try:
            return real(vehicles) if stats is None else real(vehicles, stats)
        except psycopg.errors.LockNotAvailable as exc:
            states.append(exc.sqlstate)
            timed_out.set()
            raise

    async def run():
        co = iw.InventoryWriteCoordinator()
        task = asyncio.ensure_future(
            co.upsert_vehicles([_car("d-one", v, price=33000) for v in (VIN_A, VIN_B, VIN_C)])
        )
        # Free the row only after the server has actually timed the first attempt out.
        await asyncio.get_running_loop().run_in_executor(None, timed_out.wait, STEP)
        release()
        return await task

    monkeypatch.setattr(sdb, "upsert_vehicles", watching)
    assert asyncio.run(asyncio.wait_for(run(), timeout=STEP * 2)) == 3
    assert states and set(states) == {"55P03"}
    assert _prices(write_env) == {VIN_A: 33000.0, VIN_B: 33000.0, VIN_C: 33000.0}


def test_a_lock_that_never_clears_gives_up_after_three_attempts(write_env, monkeypatch):
    _seed([VIN_A])
    monkeypatch.setenv("PGOPTIONS", "-c lock_timeout=100ms")
    release = _hold_row_lock(write_env, VIN_A)
    real = sdb.upsert_vehicles
    attempts = {"n": 0}

    def counting(vehicles, stats=None):
        attempts["n"] += 1
        return real(vehicles) if stats is None else real(vehicles, stats)

    monkeypatch.setattr(sdb, "upsert_vehicles", counting)
    try:
        with pytest.raises(Exception) as err:
            asyncio.run(iw.InventoryWriteCoordinator().upsert_vehicles([_car("d-one", VIN_A, price=1)]))
    finally:
        release()
    assert type(err.value).__name__ == "LockNotAvailable"
    assert attempts["n"] == 3
    assert _prices(write_env) == {VIN_A: 30000.0}, "the failed writes left the stored row untouched"


def test_two_dealers_claiming_one_vin_at_once_leave_one_owner(write_env, monkeypatch):
    """Both writers read 'no owner' before either writes; the SQL backstop picks the single winner."""
    barrier = threading.Barrier(2, timeout=STEP)
    real_prefetch = sdb.prefetch_existing

    def prefetch_then_meet(*args, **kwargs):
        result = real_prefetch(*args, **kwargs)
        barrier.wait()  # both prefetches are done (and empty) before any INSERT
        return result

    monkeypatch.setattr(sdb, "prefetch_existing", prefetch_then_meet)
    stats = {"x": {}, "y": {}}

    async def run():
        co = iw.InventoryWriteCoordinator()
        return await asyncio.gather(
            co.upsert_vehicles([_car("dealer-x", VIN_A, price=1111)], stats["x"]),
            co.upsert_vehicles([_car("dealer-y", VIN_A, price=2222)], stats["y"]),
        )

    counts = asyncio.run(asyncio.wait_for(run(), timeout=STEP * 2))

    assert sorted(counts) == [0, 1], "exactly one writer owns the VIN"
    rows = _select(write_env, "SELECT dealer_id, price FROM cars WHERE vin = %s", (VIN_A,))
    assert len(rows) == 1
    owner, price = rows[0]
    winner, loser = ("x", "y") if owner == "dealer-x" else ("y", "x")
    assert float(price) == (1111.0 if winner == "x" else 2222.0), "the loser's fields never reached the row"
    assert stats[loser]["vin_owner_conflicts"] == 1
    assert stats[loser]["vin_owner_conflict_owners"] == {owner: 1}
    assert stats[winner]["vin_owner_conflicts"] == 0
    assert _select(
        write_env, "SELECT vin, owner_dealer_id, claimant_dealer_id FROM vin_owner_conflicts"
    ) == [(VIN_A, owner, f"dealer-{loser}")]


def test_strong_and_unstamped_claimants_racing_leave_the_strong_one_owner(write_env, monkeypatch):
    """N8.6: both prefetches see no owner; whichever INSERT lands first, the SQL backstop
    applies the evidence rules, so the store whose feed stamped the car owns it."""
    barrier = threading.Barrier(2, timeout=STEP)
    real_prefetch = sdb.prefetch_existing

    def prefetch_then_meet(*args, **kwargs):
        result = real_prefetch(*args, **kwargs)
        barrier.wait()
        return result

    monkeypatch.setattr(sdb, "prefetch_existing", prefetch_then_meet)
    strong = _car("dealer-strong", VIN_A, price=1111)
    strong["_rooftop_evidence_tier"] = "strong"

    async def run():
        co = iw.InventoryWriteCoordinator()
        return await asyncio.gather(
            co.upsert_vehicles([_car("dealer-plain", VIN_A, price=2222)]),
            co.upsert_vehicles([strong]),
        )

    asyncio.run(asyncio.wait_for(run(), timeout=STEP * 2))
    assert _select(write_env, "SELECT dealer_id, rooftop_tier FROM cars WHERE vin = %s", (VIN_A,)) == [
        ("dealer-strong", "strong")
    ]


@pytest.mark.parametrize(
    ("age_hours", "moves"),
    [(1, False), (47, False), (49, True)],
    ids=["1h-fresh", "47h-inside-window", "49h-outside-window"],
)
def test_backstop_window_compares_scraped_at_as_text(write_env, monkeypatch, age_hours, moves):
    """The statement's guard is ``cars.scraped_at < <cutoff>`` on TEXT: the product's own
    ``...Z`` stamps sort correctly either side of the 48 h cutoff."""
    _seed([VIN_A], dealer_id="owner")
    stamp = (datetime.now(timezone.utc) - timedelta(hours=age_hours)).replace(tzinfo=None).isoformat() + "Z"
    with write_env.connect() as conn:
        conn.execute("UPDATE cars SET scraped_at = %s WHERE vin = %s", (stamp, VIN_A))
        conn.commit()
    monkeypatch.setattr(sdb, "_vin_owned_elsewhere", lambda *a, **k: False)  # statement-only check
    stats: dict = {}
    assert sdb.upsert_vehicles([_car("claimant", VIN_A, price=5)], stats) == (1 if moves else 0)
    [(dealer,)] = _select(write_env, "SELECT dealer_id FROM cars WHERE vin = %s", (VIN_A,))
    assert dealer == ("claimant" if moves else "owner")
    assert stats["vin_owner_conflicts"] == (0 if moves else 1)
