"""S5: phone sign-in tables and repository (Postgres tier), plus the offline guards.

Each acceptance bullet of card S5 is one or more named tests below:
- two concurrent approvals of one challenge succeed once
- a second identity for the same number or user fails on the index
- counts respect time windows
- wrong tries are not reset by a new challenge and lock at 5
- cleanup removes only rows past retention
- the migration applies on an empty database and twice
Test numbers are in the fictional +1 202 555 01xx range; hashes are made-up hex.
"""

from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from backend.auth.phone.provider import LineInfo
from backend.db.repositories import auth_phone_repo as repo
from backend.db.repositories.auth_phone_repo import AuthPhoneDbError, IdentityTaken, SendRow

ROOT = Path(__file__).resolve().parents[2]
V031 = ROOT / "migrations" / "V031__auth_phone.sql"
TABLES = (
    "auth_phone_identities",
    "auth_phone_challenges",
    "auth_phone_sends",
    "auth_phone_wrong_tries",
    "auth_phone_lookup_cache",
    "auth_session_epochs",
)
UTC = timezone.utc
T0 = datetime(2026, 11, 15, 15, 0, tzinfo=UTC)
PHONE_A = "+12025550101"
PHONE_B = "+12025550102"
HASH_A = "a1" * 32
HASH_B = "b2" * 32
NONCE = "nonce-browser-one"

pg = pytest.mark.pg


def _send(**kw) -> SendRow:
    base = dict(
        phone_hash=HASH_A,
        ip_hash="c3" * 32,
        ip_prefix_hash="d4" * 32,
        country="US",
        purpose="login",
        pool="new",
        has_device_cookie=True,
        device_hash="e5" * 32,
    )
    base.update(kw)
    return SendRow(**base)


def _challenge(**kw) -> repo.Challenge:
    base = dict(
        phone_e164=PHONE_A,
        phone_hash=HASH_A,
        purpose="login",
        session_nonce=NONCE,
        provider="fake",
        now=T0,
        ttl_s=300,
    )
    base.update(kw)
    return repo.create_challenge(**base)


def _identity(**kw) -> repo.Identity:
    base = dict(
        phone_e164=PHONE_A,
        phone_hash=HASH_A,
        country="US",
        now=T0,
        consent_at=T0,
        consent_text_version="1",
    )
    base.update(kw)
    return repo.insert_identity(**base)


# ------------------------------------------------------------ offline


def test_migration_file_is_additive_and_creates_the_six_tables():
    sql = V031.read_text(encoding="utf-8")
    upper = sql.upper()
    for banned in ("DROP ", "ALTER TABLE", "DELETE FROM", "TRUNCATE", "UPDATE ", "REFERENCES ", "CONCURRENTLY"):
        assert banned not in upper, banned
    assert upper.count("CREATE TABLE IF NOT EXISTS") == len(TABLES)
    for table in TABLES:
        assert f"CREATE TABLE IF NOT EXISTS public.{table} (" in sql
    assert "%" not in sql  # the runner may pass parameters; a stray percent sign would break it


def test_migration_never_stores_a_code_and_drops_sessions_valid_after_from_identities():
    sql = V031.read_text(encoding="utf-8").lower()
    columns = [line.split()[0] for line in sql.splitlines() if line.startswith("    ") and line.split()]
    assert not any("code" in c for c in columns if not c.startswith("constraint"))
    identities = sql.split("create table if not exists public.auth_phone_identities", 1)[1].split(");", 1)[0]
    assert "sessions_valid_after" not in identities
    assert "dormant_user_id" in identities and "detached_at" in identities


def test_repository_vocabulary_matches_the_migration_checks():
    sql = V031.read_text(encoding="utf-8")
    for word in repo.CHALLENGE_PURPOSES + repo.POOLS + repo.CHANNELS + repo.SEND_OUTCOMES + repo.CHALLENGE_ENDS:
        assert f"'{word}'" in sql, word


def test_unknown_send_filter_is_refused_before_any_sql():
    with pytest.raises(ValueError):
        repo._send_where(T0, None, {"outcome; DROP TABLE x": 1}, None, None)


def test_bad_purpose_pool_or_outcome_is_refused_before_any_sql():
    with pytest.raises(ValueError):
        repo.record_send(_send(pool="vip"), now=T0, conn=object())
    with pytest.raises(ValueError):
        repo.record_send(_send(purpose="spam"), now=T0, conn=object())
    with pytest.raises(ValueError):
        repo.set_send_outcome(1, "delivered", conn=object())


# ------------------------------------------------------------ migration (Postgres)


@pg
def test_migration_applies_on_an_empty_database_and_twice():
    import psycopg

    from backend.tests import pg_harness as pgh

    admin = pgh.admin_dsn_or_skip()
    sql = V031.read_text(encoding="utf-8")
    with pgh.scratch_db(admin) as dsn:
        with psycopg.connect(dsn) as conn:
            conn.execute(sql)
            conn.commit()
            conn.execute(sql)
            conn.commit()
            found = {
                r[0]
                for r in conn.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
                ).fetchall()
            }
            fks = conn.execute(
                "SELECT count(*) FROM pg_constraint WHERE contype = 'f' AND connamespace = 'public'::regnamespace"
            ).fetchone()[0]
    assert found == set(TABLES)
    assert fks == 0  # users still live in SQLite: no foreign key


@pg
def test_migration_is_part_of_the_chain_template(pg_db):
    with pg_db.connect() as conn:
        n = conn.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = ANY(%s)",
            (list(TABLES),),
        ).fetchone()[0]
        applied = conn.execute("SELECT max(version) FROM schema_migrations").fetchone()[0]
    assert n == len(TABLES)
    assert applied >= 31


@pg
def test_e164_check_refuses_a_malformed_number(pg_db):
    with pytest.raises(AuthPhoneDbError) as exc:
        _identity(phone_e164="2025550101")
    assert exc.value.sqlstate == "23514"  # check_violation
    assert exc.value.constraint == "auth_phone_e164_shape"


# ------------------------------------------------------------ challenges


@pg
def test_two_concurrent_approvals_of_one_challenge_succeed_once(pg_db):
    ch = _challenge()
    barrier = threading.Barrier(8)
    results: list[object] = []
    lock = threading.Lock()

    def approve():
        barrier.wait()
        got = repo.close_approved(ch.public_id, NONCE, now=T0 + timedelta(seconds=30))
        with lock:
            results.append(got)

    threads = [threading.Thread(target=approve) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    winners = [r for r in results if r is not None]
    assert len(winners) == 1
    assert winners[0].phone_e164 == PHONE_A  # the caller still learns the number
    assert winners[0].status == "approved"
    with pg_db.connect() as conn:
        status, phone = conn.execute(
            "SELECT status, phone_e164 FROM auth_phone_challenges WHERE public_id = %s", (ch.public_id,)
        ).fetchone()
    assert (status, phone) == ("approved", None)  # the stored copy is cleared


@pg
def test_approval_needs_the_same_browser_and_an_unexpired_code(pg_db):
    ch = _challenge()
    assert repo.close_approved(ch.public_id, "another-browser", now=T0) is None
    assert repo.close_approved(ch.public_id, NONCE, now=T0 + timedelta(seconds=300)) is None  # exactly 5 minutes
    assert repo.close_approved(ch.public_id, NONCE, now=T0 + timedelta(seconds=299)) is not None


@pg
def test_fetch_challenge_needs_the_nonce(pg_db):
    ch = _challenge()
    assert repo.fetch_challenge(ch.public_id, NONCE).public_id == ch.public_id
    assert repo.fetch_challenge(ch.public_id, "another-browser") is None
    assert repo.fetch_challenge("no-such-id", NONCE) is None


@pg
def test_check_attempts_are_reserved_and_stop_at_five(pg_db):
    ch = _challenge()
    got = [repo.reserve_attempt(ch.public_id, NONCE, now=T0) for _ in range(6)]
    assert got == [1, 2, 3, 4, 5, None]
    assert repo.refund_attempt(ch.public_id, NONCE)
    assert repo.reserve_attempt(ch.public_id, NONCE, now=T0) == 5
    assert repo.reserve_attempt(ch.public_id, "another-browser", now=T0) is None


@pg
def test_ending_a_challenge_clears_its_number(pg_db):
    ch = _challenge()
    assert repo.finish_challenge(ch.public_id, "cancelled", now=T0, session_nonce=NONCE)
    assert not repo.finish_challenge(ch.public_id, "failed", now=T0)
    with pg_db.connect() as conn:
        assert conn.execute(
            "SELECT status, phone_e164 FROM auth_phone_challenges WHERE public_id = %s", (ch.public_id,)
        ).fetchone() == ("cancelled", None)


@pg
def test_resend_waits_and_restarts_the_code_life(pg_db):
    ch = _challenge()
    assert repo.note_resend(ch.public_id, NONCE, now=T0 + timedelta(seconds=59), min_wait_s=60, ttl_s=300) is None
    assert repo.note_resend(ch.public_id, NONCE, now=T0 + timedelta(seconds=60), min_wait_s=60, ttl_s=300) == 2
    again = repo.fetch_challenge(ch.public_id, NONCE)
    assert again.expires_at == T0 + timedelta(seconds=360)


# ------------------------------------------------------------ identities


@pg
def test_a_second_identity_for_the_same_number_fails_on_the_index(pg_db):
    _identity(user_id=7, state="active")
    with pytest.raises(IdentityTaken):
        _identity(user_id=8, state="active")
    with pytest.raises(IdentityTaken):
        _identity()  # reserving, no user: still the same number


@pg
def test_a_second_identity_for_the_same_user_fails_on_the_index(pg_db):
    _identity(user_id=7, state="active")
    with pytest.raises(IdentityTaken):
        _identity(phone_e164=PHONE_B, phone_hash=HASH_B, user_id=7, state="active")


@pg
def test_two_reserving_identities_without_a_user_can_coexist(pg_db):
    _identity()
    _identity(phone_e164=PHONE_B, phone_hash=HASH_B)
    assert repo.find_identity_by_phone(PHONE_B).user_id is None


@pg
def test_identity_found_by_number_or_user_and_activated_once(pg_db):
    ident = _identity()
    assert ident.state == "reserving"
    assert repo.activate_identity(ident.id, 42, now=T0)
    assert not repo.activate_identity(ident.id, 43, now=T0)
    by_user = repo.find_identity_by_user(42)
    assert by_user is not None and by_user.phone_e164 == PHONE_A and by_user.state == "active"
    assert repo.find_identity_by_phone(PHONE_A).user_id == 42
    assert repo.find_identity_by_user(43) is None


@pg
def test_an_active_identity_must_have_a_user(pg_db):
    with pytest.raises(AuthPhoneDbError) as exc:
        _identity(state="active")
    assert exc.value.sqlstate == "23514"


@pg
def test_change_number_moves_the_identity_and_refuses_a_taken_number(pg_db):
    _identity(user_id=7, state="active")
    _identity(phone_e164=PHONE_B, phone_hash=HASH_B, user_id=8, state="active")
    with pytest.raises(IdentityTaken):
        repo.change_number(7, phone_e164=PHONE_B, phone_hash=HASH_B, country="US", now=T0)
    moved = repo.change_number(7, phone_e164="+12025550103", phone_hash="f6" * 32, country="US", now=T0)
    assert moved is not None and moved.phone_e164 == "+12025550103"
    assert repo.change_number(99, phone_e164="+12025550104", phone_hash="0a" * 32, country="US", now=T0) is None


@pg
def test_detach_dormant_keeps_the_old_account_and_frees_the_number(pg_db):
    ident = _identity(user_id=7, state="active")
    assert repo.detach_dormant(ident.id, now=T0)
    row = repo.find_identity_by_phone(PHONE_A)
    assert (row.user_id, row.dormant_user_id, row.state) == (None, 7, "reserving")
    assert repo.activate_identity(ident.id, 50, now=T0)
    assert repo.max_identity_user_id() == 50


@pg
def test_max_identity_user_id_counts_dormant_accounts(pg_db):
    assert repo.max_identity_user_id() == 0
    ident = _identity(user_id=90, state="active")
    repo.detach_dormant(ident.id, now=T0)
    assert repo.max_identity_user_id() == 90


# ------------------------------------------------------------ sends and windows


@pg
def test_send_counts_respect_time_windows(pg_db):
    repo.record_send(_send(), now=T0 - timedelta(minutes=16))
    repo.record_send(_send(), now=T0 - timedelta(minutes=14))
    repo.record_send(_send(), now=T0 - timedelta(minutes=1))
    repo.record_send(_send(phone_hash=HASH_B), now=T0 - timedelta(minutes=1))
    repo.record_send(_send(), now=T0)  # the window's end is exclusive
    since = T0 - timedelta(minutes=15)
    assert repo.count_sends(since=since, until=T0, phone_hash=HASH_A) == 2
    assert repo.count_sends(since=since, until=T0) == 3
    assert repo.count_sends(since=T0 - timedelta(days=1), phone_hash=HASH_A) == 4
    assert repo.count_sends(since=T0 + timedelta(seconds=1)) == 0


@pg
def test_send_counts_filter_by_ip_device_prefix_pool_and_outcome(pg_db):
    repo.record_send(_send(outcome="sent"), now=T0)
    repo.record_send(_send(outcome="blocked_cap", pool="known"), now=T0)
    repo.record_send(_send(outcome="unavailable", device_hash=None, has_device_cookie=False), now=T0)
    since = T0 - timedelta(hours=1)
    assert repo.count_sends(since=since, ip_hash="c3" * 32) == 3
    assert repo.count_sends(since=since, ip_prefix_hash="d4" * 32, pool="known") == 1
    assert repo.count_sends(since=since, device_hash="e5" * 32) == 2
    assert repo.count_sends(since=since, has_device_cookie=False) == 1
    assert repo.count_sends(since=since, exclude_outcomes=repo.LIMIT_FREE_OUTCOMES) == 1
    assert repo.count_sends(since=since, outcomes=("sent",)) == 1


@pg
def test_cost_sums_text_and_lookup_inside_the_window(pg_db):
    repo.record_send(_send(est_cost_cents=6, lookup_cost_cents=1, outcome="sent"), now=T0 - timedelta(days=2))
    repo.record_send(_send(est_cost_cents=6, lookup_cost_cents=1, outcome="sent"), now=T0)
    repo.record_send(_send(est_cost_cents=0, lookup_cost_cents=Decimal("0.5"), outcome="blocked_voip"), now=T0)
    assert repo.sum_cost_cents(since=T0 - timedelta(hours=1)) == Decimal("7.5")
    assert repo.sum_cost_cents(since=T0 - timedelta(days=3)) == Decimal("14.5")


@pg
def test_pending_send_moves_to_its_outcome_once(pg_db):
    send_id = repo.record_send(_send(est_cost_cents=6, lookup_cost_cents=1), now=T0)
    assert repo.set_send_outcome(send_id, "provider_error", est_cost_cents=0)
    assert not repo.set_send_outcome(send_id, "sent")
    assert repo.sum_cost_cents(since=T0 - timedelta(minutes=1)) == Decimal("1")  # lookup cost stays spent


@pg
def test_reserve_send_lets_exactly_one_through_at_cap_minus_one(pg_db):
    cap = 5
    for _ in range(cap - 1):
        repo.record_send(_send(outcome="sent"), now=T0 - timedelta(minutes=5))
    since = T0 - timedelta(hours=1)

    def decide(window):
        used = window.count(since=since, exclude_outcomes=repo.LIMIT_FREE_OUTCOMES)
        return "blocked_cap" if used >= cap else None

    barrier = threading.Barrier(12)
    outcomes: list[str] = []
    lock = threading.Lock()

    def go():
        barrier.wait()
        res = repo.reserve_send(_send(est_cost_cents=6), now=T0, decide=decide)
        with lock:
            outcomes.append(res.outcome)

    threads = [threading.Thread(target=go) for _ in range(12)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert outcomes.count("pending") == 1
    assert outcomes.count("blocked_cap") == 11
    # refusals are recorded, with no cost
    assert repo.count_sends(since=since, outcomes=("blocked_cap",)) == 11
    assert repo.sum_cost_cents(since=T0 - timedelta(minutes=1)) == Decimal("6")


@pg
def test_reserve_send_rolls_back_and_raises_when_decide_fails(pg_db):
    def boom(window):
        window.count(since=T0)
        raise RuntimeError("decide failed")

    with pytest.raises(RuntimeError):
        repo.reserve_send(_send(), now=T0, decide=boom)
    assert repo.count_sends(since=T0 - timedelta(days=1)) == 0
    with pytest.raises(ValueError):
        repo.reserve_send(_send(), now=T0, decide=lambda w: "sent")
    assert repo.count_sends(since=T0 - timedelta(days=1)) == 0


# ------------------------------------------------------------ wrong tries


@pg
def test_wrong_tries_are_not_reset_by_a_new_challenge_and_lock_at_5(pg_db):
    first = _challenge()
    for i in range(3):
        state = repo.add_wrong_try(HASH_A, now=T0 + timedelta(seconds=i))
    assert state.tries == 3 and not state.locked
    repo.finish_challenge(first.public_id, "cancelled", now=T0)
    _challenge(now=T0 + timedelta(seconds=10))  # a new code for the same number
    assert repo.wrong_try_state(HASH_A, now=T0 + timedelta(seconds=11)).tries == 3
    state = repo.add_wrong_try(HASH_A, now=T0 + timedelta(seconds=12))
    assert state.tries == 4 and not state.locked
    state = repo.add_wrong_try(HASH_A, now=T0 + timedelta(seconds=13))
    assert state.tries == 5 and state.locked
    assert state.locked_until == T0 + timedelta(seconds=13, minutes=15)
    # still locked after another new code, until the 15 minutes pass
    _challenge(now=T0 + timedelta(minutes=2))
    assert repo.wrong_try_state(HASH_A, now=T0 + timedelta(minutes=14)).locked
    assert not repo.wrong_try_state(HASH_A, now=T0 + timedelta(minutes=16)).locked
    # another number is not affected
    assert repo.wrong_try_state(HASH_B, now=T0).tries == 0


@pg
def test_concurrent_wrong_tries_are_all_counted(pg_db):
    barrier = threading.Barrier(6)

    def go():
        barrier.wait()
        repo.add_wrong_try(HASH_A, now=T0, max_tries=100)

    threads = [threading.Thread(target=go) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert repo.wrong_try_state(HASH_A, now=T0).tries == 6


@pg
def test_wrong_try_count_restarts_after_the_lock_and_after_the_window(pg_db):
    for _ in range(5):
        repo.add_wrong_try(HASH_A, now=T0)
    after = repo.add_wrong_try(HASH_A, now=T0 + timedelta(minutes=16))
    assert after.tries == 1 and not after.locked
    repo.add_wrong_try(HASH_B, now=T0)
    later = repo.add_wrong_try(HASH_B, now=T0 + timedelta(hours=25))
    assert later.tries == 1


# ------------------------------------------------------------ session epochs and lookup cache


@pg
def test_sessions_valid_after_is_set_and_never_moves_back(pg_db):
    assert repo.sessions_valid_after(7) is None
    assert repo.set_sessions_valid_after(7, now=T0, reason="sign_out_everywhere") == T0
    assert repo.set_sessions_valid_after(7, now=T0 - timedelta(hours=1), reason="number_change") == T0
    assert repo.set_sessions_valid_after(7, now=T0 + timedelta(hours=1), reason="claim") == T0 + timedelta(hours=1)
    assert repo.sessions_valid_after(8) is None


@pg
def test_lookup_cache_keeps_a_classification_for_its_ttl(pg_db):
    clock = {"now": T0}
    cache = repo.PgLookupCache(clock=lambda: clock["now"])
    info = LineInfo(ok=False, line_type="nonFixedVoip", country="US", reason="voip")
    cache.put(HASH_A, info, 30 * 86400)
    assert cache.get(HASH_A) == info
    assert cache.get(HASH_B) is None
    clock["now"] = T0 + timedelta(days=30)
    assert cache.get(HASH_A) is None


# ------------------------------------------------------------ retention


@pg
def test_cleanup_removes_only_rows_past_retention(pg_db):
    now = T0
    old_end = _challenge(now=now - timedelta(days=9))
    repo.finish_challenge(old_end.public_id, "failed", now=now - timedelta(days=8))
    young_end = _challenge(now=now - timedelta(days=6))
    repo.finish_challenge(young_end.public_id, "failed", now=now - timedelta(days=6))
    stale_open = _challenge(now=now - timedelta(minutes=10))  # expired 5 minutes ago, still open
    live_open = _challenge(now=now - timedelta(minutes=1))

    repo.record_send(_send(), now=now - timedelta(days=31))
    repo.record_send(_send(), now=now - timedelta(days=29))

    repo.add_wrong_try(HASH_A, now=now - timedelta(hours=30))  # never locked, window long over
    for _ in range(5):
        repo.add_wrong_try(HASH_B, now=now - timedelta(hours=23))  # locked; unlocked 22h45m ago

    cache = repo.PgLookupCache(clock=lambda: now - timedelta(days=31))
    cache.put("aa" * 32, LineInfo(True, "mobile", "US", "ok"), 30 * 86400)  # expired a day ago
    repo.PgLookupCache(clock=lambda: now).put("bb" * 32, LineInfo(True, "mobile", "US", "ok"), 30 * 86400)

    _identity(user_id=7, state="active", now=now - timedelta(days=400))
    repo.set_sessions_valid_after(7, now=now - timedelta(days=400), reason="old")

    out = repo.cleanup(now=now)
    assert out == {"challenges_expired": 1, "challenges": 1, "sends": 1, "wrong_tries": 1, "lookup_cache": 1}

    with pg_db.connect() as conn:
        left = {r[0]: (r[1], r[2]) for r in conn.execute(
            "SELECT public_id, status, phone_e164 FROM auth_phone_challenges"
        ).fetchall()}
        sends = conn.execute("SELECT count(*) FROM auth_phone_sends").fetchone()[0]
        tries = [r[0] for r in conn.execute("SELECT phone_hash FROM auth_phone_wrong_tries").fetchall()]
        cached = [r[0] for r in conn.execute("SELECT phone_hash FROM auth_phone_lookup_cache").fetchall()]
        idents = conn.execute("SELECT count(*) FROM auth_phone_identities").fetchone()[0]
        epochs = conn.execute("SELECT count(*) FROM auth_session_epochs").fetchone()[0]
    assert old_end.public_id not in left
    assert left[young_end.public_id] == ("failed", None)
    assert left[stale_open.public_id] == ("expired", None)  # ended and its number cleared
    assert left[live_open.public_id] == ("open", PHONE_A)
    assert sends == 1
    assert tries == [HASH_B]
    assert cached == ["bb" * 32]
    assert idents == 1 and epochs == 1  # never deleted by age


# ------------------------------------------------------------ registry (rule 5)


def test_the_registry_claims_v031_for_the_auth_phone_file():
    import re

    text = (ROOT / "docs" / "NEXT_PHASES_PLAN_2026_10.md").read_text(encoding="utf-8")
    rows = dict(re.findall(r"^\s*\| (V\d{3}) \| (.+?) \| .+? \|\s*$", text, flags=re.M))
    assert "V031__auth_phone.sql" in rows["V031"] and "reserved" not in rows["V031"]
    assert "V032" in rows and "users.auth_epoch" in rows["V033"]


def test_the_schema_files_run_v001_to_v031_with_no_gap():
    from backend.scripts.migrate import discover_migrations

    migrations = discover_migrations(ROOT / "migrations")
    versions = [m.version for m in migrations]
    assert versions[:31] == list(range(1, 32))
    assert [m.path.name for m in migrations if m.version == 31] == ["V031__auth_phone.sql"]


# ------------------------------------------------------------ rule 8: no number, code or hash leaves in an error


_DIGIT_RUN = __import__("re").compile(r"\d{7,}")
_HEX64 = __import__("re").compile(r"[0-9a-f]{64}")


def _assert_clean(text: str) -> None:
    assert PHONE_A not in text and PHONE_A[2:] not in text and "000000" not in text
    assert not _DIGIT_RUN.search(text), text
    assert not _HEX64.search(text), text


@pg
def test_database_errors_carry_no_number_or_hash(pg_db, caplog):
    import traceback

    caplog.set_level("DEBUG")
    _identity(user_id=7, state="active")
    triggers = [
        lambda: _identity(phone_e164="2025550101"),  # e164 check
        lambda: _identity(phone_e164=PHONE_B, phone_hash=HASH_B, state="active"),  # active needs a user
        lambda: _identity(user_id=8, state="active"),  # unique number
        lambda: repo.record_send(_send(country=None), now=T0),  # not-null: the row would be in the message
        lambda: repo.create_challenge(
            phone_e164="12025550101", phone_hash=HASH_A, purpose="login", session_nonce=NONCE,
            provider="fake", now=T0, ttl_s=300,
        ),  # challenge e164 check
    ]
    for trigger in triggers:
        with pytest.raises(Exception) as info:
            trigger()
        exc = info.value
        assert isinstance(exc, (AuthPhoneDbError, IdentityTaken))
        _assert_clean(str(exc))
        _assert_clean(repr(exc))
        _assert_clean("".join(traceback.format_exception_only(exc)))
        # the driver's own error (whose message repeats the row) is not chained into the report at all
        assert exc.__cause__ is None and exc.__context__ is None
    _assert_clean(caplog.text)


@pg
def test_a_none_filter_counts_rows_without_that_value(pg_db):
    repo.record_send(_send(), now=T0)
    repo.record_send(_send(device_hash=None, has_device_cookie=False), now=T0)
    since = T0 - timedelta(minutes=1)
    assert repo.count_sends(since=since, device_hash=None) == 1
    assert repo.count_sends(since=since) == 2


@pg
def test_cleanup_keeps_a_wrong_try_row_that_is_still_locked(pg_db):
    hash_c = "c7" * 32
    repo.add_wrong_try(hash_c, now=T0 - timedelta(hours=23, minutes=50))
    for _ in range(4):
        repo.add_wrong_try(hash_c, now=T0 - timedelta(minutes=5))
    repo.cleanup(now=T0)
    assert repo.wrong_try_state(hash_c, now=T0).locked
