"""S10: session lifetimes by role, and sign out everywhere.

Each acceptance bullet of card S10 (docs/auth_sms/TASKS.md) is one or more named tests:
- a shopper cookie 29 days old works and one 31 days old does not
- an admin cookie 11 h 59 min old works and one 12 h 01 min old is refused even if used a minute ago
- a dealer cookie 15 days old is refused
- an admin cookie from before the deploy is refused
- a role promotion applies at once
- a revoked cookie is refused by /api/cars/<id>/save, comments, chat, /admin and /dev
- deleted and suspended users are signed out on the next request
- sessions cost no extra query on the shopper path beyond the cache
- sign out everywhere refuses an older cookie and accepts a newer one
Offline tests use an in-memory stand-in for the auth_session_epochs table; the Postgres tests at the
end run the same rules against the real V031 table.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

import pytest

from backend.auth import session as auth_session
from backend.auth.phone import session_epoch

DAY = 86400.0
HOUR = 3600.0
CSRF = "c" * 32
NOW = 1_800_000_000.0  # a fixed "now" for the hook (2027-01-15)


# ---------------------------------------------------------------- fixtures


class FakeEpochs:
    """Stands in for auth_phone_repo's two epoch functions; counts reads."""

    def __init__(self) -> None:
        self.rows: dict[int, datetime] = {}
        self.reads = 0
        self.fail_reads = False

    def read(self, user_id, *, conn=None):
        self.reads += 1
        if self.fail_reads:
            raise RuntimeError("database down")
        return self.rows.get(int(user_id))

    def write(self, user_id, *, now, reason, conn=None):
        prev = self.rows.get(int(user_id))
        val = now if prev is None or now > prev else prev
        self.rows[int(user_id)] = val
        return val


@pytest.fixture
def epochs(monkeypatch):
    from backend.db.repositories import auth_phone_repo

    fake = FakeEpochs()
    session_epoch.clear_cache()
    monkeypatch.setattr(session_epoch, "_store_configured", lambda: True)
    monkeypatch.setattr(auth_phone_repo, "sessions_valid_after", fake.read)
    monkeypatch.setattr(auth_phone_repo, "set_sessions_valid_after", fake.write)
    yield fake
    session_epoch.clear_cache()


@pytest.fixture
def clock(monkeypatch):
    """Fixed wall clock for the hook and the cache."""

    class Clock:
        now = NOW
        mono = 1000.0

    c = Clock()
    monkeypatch.setattr(auth_session, "_now", lambda: c.now)
    monkeypatch.setattr(session_epoch, "_monotonic", lambda: c.mono)
    return c


@pytest.fixture
def main(app_factory):
    return app_factory()


def _user(username: str, role: str) -> int:
    from backend.db.users_db import save_user

    uid = save_user(username, f"{username}@example.com", "long-enough-password-1", role=role, org_id=None)
    return int(uid)


def _set_role(uid: int, role: str) -> None:
    """Change a role the way the admin edit page does (through the users_db API)."""
    from backend.db.users_db import admin_update_user, get_user_profile

    actor = _user(f"actor_for_{uid}", "admin")
    u = get_user_profile(uid)
    err = admin_update_user(
        uid,
        username=u["username"],
        email=u["email"],
        role=role,
        dealer_id="dealer-test" if role.startswith("dealership_") else None,
        actor_user_id=actor,
    )
    assert err is None, err


def _sign_in(client, uid: int, *, role: str, auth_at: float | None, seen_at: float | None = None, **extra):
    with client.session_transaction() as s:
        s.permanent = True
        s["user_id"] = uid
        s["user_role"] = role
        s["_csrf_token"] = CSRF
        if auth_at is not None:
            s[auth_session.AUTH_AT] = auth_at
            s[auth_session.AUTH_SEEN_AT] = seen_at if seen_at is not None else auth_at
            s[auth_session.AUTH_METHOD] = "password"
        s.update(extra)


def _signed_in(client) -> bool:
    with client.session_transaction() as s:
        return s.get("user_id") is not None


def _me(client):
    return client.get("/api/auth/me")


# ---------------------------------------------------------------- lifetimes


def test_shopper_cookie_29_days_old_works_and_31_days_old_does_not(main, clock):
    uid = _user("shopper_a", "general_user")
    with main.app.test_client() as c:
        _sign_in(c, uid, role="general_user", auth_at=NOW - 40 * DAY, seen_at=NOW - 29 * DAY)
        assert _me(c).status_code == 200
        with c.session_transaction() as s:  # sliding: the request moved the last-seen time
            assert s[auth_session.AUTH_SEEN_AT] == NOW
    with main.app.test_client() as c:
        _sign_in(c, uid, role="general_user", auth_at=NOW - 40 * DAY, seen_at=NOW - 31 * DAY)
        assert _me(c).status_code == 401
        assert not _signed_in(c)


def test_shopper_limit_follows_the_setting(main, clock, monkeypatch):
    monkeypatch.setenv("SESSION_DAYS_SHOPPER", "7")
    uid = _user("shopper_b", "general_user")
    with main.app.test_client() as c:
        _sign_in(c, uid, role="general_user", auth_at=NOW - 8 * DAY)
        assert _me(c).status_code == 401


def test_admin_cookie_11h59_works_and_12h01_is_refused_even_if_used_a_minute_ago(main, clock):
    uid = _user("admin_a", "admin")
    with main.app.test_client() as c:
        _sign_in(c, uid, role="admin", auth_at=NOW - (11 * HOUR + 59 * 60))
        assert _me(c).status_code == 200
    with main.app.test_client() as c:
        _sign_in(c, uid, role="admin", auth_at=NOW - (12 * HOUR + 60), seen_at=NOW - 60)
        assert _me(c).status_code == 401
        assert not _signed_in(c)


def test_admin_limit_is_absolute_not_sliding(main, clock):
    uid = _user("admin_b", "admin")
    with main.app.test_client() as c:
        _sign_in(c, uid, role="admin", auth_at=NOW - 6 * HOUR)
        assert _me(c).status_code == 200
        clock.now = NOW + 6 * HOUR + 60  # used all along, still ends 12 h after sign-in
        assert _me(c).status_code == 401


def test_dealer_cookie_15_days_old_is_refused(main, clock):
    uid = _user("dealer_a", "dealership_owner")
    with main.app.test_client() as c:
        _sign_in(c, uid, role="dealership_owner", auth_at=NOW - 13 * DAY, seen_at=NOW - 60)
        assert _me(c).status_code == 200
    with main.app.test_client() as c:
        _sign_in(c, uid, role="dealership_owner", auth_at=NOW - 15 * DAY, seen_at=NOW - 60)
        assert _me(c).status_code == 401


def test_other_staff_cookie_15_days_old_is_refused(main, clock):
    uid = _user("staff_a", "dealer_staff")
    with main.app.test_client() as c:
        _sign_in(c, uid, role="general_user", auth_at=NOW - 15 * DAY, seen_at=NOW - 60)
        assert _me(c).status_code == 401


# ---------------------------------------------------------------- cookies from before the deploy


def test_admin_cookie_from_before_the_deploy_is_refused(main, clock):
    uid = _user("admin_old", "admin")
    with main.app.test_client() as c:
        _sign_in(c, uid, role="admin", auth_at=None)
        assert _me(c).status_code == 401
        assert not _signed_in(c)


def test_staff_cookie_from_before_the_deploy_is_refused(main, clock):
    uid = _user("staff_old", "dealer_staff")
    with main.app.test_client() as c:
        _sign_in(c, uid, role="general_user", auth_at=None)
        assert _me(c).status_code == 401


def test_dev_cookie_from_before_the_deploy_is_refused(main, clock):
    with main.app.test_client() as c:
        with c.session_transaction() as s:
            s["admin_user_id"] = 1
            s["admin_username"] = "operator"
        r = c.get("/dev/")
        assert r.status_code in (301, 302)
        assert "/dev/login" in r.headers["Location"]
        with c.session_transaction() as s:
            assert "admin_user_id" not in s


@pytest.mark.parametrize("role,username", [("general_user", "shop_old"), ("dealership_member", "deal_old")])
def test_shopper_and_dealer_cookies_from_before_the_deploy_are_kept_and_stamped(main, clock, role, username):
    uid = _user(username, role)
    with main.app.test_client() as c:
        _sign_in(c, uid, role=role, auth_at=None)
        assert _me(c).status_code == 200
        with c.session_transaction() as s:
            assert s[auth_session.AUTH_AT] == NOW
            assert s[auth_session.AUTH_METHOD] == "legacy"


def test_cookie_without_auth_at_is_refused_once_a_valid_after_row_exists(main, clock, epochs):
    uid = _user("shop_rev_old", "general_user")
    epochs.rows[uid] = datetime.fromtimestamp(NOW - 30 * DAY, tz=timezone.utc)
    with main.app.test_client() as c:
        _sign_in(c, uid, role="general_user", auth_at=None)
        assert _me(c).status_code == 401


def test_malformed_or_future_auth_at_is_refused(main, clock):
    uid = _user("shop_bad", "general_user")
    for bad in ("yesterday", NOW + 2 * DAY):
        with main.app.test_client() as c:
            _sign_in(c, uid, role="general_user", auth_at=NOW)
            with c.session_transaction() as s:
                s[auth_session.AUTH_AT] = bad
            assert _me(c).status_code == 401


# ---------------------------------------------------------------- role changes, deletes, suspends


def test_role_promotion_applies_at_once(main, clock):
    uid = _user("promoted", "general_user")
    with main.app.test_client() as c:
        _sign_in(c, uid, role="general_user", auth_at=NOW - 2 * DAY, seen_at=NOW - 60)
        assert _me(c).status_code == 200  # a 2-day-old shopper session is fine
        _set_role(uid, "admin")
        assert _me(c).status_code == 401  # ...but not as an admin (12 h absolute)
        assert not _signed_in(c)


@pytest.mark.parametrize("role", ["admin", "dealership_owner", "dealer_staff"])
def test_a_phone_session_ends_when_its_account_stops_being_a_shopper(main, clock, role):
    # A text code proves a shopper only: a promoted account must sign in by its own door
    # (stage 1 review L5), even inside that role's own time limit.
    uid = _user("phone-promoted-" + role, "general_user")
    with main.app.test_client() as c:
        _sign_in(c, uid, role="general_user", auth_at=NOW - 60, seen_at=NOW - 60)
        with c.session_transaction() as s:
            s[auth_session.AUTH_METHOD] = "phone"
        assert _me(c).status_code == 200
        _set_role(uid, role)
        assert _me(c).status_code == 401
        assert not _signed_in(c)


def test_a_password_session_keeps_a_promotion_within_the_limit(main, clock):
    uid = _user("pw-promoted", "general_user")
    with main.app.test_client() as c:
        _sign_in(c, uid, role="general_user", auth_at=NOW - 60, seen_at=NOW - 60)
        _set_role(uid, "admin")
        assert _me(c).status_code == 200


def test_role_promotion_within_the_limit_records_the_new_role(main, clock):
    uid = _user("promoted2", "general_user")
    with main.app.test_client() as c:
        _sign_in(c, uid, role="general_user", auth_at=NOW - HOUR)
        _set_role(uid, "dealership_admin")
        assert _me(c).status_code == 200
        with c.session_transaction() as s:
            assert s[auth_session.AUTH_ROLE] == auth_session.CLASS_DEALER


def test_suspended_user_is_signed_out_on_the_next_request(main, clock):
    uid = _user("suspended", "general_user")
    with main.app.test_client() as c:
        _sign_in(c, uid, role="general_user", auth_at=NOW - HOUR)
        assert _me(c).status_code == 200
        from backend.db.users_db import set_user_is_active

        assert set_user_is_active(uid, active=False)
        c.get("/")
        assert not _signed_in(c)


def test_deleted_user_is_signed_out_on_the_next_request(main, clock):
    from backend.db.users_db import delete_user_by_id

    uid = _user("deleted", "general_user")
    with main.app.test_client() as c:
        _sign_in(c, uid, role="general_user", auth_at=NOW - HOUR)
        assert _me(c).status_code == 200
        ok, _err = delete_user_by_id(uid)
        assert ok
        c.get("/")
        assert not _signed_in(c)


# ---------------------------------------------------------------- revocation


def _revoke(epochs, uid: int, when: float) -> None:
    epochs.rows[uid] = datetime.fromtimestamp(when, tz=timezone.utc)
    session_epoch.forget_cached(uid)


def _probe_save(main, c, monkeypatch):
    from backend.routes import listings_api

    monkeypatch.setattr(listings_api, "get_car_by_id", lambda *a, **k: None)
    return c.post("/api/cars/1/save", headers={"X-CSRF-Token": CSRF})


def _probe_comments(main, c, monkeypatch):
    return c.post("/api/cars/1/comments", json={"body": "hello"}, headers={"X-CSRF-Token": CSRF})


def _probe_chat(main, c, monkeypatch):
    return c.post("/api/ai/chat", json={"message": "hello"}, headers={"X-CSRF-Token": CSRF})


_PROBES = {"save": _probe_save, "comments": _probe_comments, "chat": _probe_chat}


@pytest.mark.parametrize("probe", sorted(_PROBES))
def test_revoked_cookie_is_refused_by_shopper_routes(main, clock, epochs, monkeypatch, probe):
    uid = _user(f"rev_{probe}", "general_user")
    with main.app.test_client() as c:
        _sign_in(c, uid, role="general_user", auth_at=NOW - DAY)
        assert _me(c).status_code == 200
        _revoke(epochs, uid, NOW - 60)
        r = _PROBES[probe](main, c, monkeypatch)
        assert r.status_code in (400, 401, 403), r.status_code
        assert not _signed_in(c)


def test_revoked_cookie_is_refused_by_admin(main, clock, epochs):
    uid = _user("rev_admin", "admin")
    with main.app.test_client() as c:
        _sign_in(c, uid, role="admin", auth_at=NOW - HOUR)
        _revoke(epochs, uid, NOW - 60)
        r = c.get("/admin/")
        assert r.status_code in (301, 302)
        assert "/login" in r.headers["Location"]
        assert not _signed_in(c)


def test_revoked_cookie_is_refused_by_dev(main, clock, epochs):
    uid = _user("rev_dev", "admin")
    with main.app.test_client() as c:
        _sign_in(c, uid, role="admin", auth_at=NOW - HOUR)
        _revoke(epochs, uid, NOW - 60)
        r = c.get("/dev/")
        assert r.status_code in (301, 302)
        assert "/dev/login" in r.headers["Location"]
        assert not _signed_in(c)


def test_session_issued_after_valid_after_is_accepted(main, clock, epochs):
    uid = _user("rev_newer", "general_user")
    _revoke(epochs, uid, NOW - DAY)
    with main.app.test_client() as c:
        _sign_in(c, uid, role="general_user", auth_at=NOW - DAY + 1)
        assert _me(c).status_code == 200


# ---------------------------------------------------------------- cache and failure modes


def test_shopper_path_costs_no_extra_query_beyond_the_cache(main, clock, epochs):
    uid = _user("cached", "general_user")
    with main.app.test_client() as c:
        _sign_in(c, uid, role="general_user", auth_at=NOW - DAY)
        for _ in range(5):
            assert _me(c).status_code == 200
        assert epochs.reads == 1
        clock.mono += 29
        _me(c)
        assert epochs.reads == 1  # still inside the 30 s window
        clock.mono += 2
        _me(c)
        assert epochs.reads == 2


def test_admin_and_dealer_cache_is_5_seconds(main, clock, epochs):
    uid = _user("cached_admin", "admin")
    with main.app.test_client() as c:
        _sign_in(c, uid, role="admin", auth_at=NOW - HOUR)
        _me(c)
        _me(c)
        assert epochs.reads == 1
        clock.mono += 5.5
        _me(c)
        assert epochs.reads == 2


def test_request_without_a_session_cookie_makes_no_lookup(main, clock, epochs):
    with main.app.test_client() as c:
        r = c.get("/api/auth/me")
        assert r.status_code == 401
    assert epochs.reads == 0


def test_lookup_error_fails_closed_for_admin_and_dealer(main, clock, epochs):
    epochs.fail_reads = True
    for name, role in (("fc_admin", "admin"), ("fc_dealer", "dealership_owner")):
        uid = _user(name, role)
        with main.app.test_client() as c:
            _sign_in(c, uid, role=role, auth_at=NOW - HOUR)
            assert _me(c).status_code == 401, role


def test_lookup_error_fails_open_for_shoppers(main, clock, epochs):
    epochs.fail_reads = True
    uid = _user("fo_shop", "general_user")
    with main.app.test_client() as c:
        _sign_in(c, uid, role="general_user", auth_at=NOW - HOUR)
        assert _me(c).status_code == 200


def test_missing_table_is_not_a_failure(monkeypatch):
    """Before the owner applies V031 nothing can have been revoked: no row, no lock-out."""
    from backend.db.repositories import auth_phone_repo
    from backend.db.repositories.auth_phone_repo import AuthPhoneDbError

    def missing(*_a, **_k):
        raise AuthPhoneDbError("42P01")

    session_epoch.clear_cache()
    monkeypatch.setattr(session_epoch, "_store_configured", lambda: True)
    monkeypatch.setattr(auth_phone_repo, "sessions_valid_after", missing)
    monkeypatch.setattr(auth_phone_repo, "set_sessions_valid_after", missing)
    assert session_epoch.get_valid_after(7, strict=True) is None
    assert session_epoch.revoke_user_sessions(7, reason="test") is None


def test_no_postgres_configured_means_no_lookup(monkeypatch):
    from backend.db.repositories import auth_phone_repo

    session_epoch.clear_cache()
    monkeypatch.setattr(session_epoch, "_store_configured", lambda: False)
    monkeypatch.setattr(auth_phone_repo, "sessions_valid_after", lambda *a, **k: pytest.fail("queried"))
    assert session_epoch.get_valid_after(7, strict=False) is None


def test_a_read_racing_a_write_does_not_cache_the_old_answer(monkeypatch):
    from backend.db.repositories import auth_phone_repo

    session_epoch.clear_cache()
    monkeypatch.setattr(session_epoch, "_store_configured", lambda: True)

    def slow_read(user_id, **_k):
        session_epoch.forget_cached(user_id)  # a revoke lands while this read is in flight
        return None

    monkeypatch.setattr(auth_phone_repo, "sessions_valid_after", slow_read)
    assert session_epoch.get_valid_after(9, strict=True) is None
    assert 9 not in session_epoch._cache


# ---------------------------------------------------------------- the authenticator flag


def test_mfa_ok_obeys_the_12_hour_absolute_limit(main, clock):
    uid = _user("mfa_shop", "general_user")
    with main.app.test_client() as c:
        _sign_in(c, uid, role="general_user", auth_at=NOW - 13 * HOUR, seen_at=NOW - 60, mfa_ok=True)
        assert _me(c).status_code == 200
        with c.session_transaction() as s:
            assert s.get("mfa_ok") is False
    with main.app.test_client() as c:
        _sign_in(c, uid, role="general_user", auth_at=NOW - 11 * HOUR, mfa_ok=True)
        _me(c)
        with c.session_transaction() as s:
            assert s.get("mfa_ok") is True


# ---------------------------------------------------------------- sign-in stamps


def _form(c, url, data):
    with c.session_transaction() as s:
        s["_csrf_token"] = CSRF
    return c.post(url, data={"csrf_token": CSRF, **data})


def test_password_login_stamps_auth_at_method_and_role(main):
    _user("stamp_pw", "general_user")
    before = time.time()
    with main.app.test_client() as c:
        r = _form(c, "/login", {"login": "stamp_pw", "password": "long-enough-password-1"})
        assert r.status_code == 302
        with c.session_transaction() as s:
            assert s[auth_session.AUTH_AT] >= before
            assert s[auth_session.AUTH_METHOD] == "password"
            assert s[auth_session.AUTH_ROLE] == auth_session.CLASS_SHOPPER


@pytest.mark.parametrize("modname,method", [("google_oauth", "google"), ("apple_oauth", "apple")])
def test_oauth_completion_stamps_its_method(main, monkeypatch, modname, method):
    import importlib

    mod = importlib.import_module(f"backend.auth.{modname}")
    uid = _user(f"stamp_{method}", "general_user")
    monkeypatch.setattr(mod, "sync_env_admin_user_row", lambda _uid: None, raising=False)
    with main.app.test_request_context("/"):
        from flask import session

        mod._complete_login(uid)
        assert session[auth_session.AUTH_METHOD] == method
        assert session[auth_session.AUTH_AT] > 0


def test_dev_login_stamps_dev_auth_at(main, clock):
    from backend.dev.routes import _finalize_dev_session

    with main.app.test_request_context("/"):
        from flask import session

        assert _finalize_dev_session(user_id=3, username="op")
        assert session[auth_session.DEV_AUTH_AT] == NOW


def test_dev_session_ends_12_hours_after_sign_in(main, clock):
    with main.app.test_client() as c:
        with c.session_transaction() as s:
            s["admin_user_id"] = 1
            s["admin_username"] = "operator"
            s[auth_session.DEV_AUTH_AT] = NOW - (12 * HOUR + 60)
        r = c.get("/dev/")
        assert "/dev/login" in r.headers.get("Location", "")


def test_dev_session_11h59_after_sign_in_is_kept(main, clock):
    with main.app.test_client() as c:
        with c.session_transaction() as s:
            s["admin_user_id"] = 1
            s["admin_username"] = "operator"
            s[auth_session.DEV_AUTH_AT] = NOW - (11 * HOUR + 59 * 60)
        r = c.get("/dev/")
        assert "/dev/login" not in r.headers.get("Location", "")
        with c.session_transaction() as s:
            assert s.get("admin_user_id") == 1


def test_legacy_general_role_spelling_is_a_shopper_and_kept_at_deploy(main, clock):
    uid = _user("legacy_general", "general")
    with main.app.test_client() as c:
        _sign_in(c, uid, role="general_user", auth_at=None)
        assert _me(c).status_code == 200


def test_demoted_admin_cookie_role_is_corrected_at_once(main, clock):
    uid = _user("demoted", "admin")
    with main.app.test_client() as c:
        _sign_in(c, uid, role="admin", auth_at=NOW - HOUR)
        _set_role(uid, "general_user")
        assert _me(c).status_code == 200
        with c.session_transaction() as s:
            assert s["user_role"] == "general_user"


def test_cookie_lifetime_is_30_days(main):
    assert main.app.config["PERMANENT_SESSION_LIFETIME"].days == 30


def test_session_hook_is_the_first_app_hook(main):
    names = [f.__name__ for f in main.app.before_request_funcs[None]]
    assert names[0] == "enforce_session_policy"
    assert names.index("enforce_session_policy") < names.index("_csrf_mutating_requests")
    assert names.index("enforce_session_policy") < names.index("_device_cookie_before")


# ---------------------------------------------------------------- sign out everywhere and the writers


def test_sign_out_everywhere_refuses_an_older_cookie_and_accepts_a_newer_one(main, epochs):
    uid = _user("soe", "general_user")
    t0 = time.time()
    old, me = main.app.test_client(), main.app.test_client()
    _sign_in(old, uid, role="general_user", auth_at=t0 - HOUR)
    _sign_in(me, uid, role="general_user", auth_at=t0 - HOUR)
    assert _me(old).status_code == 200
    r = _form(me, "/account/profile", {"form_action": "sign_out_everywhere"})
    assert r.status_code == 200
    assert b"Every other browser is signed out" in r.data
    assert uid in epochs.rows
    assert _me(old).status_code == 401  # the older cookie is refused
    assert _me(me).status_code == 200  # this browser got a newer session
    with me.session_transaction() as s:
        assert s[auth_session.AUTH_AT] >= epochs.rows[uid].timestamp()


def test_sign_out_everywhere_does_not_restart_the_admin_12_hour_limit(main, clock, epochs):
    uid = _user("soe_admin", "admin")
    c = main.app.test_client()
    _sign_in(c, uid, role="admin", auth_at=NOW - (11 * HOUR + 50 * 60), mfa_ok=False)
    r = _form(c, "/account/profile", {"form_action": "sign_out_everywhere"})
    assert r.status_code == 200
    with c.session_transaction() as s:
        assert s.get("mfa_ok") is False  # the form never turns the authenticator flag on
    clock.now = NOW + 11 * 60  # 12 h 01 min after the ORIGINAL sign-in
    assert _me(c).status_code == 401


def test_sign_out_everywhere_raises_auth_at_to_a_newer_valid_after(main, clock, epochs, monkeypatch):
    uid = _user("soe_clamp", "general_user")
    ahead = NOW + 60  # another process wrote a later mark (clock skew between writers)
    monkeypatch.setattr(
        epochs, "write", lambda user_id, *, now, reason, conn=None: datetime.fromtimestamp(ahead, tz=timezone.utc)
    )
    from backend.db.repositories import auth_phone_repo

    monkeypatch.setattr(auth_phone_repo, "set_sessions_valid_after", epochs.write)
    c = main.app.test_client()
    _sign_in(c, uid, role="general_user", auth_at=NOW - HOUR)
    _form(c, "/account/profile", {"form_action": "sign_out_everywhere"})
    with c.session_transaction() as s:
        assert s[auth_session.AUTH_AT] == ahead
    epochs.rows[uid] = datetime.fromtimestamp(ahead, tz=timezone.utc)
    session_epoch.forget_cached(uid)
    assert _me(c).status_code == 200  # auth_at == valid_after is accepted


def test_sign_out_everywhere_says_so_when_it_cannot_store_the_mark(main, monkeypatch):
    monkeypatch.setattr(session_epoch, "_store_configured", lambda: False)
    uid = _user("soe_fail", "general_user")
    with main.app.test_client() as c:
        _sign_in(c, uid, role="general_user", auth_at=time.time() - HOUR)
        r = _form(c, "/account/profile", {"form_action": "sign_out_everywhere"})
        assert b"could not sign out your other browsers" in r.data


def test_password_change_writes_valid_after_and_keeps_this_browser(main, epochs):
    uid = _user("pwchange", "general_user")
    with main.app.test_client() as c:
        _sign_in(c, uid, role="general_user", auth_at=time.time() - HOUR)
        r = _form(
            c,
            "/account/profile",
            {
                "form_action": "password",
                "current_password": "long-enough-password-1",
                "new_password": "another-long-password-2",
                "confirm_password": "another-long-password-2",
            },
        )
        assert r.status_code == 200
        assert uid in epochs.rows
        assert _me(c).status_code == 200


def test_password_reset_writes_valid_after(main, epochs, monkeypatch):
    from backend.auth import password_reset

    uid = _user("pwreset", "general_user")
    monkeypatch.setattr(password_reset, "get_user_id_by_password_reset_token_hash", lambda _h: uid)
    with main.app.test_request_context("/"):
        ok, _msg = password_reset.complete_password_reset(token="t" * 40, new_password="brand-new-password-3")
    assert ok
    assert uid in epochs.rows


def test_admin_suspend_and_delete_write_valid_after(main, epochs):
    admin_id = _user("hub_admin", "admin")
    a = _user("hub_target_a", "general_user")
    b = _user("hub_target_b", "general_user")
    with main.app.test_client() as c:
        _sign_in(c, admin_id, role="admin", auth_at=time.time() - 60)
        for target, action in ((a, "suspend"), (b, "delete")):
            r = c.post(
                "/admin/users",
                data={"csrf_token": CSRF, "user_id": str(target), "action": action},
            )
            assert r.status_code in (302, 303), (action, r.status_code)
    assert a in epochs.rows and b in epochs.rows


def test_admin_edit_page_password_set_and_delete_write_valid_after(main, epochs):
    admin_id = _user("edit_admin", "admin")
    a = _user("edit_target_a", "general_user")
    b = _user("edit_target_b", "general_user")
    with main.app.test_client() as c:
        _sign_in(c, admin_id, role="admin", auth_at=time.time() - 60)
        r = c.post(
            f"/admin/users/{a}/edit",
            data={
                "csrf_token": CSRF,
                "action": "reset_password",
                "new_password": "a-new-long-password-9",
                "new_password_confirm": "a-new-long-password-9",
            },
        )
        assert r.status_code in (302, 303)
        r = c.post(f"/admin/users/{b}/edit", data={"csrf_token": CSRF, "action": "delete"})
        assert r.status_code in (302, 303)
    assert a in epochs.rows and b in epochs.rows


def test_session_epoch_has_one_reader_and_one_writer():
    """R4 puts a shared cache in front of these: no other module may touch the table directly."""
    import pathlib
    import subprocess

    repo = pathlib.Path(__file__).resolve().parents[2]
    out = subprocess.run(
        ["git", "grep", "-l", "-E", "sessions_valid_after\\(|auth_session_epochs", "--", "backend/*.py"],
        cwd=repo,
        capture_output=True,
        text=True,
        check=False,
    ).stdout.split()
    allowed = {"backend/auth/phone/session_epoch.py", "backend/db/repositories/auth_phone_repo.py"}
    offenders = [p for p in out if not p.startswith("backend/tests/") and p not in allowed]
    assert offenders == []


# ---------------------------------------------------------------- Postgres tier: the real V031 table

pg = pytest.mark.pg


@pg
def test_pg_revoke_then_lookup_round_trip(pg_db):
    session_epoch.clear_cache()
    assert session_epoch.get_valid_after(41, strict=True) is None
    when = datetime(2026, 11, 16, 12, 0, tzinfo=timezone.utc)
    stored = session_epoch.revoke_user_sessions(41, reason="sign_out_everywhere", now=when)
    assert stored == when.timestamp()
    assert session_epoch.get_valid_after(41, strict=True) == when.timestamp()
    # never moves backwards
    session_epoch.revoke_user_sessions(41, reason="password_change", now=datetime(2026, 1, 1, tzinfo=timezone.utc))
    assert session_epoch.get_valid_after(41, strict=False) == when.timestamp()


@pg
def test_pg_missing_table_reads_as_no_row(pg_db):
    with pg_db.connect() as conn:
        conn.execute("DROP TABLE public.auth_session_epochs")
        conn.commit()
    session_epoch.clear_cache()
    assert session_epoch.get_valid_after(41, strict=True) is None
    assert session_epoch.revoke_user_sessions(41, reason="sign_out_everywhere") is None


@pg
def test_pg_sign_out_everywhere_end_to_end(pg_db, app_factory):
    main = app_factory()
    session_epoch.clear_cache()
    uid = _user("pg_soe", "general_user")
    t0 = time.time()
    old, me = main.app.test_client(), main.app.test_client()
    _sign_in(old, uid, role="general_user", auth_at=t0 - HOUR)
    _sign_in(me, uid, role="general_user", auth_at=t0 - HOUR)
    assert _me(old).status_code == 200
    r = _form(me, "/account/profile", {"form_action": "sign_out_everywhere"})
    assert b"Every other browser is signed out" in r.data
    assert _me(old).status_code == 401
    assert _me(me).status_code == 200
