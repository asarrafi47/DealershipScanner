"""Session and login updates.

1. /dev operator login: per-account login attempt limit (app-login window and limits) and
   consistent login handling.
2. Logout (/logout, /api/auth/logout, /dev/logout) ends the server-side session.
3. /dev sessions are re-checked against the operator store (row, password fingerprint).
4. /dev/manifest console: console requests follow DEV_IP_ALLOWLIST, login rate limits, session expiry.
5. No Server-Timing on the sign-in, sign-up and recovery routes.

Offline tests use the in-memory stand-in for the valid_after table from test_session_lifetime;
the ``pg`` tests at the end run against real Postgres.
"""

from __future__ import annotations

import time
from datetime import datetime

import pytest

from backend.auth import session as auth_session
from backend.auth.phone import session_epoch

CSRF = "c" * 32
OP_PASSWORD = "operator-long-password-1"
APP_PASSWORD = "long-enough-password-1"


class FakeEpochs:
    def __init__(self) -> None:
        self.rows: dict[int, datetime] = {}

    fail_reads = False

    def read(self, user_id, *, conn=None):
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
def main(app_factory):
    auth_session.clear_dev_operator_cache()
    m = app_factory()
    yield m
    auth_session.clear_dev_operator_cache()


@pytest.fixture
def rl_clock(monkeypatch):
    import backend.utils.ip_rate_limit as rl

    clock = [10_000.0]
    monkeypatch.setattr(rl.time, "monotonic", lambda: clock[0])
    return clock


def _operator(username: str = "op1") -> int:
    from backend.db.admin_users_db import authenticate_admin, save_dev_admin_user

    save_dev_admin_user(username, f"{username}@example.com", OP_PASSWORD)
    return authenticate_admin(username, OP_PASSWORD)[0]


def _post(client, url, data, *, ip="10.1.1.1"):
    with client.session_transaction() as s:
        s["_csrf_token"] = CSRF
    return client.post(url, data={"csrf_token": CSRF, **data}, environ_overrides={"REMOTE_ADDR": ip})


def _dev_login(client, login="op1", password=OP_PASSWORD, *, ip="10.1.1.1"):
    return _post(client, "/dev/login", {"login": login, "password": password}, ip=ip)


def _share_session(src, dst) -> None:
    """Give ``dst`` the same session cookie as ``src`` (a second browser tab on one session)."""
    cookie = src.get_cookie("session")
    assert cookie is not None
    dst.set_cookie("session", cookie.value)


def _on_dev(client) -> bool:
    r = client.get("/dev/")
    return "/dev/login" not in (r.headers.get("Location") or "")


# ---------------------------------------------------------------- 1. /dev operator login


def test_dev_login_locks_the_account_across_ips_then_reopens_after_the_window(main, rl_clock):
    _operator()
    c = main.app.test_client()
    for i in range(10):
        r = _dev_login(c, password="wrong-password", ip=f"10.2.0.{i + 1}")
        assert r.status_code == 200 and b"Invalid" in r.data
    r = _dev_login(c, ip="10.2.0.99")  # the right password, but the account is locked
    assert r.status_code == 429
    assert b"Too many failed attempts" in r.data
    rl_clock[0] += 901.0  # the app login's 15-minute window has passed
    r = _dev_login(c, ip="10.2.0.99")
    assert r.status_code in (302, 303) and "/dev/login" not in r.headers["Location"]


def test_dev_login_success_refunds_its_slot(main, rl_clock):
    _operator()
    c = main.app.test_client()
    for i in range(9):
        _dev_login(c, password="wrong-password", ip=f"10.3.0.{i + 1}")
    for _ in range(3):  # each success gives its slot back, so the operator is never locked out by it
        r = _dev_login(main.app.test_client())
        assert r.status_code in (302, 303)


def test_dev_login_consistent_handling(main, monkeypatch):
    from backend.db import admin_users_db

    seen: list[str] = []
    real = admin_users_db.verify_or_legacy
    monkeypatch.setattr(admin_users_db, "verify_or_legacy", lambda pw, h: seen.append(h) or real(pw, h))
    assert admin_users_db.authenticate_admin("nobody-here", "some-password") is None
    assert seen == [admin_users_db._dummy_hash()]


# ---------------------------------------------------------------- 2. logout ends the session


def _app_login(client, username):
    r = _post(client, "/login", {"login": username, "password": APP_PASSWORD})
    assert r.status_code == 302


@pytest.mark.parametrize("logout", ["/logout", "/api/auth/logout"])
def test_logout_ends_the_server_side_session(main, epochs, logout):
    from backend.db.users_db import save_user

    uid = save_user("leaver", "leaver@example.com", APP_PASSWORD, role="general_user")
    me, other = main.app.test_client(), main.app.test_client()
    _app_login(me, "leaver")
    _share_session(me, other)
    assert other.get("/api/auth/me").status_code == 200
    if logout == "/logout":
        _post(me, logout, {})
    else:
        with me.session_transaction() as s:
            s["_csrf_token"] = CSRF
        me.post(logout, headers={"X-CSRF-Token": CSRF})
    assert int(uid) in epochs.rows
    assert other.get("/api/auth/me").status_code == 401
    # Signing in again afterwards works.
    again = main.app.test_client()
    _app_login(again, "leaver")
    assert again.get("/api/auth/me").status_code == 200


def test_dev_logout_ends_the_operator_session(main, epochs):
    op = _operator()
    me, other = main.app.test_client(), main.app.test_client()
    assert _dev_login(me).status_code in (302, 303)
    _share_session(me, other)
    assert _on_dev(other)
    _post(me, "/dev/logout", {})
    assert session_epoch.dev_operator_epoch_id(op) in epochs.rows
    assert not _on_dev(other)
    fresh = main.app.test_client()
    assert _dev_login(fresh).status_code in (302, 303)
    assert _on_dev(fresh)


# ---------------------------------------------------------------- 3. /dev sessions re-checked


def test_deleted_operator_is_signed_out(main):
    from backend.db.dev_users_sqlite import get_dev_users_conn

    op = _operator()
    c = main.app.test_client()
    _dev_login(c)
    assert _on_dev(c)
    conn = get_dev_users_conn()
    assert op
    conn.execute("DELETE FROM admin_users WHERE username = 'op1'")
    conn.commit()
    conn.close()
    auth_session.clear_dev_operator_cache()
    assert not _on_dev(c)
    with c.session_transaction() as s:
        assert "admin_user_id" not in s


def test_operator_password_change_ends_earlier_sessions(main):
    from backend.db.admin_users_db import hash_password
    from backend.db.dev_users_sqlite import get_dev_users_conn

    op = _operator()
    c = main.app.test_client()
    _dev_login(c)
    assert _on_dev(c)
    conn = get_dev_users_conn()
    assert op
    new_hash = hash_password("a-new-password-2")
    assert "'" not in new_hash
    conn.execute(f"UPDATE admin_users SET password = '{new_hash}' WHERE username = 'op1'")
    conn.commit()
    conn.close()
    auth_session.clear_dev_operator_cache()
    assert not _on_dev(c)
    with c.session_transaction() as s:
        assert "dev_pw_fp" not in s  # the cookie never carries the hash, and is cleared here
    assert _dev_login(main.app.test_client(), password="a-new-password-2").status_code in (302, 303)


def test_dev_session_fails_closed_when_the_operator_store_fails(main, monkeypatch):
    from backend.db import admin_users_db

    _operator()
    c = main.app.test_client()
    _dev_login(c)
    assert _on_dev(c)
    auth_session.clear_dev_operator_cache()

    def boom(_uid):
        raise RuntimeError("store down")

    monkeypatch.setattr(admin_users_db, "get_admin_by_id", boom)
    assert not _on_dev(c)


def test_operator_cookie_holds_a_fingerprint_not_the_hash(main):
    from backend.db.admin_users_db import get_admin_by_id

    op = _operator()
    c = main.app.test_client()
    _dev_login(c)
    with c.session_transaction() as s:
        fp = s["dev_pw_fp"]
    stored = get_admin_by_id(op)["password"]
    assert fp and fp not in stored and len(fp) == 32


# ---------------------------------------------------------------- 4. /dev/manifest console


@pytest.fixture
def console(app_factory, monkeypatch):
    monkeypatch.setenv("DEV_CONSOLE", "1")
    monkeypatch.setenv("DEV_CONSOLE_SECRET", "console-secret-xyz")
    return app_factory()


def _console_login(client, secret="console-secret-xyz", ip="10.4.0.1"):
    return _post(client, "/dev/manifest/login", {"secret": secret}, ip=ip)


@pytest.mark.parametrize("path", ["/dev/manifest/login", "/dev/manifest", "/api/dev/dealers"])
def test_console_follows_the_ip_access_setting(console, monkeypatch, path):
    monkeypatch.setenv("DEV_IP_ALLOWLIST", "10.9.9.9")
    c = console.app.test_client()
    r = c.get(path, environ_overrides={"REMOTE_ADDR": "10.4.0.1"})
    assert r.status_code == 403
    r = c.get(path, environ_overrides={"REMOTE_ADDR": "10.9.9.9"})
    assert r.status_code != 403


def test_console_login_is_rate_limited_per_ip(console, rl_clock):
    c = console.app.test_client()
    codes = [_console_login(c, secret="wrong").status_code for _ in range(11)]
    assert codes[:10] == [200] * 10 and codes[10] == 429
    assert _console_login(c, ip="10.4.0.2").status_code in (302, 303)  # another address still can


def test_console_login_has_a_global_cap(console, rl_clock):
    c = console.app.test_client()
    codes = [_console_login(c, secret="wrong", ip=f"10.5.{i // 250}.{i % 250 + 1}").status_code for i in range(61)]
    assert codes[:60] == [200] * 60 and codes[60] == 429


def test_console_session_needs_a_start_time(console):
    c = console.app.test_client()
    with c.session_transaction() as s:
        s["dev_console_ok"] = True
    assert c.get("/api/dev/dealers").status_code == 401


def test_console_session_expires_after_the_admin_session_hours(console, monkeypatch):
    c = console.app.test_client()
    assert _console_login(c).status_code in (302, 303)
    with c.session_transaction() as s:
        assert s["dev_console_at"] > 0
    assert c.get("/api/dev/dealers").status_code != 401
    later = time.time() + 12 * 3600 + 60
    monkeypatch.setattr(auth_session, "_now", lambda: later)
    assert c.get("/api/dev/dealers").status_code == 401


# ---------------------------------------------------------------- 5. Server-Timing


@pytest.mark.parametrize(
    "method,path",
    [
        ("get", "/login"),
        ("get", "/register"),
        ("get", "/forgot-password"),
        ("get", "/reset-password"),
        ("post", "/api/auth/login"),
        ("post", "/api/auth/register"),
        ("get", "/dev/login"),
        ("get", "/dev/manifest/login"),
        ("get", "/auth/google"),
        ("get", "/auth/google/callback"),
        ("get", "/auth/apple"),
        ("get", "/auth/apple/callback"),
        ("get", "/dealer/login"),
    ],
)
def test_auth_doors_carry_no_server_timing(app_factory, method, path):
    main = app_factory(SERVER_TIMING="1", PASSWORD_RESET_ENABLED="1")
    c = main.app.test_client()
    r = getattr(c, method)(path, json={} if method == "post" else None)
    assert r.mimetype in ("text/html", "application/json")
    assert "Server-Timing" not in r.headers


def test_other_pages_keep_server_timing(app_factory):
    main = app_factory(SERVER_TIMING="1")
    r = main.app.test_client().get("/api/auth/csrf")
    assert "Server-Timing" in r.headers


# ---------------------------------------------------------------- more session cases


def _from_epoch(ts: float):
    from datetime import timezone

    return datetime.fromtimestamp(ts, tz=timezone.utc)


def test_console_logout_ends_the_console_session(console, epochs):
    me, other = console.app.test_client(), console.app.test_client()
    assert _console_login(me).status_code in (302, 303)
    _share_session(me, other)
    assert other.get("/dev/manifest").status_code == 200
    _post(me, "/dev/manifest/logout", {}, ip="10.4.0.1")
    r = other.get("/dev/manifest")
    assert r.status_code in (301, 302) and "/dev/manifest/login" in r.headers["Location"]
    fresh = console.app.test_client()
    assert _console_login(fresh, ip="10.4.0.3").status_code in (302, 303)
    assert fresh.get("/dev/manifest").status_code == 200  # a new console session works


def test_console_session_fails_closed_when_the_mark_lookup_fails(console, epochs):
    c = console.app.test_client()
    assert _console_login(c).status_code in (302, 303)
    session_epoch.clear_cache()
    epochs.fail_reads = True
    assert c.get("/api/dev/dealers").status_code == 401


def test_console_keys_sit_below_every_operator_key():
    assert session_epoch.dev_operator_epoch_id(1) == -1
    assert session_epoch.dev_operator_epoch_id(2**62) == -(2**62)
    with pytest.raises(ValueError):
        session_epoch.dev_operator_epoch_id(2**62 + 1)
    for nonce in ("a", "b", "x" * 40, ""):
        key = session_epoch.console_epoch_id(nonce)
        assert -(2**63) <= key < -(2**62)


def test_console_global_cap_counts_only_wrong_secrets(console, rl_clock):
    for i in range(70):  # correct secrets from many IPs are not counted by the all-IP cap
        assert _console_login(console.app.test_client(), ip=f"10.7.{i // 250}.{i % 250 + 1}").status_code in (302, 303)
    c = console.app.test_client()
    codes = [_console_login(c, secret="wrong", ip=f"10.8.{i // 250}.{i % 250 + 1}").status_code for i in range(61)]
    assert codes[:60] == [200] * 60 and codes[60] == 429
    assert _console_login(console.app.test_client(), ip="10.8.9.9").status_code in (302, 303)


def test_dev_login_attempt_limit_keys_on_the_operator(main, rl_clock):
    _operator()
    c = main.app.test_client()
    for i in range(5):
        _dev_login(c, login="op1", password="wrong-password", ip=f"10.9.0.{i + 1}")
        _dev_login(c, login="OP1@Example.com", password="wrong-password", ip=f"10.9.1.{i + 1}")
    assert _dev_login(c, login="op1", ip="10.9.2.1").status_code == 429


def test_dev_login_attempt_limit_applies_to_every_username(main, rl_clock, monkeypatch):
    from backend.db import admin_users_db

    calls: list[str] = []
    real = admin_users_db.resolve_admin_id
    monkeypatch.setattr(admin_users_db, "resolve_admin_id", lambda li: calls.append(li) or real(li))
    c = main.app.test_client()
    codes = [
        _dev_login(c, login="Ghost", password="wrong-password", ip=f"10.9.3.{i + 1}").status_code for i in range(11)
    ]
    assert codes[:10] == [200] * 10 and codes[10] == 429
    assert len(calls) == 11  # one id lookup per attempt


def test_fresh_sign_in_is_lifted_to_a_mark_ahead_of_this_clock(main, epochs):
    from backend.db.users_db import save_user

    uid = int(save_user("skewed", "skewed@example.com", APP_PASSWORD, role="general_user"))
    ahead = time.time() + 60  # another instance's clock wrote a logout mark a minute ahead
    epochs.rows[uid] = _from_epoch(ahead)
    c = main.app.test_client()
    _app_login(c, "skewed")
    assert c.get("/api/auth/me").status_code == 200
    with c.session_transaction() as s:
        assert s[auth_session.AUTH_AT] >= epochs.rows[uid].timestamp()


def test_fresh_dev_sign_in_is_lifted_to_a_mark_ahead_of_this_clock(main, epochs):
    op = _operator()
    epochs.rows[session_epoch.dev_operator_epoch_id(op)] = _from_epoch(time.time() + 60)
    c = main.app.test_client()
    assert _dev_login(c).status_code in (302, 303)
    assert _on_dev(c)


def test_operator_cookie_without_a_fingerprint_is_refused(main):
    op = _operator()
    c = main.app.test_client()
    with c.session_transaction() as s:
        s["admin_user_id"] = op
        s["admin_username"] = "op1"
        s[auth_session.DEV_AUTH_AT] = auth_session._now()
    assert not _on_dev(c)


def test_seeded_operator_cookie_helper_is_accepted(main):
    from backend.tests.conftest import seed_dev_operator_session

    c = main.app.test_client()
    seed_dev_operator_session(c)
    assert _on_dev(c)


# ---------------------------------------------------------------- Postgres tier

pg = pytest.mark.pg


@pg
def test_pg_operator_lookup_consistent_handling_and_logout_end_to_end(pg_db, app_factory, monkeypatch):
    from backend.db import admin_users_db

    monkeypatch.setenv("DEV_USERS_DB_BACKEND", "pg")
    monkeypatch.setenv("ADMIN_PASSWORD", "env-admin-password")
    main = app_factory(DEV_USERS_DB_BACKEND="pg", ADMIN_PASSWORD="env-admin-password")
    session_epoch.clear_cache()
    auth_session.clear_dev_operator_cache()
    op = _operator("pgop")
    row = admin_users_db.get_admin_by_id(op)
    assert row["username"] == "pgop" and row["password"].startswith("$")
    assert admin_users_db.get_admin_by_id(op + 1000) is None

    seen: list[str] = []
    real = admin_users_db.verify_or_legacy
    monkeypatch.setattr(admin_users_db, "verify_or_legacy", lambda pw, h: seen.append(h) or real(pw, h))
    assert admin_users_db.authenticate_admin("nobody-pg", "x-password") is None
    assert seen == [admin_users_db._dummy_hash()]
    monkeypatch.setattr(admin_users_db, "verify_or_legacy", real)

    me, other = main.app.test_client(), main.app.test_client()
    assert _dev_login(me, "pgop").status_code in (302, 303)
    _share_session(me, other)
    assert _on_dev(other)
    _post(me, "/dev/logout", {})
    assert session_epoch.get_valid_after(session_epoch.dev_operator_epoch_id(op), strict=True) is not None
    assert not _on_dev(other)


@pg
def test_pg_app_logout_ends_the_server_side_session(pg_db, app_factory):
    from backend.db.users_db import save_user

    main = app_factory()
    session_epoch.clear_cache()
    save_user("pg_leaver", "pg_leaver@example.com", APP_PASSWORD, role="general_user")
    me, other = main.app.test_client(), main.app.test_client()
    _app_login(me, "pg_leaver")
    _share_session(me, other)
    assert other.get("/api/auth/me").status_code == 200
    _post(me, "/logout", {})
    assert other.get("/api/auth/me").status_code == 401
