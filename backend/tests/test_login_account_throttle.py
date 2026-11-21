"""SEC-A-010: per-account login failure counter and IPv6 /64 keying."""
from __future__ import annotations

from backend.auth.pages import _login_ip_key


def _login(c, login, password, ip=None):
    tok = c.get("/api/auth/csrf").get_json()["csrf_token"]
    kw = {"environ_overrides": {"REMOTE_ADDR": ip}} if ip else {}
    return c.post("/api/auth/login", json={"login": login, "password": password},
                  headers={"X-CSRF-Token": tok}, **kw)


def _user(name):
    from backend.db.users_db import save_user
    from backend.utils.roles import ROLE_GENERAL

    save_user(name, f"{name}@example.com", "long-enough-password", role=ROLE_GENERAL)


def test_ipv6_collapses_to_slash64():
    a = _login_ip_key("2001:db8:1:2:aaaa::1")
    b = _login_ip_key("2001:db8:1:2:bbbb::9")
    assert a == b == "2001:db8:1:2::/64"
    assert _login_ip_key("2001:db8:1:3::1") != a
    assert _login_ip_key("203.0.113.5") == "203.0.113.5"
    assert _login_ip_key("::ffff:203.0.113.5") == "203.0.113.5"
    assert _login_ip_key("garbage:x") == "garbage:x"


def test_account_lockout_across_ips(app_factory):
    main = app_factory()
    c = main.app.test_client()
    pw = "long-enough-password"
    _user("victim1")
    codes = []
    for i in range(12):
        rv = _login(c, "Victim1", "wrong-pass-%d" % i, f"198.51.100.{i + 1}")
        codes.append(rv.status_code)
    assert codes[:10] == [401] * 10
    assert codes[10:] == [429, 429]
    # even the right password is refused while locked
    rv = _login(c, "victim1", pw, "198.51.100.200")
    assert rv.status_code == 429


def test_success_not_counted_and_other_account_unaffected(app_factory):
    main = app_factory()
    c = main.app.test_client()
    pw = "long-enough-password"
    _user("okuser")
    for _ in range(3):
        assert _login(c, "okuser", pw).status_code == 200
    for i in range(10):
        _login(c, "someoneelse", "x%d" % i, f"198.51.100.{i + 1}")
    assert _login(c, "okuser", pw).status_code == 200


def test_username_and_email_share_one_bucket(app_factory):
    main = app_factory()
    c = main.app.test_client()
    _user("aliasv")
    codes = []
    for i in range(12):
        ident = "aliasv" if i % 2 == 0 else "aliasv@example.com"
        codes.append(_login(c, ident, "wrong-%d" % i, f"198.51.100.{i + 1}").status_code)
    assert codes[:10] == [401] * 10
    assert codes[10:] == [429, 429]


def test_success_refunds_slot(app_factory):
    main = app_factory()
    c = main.app.test_client()
    pw = "long-enough-password"
    _user("refunder")
    for i in range(9):
        assert _login(c, "refunder", "bad%d" % i, f"198.51.100.{i + 1}").status_code == 401
    for _ in range(5):
        assert _login(c, "refunder", pw).status_code == 200
    # 9 failures + successes must not have consumed the last slot
    assert _login(c, "refunder", "bad-last").status_code == 401
    assert _login(c, "refunder", "bad-after").status_code == 429


def test_slot_reserved_before_verification(app_factory, monkeypatch):
    """Failures are counted even when many guesses are in flight: the slot is taken
    before authenticate runs, so a guess seeing the lock never reaches the hash."""
    import backend.auth.pages as pages

    main = app_factory()
    c = main.app.test_client()
    _user("racer")
    seen = []

    def spy(login, pw):
        seen.append(pw)
        return None

    monkeypatch.setattr(pages, "authenticate_app_user", spy)
    for i in range(12):
        _login(c, "racer", "g%d" % i, f"198.51.100.{i + 1}")
    assert len(seen) == 10


def test_dealer_login_shares_account_lockout(app_factory):
    main = app_factory()
    c = main.app.test_client()
    _user("dvictim")
    tok = c.get("/api/auth/csrf").get_json()["csrf_token"]
    codes = []
    for i in range(12):
        rv = c.post("/dealer/login", data={"login": "dvictim", "password": "bad-%d" % i, "csrf_token": tok},
                    headers={"X-CSRF-Token": tok},
                    environ_overrides={"REMOTE_ADDR": f"198.51.100.{i + 1}"})
        codes.append(rv.status_code)
    assert 429 in codes[10:], codes
    # the shared counter also locks the main login for the same account
    assert _login(c, "dvictim", "long-enough-password", "198.51.100.220").status_code == 429
