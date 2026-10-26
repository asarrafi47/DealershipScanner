"""P16.1: session revocation through users.auth_epoch.

A password change/reset moves the counter; the request gate drops any session whose
``ae`` stamp is behind it. The actor keeps their own session, a hash rehash never
moves the counter, and a session from before the deploy is stamped, not dropped.
"""
from __future__ import annotations


_PW = "long-enough-password"
_NEW = "another-long-password"
_TOK = "t" * 32


def _csrf(client):
    from backend.utils.csrf import _SESSION_KEY

    with client.session_transaction() as sess:
        sess[_SESSION_KEY] = _TOK
    return _TOK


def _login(client, login="rev1"):
    tok = _csrf(client)
    rv = client.post(
        "/api/auth/login",
        json={"login": login, "password": _PW},
        headers={"X-CSRF-Token": tok},
    )
    assert rv.status_code == 200, rv.get_data(as_text=True)


def _me(client) -> int:
    return client.get("/api/auth/me").status_code


def _epoch(uid: int) -> int:
    from backend.db.users_db import get_user_profile

    return get_user_profile(uid)["auth_epoch"]


def _setup(app_factory):
    app = app_factory().app
    from backend.db.users_db import get_user_by_login, save_user
    from backend.utils.roles import ROLE_GENERAL

    save_user("rev1", "rev1@example.com", _PW, role=ROLE_GENERAL)
    return app, int(get_user_by_login("rev1")["id"])


def test_login_stamps_session_epoch(app_factory):
    app, uid = _setup(app_factory)
    with app.test_client() as c:
        _login(c)
        with c.session_transaction() as s:
            assert s["ae"] == _epoch(uid) == 0


def test_password_reset_ends_other_sessions_only(app_factory):
    app, uid = _setup(app_factory)
    from backend.auth.password_reset import complete_password_reset, hash_reset_token
    from backend.db.users_db import set_user_password_reset_token

    a, b = app.test_client(), app.test_client()
    if True:
        _login(a)
        _login(b)
        assert _me(a) == _me(b) == 200
        import time

        set_user_password_reset_token(uid, hash_reset_token("tok-rev1"), int(time.time()) + 600)
        ok, _msg = complete_password_reset(token="tok-rev1", new_password=_NEW)
        assert ok and _epoch(uid) == 1
        assert _me(a) == 401
        assert _me(b) == 401


def test_admin_reset_ends_target_sessions(app_factory):
    app, uid = _setup(app_factory)
    from backend.db.users_db import admin_reset_user_password

    with app.test_client() as c:
        _login(c)
        assert _me(c) == 200
        assert admin_reset_user_password(uid, _NEW) is None
        assert _epoch(uid) == 1
        assert _me(c) == 401


def test_self_service_change_keeps_actor_ends_other(app_factory):
    app, uid = _setup(app_factory)
    actor, other = app.test_client(), app.test_client()
    if True:
        _login(actor)
        _login(other)
        tok = _csrf(actor)
        rv = actor.post(
            "/account/profile",
            data={
                "csrf_token": tok,
                "form_action": "password",
                "current_password": _PW,
                "new_password": _NEW,
                "confirm_password": _NEW,
            },
        )
        assert rv.status_code == 200, rv.get_data(as_text=True)[:300]
        assert _epoch(uid) == 1
        assert _me(actor) == 200
        assert _me(other) == 401


def test_rehash_does_not_bump_epoch(app_factory):
    app, uid = _setup(app_factory)
    from backend.db.users_db import _common
    from backend.db.users_sqlite import get_users_conn

    conn = get_users_conn()
    stored = conn.execute("SELECT password FROM users WHERE id=?", (uid,)).fetchone()[0]
    conn.close()

    class _Sync:  # run the rehash body inline instead of on its daemon thread
        def __init__(self, target=None, **_kw):
            self._t = target

        def start(self):
            self._t()

    real = _common.threading.Thread
    _common.threading.Thread = _Sync
    try:
        _common._schedule_password_rehash(uid, _PW, stored)
    finally:
        _common.threading.Thread = real
    conn = get_users_conn()
    new = conn.execute("SELECT password FROM users WHERE id=?", (uid,)).fetchone()[0]
    conn.close()
    assert new != stored
    assert _epoch(uid) == 0
    c = app.test_client()
    _login(c)
    assert _me(c) == 200


def test_pre_deploy_session_is_stamped_not_dropped(app_factory):
    app, uid = _setup(app_factory)
    with app.test_client() as c:
        _login(c)
        with c.session_transaction() as s:
            s.pop("ae")  # a cookie minted before the column existed
        assert _me(c) == 200
        with c.session_transaction() as s:
            assert s["ae"] == 0


def test_db_error_fails_open(app_factory, monkeypatch):
    app, uid = _setup(app_factory)
    import backend.billing.access as access

    with app.test_client() as c:
        _login(c)

        def boom(_uid):
            return None, True

        monkeypatch.setattr(access, "_load_profile", boom)
        with c.session_transaction() as s:
            s["ae"] = 99
        c.get("/api/auth/me")  # must not clear the session on a lookup failure
        with c.session_transaction() as s:
            assert s.get("user_id") == uid
