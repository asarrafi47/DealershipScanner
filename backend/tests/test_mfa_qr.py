from __future__ import annotations

import importlib
import re

try:
    import flask_socketio  # noqa: F401
except ImportError as e:  # pragma: no cover
    raise AssertionError("flask-socketio is required for QR MFA tests") from e


def _app(monkeypatch, tmp_path):
    monkeypatch.setenv("FLASK_ENV", "development")
    monkeypatch.setenv("MFA_DELIVERY_MODE", "log")
    monkeypatch.setenv("MFA_QR_INMEMORY", "1")
    monkeypatch.delenv("REDIS_URL", raising=False)
    monkeypatch.setenv("USERS_DB_PATH", str(tmp_path / "users_mfa_qr.db"))
    monkeypatch.setenv("DEV_USERS_DB_PATH", str(tmp_path / "dev_users_mfa_qr.db"))
    for k in (
        "ADMIN_PASSWORD",
        "ADMIN_USERNAME",
        "ADMIN_EMAIL",
        "USERS_DB_ENCRYPTION_KEY",
        "DEV_USERS_DB_ENCRYPTION_KEY",
    ):
        monkeypatch.delenv(k, raising=False)
    import backend.main as main

    importlib.reload(main)
    return main


def _register_and_choose_qr(client, app):
    from flask import session

    with client:
        client.get("/register")
        csrf = session.get("_csrf_token")
        assert csrf
        r = client.post(
            "/register",
            data={
                "csrf_token": csrf,
                "username": "qr1",
                "email": "qr1@example.com",
                "password": "long-enough-password",
            },
            follow_redirects=False,
        )
        assert r.status_code in (302, 303)
        r2 = client.get("/mfa/choose", follow_redirects=True)
        assert r2.status_code == 200
        csrf2 = session.get("_csrf_token")
        r3 = client.post(
            "/mfa/choose",
            data={"csrf_token": csrf2, "channel": "qr"},
            follow_redirects=False,
        )
        assert r3.status_code in (302, 303)
        assert "qr-wait" in (r3.headers.get("Location") or "")
        with client.session_transaction() as s:
            aid = (s.get("mfa_qr_attempt_id") or "").strip()
            assert len(aid) > 10
    return aid


def test_mfa_qr_approve_and_complete_flow(monkeypatch, tmp_path):
    main = _app(monkeypatch, tmp_path)
    app = main.app
    client_d = app.test_client()
    aid = _register_and_choose_qr(client_d, app)
    r_wait = client_d.get("/mfa/qr-wait", follow_redirects=True)
    assert r_wait.status_code == 200

    client_p = app.test_client()
    r0 = client_p.get(f"/mfa/qr-confirm/{aid}", follow_redirects=True)
    assert r0.status_code == 200
    t = (r0.get_data(as_text=True) or "")
    m = re.search(r'name="ap_nonce"\s+value="([^"]+)"', t) or re.search(
        r'value="([^"]+)"\s+name="ap_nonce"', t
    )
    assert m, t[:400]
    nonce = m.group(1)
    r1 = client_p.post(
        f"/mfa/qr-confirm/{aid}",
        data={"ap_nonce": nonce},
        follow_redirects=True,
    )
    assert r1.status_code in (200, 302, 303)

    with client_d.session_transaction() as s:
        csf = s.get("_csrf_token")
    r_done = client_d.post(
        "/mfa/qr/complete",
        data={"csrf_token": csf, "attempt_id": aid},
        follow_redirects=False,
        headers={"X-MFA-QR-JSON": "1"},
    )
    if r_done.status_code != 200:
        print(r_done.get_data(as_text=True)[:500])
    assert r_done.status_code == 200
    assert r_done.is_json
    body = r_done.get_json() or {}
    assert body.get("ok") is True
    assert body.get("next")

    with client_d.session_transaction() as s2:
        assert int(s2.get("user_id") or 0) > 0


def test_qr_segno_emits_valid_png():
    from backend.utils.qr_segno import png_bytes

    p = png_bytes(
        data="otpauth://totp/Issuer:test@ex.com?secret=JBSWY3DPEHPK3PXP&issuer=Issue",
        box_size=3,
    )
    assert p[:8] == b"\x89PNG\r\n\x1a\n"
    assert len(p) > 40
