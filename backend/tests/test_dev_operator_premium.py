"""Dev operator session grants premium APIs (scan lab options/sticker)."""

from __future__ import annotations


def test_dev_admin_grants_premium_access(monkeypatch):
    import backend.main as main

    monkeypatch.setattr(main, "_billing_enabled", lambda: True)
    monkeypatch.setattr(main, "is_production_env", lambda: True)

    ctx = main.app.test_request_context("/dev/scan-lab")
    ctx.push()
    try:
        from flask import session

        session["admin_user_id"] = 1
        session["admin_username"] = "testdev"
        assert main._dev_operator_grants_premium() is True
        ok, err = main._require_premium_feature()
        assert ok is True
        assert err == ""
        assert main._session_has_paid_access() is True
    finally:
        ctx.pop()
