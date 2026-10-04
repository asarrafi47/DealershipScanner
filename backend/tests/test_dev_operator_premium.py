"""Dev operator session grants premium APIs (scan lab options/sticker)."""

from __future__ import annotations


def test_dev_admin_grants_premium_access(monkeypatch):
    import backend.main as main
    from backend.billing import access

    monkeypatch.setenv("BILLING_STRIPE_ENABLED", "1")
    monkeypatch.setattr("backend.utils.runtime_env.is_production_env", lambda: True)

    ctx = main.app.test_request_context("/dev/scan-lab")
    ctx.push()
    try:
        from flask import session

        session["admin_user_id"] = 1
        session["admin_username"] = "testdev"
        viewer = access.current_access()
        assert viewer.dev_operator is True
        ok, err = access.check_feature("window_sticker")
        assert ok is True
        assert err == ""
        assert viewer.sees_paid_ui() is True
        assert viewer.shows("ai_car_chat") is True
    finally:
        ctx.pop()
