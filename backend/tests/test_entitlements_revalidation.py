"""
Paid session flags must be revalidated against the DB (audit S3/S5).

Sessions are 14-day client-side cookies: a Stripe cancel/refund clears the
users row but can't touch issued cookies, so entitlements_from_session must
treat session subscription flags as hints, not authority.
"""
from __future__ import annotations

import pytest

from backend.billing import entitlements
from backend.billing.catalog import FEATURE_AI_CAR_CHAT
from backend.billing.entitlements import (
    entitlements_from_session,
    invalidate_billing_cache,
)


@pytest.fixture(autouse=True)
def _fresh_billing_cache():
    invalidate_billing_cache()
    yield
    invalidate_billing_cache()


def _patch_snapshot(monkeypatch, result):
    import backend.db.users_db as users_db

    monkeypatch.setattr(users_db, "get_user_billing_snapshot", lambda uid: result)


def test_canceled_plan_in_session_loses_features(monkeypatch):
    _patch_snapshot(monkeypatch, {"id": 7, "is_premium": False, "subscription_plan_id": None})
    session = {"user_id": 7, "subscription_plan_id": "complete", "user_is_premium": True}
    feats = entitlements_from_session(session)
    assert FEATURE_AI_CAR_CHAT not in feats
    # Cookie self-heals so account UI reflects reality too.
    assert session["subscription_plan_id"] is None
    assert session["user_is_premium"] is False


def test_active_plan_confirmed_keeps_features(monkeypatch):
    _patch_snapshot(monkeypatch, {"id": 7, "is_premium": True, "subscription_plan_id": "complete"})
    session = {"user_id": 7, "subscription_plan_id": "complete", "user_is_premium": True}
    assert FEATURE_AI_CAR_CHAT in entitlements_from_session(session)


def test_paid_flags_without_user_id_are_dropped(monkeypatch):
    def _boom(uid):  # pragma: no cover - must not be reached
        raise AssertionError("no DB lookup expected without a user id")

    import backend.db.users_db as users_db

    monkeypatch.setattr(users_db, "get_user_billing_snapshot", _boom)
    session = {"subscription_plan_id": "complete", "user_is_premium": True}
    assert FEATURE_AI_CAR_CHAT not in entitlements_from_session(session)


def test_db_failure_fails_open_to_session_flags(monkeypatch):
    def _down(uid):
        raise RuntimeError("db offline")

    import backend.db.users_db as users_db

    monkeypatch.setattr(users_db, "get_user_billing_snapshot", _down)
    session = {"user_id": 7, "subscription_plan_id": "complete", "user_is_premium": True}
    assert FEATURE_AI_CAR_CHAT in entitlements_from_session(session)


def test_webhook_invalidation_is_immediate(monkeypatch):
    _patch_snapshot(monkeypatch, {"id": 7, "is_premium": True, "subscription_plan_id": "complete"})
    session = {"user_id": 7, "subscription_plan_id": "complete", "user_is_premium": True}
    assert FEATURE_AI_CAR_CHAT in entitlements_from_session(session)

    # Simulate the cancel webhook: DB cleared + cache invalidated for the user.
    _patch_snapshot(monkeypatch, {"id": 7, "is_premium": False, "subscription_plan_id": None})
    invalidate_billing_cache(7)
    session = {"user_id": 7, "subscription_plan_id": "complete", "user_is_premium": True}
    assert FEATURE_AI_CAR_CHAT not in entitlements_from_session(session)


def test_revalidation_result_is_cached_within_ttl(monkeypatch):
    calls = {"n": 0}

    def _counting(uid):
        calls["n"] += 1
        return {"id": uid, "is_premium": True, "subscription_plan_id": "complete"}

    import backend.db.users_db as users_db

    monkeypatch.setattr(users_db, "get_user_billing_snapshot", _counting)
    for _ in range(3):
        session = {"user_id": 7, "subscription_plan_id": "complete", "user_is_premium": True}
        entitlements_from_session(session)
    assert calls["n"] == 1


def test_admin_role_short_circuits_without_db(monkeypatch):
    def _boom(uid):  # pragma: no cover - must not be reached
        raise AssertionError("admin path must not hit the DB")

    import backend.db.users_db as users_db

    monkeypatch.setattr(users_db, "get_user_billing_snapshot", _boom)
    session = {"user_role": "platform_admin", "user_id": 7, "subscription_plan_id": "complete"}
    assert FEATURE_AI_CAR_CHAT in entitlements_from_session(session)


def _patch_org(monkeypatch, result):
    import backend.db.users_db as users_db

    monkeypatch.setattr(users_db, "get_org", lambda org_id: result)


def test_canceled_org_subscription_loses_features(monkeypatch):
    _patch_org(monkeypatch, {"id": 3, "stripe_subscription_status": "canceled"})
    session = {"user_id": 7, "org_id": 3, "org_subscription_status": "active"}
    feats = entitlements_from_session(session)
    assert FEATURE_AI_CAR_CHAT not in feats
    assert session["org_subscription_status"] == "canceled"


def test_active_org_subscription_confirmed_keeps_features(monkeypatch):
    _patch_org(monkeypatch, {"id": 3, "stripe_subscription_status": "active"})
    session = {"user_id": 7, "org_id": 3, "org_subscription_status": "active"}
    assert FEATURE_AI_CAR_CHAT in entitlements_from_session(session)


def test_org_status_without_org_id_is_dropped(monkeypatch):
    def _boom(org_id):  # pragma: no cover - must not be reached
        raise AssertionError("no org lookup expected without an org id")

    import backend.db.users_db as users_db

    monkeypatch.setattr(users_db, "get_org", _boom)
    session = {"user_id": 7, "org_subscription_status": "active"}
    assert FEATURE_AI_CAR_CHAT not in entitlements_from_session(session)


def test_ai_blueprint_client_ip_ignores_spoofed_xff(monkeypatch):
    """S5: both AI blueprints must key rate limits on the trusted client IP."""
    from flask import Flask

    from backend.routes import ai_chat_bp as chat_mod
    from backend.routes import ai_narrate_bp as narrate_mod

    monkeypatch.delenv("TRUST_PROXY_HEADERS", raising=False)
    app = Flask(__name__)
    with app.test_request_context(
        "/",
        headers={"X-Forwarded-For": "6.6.6.6"},
        environ_base={"REMOTE_ADDR": "10.0.0.9"},
    ):
        assert chat_mod._client_ip() == "10.0.0.9"
        assert narrate_mod._client_ip() == "10.0.0.9"

    monkeypatch.setenv("TRUST_PROXY_HEADERS", "1")
    with app.test_request_context(
        "/",
        headers={"X-Forwarded-For": "6.6.6.6"},
        environ_base={"REMOTE_ADDR": "10.0.0.9"},
    ):
        assert chat_mod._client_ip() == "6.6.6.6"
        assert narrate_mod._client_ip() == "6.6.6.6"
