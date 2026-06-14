"""Tests for Stripe Customer Portal scaffold (C3)."""

from __future__ import annotations

import pytest

from backend.billing.stripe_billing import billing_enabled, create_customer_portal_session
from backend.db.users_db import get_user_billing_snapshot, init_users_db, grant_user_premium, save_user


def test_billing_snapshot_empty_customer(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "users.db"
    monkeypatch.setenv("USERS_DB_PATH", str(db_path))
    init_users_db()
    uid = save_user("billuser", "bill@example.com", "password123", role="general_user")
    snap = get_user_billing_snapshot(uid)
    assert snap
    assert snap.get("is_premium") is False
    assert not (snap.get("premium_stripe_customer_id") or "").strip()


def test_billing_snapshot_with_customer(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "users.db"
    monkeypatch.setenv("USERS_DB_PATH", str(db_path))
    init_users_db()
    uid = save_user("paiduser", "paid@example.com", "password123", role="general_user")
    grant_user_premium(uid, customer_id="cus_test123", plan_id="assistant")
    snap = get_user_billing_snapshot(uid)
    assert snap
    assert snap.get("is_premium") is True
    assert snap.get("premium_stripe_customer_id") == "cus_test123"
    assert snap.get("subscription_plan_id") == "assistant"


def test_portal_requires_billing_enabled(monkeypatch) -> None:
    monkeypatch.delenv("BILLING_STRIPE_ENABLED", raising=False)
    assert billing_enabled() is False
    with pytest.raises(RuntimeError, match="disabled"):
        create_customer_portal_session(request=None, customer_id="cus_x")  # type: ignore[arg-type]
