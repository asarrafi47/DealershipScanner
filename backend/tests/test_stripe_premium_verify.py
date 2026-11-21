"""Unit tests for Stripe premium Checkout session verification."""

from __future__ import annotations

from unittest.mock import patch

import pytest


def test_verify_premium_checkout_session_requires_paid_and_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BILLING_STRIPE_ENABLED", "1")
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test_unit")
    from backend.billing.stripe_billing import verify_premium_checkout_session

    paid = {
        "status": "complete",
        "mode": "subscription",
        "payment_status": "paid",
        "subscription": {"id": "sub_1", "status": "active"},
        "metadata": {"user_id": "42", "premium": "1"},
    }
    with patch("stripe.checkout.Session.retrieve", return_value=paid):
        assert verify_premium_checkout_session(session_id="cs_test", user_id=42) is True
        assert verify_premium_checkout_session(session_id="cs_test", user_id=99) is False

    unpaid = {
        "status": "complete",
        "mode": "subscription",
        "payment_status": "unpaid",
        "metadata": {"user_id": "42", "premium": "1"},
    }
    with patch("stripe.checkout.Session.retrieve", return_value=unpaid):
        assert verify_premium_checkout_session(session_id="cs_test", user_id=42) is False

    wrong_mode = {
        "status": "complete",
        "mode": "payment",
        "payment_status": "paid",
        "metadata": {"user_id": "42", "premium": "1"},
    }
    with patch("stripe.checkout.Session.retrieve", return_value=wrong_mode):
        assert verify_premium_checkout_session(session_id="cs_test", user_id=42) is False


def _paid(**over):
    cs = {
        "status": "complete",
        "mode": "subscription",
        "payment_status": "paid",
        "subscription": {"id": "sub_1", "status": "active"},
        "metadata": {"user_id": "42", "premium": "1"},
    }
    cs.update(over)
    return cs


def test_verify_rejects_replay_of_cancelled_subscription_session(monkeypatch: pytest.MonkeyPatch) -> None:
    """SEC-A-003: a completed session whose subscription ended must not re-grant."""
    monkeypatch.setenv("BILLING_STRIPE_ENABLED", "1")
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test_unit")
    from backend.billing.checkout_service import verify_plan_checkout_session
    from backend.billing.stripe_billing import verify_premium_checkout_session

    for sub in ({"id": "sub_1", "status": "canceled"}, "sub_1", None):
        cs = _paid(subscription=sub)
        with patch("stripe.checkout.Session.retrieve", return_value=cs):
            assert verify_premium_checkout_session(session_id="cs_x", user_id=42) is False
        cs = _paid(subscription=sub, metadata={"user_id": "42", "plan_id": "complete", "premium": "1"})
        with patch("stripe.checkout.Session.retrieve", return_value=cs):
            assert verify_plan_checkout_session(session_id="cs_x", user_id=42, plan_id="complete") is False

    trial = _paid(subscription={"id": "sub_1", "status": "trialing"})
    with patch("stripe.checkout.Session.retrieve", return_value=trial):
        assert verify_premium_checkout_session(session_id="cs_x", user_id=42) is True


def test_legacy_premium_verify_rejects_research_plan_session(monkeypatch: pytest.MonkeyPatch) -> None:
    """B-032: a Research plan session must not grant the Complete bundle."""
    monkeypatch.setenv("BILLING_STRIPE_ENABLED", "1")
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test_unit")
    from backend.billing.stripe_billing import verify_premium_checkout_session

    cs = _paid(metadata={"user_id": "42", "premium": "1", "plan_id": "research"})
    with patch("stripe.checkout.Session.retrieve", return_value=cs):
        assert verify_premium_checkout_session(session_id="cs_x", user_id=42) is False
