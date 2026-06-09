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
