from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

from backend.utils.runtime_env import is_production_env


def billing_enabled() -> bool:
    return (os.environ.get("BILLING_STRIPE_ENABLED") or "").strip().lower() in ("1", "true", "yes", "on")


def _require_env(name: str) -> str:
    v = (os.environ.get(name) or "").strip()
    if not v and is_production_env() and billing_enabled():
        raise RuntimeError(f"{name} must be set when BILLING_STRIPE_ENABLED=1 in production.")
    return v


def stripe_secret_key() -> str:
    return _require_env("STRIPE_SECRET_KEY")


def stripe_webhook_secret() -> str:
    return _require_env("STRIPE_WEBHOOK_SECRET")


def stripe_price_id() -> str:
    return _require_env("STRIPE_PRICE_ID")


def base_url_from_request(request) -> str:
    forced = (os.environ.get("PUBLIC_BASE_URL") or "").strip().rstrip("/")
    if forced:
        return forced
    # Best-effort for local usage. For reverse proxies, set PUBLIC_BASE_URL.
    return (request.host_url or "").rstrip("/")


def unix_to_iso(ts: int | float | None) -> str | None:
    if ts is None:
        return None
    try:
        dt = datetime.fromtimestamp(float(ts), tz=timezone.utc)
    except Exception:
        return None
    return dt.isoformat()


def create_checkout_session(
    *,
    request: Any,
    org_id: int,
    user_id: int,
    user_email: str,
) -> dict[str, Any]:
    if not billing_enabled():
        raise RuntimeError("Billing is disabled.")
    import stripe  # type: ignore

    stripe.api_key = stripe_secret_key()
    price = stripe_price_id()
    if not price:
        raise RuntimeError("STRIPE_PRICE_ID is not set.")
    base = base_url_from_request(request)
    success = f"{base}/billing/success?session_id={{CHECKOUT_SESSION_ID}}"
    cancel = f"{base}/billing/required"
    cs = stripe.checkout.Session.create(
        mode="subscription",
        line_items=[{"price": price, "quantity": 1}],
        success_url=success,
        cancel_url=cancel,
        client_reference_id=str(int(org_id)),
        customer_email=(user_email or "").strip() or None,
        metadata={
            "org_id": str(int(org_id)),
            "user_id": str(int(user_id)),
        },
        subscription_data={
            "metadata": {
                "org_id": str(int(org_id)),
            }
        },
        allow_promotion_codes=True,
    )
    return dict(cs)


def construct_webhook_event(payload: bytes, sig_header: str) -> dict[str, Any]:
    import stripe  # type: ignore

    stripe.api_key = stripe_secret_key()
    secret = stripe_webhook_secret()
    if not secret:
        raise RuntimeError("STRIPE_WEBHOOK_SECRET is not set.")
    evt = stripe.Webhook.construct_event(payload=payload, sig_header=sig_header, secret=secret)
    return dict(evt)

