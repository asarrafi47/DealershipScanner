"""
Unified Stripe Checkout scaffolding for subscription packages.

Existing org and legacy premium routes remain; new plan-based checkout uses this module.
"""
from __future__ import annotations

from typing import Any

from backend.billing.catalog import LEGACY_PREMIUM_PLAN_ID, get_plan, stripe_price_id_for_plan
from backend.billing.discounts import checkout_discount_kwargs, resolve_discount
from backend.billing.stripe_billing import base_url_from_request, billing_enabled, stripe_secret_key


def create_plan_checkout_session(
    *,
    request: Any,
    user_id: int,
    user_email: str,
    plan_id: str,
    promo_code: str | None = None,
) -> dict[str, Any]:
    """
    Start Stripe Checkout for a catalog plan (``research``, ``assistant``, ``complete``).

    Metadata includes ``plan_id`` for webhook entitlement grants.
    """
    if not billing_enabled():
        raise RuntimeError("Billing is disabled.")

    pid = (plan_id or "").strip().lower() or LEGACY_PREMIUM_PLAN_ID
    plan = get_plan(pid)
    if not plan or plan.monthly_cents <= 0:
        raise ValueError(f"Unknown or free plan: {plan_id!r}")

    price = stripe_price_id_for_plan(pid)
    if not price:
        raise RuntimeError(f"Stripe price not configured for plan {pid!r}")

    import stripe  # type: ignore

    stripe.api_key = stripe_secret_key()
    base = base_url_from_request(request)
    uid = str(int(user_id))
    success = f"{base}/billing/plan/success?session_id={{CHECKOUT_SESSION_ID}}&plan={pid}"
    cancel = f"{base}/premium?plan={pid}"

    discount = resolve_discount(promo_code)
    md = {"user_id": uid, "plan_id": pid, "premium": "1"}

    subscription_data: dict[str, Any] = {"metadata": dict(md)}
    if plan.trial_days > 0:
        subscription_data["trial_period_days"] = plan.trial_days

    kwargs: dict[str, Any] = {
        "mode": "subscription",
        "line_items": [{"price": price, "quantity": 1}],
        "success_url": success,
        "cancel_url": cancel,
        "customer_email": (user_email or "").strip() or None,
        "metadata": md,
        "subscription_data": subscription_data,
        **checkout_discount_kwargs(discount),
    }
    cs = stripe.checkout.Session.create(**kwargs)
    return dict(cs) if not isinstance(cs, dict) else cs


def verify_plan_checkout_session(*, session_id: str, user_id: int, plan_id: str) -> bool:
    """Confirm Checkout completed for the expected user and plan."""
    sid = (session_id or "").strip()
    pid = (plan_id or "").strip().lower()
    if not sid or not pid or not billing_enabled():
        return False
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return False
    if uid <= 0:
        return False

    import stripe  # type: ignore

    stripe.api_key = stripe_secret_key()
    cs = stripe.checkout.Session.retrieve(sid)
    if not isinstance(cs, dict):
        cs = dict(cs)
    if (cs.get("status") or "").strip().lower() != "complete":
        return False
    pay = (cs.get("payment_status") or "").strip().lower()
    if pay not in ("paid", "no_payment_required"):
        return False
    mode = (cs.get("mode") or "").strip().lower()
    if mode and mode != "subscription":
        return False
    md = cs.get("metadata") if isinstance(cs.get("metadata"), dict) else {}
    if str(md.get("user_id") or "").strip() != str(uid):
        return False
    if str(md.get("plan_id") or "").strip().lower() != pid:
        return False
    return True
