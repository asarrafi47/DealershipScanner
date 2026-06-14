"""
Discount and promotion scaffolding for Stripe Checkout.

Supports:
- Stripe Promotion Codes (``allow_promotion_codes=True`` on Checkout — already enabled)
- Pre-applied coupons via ``discounts=[{coupon: ...}]``
- Admin-issued promo codes mapped from env (``BILLING_PROMO_<CODE>`` → Stripe coupon id)
- Trial extension via plan ``trial_days`` in catalog
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any


_PROMO_CODE_RE = re.compile(r"^[A-Za-z0-9_-]{3,32}$")


@dataclass(frozen=True)
class DiscountApplication:
    """How a discount should be applied at Checkout."""

    stripe_coupon_id: str | None = None
    stripe_promotion_code_id: str | None = None
    allow_user_promo_entry: bool = True
    label: str = ""


def normalize_promo_code(raw: str | None) -> str:
    return (raw or "").strip().upper()


def promo_code_valid_format(code: str) -> bool:
    return bool(_PROMO_CODE_RE.match(code))


def _env_promo_coupon_map() -> dict[str, str]:
    """``BILLING_PROMO_LAUNCH20=stripe_coupon_id`` style mappings."""
    out: dict[str, str] = {}
    prefix = "BILLING_PROMO_"
    for key, val in os.environ.items():
        if not key.startswith(prefix):
            continue
        code = key[len(prefix) :].strip().upper().replace("_", "-")
        coupon = (val or "").strip()
        if code and coupon:
            out[code] = coupon
    return out


def resolve_discount(promo_code: str | None) -> DiscountApplication:
    """
    Map a user-entered or URL promo code to a Stripe coupon.

    Unknown codes still allow Stripe's native promotion code field at Checkout.
    """
    code = normalize_promo_code(promo_code)
    if not code:
        return DiscountApplication(allow_user_promo_entry=True)

    if not promo_code_valid_format(code):
        return DiscountApplication(allow_user_promo_entry=True, label="invalid_format")

    mapped = _env_promo_coupon_map().get(code.replace("-", "").replace("_", ""))
    # Also try exact key with underscores (BILLING_PROMO_LAUNCH20 → LAUNCH20)
    if not mapped:
        mapped = _env_promo_coupon_map().get(code)

    if mapped:
        return DiscountApplication(
            stripe_coupon_id=mapped,
            allow_user_promo_entry=False,
            label=code,
        )

    # Let Stripe validate promotion codes entered at Checkout
    return DiscountApplication(allow_user_promo_entry=True, label=code)


def checkout_discount_kwargs(discount: DiscountApplication) -> dict[str, Any]:
    """Stripe Checkout.Session.create kwargs fragment for discounts."""
    out: dict[str, Any] = {"allow_promotion_codes": discount.allow_user_promo_entry}
    if discount.stripe_coupon_id:
        out["discounts"] = [{"coupon": discount.stripe_coupon_id}]
        # Stripe disallows allow_promotion_codes when discounts is pre-set
        out["allow_promotion_codes"] = False
    return out
