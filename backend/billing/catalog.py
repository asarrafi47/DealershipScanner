"""
Subscription package catalog — plan definitions, feature bundles, Stripe price mapping.

Scaffold for multi-plan billing. Stripe Price IDs come from env (``STRIPE_PRICE_<PLAN>``)
or legacy ``STRIPE_PREMIUM_PRICE_ID`` for the ``complete`` plan.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import FrozenSet


@dataclass(frozen=True)
class SubscriptionPlan:
    """A sellable package with a feature bundle."""

    id: str
    name: str
    description: str
    monthly_cents: int
    features: FrozenSet[str]
    stripe_price_env: str
    trial_days: int = 0
    highlight: bool = False


# Canonical feature ids (gates use these strings).
FEATURE_AI_CAR_CHAT = "ai_car_chat"
FEATURE_AI_COMPARE_CHAT = "ai_compare_chat"
FEATURE_WINDOW_STICKER = "window_sticker"
FEATURE_VEHICLE_HISTORY = "vehicle_history"
FEATURE_MARKET_INTEL = "market_intel"
FEATURE_NEARBY_DEALERS = "nearby_dealers"
FEATURE_PACKAGES_ENSURE = "packages_ensure"
FEATURE_SAVED_SEARCHES = "saved_searches"

ALL_FEATURES: FrozenSet[str] = frozenset(
    {
        FEATURE_AI_CAR_CHAT,
        FEATURE_AI_COMPARE_CHAT,
        FEATURE_WINDOW_STICKER,
        FEATURE_VEHICLE_HISTORY,
        FEATURE_MARKET_INTEL,
        FEATURE_NEARBY_DEALERS,
        FEATURE_PACKAGES_ENSURE,
        FEATURE_SAVED_SEARCHES,
    }
)

FREE_FEATURES: FrozenSet[str] = frozenset()

RESEARCH_FEATURES: FrozenSet[str] = frozenset(
    {
        FEATURE_MARKET_INTEL,
        FEATURE_VEHICLE_HISTORY,
        FEATURE_NEARBY_DEALERS,
    }
)

ASSISTANT_FEATURES: FrozenSet[str] = RESEARCH_FEATURES | frozenset(
    {
        FEATURE_AI_CAR_CHAT,
        FEATURE_AI_COMPARE_CHAT,
    }
)

COMPLETE_FEATURES: FrozenSet[str] = ASSISTANT_FEATURES | frozenset(
    {
        FEATURE_WINDOW_STICKER,
        FEATURE_PACKAGES_ENSURE,
    }
)

SUBSCRIPTION_PLANS: dict[str, SubscriptionPlan] = {
    "free": SubscriptionPlan(
        id="free",
        name="Free",
        description="Browse listings, search, and save cars.",
        monthly_cents=0,
        features=FREE_FEATURES,
        stripe_price_env="",
    ),
    "research": SubscriptionPlan(
        id="research",
        name="Research",
        description="Market stats, vehicle history, and dealer picker.",
        monthly_cents=499,
        features=RESEARCH_FEATURES,
        stripe_price_env="STRIPE_PRICE_RESEARCH",
        trial_days=7,
    ),
    "assistant": SubscriptionPlan(
        id="assistant",
        name="Assistant",
        description="Research plus AI car and compare chat.",
        monthly_cents=999,
        features=ASSISTANT_FEATURES,
        stripe_price_env="STRIPE_PRICE_ASSISTANT",
        trial_days=7,
        highlight=True,
    ),
    "complete": SubscriptionPlan(
        id="complete",
        name="Complete",
        description="All features including window stickers and packages.",
        monthly_cents=1499,
        features=COMPLETE_FEATURES,
        stripe_price_env="STRIPE_PRICE_COMPLETE",
        trial_days=14,
    ),
}

# Legacy single-price checkout maps to ``complete``.
LEGACY_PREMIUM_PLAN_ID = "complete"


def get_plan(plan_id: str | None) -> SubscriptionPlan | None:
    pid = (plan_id or "").strip().lower()
    if not pid:
        return None
    return SUBSCRIPTION_PLANS.get(pid)


def paid_plan_ids() -> list[str]:
    return [p.id for p in SUBSCRIPTION_PLANS.values() if p.monthly_cents > 0]


def stripe_price_id_for_plan(plan_id: str) -> str:
    """Resolve Stripe Price ID from env. Falls back to STRIPE_PREMIUM_PRICE_ID for complete."""
    plan = get_plan(plan_id)
    if not plan or not plan.stripe_price_env:
        if plan_id == LEGACY_PREMIUM_PLAN_ID:
            return (os.environ.get("STRIPE_PREMIUM_PRICE_ID") or "").strip()
        return ""
    direct = (os.environ.get(plan.stripe_price_env) or "").strip()
    if direct:
        return direct
    if plan_id == LEGACY_PREMIUM_PLAN_ID:
        return (os.environ.get("STRIPE_PREMIUM_PRICE_ID") or "").strip()
    return ""


def features_for_plan(plan_id: str | None) -> FrozenSet[str]:
    plan = get_plan(plan_id)
    return plan.features if plan else FREE_FEATURES


def minimum_plan_for_feature(feature_id: str) -> str | None:
    """Cheapest paid plan that includes ``feature_id`` (for upsell hints)."""
    fid = (feature_id or "").strip().lower()
    if not fid:
        return None
    best_cents: int | None = None
    best_id: str | None = None
    for plan in SUBSCRIPTION_PLANS.values():
        if plan.monthly_cents <= 0 or fid not in plan.features:
            continue
        if best_cents is None or plan.monthly_cents < best_cents:
            best_cents = plan.monthly_cents
            best_id = plan.id
    return best_id


def plan_display_list() -> list[dict]:
    """Serializable plan list for pricing templates / API."""
    out: list[dict] = []
    for plan in SUBSCRIPTION_PLANS.values():
        if plan.monthly_cents <= 0:
            continue
        out.append(
            {
                "id": plan.id,
                "name": plan.name,
                "description": plan.description,
                "monthly_cents": plan.monthly_cents,
                "monthly_display": f"${plan.monthly_cents / 100:.2f}",
                "features": sorted(plan.features),
                "trial_days": plan.trial_days,
                "highlight": plan.highlight,
                "stripe_configured": bool(stripe_price_id_for_plan(plan.id)),
            }
        )
    return out
