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
        FEATURE_SAVED_SEARCHES,
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
        description="Market prices, dealership picker, saved searches, recall and title check.",
        monthly_cents=499,
        features=RESEARCH_FEATURES,
        stripe_price_env="STRIPE_PRICE_RESEARCH",
        trial_days=7,
    ),
    "assistant": SubscriptionPlan(
        id="assistant",
        name="Assistant",
        description="Everything in Research, plus AI chat about any car or comparison.",
        monthly_cents=999,
        features=ASSISTANT_FEATURES,
        stripe_price_env="STRIPE_PRICE_ASSISTANT",
        trial_days=7,
        highlight=True,
    ),
    "complete": SubscriptionPlan(
        id="complete",
        name="Complete",
        description="Everything in Assistant, plus window stickers and factory packages.",
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


# Rows for the /premium comparison, in reading order. ``from_plan`` is the cheapest
# plan that unlocks the row; rows gated by "any paid plan" (not a feature id) start at
# research. Descriptions say only what the feature does today (2026-09-30 audit).
PRICING_FEATURE_ROWS: tuple[dict[str, str], ...] = (
    {"label": "Nationwide inventory search", "from_plan": "free",
     "detail": "Every listing we scan, with radius, make, model and price filters and plain-English search."},
    {"label": "Specs, EPA and NHTSA data", "from_plan": "free",
     "detail": "Engine, drivetrain and fuel economy checked against NHTSA's VIN decoder, plus safety ratings and recalls."},
    {"label": "Saved cars and trim neighbors", "from_plan": "free",
     "detail": "Keep a shortlist, and see the trims just above and below the one you're looking at."},
    {"label": "Dealership picker", "from_plan": "research",
     "detail": "Choose the exact dealerships you want to see, near or far, instead of a radius."},
    {"label": "Market price by trim", "from_plan": "research",
     "detail": "The average asking price for the same year, model and trim across our dealer network, next to this car's price."},
    {"label": "Saved searches", "from_plan": "research",
     "detail": "Keep a search and come back to it."},
    {"label": "Recall and title check", "from_plan": "research",
     "detail": "Open recalls, a VIN check and title warnings found in the listing. Not a Carfax or NMVTIS report."},
    {"label": "Full trim lineup", "from_plan": "research",
     "detail": "Every trim of the model, with what each step up adds."},
    {"label": "EV battery estimate", "from_plan": "research",
     "detail": "An estimate of battery health and range from the car's age and mileage. A guide, not a battery test."},
    {"label": "AI car chat", "from_plan": "assistant",
     "detail": "Ask questions about one listing; answers come from that car's own data."},
    {"label": "AI compare chat", "from_plan": "assistant",
     "detail": "Ask how the cars you're comparing differ."},
    {"label": "Window stickers and factory packages", "from_plan": "complete",
     "detail": "The original window sticker where we can get it, and what each installed package contains and cost."},
)

_PLAN_ORDER = ("free", "research", "assistant", "complete")


def pricing_rows() -> list[dict]:
    """PRICING_FEATURE_ROWS with an ``in_plan`` map {plan_id: bool} for the table."""
    out = []
    for row in PRICING_FEATURE_ROWS:
        start = _PLAN_ORDER.index(row["from_plan"])
        out.append({**row, "in_plan": {pid: i >= start for i, pid in enumerate(_PLAN_ORDER)}})
    return out
