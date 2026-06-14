"""
Feature entitlement resolution scaffold.

Today: maps legacy ``is_premium`` to ``complete`` plan features.
Future: ``user_subscriptions`` + ``plan_features`` in Postgres.
"""
from __future__ import annotations

from typing import FrozenSet

from backend.billing.catalog import (
    COMPLETE_FEATURES,
    FEATURE_AI_CAR_CHAT,
    FEATURE_AI_COMPARE_CHAT,
    FEATURE_MARKET_INTEL,
    FEATURE_NEARBY_DEALERS,
    FEATURE_PACKAGES_ENSURE,
    FEATURE_VEHICLE_HISTORY,
    FEATURE_WINDOW_STICKER,
    LEGACY_PREMIUM_PLAN_ID,
    features_for_plan,
)
from backend.billing.stripe_billing import billing_enabled, stripe_subscription_active
from backend.utils.roles import is_admin_role

# Map legacy premium API checks to feature ids (for gradual migration).
_LEGACY_PREMIUM_FEATURES = COMPLETE_FEATURES

FEATURE_LABELS: dict[str, str] = {
    FEATURE_AI_CAR_CHAT: "AI car assistant",
    FEATURE_AI_COMPARE_CHAT: "Compare assistant",
    FEATURE_WINDOW_STICKER: "Window stickers",
    FEATURE_VEHICLE_HISTORY: "Vehicle history",
    FEATURE_MARKET_INTEL: "Market intelligence",
    FEATURE_NEARBY_DEALERS: "Nearby dealers",
    FEATURE_PACKAGES_ENSURE: "Package details",
}


def entitlements_from_session(session) -> FrozenSet[str]:
    """Compute feature entitlements from Flask session (scaffold)."""
    if is_admin_role(session.get("user_role")):
        return _LEGACY_PREMIUM_FEATURES

    # Explicit plan id once subscription webhooks populate session
    plan_id = (session.get("subscription_plan_id") or "").strip().lower()
    if plan_id:
        return features_for_plan(plan_id)

    if bool(session.get("user_is_premium")):
        return features_for_plan(LEGACY_PREMIUM_PLAN_ID)

    if stripe_subscription_active(session.get("org_subscription_status")):
        # Dealer org subscription currently grants full consumer features
        return _LEGACY_PREMIUM_FEATURES

    return features_for_plan("free")


def session_has_feature(session, feature_id: str) -> bool:
    fid = (feature_id or "").strip().lower()
    if not fid:
        return False
    if not billing_enabled():
        # Billing off: caller still enforces login in production (SEC-073)
        return True
    return fid in entitlements_from_session(session)


def require_feature(session, feature_id: str) -> tuple[bool, str]:
    if session_has_feature(session, feature_id):
        return True, ""
    return False, "feature_required"
