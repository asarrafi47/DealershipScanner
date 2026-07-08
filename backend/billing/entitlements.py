"""
Feature entitlement resolution scaffold.

Today: maps legacy ``is_premium`` to ``complete`` plan features.
Future: ``user_subscriptions`` + ``plan_features`` in Postgres.
"""
from __future__ import annotations

import logging
import time
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

_log = logging.getLogger(__name__)

# Sessions are 14-day client-side cookies; a cancel/refund webhook clears the DB
# but can't touch issued cookies. Paid claims in the session are therefore
# revalidated against the users row, cached briefly per worker so the check
# doesn't add a DB read to every gated request.
_REVALIDATE_TTL_SECONDS = 60.0
_billing_cache: dict[int, tuple[float, str | None, bool]] = {}
_BILLING_CACHE_MAX = 10_000


def _live_subscription_flags(user_id: int) -> tuple[str | None, bool] | None:
    """DB-authoritative ``(subscription_plan_id, is_premium)``; None if unknown."""
    now = time.monotonic()
    hit = _billing_cache.get(user_id)
    if hit is not None and hit[0] > now:
        return hit[1], hit[2]
    try:
        from backend.db.users_db import get_user_billing_snapshot

        snap = get_user_billing_snapshot(user_id) or {}
    except Exception:
        # DB unavailable: fail open to the session's claims rather than lock
        # paying users out; the stale-cookie window only persists for the outage.
        _log.warning("billing revalidation failed for user %s", user_id, exc_info=True)
        return None
    plan_id = (snap.get("subscription_plan_id") or "").strip().lower() or None
    is_premium = bool(snap.get("is_premium"))
    if len(_billing_cache) >= _BILLING_CACHE_MAX:
        _billing_cache.clear()
    _billing_cache[user_id] = (now + _REVALIDATE_TTL_SECONDS, plan_id, is_premium)
    return plan_id, is_premium


_org_status_cache: dict[int, tuple[float, str | None]] = {}


def _live_org_status(org_id: int) -> tuple[str | None] | None:
    """DB-authoritative ``(stripe_subscription_status,)``; None if unknown."""
    now = time.monotonic()
    hit = _org_status_cache.get(org_id)
    if hit is not None and hit[0] > now:
        return (hit[1],)
    try:
        from backend.db.users_db import get_org

        org = get_org(org_id) or {}
    except Exception:
        _log.warning("org billing revalidation failed for org %s", org_id, exc_info=True)
        return None
    status = (org.get("stripe_subscription_status") or "").strip().lower() or None
    if len(_org_status_cache) >= _BILLING_CACHE_MAX:
        _org_status_cache.clear()
    _org_status_cache[org_id] = (now + _REVALIDATE_TTL_SECONDS, status)
    return (status,)


def invalidate_billing_cache(user_id: int | None = None) -> None:
    """Drop cached billing flags (call from webhooks so revocation is immediate)."""
    if user_id is None:
        _billing_cache.clear()
        _org_status_cache.clear()
    else:
        _billing_cache.pop(int(user_id), None)


def invalidate_org_billing_cache(org_id: int | None = None) -> None:
    """Drop cached org subscription status (call from org webhooks)."""
    if org_id is None:
        _org_status_cache.clear()
    else:
        _org_status_cache.pop(int(org_id), None)


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
    is_premium = bool(session.get("user_is_premium"))

    # Session flags are only hints: revalidate paid claims against the DB so a
    # canceled/refunded subscription loses access now, not when the cookie dies.
    if plan_id or is_premium:
        uid = session.get("user_id")
        try:
            uid = int(uid) if uid else 0
        except (TypeError, ValueError):
            uid = 0
        if uid > 0:
            live = _live_subscription_flags(uid)
            if live is not None:
                plan_id, is_premium = (live[0] or ""), live[1]
                # Self-heal the cookie so account UI reflects reality too.
                session["subscription_plan_id"] = live[0]
                session["user_is_premium"] = is_premium
        else:
            # Paid flags without an authenticated user are never issued; drop them.
            plan_id, is_premium = "", False

    if plan_id:
        return features_for_plan(plan_id)

    if is_premium:
        return features_for_plan(LEGACY_PREMIUM_PLAN_ID)

    # Dealer org subscription currently grants full consumer features. Like the
    # user flags above, the session's cached status must be revalidated so a
    # canceled org subscription doesn't ride out the cookie's lifetime.
    org_status = session.get("org_subscription_status")
    if stripe_subscription_active(org_status):
        org_id = session.get("org_id")
        try:
            org_id = int(org_id) if org_id else 0
        except (TypeError, ValueError):
            org_id = 0
        if org_id > 0:
            live = _live_org_status(org_id)
            if live is not None:
                org_status = live[0]
                session["org_subscription_status"] = org_status
        else:
            org_status = None
        if stripe_subscription_active(org_status):
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
