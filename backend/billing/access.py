"""One paid-access policy for every gate (monolith audit 2026-10-01, W2 + W22).

Before this module there were four gates that disagreed: ``main._require_feature``
(per-plan, DB-revalidated), ``billing.entitlements.require_feature`` called directly
by the AI chat blueprint (no dev-operator bypass, different error code), and
``_session_has_paid_access`` / ``_viewer_sees_premium_features`` (trusted the
cookie's ``user_is_premium`` and ``user_role``). A Research subscriber therefore saw
window-sticker / packages / chat UI that the APIs then refused, and a demoted admin
kept every paid feature for the 14-day cookie lifetime while ``/admin`` already
re-read the role from the DB.

Now every gate reads one :class:`AccessContext`, built once per request from the DB
users row (role, plan, is_premium, org subscription) plus the dev-operator session
and the billing switch, and cached on :data:`flask.g`.

Rules
-----
- API gate, :meth:`AccessContext.check`: dev operator passes; an anonymous caller
  is refused with ``login_required`` when billing is on or in production; with
  billing off every other caller passes; with billing on the caller needs the
  feature in their plan (``premium_required`` otherwise).
- UI gate, :meth:`AccessContext.shows`: a page shows a paid UI piece only if
  :meth:`can` the feature id that piece's API checks, and only to a signed-in
  viewer (or the dev operator) — anonymous visitors never get paid UI, even in
  development where the API is open.
- :meth:`AccessContext.sees_paid_ui` ("has paid access"): the viewer holds a paid
  entitlement (admin, dev operator, a paid plan, legacy premium, or an active org
  subscription). It is independent of the billing switch: with billing off it
  still separates paying members from free accounts exactly as before (nav chatbot,
  listings market overlay, upgrade prompts).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, FrozenSet

from backend.billing.catalog import (
    ALL_FEATURES,
    COMPLETE_FEATURES,
    LEGACY_PREMIUM_PLAN_ID,
    features_for_plan,
    get_plan,
    minimum_plan_for_feature,
)
from backend.utils.roles import is_admin_role, normalize_role

_log = logging.getLogger(__name__)

_G_KEY = "_ds_access_ctx"

LOGIN_REQUIRED = "login_required"
PREMIUM_REQUIRED = "premium_required"


def billing_enabled() -> bool:
    from backend.config import Config

    return Config.billing_stripe_enabled()


def _production() -> bool:
    from backend.utils import runtime_env

    return runtime_env.is_production_env()


def _dev_pass_through_allowed() -> bool:
    from backend.utils.production_security import app_admin_dev_pass_through_allowed

    return app_admin_dev_pass_through_allowed()


def _org_active(status: str | None) -> bool:
    return (status or "").strip().lower() in ("active", "trialing")


def _int_or_zero(v: Any) -> int:
    try:
        return int(v) if v else 0
    except (TypeError, ValueError):
        return 0


@dataclass(frozen=True)
class AccessContext:
    """What one viewer may use and see, for one request."""

    logged_in: bool = False
    user_id: int = 0
    is_admin: bool = False
    dev_operator: bool = False
    plan_id: str | None = None
    is_premium: bool = False
    org_active: bool = False
    features: FrozenSet[str] = field(default_factory=frozenset)
    billing_enabled: bool = False
    production: bool = False
    profile: dict | None = None

    # -- API gate ---------------------------------------------------------
    def check(self, feature_id: str) -> tuple[bool, str]:
        """``(ok, error_code)``; error is ``login_required`` or ``premium_required``."""
        fid = (feature_id or "").strip().lower()
        if self.dev_operator:
            return True, ""
        if not self.logged_in and (self.billing_enabled or self.production):
            return False, LOGIN_REQUIRED
        if not self.billing_enabled:
            return True, ""
        if fid and fid in self.features:
            return True, ""
        return False, PREMIUM_REQUIRED

    def can(self, feature_id: str) -> bool:
        return self.check(feature_id)[0]

    # -- UI gates ---------------------------------------------------------
    def shows(self, feature_id: str) -> bool:
        """Show the UI piece whose API checks ``feature_id`` (signed-in viewers only)."""
        return (self.logged_in or self.dev_operator) and self.can(feature_id)

    def sees_paid_ui(self) -> bool:
        """Viewer holds a paid entitlement (template ``has_paid_access``)."""
        return bool(
            self.dev_operator
            or self.is_admin
            or self.is_premium
            or self.org_active
            or self.features
        )

    def entitlements(self) -> FrozenSet[str]:
        """Feature ids this viewer's plan grants (billing-agnostic)."""
        return self.features


def _load_profile(uid: int) -> tuple[dict | None, bool]:
    """``(profile, lookup_failed)`` for the users row."""
    try:
        from backend.db.users_db import get_user_profile

        return get_user_profile(uid), False
    except Exception:
        _log.warning("access: get_user_profile(%s) failed", uid, exc_info=True)
        return None, True


def _live_org_status(org_id: int, hint: str | None) -> str | None:
    from backend.billing.entitlements import _live_org_status as _cached_org_status

    live = _cached_org_status(org_id)
    if live is None:  # DB unavailable: fall back to the session's hint
        return hint
    return live[0]


def session_premium_flag(is_premium: Any, plan_id: str | None) -> bool:
    """The session's ``user_is_premium`` value for a users row.

    The cookie's legacy premium flag means the Complete bundle only: the DB column
    is also 1 for Research/Assistant subscribers (``grant_user_premium``). Login
    finalize, checkout success and the per-request self-heal all use this one rule
    so the flag never flips between requests.
    """
    plan = (plan_id or "").strip().lower() or None
    return bool(is_premium) and plan in (None, LEGACY_PREMIUM_PLAN_ID)


def _heal_session(sess, key: str, value: Any) -> None:
    try:
        if sess.get(key) != value:
            sess[key] = value
    except Exception:  # NullSession (no secret key) in bare test apps
        pass


def build_access_context(sess=None) -> AccessContext:
    """Build the viewer's access from the DB (never from cookie role/premium claims)."""
    if sess is None:
        from flask import session as sess  # noqa: PLW0127
    billing = billing_enabled()
    production = _production()
    logged_in = bool(sess.get("user_id"))
    uid = _int_or_zero(sess.get("user_id"))

    profile: dict | None = None
    lookup_failed = False
    if uid > 0:
        profile, lookup_failed = _load_profile(uid)
    if profile is not None:
        from backend.db.users_db import _user_row_is_active

        if not _user_row_is_active(profile):
            profile = None

    is_admin = bool(profile and is_admin_role(profile.get("role")))
    plan_id: str | None = None
    is_premium = False
    org_status: str | None = None
    if profile is not None:
        plan_id = (profile.get("subscription_plan_id") or "").strip().lower() or None
        is_premium = bool(profile.get("is_premium"))
        org_id = _int_or_zero(profile.get("org_id"))
        if org_id > 0:
            org_status = _live_org_status(org_id, sess.get("org_subscription_status"))
        # Self-heal the cookie so the account UI and the remaining cookie readers
        # (oauth upsell, billing pages) see the DB truth too.
        _heal_session(sess, "user_role", normalize_role(profile.get("role")))
        _heal_session(sess, "user_is_premium", session_premium_flag(is_premium, plan_id))
        _heal_session(sess, "subscription_plan_id", plan_id)
        if org_id > 0:
            _heal_session(sess, "org_subscription_status", org_status)
    elif lookup_failed:
        # DB outage: fail open to the session's paid claims (as entitlements did)
        # so paying users are not locked out; admin fails closed.
        plan_id = (sess.get("subscription_plan_id") or "").strip().lower() or None
        is_premium = bool(sess.get("user_is_premium"))
        org_status = sess.get("org_subscription_status")

    org_active = _org_active(org_status)
    dev_operator = bool(sess.get("admin_user_id")) or (
        logged_in and is_admin and _dev_pass_through_allowed()
    )

    features: FrozenSet[str] = frozenset()
    if dev_operator:
        features = ALL_FEATURES
    elif is_admin:
        features = COMPLETE_FEATURES
    else:
        # Same precedence as the pre-2026-10 entitlements_from_session: the user's
        # own plan wins, then the legacy premium flag, and only a user with
        # neither gets the dealer org's Complete bundle (plan + org = plan only;
        # changing that is a billing-policy decision for the owner).
        if plan_id:
            features = features_for_plan(plan_id)
        elif is_premium:
            features = features_for_plan(LEGACY_PREMIUM_PLAN_ID)
        elif org_active:
            features = COMPLETE_FEATURES

    return AccessContext(
        logged_in=logged_in,
        user_id=uid,
        is_admin=is_admin,
        dev_operator=dev_operator,
        plan_id=plan_id,
        is_premium=is_premium,
        org_active=org_active,
        features=frozenset(features),
        billing_enabled=billing,
        production=production,
        profile=profile,
    )


def current_access() -> AccessContext:
    """The request's :class:`AccessContext`, cached on ``flask.g``.

    The cache key is the session identity (``user_id``, ``admin_user_id``), so a
    login or logout earlier in the same request rebuilds it; billing writes call
    :func:`invalidate_access`.
    """
    from flask import g, has_request_context, session

    if not has_request_context():
        return AccessContext(billing_enabled=billing_enabled(), production=_production())
    key = (session.get("user_id"), session.get("admin_user_id"))
    hit = g.get(_G_KEY)
    if hit is not None and hit[0] == key:
        return hit[1]
    ctx = build_access_context(session)
    setattr(g, _G_KEY, (key, ctx))
    return ctx


def invalidate_access() -> None:
    """Drop the request's cached context (after a grant/revoke in this request)."""
    from flask import g, has_request_context

    if has_request_context():
        g.pop(_G_KEY, None)


# -- convenience wrappers used by the gates ---------------------------------

def check_feature(feature_id: str) -> tuple[bool, str]:
    return current_access().check(feature_id)


def can_use(feature_id: str) -> bool:
    return current_access().can(feature_id)


def shows(feature_id: str) -> bool:
    return current_access().shows(feature_id)


def sees_paid_ui() -> bool:
    return current_access().sees_paid_ui()


def is_site_admin() -> bool:
    """DB-revalidated admin role for the current request."""
    return current_access().is_admin


def denied_json(feature_id: str, err: str, **extra: Any) -> dict[str, Any]:
    """403 JSON body; adds the upgrade hint when billing blocks a feature."""
    body: dict[str, Any] = {"ok": False, "error": err, **extra}
    if err == PREMIUM_REQUIRED:
        plan_id = minimum_plan_for_feature(feature_id)
        if plan_id:
            plan = get_plan(plan_id)
            body["required_feature"] = feature_id
            body["upgrade_plan_id"] = plan_id
            body["upgrade_plan_name"] = plan.name if plan else plan_id
            try:
                from flask import url_for

                body["upgrade_url"] = url_for("premium_page") + f"?plan={plan_id}"
            except Exception:  # app without the premium page (bare blueprint tests)
                pass
    return body


def template_context() -> dict[str, Any]:
    """Context-processor values: ``has_paid_access``, ``viewer_can``, ``viewer_features``."""
    ctx = current_access()
    return {
        "has_paid_access": ctx.sees_paid_ui(),
        "viewer_can": ctx.shows,
        "viewer_features": sorted(ctx.features),
    }
