from __future__ import annotations

import logging
from typing import Any

from flask import Blueprint, abort, jsonify, redirect, render_template, request, session, url_for

from backend.billing.checkout_service import create_plan_checkout_session, verify_plan_checkout_session
from backend.billing.catalog import get_plan, plan_display_list
from backend.billing.stripe_billing import (
    billing_enabled,
    construct_premium_webhook_event,
    construct_webhook_event,
    create_checkout_session,
    create_premium_checkout_session,
    stripe_subscription_active,
    unix_to_iso,
    verify_premium_checkout_session,
)
from backend.db.users_db import (
    get_org,
    grant_user_premium,
    revoke_user_premium,
    update_org_stripe_subscription,
)
from backend.utils.roles import is_admin_role

_log = logging.getLogger(__name__)

bp = Blueprint("billing", __name__, url_prefix="/billing")


def _require_app_login() -> int:
    uid = session.get("user_id")
    if not uid:
        return 0
    try:
        return int(uid)
    except (TypeError, ValueError):
        return 0


def _session_org_id() -> int:
    oid = session.get("org_id")
    if not oid:
        return 0
    try:
        return int(oid)
    except (TypeError, ValueError):
        return 0


@bp.route("/required")
def billing_required():
    if not billing_enabled():
        return redirect(url_for("app_home"))
    uid = _require_app_login()
    if not uid:
        return redirect(url_for("login_page"))
    if is_admin_role(session.get("user_role")):
        return redirect(url_for("app_home"))
    org_id = _session_org_id()
    org = get_org(org_id) if org_id else None
    return render_template("billing_required.html", org=org, billing_enabled=True)


@bp.route("/checkout")
def billing_checkout():
    if not billing_enabled():
        return redirect(url_for("app_home"))
    uid = _require_app_login()
    if not uid:
        return redirect(url_for("login_page"))
    if is_admin_role(session.get("user_role")):
        return redirect(url_for("app_home"))
    org_id = _session_org_id()
    if not org_id:
        return redirect(url_for("app_home"))
    user_email = (session.get("user_email") or "").strip() or ""
    if not user_email:
        # get_user_by_login returns email, but we don't store it in session currently
        user_email = ""
    try:
        cs = create_checkout_session(request=request, org_id=org_id, user_id=uid, user_email=user_email)
        return redirect(cs["url"])
    except Exception:
        _log.exception("billing checkout session creation failed")
        return redirect(url_for("billing.billing_required"))


@bp.route("/success")
def billing_success():
    if not billing_enabled():
        return redirect(url_for("app_home"))
    uid = _require_app_login()
    if not uid:
        return redirect(url_for("login_page"))
    if is_admin_role(session.get("user_role")):
        return redirect(url_for("app_home"))
    org_id = _session_org_id()
    if not org_id:
        return redirect(url_for("app_home"))
    org = get_org(org_id)
    if org:
        session["org_subscription_status"] = (org.get("stripe_subscription_status") or "").strip().lower() or None
    return render_template("billing_success.html", org=org)


@bp.route("/webhook", methods=["POST"])
def stripe_webhook():
    if not billing_enabled():
        abort(404)
    payload = request.get_data(cache=False) or b""
    sig = request.headers.get("Stripe-Signature") or ""
    try:
        event = construct_webhook_event(payload, sig)
    except Exception:
        _log.warning("stripe webhook signature verification failed", exc_info=True)
        return jsonify({"ok": False, "error": "invalid_signature"}), 400

    etype = (event.get("type") or "").strip()
    obj = ((event.get("data") or {}).get("object") or {}) if isinstance(event.get("data"), dict) else {}

    def _org_id_from_metadata(o: dict[str, Any]) -> int:
        md = o.get("metadata") or {}
        if not isinstance(md, dict):
            return 0
        raw = (md.get("org_id") or "").strip()
        try:
            return int(raw)
        except (TypeError, ValueError):
            return 0

    org_id = _org_id_from_metadata(obj)
    customer = obj.get("customer")
    subscription_id = obj.get("id")
    status = obj.get("status")
    cpe = unix_to_iso(obj.get("current_period_end"))

    # Some event types send a Checkout Session object. Prefer the subscription it points to.
    if etype in ("checkout.session.completed",):
        org_id = _org_id_from_metadata(obj) or org_id
        customer = obj.get("customer") or customer
        subscription_id = obj.get("subscription") or subscription_id
        status = None
        cpe = None

    if org_id:
        update_org_stripe_subscription(
            org_id,
            customer_id=str(customer) if customer else None,
            subscription_id=str(subscription_id) if subscription_id else None,
            status=str(status) if status else None,
            current_period_end_iso=str(cpe) if cpe else None,
        )

    return jsonify({"ok": True})


# ── Consumer premium (user-level subscription) ───────────────────────────────

@bp.route("/premium/checkout")
def premium_checkout():
    if not billing_enabled():
        return redirect(url_for("premium_page"))
    uid = _require_app_login()
    if not uid:
        return redirect(url_for("login_page") + "?next=/premium")
    user_email = (session.get("user_email") or "").strip()
    try:
        cs = create_premium_checkout_session(request=request, user_id=uid, user_email=user_email)
        return redirect(cs["url"])
    except Exception:
        _log.exception("premium checkout session creation failed")
        return redirect(url_for("premium_page"))


@bp.route("/premium/success")
def premium_success():
    uid = _require_app_login()
    if not uid:
        return redirect(url_for("login_page") + "?next=/premium")
    activated = False
    checkout_error = ""
    if billing_enabled():
        stripe_sid = (request.args.get("session_id") or "").strip()
        if not stripe_sid:
            checkout_error = "missing_session"
        elif verify_premium_checkout_session(session_id=stripe_sid, user_id=uid):
            grant_user_premium(uid, session_id=stripe_sid)
            session["user_is_premium"] = True
            activated = True
        else:
            checkout_error = "payment_not_verified"
            _log.warning(
                "premium success: checkout session not verified (user_id=%s session_id=%s)",
                uid,
                stripe_sid[:24] + "…" if len(stripe_sid) > 24 else stripe_sid,
            )
    else:
        checkout_error = "billing_disabled"
    return render_template(
        "premium_success.html",
        premium_activated=activated,
        checkout_error=checkout_error,
    )


@bp.route("/premium/webhook", methods=["POST"])
def premium_webhook():
    if not billing_enabled():
        abort(404)
    payload = request.get_data(cache=False) or b""
    sig = request.headers.get("Stripe-Signature") or ""
    try:
        event = construct_premium_webhook_event(payload, sig)
    except Exception:
        _log.warning("premium webhook signature verification failed", exc_info=True)
        return jsonify({"ok": False, "error": "invalid_signature"}), 400

    etype = (event.get("type") or "").strip()
    obj = ((event.get("data") or {}).get("object") or {}) if isinstance(event.get("data"), dict) else {}

    def _premium_user_id_from_metadata(o: dict[str, Any]) -> int:
        md = o.get("metadata") or {}
        if not isinstance(md, dict):
            return 0
        raw_uid = (md.get("user_id") or "").strip()
        try:
            return int(raw_uid)
        except (TypeError, ValueError):
            return 0

    if etype == "checkout.session.completed":
        user_id = _premium_user_id_from_metadata(obj)
        customer = obj.get("customer")
        session_id = obj.get("id")
        subscription_id = obj.get("subscription")
        if user_id > 0:
            md = obj.get("metadata") or {}
            plan_id = (md.get("plan_id") or "").strip().lower() or None
            grant_user_premium(
                user_id,
                customer_id=str(customer) if customer else None,
                session_id=str(session_id) if session_id else None,
                subscription_id=str(subscription_id) if subscription_id else None,
                plan_id=plan_id,
            )
            _log.info("premium granted to user_id=%d via webhook plan=%s", user_id, plan_id)

    elif etype in ("customer.subscription.updated", "customer.subscription.deleted"):
        user_id = _premium_user_id_from_metadata(obj)
        if user_id <= 0:
            return jsonify({"ok": True})
        customer = obj.get("customer")
        subscription_id = obj.get("id")
        status = obj.get("status")
        if etype == "customer.subscription.deleted" or not stripe_subscription_active(status):
            revoke_user_premium(user_id)
            _log.info("premium revoked for user_id=%d (status=%s)", user_id, status)
        else:
            md = obj.get("metadata") or {}
            plan_id = (md.get("plan_id") or "").strip().lower() or None
            grant_user_premium(
                user_id,
                customer_id=str(customer) if customer else None,
                subscription_id=str(subscription_id) if subscription_id else None,
                plan_id=plan_id,
            )
            _log.info("premium renewed for user_id=%d (status=%s plan=%s)", user_id, status, plan_id)

    return jsonify({"ok": True})


# ── Multi-plan consumer checkout (catalog scaffold) ───────────────────────────

@bp.route("/plans")
def billing_plans_api():
    """JSON catalog for pricing UI (no secrets)."""
    return jsonify({"plans": plan_display_list(), "billing_enabled": billing_enabled()})


@bp.route("/plan/checkout")
def plan_checkout():
    if not billing_enabled():
        return redirect(url_for("premium_page"))
    uid = _require_app_login()
    if not uid:
        return redirect(url_for("login_page") + "?next=/premium")
    plan_id = (request.args.get("plan") or "complete").strip().lower()
    promo = (request.args.get("promo") or request.args.get("code") or "").strip() or None
    plan = get_plan(plan_id)
    if not plan or plan.monthly_cents <= 0:
        return redirect(url_for("premium_page"))
    user_email = (session.get("user_email") or "").strip()
    try:
        cs = create_plan_checkout_session(
            request=request,
            user_id=uid,
            user_email=user_email,
            plan_id=plan_id,
            promo_code=promo,
        )
        return redirect(cs["url"])
    except Exception:
        _log.exception("plan checkout failed plan_id=%s", plan_id)
        return redirect(url_for("premium_page", plan=plan_id))


@bp.route("/plan/success")
def plan_checkout_success():
    uid = _require_app_login()
    if not uid:
        return redirect(url_for("login_page") + "?next=/premium")
    plan_id = (request.args.get("plan") or "complete").strip().lower()
    activated = False
    checkout_error = ""
    if billing_enabled():
        stripe_sid = (request.args.get("session_id") or "").strip()
        if not stripe_sid:
            checkout_error = "missing_session"
        elif verify_plan_checkout_session(session_id=stripe_sid, user_id=uid, plan_id=plan_id):
            grant_user_premium(uid, session_id=stripe_sid, plan_id=plan_id)
            session["user_is_premium"] = True
            session["subscription_plan_id"] = plan_id
            activated = True
        else:
            checkout_error = "payment_not_verified"
    else:
        checkout_error = "billing_disabled"
    return render_template(
        "premium_success.html",
        premium_activated=activated,
        checkout_error=checkout_error,
        plan_id=plan_id,
    )

