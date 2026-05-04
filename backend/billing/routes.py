from __future__ import annotations

import logging
from typing import Any

from flask import Blueprint, abort, jsonify, redirect, render_template, request, session, url_for

from backend.billing.stripe_billing import (
    billing_enabled,
    construct_webhook_event,
    create_checkout_session,
    unix_to_iso,
)
from backend.db.users_db import get_org, update_org_stripe_subscription
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
        return redirect(url_for("dashboard"))
    uid = _require_app_login()
    if not uid:
        return redirect(url_for("login_page"))
    if is_admin_role(session.get("user_role")):
        return redirect(url_for("dashboard"))
    org_id = _session_org_id()
    org = get_org(org_id) if org_id else None
    return render_template("billing_required.html", org=org, billing_enabled=True)


@bp.route("/checkout")
def billing_checkout():
    if not billing_enabled():
        return redirect(url_for("dashboard"))
    uid = _require_app_login()
    if not uid:
        return redirect(url_for("login_page"))
    if is_admin_role(session.get("user_role")):
        return redirect(url_for("dashboard"))
    org_id = _session_org_id()
    if not org_id:
        return redirect(url_for("dashboard"))
    user_email = (session.get("user_email") or "").strip() or ""
    if not user_email:
        # get_user_by_login returns email, but we don't store it in session currently
        user_email = ""
    cs = create_checkout_session(request=request, org_id=org_id, user_id=uid, user_email=user_email)
    return redirect(cs["url"])


@bp.route("/success")
def billing_success():
    if not billing_enabled():
        return redirect(url_for("dashboard"))
    uid = _require_app_login()
    if not uid:
        return redirect(url_for("login_page"))
    if is_admin_role(session.get("user_role")):
        return redirect(url_for("dashboard"))
    org_id = _session_org_id()
    if not org_id:
        return redirect(url_for("dashboard"))
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

