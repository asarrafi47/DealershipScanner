"""Golden parity for the single paid-access policy (backend/billing/access.py).

``fixtures/paid_access_legacy_golden.json`` was captured from the four legacy gates
(``main._require_feature``, ``billing.entitlements.require_feature`` as used by the
AI chat blueprint, ``main._session_has_paid_access`` and
``main._viewer_sees_premium_features``) plus the template expressions that consumed
them, on b1578241d, before the consolidation. Matrix: {dev, prod} x {billing off, on}
x {anonymous, free, research, assistant, complete, org_paid, admin, demoted_admin,
dev_operator, research_org, org_only} x {every feature id, the AI chat endpoint, has_paid_access and every
paid UI piece}, against real users/org rows in the test users DB.

``research_org`` (a Research subscriber who is also a member of an active paid org)
and ``org_only`` (a general user whose only entitlement is the org) were captured
later from the same b1578241d tree (``git archive``) with the same observer; that
capture reproduced all 36 original rows exactly. Old rule: the user's own plan wins
and the org is ignored; only a user with no plan and no premium flag gets the org's
Complete bundle.

The new single source must reproduce the golden exactly, except the cells in
``EXPECTED_CHANGES`` -- each one a bug the audit (web.md W2/W22) asked to fix.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from flask import render_template_string

from backend.billing.catalog import ALL_FEATURES

GOLDEN = json.loads((Path(__file__).parent / "fixtures" / "paid_access_legacy_golden.json").read_text())
TEMPLATES = Path(__file__).resolve().parents[2] / "frontend" / "templates"

FEATURES = sorted(ALL_FEATURES)
PERSONAS = ["anonymous", "free", "research", "assistant", "complete", "org_paid",
            "admin", "demoted_admin", "dev_operator", "research_org", "org_only"]

# Each UI piece: (template file, the exact expression it now uses).
NEW_UI = {
    "nav_chatbot": ("_nav_public.html", "has_paid_access and viewer_can('ai_car_chat')"),
    "car_premium_ui": ("car.html", "viewer_can('window_sticker')"),
    "car_packages_js": ("car.html", "viewer_can('packages_ensure')"),
    "car_options_upsell": ("car.html", "logged_in and billing_stripe_enabled and not viewer_can('packages_ensure')"),
    "nav_upsell": ("home.html", "billing_stripe_enabled and not has_paid_access"),
    "listings_market_json": ("listings.html", "has_paid_access and viewer_can('market_intel')"),
    "listings_dealer_picker": ("listings.html", "has_paid_access and viewer_can('nearby_dealers')"),
    "compare_upsell_if_no_chat": ("compare.html", "billing_stripe_enabled and not viewer_can('ai_compare_chat')"),
}

_PAID_PIECES_OFF_PLAN = {
    # piece -> feature its API checks (for the plan-mismatch fixes below)
    "srv:trim_ladder_adds": "window_sticker",
    "viewer_premium": "window_sticker",
    "ui:car_premium_ui": "window_sticker",
    "ui:car_packages_js": "packages_ensure",
    "srv:compare_chat": "ai_compare_chat",
    "ui:nav_chatbot": "ai_car_chat",
}


def _expected_changes() -> dict[str, dict]:
    """Every cell where the consolidated policy answers differently, with the reason."""
    ch: dict[str, dict] = {}

    def put(key, field, value):
        if GOLDEN[key][field] != value:
            ch.setdefault(key, {})[field] = value

    for env in ("dev", "prod"):
        for billing in ("off", "on"):
            # W22: a demoted admin (cookie still says admin, DB says general_user)
            # is now a free user on the next request, in every gate.
            k = f"{env}|{billing}|demoted_admin"
            free = GOLDEN[f"{env}|{billing}|free"]
            for field, value in free.items():
                if GOLDEN[k][field] != value:
                    put(k, field, value)
        # W2: with billing on, Research/Assistant subscribers no longer see paid UI
        # whose API refuses them (sticker / packages / trim adds / chats).
        for plan in ("research", "assistant", "research_org"):
            k = f"{env}|on|{plan}"
            for field, fid in _PAID_PIECES_OFF_PLAN.items():
                if GOLDEN[k][f"api:{fid}"] != "ok":
                    put(k, field, False)
            if GOLDEN[k]["api:packages_ensure"] != "ok":
                put(k, "ui:car_options_upsell", True)
            if GOLDEN[k]["api:ai_compare_chat"] != "ok":
                put(k, "ui:compare_upsell_if_no_chat", True)
            if GOLDEN[k]["api:ai_car_chat"] != "ok":
                # chat endpoint now uses the shared gate: premium_required + upgrade hint
                put(k, "api:ai_chat_bp", "403:premium_required")
        # AI chat endpoint joins the shared gate: anonymous with billing on gets
        # login_required (401), free users premium_required, dev operator passes.
        put(f"{env}|on|anonymous", "api:ai_chat_bp", "401:login_required")
        put(f"{env}|on|free", "api:ai_chat_bp", "403:premium_required")
        put(f"{env}|on|demoted_admin", "api:ai_chat_bp", "403:premium_required")
        for billing in ("off", "on"):
            put(f"{env}|{billing}|dev_operator", "api:ai_chat_bp", "400:message_required")
    return ch


EXPECTED_CHANGES = _expected_changes()


def _seed():
    from backend.db.users_db import (
        create_org, grant_user_premium, save_user, update_org_stripe_subscription,
    )
    ids = {}
    for name, role in [("free", "general_user"), ("research", "general_user"),
                       ("assistant", "general_user"), ("complete", "general_user"),
                       ("admin", "admin"), ("demoted_admin", "general_user")]:
        ids[name] = save_user(f"pa_{name}", f"pa_{name}@example.com", "long-enough-password-1", role=role)
    for plan in ("research", "assistant", "complete"):
        grant_user_premium(ids[plan], plan_id=plan)
    org = create_org("Paid Org")
    update_org_stripe_subscription(org, status="active")
    ids["org_paid"] = save_user("pa_org", "pa_org@example.com", "long-enough-password-1",
                                role="dealership_member", org_id=org)
    ids["research_org"] = save_user("pa_research_org", "pa_research_org@example.com",
                                    "long-enough-password-1", role="dealership_member", org_id=org)
    grant_user_premium(ids["research_org"], plan_id="research")
    ids["org_only"] = save_user("pa_org_only", "pa_org_only@example.com",
                                "long-enough-password-1", role="general_user", org_id=org)
    ids["_org"] = org
    return ids


def _session_for(persona, ids):
    if persona == "anonymous":
        return {}
    if persona == "dev_operator":
        return {"admin_user_id": 1, "admin_username": "devop"}
    s = {"user_id": ids[persona], "username": f"pa_{persona}", "user_role": "general_user",
         "user_is_premium": False}
    if persona in ("research", "assistant", "complete", "research_org"):
        # what plan_checkout_success / _finalize_app_session left in the cookie
        s["user_is_premium"] = True
        s["subscription_plan_id"] = "research" if persona == "research_org" else persona
    if persona in ("admin", "demoted_admin"):
        s["user_role"] = "admin"
    if persona in ("org_paid", "research_org", "org_only"):
        if persona != "org_only":
            s["user_role"] = "dealership_member"
        s["org_id"] = ids["_org"]
        s["org_subscription_status"] = "active"
    return s


def _observe(main, persona, ids):
    from flask import session

    from backend.billing import access
    from backend.billing.entitlements import invalidate_billing_cache

    invalidate_billing_cache()
    out = {}
    with main.app.test_request_context("/"):
        session.update(_session_for(persona, ids))
        for fid in FEATURES:
            ok, err = access.check_feature(fid)
            out[f"api:{fid}"] = "ok" if ok else err
        ctx = access.current_access()
        out["has_paid_access"] = ctx.sees_paid_ui()
        out["viewer_premium"] = ctx.shows("window_sticker")
        # the server-side pieces in routes/cars_pages.py
        out["srv:car_market_intel"] = ctx.sees_paid_ui() and ctx.shows("market_intel")
        out["srv:trim_ladder_adds"] = ctx.shows("window_sticker")
        out["srv:compare_chat"] = ctx.shows("ai_compare_chat")
        out["srv:saved_searches_enabled"] = bool(access.check_feature("saved_searches")[0])
        for k, (_tpl, expr) in NEW_UI.items():
            v = render_template_string("{{ 1 if (" + expr + ") else 0 }}", logged_in=bool(session.get("user_id")))
            out[f"ui:{k}"] = v.strip() == "1"
    with main.app.test_request_context("/api/ai/chat", method="POST", json={}):
        session.update(_session_for(persona, ids))
        rv = main.app.view_functions["ai_chat_bp.api_ai_chat"]()
        body, status = (rv if isinstance(rv, tuple) else (rv, rv.status_code))
        out["api:ai_chat_bp"] = f"{status}:{body.get_json().get('error')}"
    return out


def test_ui_expressions_are_the_templates_expressions():
    for name, (tpl, expr) in NEW_UI.items():
        assert expr in (TEMPLATES / tpl).read_text(), f"{name}: {expr!r} not in {tpl}"


def test_matrix_matches_golden_except_listed_fixes(app_factory, monkeypatch):
    main = app_factory(BILLING_STRIPE_ENABLED="0")
    with main.app.app_context():
        ids = _seed()
    mismatches = []
    for env in ("dev", "prod"):
        for billing in ("off", "on"):
            monkeypatch.setenv("BILLING_STRIPE_ENABLED", "1" if billing == "on" else "0")
            monkeypatch.setenv("FLASK_ENV", "production" if env == "prod" else "development")
            for p in PERSONAS:
                key = f"{env}|{billing}|{p}"
                want = dict(GOLDEN[key])
                want.update(EXPECTED_CHANGES.get(key, {}))
                got = _observe(main, p, ids)
                for field in sorted(set(want) | set(got)):
                    if want.get(field) != got.get(field):
                        mismatches.append((key, field, GOLDEN[key].get(field), want.get(field), got.get(field)))
    assert not mismatches, "\n".join(f"{k} {f}: legacy={l!r} expected={w!r} got={g!r}" for k, f, l, w, g in mismatches)


def test_billing_off_changes_only_the_demoted_admin_and_dev_operator_chat():
    """Production runs billing OFF: what each real user sees must not move."""
    for key, fields in EXPECTED_CHANGES.items():
        env, billing, persona = key.split("|")
        if billing == "off":
            assert persona in ("demoted_admin", "dev_operator"), key
            if persona == "dev_operator":
                # prod: the chat endpoint now honours the dev-operator bypass too
                assert set(fields) == {"api:ai_chat_bp"} and env == "prod"


def test_research_plan_checkout_sets_the_plan_not_blanket_premium(app_factory, monkeypatch):
    """W2 addendum: plan_checkout_success wrote user_is_premium=True for every plan."""
    main = app_factory(BILLING_STRIPE_ENABLED="1")
    import backend.billing.routes as billing_routes
    from backend.billing.catalog import RESEARCH_FEATURES
    from backend.db.users_db import save_user

    monkeypatch.setattr(billing_routes, "verify_plan_checkout_session", lambda **_k: True)
    with main.app.app_context():
        uid = save_user("pa_buyer", "pa_buyer@example.com", "long-enough-password-1", role="general_user")
    client = main.app.test_client()
    with client.session_transaction() as sess:
        sess.update({"user_id": uid, "username": "pa_buyer", "user_role": "general_user"})
    rv = client.get("/billing/plan/success?plan=research&session_id=cs_test_1")
    assert rv.status_code == 200
    with client.session_transaction() as sess:
        assert sess["subscription_plan_id"] == "research"
        assert sess["user_is_premium"] is False
    from flask import session

    from backend.billing import access

    with main.app.test_request_context("/"):
        session.update({"user_id": uid, "user_role": "general_user"})
        ctx = access.current_access()
        assert ctx.features == RESEARCH_FEATURES
        assert ctx.sees_paid_ui() is True
        assert not ctx.shows("window_sticker") and not ctx.shows("ai_car_chat")
        assert ctx.shows("market_intel")


def test_demoted_admin_loses_paid_features_on_the_next_request(app_factory, monkeypatch):
    main = app_factory(BILLING_STRIPE_ENABLED="1")
    from flask import session

    from backend.billing import access
    from backend.db.users_db import save_user

    with main.app.app_context():
        uid = save_user("pa_adm", "pa_adm@example.com", "long-enough-password-1", role="admin")
    with main.app.test_request_context("/"):
        session.update({"user_id": uid, "user_role": "admin"})
        assert access.current_access().can("window_sticker")
    conn_mod = __import__("backend.db.users_db", fromlist=["get_conn"])
    conn = conn_mod.get_conn()
    conn.execute("UPDATE users SET role = 'general_user' WHERE id = ?", (uid,))
    conn.commit()
    conn.close()
    with main.app.test_request_context("/"):
        session.update({"user_id": uid, "user_role": "admin"})  # stale 14-day cookie
        ctx = access.current_access()
        assert not ctx.is_admin
        assert ctx.check("window_sticker") == (False, "premium_required")
        assert session["user_role"] == "general_user"  # cookie healed


def test_plan_plus_active_org_gets_the_plan_only(app_factory, monkeypatch):
    """Old entitlements_from_session rule: the user's plan wins; the org is ignored."""
    main = app_factory(BILLING_STRIPE_ENABLED="1")
    from flask import session

    from backend.billing import access
    from backend.billing.catalog import COMPLETE_FEATURES, RESEARCH_FEATURES

    with main.app.app_context():
        ids = _seed()
    for persona, want in (("research_org", RESEARCH_FEATURES), ("org_only", COMPLETE_FEATURES)):
        with main.app.test_request_context("/"):
            session.update(_session_for(persona, ids))
            ctx = access.current_access()
            assert ctx.org_active is True, persona
            assert ctx.features == want, persona


@pytest.mark.parametrize("persona, flag", [("research", False), ("assistant", False),
                                           ("complete", True), ("free", False),
                                           ("research_org", False), ("org_only", False)])
def test_user_is_premium_is_set_once_and_never_flips(app_factory, persona, flag):
    """Login finalize and the per-request self-heal store the same value."""
    main = app_factory(BILLING_STRIPE_ENABLED="1")
    from flask import session

    from backend.auth.session import finalize_app_session
    from backend.billing import access

    with main.app.app_context():
        ids = _seed()
    with main.app.test_request_context("/"):
        assert finalize_app_session(ids[persona])
        after_login = dict(session)
        assert after_login["user_is_premium"] is flag
    with main.app.test_request_context("/"):
        session.update(after_login)
        access.current_access()
        assert session["user_is_premium"] is flag
        assert session.get("subscription_plan_id") == after_login.get("subscription_plan_id")


def test_session_premium_flag_rule():
    from backend.billing.access import session_premium_flag

    assert session_premium_flag(1, None) is True
    assert session_premium_flag(True, "complete") is True
    assert session_premium_flag(1, " Complete ") is True
    assert session_premium_flag(1, "research") is False
    assert session_premium_flag(1, "assistant") is False
    assert session_premium_flag(0, None) is False
    assert session_premium_flag(0, "complete") is False
