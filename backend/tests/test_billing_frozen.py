"""F1: the site is free, billing is dormant (owner decision 2026-11-13)."""

from __future__ import annotations

import logging
import re

import pytest

from backend.utils.production_security import assert_production_security_config

_PRICE = re.compile(r"\$\d+\.\d\d")
_UPSELL = re.compile(r"(?i)upgrade|go premium|view plans|premium plans")
_PLAN_NAMES = ("Research plan", "Assistant plan", "Complete plan")
_PS_LOGGER = "backend.utils.production_security"


@pytest.fixture()
def free_app(monkeypatch, app_factory):
    monkeypatch.delenv("BILLING_STRIPE_ENABLED", raising=False)
    return app_factory().app


def test_shipped_default_is_billing_off(monkeypatch):
    from backend.config import Config

    monkeypatch.delenv("BILLING_STRIPE_ENABLED", raising=False)
    assert Config.billing_stripe_enabled() is False


@pytest.mark.parametrize("path", ["/", "/listings", "/compare", "/login", "/register"])
def test_no_price_plan_or_upgrade_text_with_billing_off(free_app, path):
    r = free_app.test_client().get(path)
    assert r.status_code == 200
    body = r.get_data(as_text=True)
    assert not _PRICE.search(body)
    assert not _UPSELL.search(body)
    assert not any(n in body for n in _PLAN_NAMES)


def test_premium_page_shows_no_price_with_billing_off(free_app):
    r = free_app.test_client().get("/premium")
    assert r.status_code == 200
    body = r.get_data(as_text=True)
    assert not _PRICE.search(body)
    assert "aren't on sale yet" in body
    assert "Upgrade to Premium" not in body


def test_billing_plans_json_is_404_with_billing_off(free_app):
    assert free_app.test_client().get("/billing/plans").status_code == 404


@pytest.mark.parametrize(
    "path,dest",
    [
        ("/billing/required", "/home"),
        ("/billing/checkout", "/home"),
        ("/billing/premium/checkout", "/premium"),
        ("/billing/plan/checkout", "/premium"),
    ],
)
def test_billing_pages_redirect_with_billing_off(free_app, path, dest):
    r = free_app.test_client().get(path, follow_redirects=False)
    assert r.status_code in (302, 303)
    assert r.headers["Location"].endswith(dest)


@pytest.mark.parametrize("path", ["/billing/webhook", "/billing/premium/webhook"])
def test_both_stripe_webhooks_404_with_billing_off(free_app, path):
    assert free_app.test_client().post(path, data=b"{}").status_code == 404


def _prod_env(monkeypatch):
    import backend.db.inventory_pg as inventory_pg
    import backend.utils.production_security as ps

    monkeypatch.setenv("FLASK_ENV", "production")
    monkeypatch.setattr(ps, "assert_credential_db_encryption_config", lambda: None)
    monkeypatch.setattr(inventory_pg, "assert_inventory_backend_configured", lambda: None)


def _warnings(caplog):
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger=_PS_LOGGER):
        assert_production_security_config()
    return [r.getMessage() for r in caplog.records if r.name == _PS_LOGGER]


def test_boot_warning_when_production_starts_with_billing_on(monkeypatch, caplog):
    _prod_env(monkeypatch)
    monkeypatch.setenv("BILLING_STRIPE_ENABLED", "1")
    assert any("billing is dormant by owner decision" in m for m in _warnings(caplog))


def test_no_billing_warning_when_production_billing_off(monkeypatch, caplog):
    _prod_env(monkeypatch)
    monkeypatch.delenv("BILLING_STRIPE_ENABLED", raising=False)
    assert not any("BILLING_STRIPE_ENABLED" in m and "dormant" in m for m in _warnings(caplog))


# ---------------------------------------------------------------- review follow-ups (wave 1 integration)

_GENERAL_USER = {"user_id": 1, "username": "free_probe", "user_role": "general_user", "user_is_premium": False}

# One route per feature id (backend/billing/catalog.py). Signed in, no plan, billing off: never 403.
_FEATURE_ROUTES = [
    ("ai_car_chat", "POST", "/api/car/1/chat"),
    ("ai_car_chat", "POST", "/api/ai/chat"),
    ("ai_compare_chat", "POST", "/api/compare/chat"),
    ("window_sticker", "GET", "/api/cars/1/window-sticker"),
    ("vehicle_history", "GET", "/api/cars/1/vehicle-history-intelligence"),
    ("market_intel", "GET", "/api/listings/market-stats"),
    ("nearby_dealers", "GET", "/api/nearby-dealers"),
    ("packages_ensure", "POST", "/api/cars/1/packages/ensure"),
    ("saved_searches", "GET", "/api/saved-searches"),
]


def _signed_in(app, **session_values):
    client = app.test_client()
    with client.session_transaction() as sess:
        sess.update(session_values or _GENERAL_USER)
    return client


def test_every_feature_id_has_a_route_in_the_table():
    from backend.billing import catalog

    assert {fid for fid, _m, _p in _FEATURE_ROUTES} == set(catalog.ALL_FEATURES)


@pytest.mark.parametrize("feature,method,path", _FEATURE_ROUTES)
def test_signed_in_general_user_gets_no_403_from_each_feature_route(free_app, feature, method, path):
    from backend.utils.csrf import _SESSION_KEY

    client = _signed_in(free_app)
    with client.session_transaction() as sess:
        sess[_SESSION_KEY] = "t" * 32
    r = client.open(path, method=method, json={} if method == "POST" else None,
                    headers={"X-CSRF-Token": "t" * 32} if method == "POST" else {})
    body = r.get_data(as_text=True)
    assert "premium_required" not in body and "login_required" not in body, (feature, path)
    assert r.status_code != 403, (feature, path, body[:200])


def test_account_billing_redirects_to_the_profile_with_billing_off(free_app):
    r = _signed_in(free_app).get("/account/billing", follow_redirects=False)
    assert r.status_code in (302, 303)
    assert r.headers["Location"].endswith("/account/profile")


def test_account_billing_portal_stays_off_with_billing_off(free_app):
    r = _signed_in(free_app).get("/account/billing/portal", follow_redirects=False)
    assert r.status_code in (302, 303)
    assert "stripe" not in r.headers["Location"].lower()


def test_account_profile_has_no_price_plan_name_or_billing_row_with_billing_off(free_app):
    r = _signed_in(free_app).get("/account/profile")
    assert r.status_code == 200
    body = r.get_data(as_text=True)
    assert not _PRICE.search(body) and not _UPSELL.search(body)
    assert not any(n in body for n in _PLAN_NAMES)
    assert "<dt>Plan</dt>" not in body and "<dt>Billing</dt>" not in body
    assert "Plans &amp; subscription" not in body


def test_signed_out_production_request_gets_login_required_not_premium_required(app_factory):
    from backend.utils.csrf import _SESSION_KEY

    app = app_factory(production=True, BILLING_STRIPE_ENABLED=None).app
    client = app.test_client()
    with client.session_transaction() as sess:
        sess[_SESSION_KEY] = "t" * 32
    r = client.post("/api/car/1/chat", json={"message": "hi"}, headers={"X-CSRF-Token": "t" * 32})
    body = r.get_data(as_text=True)
    assert "premium_required" not in body
    assert r.status_code in (401, 403) and "login_required" in body


_LADDER = {
    "label": "Trim lineup", "make": "Ram", "model": "1500", "listing_trim": "Laramie", "quality": "high", "matched": True,
    "steps": [{"name": "Tradesman", "is_current": False, "is_passed": True},
              {"name": "Laramie", "is_current": True, "is_passed": False}],
}


def _car_page(app, monkeypatch, client):
    from backend.routes import cars_pages

    car = {"id": 7, "vin": "1C6RRFFG4NN401203", "year": 2022, "make": "Ram", "model": "1500", "trim": "Laramie",
           "price": 45000, "mileage": 12000, "inactive": 0, "gallery": [], "history_highlights": []}
    monkeypatch.setattr(cars_pages, "get_car_by_id", lambda *_a, **_k: dict(car))
    monkeypatch.setattr(cars_pages._cd_insights, "trim_ladder_for_viewer", lambda _car: dict(_LADDER))
    r = client.get("/car/7")
    assert r.status_code == 200
    return r.get_data(as_text=True)


def test_car_page_says_nothing_about_premium_with_billing_off(free_app, monkeypatch):
    body = _car_page(free_app, monkeypatch, _signed_in(free_app))
    assert "Premium only" not in body
    assert "premium-history-upsell" not in body
    assert not _UPSELL.search(body) and "Premium" not in body.replace("premium-history-decode", "")


def test_anonymous_trim_panel_is_not_empty_with_billing_off(free_app, monkeypatch):
    body = _car_page(free_app, monkeypatch, free_app.test_client())
    assert "What this trim adds" in body
    assert "to see what this trim adds" in body  # a sentence under the label, not an empty panel
    assert "Premium" not in body.replace("premium-history-decode", "")


def test_car_page_with_billing_on_still_offers_the_premium_dialog(app_factory, monkeypatch):
    app = app_factory(BILLING_STRIPE_ENABLED="1", ALLOW_DEFAULT_APP_USER="0").app
    body = _car_page(app, monkeypatch, _signed_in(app))
    assert "Premium only" in body or "premium-history-upsell" in body


def test_autonomous_dev_doc_does_not_curl_the_dead_plans_route():
    from pathlib import Path

    text = (Path(__file__).resolve().parents[2] / "deploy" / "AUTONOMOUS_DEV.md").read_text(encoding="utf-8")
    assert "/billing/plans" not in text.replace("# /billing/plans answers 404", "")


def test_register_page_hides_the_premium_choice_with_billing_off(free_app):
    body = free_app.test_client().get("/register").get_data(as_text=True)
    assert "plan-premium" not in body and "Premium" not in body and "Full access" not in body
    assert 'name="plan" value="free"' in body  # the form still says free, with no choice to make


def test_register_page_offers_the_choice_with_billing_on(app_factory):
    app = app_factory(BILLING_STRIPE_ENABLED="1").app
    body = app.test_client().get("/register").get_data(as_text=True)
    assert "plan-premium" in body and "plan-free" in body


def test_a_hand_made_premium_plan_is_ignored_with_billing_off(free_app):
    client = free_app.test_client()
    from backend.utils.csrf import _SESSION_KEY

    with client.session_transaction() as sess:
        sess[_SESSION_KEY] = "t" * 32
    r = client.post("/register", data={"username": "freeperson", "email": "freeperson@example.com",
                                      "password": "a-long-enough-password-1", "plan": "premium",
                                      "csrf_token": "t" * 32})
    assert r.status_code in (302, 303), r.get_data(as_text=True)[:300]
    assert not r.headers["Location"].rstrip("/").endswith("/premium")
    with client.session_transaction() as sess:
        assert "post_auth_intent" not in sess


def test_a_premium_login_intent_goes_to_the_app_home_with_billing_off(free_app):
    from backend.auth.session import post_login_redirect

    with free_app.test_request_context("/"):
        from flask import session

        session["post_auth_intent"] = "premium"
        r = post_login_redirect()
        assert "/premium" not in r.headers["Location"]
        assert "post_auth_intent" not in session


def test_a_new_google_or_apple_user_is_not_offered_premium_with_billing_off(free_app, monkeypatch):
    from backend.auth import apple_oauth, google_oauth

    for mod, fn in ((google_oauth, "_complete_login"), (apple_oauth, "_complete_login")):
        monkeypatch.setattr("backend.auth.session.finalize_app_session", lambda uid: True)
        monkeypatch.setattr(mod, "sync_env_admin_user_row", lambda uid: None, raising=False)
        with free_app.test_request_context("/"):
            from flask import session

            kwargs = {"is_new_user": True}
            r = getattr(mod, fn)(5, **kwargs)
            assert "/premium" not in r.headers["Location"], mod.__name__
            assert "auth_welcome_source" not in session
