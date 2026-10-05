"""Regression tests for the May-2026 UX audit residue (upgrade prompts, upsell, logout).

Three defects pinned here:

1. A signed-in user whose paid access comes from an active org subscription (or
   an admin role) must not be shown "View plans and upgrade" on /account/billing
   -- ``is_premium`` there is only the DB column, paid access is
   ``backend.billing.access`` (sees_paid_ui), read from the DB rows.
2. The premium upsell (home panel, dashboard quick action, sidebar link) must
   render for a free user while billing is ON, and none of it may render when
   ``BILLING_STRIPE_ENABLED`` is off -- with billing off every logged-in user
   effectively has premium and the checkout behind the button is dead.
3. The car detail page must keep a reachable logout control (the shared nav
   sidebar's logout form) for signed-in users.
"""

from __future__ import annotations

import pytest

from backend.routes import cars_pages as _cars_pages_routes

_FAKE_CAR = {
    "id": 7,
    "vin": "1C6RRFFG4NN401203",
    "year": 2022,
    "make": "Ram",
    "model": "1500",
    "trim": "Laramie",
    "price": 45000,
    "mileage": 12000,
    "inactive": 0,
    "gallery": [],
    "history_highlights": [],
}

_HOME_UPSELL = 'id="premium-upsell-section"'
_DASH_UPSELL = "dash-hub-action--accent"
_SIDEBAR_UPSELL = "app-sidebar__link--premium"


def _make_app(app_factory, tmp_path, **env):
    return app_factory(
        ALLOW_DEFAULT_APP_USER="0",
        RATE_LIMIT_SQLITE_PATH=str(tmp_path / "rate_limits.db"),
        **env,
    )


def _client_with_session(main, sess_values: dict):
    client = main.app.test_client()
    with client.session_transaction() as sess:
        for k, v in sess_values.items():
            sess[k] = v
    return client


_FREE_SESSION = {
    "user_id": 1,
    "username": "ux_probe",
    "user_role": "general_user",
    "user_is_premium": False,
}

_ORG_PREMIUM_SESSION = {
    "user_id": 1,
    "username": "ux_probe",
    "user_role": "general_user",
    "user_is_premium": False,
    "org_subscription_status": "active",
}


def _patch_org_premium_user(monkeypatch: pytest.MonkeyPatch) -> None:
    """Paid access is read from the DB users/org rows (backend.billing.access), not
    the cookie's org_subscription_status: give user 1 an org with an active sub."""
    from backend.billing.entitlements import invalidate_org_billing_cache

    invalidate_org_billing_cache()
    monkeypatch.setattr(
        "backend.db.users_db.get_user_profile",
        lambda *_a, **_k: {"id": 1, "username": "ux_probe", "role": "general_user",
                           "org_id": 5, "is_premium": False, "subscription_plan_id": None},
    )
    monkeypatch.setattr(
        "backend.db.users_db.get_org",
        lambda *_a, **_k: {"id": 5, "stripe_subscription_status": "active"},
    )


def _patch_billing_snapshot(monkeypatch: pytest.MonkeyPatch, snapshot: dict) -> None:
    monkeypatch.setattr(
        "backend.db.users_db.get_user_billing_snapshot",
        lambda *_a, **_k: dict(snapshot),
    )


def test_org_premium_user_sees_no_upgrade_prompt_on_billing_page(
    monkeypatch: pytest.MonkeyPatch, tmp_path, app_factory
) -> None:
    """Defect 1: org-subscription paid access, but the DB is_premium column is False."""
    main = _make_app(app_factory, tmp_path, BILLING_STRIPE_ENABLED="1")
    _patch_billing_snapshot(monkeypatch, {"is_premium": False})
    _patch_org_premium_user(monkeypatch)
    client = _client_with_session(main, _ORG_PREMIUM_SESSION)
    rv = client.get("/account/billing")
    assert rv.status_code == 200
    assert "View plans and upgrade" not in rv.get_data(as_text=True)


def test_truly_free_user_still_sees_upgrade_prompt_on_billing_page(
    monkeypatch: pytest.MonkeyPatch, tmp_path, app_factory
) -> None:
    """Guard the other direction: the prompt must not vanish for actual free users."""
    main = _make_app(app_factory, tmp_path, BILLING_STRIPE_ENABLED="1")
    _patch_billing_snapshot(monkeypatch, {"is_premium": False})
    client = _client_with_session(main, _FREE_SESSION)
    rv = client.get("/account/billing")
    assert rv.status_code == 200
    assert "View plans and upgrade" in rv.get_data(as_text=True)


def test_free_user_sees_upsell_everywhere_when_billing_on(
    monkeypatch: pytest.MonkeyPatch, tmp_path, app_factory
) -> None:
    """Defect 2 (billing on): home panel, dashboard action, and sidebar link all render."""
    main = _make_app(app_factory, tmp_path, BILLING_STRIPE_ENABLED="1")
    client = _client_with_session(main, _FREE_SESSION)

    home = client.get("/home")
    assert home.status_code == 200
    home_body = home.get_data(as_text=True)
    assert _HOME_UPSELL in home_body
    assert _SIDEBAR_UPSELL in home_body

    dash = client.get("/dashboard")
    assert dash.status_code == 200
    assert _DASH_UPSELL in dash.get_data(as_text=True)


def test_premium_user_sees_no_upsell_when_billing_on(
    monkeypatch: pytest.MonkeyPatch, tmp_path, app_factory
) -> None:
    main = _make_app(app_factory, tmp_path, BILLING_STRIPE_ENABLED="1")
    _patch_org_premium_user(monkeypatch)
    client = _client_with_session(main, _ORG_PREMIUM_SESSION)

    home = client.get("/home")
    assert home.status_code == 200
    home_body = home.get_data(as_text=True)
    assert _HOME_UPSELL not in home_body
    assert _SIDEBAR_UPSELL not in home_body

    dash = client.get("/dashboard")
    assert dash.status_code == 200
    assert _DASH_UPSELL not in dash.get_data(as_text=True)


def test_billing_off_hides_every_upsell_and_dead_upgrade_button(
    monkeypatch: pytest.MonkeyPatch, tmp_path, app_factory
) -> None:
    """Defect 2 (billing off): everyone logged-in effectively has premium, so no upsell
    and no dead "Upgrade to Premium" checkout button on /premium."""
    main = _make_app(app_factory, tmp_path, BILLING_STRIPE_ENABLED="0")
    client = _client_with_session(main, _FREE_SESSION)

    home = client.get("/home")
    assert home.status_code == 200
    home_body = home.get_data(as_text=True)
    assert _HOME_UPSELL not in home_body
    assert _SIDEBAR_UPSELL not in home_body

    dash = client.get("/dashboard")
    assert dash.status_code == 200
    assert _DASH_UPSELL not in dash.get_data(as_text=True)

    prem = client.get("/premium")
    assert prem.status_code == 200
    prem_body = prem.get_data(as_text=True)
    assert "Upgrade to Premium" not in prem_body
    # 2026-09-30: billing off no longer claims "You have Premium access" (the user
    # does not get the has_paid_access features); it says plans aren't on sale.
    assert "You have Premium access" not in prem_body
    assert "aren't on sale yet" in prem_body
    assert "Most popular" not in prem_body and "Billing coming soon" not in prem_body


def test_car_page_keeps_logout_control_for_signed_in_user(
    monkeypatch: pytest.MonkeyPatch, tmp_path, app_factory
) -> None:
    """Defect 3 regression pin: /car/<id> renders the shared nav's logout form."""
    main = _make_app(app_factory, tmp_path, BILLING_STRIPE_ENABLED="1")
    monkeypatch.setattr(_cars_pages_routes, "get_car_by_id", lambda *_a, **_k: dict(_FAKE_CAR))
    client = _client_with_session(main, _FREE_SESSION)
    rv = client.get("/car/7")
    assert rv.status_code == 200
    body = rv.get_data(as_text=True)
    assert 'action="/logout"' in body
    assert "dev-logout-form" in body
