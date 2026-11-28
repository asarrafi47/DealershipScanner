"""Profile update validation, inventory role check, narrate sign-in, cache headers, /dev role re-read."""

from __future__ import annotations

import pytest
from flask import Flask, make_response, session

CSRF = "profile-portal-test-csrf-token-0123456789"
ADMIN_EMAIL = "ops-admin@example.com"
ADMIN_USERNAME = "opsadmin"


def _login(client, uid: int, *, username: str = "someone", role: str | None = None) -> None:
    with client.session_transaction() as sess:
        sess["user_id"] = int(uid)
        sess["username"] = username
        sess["mfa_ok"] = True
        sess["_csrf_token"] = CSRF
        if role is not None:
            sess["user_role"] = role


def _role_of(uid: int) -> str:
    from backend.db.users_db import get_user_profile
    from backend.utils.roles import normalize_role

    return normalize_role((get_user_profile(uid) or {}).get("role"))


# ---------------------------------------------------------------------------
# Profile update validation
# ---------------------------------------------------------------------------


@pytest.fixture
def profile_app(app_factory):
    main = app_factory(
        APP_ADMIN_EMAILS=ADMIN_EMAIL,
        APP_ADMIN_USERNAMES=ADMIN_USERNAME,
        BILLING_STRIPE_ENABLED="0",
        ALLOW_DEFAULT_APP_USER="0",
    )
    from backend.db.users_db import init_users_db

    init_users_db()
    main.app.config["TESTING"] = True
    return main.app


def _post_profile(client, username: str, email: str):
    return client.post(
        "/account/profile",
        data={
            "form_action": "profile",
            "username": username,
            "email": email,
            "csrf_token": CSRF,
        },
    )


def test_profile_update_validation_reserved_email(profile_app):
    from backend.db.users_db import get_user_profile, save_user
    from backend.utils.roles import REGISTRATION_ADMIN_BLOCKED_MSG

    uid = int(save_user("shopper1", "shopper1@example.com", "longpassword123", role="general_user"))
    client = profile_app.test_client()
    _login(client, uid, username="shopper1")

    rv = _post_profile(client, "shopper1", ADMIN_EMAIL)
    assert rv.status_code == 200
    assert REGISTRATION_ADMIN_BLOCKED_MSG in rv.get_data(as_text=True)
    assert _role_of(uid) == "general_user"
    assert (get_user_profile(uid) or {}).get("email") == "shopper1@example.com"
    me = client.get("/api/auth/me").get_json()
    assert me["user"]["role"] != "admin"
    with client.session_transaction() as sess:
        assert sess.get("user_role") != "admin"


def test_profile_update_validation_reserved_username(profile_app):
    from backend.db.users_db import get_user_profile, save_user

    uid = int(save_user("shopper2", "shopper2@example.com", "longpassword123", role="general_user"))
    client = profile_app.test_client()
    _login(client, uid, username="shopper2")

    _post_profile(client, ADMIN_USERNAME.upper(), "shopper2@example.com")
    assert _role_of(uid) == "general_user"
    assert (get_user_profile(uid) or {}).get("username") == "shopper2"
    me = client.get("/api/auth/me").get_json()
    assert me["user"]["role"] != "admin"


def test_profile_update_validation_keeps_own_identity(profile_app):
    from backend.db.users_db import get_user_profile, save_user

    uid = int(save_user(ADMIN_USERNAME, ADMIN_EMAIL, "longpassword123", role="admin"))
    client = profile_app.test_client()
    _login(client, uid, username=ADMIN_USERNAME, role="admin")

    # Re-saving the existing username/email is allowed.
    rv = _post_profile(client, ADMIN_USERNAME, ADMIN_EMAIL)
    assert rv.status_code == 200
    from backend.utils.roles import REGISTRATION_ADMIN_BLOCKED_MSG

    assert REGISTRATION_ADMIN_BLOCKED_MSG not in rv.get_data(as_text=True)
    # Changing only the username keeps the reserved email the account already holds.
    _post_profile(client, "opsadmin2", ADMIN_EMAIL)
    prof = get_user_profile(uid) or {}
    assert prof.get("username") == "opsadmin2"
    assert prof.get("email") == ADMIN_EMAIL
    assert _role_of(uid) == "admin"


def test_update_user_profile_validation_reserved_identity(profile_app):
    from backend.db.users_db import save_user, update_user_profile
    from backend.utils.roles import REGISTRATION_ADMIN_BLOCKED_MSG

    uid = int(save_user("shopper3", "shopper3@example.com", "longpassword123", role="general_user"))
    assert update_user_profile(uid, "shopper3", ADMIN_EMAIL.upper()) == REGISTRATION_ADMIN_BLOCKED_MSG
    assert update_user_profile(uid, ADMIN_USERNAME, "shopper3@example.com") == REGISTRATION_ADMIN_BLOCKED_MSG
    assert update_user_profile(uid, "shopper3b", "shopper3b@example.com") is None


# ---------------------------------------------------------------------------
# Inventory role check
# ---------------------------------------------------------------------------


@pytest.fixture
def portal(app_factory, monkeypatch):
    main = app_factory(BILLING_STRIPE_ENABLED="0", ALLOW_DEFAULT_APP_USER="0")
    from backend.db.users_db import init_users_db

    init_users_db()
    main.app.config["TESTING"] = True

    import backend.dealer.portal_sync as portal_sync
    import backend.dealer.routes as dealer_routes

    prefill_calls: list[str] = []
    synced: list[int] = []

    def fake_prefill(vin, **_kw):
        prefill_calls.append(vin)
        return {"vin": vin, "year": 2022, "make": "Honda", "model": "Civic"}, None

    monkeypatch.setattr(dealer_routes, "build_vehicle_prefill_from_vin", fake_prefill)
    monkeypatch.setattr(
        portal_sync,
        "sync_portal_vehicle_to_public_listings",
        lambda uid, vehicle: synced.append(int(uid)) or "ok",
    )
    return {"app": main.app, "prefill": prefill_calls, "synced": synced}


VIN = "1HGBH41JXMN109186"


def test_inventory_role_check_general_user(portal):
    from backend.db import dealer_portal_db as ddb
    from backend.db.users_db import save_user

    uid = int(save_user("shopper4", "shopper4@example.com", "longpassword123", role="general_user"))
    client = portal["app"].test_client()
    _login(client, uid, username="shopper4")

    assert client.get("/inventory").status_code == 403
    assert client.get("/inventory/listings").status_code == 403
    rv = client.post("/inventory/add-vin", data={"vin": VIN, "csrf_token": CSRF})
    assert rv.status_code == 403
    assert portal["prefill"] == []
    assert portal["synced"] == []
    assert ddb.list_vehicles_for_user(uid) == []


def test_inventory_role_check_dealer_role(portal):
    from backend.db import dealer_portal_db as ddb
    from backend.db.users_db import save_user

    uid = int(
        save_user("dealer1", "dealer1@example.com", "longpassword123", role="dealership_owner")
    )
    client = portal["app"].test_client()
    _login(client, uid, username="dealer1")

    assert client.get("/inventory").status_code == 200
    assert client.get("/inventory/listings").status_code == 200
    rv = client.post("/inventory/add-vin", data={"vin": VIN, "csrf_token": CSRF})
    assert rv.status_code == 302
    assert portal["prefill"] == [VIN]
    assert portal["synced"] == [uid]
    assert len(ddb.list_vehicles_for_user(uid)) == 1


# ---------------------------------------------------------------------------
# Narrate endpoint requires sign-in
# ---------------------------------------------------------------------------


@pytest.fixture
def narrate(app_factory, monkeypatch):
    main = app_factory(BILLING_STRIPE_ENABLED="0", ALLOW_DEFAULT_APP_USER="0")
    from backend.db.users_db import init_users_db

    init_users_db()
    main.app.config["TESTING"] = True

    import backend.routes.ai_narrate_bp as bp_mod

    bp_mod._cache.clear()
    calls: list[int] = []

    def fake_get_car_by_id(car_id, *, include_inactive=False):
        return {"id": car_id, "year": 2022, "make": "BMW", "model": "M3"} if car_id == 1 else None

    def fake_narrate(row):
        calls.append(int(row["id"]))
        return "A tidy 2022 BMW M3."

    monkeypatch.setattr(bp_mod, "get_car_by_id", fake_get_car_by_id)
    monkeypatch.setattr(bp_mod, "narrate_vehicle", fake_narrate)
    yield {"app": main.app, "calls": calls}
    bp_mod._cache.clear()


def test_narrate_requires_sign_in(narrate):
    client = narrate["app"].test_client()
    rv = client.get("/api/car/1/narrate")
    assert rv.status_code == 401
    assert rv.get_json()["ok"] is False
    assert narrate["calls"] == []


def test_narrate_requires_sign_in_existing_account(narrate):
    client = narrate["app"].test_client()
    _login(client, 987654)
    rv = client.get("/api/car/1/narrate", headers={"X-CSRF-Token": CSRF})
    assert rv.status_code == 401
    assert narrate["calls"] == []


def test_narrate_requires_csrf_header(narrate):
    from backend.db.users_db import save_user

    uid = int(save_user("reader1", "reader1@example.com", "longpassword123", role="general_user"))
    client = narrate["app"].test_client()
    _login(client, uid, username="reader1")
    rv = client.get("/api/car/1/narrate")
    assert rv.status_code == 403
    assert narrate["calls"] == []


def test_narrate_signed_in_with_header_is_served(narrate):
    from backend.db.users_db import save_user

    uid = int(save_user("reader2", "reader2@example.com", "longpassword123", role="general_user"))
    client = narrate["app"].test_client()
    _login(client, uid, username="reader2")
    rv = client.get("/api/car/1/narrate", headers={"X-CSRF-Token": CSRF})
    assert rv.status_code == 200
    assert rv.get_json()["description"] == "A tidy 2022 BMW M3."
    assert narrate["calls"] == [1]


# ---------------------------------------------------------------------------
# Cache headers on responses that set cookies
# ---------------------------------------------------------------------------


@pytest.fixture
def scoped_listings(sqlite_inventory, monkeypatch):
    from backend.db.repositories import grid_cards_repo as gc
    from backend.db.repositories import listings_repo as lr
    from backend.routes import listings_api
    from backend.tests.test_listings_scoped_grid import _seed

    lr.clear_inventory_listings_cache()
    gc.reset_grid_cards_state()
    listings_api.clear_cars_scope_cache()
    _seed(sqlite_inventory)
    monkeypatch.setattr(listings_api, "hidden_dealer_ids_for_user", lambda _uid: set())
    yield sqlite_inventory
    lr.clear_inventory_listings_cache()
    listings_api.clear_cars_scope_cache()


@pytest.fixture
def listings_client():
    from backend.main import app

    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c


def test_listings_cars_signed_in_is_private(scoped_listings, listings_client):
    with listings_client.session_transaction() as sess:
        sess.permanent = True  # as after a real sign-in
        sess["user_id"] = 4242
    rv = listings_client.get("/api/listings/cars?zip=92694&radius=50")
    assert rv.status_code == 200
    cc = rv.headers["Cache-Control"]
    assert cc.startswith("private")
    assert "public" not in cc and "s-maxage" not in cc


def test_listings_cars_anonymous_stays_public(scoped_listings, listings_client):
    rv = listings_client.get("/api/listings/cars?zip=92694&radius=50")
    assert rv.status_code == 200
    assert "Set-Cookie" not in rv.headers
    assert rv.headers["Cache-Control"] == "public, max-age=0, s-maxage=60, stale-while-revalidate=30"


def test_main_app_uses_cookie_safe_session_interface():
    from backend.main import app
    from backend.web.session_cache_guard import CookieSafeSessionInterface

    assert isinstance(app.session_interface, CookieSafeSessionInterface)


def _guard_app() -> Flask:
    from backend.web.session_cache_guard import CookieSafeSessionInterface

    app = Flask(__name__)
    app.secret_key = "session-cache-guard-test"
    app.session_interface = CookieSafeSessionInterface()

    @app.get("/writes-session")
    def writes_session():
        session["seen"] = True
        resp = make_response("ok")
        resp.headers["Cache-Control"] = "public, max-age=0, s-maxage=60"
        return resp

    @app.get("/no-session")
    def no_session():
        resp = make_response("ok")
        resp.headers["Cache-Control"] = "public, max-age=0, s-maxage=60"
        return resp

    return app


def test_cache_headers_on_response_that_sets_cookie():
    client = _guard_app().test_client()
    rv = client.get("/writes-session")
    assert "Set-Cookie" in rv.headers
    cc = rv.headers["Cache-Control"]
    assert cc.startswith("private")
    assert "public" not in cc and "s-maxage" not in cc
    assert "Cookie" in rv.headers.get("Vary", "")


@pytest.mark.parametrize(
    "given, expected",
    [
        ("public, max-age=0, s-maxage=60", "private, max-age=0"),
        ("public, max-age=31536000, immutable", "private, max-age=31536000, immutable"),
        ("public, s-maxage=60", "private, no-cache"),
        ("s-maxage=60, proxy-revalidate", "private, no-cache"),
        ("public, private, max-age=60", "private, max-age=60"),
    ],
)
def test_private_cache_control_drops_shared_directives(given, expected):
    from backend.web.session_cache_guard import private_cache_control

    assert private_cache_control(given) == expected


def test_static_asset_for_signed_in_user_keeps_browser_cache(app_factory):
    """A signed-in user's static asset is private but still cached by the browser."""
    client = app_factory().app.test_client()
    with client.session_transaction() as sess:
        sess["user_id"] = 1
        sess.permanent = True
    rv = client.get("/static/ai_chatbot.css?v=1")
    assert rv.status_code == 200
    assert "Set-Cookie" in rv.headers
    cc = rv.headers.get("Cache-Control", "")
    assert "public" not in cc
    assert cc.startswith("private")
    assert "max-age=31536000" in cc


def test_public_response_without_cookie_is_unchanged():
    client = _guard_app().test_client()
    rv = client.get("/no-session")
    assert "Set-Cookie" not in rv.headers
    assert rv.headers["Cache-Control"] == "public, max-age=0, s-maxage=60"


# ---------------------------------------------------------------------------
# /dev role re-read
# ---------------------------------------------------------------------------


@pytest.fixture
def dev_app(app_factory):
    main = app_factory(
        production=True,
        ALLOW_DEFAULT_APP_USER="0",
        ALLOW_APP_ADMIN_DEV_PASS_THROUGH="1",
    )
    from backend.db.users_db import init_users_db

    init_users_db()
    return main.app


def _set_active(uid: int, active: bool) -> None:
    from backend.db.users_db._common import get_conn

    conn = get_conn()
    try:
        conn.execute("UPDATE users SET is_active = ? WHERE id = ?", (1 if active else 0, uid))
        conn.commit()
    finally:
        conn.close()


def test_dev_role_reread_general_user(dev_app):
    from backend.db.users_db import save_user

    uid = int(save_user("plain1", "plain1@example.com", "longpassword123", role="general_user"))
    client = dev_app.test_client()
    _login(client, uid, username="plain1", role="admin")
    assert client.get("/dev/api/status").status_code == 401


def test_dev_role_reread_missing_account(dev_app):
    client = dev_app.test_client()
    _login(client, 987654, username="ghost", role="admin")
    assert client.get("/dev/api/status").status_code == 401


def test_dev_role_reread_inactive_admin(dev_app):
    from backend.db.users_db import save_user

    uid = int(save_user("adm2", "adm2@example.com", "longpassword123", role="admin"))
    _set_active(uid, False)
    client = dev_app.test_client()
    _login(client, uid, username="adm2", role="admin")
    assert client.get("/dev/api/status").status_code == 401


def test_dev_role_reread_active_admin(dev_app):
    from backend.db.users_db import save_user

    uid = int(save_user("adm1", "adm1@example.com", "longpassword123", role="admin"))
    client = dev_app.test_client()
    _login(client, uid, username="adm1", role="admin")
    assert client.get("/dev/api/status").status_code == 200

