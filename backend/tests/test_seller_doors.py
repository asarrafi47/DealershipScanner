"""S19: close today's open seller doors (docs/auth_sms/TASKS.md).

Before this card any account could register as a dealership with no check, post cars that went
straight to the public listings, take a VIN a scanned dealer already listed without being told, and
a suspended seller's cars stayed on the site. Each test pins one door. No network (a fake Turnstile
verifier, a stubbed VIN decode), no real person, no database address.
"""
from __future__ import annotations

from typing import Any

import pytest

from backend.auth.phone import turnstile
from backend.dealer import portal_sync
from backend.dealer import routes as dealer_routes
from backend.utils.csrf import _SESSION_KEY as CSRF_KEY

CSRF = "csrf-test-token"
VIN_A = "1N4BL4DV0PN300001"  # inventory gate
VIN_HELD = "1N4BL4DV0PN300011"  # a new account's car, kept private
VIN_GF = "1N4BL4DV0PN300012"  # a grandfathered account's car
VIN_COLLIDE = "1N4BL4DV0PN300013"  # a VIN a scanned dealer already lists
VIN_KEEP = "1N4BL4DV0PN300014"  # a scanned row that suspend must not touch
STORE = "real-dealer-com"
POSTING_HELD = "Posting cars publicly opens soon. Your car is saved."
COLLISION = "This VIN is already listed on the site; it was saved but not published."
NOT_DEALERSHIP = "Inventory is for dealership accounts for now."
LOGIN_FAILED = "Invalid username or password."


# --------------------------------------------------------------------------- helpers


@pytest.fixture()
def app(app_factory, monkeypatch):
    monkeypatch.delenv("MARKETPLACE_POSTING_ENABLED", raising=False)
    return app_factory().app


def _user(name: str, role: str = "dealership_owner", org: bool = True) -> int:
    from backend.db.users_db import create_org, save_user

    org_id = create_org(f"{name} motors") if org else None
    return int(save_user(name, f"{name}@example.com", "a-long-test-password-1", role=role, org_id=org_id))


def _login(client, uid: int) -> None:
    with client.session_transaction() as sess:
        sess["user_id"] = uid
        sess[CSRF_KEY] = CSRF


def _post(client, url: str, data: dict | None = None, **kw):
    return client.post(url, data={"csrf_token": CSRF, **(data or {})}, follow_redirects=False, **kw)


def _car(vin: str) -> dict[str, Any] | None:
    from backend.db.repositories.cars_repo import get_car_by_vin

    row = get_car_by_vin(vin)
    return dict(row) if row else None


def _scanned_car(vin: str = VIN_A) -> None:
    from backend.scanner.database import upsert_vehicles

    assert upsert_vehicles([{"vin": vin, "title": "2023 Nissan Altima", "year": 2023, "make": "Nissan",
                             "model": "Altima", "price": 30000, "dealer_id": STORE, "dealer_name": "Real Dealer",
                             "dealer_url": "https://real-dealer.example"}]) == 1


def _stub_vin_decode(monkeypatch) -> None:
    monkeypatch.setattr(
        dealer_routes, "build_vehicle_prefill_from_vin",
        lambda vin: ({"vin": vin, "title": "2023 Nissan Altima", "year": 2023, "make": "Nissan",
                      "model": "Altima", "price": 29000}, None),
    )


def _grandfather(monkeypatch, *, yes: bool) -> None:
    """Every vehicle row counts as older than the cut-off (``yes``) or as newer than it."""
    monkeypatch.setattr(portal_sync, "GRANDFATHER_CUTOFF", "2999-01-01T00:00:00+00:00" if yes else "2000-01-01T00:00:00+00:00")


def _listings_page(client) -> str:
    r = client.get("/inventory/listings")
    assert r.status_code == 200
    return r.get_data(as_text=True)


# --------------------------------------------------------------------------- the inventory gate


def test_inventory_refuses_a_general_user(app):
    uid = _user("shopper", role="general_user", org=False)
    with app.test_client() as c:
        _login(c, uid)
        for url in ("/inventory", "/inventory/listings"):
            r = c.get(url)
            assert r.status_code == 403 and NOT_DEALERSHIP in r.get_data(as_text=True)
        r = _post(c, "/inventory/add-vin", {"vin": VIN_A})
        assert r.status_code == 403 and NOT_DEALERSHIP in r.get_data(as_text=True)
        r = _post(c, "/inventory/1/delete")
        assert r.status_code == 403
        assert c.get("/dealer-uploads/%d/1/x.jpg" % uid).status_code == 403
    # not signed in: still sent to the sign-in page, not told about roles
    with app.test_client() as c:
        r = c.get("/inventory")
        assert r.status_code in (302, 303) and "/dealer/login" in (r.headers.get("Location") or "")


def test_dealership_and_admin_accounts_still_reach_the_inventory(app):
    for name, role in (("owner1", "dealership_owner"), ("member1", "dealership_member"), ("admin1", "admin")):
        uid = _user(name, role=role)
        with app.test_client() as c:
            _login(c, uid)
            assert c.get("/inventory/listings").status_code == 200, role


# --------------------------------------------------------------------------- posting stays private


def test_posting_flag_off_keeps_new_accounts_private(app, monkeypatch):
    _stub_vin_decode(monkeypatch)
    _grandfather(monkeypatch, yes=False)
    uid = _user("newseller")
    with app.test_client() as c:
        _login(c, uid)
        assert _post(c, "/inventory/add-vin", {"vin": VIN_HELD}).status_code in (302, 303)
        page = _listings_page(c)
    assert POSTING_HELD in page
    assert _car(VIN_HELD) is None  # saved in the seller's own inventory, never copied to the public listings
    from backend.db import dealer_portal_db as ddb

    assert [v["vin"] for v in ddb.list_vehicles_for_user(uid)] == [VIN_HELD]
    # the owner's switch (environment only) opens posting for everyone
    monkeypatch.setenv("MARKETPLACE_POSTING_ENABLED", "1")
    assert portal_sync.sync_portal_vehicle_to_public_listings(uid, ddb.list_vehicles_for_user(uid)[0]) == "published"
    assert _car(VIN_HELD)["dealer_id"] == portal_sync.dealer_portal_dealer_id(uid)


def test_grandfathered_account_still_syncs(app, monkeypatch):
    _stub_vin_decode(monkeypatch)
    _grandfather(monkeypatch, yes=True)
    uid = _user("oldseller")
    with app.test_client() as c:
        _login(c, uid)
        assert _post(c, "/inventory/add-vin", {"vin": VIN_GF}).status_code in (302, 303)
        assert POSTING_HELD not in _listings_page(c)
    assert _car(VIN_GF)["dealer_id"] == portal_sync.dealer_portal_dealer_id(uid)


def test_vin_collision_is_told_to_the_seller(app, monkeypatch):
    _stub_vin_decode(monkeypatch)
    _grandfather(monkeypatch, yes=True)
    _scanned_car(VIN_COLLIDE)
    uid = _user("collider")
    with app.test_client() as c:
        _login(c, uid)
        assert _post(c, "/inventory/add-vin", {"vin": VIN_COLLIDE}).status_code in (302, 303)
        assert COLLISION in _listings_page(c)
    assert _car(VIN_COLLIDE)["dealer_id"] == STORE  # the scanned dealer's row was not touched


# --------------------------------------------------------------------------- dealer sign-up


def _register_data(n: int, **extra) -> dict:
    return {"username": f"seller{n}", "email": f"seller{n}@example.com", "password": "a-long-test-password-1",
            "org_name": f"Seller {n} Motors", **extra}


def _signup(client, n: int, token: str | None = "pass", ip: str = "198.51.100.10"):
    extra = {"cf-turnstile-response": token} if token is not None else {}
    with client.session_transaction() as sess:
        sess[CSRF_KEY] = CSRF
    return _post(client, "/dealer/register", _register_data(n, **extra), environ_overrides={"REMOTE_ADDR": ip})


def test_dealer_register_needs_turnstile(app, fake_turnstile):
    from backend.db.users_db import find_users_by_email

    with app.test_client() as c:
        r = _signup(c, 1, token=None)
        assert r.status_code == 400 and "complete the check" in r.get_data(as_text=True)
        r = _signup(c, 1, token="wrong")
        assert r.status_code == 400
        assert find_users_by_email("seller1@example.com") == []  # nothing was created
        r = _signup(c, 1, token="pass")
        assert r.status_code in (302, 303)
        assert find_users_by_email("seller1@example.com")
        r = _signup(c, 2, token="pass")  # the same token cannot be used twice
        assert r.status_code == 400
    assert fake_turnstile.calls  # the check really ran


def test_dealer_register_closed_without_keys_in_production(app_factory, monkeypatch):
    for key in ("TURNSTILE_SITE_KEY", "TURNSTILE_SECRET_KEY"):
        monkeypatch.delenv(key, raising=False)
    turnstile.set_verifier(None)
    app = app_factory(production=True).app
    from backend.db.users_db import find_users_by_email

    with app.test_client() as c:
        page = c.get("/dealer/register")
        text = page.get_data(as_text=True)
        assert page.status_code == 200 and "Dealer sign-up is not available right now." in text
        assert "<form" not in text.split("</nav>")[-1] or 'name="org_name"' not in text  # no sign-up form
        for token in (None, "anything"):
            r = _signup(c, 3, token=token)
            assert r.status_code == 503 and "Dealer sign-up is not available right now." in r.get_data(as_text=True)
    assert find_users_by_email("seller3@example.com") == []


def test_signup_paused_refuses_with_sentence(app, fake_turnstile):
    from backend import settings_store
    from backend.db.users_db import find_users_by_email

    settings_store.reset_cache()
    settings_store.set_loader(lambda: {"seller.signup_paused": "1"})
    try:
        with app.test_client() as c:
            r = _signup(c, 4)
            assert r.status_code == 503 and "Dealer sign-up is paused" in r.get_data(as_text=True)
        assert find_users_by_email("seller4@example.com") == []
        assert not fake_turnstile.calls  # paused before the check is even asked
    finally:
        settings_store.set_loader(None)
        settings_store.reset_cache()


def test_signups_are_limited_to_three_an_hour_per_address(app, fake_turnstile):
    turnstile.set_verifier(turnstile.FakeTurnstileVerifier({"t1", "t2", "t3", "t4", "t5"}))
    ip = "198.51.100.55"
    with app.test_client() as c:
        for n in (10, 11, 12):
            assert _signup(c, n, token=f"t{n - 9}", ip=ip).status_code in (302, 303), n
        r = _signup(c, 13, token="t4", ip=ip)
        assert r.status_code == 429 and "Too many sign-ups" in r.get_data(as_text=True)
        # another address is not affected
        assert _signup(c, 14, token="t5", ip="198.51.100.56").status_code in (302, 303)


def test_turnstile_host_is_allowed_only_on_the_signup_page(app_factory, monkeypatch):
    monkeypatch.setenv("TURNSTILE_SITE_KEY", "1x00000000000000000000AA")
    monkeypatch.setenv("TURNSTILE_SECRET_KEY", "test-secret-not-real")
    app = app_factory(production=True).app
    host = "https://challenges.cloudflare.com"
    with app.test_client() as c:
        csp = c.get("/dealer/register").headers.get("Content-Security-Policy", "")
        directives = {d.split(" ", 1)[0]: d for d in csp.split("; ") if d}
        for name in ("script-src", "frame-src", "connect-src"):
            assert host in directives[name], name
        assert host in c.get("/dealer/register").get_data(as_text=True)  # the widget script tag
        other = c.get("/login").headers.get("Content-Security-Policy", "")
        assert other and host not in other


# --------------------------------------------------------------------------- suspend


def _portal_cars(uid: int, vins: tuple[str, ...]) -> None:
    from backend.scanner.database import upsert_vehicles

    pid = portal_sync.dealer_portal_dealer_id(uid)
    rows = [{"vin": v, "title": "2022 Honda Civic", "year": 2022, "make": "Honda", "model": "Civic",
             "price": 20000, "dealer_id": pid, "dealer_name": "Seller"} for v in vins]
    assert upsert_vehicles(rows) == len(vins)


def test_suspend_retires_every_portal_row(app):
    from backend.db.users_db import set_user_is_active

    vins = ("2HGFC2F69NH500001", "2HGFC2F69NH500002")
    uid = _user("suspendme")
    _portal_cars(uid, vins)
    assert all(_car(v)["listing_active"] == 1 for v in vins)
    assert set_user_is_active(uid, active=False)
    for vin in vins:
        row = _car(vin)
        assert row["listing_active"] == 0 and row["listing_removed_at"]
    # un-suspending does not put them back: the seller publishes again
    assert set_user_is_active(uid, active=True)
    assert all(_car(v)["listing_active"] == 0 for v in vins)


def test_scanner_rows_unchanged_by_suspend(app):
    from backend.db.users_db import set_user_is_active

    uid = _user("suspendme2")
    other = _user("keepme")
    _scanned_car(VIN_KEEP)
    _portal_cars(uid, ("2HGFC2F69NH500011",))
    _portal_cars(other, ("2HGFC2F69NH500012",))  # another seller's row too
    before = _car(VIN_KEEP)
    assert set_user_is_active(uid, active=False)
    after = _car(VIN_KEEP)
    assert (after["listing_active"], after["listing_removed_at"], after["dealer_id"]) == (
        before["listing_active"], before["listing_removed_at"], STORE)
    assert _car("2HGFC2F69NH500011")["listing_active"] == 0  # the suspended seller's own row did retire
    assert _car("2HGFC2F69NH500012")["listing_active"] == 1  # someone else's portal rows are not touched


# --------------------------------------------------------------------------- login and CSRF


def test_dealer_login_failures_all_say_the_same_thing(app):
    from backend.db.users_db import set_user_is_active

    uid = _user("realdealer")
    suspended = _user("suspendedone")
    assert set_user_is_active(suspended, active=False)
    cases = [("realdealer", "wrong-password-xyz-1"), ("nosuchperson", "a-long-test-password-1"),
             ("suspendedone", "a-long-test-password-1")]
    with app.test_client() as c:
        for n, (login, password) in enumerate(cases):
            with c.session_transaction() as sess:
                sess[CSRF_KEY] = CSRF
            r = _post(c, "/dealer/login", {"login": login, "password": password},
                      environ_overrides={"REMOTE_ADDR": f"198.51.100.{100 + n}"})
            text = r.get_data(as_text=True)
            assert r.status_code == 200 and LOGIN_FAILED in text, login
            assert "suspend" not in text.lower() and "Login failed" not in text, login
    assert uid


def test_every_dealer_post_is_csrf_protected(app):
    uid = _user("csrfseller")
    rules = [r for r in app.url_map.iter_rules()
             if r.endpoint.startswith("dealer_portal.") and "POST" in (r.methods or ())]
    assert len(rules) >= 7, [r.endpoint for r in rules]
    with app.test_client() as c:
        for rule in rules:
            values = {a: (1 if "int" in type(rule._converters[a]).__name__.lower() else "x") for a in rule.arguments}
            _, path = rule.build(values)
            with c.session_transaction() as sess:
                sess["user_id"] = uid
                sess[CSRF_KEY] = CSRF
            for data in ({"vin": VIN_A, "login": "a", "password": "b"}, {"csrf_token": "not-the-token"}):
                r = c.post(path, data=data, follow_redirects=False)
                # A refusal is a 400/403, or the login form's own redirect back with session_expired.
                refused = r.status_code in (400, 403) or (
                    r.status_code in (302, 303) and "session_expired" in (r.headers.get("Location") or ""))
                assert refused, (rule.endpoint, r.status_code, r.headers.get("Location"))
