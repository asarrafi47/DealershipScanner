"""Card S19: close today's open seller doors (docs/auth_sms/TASKS.md, DESIGN.md section 23).

Every acceptance bullet of the card is one named test here. Turnstile is always the fake verifier
(``FakeTurnstileVerifier``) or a stub: no test touches the network. The ``pg`` tests at the end run
the grandfather rule and the owner's read-only script on a throwaway Postgres.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

_PW = "seller-doors-password-1"
_CSRF = "c" * 43
_VIN_A = "1HGCM82633A004352"
_VIN_B = "1M8GDM9AXKP042788"
_VIN_C = "5YJSA1DN2CFP01785"
_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "list_grandfathered_sellers.py"


# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def fake_turnstile(monkeypatch):
    from backend.auth.phone import turnstile

    fake = turnstile.FakeTurnstileVerifier({"pass", "pass-2", "pass-3", "pass-4", "pass-5"})
    monkeypatch.setattr(turnstile, "_default", fake)
    return fake


@pytest.fixture
def portal_db(tmp_path, monkeypatch):
    """dealer_vehicles in this test's own SQLite file (the module reads its path at import)."""
    from backend.db import dealer_portal_db as ddb

    monkeypatch.setattr(ddb, "DB_PATH", str(tmp_path / "dealer_portal_s19.db"))
    ddb.init_dealer_portal_db()
    return ddb


@pytest.fixture
def settings_rows(monkeypatch):
    """The ``app_settings`` rows the settings store sees (no database read)."""
    from backend import settings_store

    rows: dict[str, str] = {}
    settings_store.set_loader(lambda: dict(rows))
    settings_store.reset_cache()
    yield rows
    settings_store.set_loader(None)
    settings_store.reset_cache()


@pytest.fixture
def site(app_factory, sqlite_inventory, fake_turnstile, settings_rows, portal_db, monkeypatch):
    """A development app, public posting off, the fake Turnstile, an empty settings table."""
    monkeypatch.delenv("SELLER_SIGNUP_PAUSED", raising=False)
    main = app_factory(MARKETPLACE_POSTING_ENABLED="", TURNSTILE_SITE_KEY="", TURNSTILE_SECRET_KEY="")
    return main.app


def _user(username: str, role: str) -> int:
    from backend.db.users_db import save_user

    return int(save_user(username, f"{username}@example.com", _PW, role=role))


def _sign_in(client, uid: int) -> None:
    from backend.utils.csrf import _SESSION_KEY

    with client.session_transaction() as sess:
        sess["user_id"] = uid
        sess["mfa_ok"] = True
        sess[_SESSION_KEY] = _CSRF


def _post(client, url: str, data: dict | None = None, **kw):
    from backend.utils.csrf import _SESSION_KEY

    with client.session_transaction() as sess:
        sess[_SESSION_KEY] = _CSRF
    return client.post(url, data={"csrf_token": _CSRF, **(data or {})}, **kw)


def _old_vehicle(uid: int, vin: str) -> int:
    """A dealer_vehicles row saved before the cut-off (what a grandfathered account has)."""
    from backend.db import dealer_portal_db as ddb
    from backend.dealer.portal_sync import GRANDFATHER_CUTOFF

    vid = ddb.insert_vehicle(uid, {"vin": vin, "year": 2003, "make": "Honda", "model": "Accord"})
    before = (GRANDFATHER_CUTOFF - timedelta(days=30)).isoformat()
    conn = ddb.get_conn()
    try:
        conn.execute("UPDATE dealer_vehicles SET created_at = ? WHERE id = ?", (before, vid))
        conn.commit()
    finally:
        conn.close()
    return vid


def _car(vin: str):
    from backend.db.repositories.cars_repo import get_car_by_vin

    return get_car_by_vin(vin)


def _cars_rows(sqlite_inventory) -> dict[str, tuple]:
    conn = sqlite_inventory.get_conn()
    try:
        rows = conn.execute("SELECT vin, dealer_id, listing_active FROM cars").fetchall()
    finally:
        conn.close()
    return {r[0]: (r[1], r[2]) for r in rows}


def _register_form(token: str | None = "pass", n: int = 1) -> dict:
    data = {
        "username": f"newdealer{n}",
        "email": f"newdealer{n}@example.com",
        "password": _PW,
        "org_name": f"Test Motors {n}",
    }
    if token is not None:
        data["cf-turnstile-response"] = token
    return data


def _user_exists(username: str) -> bool:
    from backend.db.users_db import user_exists_by_username

    return bool(user_exists_by_username(username))


# ---------------------------------------------------------------------------
# (2) /inventory: dealership_* or admin only
# ---------------------------------------------------------------------------


def test_inventory_refuses_a_general_user(site):
    from backend.db import dealer_portal_db as ddb
    from backend.dealer.routes import INVENTORY_ROLE_MSG

    shopper = _user("shopper1", "general_user")
    vid = ddb.insert_vehicle(shopper, {"vin": _VIN_A})  # a car saved before this card (allowed then)
    with site.test_client() as c:
        _sign_in(c, shopper)
        for method, url in (
            ("get", "/inventory"),
            ("get", "/inventory/listings"),
            ("post", "/inventory/add-vin"),
            ("post", f"/inventory/{vid}/update"),
            ("post", f"/inventory/{vid}/upload"),
            ("post", f"/inventory/{vid}/photo/delete"),
            ("post", f"/inventory/{vid}/delete"),
            ("get", f"/dealer-uploads/{shopper}/{vid}/x.jpg"),
        ):
            rv = c.get(url) if method == "get" else _post(c, url, {"vin": _VIN_B, "price": "1"})
            assert rv.status_code == 403, url
            assert INVENTORY_ROLE_MSG in rv.get_data(as_text=True), url
    assert ddb.count_user_vehicles_with_vin(shopper, _VIN_B) == 0
    assert ddb.get_vehicle(shopper, vid) is not None  # nothing was deleted either


def test_inventory_refuses_a_legacy_role_without_a_dealership(site):
    from backend.db.users_db.accounts import get_conn

    uid = _user("legacy1", "general_user")
    conn = get_conn()
    conn.execute("UPDATE users SET role = NULL WHERE id = ?", (uid,))
    conn.commit()
    conn.close()
    with site.test_client() as c:
        _sign_in(c, uid)
        assert c.get("/inventory").status_code == 403


@pytest.mark.parametrize("role", ["dealership_owner", "dealership_admin", "dealership_member", "admin"])
def test_inventory_lets_dealers_and_admins_in(site, role):
    uid = _user(f"ok_{role}", role)
    with site.test_client() as c:
        _sign_in(c, uid)
        assert c.get("/inventory").status_code == 200
        assert c.get("/inventory/listings").status_code == 200


def test_inventory_signed_out_still_goes_to_sign_in(site):
    with site.test_client() as c:
        rv = c.get("/inventory")
    assert rv.status_code in (301, 302, 303)
    assert "/dealer/login" in rv.headers["Location"]


# ---------------------------------------------------------------------------
# (1) MARKETPLACE_POSTING_ENABLED and the grandfather rule
# ---------------------------------------------------------------------------


def test_posting_flag_off_keeps_new_accounts_private(site, sqlite_inventory):
    from backend.db import dealer_portal_db as ddb
    from backend.dealer.routes import POSTING_SOON_MSG

    dealer = _user("newlot1", "dealership_owner")
    with site.test_client() as c:
        _sign_in(c, dealer)
        rv = _post(c, "/inventory/add-vin", {"vin": _VIN_A}, follow_redirects=True)
        page = rv.get_data(as_text=True)
    assert ddb.count_user_vehicles_with_vin(dealer, _VIN_A) == 1  # saved for the seller
    assert _car(_VIN_A) is None  # not public
    assert POSTING_SOON_MSG in page


def test_posting_flag_off_takes_a_new_accounts_public_car_down_on_save(site, sqlite_inventory):
    """A car an account made after the cut-off already put on the site leaves it on the next save."""
    from backend.db import dealer_portal_db as ddb

    dealer = _user("newlot2", "dealership_owner")
    vid = ddb.insert_vehicle(dealer, {"vin": _VIN_A, "make": "Honda"})
    sqlite_inventory.add_cars([{"vin": _VIN_A, "dealer_id": f"dealer-portal-{dealer}", "listing_active": 1}])
    with site.test_client() as c:
        _sign_in(c, dealer)
        _post(c, f"/inventory/{vid}/update", {"price": "9000"})
    assert _cars_rows(sqlite_inventory)[_VIN_A] == (f"dealer-portal-{dealer}", 0)


def test_posting_flag_on_publishes_a_new_account(
    app_factory, sqlite_inventory, fake_turnstile, settings_rows, portal_db
):
    app = app_factory(MARKETPLACE_POSTING_ENABLED="1").app
    dealer = _user("newlot3", "dealership_owner")
    with app.test_client() as c:
        _sign_in(c, dealer)
        rv = _post(c, "/inventory/listings", {})  # GET-only route: 405, proves nothing posted
        assert rv.status_code == 405
        _post(c, "/inventory/add-vin", {"vin": _VIN_A})
        page = c.get("/inventory/listings").get_data(as_text=True)
    car = _car(_VIN_A)
    assert car is not None and car["dealer_id"] == f"dealer-portal-{dealer}"
    from backend.dealer.routes import POSTING_SOON_MSG

    assert POSTING_SOON_MSG not in page


def test_grandfathered_account_still_syncs(site, sqlite_inventory):
    from backend.dealer.portal_sync import is_grandfathered_account
    from backend.dealer.routes import POSTING_SOON_MSG

    dealer = _user("oldlot1", "dealership_owner")
    _old_vehicle(dealer, _VIN_C)
    assert is_grandfathered_account(dealer)
    with site.test_client() as c:
        _sign_in(c, dealer)
        _post(c, "/inventory/add-vin", {"vin": _VIN_A})
        page = c.get("/inventory/listings").get_data(as_text=True)
    car = _car(_VIN_A)
    assert car is not None and car["dealer_id"] == f"dealer-portal-{dealer}"
    assert int(car["listing_active"]) == 1
    assert POSTING_SOON_MSG not in page


def test_rows_at_or_after_the_cutoff_do_not_grandfather(site):
    from backend.db import dealer_portal_db as ddb
    from backend.dealer.portal_sync import GRANDFATHER_CUTOFF, is_grandfathered_account

    dealer = _user("edge1", "dealership_owner")
    vid = ddb.insert_vehicle(dealer, {"vin": _VIN_A})
    conn = ddb.get_conn()
    conn.execute("UPDATE dealer_vehicles SET created_at = ? WHERE id = ?", (GRANDFATHER_CUTOFF.isoformat(), vid))
    conn.commit()
    conn.close()
    assert not is_grandfathered_account(dealer)
    assert not is_grandfathered_account(0)
    assert GRANDFATHER_CUTOFF.tzinfo is not None


# ---------------------------------------------------------------------------
# (3) Turnstile, the production key check, the pause switch, 3 sign-ups an hour
# ---------------------------------------------------------------------------


def test_dealer_register_needs_turnstile(site, fake_turnstile):
    from backend.dealer.routes import SIGNUP_TURNSTILE_MSG

    with site.test_client() as c:
        for token in (None, "", "wrong-token"):
            rv = _post(c, "/dealer/register", _register_form(token, 1))
            assert rv.status_code == 400
            assert SIGNUP_TURNSTILE_MSG in rv.get_data(as_text=True)
            assert not _user_exists("newdealer1")
        rv = _post(c, "/dealer/register", _register_form("pass", 1))
        assert rv.status_code in (302, 303)
        assert _user_exists("newdealer1")
    with site.test_client() as c:  # a used token is refused (single use)
        rv = _post(c, "/dealer/register", _register_form("pass", 2))
        assert rv.status_code == 400
        assert not _user_exists("newdealer2")
    assert ("pass", "127.0.0.1") in fake_turnstile.calls


def test_dealer_register_checks_turnstile_before_saying_a_name_is_taken(site):
    _user("newdealer1", "dealership_owner")
    with site.test_client() as c:
        rv = _post(c, "/dealer/register", _register_form(None, 1))
    assert rv.status_code == 400
    assert "already registered" not in rv.get_data(as_text=True)


def test_dealer_register_closed_without_keys_in_production(app_factory, monkeypatch):
    from backend.auth.phone import turnstile
    from backend.dealer.routes import SIGNUP_CLOSED_MSG

    class _MustNotBeCalled:
        calls = 0

        def verify(self, token, ip):
            self.calls += 1
            return turnstile.TurnstileResult(True, turnstile.OK)

    stub = _MustNotBeCalled()
    monkeypatch.setattr(turnstile, "_default", stub)
    app = app_factory(production=True, TURNSTILE_SITE_KEY=None, TURNSTILE_SECRET_KEY=None).app
    with app.test_client() as c:
        page = c.get("/dealer/register").get_data(as_text=True)
        assert SIGNUP_CLOSED_MSG in page
        assert 'name="username"' not in page  # no form at all
        rv = _post(c, "/dealer/register", _register_form("pass", 7))
        assert rv.status_code == 503
        assert SIGNUP_CLOSED_MSG in rv.get_data(as_text=True)
    assert not _user_exists("newdealer7")
    assert stub.calls == 0


def test_dealer_register_open_with_keys_in_production_shows_the_widget(app_factory, monkeypatch):
    from backend.auth.phone import turnstile
    from backend.dealer.routes import SIGNUP_CLOSED_MSG

    class _Refuse:
        def verify(self, token, ip):
            return turnstile.TurnstileResult(False, turnstile.REJECTED)

    monkeypatch.setattr(turnstile, "_default", _Refuse())
    app = app_factory(
        production=True, TURNSTILE_SITE_KEY="site-key-for-tests", TURNSTILE_SECRET_KEY="secret-for-tests",
        CSP_ENFORCE="1",
    ).app
    with app.test_client() as c:
        rv = c.get("/dealer/register")
        page = rv.get_data(as_text=True)
        assert SIGNUP_CLOSED_MSG not in page
        assert 'data-sitekey="site-key-for-tests"' in page
        assert "secret-for-tests" not in page
        csp = rv.headers.get("Content-Security-Policy", "")
        assert "https://challenges.cloudflare.com" in csp
        # every other page keeps the strict policy
        assert "challenges.cloudflare.com" not in c.get("/dealer/login").headers.get("Content-Security-Policy", "")
        rv = _post(c, "/dealer/register", _register_form("anything", 8))
        assert rv.status_code == 400
    assert not _user_exists("newdealer8")


def test_signup_paused_refuses_with_sentence(site, settings_rows, fake_turnstile):
    from backend import settings_store
    from backend.dealer.routes import SIGNUP_PAUSED_MSG

    settings_rows["seller.signup_paused"] = "true"
    settings_store.reset_cache()
    with site.test_client() as c:
        rv = _post(c, "/dealer/register", _register_form("pass", 3))
    assert rv.status_code == 503
    assert SIGNUP_PAUSED_MSG in rv.get_data(as_text=True)
    assert not _user_exists("newdealer3")
    assert fake_turnstile.calls == []  # paused is checked first
    settings_rows["seller.signup_paused"] = "false"
    settings_store.reset_cache()
    with site.test_client() as c:
        rv = _post(c, "/dealer/register", _register_form("pass", 3))
    assert rv.status_code in (302, 303)
    assert _user_exists("newdealer3")


def test_signup_paused_by_environment(site, monkeypatch):
    from backend import settings_store
    from backend.dealer.routes import SIGNUP_PAUSED_MSG

    monkeypatch.setenv("SELLER_SIGNUP_PAUSED", "1")
    settings_store.reset_cache()
    with site.test_client() as c:
        rv = _post(c, "/dealer/register", _register_form("pass", 4))
    assert rv.status_code == 503 and SIGNUP_PAUSED_MSG in rv.get_data(as_text=True)


def test_three_dealer_signups_an_hour_per_address(site):
    from backend.dealer.routes import SIGNUP_HOURLY_MSG

    with site.test_client() as c:
        # a failed attempt does not use a slot
        assert _post(c, "/dealer/register", _register_form("wrong", 9)).status_code == 400
        for n, token in ((1, "pass"), (2, "pass-2"), (3, "pass-3")):
            with c.session_transaction() as sess:
                sess.clear()
            rv = _post(c, "/dealer/register", _register_form(token, n))
            assert rv.status_code in (302, 303), n
        with c.session_transaction() as sess:
            sess.clear()
        rv = _post(c, "/dealer/register", _register_form("pass-4", 4))
    assert rv.status_code == 429
    assert SIGNUP_HOURLY_MSG in rv.get_data(as_text=True)
    assert not _user_exists("newdealer4")


# ---------------------------------------------------------------------------
# (4) Suspend retires the account's portal rows, and nothing else
# ---------------------------------------------------------------------------


def _seed_suspend_world(sqlite_inventory):
    target = _user("suspendme", "dealership_owner")
    other = _user("neighbour", "dealership_owner")
    sqlite_inventory.add_cars([
        {"vin": "TGT00000000000001", "dealer_id": f"dealer-portal-{target}", "listing_active": 1},
        {"vin": "TGT00000000000002", "dealer_id": f"dealer-portal-{target}", "listing_active": 1},
        {"vin": "OTH00000000000001", "dealer_id": f"dealer-portal-{other}", "listing_active": 1},
        # same prefix, longer id: must not match the exact dealer-portal-<id>
        {"vin": "PFX00000000000001", "dealer_id": f"dealer-portal-{target}0", "listing_active": 1},
        {"vin": "SCN00000000000001", "dealer_id": "scanned-dealer-com", "listing_active": 1},
        {"vin": "SCN00000000000002", "dealer_id": "scanned-dealer-com", "listing_active": 0},
    ])
    return target, other


def test_suspend_retires_every_portal_row(site, sqlite_inventory):
    from backend.db.users_db import set_user_is_active

    target, _other = _seed_suspend_world(sqlite_inventory)
    admin = _user("siteadmin1", "admin")
    with site.test_client() as c:
        _sign_in(c, admin)
        rv = _post(c, "/admin/users", {"action": "suspend", "user_id": str(target)})
        assert rv.status_code in (302, 303)
    rows = _cars_rows(sqlite_inventory)
    assert rows["TGT00000000000001"] == (f"dealer-portal-{target}", 0)
    assert rows["TGT00000000000002"] == (f"dealer-portal-{target}", 0)
    # Un-suspending does not republish.
    assert set_user_is_active(target, active=True)
    rows = _cars_rows(sqlite_inventory)
    assert rows["TGT00000000000001"][1] == 0 and rows["TGT00000000000002"][1] == 0


def test_scanner_rows_unchanged_by_suspend(site, sqlite_inventory):
    from backend.db.users_db import set_user_is_active

    target, other = _seed_suspend_world(sqlite_inventory)
    before = _cars_rows(sqlite_inventory)
    assert set_user_is_active(target, active=False)
    after = _cars_rows(sqlite_inventory)
    for vin in ("OTH00000000000001", "PFX00000000000001", "SCN00000000000001", "SCN00000000000002"):
        assert after[vin] == before[vin], vin


# ---------------------------------------------------------------------------
# (5) The VIN collision is told to the seller
# ---------------------------------------------------------------------------


def test_vin_collision_is_told_to_the_seller(site, sqlite_inventory):
    from backend.db import dealer_portal_db as ddb
    from backend.dealer.routes import VIN_TAKEN_MSG

    sqlite_inventory.add_cars([{"vin": _VIN_A, "dealer_id": "scanned-dealer-com", "listing_active": 1}])
    dealer = _user("collide1", "dealership_owner")
    _old_vehicle(dealer, _VIN_C)  # grandfathered, so the sync is attempted
    with site.test_client() as c:
        _sign_in(c, dealer)
        rv = _post(c, "/inventory/add-vin", {"vin": _VIN_A}, follow_redirects=True)
    assert VIN_TAKEN_MSG in rv.get_data(as_text=True)
    assert ddb.count_user_vehicles_with_vin(dealer, _VIN_A) == 1  # saved
    assert _cars_rows(sqlite_inventory)[_VIN_A] == ("scanned-dealer-com", 1)  # not published, not taken


def test_sync_reports_each_outcome(site, sqlite_inventory, monkeypatch):
    from backend.dealer import portal_sync as ps

    dealer = _user("outcome1", "dealership_owner")
    assert ps.sync_portal_vehicle_to_public_listings(dealer, {}) == ps.SYNC_SKIPPED
    assert ps.sync_portal_vehicle_to_public_listings(dealer, {"vin": _VIN_A}) == ps.SYNC_PRIVATE
    monkeypatch.setenv("MARKETPLACE_POSTING_ENABLED", "1")
    assert ps.sync_portal_vehicle_to_public_listings(dealer, {"vin": _VIN_A}) == ps.SYNC_PUBLISHED
    sqlite_inventory.add_cars([{"vin": _VIN_B, "dealer_id": "scanned-dealer-com"}])
    assert ps.sync_portal_vehicle_to_public_listings(dealer, {"vin": _VIN_B}) == ps.SYNC_VIN_TAKEN

    def _boom(*_a, **_k):
        raise RuntimeError("read failed")

    monkeypatch.setattr("backend.db.repositories.cars_repo.get_car_by_vin", _boom)
    assert ps.sync_portal_vehicle_to_public_listings(dealer, {"vin": _VIN_C}) == ps.SYNC_FAILED


# ---------------------------------------------------------------------------
# (6) /dealer/login shows the generic error for every failure
# ---------------------------------------------------------------------------


def test_dealer_login_shows_the_generic_error(site):
    from backend.dealer.routes import DEALER_LOGIN_ERROR

    _user("loginme", "dealership_owner")
    with site.test_client() as c:
        for form in (
            {"login": "loginme", "password": "wrong-password-123"},
            {"login": "nobody-here", "password": "wrong-password-123"},
            {"login": "", "password": ""},
        ):
            rv = _post(c, "/dealer/login", form)
            assert rv.status_code == 200
            assert DEALER_LOGIN_ERROR in rv.get_data(as_text=True), form


# ---------------------------------------------------------------------------
# CSRF on every dealer_portal POST
# ---------------------------------------------------------------------------


def test_every_dealer_post_is_csrf_protected(site, sqlite_inventory):
    from backend.db import dealer_portal_db as ddb
    from backend.utils.csrf import _SESSION_KEY

    dealer = _user("csrf1", "dealership_owner")
    vid = ddb.insert_vehicle(dealer, {"vin": _VIN_A})
    endpoints = sorted(
        (rule.endpoint, rule)
        for rule in site.url_map.iter_rules()
        if rule.endpoint.startswith("dealer_portal.") and "POST" in (rule.methods or set())
    )
    names = {e for e, _ in endpoints}
    assert {
        "dealer_portal.dealer_login", "dealer_portal.dealer_register", "dealer_portal.inventory_add_vin",
        "dealer_portal.inventory_update_vehicle", "dealer_portal.inventory_upload_photos",
        "dealer_portal.inventory_delete_photo", "dealer_portal.inventory_delete_vehicle",
    } <= names
    for endpoint, rule in endpoints:
        url = re.sub(r"<int:vehicle_id>", str(vid), rule.rule)
        assert "<" not in url, url
        with site.test_client() as c:
            _sign_in(c, dealer)
            with c.session_transaction() as sess:
                assert sess[_SESSION_KEY] == _CSRF
            for data in ({}, {"csrf_token": "x" * 43}):
                rv = c.post(url, data={"vin": _VIN_B, "price": "1", "login": "csrf1", "password": _PW,
                                       **_register_form("pass", 5), **data})
                refused = rv.status_code == 403 or (
                    rv.status_code in (302, 303) and "session_expired" in rv.headers.get("Location", "")
                )
                assert refused, (endpoint, rv.status_code)
    assert ddb.count_user_vehicles_with_vin(dealer, _VIN_B) == 0
    assert ddb.get_vehicle(dealer, vid) is not None
    assert not _user_exists("newdealer5")


# ---------------------------------------------------------------------------
# The owner's read-only script
# ---------------------------------------------------------------------------


def _script_text() -> str:
    return _SCRIPT.read_text(encoding="utf-8")


def test_list_script_has_no_write_or_ddl_statements():
    from backend.scripts import list_grandfathered_sellers as lg

    for stmt in lg.STATEMENTS:
        assert stmt.lstrip().upper().startswith("SELECT"), stmt
    # Every SQL string literal in the file, and every execute() call, is a SELECT or the read-only switch.
    text = _script_text()
    forbidden = re.compile(
        r"\b(INSERT|UPDATE|DELETE|MERGE|UPSERT|CREATE|ALTER|DROP|TRUNCATE|GRANT|REVOKE|COPY|VACUUM|"
        r"REINDEX|CLUSTER|COMMENT\s+ON|CALL|DO\s+\$|LOCK)\b",
        re.IGNORECASE,
    )
    code = re.sub(r'"""[\s\S]*?"""', "", text)  # the docstring explains, it does not run
    code = "\n".join(line.split("#", 1)[0] for line in code.splitlines())
    assert not forbidden.search(code), forbidden.search(code)
    executed = re.findall(r"\.execute\(([^)]*)\)", code)
    assert executed and all(
        e.strip() in ("SQL_VEHICLE_ROWS", "SQL_LIVE_PUBLIC", '"SET TRANSACTION READ ONLY"') for e in executed
    ), executed
    assert "conn.read_only = True" in code
    assert "commit" not in code
    # standard library plus psycopg only
    imports = set(re.findall(r"^\s*(?:from|import)\s+([A-Za-z_][\w]*)", code, flags=re.MULTILINE))
    assert imports <= {"__future__", "os", "sys", "datetime", "psycopg"}, imports


def test_list_script_cutoff_matches_the_app():
    from backend.dealer.portal_sync import GRANDFATHER_CUTOFF
    from backend.scripts import list_grandfathered_sellers as lg

    assert lg.GRANDFATHER_CUTOFF == GRANDFATHER_CUTOFF


def test_list_script_prints_ids_and_counts_only():
    from backend.scripts import list_grandfathered_sellers as lg

    before = (lg.GRANDFATHER_CUTOFF - timedelta(days=1)).isoformat()
    after = (lg.GRANDFATHER_CUTOFF + timedelta(hours=1)).isoformat()
    report = lg.summarise(
        [(7, before), (7, after), (9, after), (11, "not a date"), (12, datetime(2026, 1, 1))],
        [("dealer-portal-7", 2), ("dealer-portal-9", 1), ("scanned-dealer-com", 50), ("dealer-portal-x", 3)],
    )
    assert [r["user_id"] for r in report["grandfathered"]] == [7, 12]
    assert report["grandfathered"][0] == {"user_id": 7, "rows_before_cutoff": 1, "rows_now": 2, "live_public_cars": 2}
    assert report["not_grandfathered_with_live_cars"] == [{"user_id": 9, "rows_now": 1, "live_public_cars": 1}]
    assert report["unreadable_dates"] == 1
    out = lg.render(report)
    assert "account 7:" in out and "account 9:" in out
    assert "@" not in out and "scanned-dealer-com" not in out
    assert not re.search(r"\d{7,}", out)


def test_list_script_refuses_without_a_database_address(monkeypatch, capsys):
    from backend.scripts import list_grandfathered_sellers as lg

    monkeypatch.setenv("INVENTORY_DATABASE_URL", "")
    monkeypatch.setenv("DATABASE_URL", "")
    assert lg.main() == 2
    assert "INVENTORY_DATABASE_URL" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# Postgres tier: the grandfather rule and the script on the real schema
# ---------------------------------------------------------------------------


def _pg_vehicle(pg_db, user_id: int, vin: str, created_at: str) -> None:
    with pg_db.connect() as conn:
        conn.execute(
            "INSERT INTO dealer_vehicles (user_id, vin, gallery_json, created_at, updated_at) "
            "VALUES (%s, %s, '[]', %s, %s)",
            (user_id, vin, created_at, created_at),
        )
        conn.commit()


@pytest.mark.pg
def test_grandfather_rule_on_postgres(pg_db):
    from backend.dealer.portal_sync import GRANDFATHER_CUTOFF, is_grandfathered_account

    old = (GRANDFATHER_CUTOFF - timedelta(days=2)).isoformat()
    new = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    _pg_vehicle(pg_db, 41, _VIN_A, old)
    _pg_vehicle(pg_db, 42, _VIN_B, new)
    assert is_grandfathered_account(41)
    assert not is_grandfathered_account(42)
    assert not is_grandfathered_account(43)


@pytest.mark.pg
def test_list_script_runs_read_only_on_postgres(pg_db, capsys):
    from backend.dealer.portal_sync import GRANDFATHER_CUTOFF
    from backend.scripts import list_grandfathered_sellers as lg

    old = (GRANDFATHER_CUTOFF - timedelta(days=2)).isoformat()
    new = (GRANDFATHER_CUTOFF + timedelta(days=1)).isoformat()
    _pg_vehicle(pg_db, 41, _VIN_A, old)
    _pg_vehicle(pg_db, 42, _VIN_B, new)
    pg_db.add_cars([
        {"vin": _VIN_A, "dealer_id": "dealer-portal-41", "listing_active": 1},
        {"vin": _VIN_B, "dealer_id": "dealer-portal-42", "listing_active": 1},
        {"vin": _VIN_C, "dealer_id": "scanned-dealer-com", "listing_active": 1},
    ])
    with pg_db.connect() as conn:
        before = conn.execute("SELECT COUNT(*) FROM cars").fetchone()[0]
    assert lg.main() == 0
    out = capsys.readouterr().out
    assert "Grandfathered accounts: 1" in out and "account 41:" in out
    assert "Accounts after the cut-off with live public cars: 1" in out and "account 42:" in out
    with pg_db.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM cars").fetchone()[0] == before
