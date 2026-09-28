"""Per-user hidden dealerships: repo, /api/profile/hidden-dealers, and search exclusion."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

CSRF = "hidden-dealers-test-csrf-token-32-chars-long"
DEALER_ID = "t-dealer"
DEALER_NAME = "Test Dealer"
OTHER_DEALER_ID = "other-dealer"


def _fresh_app(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("USERS_DB_PATH", str(tmp_path / "users.db"))
    monkeypatch.setenv("INVENTORY_DB_PATH", str(tmp_path / "inventory.db"))
    monkeypatch.setenv("BILLING_STRIPE_ENABLED", "0")
    monkeypatch.setenv("DATABASE_URL", "")
    monkeypatch.setenv("INVENTORY_DATABASE_URL", "")
    monkeypatch.setattr("backend.db.inventory_pg.is_inventory_postgres", lambda: False)
    from backend.db import inventory_db
    from backend.db.inventory_db import init_inventory_db
    from backend.db.users_db import init_users_db, save_user
    from backend.main import app

    monkeypatch.setattr(inventory_db, "DB_PATH", str(tmp_path / "inventory.db"))
    init_users_db()
    init_inventory_db()
    app.config["TESTING"] = True
    return app, save_user


def _insert_car(vin: str, dealer_id: str, dealer_name: str, model: str = "Civic") -> int:
    from backend.db.inventory_db import get_conn

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO cars (
                vin, title, year, make, model, trim, price, mileage,
                image_url, dealer_name, dealer_url, dealer_id, scraped_at,
                fuel_type, transmission, drivetrain,
                exterior_color, interior_color, gallery, listing_active
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                vin,
                f"2022 Honda {model} Sport",
                2022,
                "Honda",
                model,
                "Sport",
                24500,
                12000,
                "https://example.com/civic.jpg",
                dealer_name,
                f"https://{dealer_id}.test/",
                dealer_id,
                "2026-01-01T00:00:00Z",
                "Gas",
                "Automatic",
                "FWD",
                "Blue",
                "Black",
                "[]",
                1,
            ),
        )
        conn.commit()
        row = cur.execute("SELECT id FROM cars WHERE vin = ?", (vin,)).fetchone()
        return int(row[0])
    finally:
        conn.close()


@pytest.fixture
def env(monkeypatch, tmp_path):
    app, save_user = _fresh_app(monkeypatch, tmp_path)
    car_id = _insert_car("1HGBH41JXMN109186", DEALER_ID, DEALER_NAME)
    other_id = _insert_car("2HGBH41JXMN109187", OTHER_DEALER_ID, "Other Dealer", model="Accord")
    uid = save_user("hideuser", "hide@example.com", "longpassword123", role="general", org_id=None)
    return {"app": app, "uid": int(uid), "car_id": car_id, "other_id": other_id}


def _login(client, uid: int) -> None:
    with client.session_transaction() as sess:
        sess["user_id"] = int(uid)
        sess["username"] = "hideuser"
        sess["mfa_ok"] = True
        sess["_csrf_token"] = CSRF


# ---------------------------------------------------------------------------
# Repository
# ---------------------------------------------------------------------------

def test_repo_add_list_remove(env):
    from backend.db.repositories import hidden_dealers_repo as repo

    uid = env["uid"]
    assert repo.list_hidden_dealers(uid) == []
    assert repo.hidden_dealer_ids_for_user(uid) == set()

    assert repo.hide_dealer(uid, "T-Dealer", DEALER_NAME) is True
    # Second hide of the same rooftop is a no-op, not an error or a duplicate row.
    assert repo.hide_dealer(uid, DEALER_ID, DEALER_NAME) is False

    rows = repo.list_hidden_dealers(uid)
    assert [r["dealer_id"] for r in rows] == [DEALER_ID]
    assert rows[0]["dealer_name"] == DEALER_NAME
    assert repo.hidden_dealer_ids_for_user(uid) == {DEALER_ID}
    assert repo.is_dealer_hidden(uid, DEALER_ID) is True

    # Another user does not see it; anonymous callers get an empty set.
    assert repo.hidden_dealer_ids_for_user(uid + 1) == set()
    assert repo.hidden_dealer_ids_for_user(None) == set()

    assert repo.unhide_dealer(uid, DEALER_ID) is True
    assert repo.unhide_dealer(uid, DEALER_ID) is False
    assert repo.list_hidden_dealers(uid) == []
    assert repo.is_dealer_hidden(uid, DEALER_ID) is False


def test_repo_rejects_blank_dealer_id(env):
    from backend.db.repositories import hidden_dealers_repo as repo

    with pytest.raises(ValueError):
        repo.hide_dealer(env["uid"], "   ")


# ---------------------------------------------------------------------------
# API auth
# ---------------------------------------------------------------------------

def test_api_requires_login(env):
    client = env["app"].test_client()
    rv = client.get("/api/profile/hidden-dealers")
    assert rv.status_code == 401
    assert rv.get_json()["error"] == "not_logged_in"

    with client.session_transaction() as sess:
        sess["_csrf_token"] = CSRF
    rv = client.post(
        "/api/profile/hidden-dealers",
        data=json.dumps({"dealer_id": DEALER_ID}),
        content_type="application/json",
        headers={"X-CSRF-Token": CSRF},
    )
    assert rv.status_code == 401

    rv = client.delete(f"/api/profile/hidden-dealers/{DEALER_ID}")
    assert rv.status_code == 401


def test_api_add_without_csrf_header_is_rejected(env):
    client = env["app"].test_client()
    _login(client, env["uid"])
    rv = client.post(
        "/api/profile/hidden-dealers",
        data=json.dumps({"dealer_id": DEALER_ID}),
        content_type="application/json",
    )
    assert rv.status_code == 403


# ---------------------------------------------------------------------------
# API roundtrip
# ---------------------------------------------------------------------------

def test_api_add_list_remove_roundtrip(env):
    client = env["app"].test_client()
    _login(client, env["uid"])
    hdrs = {"X-CSRF-Token": CSRF}

    rv = client.get("/api/profile/hidden-dealers")
    assert rv.status_code == 200
    assert rv.get_json() == {"ok": True, "dealers": []}

    # Unknown dealer -> 404; missing id -> 400.
    rv = client.post(
        "/api/profile/hidden-dealers",
        data=json.dumps({"dealer_id": "nobody-here-com"}),
        content_type="application/json",
        headers=hdrs,
    )
    assert rv.status_code == 404
    rv = client.post(
        "/api/profile/hidden-dealers",
        data=json.dumps({}),
        content_type="application/json",
        headers=hdrs,
    )
    assert rv.status_code == 400

    rv = client.post(
        "/api/profile/hidden-dealers",
        data=json.dumps({"dealer_id": "T-Dealer"}),
        content_type="application/json",
        headers=hdrs,
    )
    assert rv.status_code == 200, rv.get_json()
    body = rv.get_json()
    assert body["ok"] is True and body["hidden"] is True and body["added"] is True
    assert body["dealer"] == {"dealer_id": DEALER_ID, "dealer_name": DEALER_NAME}

    rv = client.get("/api/profile/hidden-dealers")
    dealers = rv.get_json()["dealers"]
    assert [d["dealer_id"] for d in dealers] == [DEALER_ID]
    assert dealers[0]["dealer_name"] == DEALER_NAME

    rv = client.delete(f"/api/profile/hidden-dealers/{DEALER_ID}", headers=hdrs)
    assert rv.status_code == 200
    assert rv.get_json()["hidden"] is False
    rv = client.delete(f"/api/profile/hidden-dealers/{DEALER_ID}", headers=hdrs)
    assert rv.status_code == 404
    assert client.get("/api/profile/hidden-dealers").get_json()["dealers"] == []


# ---------------------------------------------------------------------------
# Search exclusion
# ---------------------------------------------------------------------------

def test_search_cars_excludes_hidden_dealers(env):
    from backend.db.inventory_db import search_cars
    from backend.db.repositories import hidden_dealers_repo as repo

    ids = {c["id"] for c in search_cars(makes=["Honda"], include_incomplete=True)}
    assert env["car_id"] in ids and env["other_id"] in ids

    repo.hide_dealer(env["uid"], DEALER_ID, DEALER_NAME)
    hidden = repo.hidden_dealer_ids_for_user(env["uid"])
    ids = {
        c["id"]
        for c in search_cars(makes=["Honda"], include_incomplete=True, exclude_dealer_ids=hidden)
    }
    assert env["car_id"] not in ids
    assert env["other_id"] in ids

    # Anonymous callers pass nothing and still see every row.
    ids = {c["id"] for c in search_cars(makes=["Honda"], include_incomplete=True, exclude_dealer_ids=set())}
    assert env["car_id"] in ids


def test_search_cars_by_make_model_pairs_excludes_hidden_dealers(env):
    from backend.db.inventory_db import search_cars_by_make_model_pairs

    ids = {c["id"] for c in search_cars_by_make_model_pairs([("Honda", "Civic")], include_incomplete=True)}
    assert env["car_id"] in ids
    ids = {
        c["id"]
        for c in search_cars_by_make_model_pairs(
            [("Honda", "Civic")], include_incomplete=True, exclude_dealer_ids=[DEALER_ID]
        )
    }
    assert env["car_id"] not in ids


def test_smart_search_hides_dealer_for_that_user_only(env):
    """POST /api/search/smart: the hidden dealer's car vanishes for the user, stays for guests."""
    from backend.db.repositories import hidden_dealers_repo as repo

    app = env["app"]
    repo.hide_dealer(env["uid"], DEALER_ID, DEALER_NAME)

    def smart(client):
        with client.session_transaction() as sess:
            sess["_csrf_token"] = CSRF
        rv = client.post(
            "/api/search/smart",
            data=json.dumps({"query": "Honda"}),
            content_type="application/json",
            headers={"X-CSRF-Token": CSRF},
        )
        assert rv.status_code == 200, rv.get_data(as_text=True)
        return {c["id"] for c in rv.get_json()["results"]}

    anon_ids = smart(app.test_client())
    assert env["car_id"] in anon_ids and env["other_id"] in anon_ids

    client = app.test_client()
    _login(client, env["uid"])
    user_ids = smart(client)
    assert env["car_id"] not in user_ids
    assert env["other_id"] in user_ids


def test_listings_page_ships_hidden_set_and_note(env):
    from backend.db.repositories import hidden_dealers_repo as repo

    app = env["app"]
    anon = app.test_client().get("/listings")
    assert anon.status_code == 200
    anon_html = anon.get_data(as_text=True)
    assert 'id="ds-listings-hidden-dealers"' in anon_html
    assert "dealership hidden" not in anon_html

    repo.hide_dealer(env["uid"], DEALER_ID, DEALER_NAME)
    client = app.test_client()
    _login(client, env["uid"])
    rv = client.get("/listings")
    assert rv.status_code == 200
    html = rv.get_data(as_text=True)
    assert f'id="ds-listings-hidden-dealers" nonce=' in html
    assert f'["{DEALER_ID}"]' in html
    assert "1 dealership hidden" in html
    assert "/account/profile#hidden-dealers" in html


def test_profile_page_lists_hidden_dealers(env):
    from backend.db.repositories import hidden_dealers_repo as repo

    repo.hide_dealer(env["uid"], DEALER_ID, DEALER_NAME)
    client = env["app"].test_client()
    _login(client, env["uid"])
    rv = client.get("/account/profile")
    assert rv.status_code == 200
    html = rv.get_data(as_text=True)
    assert 'id="hidden-dealers"' in html
    assert DEALER_NAME in html
    assert f'data-dealer-id="{DEALER_ID}"' in html


def test_dealership_page_shows_toggle_state(env):
    from backend.db.repositories import hidden_dealers_repo as repo

    app = env["app"]
    anon_html = app.test_client().get(f"/dealership/{DEALER_ID}").get_data(as_text=True)
    assert 'id="dealer-hide-toggle"' not in anon_html

    client = app.test_client()
    _login(client, env["uid"])
    html = client.get(f"/dealership/{DEALER_ID}").get_data(as_text=True)
    assert 'id="dealer-hide-toggle"' in html
    assert 'data-hidden="0"' in html
    assert "Hide this dealership from my searches" in html

    repo.hide_dealer(env["uid"], DEALER_ID, DEALER_NAME)
    html = client.get(f"/dealership/{DEALER_ID}").get_data(as_text=True)
    assert 'data-hidden="1"' in html
    assert "Unhide this dealership" in html
