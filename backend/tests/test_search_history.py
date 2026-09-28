"""Per-user search history: repo, /api/profile/search-history, recording on the
search endpoints, run-again URL builder, and the profile page."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest

CSRF = "search-history-test-csrf-token-32-chars-long"
DEALER_ID = "t-dealer"
T0 = datetime(2026, 9, 28, 12, 0, 0, tzinfo=timezone.utc)


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


def _insert_car(vin: str, dealer_id: str, model: str = "Civic") -> int:
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
                vin, f"2022 Honda {model} Sport", 2022, "Honda", model, "Sport", 24500, 12000,
                "https://example.com/civic.jpg", "Test Dealer", f"https://{dealer_id}.test/",
                dealer_id, "2026-01-01T00:00:00Z", "Gas", "Automatic", "FWD",
                "Blue", "Black", "[]", 1,
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
    car_id = _insert_car("1HGBH41JXMN109186", DEALER_ID)
    uid = save_user("histuser", "hist@example.com", "longpassword123", role="general", org_id=None)
    other = save_user("otheruser", "other@example.com", "longpassword123", role="general", org_id=None)
    return {"app": app, "uid": int(uid), "other_uid": int(other), "car_id": car_id}


def _login(client, uid: int) -> None:
    with client.session_transaction() as sess:
        sess["user_id"] = int(uid)
        sess["username"] = "histuser"
        sess["mfa_ok"] = True
        sess["_csrf_token"] = CSRF


def _count_rows(uid: int) -> int:
    from backend.db.repositories.base_repo import db_conn

    with db_conn() as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM user_search_history WHERE user_id = ?", (int(uid),)
        ).fetchone()
    return int(row[0])


# ---------------------------------------------------------------------------
# Repository
# ---------------------------------------------------------------------------

def test_repo_record_and_dedupe_window(env):
    from backend.db.repositories import search_history_repo as repo

    uid = env["uid"]
    filters = {"make": ["Honda"], "model": ["Civic"], "max_price": "30000"}

    first = repo.record_search_history(uid, filters, query_text=None, result_count=12, now=T0)
    assert first is not None
    # Same filters (different key order) 5 minutes later: skipped, same id back.
    again = repo.record_search_history(
        uid, {"max_price": "30000", "model": ["Civic"], "make": ["Honda"]},
        result_count=13, now=T0 + timedelta(minutes=5),
    )
    assert again == first
    assert _count_rows(uid) == 1

    # Different filters inside the window: recorded.
    second = repo.record_search_history(uid, {"make": ["Toyota"]}, now=T0 + timedelta(minutes=6))
    assert second is not None and second != first
    assert _count_rows(uid) == 2

    # Original filters again, but the most recent row is now the Toyota one -> recorded.
    third = repo.record_search_history(uid, filters, now=T0 + timedelta(minutes=7))
    assert third not in (first, second)
    assert _count_rows(uid) == 3

    # Same filters as the newest row but after the window -> recorded.
    fourth = repo.record_search_history(uid, filters, now=T0 + timedelta(minutes=18))
    assert fourth != third
    assert _count_rows(uid) == 4

    # Another user's history is separate; an empty search is not recorded.
    assert _count_rows(env["other_uid"]) == 0
    assert repo.record_search_history(uid, {}, now=T0) is None
    assert _count_rows(uid) == 4


def test_repo_list_order_and_limit(env):
    from backend.db.repositories import search_history_repo as repo

    uid = env["uid"]
    for i in range(5):
        repo.record_search_history(
            uid, {"make": [f"Make{i}"]}, query_text=f"q{i}", result_count=i,
            now=T0 + timedelta(minutes=i),
        )
    rows = repo.list_search_history(uid, limit=20)
    assert [r["filters"]["make"] for r in rows] == [["Make4"], ["Make3"], ["Make2"], ["Make1"], ["Make0"]]
    assert rows[0]["query_text"] == "q4" and rows[0]["result_count"] == 4
    assert rows[0]["created_at"] == "2026-09-28T12:04:00+00:00"

    rows = repo.list_search_history(uid, limit=2)
    assert [r["query_text"] for r in rows] == ["q4", "q3"]
    # Nonsense limits fall back to sane bounds.
    assert len(repo.list_search_history(uid, limit=0)) == 1
    assert len(repo.list_search_history(uid, limit="x")) == 5


def test_repo_prunes_to_last_100(env):
    from backend.db.repositories import search_history_repo as repo

    uid = env["uid"]
    for i in range(105):
        repo.record_search_history(uid, {"make": [f"M{i:03d}"]}, now=T0 + timedelta(seconds=i))
    assert _count_rows(uid) == 100
    rows = repo.list_search_history(uid, limit=100)
    assert rows[0]["filters"]["make"] == ["M104"]
    assert rows[-1]["filters"]["make"] == ["M005"]

    # Explicit prune to a smaller window.
    assert repo.prune_search_history(uid, keep=10) == 90
    assert _count_rows(uid) == 10
    assert repo.prune_search_history(uid, keep=10) == 0


def test_repo_delete_one_and_clear(env):
    from backend.db.repositories import search_history_repo as repo

    uid, other = env["uid"], env["other_uid"]
    a = repo.record_search_history(uid, {"make": ["Honda"]}, now=T0)
    b = repo.record_search_history(uid, {"make": ["Toyota"]}, now=T0 + timedelta(minutes=1))
    c = repo.record_search_history(other, {"make": ["Ford"]}, now=T0)

    # Another user cannot delete this user's row.
    assert repo.delete_search_history_entry(other, a) is False
    assert repo.delete_search_history_entry(uid, a) is True
    assert repo.delete_search_history_entry(uid, a) is False
    assert [r["id"] for r in repo.list_search_history(uid)] == [b]

    assert repo.clear_search_history(uid) == 1
    assert repo.list_search_history(uid) == []
    assert [r["id"] for r in repo.list_search_history(other)] == [c]


def test_repo_rejects_oversized_filters(env):
    from backend.db.repositories import search_history_repo as repo

    with pytest.raises(ValueError):
        repo.record_search_history(env["uid"], {"q": "x" * 5000})


def test_parse_created_at_accepts_engine_defaults():
    from backend.db.repositories.search_history_repo import parse_created_at

    iso = parse_created_at("2026-09-28T12:00:00+00:00")
    assert iso == T0
    assert parse_created_at("2026-09-28 12:00:00") == T0              # SQLite datetime('now')
    assert parse_created_at("2026-09-28 12:00:00.123456+00") == T0.replace(microsecond=123456)  # PG ::text
    assert parse_created_at("2026-09-28T12:00:00Z") == T0
    assert parse_created_at("garbage") is None
    assert parse_created_at(None) is None


# ---------------------------------------------------------------------------
# Run-again URL builder + label
# ---------------------------------------------------------------------------

def test_listings_url_for_filters_roundtrip():
    from backend.utils.search_history_format import listings_url_for_filters

    url = listings_url_for_filters(
        {
            "make": ["Honda", "Toyota"],
            "model": "Civic",
            "max_price": 30000,
            "zip_code": "28202",
            "radius": "50",
            "dealer_registry_ids": ["12", "13"],
            "cpo_only": True,
            "page": "3",
            "q": "",
            "trim": [],
        }
    )
    parts = urlsplit(url)
    assert parts.path == "/listings"
    qs = parse_qs(parts.query, keep_blank_values=True)
    assert qs == {
        "cpo_only": ["1"],
        "dealer_registry_id": ["12", "13"],
        "make": ["Honda", "Toyota"],
        "max_price": ["30000"],
        "model": ["Civic"],
        "radius": ["50"],
        "zip_code": ["28202"],
    }
    assert listings_url_for_filters({}) == "/listings"
    assert listings_url_for_filters(None) == "/listings"
    assert listings_url_for_filters({"q": "red awd wagon"}) == "/listings?q=red+awd+wagon"


def test_describe_search_filters_label():
    from backend.utils.search_history_format import describe_search_filters

    label = describe_search_filters(
        {
            "make": ["Honda"], "model": ["Civic"], "min_year": "2020", "max_year": "2023",
            "max_price": "30000", "zip_code": "28202", "radius": "50", "q": "low miles",
        }
    )
    assert label == "Honda Civic · 2020–2023 · under $30,000 · within 50 mi of 28202 · “low miles”"
    assert describe_search_filters({}) == "All listings"
    assert describe_search_filters({"min_year": "2021"}) == "2021 or newer"
    assert describe_search_filters({"min_price": 10000, "max_price": 20000}) == "$10,000–$20,000"
    assert describe_search_filters({"cpo_only": "1", "zip_code": "28202"}) == "Certified pre-owned · near 28202"
    assert describe_search_filters({}, query_text="Tacoma") == "“Tacoma”"


def test_humanize_when():
    from backend.utils.search_history_format import humanize_when

    now = T0
    assert humanize_when("2026-09-28T11:59:40+00:00", now=now) == "just now"
    assert humanize_when("2026-09-28T11:55:00+00:00", now=now) == "5 minutes ago"
    assert humanize_when("2026-09-28T09:00:00+00:00", now=now) == "3 hours ago"
    assert humanize_when("2026-09-27T09:00:00+00:00", now=now) == "yesterday"
    assert humanize_when("2026-09-25T09:00:00+00:00", now=now) == "3 days ago"
    assert humanize_when("2026-09-01T09:00:00+00:00", now=now) == "Sep 1"
    assert humanize_when("2025-09-01T09:00:00+00:00", now=now) == "Sep 1, 2025"
    assert humanize_when("nope", now=now) == ""


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------

def test_api_requires_login(env):
    client = env["app"].test_client()
    rv = client.get("/api/profile/search-history")
    assert rv.status_code == 401
    assert rv.get_json() == {"ok": False, "error": "not_logged_in", "searches": []}
    assert client.delete("/api/profile/search-history/1").status_code == 401
    assert client.delete("/api/profile/search-history").status_code == 401


def test_api_list_delete_clear_roundtrip(env):
    from backend.db.repositories import search_history_repo as repo

    uid = env["uid"]
    a = repo.record_search_history(uid, {"make": ["Honda"], "model": ["Civic"]}, result_count=3, now=T0)
    b = repo.record_search_history(uid, {"q": "red awd wagon"}, query_text="red awd wagon", result_count=0,
                                   now=T0 + timedelta(minutes=1))
    c = repo.record_search_history(env["other_uid"], {"make": ["Ford"]}, now=T0)

    client = env["app"].test_client()
    _login(client, uid)
    hdrs = {"X-CSRF-Token": CSRF}

    rv = client.get("/api/profile/search-history?limit=20")
    assert rv.status_code == 200
    body = rv.get_json()
    assert body["ok"] is True
    assert [s["id"] for s in body["searches"]] == [b, a]
    top = body["searches"][0]
    assert top["label"] == "“red awd wagon”"
    assert top["url"] == "/listings?q=red+awd+wagon"
    assert top["result_count"] == 0
    assert top["query_text"] == "red awd wagon"
    assert top["when"]
    assert body["searches"][1]["url"] == "/listings?make=Honda&model=Civic"

    rv = client.get("/api/profile/search-history?limit=1")
    assert [s["id"] for s in rv.get_json()["searches"]] == [b]

    # Someone else's row: 404, and it stays.
    assert client.delete(f"/api/profile/search-history/{c}", headers=hdrs).status_code == 404
    rv = client.delete(f"/api/profile/search-history/{a}", headers=hdrs)
    assert rv.status_code == 200 and rv.get_json() == {"ok": True, "id": a}
    assert client.delete(f"/api/profile/search-history/{a}", headers=hdrs).status_code == 404
    assert [s["id"] for s in client.get("/api/profile/search-history").get_json()["searches"]] == [b]

    rv = client.delete("/api/profile/search-history", headers=hdrs)
    assert rv.status_code == 200 and rv.get_json() == {"ok": True, "removed": 1}
    assert client.get("/api/profile/search-history").get_json()["searches"] == []
    assert [r["id"] for r in repo.list_search_history(env["other_uid"])] == [c]


# ---------------------------------------------------------------------------
# Recording on the search endpoints
# ---------------------------------------------------------------------------

def test_listings_page_records_for_logged_in_user_only(env, monkeypatch):
    from backend.db.repositories import search_history_repo as repo

    app = env["app"]
    uid = env["uid"]

    anon = app.test_client()
    assert anon.get("/listings?make=Honda&model=Civic").status_code == 200
    assert anon.get("/listings?q=Honda").status_code == 200
    assert _count_rows(uid) == 0

    client = app.test_client()
    _login(client, uid)
    # Bare listings visit is browsing, not a search.
    assert client.get("/listings").status_code == 200
    assert _count_rows(uid) == 0

    # Filter-only visit: recorded without a count (grid filters client-side).
    assert client.get("/listings?make=Honda&model=Civic&max_price=30000&dealer_registry_id=7").status_code == 200
    rows = repo.list_search_history(uid)
    assert len(rows) == 1
    assert rows[0]["filters"] == {
        "make": ["Honda"], "model": ["Civic"], "max_price": "30000", "dealer_registry_id": ["7"],
    }
    assert rows[0]["result_count"] is None
    assert rows[0]["query_text"] is None

    # Query search: recorded with the server-side count and the text. Searches are
    # ZIP + radius scoped since 2026-09-28; the fixture car has no dealer location,
    # so the real search runs with the geo kwargs dropped (radius is tested in
    # test_listings_scoped_grid).
    import backend.listings.routes as listings_routes

    real_hybrid = listings_routes.hybrid_search_with_kwargs

    def unscoped_hybrid(q, kwargs, **kw):
        kwargs = {k: v for k, v in kwargs.items() if k not in ("zip_code", "radius_miles")}
        return real_hybrid(q, kwargs, **kw)

    monkeypatch.setattr(listings_routes, "hybrid_search_with_kwargs", unscoped_hybrid)
    assert client.get("/listings?q=Honda+Civic&zip_code=28202&radius=50").status_code == 200
    rows = repo.list_search_history(uid)
    assert len(rows) == 2
    assert rows[0]["filters"] == {"q": "Honda Civic", "zip_code": "28202", "radius": "50"}
    assert rows[0]["query_text"] == "Honda Civic"
    assert rows[0]["result_count"] == 1

    # Reloading the same URL inside the dedupe window adds nothing.
    assert client.get("/listings?q=Honda+Civic&zip_code=28202&radius=50").status_code == 200
    assert _count_rows(uid) == 2


def test_smart_search_records_for_logged_in_user_only(env):
    from backend.db.repositories import search_history_repo as repo

    app = env["app"]

    def smart(client, payload):
        with client.session_transaction() as sess:
            sess["_csrf_token"] = CSRF
        rv = client.post(
            "/api/search/smart", data=json.dumps(payload),
            content_type="application/json", headers={"X-CSRF-Token": CSRF},
        )
        assert rv.status_code == 200, rv.get_data(as_text=True)
        return rv.get_json()

    # Smart search is ZIP + radius scoped since 2026-09-28: every call carries one.
    smart(app.test_client(), {"query": "Honda", "zip_code": "28202", "radius": 50})
    assert _count_rows(env["uid"]) == 0

    client = app.test_client()
    _login(client, env["uid"])
    body = smart(client, {"query": "Honda", "zip_code": "28202", "radius": 50})
    rows = repo.list_search_history(env["uid"])
    assert len(rows) == 1
    assert rows[0]["filters"] == {"q": "Honda", "zip_code": "28202", "radius": "50"}
    assert rows[0]["query_text"] == "Honda"
    assert rows[0]["result_count"] == len(body["results"])

    # Empty query: nothing to remember.
    smart(client, {"query": "", "zip_code": "28202", "radius": 50})
    assert _count_rows(env["uid"]) == 1


def test_failing_history_write_does_not_break_search(env, monkeypatch):
    import backend.listings.routes as listings_routes
    import backend.main as main

    def boom(*a, **k):
        raise RuntimeError("history table is on fire")

    monkeypatch.setattr(listings_routes, "record_search_history", boom)
    monkeypatch.setattr(main, "record_search_history", boom)

    client = env["app"].test_client()
    _login(client, env["uid"])

    # Searches are ZIP + radius scoped since 2026-09-28. The fixture car has no
    # dealer location, so keep the real search but drop the geo kwargs: this test is
    # about the history write failing, not about the radius.
    import backend.utils.hybrid_search as hs

    real_hybrid = hs.hybrid_search_with_kwargs
    real_smart = hs.hybrid_smart_search

    def unscoped_hybrid(q, kwargs, **kw):
        kwargs = {k: v for k, v in kwargs.items() if k not in ("zip_code", "radius_miles")}
        return real_hybrid(q, kwargs, **kw)

    def unscoped_smart(q, filters, **kw):
        kw["listing_geo_kwargs"] = None
        return real_smart(q, filters, **kw)

    monkeypatch.setattr(listings_routes, "hybrid_search_with_kwargs", unscoped_hybrid)
    monkeypatch.setattr(hs, "hybrid_smart_search", unscoped_smart)

    rv = client.get("/listings?q=Honda&zip_code=28202&radius=50")
    assert rv.status_code == 200
    assert "Honda" in rv.get_data(as_text=True)

    rv = client.post(
        "/api/search/smart", data=json.dumps({"query": "Honda", "zip_code": "28202", "radius": 50}),
        content_type="application/json", headers={"X-CSRF-Token": CSRF},
    )
    assert rv.status_code == 200
    body = rv.get_json()
    assert body["ok"] is True
    assert env["car_id"] in {c["id"] for c in body["results"]}
    assert _count_rows(env["uid"]) == 0


# ---------------------------------------------------------------------------
# Profile page
# ---------------------------------------------------------------------------

def test_profile_page_lists_recent_and_saved_searches(env):
    from backend.db.repositories import search_history_repo as repo
    from backend.db.repositories.saved_searches_repo import create_saved_search

    uid = env["uid"]
    hid = repo.record_search_history(
        uid, {"make": ["Honda"], "model": ["Civic"], "max_price": "30000"}, result_count=4, now=T0,
    )
    sid = create_saved_search(uid, {"make": ["Toyota"], "zip_code": "28202", "radius": "25"})

    client = env["app"].test_client()
    _login(client, uid)
    rv = client.get("/account/profile")
    assert rv.status_code == 200
    html = rv.get_data(as_text=True)

    assert 'id="recent-searches"' in html
    assert f'data-search-id="{hid}"' in html
    assert "Honda Civic · under $30,000" in html
    assert 'href="/listings?make=Honda&amp;max_price=30000&amp;model=Civic"' in html
    assert "4 results" in html
    assert "Run again" in html
    assert 'class="secondary-button account-profile__history-btn account-profile__history-save"' in html
    assert 'id="recent-searches-clear"' in html

    assert 'id="saved-searches"' in html
    assert f'data-saved-id="{sid}"' in html
    assert "Toyota · within 25 mi of 28202" in html
    assert 'href="/listings?make=Toyota&amp;radius=25&amp;zip_code=28202"' in html


def test_profile_page_empty_state(env):
    client = env["app"].test_client()
    _login(client, env["uid"])
    html = client.get("/account/profile").get_data(as_text=True)
    assert "No searches yet." in html
    assert "You have not saved any searches." in html
    assert 'id="recent-searches-clear" hidden' in html
