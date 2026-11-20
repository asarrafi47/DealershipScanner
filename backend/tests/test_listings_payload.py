"""/listings document size, the cacheable facets payload and the server-rendered page.

Web speed 2026-11 (PR 4): the document used to inline the fleet cascade table and every
package row (82% of its bytes, ~1.5 MB in production, identical on page 1 and page 2).
They now live in ``GET /api/listings/facets?v=`` (public, immutable, no per-user data),
and a search URL with a cached area carries its page of cards plus prev/next links.
"""

from __future__ import annotations

import gzip
import json
import re
import sqlite3

import pytest

# pgeocode centroid for 92694 (Ladera Ranch, CA) is ~(33.55, -117.64).
NEAR = (33.56, -117.66)
MID = (33.75, -117.87)

N_CARS = 60  # > 2 pages of 24


def _seed(inv):
    conn = sqlite3.connect(str(inv.path))
    try:
        for rid, (lat, lon), host in ((1, NEAR, "near-motors"), (2, MID, "mid-motors")):
            conn.execute(
                "INSERT INTO dealerships (id, name, website_url, city, state, latitude, longitude, is_active) "
                "VALUES (?, ?, ?, 'X', 'CA', ?, ?, 1)",
                (rid, host, f"https://{host}.test", lat, lon),
            )
        conn.commit()
    finally:
        conn.close()
    rows = []
    for i in range(N_CARS):
        dealer, rid = ("near-motors", 1) if i % 2 else ("mid-motors", 2)
        make, model, body = ("Toyota", "Camry", "Sedan") if i % 3 else ("Ford", "F-150", "Truck")
        rows.append({
            "title": f"2022 {make} {model} #{i}",
            "year": 2022,
            "make": make,
            "model": model,
            "trim": "LE",
            "price": 20000 + 137 * i,
            "mileage": 1000 + i,
            "image_url": f"https://cdn.example.com/{i}.jpg",
            "gallery": json.dumps([f"https://cdn.example.com/{i}-{k}.jpg" for k in range(3)]),
            "dealer_name": dealer,
            "dealer_url": f"https://{dealer}.test",
            "dealer_id": f"{dealer}-test",
            "dealership_registry_id": rid,
            "body_style": body,
            "fuel_type": "Gasoline",
            "packages": json.dumps({"packages_normalized": [{"name": f"Package {i % 5}"}]}),
        })
    inv.add_cars(rows)


@pytest.fixture
def fleet(sqlite_inventory):
    from backend.db.repositories import grid_cards_repo as gc
    from backend.db.repositories import listings_repo as lr
    from backend.listings.facets_payload import reset_facets_payload
    from backend.routes import listings_api

    lr.clear_inventory_listings_cache()
    gc.reset_grid_cards_state()
    listings_api.clear_cars_scope_cache()
    reset_facets_payload()
    _seed(sqlite_inventory)
    yield sqlite_inventory
    lr.clear_inventory_listings_cache()
    reset_facets_payload()


@pytest.fixture
def client():
    from backend.main import app

    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c


def _json(resp):
    body = resp.get_data()
    if resp.headers.get("Content-Encoding") == "gzip":
        body = gzip.decompress(body)
    return json.loads(body)


def _facets_url(html: str) -> str:
    m = re.search(r'id="ds-listings-car-rows" data-facets-url="([^"]+)"', html)
    assert m, "the listings marker must carry the facets payload URL"
    return m.group(1).replace("&amp;", "&")


def _card_ids(html: str) -> list[int]:
    grid = html.split('id="results-grid"', 1)[1].split('id="empty-state"', 1)[0]
    return [int(x) for x in re.findall(r'<a href="/car/(\d+)[^"]*" class="result-card-link"', grid)]


# ── document size / what is inlined ─────────────────────────────────────


def test_cascade_table_and_package_rows_are_not_inlined(fleet, client):
    html = client.get("/listings").get_data(as_text=True)
    m = re.search(r'<script type="application/json" id="ds-listings-car-rows"[^>]*>(.*?)</script>', html, re.S)
    assert m and m.group(1).strip() == "[]"
    assert 'id="ds-listings-package-rows"' not in html
    assert "Package 3" not in html  # package names arrive with the payload, not the page
    # Every JSON island keeps the CSP nonce pattern.
    for tag in re.findall(r'<script type="application/json"[^>]*>', html):
        assert 'nonce="' in tag


def test_listings_document_under_200_kb(fleet, client):
    from backend.listings import routes as listings_routes

    # A production-sized cascade table (12.7k rows) must not reach the document.
    big = [
        {"make": f"Make{i % 60}", "model": f"Model{i % 900}", "trim": f"Trim{i}", "fuel": "Gasoline",
         "cyl": 4, "drive": "AWD", "body_style": "SUV", "induction": None}
        for i in range(12_700)
    ]
    real = listings_routes.get_filter_options

    def with_big_table():
        opts = real()
        opts["car_rows"] = big
        opts["package_rows"] = [{"make": "Ford", "model": "F-150", "name": f"Pkg {i}"} for i in range(5000)]
        return opts

    import pytest as _pytest

    mp = _pytest.MonkeyPatch()
    mp.setattr(listings_routes, "get_filter_options", with_big_table)
    try:
        for path in ("/listings", "/listings?page=2", "/listings?zip_code=92694&radius=50"):
            body = client.get(path).get_data()
            assert len(body) < 200 * 1024, (path, len(body))
    finally:
        mp.undo()


# ── the facets payload ─────────────────────────────────────────────────


def test_facets_payload_is_versioned_immutable_and_cookie_free(fleet, client):
    url = _facets_url(client.get("/listings").get_data(as_text=True))
    assert url.startswith("/api/listings/facets?v=")
    client.delete_cookie("session")  # /listings minted one; a cookieless visitor gets "public"
    r = client.get(url, headers={"Accept-Encoding": "gzip"})
    assert r.status_code == 200
    assert r.headers["Cache-Control"] == "public, max-age=31536000, immutable"
    assert r.headers["Content-Encoding"] == "gzip"
    assert r.headers["Vary"] == "Accept-Encoding"
    assert "Set-Cookie" not in r.headers
    etag = r.headers["ETag"]
    data = _json(r)
    assert data["ok"] is True
    # The cascade table (packed) and the long lists the page used to inline or lazy-load.
    assert data["car_rows"]["c"][:3] == ["make", "model", "trim"]
    assert len(data["car_rows"]["r"]) >= 2
    assert {"make", "model", "name"} <= set(data["package_rows"][0])
    assert ["Toyota", "Camry"] in data["model_rows"]
    assert "Package 3" in data["all_package_names"]
    # Revalidation, and a stale version is served short-lived (never pinned for a year).
    assert client.get(url, headers={"If-None-Match": etag}).status_code == 304
    stale = client.get("/api/listings/facets?v=0000000000000000")
    assert stale.status_code == 200
    assert stale.headers["Cache-Control"] == "public, max-age=60"
    assert "Set-Cookie" not in stale.headers


def test_facets_payload_carries_no_per_user_data(fleet, client, monkeypatch):
    import backend.db.inventory_db as inv
    from backend.routes import listings_api

    import backend.auth.session as auth_session

    anon = client.get("/api/listings/facets").get_data()
    # A signed-in shopper with hidden dealerships (the session policy keeps the test session).
    monkeypatch.setattr(auth_session, "_check_app_session", lambda now: None)
    monkeypatch.setattr(inv, "hidden_dealer_ids_for_user", lambda uid: {"near-motors-test"})
    monkeypatch.setattr(listings_api, "hidden_dealer_ids_for_user", lambda uid: {"near-motors-test"}, raising=False)
    with client.session_transaction() as sess:
        sess["user_id"] = 4242
        sess["username"] = "someone"
    signed_in = client.get("/api/listings/facets")
    assert signed_in.get_data() == anon
    # A cookie-bearing request is never told "public": its response may carry a refreshed cookie.
    assert signed_in.headers["Cache-Control"].startswith("private")
    assert "public" not in signed_in.headers["Cache-Control"]
    assert "Set-Cookie" not in signed_in.headers
    assert "Cookie" not in signed_in.headers.get("Vary", "")
    assert b"4242" not in signed_in.get_data()


def test_version_moves_when_the_facets_change(fleet, client):
    from backend.db.repositories import listings_repo as lr

    v1 = _facets_url(client.get("/listings").get_data(as_text=True))
    fleet.add_cars([{
        "vin": "5YJ3E1EA7KF000001",
        "title": "2023 Lucid Air", "year": 2023, "make": "Lucid", "model": "Air", "price": 70000,
        "dealer_name": "near-motors", "dealer_url": "https://near-motors.test", "dealer_id": "near-motors-test",
        "dealership_registry_id": 1,
    }])
    lr.clear_inventory_listings_cache()
    v2 = _facets_url(client.get("/listings").get_data(as_text=True))
    assert v1 != v2
    assert "Lucid" in _json(client.get(v2))["makes"]


# ── server-rendered page of cards ───────────────────────────────────────


def _warm_area(client, zip_code="92694", radius="50"):
    """Cache the area body the way a shopper's client does. Off Postgres the data token is
    the database file's mtime and the first build writes the card store, so the first body
    is born stale; the second request builds the one that stays current."""
    for _ in range(2):
        r = client.get(f"/api/listings/cars?zip={zip_code}&radius={radius}")
        assert r.status_code == 200
    return [c["id"] for c in _json(r)["cars"]]


def test_page_two_differs_from_page_one_and_follows_the_api_order(fleet, client):
    api_ids = _warm_area(client)
    assert len(api_ids) == N_CARS
    p1 = client.get("/listings?zip_code=92694&radius=50").get_data(as_text=True)
    p2 = client.get("/listings?zip_code=92694&radius=50&page=2").get_data(as_text=True)
    ids1, ids2 = _card_ids(p1), _card_ids(p2)
    assert ids1 == api_ids[:24]
    assert ids2 == api_ids[24:48]
    assert p1 != p2
    # Crawlable plain links, rel=prev/next, in the head and in the pager.
    assert '<link rel="next" href="/listings?zip_code=92694&amp;radius=50&amp;page=2">' in p1
    assert 'rel="prev"' not in p1
    assert '<link rel="prev" href="/listings?zip_code=92694&amp;radius=50">' in p2
    assert 'rel="next" href="/listings?zip_code=92694&amp;radius=50&amp;page=3"' in p2
    assert "25–48 of 60" in p2
    # The chip row main.js fills is drawn too, so the cards do not shift down (CLS).
    assert 'data-chip-param="zip_code" data-chip-value="92694"' in p1
    assert 'id="listings-active-chips-wrap" hidden' not in p1
    # Last page clamps like the client pager.
    p9 = client.get("/listings?zip_code=92694&radius=50&page=9").get_data(as_text=True)
    assert _card_ids(p9) == api_ids[48:]
    assert 'rel="next"' not in p9


def test_server_page_applies_the_checked_facets_like_the_client(fleet, client):
    _warm_area(client)
    api = _json(client.get("/api/listings/cars?zip=92694&radius=50"))["cars"]
    fords = [c["id"] for c in api if c["make"] == "Ford"]
    html = client.get("/listings?zip_code=92694&radius=50&make=Ford").get_data(as_text=True)
    assert _card_ids(html) == fords[:24]
    # A value with no option checks nothing on the client, so it filters nothing here.
    html = client.get("/listings?zip_code=92694&radius=50&make=ford").get_data(as_text=True)
    assert _card_ids(html) == [c["id"] for c in api][:24]
    cheap = [c["id"] for c in api if c["body_style"] == "Sedan" and c["price"] <= 25000]
    html = client.get("/listings?zip_code=92694&radius=50&body_style=Sedan&max_price=25000").get_data(as_text=True)
    assert _card_ids(html) == cheap[:24]


def test_filters_the_index_cannot_reproduce_leave_the_grid_to_the_client(fleet, client):
    _warm_area(client)
    for qs in ("exterior_color=black", "package=Package+1", "sort=price_desc", "inventory_condition=new"):
        html = client.get(f"/listings?zip_code=92694&radius=50&{qs}").get_data(as_text=True)
        assert _card_ids(html) == [], qs
        assert 'data-search-started="1"' in html


def test_no_zip_keeps_the_start_prompt_and_no_cards(fleet, client):
    _warm_area(client)
    for path in ("/listings", "/listings?page=2"):
        html = client.get(path).get_data(as_text=True)
        assert _card_ids(html) == []
        assert 'data-search-started="0"' in html
        assert 'id="listings-start-prompt"' in html
        assert 'rel="next"' not in html


def test_cold_area_never_builds_on_the_document_request(fleet, client, monkeypatch):
    """TTFB must not grow with the inventory: an uncached area renders without cards and
    the document never runs the area build itself."""
    from backend.db.repositories import grid_cards_repo as gc
    from backend.routes import listings_api

    def boom(*_a, **_k):
        raise AssertionError("area built on the /listings request")

    monkeypatch.setattr(gc, "cards_near", boom)
    kicked = []
    monkeypatch.setattr(listings_api, "_kick_scope_rebuild", lambda *a, **k: kicked.append(a[0]) or True)
    html = client.get("/listings?zip_code=92694&radius=50").get_data(as_text=True)
    assert _card_ids(html) == []
    assert 'data-search-started="1"' in html
    assert kicked == []  # off Postgres the client's own request builds it

    # On Postgres one background build is started for the client to join.
    monkeypatch.setattr(listings_api, "is_inventory_postgres", lambda: True)
    client.get("/listings?zip_code=92694&radius=50")
    assert kicked == [("92694", 50.0, ())]


def test_ssr_index_is_compact_and_counted_in_the_cache_budget(fleet, client):
    from backend.routes import listings_api

    _warm_area(client)
    entry = listings_api._cars_scope_get(("92694", 50.0, ()))
    assert entry is not None and entry.ssr_index is not None
    assert len(entry.ssr_index) == N_CARS
    assert 0 < entry.ssr_index.nbytes < 200 * N_CARS
    assert listings_api._cars_scope_bytes == len(entry.gz_body) + entry.ssr_index.nbytes
