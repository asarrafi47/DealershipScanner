"""``/listings?q=...`` must never embed an unbounded result set.

Efficiency review 2026-09-28, finding D1: with pgvector unconfigured the free-text
path fell back to ``search_cars(**sql_kwargs)`` where ``sql_kwargs`` were the
(empty) GET facets, so the page embedded the whole fleet (214k cars, 237 MB, 37 s).
The fix merges the parsed filters into the SQL fallback, refuses a fallback that
constrains nothing, caps the embedded grid and gives ``search_cars`` a hard limit.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

_MAKES = ("Honda", "Toyota", "Ford")


def _fresh_app(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("USERS_DB_PATH", str(tmp_path / "users.db"))
    monkeypatch.setenv("INVENTORY_DB_PATH", str(tmp_path / "inventory.db"))
    monkeypatch.setenv("BILLING_STRIPE_ENABLED", "0")
    monkeypatch.setenv("DATABASE_URL", "")
    monkeypatch.setenv("INVENTORY_DATABASE_URL", "")
    monkeypatch.setattr("backend.db.inventory_pg.is_inventory_postgres", lambda: False)
    from backend.db import inventory_db
    from backend.db.inventory_db import init_inventory_db
    from backend.db.users_db import init_users_db
    from backend.main import app

    monkeypatch.setattr(inventory_db, "DB_PATH", str(tmp_path / "inventory.db"))
    init_users_db()
    init_inventory_db()
    app.config["TESTING"] = True
    return app


def _seed_cars(n: int) -> None:
    from backend.db.inventory_db import get_conn

    conn = get_conn()
    try:
        cur = conn.cursor()
        for i in range(n):
            make = _MAKES[i % len(_MAKES)]
            cur.execute(
                """
                INSERT INTO cars (
                    vin, title, year, make, model, trim, price, mileage,
                    image_url, dealer_name, dealer_url, dealer_id, scraped_at,
                    fuel_type, transmission, drivetrain, condition,
                    exterior_color, interior_color, gallery, listing_active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"1HGBH41JXMN{i:06d}", f"2022 {make} Model {i}", 2022, make, "Model",
                    "Sport", 20000 + i, 12000, "https://example.com/car.jpg",
                    "Test Dealer", "https://t-dealer.test/", "t-dealer",
                    "2026-01-01T00:00:00Z", "Gas", "Automatic", "FWD", "Used",
                    "Blue", "Black", '["https://example.com/car.jpg"]', 1,
                ),
            )
        conn.commit()
    finally:
        conn.close()


def _initial_grid(html: str) -> list[dict]:
    m = re.search(
        r'<script type="application/json" id="ds-listings-initial-grid"[^>]*>(.*?)</script>',
        html,
        re.S,
    )
    assert m, "initial grid blob missing"
    return json.loads(m.group(1))


@pytest.fixture
def env(monkeypatch, tmp_path):
    app = _fresh_app(monkeypatch, tmp_path)
    _seed_cars(30)
    # /listings?q= is ZIP + radius scoped since 2026-09-28 (no ZIP -> no search; see
    # test_listings_scoped_grid). These fixture cars have no dealer location, so the
    # requests below carry a ZIP and the real search runs with the geo kwargs dropped:
    # what is under test here is the bound, not the radius.
    import backend.listings.routes as routes

    real = routes.hybrid_search_with_kwargs

    def unscoped(q, kwargs, **kw):
        kwargs = {k: v for k, v in kwargs.items() if k not in ("zip_code", "radius_miles")}
        return real(q, kwargs, **kw)

    monkeypatch.setattr(routes, "hybrid_search_with_kwargs", unscoped)
    return app


def test_listings_q_without_pgvector_is_bounded_and_filtered(env, monkeypatch):
    """No semantic index -> parsed filters run as SQL, with a limit, and the grid is capped."""
    import backend.utils.hybrid_search as hs
    from backend.listings.routes import LISTINGS_SEARCH_GRID_MAX

    monkeypatch.setattr(hs, "_semantic_car_ids", lambda q, n: [])
    calls: list[dict] = []
    real = hs.search_cars

    def spy(**kw):
        calls.append(kw)
        return real(**kw)

    monkeypatch.setattr(hs, "search_cars", spy)

    rv = env.test_client().get("/listings?q=used+toyota+under+30k&zip_code=28202&radius=50")
    assert rv.status_code == 200
    grid = _initial_grid(rv.get_data(as_text=True))
    assert 0 < len(grid) < 500
    assert len(grid) <= LISTINGS_SEARCH_GRID_MAX
    assert {c["make"] for c in grid} == {"Toyota"}
    assert calls, "search_cars was never called"
    kw = calls[-1]
    assert kw.get("limit") == LISTINGS_SEARCH_GRID_MAX
    assert [m.lower() for m in kw.get("makes") or []] == ["toyota"]
    assert kw.get("max_price") == 30000


def test_listings_q_grid_is_capped_at_the_constant(env, monkeypatch):
    import backend.listings.routes as routes
    import backend.utils.hybrid_search as hs

    monkeypatch.setattr(hs, "_semantic_car_ids", lambda q, n: [])
    monkeypatch.setattr(routes, "LISTINGS_SEARCH_GRID_MAX", 4)
    rv = env.test_client().get("/listings?q=honda&zip_code=28202&radius=50")
    assert rv.status_code == 200
    grid = _initial_grid(rv.get_data(as_text=True))
    assert len(grid) == 4


def test_sql_fallback_refuses_unfiltered_run(env, monkeypatch):
    """A parse that maps to no SQL constraint must not fall back to the whole fleet."""
    import backend.utils.hybrid_search as hs

    monkeypatch.setattr(hs, "_semantic_car_ids", lambda q, n: [])
    monkeypatch.setattr(hs, "_has_structured_filters", lambda f: True)
    monkeypatch.setattr(hs, "search_cars", lambda **kw: pytest.fail("fleet-wide search_cars"))
    rows, meta = hs.hybrid_search_with_kwargs("anything", {}, parsed_filters={"fully_loaded": True})
    assert rows == []
    assert meta["mode"] == "sql_fallback_unfiltered_refused"


def test_search_cars_limit_default_and_opt_out(env):
    from backend.db.repositories import search_repo

    rows = search_repo.search_cars(makes=["Honda"], limit=3)
    assert len(rows) == 3
    # Cheapest first, like the unbounded sort.
    assert [r["price"] for r in rows] == sorted(r["price"] for r in rows)

    assert len(search_repo.search_cars(makes=["Honda"], limit=None)) == 10
    assert len(search_repo.search_cars(limit=None)) == 30
    assert len(search_repo.search_cars(limit=0)) == 30  # 0 is not "unbounded": default applies
    assert search_repo.SEARCH_CARS_DEFAULT_LIMIT >= 30

    # Post-SQL filters keep collecting past the first chunk until the cap is met.
    rows = search_repo.search_cars(exterior_colors=["blue"], limit=7)
    assert len(rows) == 7
