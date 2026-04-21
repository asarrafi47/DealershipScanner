"""Store admin: merchandising rules, SQL scope, CSV row helper, scan_runs insert."""

from __future__ import annotations

import json

import pytest

from backend.dealer_admin import inventory_queries as invq
from backend.dealer_admin.merchandising import (
    customer_ready_codes,
    https_gallery_count,
    merchandising_issue_codes,
)


def test_merchandising_no_price():
    row = {"price": 0, "gallery": "[]", "trim": "x", "exterior_color": "b", "interior_color": "c"}
    assert "price_zero" in merchandising_issue_codes(row)


def test_merchandising_missing_packages_with_description():
    row = {
        "price": 25000,
        "description": "Loaded with options",
        "packages": None,
        "gallery": json.dumps(["https://example.com/a.jpg", "https://example.com/b.jpg", "https://example.com/c.jpg"]),
        "trim": "t",
        "exterior_color": "b",
        "interior_color": "c",
    }
    assert "missing_packages_with_description" in merchandising_issue_codes(row)


def test_https_gallery_count_string():
    row = {"gallery": json.dumps(["https://a/x.jpg", "http://ignored", "https://b/y.jpg"])}
    assert https_gallery_count(row) == 2


def test_customer_ready_all_true(monkeypatch):
    monkeypatch.setenv("STORE_ADMIN_STALE_PRICE_DAYS", "9999")
    row = {
        "price": 20000,
        "trim": "Sport",
        "exterior_color": "Black",
        "interior_color": "Black",
        "description": "",
        "packages": None,
        "gallery": json.dumps(["https://a/1.jpg", "https://a/2.jpg", "https://a/3.jpg"]),
        "last_price_change_at": "2099-01-01T00:00:00+00:00",
    }
    c = customer_ready_codes(row)
    assert c["has_price"] and c["has_photos"] and c["not_stale_price"]


def test_scope_admin_sees_all():
    sql, bind = invq._scope_sql({"role": "admin"})
    assert sql == ""
    assert bind == ()


def test_scope_dealer_staff_requires_predicate():
    sql, bind = invq._scope_sql({"role": "dealer_staff", "dealer_id": "", "dealership_registry_id": None})
    assert sql == "0"


def test_scope_dealer_staff_by_id():
    sql, bind = invq._scope_sql({"role": "dealer_staff", "dealer_id": "bmw-clt", "dealership_registry_id": None})
    assert "dealer_id" in sql
    assert bind == ("bmw-clt",)


@pytest.fixture()
def isolated_inventory_db(monkeypatch, tmp_path):
    import backend.db.inventory_db as inv

    p = str(tmp_path / "inv_sa.db")
    monkeypatch.setattr(inv, "DB_PATH", p)
    inv.init_inventory_db()
    conn = inv.get_conn()
    conn.execute("DELETE FROM cars")
    conn.commit()
    conn.execute(
        """
        INSERT INTO cars (
            vin, title, year, make, model, trim, price, mileage,
            image_url, dealer_name, dealer_url, dealer_id, scraped_at,
            zip_code, stock_number, gallery, listing_active, first_seen_at, last_price_change_at
        ) VALUES (
            '1HGBH41JXMN109186', 'Test', 2020, 'Honda', 'Accord', 'EX', 24000, 1000,
            'https://images.unsplash.com/photo-1?w=10', 'D', 'https://dealer.example', 'dealer-a',
            datetime('now'), '28202', 'S1',
            '["https://x/a.jpg","https://x/b.jpg","https://x/c.jpg"]', 1,
            datetime('now', '-60 days'), datetime('now', '-60 days')
        )
        """
    )
    conn.commit()
    conn.close()
    return p


def _car_id_for_vin(vin: str) -> int:
    import backend.db.inventory_db as inv

    conn = inv.get_conn()
    row = conn.execute("SELECT id FROM cars WHERE vin = ?", (vin,)).fetchone()
    conn.close()
    assert row
    return int(row[0])


def test_car_visible_admin(isolated_inventory_db):
    cid = _car_id_for_vin("1HGBH41JXMN109186")
    assert invq.car_visible_to_profile({"role": "admin"}, cid)


def test_car_visible_wrong_dealer(isolated_inventory_db):
    cid = _car_id_for_vin("1HGBH41JXMN109186")
    assert not invq.car_visible_to_profile({"role": "dealer_staff", "dealer_id": "other"}, cid)


def test_record_scan_outcomes(monkeypatch, tmp_path):
    import backend.db.inventory_db as inv

    monkeypatch.setattr(inv, "DB_PATH", str(tmp_path / "scan.db"))
    inv.init_inventory_db()
    n = inv.record_scan_outcomes(
        [
            {
                "dealer_id": "d1",
                "dealer_name": "One",
                "seconds": 12.5,
                "upserted": 3,
                "inventory_rows": 10,
                "deduped_rows": 9,
                "vdps_visited": 2,
                "vehicles_vdp_enriched": 1,
                "reconcile": {"ran": True, "marked_inactive": 0},
            }
        ],
        finished_at="2026-04-20T00:00:00+00:00",
    )
    assert n == 1
    rows = inv.list_scan_runs(limit=5)
    assert len(rows) == 1
    assert rows[0]["dealer_id"] == "d1"


def test_export_inventory_headers(monkeypatch, tmp_path):
    import backend.db.inventory_db as inv

    monkeypatch.setattr(inv, "DB_PATH", str(tmp_path / "ex.db"))
    inv.init_inventory_db()
    conn = inv.get_conn()
    conn.execute("DELETE FROM cars")
    conn.commit()
    conn.execute(
        """
        INSERT INTO cars (
            vin, title, year, make, model, dealer_id, scraped_at, stock_number, gallery, listing_active, price
        ) VALUES (
            '1HGBH41JXMN109187', 'T', 2021, 'Honda', 'Civic', 'd-exp',
            datetime('now'), 'x', '[]', 1, 20000
        )
        """
    )
    conn.commit()
    conn.close()
    rows = invq.export_inventory_rows({"role": "admin"}, limit=10)
    assert len(rows) == 1
    assert rows[0]["vin"] == "1HGBH41JXMN109187"


def test_admin_anonymous_redirects_to_login():
    from backend.main import app

    rv = app.test_client().get("/admin/", follow_redirects=False)
    assert rv.status_code == 302
    assert "/login" in (rv.headers.get("Location") or "")
