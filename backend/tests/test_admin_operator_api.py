"""Site-admin operator API routes (app session, role=admin)."""

from __future__ import annotations

import pytest

from backend.db.users_db import init_users_db, save_user
from backend.utils.roles import ROLE_ADMIN


def _make_app(app_factory, monkeypatch, tmp_path):
    # inventory_db.DB_PATH is resolved once at import time, so the env var has no
    # effect on it post-import — patch the module attribute directly.
    from backend.db import inventory_db as inv_db

    monkeypatch.setattr(inv_db, "DB_PATH", str(tmp_path / "inv_test.db"))
    return app_factory(INVENTORY_DB_PATH=str(tmp_path / "inv_test.db")).app


def _login_admin(client, tmp_path):
    init_users_db()
    save_user(
        username="siteadmin",
        email="siteadmin@example.com",
        password="SecurePass123!",
        role=ROLE_ADMIN,
    )
    client.get("/login")
    from flask import session

    csrf = session.get("_csrf_token")
    client.post(
        "/login",
        data={
            "csrf_token": csrf,
            "login": "siteadmin@example.com",
            "password": "SecurePass123!",
        },
        follow_redirects=True,
    )


def test_operator_incomplete_cars_forbidden_without_admin(monkeypatch, tmp_path, app_factory):
    app = _make_app(app_factory, monkeypatch, tmp_path)
    client = app.test_client()
    rv = client.get("/api/admin/operator/incomplete-cars")
    assert rv.status_code == 403


def test_operator_incomplete_cars_ok_for_site_admin(monkeypatch, tmp_path, app_factory):
    app = _make_app(app_factory, monkeypatch, tmp_path)
    client = app.test_client()
    with client:
        _login_admin(client, tmp_path)
        rv = client.get("/api/admin/operator/incomplete-cars")
    assert rv.status_code == 200
    data = rv.get_json()
    assert data["ok"] is True
    assert "cars" in data
    assert "issues_summary" in data


def test_admin_data_quality_page_requires_site_admin(monkeypatch, tmp_path, app_factory):
    app = _make_app(app_factory, monkeypatch, tmp_path)
    init_users_db()
    save_user(
        username="member",
        email="member@example.com",
        password="SecurePass123!",
        role="general_user",
    )
    client = app.test_client()
    with client:
        client.get("/login")
        from flask import session

        csrf = session.get("_csrf_token")
        client.post(
            "/login",
            data={
                "csrf_token": csrf,
                "login": "member@example.com",
                "password": "SecurePass123!",
            },
            follow_redirects=True,
        )
        rv = client.get("/admin/data-quality", follow_redirects=False)
    assert rv.status_code == 302


def test_admin_data_quality_page_ok_for_site_admin(monkeypatch, tmp_path, app_factory):
    app = _make_app(app_factory, monkeypatch, tmp_path)
    monkeypatch.setattr(
        "backend.dealer.admin.data_quality_hub.get_dealership_issue_stats",
        lambda limit=25: [],
    )
    client = app.test_client()
    with client:
        _login_admin(client, tmp_path)
        rv = client.get("/admin/data-quality")
    assert rv.status_code == 200
    assert b"Incomplete listings" in rv.data
    assert b"Dealerships with issues" in rv.data


def test_admin_data_quality_page_renders_invariants_banner(monkeypatch, tmp_path, app_factory):
    """Item 1/2/6: the nightly invariants report renders with a pass/fail banner,
    a NEW DEFECT CLASS tag, and a PARTIAL-coverage banner when sample_mode is set."""
    app = _make_app(app_factory, monkeypatch, tmp_path)
    monkeypatch.setattr(
        "backend.dealer.admin.data_quality_hub.get_dealership_issue_stats",
        lambda limit=25: [],
    )
    monkeypatch.setattr(
        "backend.dealer.admin.data_quality_hub._load_latest_invariants_report",
        lambda: {
            "found": True, "path": "x", "filename": "invariants_20260901.json",
            "generated_at": "2026-09-01T06:00:00", "elapsed_sec": 12.3,
            "rendered_tier_ran": True, "sample_mode": True, "coverage": "partial",
            "stored": [
                {"id": "a", "tier": "stored", "title": "check a", "count": 5, "baseline": 0,
                 "delta": 5, "status": "NEW_DEFECT_CLASS", "denominator": 100,
                 "detects": "bad thing", "impossible_because": "x",
                 "examples": [{"id": 123, "year": 2020, "make": "Honda", "model": "Civic", "trim": "LX"}],
                 "error": None},
            ],
            "rendered": [],
            "failing_count": 1, "new_defect_count": 1, "new_defect_ids": ["a"],
            "total_count": 1, "overall_ok": False,
        },
    )
    client = app.test_client()
    with client:
        _login_admin(client, tmp_path)
        rv = client.get("/admin/data-quality")
    assert rv.status_code == 200
    body = rv.data.decode()
    assert "New defect class" in body
    assert "PARTIAL COVERAGE" in body
    assert "car 123" in body


def test_admin_data_quality_page_ok_with_no_invariants_report(monkeypatch, tmp_path, app_factory):
    app = _make_app(app_factory, monkeypatch, tmp_path)
    monkeypatch.setattr(
        "backend.dealer.admin.data_quality_hub.get_dealership_issue_stats",
        lambda limit=25: [],
    )
    monkeypatch.setattr(
        "backend.dealer.admin.data_quality_hub._load_latest_invariants_report",
        lambda: {"found": False, "path": None},
    )
    client = app.test_client()
    with client:
        _login_admin(client, tmp_path)
        rv = client.get("/admin/data-quality")
    assert rv.status_code == 200
    assert b"No invariants report found" in rv.data


_ATTRIBUTION_ROWS = [
    {"dealer_id": "bmwofmurrieta-com", "dealer_name": "BMW of Murrieta", "active_cars": 1921,
     "confirmed": 22, "conflicting": 146, "unverified": 1753, "stuck": 1899,
     "resolved_by_move": 0,
     "stuck_why": "names 'Hendrick Porsche' (44); 1753 with no gallery evidence at all"},
    {"dealer_id": "mbontario-com", "dealer_name": "Mercedes-Benz of Ontario", "active_cars": 400,
     "confirmed": 100, "conflicting": 5, "unverified": 10, "stuck": 15,
     "resolved_by_move": 3, "stuck_why": "10 with no gallery evidence at all"},
]


def test_admin_attribution_page_requires_site_admin(monkeypatch, tmp_path, app_factory):
    app = _make_app(app_factory, monkeypatch, tmp_path)
    client = app.test_client()
    with client:
        rv = client.get("/admin/attribution", follow_redirects=False)
    assert rv.status_code == 302


def test_admin_attribution_page_ok_for_site_admin_sorted_by_stuck(monkeypatch, tmp_path, app_factory):
    """Item 5: confirmed/conflicting/unverified/stuck-and-why per group-fed dealer,
    default-sorted worst (stuck) first."""
    app = _make_app(app_factory, monkeypatch, tmp_path)
    monkeypatch.setattr(
        "backend.dealer.admin.attribution_hub.dealer_attribution_resolution",
        lambda: list(_ATTRIBUTION_ROWS),
    )
    client = app.test_client()
    with client:
        _login_admin(client, tmp_path)
        rv = client.get("/admin/attribution")
    assert rv.status_code == 200
    body = rv.data.decode()
    assert "BMW of Murrieta" in body and "Mercedes-Benz of Ontario" in body
    assert "no gallery evidence" in body
    assert body.index("BMW of Murrieta") < body.index("Mercedes-Benz of Ontario")


def test_admin_attribution_page_sortable_by_other_columns(monkeypatch, tmp_path, app_factory):
    app = _make_app(app_factory, monkeypatch, tmp_path)
    monkeypatch.setattr(
        "backend.dealer.admin.attribution_hub.dealer_attribution_resolution",
        lambda: list(_ATTRIBUTION_ROWS),
    )
    client = app.test_client()
    with client:
        _login_admin(client, tmp_path)
        rv = client.get("/admin/attribution?sort=confirmed&dir=asc")
    assert rv.status_code == 200
    body = rv.data.decode()
    assert body.index("BMW of Murrieta") < body.index("Mercedes-Benz of Ontario")


def test_admin_attribution_page_empty_state(monkeypatch, tmp_path, app_factory):
    app = _make_app(app_factory, monkeypatch, tmp_path)
    monkeypatch.setattr(
        "backend.dealer.admin.attribution_hub.dealer_attribution_resolution",
        lambda: [],
    )
    client = app.test_client()
    with client:
        _login_admin(client, tmp_path)
        rv = client.get("/admin/attribution")
    assert rv.status_code == 200
    assert b"No group-fed dealer has a judged car yet" in rv.data


def test_admin_scanner_ops_page_ok_for_site_admin(monkeypatch, tmp_path, app_factory):
    app = _make_app(app_factory, monkeypatch, tmp_path)
    client = app.test_client()
    with client:
        _login_admin(client, tmp_path)
        rv = client.get("/admin/scanner-ops")
    assert rv.status_code == 200
    assert b"Bulk magic URLs" in rv.data
