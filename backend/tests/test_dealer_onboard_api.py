"""Tests for site-admin dealer onboard API (locator → scrape bridge)."""

from __future__ import annotations

import json

from backend.dealer.admin.onboard_api import request_dealer_onboard
from backend.db.users_db import init_users_db, save_user
from backend.main import app


def test_request_dealer_onboard_requires_postgres(monkeypatch) -> None:
    monkeypatch.setattr(
        "backend.dealer.admin.onboard_api.is_inventory_postgres",
        lambda: False,
    )
    ok, err, _data = request_dealer_onboard(url="https://www.example-dealer.com")
    assert ok is False
    assert err == "postgres_required"


def test_request_dealer_onboard_invalid_url(monkeypatch) -> None:
    monkeypatch.setattr(
        "backend.dealer.admin.onboard_api.is_inventory_postgres",
        lambda: True,
    )
    ok, err, _data = request_dealer_onboard(url="javascript:alert(1)")
    assert ok is False
    assert err == "invalid_url"


def test_request_dealer_onboard_enqueues(monkeypatch) -> None:
    monkeypatch.setattr(
        "backend.dealer.admin.onboard_api.is_inventory_postgres",
        lambda: True,
    )
    monkeypatch.setattr(
        "backend.dealer.admin.onboard_api.enqueue_job",
        lambda **kwargs: 42,
    )
    ok, err, data = request_dealer_onboard(
        url="https://www.mtnviewnissan.com/",
        name="Mtn View Nissan",
    )
    assert ok is True
    assert err == ""
    assert data["job_id"] == 42
    assert data["dealer_id"] == "mtnviewnissan-com"


def test_api_admin_dealer_onboard_forbidden_without_admin(monkeypatch) -> None:
    monkeypatch.setattr(
        "backend.dealer.admin.onboard_api.is_inventory_postgres",
        lambda: True,
    )
    client = app.test_client()
    rv = client.post(
        "/api/admin/dealer-onboard",
        data=json.dumps({"url": "https://www.example.com"}),
        content_type="application/json",
    )
    assert rv.status_code == 403


def test_api_admin_dealer_onboard_ok(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "users.db"
    monkeypatch.setenv("USERS_DB_PATH", str(db_path))
    monkeypatch.delenv("INVENTORY_DATABASE_URL", raising=False)
    init_users_db()
    uid = save_user("admin1", "admin1@example.com", "password123", role="admin")
    monkeypatch.setattr(
        "backend.dealer.admin.onboard_api.is_inventory_postgres",
        lambda: True,
    )
    monkeypatch.setattr(
        "backend.dealer.admin.onboard_api.enqueue_job",
        lambda **kwargs: 99,
    )

    client = app.test_client()
    with client.session_transaction() as sess:
        sess["user_id"] = uid
        sess["user_role"] = "admin"
        sess["username"] = "admin1"
        sess["_csrf_token"] = "test-csrf-token-for-onboard-api-32chars"

    rv = client.post(
        "/api/admin/dealer-onboard",
        data=json.dumps({"url": "https://www.example-dealer.com/inventory"}),
        content_type="application/json",
        headers={"X-CSRF-Token": "test-csrf-token-for-onboard-api-32chars"},
    )
    assert rv.status_code == 200
    data = rv.get_json()
    assert data["ok"] is True
    assert data["job_id"] == 99


def test_api_admin_dealer_jobs_forbidden_without_admin(monkeypatch) -> None:
    monkeypatch.setattr(
        "backend.db.inventory_pg.is_inventory_postgres",
        lambda: True,
    )
    client = app.test_client()
    rv = client.get("/api/admin/dealer-jobs")
    assert rv.status_code == 403


def test_api_admin_dealer_jobs_ok(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "users.db"
    monkeypatch.setenv("USERS_DB_PATH", str(db_path))
    init_users_db()
    uid = save_user("admin1", "admin1@example.com", "password123", role="admin")
    monkeypatch.setattr("backend.db.inventory_pg.is_inventory_postgres", lambda: True)
    monkeypatch.setattr(
        "backend.scanner.job_queue.list_recent_jobs",
        lambda **kwargs: [
            {"id": 1, "dealer_id": "test-dealer", "job_type": "onboard", "status": "running"}
        ],
    )
    monkeypatch.setattr(
        "backend.scanner.job_queue.list_dealer_catalog",
        lambda **kwargs: [],
    )

    client = app.test_client()
    with client.session_transaction() as sess:
        sess["user_id"] = uid
        sess["user_role"] = "admin"

    rv = client.get("/api/admin/dealer-jobs")
    assert rv.status_code == 200
    data = rv.get_json()
    assert data["ok"] is True
    assert len(data["jobs"]) == 1
    assert data["active_count"] == 1


def test_api_admin_dealer_job_detail_ok(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "users.db"
    monkeypatch.setenv("USERS_DB_PATH", str(db_path))
    init_users_db()
    uid = save_user("admin1", "admin1@example.com", "password123", role="admin")
    monkeypatch.setattr("backend.db.inventory_pg.is_inventory_postgres", lambda: True)
    monkeypatch.setattr(
        "backend.scanner.job_queue.get_job",
        lambda jid: {
            "id": jid,
            "dealer_id": "test-dealer",
            "job_type": "onboard",
            "status": "failed",
            "worker_id": "worker-1",
            "created_at": "2026-01-01T00:00:00+00:00",
            "started_at": "2026-01-01T00:01:00+00:00",
            "finished_at": "2026-01-01T00:02:00+00:00",
            "error": "exit_1",
            "payload_json": '{"url": "https://example.com"}',
            "result_json": '{"log_tail": "MODULE_NOT_FOUND"}',
        },
    )

    client = app.test_client()
    with client.session_transaction() as sess:
        sess["user_id"] = uid
        sess["user_role"] = "admin"

    rv = client.get("/api/admin/dealer-jobs/7")
    assert rv.status_code == 200
    data = rv.get_json()
    assert data["ok"] is True
    assert data["job"]["payload"]["url"] == "https://example.com"
    assert data["job"]["result"]["log_tail"] == "MODULE_NOT_FOUND"


def test_api_admin_dealer_job_retry_ok(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "users.db"
    monkeypatch.setenv("USERS_DB_PATH", str(db_path))
    init_users_db()
    uid = save_user("admin1", "admin1@example.com", "password123", role="admin")
    monkeypatch.setattr(
        "backend.scanner.job_queue.retry_failed_job",
        lambda jid: (True, "", {"job_id": 99, "source_job_id": jid, "dealer_id": "test-dealer"}),
    )

    client = app.test_client()
    with client.session_transaction() as sess:
        sess["user_id"] = uid
        sess["user_role"] = "admin"
        sess["_csrf_token"] = "test-csrf-token-for-onboard-api-32chars"

    rv = client.post(
        "/api/admin/dealer-jobs/5/retry",
        data="{}",
        content_type="application/json",
        headers={"X-CSRF-Token": "test-csrf-token-for-onboard-api-32chars"},
    )
    assert rv.status_code == 200
    data = rv.get_json()
    assert data["ok"] is True
    assert data["job_id"] == 99
    assert data["source_job_id"] == 5
