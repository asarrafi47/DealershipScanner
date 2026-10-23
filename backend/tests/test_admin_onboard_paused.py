"""N3.10: the admin "onboard dealer" POST on /admin/dealers is paused.

No worker has consumed ``dealer_jobs`` since the worker and scheduler services were
deleted on 2026-10-08, so the hub must answer 409 and enqueue nothing until P15A.3
retires the queue path. The enqueue check runs the real ``enqueue_job`` against a
fake Postgres connection that records every ``dealer_jobs`` INSERT, so a call from
any code path (including a name imported before the patch) shows up as a row.
"""

from __future__ import annotations

from typing import Any

import pytest

from backend.db.users_db import init_users_db, save_user
from backend.utils.roles import ROLE_ADMIN

PAUSED_LINE = "Onboarding is paused; use dealer-discovery (see P15A.3)"


class _RecordingCursor:
    def __init__(self, rows: list[tuple[Any, ...]]):
        self._rows = rows
        self._last: tuple[int] | None = None

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> None:
        if "insert into dealer_jobs" in " ".join(sql.lower().split()):
            self._rows.append(tuple(params))
            self._last = (len(self._rows),)

    def fetchone(self) -> tuple[int] | None:
        return self._last

    def close(self) -> None:
        pass


class _RecordingConn:
    def __init__(self, rows: list[tuple[Any, ...]]):
        self._rows = rows

    def cursor(self) -> _RecordingCursor:
        return _RecordingCursor(self._rows)

    def commit(self) -> None:
        pass

    def close(self) -> None:
        pass


@pytest.fixture
def queue_rows(monkeypatch) -> list[tuple[Any, ...]]:
    """Rows the job queue would have written to ``dealer_jobs``."""
    import backend.scanner.job_queue as jq

    rows: list[tuple[Any, ...]] = []
    monkeypatch.setattr(jq, "pg_connect", lambda *a, **k: _RecordingConn(rows))
    return rows


def _make_app(app_factory, monkeypatch, tmp_path):
    from backend.db import inventory_db as inv_db

    monkeypatch.setattr(inv_db, "DB_PATH", str(tmp_path / "inv_test.db"))
    return app_factory(INVENTORY_DB_PATH=str(tmp_path / "inv_test.db")).app


def _login(client, *, email: str, role: str) -> str:
    init_users_db()
    save_user(
        username=email.split("@", 1)[0],
        email=email,
        password="SecurePass123!",
        role=role,
    )
    client.get("/login")
    from flask import session

    csrf = session.get("_csrf_token")
    client.post(
        "/login",
        data={"csrf_token": csrf, "login": email, "password": "SecurePass123!"},
        follow_redirects=True,
    )
    return session.get("_csrf_token")


def _pretend_postgres(monkeypatch) -> None:
    """Make the old code path reachable: with Postgres on, the pre-N3.10 hub enqueued."""
    monkeypatch.setattr("backend.db.inventory_pg.is_inventory_postgres", lambda: True)
    monkeypatch.setattr("backend.dealer.admin.dealers_hub.list_dealer_catalog", lambda limit=50: [])
    monkeypatch.setattr("backend.dealer.admin.dealers_hub.list_recent_jobs", lambda limit=30: [])


def test_queue_fake_records_a_real_enqueue(monkeypatch, queue_rows) -> None:
    """Guard for the fake itself: a direct enqueue_job call lands as one row."""
    from backend.scanner.job_queue import enqueue_job

    monkeypatch.setattr("backend.db.inventory_pg.is_inventory_postgres", lambda: True)
    job_id = enqueue_job(dealer_id="probe-dealer", job_type="onboard", payload={"url": "https://x.test"})
    assert job_id == 1
    assert len(queue_rows) == 1


def test_admin_onboard_post_returns_409_and_enqueues_nothing(monkeypatch, tmp_path, app_factory, queue_rows) -> None:
    app = _make_app(app_factory, monkeypatch, tmp_path)
    client = app.test_client()
    with client:
        csrf = _login(client, email="siteadmin@example.com", role=ROLE_ADMIN)
        _pretend_postgres(monkeypatch)
        rv = client.post(
            "/admin/dealers",
            data={
                "csrf_token": csrf,
                "dealer_id": "irvine-bmw",
                "url": "https://www.irvinebmw.example/inventory",
            },
            follow_redirects=False,
        )
    assert rv.status_code == 409
    assert rv.mimetype == "text/plain"
    assert rv.get_data(as_text=True).strip() == PAUSED_LINE
    assert queue_rows == []


def test_admin_onboard_post_without_fields_is_still_409(monkeypatch, tmp_path, app_factory, queue_rows) -> None:
    app = _make_app(app_factory, monkeypatch, tmp_path)
    client = app.test_client()
    with client:
        csrf = _login(client, email="siteadmin@example.com", role=ROLE_ADMIN)
        rv = client.post("/admin/dealers", data={"csrf_token": csrf}, follow_redirects=False)
    assert rv.status_code == 409
    assert queue_rows == []


def test_non_admin_onboard_post_redirects_and_enqueues_nothing(monkeypatch, tmp_path, app_factory, queue_rows) -> None:
    app = _make_app(app_factory, monkeypatch, tmp_path)
    client = app.test_client()
    with client:
        csrf = _login(client, email="member@example.com", role="general_user")
        _pretend_postgres(monkeypatch)
        rv = client.post(
            "/admin/dealers",
            data={"csrf_token": csrf, "dealer_id": "irvine-bmw", "url": "https://x.test/inv"},
            follow_redirects=False,
        )
    assert rv.status_code == 302
    assert queue_rows == []


def test_admin_onboard_post_without_csrf_is_refused(monkeypatch, tmp_path, app_factory, queue_rows) -> None:
    app = _make_app(app_factory, monkeypatch, tmp_path)
    client = app.test_client()
    with client:
        _login(client, email="siteadmin@example.com", role=ROLE_ADMIN)
        rv = client.post(
            "/admin/dealers",
            data={"dealer_id": "irvine-bmw", "url": "https://x.test/inv"},
            follow_redirects=False,
        )
    assert rv.status_code == 403
    assert queue_rows == []


@pytest.mark.parametrize("postgres_on", [True, False])
def test_hub_page_shows_paused_line_and_no_onboard_form(monkeypatch, tmp_path, app_factory, postgres_on) -> None:
    app = _make_app(app_factory, monkeypatch, tmp_path)
    client = app.test_client()
    with client:
        _login(client, email="siteadmin@example.com", role=ROLE_ADMIN)
        if postgres_on:
            _pretend_postgres(monkeypatch)
        rv = client.get("/admin/dealers")
    assert rv.status_code == 200
    html = rv.get_data(as_text=True)
    assert f'<p id="dealers-onboarding-paused">{PAUSED_LINE}</p>' in html
    assert 'name="dealer_id"' not in html
    assert "Queue onboard job" not in html
    assert '<form method="post" class="dealers-form"' not in html


# --- N3.10b: every other web path that would insert a dealer_jobs row -------------
#
# Find dealers "Request scrape" (POST /api/admin/dealer-onboard) and the hub's
# Restart / Smart restart (POST /api/admin/dealer-jobs/<id>/retry, /smart-retry)
# answer 409 with the same line and reach nothing in the job queue.

ONBOARD_URL = "/api/admin/dealer-onboard"
RETRY_URL = "/api/admin/dealer-jobs/5/retry"
SMART_RETRY_URL = "/api/admin/dealer-jobs/5/smart-retry"

FAILED_JOB = {
    "id": 5,
    "dealer_id": "irvine-bmw",
    "job_type": "onboard",
    "status": "failed",
    "worker_id": "worker-1",
    "created_at": "2026-10-01T00:00:00+00:00",
    "started_at": "2026-10-01T00:01:00+00:00",
    "finished_at": "2026-10-01T00:02:00+00:00",
    "error": "boom",
    "payload_json": '{"url": "https://www.irvinebmw.example/inventory"}',
    "result_json": "{}",
}


def _pretend_failed_job(monkeypatch) -> None:
    """Make every pre-N3.10b queue path reach ``enqueue_job``.

    Postgres on, job 5 is a failed onboard with nothing active for its dealer, and
    the diagnosis stays rule-only (no LLM, no ``dealer_scan_status`` read). Only
    the lookups are faked: ``enqueue_job`` itself is real and writes through the
    recording connection.
    """
    import backend.scanner.job_queue as jq

    monkeypatch.setattr("backend.db.inventory_pg.is_inventory_postgres", lambda: True)
    monkeypatch.setattr(jq, "get_job", lambda jid: dict(FAILED_JOB, id=int(jid)))
    monkeypatch.setattr(jq, "_has_active_job", lambda **k: False)
    monkeypatch.setattr(jq, "get_dealer_scan_reason", lambda dealer_id: None)
    monkeypatch.setattr(jq, "_merge_result_diagnosis", lambda job_id, diagnosis: None)


def _json_post(client, url: str, csrf: str | None, body: dict[str, Any]):
    headers = {"Accept": "application/json"}
    if csrf:
        headers["X-CSRF-Token"] = csrf
    return client.post(url, json=body, headers=headers)


_PAUSED_POSTS = [
    pytest.param(ONBOARD_URL, {"url": "https://www.mtnviewnissan.com/", "name": "Mtn View Nissan"}, id="onboard"),
    pytest.param(RETRY_URL, {}, id="retry"),
    pytest.param(SMART_RETRY_URL, {"use_llm": False}, id="smart-retry"),
]


def test_queue_fake_records_the_pre_pause_api_paths(monkeypatch, queue_rows) -> None:
    """Guard for the fakes: the functions the three endpoints used to call each land one row."""
    from backend.dealer.admin.onboard_api import request_dealer_onboard
    from backend.scanner.job_queue import retry_failed_job, smart_retry_failed_job

    _pretend_failed_job(monkeypatch)
    assert request_dealer_onboard(url="https://www.mtnviewnissan.com/")[0] is True
    assert retry_failed_job(5)[0] is True
    assert smart_retry_failed_job(5, use_llm=False)[0] is True
    assert len(queue_rows) == 3


@pytest.mark.parametrize(("url", "body"), _PAUSED_POSTS)
def test_admin_queue_post_returns_409_and_enqueues_nothing(
    monkeypatch, tmp_path, app_factory, queue_rows, url, body
) -> None:
    app = _make_app(app_factory, monkeypatch, tmp_path)
    client = app.test_client()
    with client:
        csrf = _login(client, email="siteadmin@example.com", role=ROLE_ADMIN)
        _pretend_failed_job(monkeypatch)
        rv = _json_post(client, url, csrf, body)
    assert rv.status_code == 409
    assert rv.get_json() == {"ok": False, "error": "onboarding_paused", "message": PAUSED_LINE}
    assert queue_rows == []


@pytest.mark.parametrize(("url", "body"), _PAUSED_POSTS)
def test_admin_queue_post_never_calls_the_queue_functions(
    monkeypatch, tmp_path, app_factory, queue_rows, url, body
) -> None:
    """The 409 comes before validation, rate limiting and diagnosis, not from them."""
    import backend.dealer.admin.onboard_api as onboard_api
    import backend.scanner.job_queue as jq

    calls: list[str] = []

    def _forbid(name: str):
        def _called(*a, **k):
            calls.append(name)
            raise AssertionError(f"{name} reached while onboarding is paused")

        return _called

    app = _make_app(app_factory, monkeypatch, tmp_path)
    client = app.test_client()
    with client:
        csrf = _login(client, email="siteadmin@example.com", role=ROLE_ADMIN)
        _pretend_failed_job(monkeypatch)
        monkeypatch.setattr(onboard_api, "request_dealer_onboard", _forbid("request_dealer_onboard"))
        monkeypatch.setattr(jq, "retry_failed_job", _forbid("retry_failed_job"))
        monkeypatch.setattr(jq, "smart_retry_failed_job", _forbid("smart_retry_failed_job"))
        monkeypatch.setattr(jq, "diagnose_job_row", _forbid("diagnose_job_row"))
        monkeypatch.setattr("backend.routes.admin_dealer_api.allow_request", _forbid("allow_request"))
        rv = _json_post(client, url, csrf, body)
    assert rv.status_code == 409
    assert calls == []
    assert queue_rows == []


@pytest.mark.parametrize(("url", "body"), _PAUSED_POSTS)
def test_non_admin_queue_post_is_forbidden_and_enqueues_nothing(
    monkeypatch, tmp_path, app_factory, queue_rows, url, body
) -> None:
    app = _make_app(app_factory, monkeypatch, tmp_path)
    client = app.test_client()
    with client:
        csrf = _login(client, email="member@example.com", role="general_user")
        _pretend_failed_job(monkeypatch)
        rv = _json_post(client, url, csrf, body)
    assert rv.status_code == 403
    assert queue_rows == []


@pytest.mark.parametrize(("url", "body"), _PAUSED_POSTS)
def test_admin_queue_post_without_csrf_is_refused(
    monkeypatch, tmp_path, app_factory, queue_rows, url, body
) -> None:
    app = _make_app(app_factory, monkeypatch, tmp_path)
    client = app.test_client()
    with client:
        _login(client, email="siteadmin@example.com", role=ROLE_ADMIN)
        _pretend_failed_job(monkeypatch)
        rv = _json_post(client, url, None, body)
    assert rv.status_code == 403
    assert queue_rows == []


def test_hub_page_draws_no_restart_buttons_for_a_failed_job(monkeypatch, tmp_path, app_factory) -> None:
    app = _make_app(app_factory, monkeypatch, tmp_path)
    client = app.test_client()
    with client:
        _login(client, email="siteadmin@example.com", role=ROLE_ADMIN)
        _pretend_postgres(monkeypatch)
        monkeypatch.setattr(
            "backend.dealer.admin.dealers_hub.list_recent_jobs",
            lambda limit=30: [dict(FAILED_JOB)],
        )
        rv = client.get("/admin/dealers")
    assert rv.status_code == 200
    html = rv.get_data(as_text=True)
    assert 'class="dealers-job-row" data-job-id="5"' in html
    assert 'class="dealers-job-detail-btn" data-job-id="5"' in html
    assert 'class="dealers-job-retry-btn" data-job-id="5"' not in html
    assert 'class="dealers-job-smart-retry-btn" data-job-id="5"' not in html
    assert 'id="dealers-job-retry-btn"' not in html
    assert 'id="dealers-job-smart-retry-btn"' not in html
    assert "const retryEnabled = false;" in html


def test_find_dealers_shows_paused_line_and_no_request_scrape(monkeypatch, tmp_path, app_factory) -> None:
    import json
    import re

    app = _make_app(app_factory, monkeypatch, tmp_path)
    client = app.test_client()
    with client:
        _login(client, email="siteadmin@example.com", role=ROLE_ADMIN)
        monkeypatch.setattr("backend.db.inventory_pg.is_inventory_postgres", lambda: True)
        rv = client.get("/find-dealers")
    assert rv.status_code == 200
    html = rv.get_data(as_text=True)
    assert f'<p class="find-dealers-hero__admin" id="find-dealers-onboarding-paused">{PAUSED_LINE}</p>' in html
    assert "Request scrape" not in html
    m = re.search(r'<script type="application/json" id="ds-find-dealers-admin"[^>]*>(.*?)</script>', html, re.S)
    assert m, "site-admin config block missing"
    assert json.loads(m.group(1)) == {"site_admin": True, "onboard_enabled": False, "onboard_paused": True}


def test_find_dealers_has_no_paused_line_for_a_member(monkeypatch, tmp_path, app_factory) -> None:
    app = _make_app(app_factory, monkeypatch, tmp_path)
    client = app.test_client()
    with client:
        _login(client, email="member@example.com", role="general_user")
        monkeypatch.setattr("backend.db.inventory_pg.is_inventory_postgres", lambda: True)
        rv = client.get("/find-dealers")
    assert rv.status_code == 200
    html = rv.get_data(as_text=True)
    assert PAUSED_LINE not in html
    assert 'id="ds-find-dealers-admin"' not in html
