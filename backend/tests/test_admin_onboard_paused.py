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
