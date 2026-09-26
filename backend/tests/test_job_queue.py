"""Tests for scanner job queue helpers (A3)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from backend.scanner import job_queue as jq


class _FakeCursor:
    def __init__(self, conn: "_FakeConn") -> None:
        self._conn = conn
        self._last_sql = ""

    def execute(self, sql: str, params: tuple = ()) -> None:
        self._last_sql = " ".join(sql.split())
        self._conn.executed.append((self._last_sql, tuple(params)))

    def fetchall(self):
        if "FROM dealer_catalog c" in self._last_sql:
            return list(self._conn.catalog_rows)
        return []

    def fetchone(self):
        return None  # no duplicate queued/running job

    def close(self) -> None:
        pass


class _FakeConn:
    """Records executed SQL; serves canned dealer_catalog join rows."""

    def __init__(self, catalog_rows: list[tuple]) -> None:
        self.catalog_rows = catalog_rows
        self.executed: list[tuple[str, tuple]] = []

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass

    def close(self) -> None:
        pass


def _run_scheduler(monkeypatch, catalog_rows: list[tuple]) -> tuple[int, _FakeConn]:
    monkeypatch.setattr("backend.db.inventory_pg.is_inventory_postgres", lambda: True)
    conn = _FakeConn(catalog_rows)
    monkeypatch.setattr(jq, "pg_connect", lambda: conn)
    return jq.schedule_due_refresh_jobs(), conn


def _job_inserts(conn: _FakeConn) -> list[tuple[str, tuple]]:
    return [e for e in conn.executed if "INSERT INTO dealer_jobs" in e[0]]


def _catalog_updates(conn: _FakeConn) -> list[tuple[str, tuple]]:
    return [e for e in conn.executed if "UPDATE dealer_catalog" in e[0]]


def test_default_scan_interval_hours(monkeypatch) -> None:
    monkeypatch.delenv("SCANNER_DEFAULT_INTERVAL_HOURS", raising=False)
    assert jq._default_scan_interval_hours() == 24
    monkeypatch.setenv("SCANNER_DEFAULT_INTERVAL_HOURS", "6")
    assert jq._default_scan_interval_hours() == 6
    monkeypatch.setenv("SCANNER_DEFAULT_INTERVAL_HOURS", "0")
    assert jq._default_scan_interval_hours() == 1


def test_record_catalog_skips_without_postgres(monkeypatch) -> None:
    monkeypatch.setattr("backend.db.inventory_pg.is_inventory_postgres", lambda: False)
    jq.record_catalog_after_success(dealer_id="test-dealer", job_type="onboard", payload={"url": "https://x.com"})


def test_retry_failed_job_not_found(monkeypatch) -> None:
    monkeypatch.setattr("backend.db.inventory_pg.is_inventory_postgres", lambda: True)
    monkeypatch.setattr(jq, "get_job", lambda _jid: None)
    ok, err, _data = jq.retry_failed_job(99)
    assert ok is False
    assert err == "not_found"


def test_retry_failed_job_not_failed(monkeypatch) -> None:
    monkeypatch.setattr("backend.db.inventory_pg.is_inventory_postgres", lambda: True)
    monkeypatch.setattr(
        jq,
        "get_job",
        lambda _jid: {
            "id": 1,
            "status": "done",
            "dealer_id": "x",
            "job_type": "onboard",
            "payload_json": "{}",
            "result_json": '{"vehicle_count": 12, "scrape_confidence": {"level": "high"}}',
        },
    )
    ok, err, data = jq.retry_failed_job(1)
    assert ok is False
    assert err == "not_failed"
    assert data["status"] == "done"


def test_retry_done_low_confidence_enqueues(monkeypatch) -> None:
    monkeypatch.setattr("backend.db.inventory_pg.is_inventory_postgres", lambda: True)
    monkeypatch.setattr(
        jq,
        "get_job",
        lambda _jid: {
            "id": 6,
            "status": "done",
            "dealer_id": "test-dealer",
            "job_type": "onboard",
            "payload_json": '{"url": "https://example.com"}',
            "result_json": '{"vehicle_count": 0, "scrape_confidence": {"level": "low", "score": 0.08}}',
        },
    )
    monkeypatch.setattr(jq, "_has_active_job", lambda **kwargs: False)
    monkeypatch.setattr(jq, "enqueue_job", lambda **kwargs: 7)
    ok, err, data = jq.retry_failed_job(6)
    assert ok is True
    assert err == ""
    assert data["job_id"] == 7
    assert data["source_job_id"] == 6


def test_retry_failed_job_enqueues(monkeypatch) -> None:
    monkeypatch.setattr("backend.db.inventory_pg.is_inventory_postgres", lambda: True)
    monkeypatch.setattr(
        jq,
        "get_job",
        lambda _jid: {
            "id": 5,
            "status": "failed",
            "dealer_id": "test-dealer",
            "job_type": "onboard",
            "payload_json": '{"url": "https://example.com"}',
        },
    )
    monkeypatch.setattr(jq, "_has_active_job", lambda **kwargs: False)
    monkeypatch.setattr(jq, "enqueue_job", lambda **kwargs: 42)
    ok, err, data = jq.retry_failed_job(5)
    assert ok is True
    assert err == ""
    assert data["job_id"] == 42
    assert data["source_job_id"] == 5


# --- scheduler reads dealer_scan_status.reason ---------------------------------


def test_schedule_skips_dns_fail_inside_backoff(monkeypatch) -> None:
    checked = datetime.now(timezone.utc) - timedelta(days=2)
    enqueued, conn = _run_scheduler(monkeypatch, [("dead-dealer-com", 24, "dns_fail", checked)])
    assert enqueued == 0
    assert _job_inserts(conn) == []
    updates = _catalog_updates(conn)
    assert len(updates) == 1  # parked, not dropped: next_scan_at pushed to end of window
    parked_until, dealer = updates[0][1]
    assert dealer == "dead-dealer-com"
    assert parked_until == (checked + timedelta(days=7)).isoformat()


def test_schedule_skips_redirect_offsite_with_iso_string_checked_at(monkeypatch) -> None:
    checked_iso = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    enqueued, conn = _run_scheduler(
        monkeypatch, [("merged-dealer-com", 24, "redirect_offsite", checked_iso)]
    )
    assert enqueued == 0
    assert _job_inserts(conn) == []
    assert len(_catalog_updates(conn)) == 1


def test_schedule_enqueues_dns_fail_after_backoff_window(monkeypatch) -> None:
    checked = datetime.now(timezone.utc) - timedelta(days=10)
    enqueued, conn = _run_scheduler(monkeypatch, [("back-dealer-com", 24, "dns_fail", checked)])
    assert enqueued == 1
    inserts = _job_inserts(conn)
    assert len(inserts) == 1
    assert inserts[0][1][0] == "back-dealer-com"


def test_schedule_dealer_without_status_row_unaffected(monkeypatch) -> None:
    enqueued, conn = _run_scheduler(monkeypatch, [("normal-dealer-com", 24, None, None)])
    assert enqueued == 1
    inserts = _job_inserts(conn)
    assert len(inserts) == 1
    assert inserts[0][1][0] == "normal-dealer-com"


def test_schedule_non_backoff_reason_keeps_normal_cadence(monkeypatch) -> None:
    checked = datetime.now(timezone.utc) - timedelta(hours=1)
    enqueued, conn = _run_scheduler(
        monkeypatch, [("blocked-dealer-com", 24, "needs_browser_probe", checked)]
    )
    assert enqueued == 1
    assert len(_job_inserts(conn)) == 1


def test_schedule_backoff_days_env_override(monkeypatch) -> None:
    monkeypatch.setenv("SCANNER_UNREACHABLE_BACKOFF_DAYS", "3")
    checked = datetime.now(timezone.utc) - timedelta(days=4)
    enqueued, conn = _run_scheduler(monkeypatch, [("dead-dealer-com", 24, "dns_fail", checked)])
    assert enqueued == 1  # outside the shortened window


# --- retry reads dealer_scan_status.reason -------------------------------------


def test_diagnose_job_row_maps_needs_browser_probe_to_profile(monkeypatch) -> None:
    monkeypatch.setattr(jq, "get_dealer_scan_reason", lambda _d: "needs_browser_probe")
    diag = jq.diagnose_job_row(
        {
            "id": 9,
            "dealer_id": "di-dealer-com",
            "job_type": "refresh",
            "error": "HTTP 403",
            "payload_json": "{}",
            "result_json": "{}",
        },
        use_llm=False,
    )
    assert diag["retry_strategy"] == "retry_with_profile"
    assert {"type": "set_profile", "value": "resilient"} in diag["retry_actions"]
    assert diag["retry_recommended"] is True
    assert diag["dealer_scan_reason"] == "needs_browser_probe"


def test_smart_retry_browser_probe_dealer_requeues_with_resilient_profile(monkeypatch) -> None:
    monkeypatch.setattr("backend.db.inventory_pg.is_inventory_postgres", lambda: True)
    monkeypatch.setattr(
        jq,
        "get_job",
        lambda _jid: {
            "id": 12,
            "status": "failed",
            "dealer_id": "di-dealer-com",
            "job_type": "refresh",
            "error": "HTTP 403",
            "payload_json": '{"url": "https://example.com"}',
            "result_json": "{}",
        },
    )
    monkeypatch.setattr(jq, "_has_active_job", lambda **kwargs: False)
    monkeypatch.setattr(jq, "_merge_result_diagnosis", lambda _jid, _diag: None)
    monkeypatch.setattr(jq, "get_dealer_scan_reason", lambda _d: "needs_browser_probe")
    captured: dict = {}

    def _capture_enqueue(**kwargs):
        captured.update(kwargs)
        return 13

    monkeypatch.setattr(jq, "enqueue_job", _capture_enqueue)
    ok, err, data = jq.smart_retry_failed_job(12, use_llm=False)
    assert ok is True
    assert err == ""
    assert data["job_id"] == 13
    assert captured["payload"]["profile"] == "resilient"
    assert data["diagnosis"]["retry_strategy"] == "retry_with_profile"


def test_smart_retry_without_status_row_stays_plain(monkeypatch) -> None:
    monkeypatch.setattr("backend.db.inventory_pg.is_inventory_postgres", lambda: True)
    monkeypatch.setattr(
        jq,
        "get_job",
        lambda _jid: {
            "id": 20,
            "status": "failed",
            "dealer_id": "fine-dealer-com",
            "job_type": "refresh",
            "error": "boom",
            "payload_json": '{"url": "https://example.com"}',
            "result_json": "{}",
        },
    )
    monkeypatch.setattr(jq, "_has_active_job", lambda **kwargs: False)
    monkeypatch.setattr(jq, "_merge_result_diagnosis", lambda _jid, _diag: None)
    monkeypatch.setattr(jq, "get_dealer_scan_reason", lambda _d: None)
    captured: dict = {}

    def _capture_enqueue(**kwargs):
        captured.update(kwargs)
        return 21

    monkeypatch.setattr(jq, "enqueue_job", _capture_enqueue)
    ok, err, _data = jq.smart_retry_failed_job(20, use_llm=False)
    assert ok is True
    assert err == ""
    assert "profile" not in captured["payload"]


# --- stale `running` reaper --------------------------------------------------


class _ReapCursor(_FakeCursor):
    def __init__(self, conn) -> None:
        super().__init__(conn)

    def fetchall(self):
        if "UPDATE dealer_jobs" in self._last_sql and "RETURNING" in self._last_sql:
            return list(self._conn.reaped_rows)
        return super().fetchall()


class _ReapConn(_FakeConn):
    def __init__(self, reaped_rows: list[tuple]) -> None:
        super().__init__([])
        self.reaped_rows = reaped_rows

    def cursor(self) -> _ReapCursor:
        return _ReapCursor(self)


def _run_reaper(monkeypatch, reaped_rows: list[tuple], **kwargs) -> tuple[int, _ReapConn]:
    monkeypatch.setattr("backend.db.inventory_pg.is_inventory_postgres", lambda: True)
    conn = _ReapConn(reaped_rows)
    monkeypatch.setattr(jq, "pg_connect", lambda: conn)
    return jq.reap_stale_running_jobs(**kwargs), conn


def test_reap_skips_without_postgres(monkeypatch) -> None:
    monkeypatch.setattr("backend.db.inventory_pg.is_inventory_postgres", lambda: False)
    monkeypatch.setattr(jq, "pg_connect", lambda: (_ for _ in ()).throw(AssertionError("must not connect")))
    assert jq.reap_stale_running_jobs() == 0


def test_reap_marks_only_running_rows_older_than_cutoff(monkeypatch) -> None:
    old = (datetime.now(timezone.utc) - timedelta(hours=3)).isoformat()
    n, conn = _run_reaper(monkeypatch, [(7, "dealer-a", "worker-1", old)], max_age_sec=3600)
    assert n == 1
    sql, params = conn.executed[-1]
    assert "SET status = 'failed'" in sql
    assert "WHERE status = 'running'" in sql
    assert "started_at < " in sql
    # params: (finished_at, error, cutoff)
    assert params[1].startswith("stale_running:")
    cutoff = datetime.fromisoformat(params[2])
    assert timedelta(seconds=3500) < datetime.now(timezone.utc) - cutoff < timedelta(seconds=3700)


def test_reap_returns_zero_when_nothing_stale(monkeypatch) -> None:
    n, _conn = _run_reaper(monkeypatch, [], max_age_sec=3600)
    assert n == 0


def test_reap_default_age_tracks_job_timeout_plus_grace(monkeypatch) -> None:
    monkeypatch.delenv("SCANNER_STALE_JOB_SEC", raising=False)
    monkeypatch.setenv("SCANNER_JOB_TIMEOUT_SEC", "1200")
    assert jq._stale_running_max_age_sec() == 1200 + 900
    monkeypatch.setenv("SCANNER_STALE_JOB_SEC", "300")
    assert jq._stale_running_max_age_sec() == 300
