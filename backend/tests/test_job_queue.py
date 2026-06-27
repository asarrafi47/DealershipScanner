"""Tests for scanner job queue helpers (A3)."""

from __future__ import annotations

from backend.scanner import job_queue as jq


def test_default_scan_interval_hours(monkeypatch) -> None:
    monkeypatch.delenv("SCANNER_DEFAULT_INTERVAL_HOURS", raising=False)
    assert jq._default_scan_interval_hours() == 24
    monkeypatch.setenv("SCANNER_DEFAULT_INTERVAL_HOURS", "6")
    assert jq._default_scan_interval_hours() == 6
    monkeypatch.setenv("SCANNER_DEFAULT_INTERVAL_HOURS", "0")
    assert jq._default_scan_interval_hours() == 1


def test_record_scan_registry_skips_without_postgres(monkeypatch) -> None:
    monkeypatch.setattr(jq, "is_inventory_postgres", lambda: False)
    jq.record_dealer_scan_registry(dealer_id="test-dealer", job_type="onboard", payload={"url": "https://x.com"})


def test_retry_failed_job_not_found(monkeypatch) -> None:
    monkeypatch.setattr(jq, "is_inventory_postgres", lambda: True)
    monkeypatch.setattr(jq, "get_job", lambda _jid: None)
    ok, err, _data = jq.retry_failed_job(99)
    assert ok is False
    assert err == "not_found"


def test_retry_failed_job_not_failed(monkeypatch) -> None:
    monkeypatch.setattr(jq, "is_inventory_postgres", lambda: True)
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
    monkeypatch.setattr(jq, "is_inventory_postgres", lambda: True)
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
    monkeypatch.setattr(jq, "is_inventory_postgres", lambda: True)
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
