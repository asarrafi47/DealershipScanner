"""N37.12: the dev job/queue store is process-local; the SQLite option is gone."""

from __future__ import annotations

from backend.dev import routes


def test_store_roundtrip_patch_and_evict(monkeypatch) -> None:
    monkeypatch.setattr(routes, "scanner_jobs", {})
    monkeypatch.setattr(routes, "import_queues", {})
    monkeypatch.setattr(routes, "_MAX_SCANNER_JOBS", 2)
    routes._dev_store_put("jobs", "a", {"log": ""})
    routes._dev_store_put("jobs", "b", {"log": ""})
    routes._dev_store_put("jobs", "c", {"log": ""})
    assert routes._dev_store_get("jobs", "a") is None
    assert routes._dev_store_patch("jobs", "b", {"log": "x"})
    assert routes._dev_store_get("jobs", "b") == {"log": "x"}
    assert not routes._dev_store_patch("jobs", "gone", {"log": "x"})


def test_queue_item_patch(monkeypatch) -> None:
    monkeypatch.setattr(routes, "import_queues", {})
    routes._dev_store_put("queues", "q", {"items": [{"job_id": "j1", "status": "queued"}], "done": False})
    routes._dev_queue_patch_item("q", "j1", status="processing")
    assert routes._dev_store_get("queues", "q")["items"][0]["status"] == "processing"
    routes._dev_queue_patch_item("missing", "j1", status="x")


def test_no_sqlite_store_option(monkeypatch) -> None:
    monkeypatch.setenv("DEV_JOB_STORE_SQLITE_PATH", "/nonexistent/x.store")
    assert not hasattr(routes, "_dev_job_store_enabled")
