"""A /dev scan or bulk import must never run two full-fleet pgvector reindexes at once.

``reindex_all`` loads every car row and re-embeds it on CPU inside the web worker. It used to
start one bare daemon thread per successful scan (one per dealer in a bulk import), so
overlapping runs multiplied memory and starved the 4 request threads of the single worker.
"""

from __future__ import annotations

import threading

import pytest

from backend.dev import routes


@pytest.fixture()
def reindex_stub(monkeypatch: pytest.MonkeyPatch):
    from backend.vector import pgvector_service

    state = {"calls": 0, "active": 0, "max_active": 0}
    entered = threading.Event()
    release = threading.Event()

    def fake_reindex_all():
        state["calls"] += 1
        state["active"] += 1
        state["max_active"] = max(state["max_active"], state["active"])
        entered.set()
        try:
            assert release.wait(10), "test never released the reindex"
        finally:
            state["active"] -= 1
        return {}

    monkeypatch.setattr(pgvector_service, "reindex_all", fake_reindex_all)
    monkeypatch.setattr(routes, "_vector_reindex_running", False)
    monkeypatch.setattr(routes, "_vector_reindex_again", False)
    return state, entered, release


def test_overlapping_requests_collapse_into_one_run_plus_one_follow_up(reindex_stub):
    state, entered, release = reindex_stub
    first = threading.Thread(target=routes._vector_reindex_background)
    first.start()
    assert entered.wait(10)

    # Nine more scans finish while the first reindex is still running: none may start a run.
    for _ in range(9):
        routes._vector_reindex_background()
    assert state["calls"] == 1

    release.set()
    first.join(15)
    assert not first.is_alive()
    # The running thread went round once more, so cars added meanwhile are still embedded.
    assert state["calls"] == 2
    assert state["max_active"] == 1
    assert routes._vector_reindex_running is False


def test_a_failing_reindex_releases_the_slot(monkeypatch: pytest.MonkeyPatch):
    from backend.vector import pgvector_service

    calls = []

    def boom():
        calls.append(1)
        raise RuntimeError("pgvector down")

    monkeypatch.setattr(pgvector_service, "reindex_all", boom)
    monkeypatch.setattr(routes, "_vector_reindex_running", False)
    monkeypatch.setattr(routes, "_vector_reindex_again", False)

    routes._vector_reindex_background()
    assert routes._vector_reindex_running is False
    routes._vector_reindex_background()  # a later scan can still reindex
    assert len(calls) == 2
