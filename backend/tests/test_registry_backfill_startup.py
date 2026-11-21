"""The registry backfill runs at startup in a thread, never on the geo-coords request."""
from __future__ import annotations

import inspect
import threading

import backend.main as main_mod
from backend.db.repositories import listings_repo


def test_geo_coords_request_path_does_not_backfill() -> None:
    src = inspect.getsource(listings_repo.listings_geo_coords_maps)
    assert "ensure_dealership_registry_backfill()" not in src


def test_start_registry_backfill_skipped_under_pytest(monkeypatch) -> None:
    started: list[str] = []
    monkeypatch.setattr(threading.Thread, "start", lambda self: started.append(self.name))
    main_mod._start_registry_backfill()
    assert started == []


def test_start_registry_backfill_runs_in_background_thread(monkeypatch) -> None:
    calls: list[str] = []
    done = threading.Event()

    def fake() -> int:
        calls.append(threading.current_thread().name)
        done.set()
        return 3

    monkeypatch.delitem(main_mod.sys.modules, "pytest")
    monkeypatch.delenv("DS_SKIP_REGISTRY_BACKFILL", raising=False)
    monkeypatch.setattr(
        "backend.db.repositories.dealers_repo.ensure_dealership_registry_backfill", fake
    )
    main_mod._start_registry_backfill()
    assert done.wait(5)
    assert calls == ["registry-backfill"]
