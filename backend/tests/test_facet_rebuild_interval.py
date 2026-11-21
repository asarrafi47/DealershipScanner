"""Facet rebuild throttle: at most one background rebuild per interval (fake clock)."""
from __future__ import annotations

from backend.db.repositories import listings_repo as lr
from backend.db.repositories.facets import queries as fq


class _Clock:
    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t


def _setup(monkeypatch, interval):
    clock = _Clock()
    tokens = [("t", 0)]
    builds: list[int] = []
    spawned: list[object] = []
    monkeypatch.setattr(lr, "_facet_clock", clock)
    if interval is None:
        monkeypatch.delenv("FACET_REBUILD_MIN_INTERVAL_S", raising=False)
    else:
        monkeypatch.setenv("FACET_REBUILD_MIN_INTERVAL_S", interval)
    monkeypatch.setattr(
        "backend.db.repositories.grid_cards_repo.grid_scope_token", lambda: tokens[0]
    )

    def _build():
        builds.append(1)
        return {"n": len(builds)}

    def _spawn(token):
        # run synchronously, as the thread body does
        lr._facet_options_cache_value = _build()
        lr._facet_options_cache_token = token
        lr._facet_options_built_at = lr._facet_clock()
        spawned.append(token)

    monkeypatch.setattr(lr, "_build_filter_options_uncached", _build)
    monkeypatch.setattr(lr, "_spawn_facet_options_rebuild", _spawn)
    lr.clear_inventory_listings_cache()
    return clock, tokens, builds, spawned


def test_cold_start_builds_inline_then_interval_gates_rebuilds(monkeypatch):
    clock, tokens, builds, spawned = _setup(monkeypatch, None)
    assert lr.get_filter_options() == {"n": 1}
    tokens[0] = ("t", 1)
    clock.t += 10
    assert lr.get_filter_options() == {"n": 1} and not spawned  # throttled
    clock.t += 300
    lr.get_filter_options()
    assert len(spawned) == 1 and lr.get_filter_options() == {"n": 2}


def test_zero_disables_throttle(monkeypatch):
    clock, tokens, builds, spawned = _setup(monkeypatch, "0")
    lr.get_filter_options()
    tokens[0] = ("t", 1)
    lr.get_filter_options()
    assert len(spawned) == 1


def test_clear_cache_forces_rebuild_inside_interval(monkeypatch):
    clock, tokens, builds, spawned = _setup(monkeypatch, "300")
    lr.get_filter_options()
    lr.clear_inventory_listings_cache()
    clock.t += 1
    assert lr.get_filter_options() == {"n": 2}


def test_bad_env_falls_back_to_default(monkeypatch):
    _setup(monkeypatch, "abc")
    assert lr._facet_rebuild_min_interval() == 300.0


def test_paint_facets_infer_each_distinct_color_once(monkeypatch):
    calls: list[str] = []

    def _infer(raw, make=None):
        calls.append(raw)
        return ["black"] if "black" in raw.lower() else ["other"]

    class _Cur:
        def execute(self, *_a):
            pass

        def fetchall(self):
            return [("Black", "Black", None)] * 50 + [("Red", "Red", "[]")] * 50

    monkeypatch.setattr(fq, "infer_paint_color_buckets", _infer)
    ext, intr = fq.paint_family_facets(_Cur())
    assert sorted(calls) == ["Black", "Red"]
    assert ext == ["black", "other"] or set(ext) == {"black", "other"}
