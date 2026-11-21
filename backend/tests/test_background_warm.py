"""Post-fork background warm-up and the cold-path guards it relies on (2026-11-20).

``gunicorn.conf.py::post_fork`` starts ``aggregate_warmup.warm_background`` after the
blocking warm: the parser vocabulary, the /listings facets and the semantic-search model,
on a daemon thread, so neither boot nor /health waits. A visitor who arrives while it runs
must wait for the build in flight instead of starting a second one (twice the GIL work,
or for the model twice the memory), and the search's pgvector DDL runs once per process.
"""

from __future__ import annotations

import threading
import time

import pytest

from backend.utils import aggregate_warmup as warmup


def test_background_warm_returns_at_once_and_isolates_failures(monkeypatch):
    gate = threading.Event()
    ran = []

    def slow():
        gate.wait(5)
        ran.append("slow")

    def bad():
        raise RuntimeError("step bug")

    t0 = time.monotonic()
    t = warmup.warm_background((("slow", slow), ("bad", bad), ("after", lambda: ran.append("after"))))
    assert time.monotonic() - t0 < 1.0 and t is not None and t.daemon
    gate.set()
    t.join(5)
    assert ran == ["slow", "after"]


@pytest.mark.parametrize("value", ["0", "false", "off"])
def test_background_warm_can_be_turned_off(monkeypatch, value):
    monkeypatch.setenv("BACKGROUND_WARM", value)
    assert warmup.warm_background((("x", lambda: pytest.fail("ran")),)) is None


def test_background_steps_cover_facets_vocabulary_and_model():
    labels = [label for label, _ in warmup.BACKGROUND_STEPS]
    assert labels == ["search parser vocabulary", "listings facets", "semantic search model"]
    assert not set(labels) & {label for label, _ in warmup.WARM_STEPS}  # never in the blocking warm


def test_cold_facet_build_runs_once_under_concurrency(monkeypatch):
    from backend.db.repositories import grid_cards_repo
    from backend.db.repositories import listings_repo as lr

    builds = []

    def slow_build():
        builds.append(1)
        time.sleep(0.2)
        return {"makes": ["X"]}

    monkeypatch.setattr(lr, "_build_filter_options_uncached", slow_build)
    monkeypatch.setattr(grid_cards_repo, "grid_scope_token", lambda: "tok")
    monkeypatch.setattr(lr, "_facet_options_cache_value", None)
    monkeypatch.setattr(lr, "_facet_options_cache_token", None)
    out = []
    threads = [threading.Thread(target=lambda: out.append(lr.get_filter_options())) for _ in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(5)
    assert len(builds) == 1 and out == [{"makes": ["X"]}] * 3


def test_semantic_model_loads_once_under_concurrency(monkeypatch):
    from backend.vector import embedding_model
    from backend.vector import pgvector_service as pv

    loads = []

    def slow_load():
        loads.append(1)
        time.sleep(0.2)
        return object()

    monkeypatch.setattr(pv, "_st_model", None)
    monkeypatch.setattr(embedding_model, "load_sentence_transformer", slow_load)
    got = []
    threads = [threading.Thread(target=lambda: got.append(pv._encode_model())) for _ in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(5)
    assert len(loads) == 1 and len({id(m) for m in got}) == 1


def test_semantic_model_step_skips_without_pgvector(monkeypatch):
    from backend.vector import pgvector_service as pv

    monkeypatch.setattr(pv, "pgvector_configured", lambda: False)
    monkeypatch.setattr(pv, "_encode_model", lambda: pytest.fail("loaded"))
    warmup._semantic_model()


def test_query_cars_ensures_the_schema_once_per_dsn(monkeypatch):
    from backend.vector import pgvector_service as pv

    class _Cur:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, *a):
            pass

        def fetchall(self):
            return [(3,), (1,)]

    class _Conn:
        def cursor(self):
            return _Cur()

        def close(self):
            pass

    ensured = []
    monkeypatch.setattr(pv, "_connect", lambda: _Conn())
    monkeypatch.setattr(pv, "ensure_schema", lambda conn: ensured.append(1))
    monkeypatch.setattr(pv, "_embed_texts", lambda texts: [[0.0]])
    monkeypatch.setenv("PGVECTOR_URL", "postgresql://a/one")
    pv.reset_query_schema_check()
    assert pv.query_cars("red suv") == [3, 1]
    pv.query_cars("blue sedan")
    assert len(ensured) == 1
    monkeypatch.setenv("PGVECTOR_URL", "postgresql://a/two")
    pv.query_cars("blue sedan")
    assert len(ensured) == 2
    pv.reset_query_schema_check()


def test_parser_vocabulary_is_loaded_once_under_concurrency(monkeypatch):
    from backend.utils import query_parser as qp

    loads = []

    def slow_kw(key):
        loads.append(key)
        time.sleep(0.2)
        return ((), (), (), (), ())

    cache: dict = {}

    def cached_kw(key):  # stands in for the lru_cache
        if key not in cache:
            cache[key] = slow_kw(key)
        return cache[key]

    monkeypatch.setattr(qp, "_load_inventory_keywords", cached_kw)
    monkeypatch.setattr(qp, "_load_package_names", lambda key: ())
    monkeypatch.setattr(qp, "_inventory_db_cache_key", lambda: "k")
    threads = [threading.Thread(target=qp.parser_vocabulary) for _ in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(5)
    assert loads == ["k"]
