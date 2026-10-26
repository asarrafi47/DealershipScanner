"""P15B.3: every SentenceTransformer load passes revision=, and prod overrides must pin one."""

from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest

from backend.vector import embedding_model as em

_REPO_ROOT = Path(__file__).resolve().parents[2]


class _FakeST:
    calls: list[tuple[tuple, dict]] = []

    def __init__(self, *args, **kwargs):
        _FakeST.calls.append((args, kwargs))

    def get_sentence_embedding_dimension(self):
        return 384


@pytest.fixture
def fake_st(monkeypatch):
    _FakeST.calls = []
    mod = types.ModuleType("sentence_transformers")
    mod.SentenceTransformer = _FakeST
    monkeypatch.setitem(sys.modules, "sentence_transformers", mod)
    monkeypatch.setattr(em, "DEFAULT_MODEL_REVISION", "abc123")
    for k in ("LISTING_EMBEDDING_MODEL", "LISTING_EMBEDDING_REVISION", "FLASK_ENV"):
        monkeypatch.setenv(k, "")
    return _FakeST


def _assert_pinned() -> None:
    assert len(_FakeST.calls) == 1
    args, kwargs = _FakeST.calls[0]
    assert args == (em.DEFAULT_MODEL_ID,)
    assert kwargs == {"revision": "abc123"}


def test_pgvector_service_passes_revision(fake_st, monkeypatch):
    from backend.vector import pgvector_service as pv

    monkeypatch.setattr(pv, "_st_model", None)
    pv._encode_model()
    _assert_pinned()


def test_catalog_service_passes_revision(fake_st):
    from backend.vector.catalog_service import MasterCatalog

    MasterCatalog()._model()
    _assert_pinned()


def test_no_call_site_builds_a_bare_sentence_transformer():
    for rel in ("ingest_master_specs", "catalog_service", "pgvector_service"):
        src = (_REPO_ROOT / "backend/vector" / f"{rel}.py").read_text()
        assert "SentenceTransformer(" not in src, rel
    ingest = (_REPO_ROOT / "backend/vector/ingest_master_specs.py").read_text()
    assert "load_sentence_transformer()" in ingest


def test_production_override_without_revision_raises(fake_st, monkeypatch):
    monkeypatch.setenv("FLASK_ENV", "production")
    monkeypatch.setenv("LISTING_EMBEDDING_MODEL", "some/other-model")
    with pytest.raises(RuntimeError, match="LISTING_EMBEDDING_REVISION"):
        em.load_sentence_transformer()
    assert _FakeST.calls == []


def test_production_override_with_revision_loads(fake_st, monkeypatch):
    monkeypatch.setenv("FLASK_ENV", "production")
    monkeypatch.setenv("LISTING_EMBEDDING_MODEL", "some/other-model")
    monkeypatch.setenv("LISTING_EMBEDDING_REVISION", "deadbeef")
    em.load_sentence_transformer()
    assert _FakeST.calls == [(("some/other-model",), {"revision": "deadbeef"})]


def test_dev_override_without_revision_allowed(fake_st, monkeypatch):
    monkeypatch.setenv("LISTING_EMBEDDING_MODEL", "some/other-model")
    em.load_sentence_transformer()
    assert _FakeST.calls == [(("some/other-model",), {"revision": None})]


def test_default_short_name_counts_as_default(fake_st, monkeypatch):
    monkeypatch.setenv("FLASK_ENV", "production")
    monkeypatch.setenv("LISTING_EMBEDDING_MODEL", "all-MiniLM-L6-v2")
    assert em.model_revision() == "abc123"


def test_web_image_bakes_model_offline():
    text = (_REPO_ROOT / "Dockerfile.web").read_text()
    lines = [ln for ln in text.splitlines() if not ln.strip().startswith("#")]
    body = "\n".join(lines)
    assert "HF_HOME=/app/models" in body
    assert "HF_HUB_OFFLINE=1" in body and "TRANSFORMERS_OFFLINE=1" in body
    assert "snapshot_download" in body and "chmod -R a+rX /app/models" in body
    # Offline flags come after the bake, or the bake itself could not download.
    assert body.index("snapshot_download") < body.index("HF_HUB_OFFLINE=1")
    assert body.index("embedding_model.py") < body.index("COPY . .")
