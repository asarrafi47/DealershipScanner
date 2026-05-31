"""SEC-087: debug audit remediation — public search meta trim and dev row redaction."""

from __future__ import annotations

import importlib

import pytest

from backend.utils.car_serialize import SENSITIVE_CAR_ROW_KEYS, redact_sensitive_car_row
from backend.utils.hybrid_search import public_search_meta


def test_public_search_meta_strips_internal_fields() -> None:
    meta = {
        "mode": "semantic_then_sql",
        "sql_count": 12,
        "vector_candidate_count": 50,
        "vector_candidate_ids_head": [1, 2, 3],
        "parsed_filters": {"make": "BMW"},
        "vector_backend": "pgvector",
    }
    public = public_search_meta(meta)
    assert public == {
        "mode": "semantic_then_sql",
        "sql_count": 12,
        "vector_candidate_count": 50,
    }
    assert "vector_candidate_ids_head" not in public
    assert "parsed_filters" not in public


def test_redact_sensitive_car_row() -> None:
    row = {
        "id": 1,
        "vin": "1HGCM82633A004352",
        "internal_notes": "ops only",
        "price_provenance_json": "{}",
    }
    out = redact_sensitive_car_row(row)
    assert out == {"id": 1, "vin": "1HGCM82633A004352"}
    assert SENSITIVE_CAR_ROW_KEYS.issuperset(
        {"internal_notes", "marked_for_review", "price_provenance_json", "kbb_snapshot_json"}
    )


def test_smart_search_api_returns_trimmed_meta(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setenv("FLASK_ENV", "development")
    monkeypatch.setenv("USERS_DB_PATH", str(tmp_path / "users_ss.db"))
    monkeypatch.setenv("DEV_USERS_DB_PATH", str(tmp_path / "dev_ss.db"))
    monkeypatch.setenv("ALLOW_DEFAULT_APP_USER", "0")
    import backend.main as main
    from backend.utils.csrf import _SESSION_KEY

    importlib.reload(main)
    client = main.app.test_client()
    tok = "pytest-csrf-token-for-smart-search"
    with client.session_transaction() as sess:
        sess[_SESSION_KEY] = tok
    rv = client.post(
        "/api/search/smart",
        json={"query": "nonexistentmakezzz"},
        headers={"X-CSRF-Token": tok},
    )
    assert rv.status_code == 200
    body = rv.get_json()
    meta = body.get("search_meta") or {}
    assert "vector_candidate_ids_head" not in meta
    assert "parsed_filters" not in meta
    if meta:
        assert set(meta.keys()).issubset({"mode", "sql_count", "vector_candidate_count"})
