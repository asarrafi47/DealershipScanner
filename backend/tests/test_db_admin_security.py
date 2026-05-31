"""SEC-080: local model_specs db_admin tool auth and SQL column allowlist."""

from __future__ import annotations

import importlib
import sqlite3

import pytest


@pytest.fixture()
def db_admin_app(monkeypatch: pytest.MonkeyPatch, tmp_path):
    monkeypatch.setenv("DB_ADMIN_TOKEN", "test-db-admin-token-32chars-min")
    db_path = tmp_path / "inventory.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE model_specs (
            make TEXT, model TEXT, transmission TEXT, drivetrain TEXT, cylinders INTEGER
        )
        """
    )
    conn.execute(
        "INSERT INTO model_specs VALUES ('BMW', 'X3', 'Auto', 'AWD', 6)"
    )
    conn.commit()
    conn.close()
    monkeypatch.setenv("INVENTORY_DB_PATH", str(db_path))

    import backend.scripts.db_admin as mod

    importlib.reload(mod)
    return mod


def test_db_admin_api_requires_token(db_admin_app) -> None:
    client = db_admin_app.app.test_client()
    assert client.get("/model-specs-admin/api/specs").status_code == 401
    rv = client.get(
        "/model-specs-admin/api/specs",
        headers={"Authorization": "Bearer test-db-admin-token-32chars-min"},
    )
    assert rv.status_code == 200
    assert len(rv.get_json()["specs"]) == 1


def test_db_admin_update_rejects_invalid_column(db_admin_app) -> None:
    client = db_admin_app.app.test_client()
    hdr = {"Authorization": "Bearer test-db-admin-token-32chars-min"}
    rv = client.put(
        "/model-specs-admin/api/specs/1",
        json={"field": "make; DROP TABLE model_specs;--", "value": "x"},
        headers=hdr,
    )
    assert rv.status_code == 400
    body = rv.get_json()
    assert body.get("error") == "invalid_field"


def test_db_admin_startup_requires_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DB_ADMIN_TOKEN", raising=False)
    import backend.scripts.db_admin as mod

    importlib.reload(mod)
    with pytest.raises(RuntimeError, match="DB_ADMIN_TOKEN"):
        mod._startup_checks()
