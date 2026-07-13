"""/api/health (liveness) and /api/ready (readiness) endpoints."""

from __future__ import annotations

import pytest


def _clear_optional_dep_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Deterministic optional-dep state: pgvector and redis unconfigured."""
    monkeypatch.delenv("PGVECTOR_URL", raising=False)
    monkeypatch.delenv("REDIS_URL", raising=False)
    # DATABASE_URL / INVENTORY_DATABASE_URL already cleared by the autouse
    # conftest fixture (_inventory_sqlite_tests_mode).


def test_api_health_ok_and_never_touches_db(monkeypatch) -> None:
    from backend import main

    def _boom(*args, **kwargs):  # pragma: no cover - must not be called
        raise AssertionError("liveness endpoint must not open a DB connection")

    monkeypatch.setattr("backend.db.inventory_db.db_conn", _boom)
    with main.app.test_client() as c:
        rv = c.get("/api/health")
        assert rv.status_code == 200
        data = rv.get_json()
        assert data["status"] == "ok"
        assert isinstance(data["version"], str) and data["version"]


def test_api_health_matches_legacy_health_shape() -> None:
    from backend import main

    with main.app.test_client() as c:
        legacy = c.get("/health").get_json()
        new = c.get("/api/health").get_json()
        assert new == legacy


def test_api_ready_ok_with_optional_deps_skipped(monkeypatch) -> None:
    from backend import main

    _clear_optional_dep_env(monkeypatch)
    with main.app.test_client() as c:
        rv = c.get("/api/ready")
        assert rv.status_code == 200
        data = rv.get_json()
        assert data["ready"] is True
        assert data["checks"] == {"db": "ok", "pgvector": "skipped", "redis": "skipped"}


def test_api_ready_503_when_inventory_db_broken(monkeypatch) -> None:
    from backend import main
    from backend.routes import health as health_mod

    _clear_optional_dep_env(monkeypatch)

    def _broken_db() -> str:
        raise RuntimeError("db down")

    monkeypatch.setattr(health_mod, "_check_inventory_db", _broken_db)
    with main.app.test_client() as c:
        rv = c.get("/api/ready")
        assert rv.status_code == 503
        data = rv.get_json()
        assert data["ready"] is False
        assert data["checks"]["db"] == "error: RuntimeError"
        # One broken dep must not mask the others.
        assert data["checks"]["pgvector"] == "skipped"
        assert data["checks"]["redis"] == "skipped"


def test_api_ready_pgvector_configured_but_broken_does_not_gate(monkeypatch) -> None:
    from backend import main
    from backend.vector import pgvector_service

    _clear_optional_dep_env(monkeypatch)
    monkeypatch.setenv("PGVECTOR_URL", "postgresql://pgvector.invalid:5432/vectors")

    def _broken_connect():
        raise ConnectionError("pg down")

    monkeypatch.setattr(pgvector_service, "_connect", _broken_connect)
    with main.app.test_client() as c:
        rv = c.get("/api/ready")
        assert rv.status_code == 200
        data = rv.get_json()
        assert data["ready"] is True
        assert data["checks"]["db"] == "ok"
        assert data["checks"]["pgvector"] == "error: ConnectionError"


def test_ready_pgvector_skipped_when_only_database_url_set(monkeypatch) -> None:
    """DATABASE_URL alone (plain inventory Postgres, possibly without the
    vector extension) must not make the readiness probe touch pgvector."""
    from backend.routes import health as health_mod

    monkeypatch.delenv("PGVECTOR_URL", raising=False)
    monkeypatch.setenv("DATABASE_URL", "postgresql://plain-pg.invalid:5432/inventory")
    assert health_mod._check_pgvector() is None


def test_pgvector_connect_closes_connection_on_register_failure(monkeypatch) -> None:
    """A post-connect failure (e.g. server lacks the vector extension) must
    close the just-opened connection instead of abandoning it to GC."""
    import sys
    import types

    from backend.vector import pgvector_service

    class _FakeConn:
        closed = False

        def close(self):
            self.closed = True

    fake_conn = _FakeConn()

    fake_psycopg = types.ModuleType("psycopg")
    fake_psycopg.connect = lambda url: fake_conn

    class _NoVectorType(Exception):
        pass

    def _register_vector(conn):
        raise _NoVectorType("vector type not found in the database")

    fake_pgvector = types.ModuleType("pgvector")
    fake_pgvector_psycopg = types.ModuleType("pgvector.psycopg")
    fake_pgvector_psycopg.register_vector = _register_vector
    fake_pgvector.psycopg = fake_pgvector_psycopg

    monkeypatch.setenv("PGVECTOR_URL", "postgresql://pgvector.invalid:5432/vectors")
    monkeypatch.setitem(sys.modules, "psycopg", fake_psycopg)
    monkeypatch.setitem(sys.modules, "pgvector", fake_pgvector)
    monkeypatch.setitem(sys.modules, "pgvector.psycopg", fake_pgvector_psycopg)

    with pytest.raises(_NoVectorType):
        pgvector_service._connect()
    assert fake_conn.closed is True


def test_pgvector_connect_closes_connection_on_import_error(monkeypatch) -> None:
    """The historical ImportError branch must also still close the connection."""
    import sys
    import types

    from backend.vector import pgvector_service

    class _FakeConn:
        closed = False

        def close(self):
            self.closed = True

    fake_conn = _FakeConn()

    fake_psycopg = types.ModuleType("psycopg")
    fake_psycopg.connect = lambda url: fake_conn

    fake_pgvector = types.ModuleType("pgvector")  # no .psycopg submodule

    monkeypatch.setenv("PGVECTOR_URL", "postgresql://pgvector.invalid:5432/vectors")
    monkeypatch.setitem(sys.modules, "psycopg", fake_psycopg)
    monkeypatch.setitem(sys.modules, "pgvector", fake_pgvector)
    monkeypatch.setitem(sys.modules, "pgvector.psycopg", None)

    with pytest.raises(RuntimeError, match="pip install pgvector"):
        pgvector_service._connect()
    assert fake_conn.closed is True


def test_api_ready_redis_configured_but_unreachable_does_not_gate(monkeypatch) -> None:
    from backend import main
    from backend.routes import health as health_mod

    _clear_optional_dep_env(monkeypatch)
    # Unreachable port; also reset the module-level client cache so this
    # test's URL (not one from an earlier call) is probed.
    monkeypatch.setenv("REDIS_URL", "redis://127.0.0.1:1/0")
    monkeypatch.setattr(health_mod, "_redis", None)
    with main.app.test_client() as c:
        rv = c.get("/api/ready")
        assert rv.status_code == 200
        data = rv.get_json()
        assert data["ready"] is True
        assert data["checks"]["db"] == "ok"
        # redis-py missing -> ModuleNotFoundError; installed -> connection error.
        assert data["checks"]["redis"].startswith("error: ")
