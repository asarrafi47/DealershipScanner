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
        assert isinstance(data["commit"], str) and data["commit"]


def test_api_health_matches_legacy_health_shape() -> None:
    from backend import main

    with main.app.test_client() as c:
        legacy = c.get("/health").get_json()
        new = c.get("/api/health").get_json()
        assert new == legacy
        assert set(new) == {"status", "version", "commit"}


# --- BUILD_COMMIT (remediation P2B.2) -------------------------------------------
#
# deploy/railway/deploy_web.sh and deploy_scanner_nightly.sh write BUILD_COMMIT
# (the full SHA) next to VERSION in the stage they upload; both health
# endpoints report it as "commit". The files are read from site_misc._ROOT_DIR,
# which these tests point at a tmp dir.

_SHA = "0123456789abcdef0123456789abcdef01234567"


def _root_with(monkeypatch, tmp_path, **files: str):
    from backend.routes import site_misc

    for name, text in files.items():
        (tmp_path / name).write_text(text, encoding="utf-8")
    monkeypatch.setattr(site_misc, "_ROOT_DIR", tmp_path)


@pytest.mark.parametrize("path", ["/health", "/api/health"])
def test_health_reports_build_commit(monkeypatch, tmp_path, path) -> None:
    from backend import main

    _root_with(monkeypatch, tmp_path, VERSION="9.8.7\n", BUILD_COMMIT=_SHA + "\n", BUILD_TAG="v9.8.7\n")
    with main.app.test_client() as c:
        rv = c.get(path)
        assert rv.status_code == 200
        assert rv.get_json() == {"status": "ok", "version": "9.8.7", "commit": _SHA}


@pytest.mark.parametrize("path", ["/health", "/api/health"])
def test_health_commit_unknown_without_build_commit(monkeypatch, tmp_path, path) -> None:
    """A dev checkout (no deploy stage) has VERSION but no BUILD_COMMIT."""
    from backend import main

    _root_with(monkeypatch, tmp_path, VERSION="9.8.7\n")
    with main.app.test_client() as c:
        rv = c.get(path)
        assert rv.status_code == 200
        assert rv.get_json() == {"status": "ok", "version": "9.8.7", "commit": "unknown"}


@pytest.mark.parametrize(
    "content",
    ["", "\n", "not-a-sha\n", "<script>alert(1)</script>", _SHA.upper(), "abc12", _SHA + " extra", "f" * 65],
)
def test_build_commit_malformed_is_unknown(monkeypatch, tmp_path, content) -> None:
    from backend.routes import site_misc

    _root_with(monkeypatch, tmp_path, BUILD_COMMIT=content)
    assert site_misc._build_commit() == "unknown"


@pytest.mark.parametrize("content", [_SHA, _SHA + "\n", "  " + _SHA[:12] + "\r\n", "a" * 64])
def test_build_commit_accepts_full_and_abbreviated_shas(monkeypatch, tmp_path, content) -> None:
    from backend.routes import site_misc

    _root_with(monkeypatch, tmp_path, BUILD_COMMIT=content)
    assert site_misc._build_commit() == content.strip()


def test_build_commit_unreadable_is_unknown(monkeypatch, tmp_path) -> None:
    from backend.routes import site_misc

    (tmp_path / "BUILD_COMMIT").mkdir()  # a directory, not a file: OSError on read
    (tmp_path / "VERSION").write_bytes(b"\xff\xfe\x00")  # not UTF-8
    monkeypatch.setattr(site_misc, "_ROOT_DIR", tmp_path)
    assert site_misc._build_commit() == "unknown"
    assert site_misc._app_version() == "dev"


def test_app_version_and_build_commit_read_the_repo_root() -> None:
    """VERSION and BUILD_COMMIT live at the project root (where the stage writes them)."""
    from pathlib import Path

    from backend.routes import site_misc

    repo_root = Path(__file__).resolve().parents[2]
    assert site_misc._ROOT_DIR == repo_root
    assert site_misc._app_version() == (repo_root / "VERSION").read_text(encoding="utf-8").strip()
    assert site_misc._CAR_IMAGES_DIR == repo_root / "car_images"


def test_railway_healthcheck_path_unchanged() -> None:
    """Railway's web healthcheck still probes /health, which still answers 200."""
    import tomllib
    from pathlib import Path

    from backend import main

    cfg = tomllib.loads((Path(__file__).resolve().parents[2] / "railway.toml").read_text(encoding="utf-8"))
    assert cfg["deploy"]["healthcheckPath"] == "/health"
    with main.app.test_client() as c:
        assert c.get("/health").status_code == 200


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
