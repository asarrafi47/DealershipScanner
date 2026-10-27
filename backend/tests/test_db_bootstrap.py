"""P13A.5: the db_bootstrap module, its launcher wiring, and the ADMIN_PASSWORD guard."""

from __future__ import annotations

from pathlib import Path

import pytest

from backend.web import db_bootstrap

ROOT = Path(__file__).resolve().parents[2]

LAUNCHERS = (
    "scripts/docker-entrypoint-web.sh",
    "start.sh",
    "scripts/run-web-local.sh",
    "run.py",
    "deploy/always-on/systemd/sarraficars-web.service",
    "deploy/always-on/launchd/com.sarraficars.web.plist",
)


@pytest.mark.parametrize("rel", LAUNCHERS)
def test_every_launcher_runs_db_bootstrap(rel: str) -> None:
    assert "db_bootstrap" in (ROOT / rel).read_text(encoding="utf-8")


def test_every_gunicorn_launcher_is_listed() -> None:
    """A new file that serves backend.main:app must join LAUNCHERS."""
    found = set()
    for pat in ("*.sh", "*.service", "*.plist"):
        for p in ROOT.rglob(pat):
            rel = p.relative_to(ROOT).as_posix()
            if rel.startswith((".venv", ".git/", "node_modules", ".claude", "workspace", "docs")):
                continue
            if "backend.main:app" in p.read_text(encoding="utf-8", errors="ignore"):
                found.add(rel)
    assert found <= set(LAUNCHERS), found - set(LAUNCHERS)


def test_steps_run_in_order_and_twice(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(
        db_bootstrap, "_resolve", lambda module, attr: (lambda: calls.append(attr))
    )
    monkeypatch.setenv("FLASK_ENV", "development")
    first = db_bootstrap.run_bootstrap()
    second = db_bootstrap.run_bootstrap()
    expected = [s[2] for s in db_bootstrap.STEPS]
    assert first == second == [s[0] for s in db_bootstrap.STEPS]
    assert calls == expected * 2
    assert expected == [
        "init_users_db",
        "init_admin_db",
        "init_inventory_db",
        "init_dealer_portal_db",
        "init_job_queue_schema",
    ]


def test_failing_step_is_named_and_exit_nonzero(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def resolve(module: str, attr: str):
        def run() -> None:
            if attr == "init_inventory_db":
                raise RuntimeError("boom")

        return run

    monkeypatch.setattr(db_bootstrap, "_resolve", resolve)
    monkeypatch.setenv("FLASK_ENV", "development")
    assert db_bootstrap.main() == 1
    err = capsys.readouterr().err
    assert "'inventory'" in err and "boom" in err


def test_real_steps_idempotent(app_factory) -> None:
    """Two real runs against the test databases (the reload already ran them once)."""
    app_factory()
    assert db_bootstrap.main() == 0
    assert db_bootstrap.main() == 0


def test_production_without_admin_password_refuses(monkeypatch: pytest.MonkeyPatch) -> None:
    from backend.utils.production_security import assert_production_security_config

    monkeypatch.setenv("FLASK_ENV", "production")
    monkeypatch.setenv("ADMIN_PASSWORD", "")
    monkeypatch.delenv("DEV_CONSOLE", raising=False)
    monkeypatch.delenv("ALLOW_UNENCRYPTED_USER_DB", raising=False)
    monkeypatch.setenv("USERS_DB_ENCRYPTION_KEY", "x" * 32)
    monkeypatch.setenv("DEV_USERS_DB_ENCRYPTION_KEY", "y" * 32)
    with pytest.raises(RuntimeError, match="ADMIN_PASSWORD"):
        assert_production_security_config()


def test_production_import_without_admin_password_raises(app_factory) -> None:
    with pytest.raises(RuntimeError, match="ADMIN_PASSWORD"):
        app_factory(production=True, ADMIN_PASSWORD=None, ALLOW_DEFAULT_APP_USER="0")
