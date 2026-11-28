"""Role-login vault load (backend/utils/vault_role.py) and the static-token path it sits beside.

Every case talks to a fake vault: ``http.server`` on 127.0.0.1 in a thread, no other
network. The clock and the sleep are injected, so backoff and token expiry cost nothing.
"""

from __future__ import annotations

import json
import logging
import os
import re
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Iterator

import pytest

from backend.utils import kmac_vault, vault_role
from backend.utils.kmac_vault import VAULT_SECRET_MAP, load_kmac_vault_secrets
from backend.utils.vault_role import (
    KEY_PREFIX,
    PROTECTED_NAMES,
    VAULT_ROLE_SECRET_NAMES,
    boot_wait_seconds,
    load_role_secrets,
    role_path_configured,
)

ROOT = Path(__file__).resolve().parents[2]

ROLE_ID = "role-id-7d1f0c5e9a2b4c8e"
SECRET_ID = "secret-id-3b9e1a7f4d6c2e0a"
TOKEN = "access-token-5c8a2f1e9d7b3a60"
TOKEN_2 = "access-token-second-0e4d8b2a6c"
STATIC_TOKEN = "static-token-9f2c4e6a8b0d1e3f"


def _val(name: str) -> str:
    return f"vault-value-of-{name.lower()}-" + "x" * 40


def _envval(name: str) -> str:
    return f"env-value-of-{name.lower()}-" + "y" * 40


# ── fake vault ────────────────────────────────────────────────────────────────────


class FakeVault:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}  # full key -> value
        self.login_script: list[tuple[int, dict]] = []  # consumed first; then login_default
        self.login_default: tuple[int, dict] = (200, {"access_token": TOKEN, "expires_in": 900})
        self.get_script: list[int] = []  # status overrides for successive GETs (0 = normal)
        self.get_status: dict[str, int] = {}  # per-key forced status
        self.valid_tokens = {TOKEN, TOKEN_2, STATIC_TOKEN}
        self.requests: list[tuple[str, str, dict, bytes]] = []
        self.lock = threading.Lock()

    def logins(self) -> list[dict]:
        return [json.loads(b) for (m, p, h, b) in self.requests if p == "/auth/token"]

    def gets(self) -> list[str]:
        return [p for (m, p, h, b) in self.requests if p.startswith("/get/")]


def _handler(vault: FakeVault):
    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):  # keep test output quiet
            pass

        def _send(self, status: int, body: dict) -> None:
            raw = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def do_POST(self):
            n = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(n)
            with vault.lock:
                vault.requests.append(("POST", self.path, dict(self.headers), body))
                step = vault.login_script.pop(0) if vault.login_script else vault.login_default
            if self.path != "/auth/token":
                return self._send(404, {"error": "not found"})
            self._send(*step)

        def do_GET(self):
            with vault.lock:
                vault.requests.append(("GET", self.path, dict(self.headers), b""))
                forced = vault.get_script.pop(0) if vault.get_script else 0
            if self.path.startswith("/redirected"):
                return self._send(200, {"key": "x", "value": "leaked"})
            if not self.path.startswith("/get/"):
                return self._send(404, {"error": "not found"})
            key = __import__("urllib.parse").parse.unquote(self.path[len("/get/"):])
            if forced == 503:
                return self._send(503, {"error": "vault is sealed"})
            if forced == 302:
                self.send_response(302)
                self.send_header("Location", "/redirected")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            if forced:
                return self._send(forced, {"error": "forced"})
            auth = self.headers.get("Authorization") or ""
            if not auth.startswith("Bearer ") or auth[7:] not in vault.valid_tokens:
                return self._send(401, {"error": "bad token"})
            if key in vault.get_status:
                return self._send(vault.get_status[key], {"error": "forced"})
            if key not in vault.values:
                return self._send(404, {"error": "missing"})
            self._send(200, {"key": key, "value": vault.values[key]})

    return H


@pytest.fixture
def fake_vault() -> Iterator[tuple[FakeVault, str]]:
    vault = FakeVault()
    server = ThreadingHTTPServer(("127.0.0.1", 0), _handler(vault))
    t = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.02}, daemon=True)
    t.start()
    try:
        yield vault, f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()


class FakeClock:
    def __init__(self, t: float = 1000.0) -> None:
        self.t = t
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.t

    def sleep(self, s: float) -> None:
        self.sleeps.append(s)
        self.t += s


_CLEAN = (
    *VAULT_ROLE_SECRET_NAMES,
    *VAULT_SECRET_MAP,
    "VAULT_ADDR",
    "VAULT_ROLE_ID",
    "VAULT_SECRET_ID",
    "VAULT_BOOT_WAIT_SECONDS",
    "VAULT_TOKEN",
    "VAULT_TOKEN_FILE",
    "VAULT_LOAD_RETRIES",
    "VAULT_HTTP_TIMEOUT",
    "KMAC_VAULT_AUTO",
    "KMAC_VAULT_DISABLE",
    "KMAC_VAULT_RAILWAY_ADDR",
    "RAILWAY_ENVIRONMENT",
    "RAILWAY_PROJECT_ID",
    "RAILWAY_ENVIRONMENT_NAME",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
)


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch, tmp_path):
    for name in _CLEAN:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("FLASK_ENV", "development")
    monkeypatch.setenv("HOME", str(tmp_path))  # no ~/.config/kmac token file
    vault_role.reset_token_cache()
    yield
    vault_role.reset_token_cache()


@pytest.fixture
def role_env(monkeypatch, fake_vault):
    vault, addr = fake_vault
    monkeypatch.setenv("VAULT_ADDR", addr)
    monkeypatch.setenv("VAULT_ROLE_ID", ROLE_ID)
    monkeypatch.setenv("VAULT_SECRET_ID", SECRET_ID)
    return vault, addr


@pytest.fixture
def all_logs(caplog):
    caplog.set_level(logging.DEBUG)  # root: every logger, urllib and http included
    return caplog


def _run(clock: FakeClock | None = None, **kw):
    clock = clock or FakeClock()
    return load_role_secrets(clock=clock, sleep=clock.sleep, **kw), clock


def _fill(vault: FakeVault, names=VAULT_ROLE_SECRET_NAMES) -> None:
    for n in names:
        vault.values[KEY_PREFIX + n] = _val(n)


def _messages(caplog, level: int | None = None) -> list[str]:
    return [r.getMessage() for r in caplog.records if level is None or r.levelno == level]


# ── off / configuration ──────────────────────────────────────────────────────────


def test_off_when_nothing_is_configured(fake_vault, monkeypatch) -> None:
    vault, _ = fake_vault
    monkeypatch.setenv("KMAC_VAULT_AUTO", "0")  # production's setting
    before = dict(os.environ)
    assert role_path_configured() is False
    assert load_kmac_vault_secrets() == []
    assert load_kmac_vault_secrets(static_token=False) == []
    assert load_role_secrets().state == "off"
    assert dict(os.environ) == before
    assert vault.requests == []


@pytest.mark.parametrize("missing", ["VAULT_ADDR", "VAULT_ROLE_ID", "VAULT_SECRET_ID"])
def test_off_unless_all_three_role_variables_are_set(role_env, monkeypatch, missing) -> None:
    vault, _ = role_env
    monkeypatch.setenv(missing, "  ")
    before = dict(os.environ)
    assert role_path_configured() is False
    assert load_kmac_vault_secrets(static_token=False) == []
    assert dict(os.environ) == before
    assert vault.requests == []


def test_kmac_vault_disable_turns_the_role_path_off(role_env, monkeypatch) -> None:
    vault, _ = role_env
    monkeypatch.setenv("KMAC_VAULT_DISABLE", "1")
    _fill(vault)
    assert role_path_configured() is False
    assert load_kmac_vault_secrets() == []
    assert vault.requests == []


def test_role_path_runs_even_with_kmac_vault_auto_off(role_env, monkeypatch) -> None:
    vault, _ = role_env
    monkeypatch.setenv("KMAC_VAULT_AUTO", "0")
    _fill(vault, ["GOOGLE_MAPS_API_KEY"])
    clock = FakeClock()
    monkeypatch.setattr(vault_role.time, "monotonic", clock)
    assert load_kmac_vault_secrets() == ["GOOGLE_MAPS_API_KEY"]
    assert os.environ["GOOGLE_MAPS_API_KEY"] == _val("GOOGLE_MAPS_API_KEY")
    # The role path replaced the static-token path: no Dealer:* key was asked for.
    assert not any("Dealer" in p for p in vault.gets())


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("", 40.0), ("abc", 40.0), ("nan", 40.0), ("inf", 40.0), ("-5", 0.0), ("12.5", 12.5), ("9999", 120.0)],
)
def test_boot_wait_seconds_parsing(monkeypatch, raw, expected) -> None:
    monkeypatch.setenv("VAULT_BOOT_WAIT_SECONDS", raw)
    assert boot_wait_seconds() == expected


def test_secret_names_exclude_database_urls_and_vault_variables() -> None:
    names = set(VAULT_ROLE_SECRET_NAMES)
    assert len(names) == len(VAULT_ROLE_SECRET_NAMES)
    for n in names:
        assert "DATABASE_URL" not in n and "DSN" not in n and not n.startswith("VAULT_"), n
        assert not n.startswith("REVEST_"), n
    required = {
        "SECRET_KEY", "USERS_DB_ENCRYPTION_KEY", "DEV_USERS_DB_ENCRYPTION_KEY", "ADMIN_PASSWORD",
        "ANTHROPIC_API_KEY", "GOOGLE_MAPS_API_KEY", "REVIEWS_IP_HASH_SALT", "TWILIO_ACCOUNT_SID",
        "TWILIO_API_KEY_SID", "TWILIO_API_KEY_SECRET", "TWILIO_VERIFY_SERVICE_SID", "TURNSTILE_SECRET_KEY",
        "PHONE_HASH_PEPPER", "CF_ORIGIN_SECRET",
    }
    assert required <= names
    assert PROTECTED_NAMES <= names


def test_every_secret_name_is_read_by_the_code() -> None:
    """Only names the code actually reads (outside tests and this loader)."""
    src = []
    for p in list((ROOT / "backend").rglob("*.py")) + list((ROOT / "scripts").rglob("*.py")):
        if "/tests/" in p.as_posix() or p.name in ("vault_role.py", "kmac_vault.py"):
            continue
        src.append(p.read_text(encoding="utf-8", errors="replace"))
    blob = "\n".join(src)
    for n in VAULT_ROLE_SECRET_NAMES:
        assert re.search(rf"[\"']{n}[\"']", blob), n


# ── role login ───────────────────────────────────────────────────────────────────


def test_role_login_loads_every_name(role_env, all_logs) -> None:
    vault, _ = role_env
    _fill(vault)
    report, clock = _run()
    assert report.state == "ok"
    assert report.from_vault == list(VAULT_ROLE_SECRET_NAMES)
    assert report.missing == [] and report.kept_env == []
    for n in VAULT_ROLE_SECRET_NAMES:
        assert os.environ[n] == _val(n)
    assert vault.logins() == [{"role_id": ROLE_ID, "secret_id": SECRET_ID}]
    assert len(vault.gets()) == len(VAULT_ROLE_SECRET_NAMES)
    assert all(p.startswith("/get/dealershipscanner:") for p in vault.gets())
    assert all(h.get("Authorization") == f"Bearer {TOKEN}" for (m, p, h, b) in vault.requests if m == "GET")
    assert clock.sleeps == []
    info = _messages(all_logs, logging.INFO)
    assert len([m for m in info if m.startswith("vault role load")]) == 1
    # Token in memory only.
    assert TOKEN not in os.environ.values()


def test_sealed_then_unsealed_within_budget(role_env) -> None:
    vault, _ = role_env
    _fill(vault, ["ANTHROPIC_API_KEY"])
    vault.login_script = [(503, {"error": "vault is sealed"})] * 3
    report, clock = _run()
    assert clock.sleeps == [1.0, 2.0, 4.0]
    assert report.state == "ok"
    assert os.environ["ANTHROPIC_API_KEY"] == _val("ANTHROPIC_API_KEY")


def test_sealed_on_a_key_then_unsealed(role_env) -> None:
    vault, _ = role_env
    _fill(vault)
    vault.get_script = [503, 503]
    report, clock = _run()
    assert clock.sleeps == [1.0, 2.0]
    assert report.from_vault == list(VAULT_ROLE_SECRET_NAMES)


def test_sealed_past_budget_falls_back_to_env(role_env, monkeypatch, all_logs) -> None:
    vault, _ = role_env
    vault.login_default = (503, {"error": "vault is sealed"})
    monkeypatch.setenv("ADMIN_PASSWORD", _envval("ADMIN_PASSWORD"))
    monkeypatch.setenv("SECRET_KEY", _envval("SECRET_KEY"))
    report, clock = _run()
    assert report.state == "sealed"
    assert clock.sleeps[:5] == [1.0, 2.0, 4.0, 8.0, 15.0]
    assert all(s <= 15.0 for s in clock.sleeps)
    assert 39.0 <= sum(clock.sleeps) <= 40.0
    assert os.environ["ADMIN_PASSWORD"] == _envval("ADMIN_PASSWORD")
    assert os.environ["SECRET_KEY"] == _envval("SECRET_KEY")
    assert "ADMIN_PASSWORD" in report.kept_env and "SECRET_KEY" in report.kept_env
    assert vault.gets() == []
    assert any("sealed past the 40s boot budget" in m for m in _messages(all_logs, logging.WARNING))


def test_budget_honours_env_and_the_cap(role_env, monkeypatch) -> None:
    vault, _ = role_env
    vault.login_default = (503, {"error": "vault is sealed"})
    monkeypatch.setenv("VAULT_BOOT_WAIT_SECONDS", "7")
    report, clock = _run()
    assert sum(clock.sleeps) == 7.0 and clock.sleeps == [1.0, 2.0, 4.0]
    vault_role.reset_token_cache()
    report, clock = _run(max_wait_seconds=3)
    assert sum(clock.sleeps) == 3.0


def test_unreachable_vault_falls_back(monkeypatch) -> None:
    import socket

    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()  # nothing listens here now
    monkeypatch.setenv("VAULT_ADDR", f"http://127.0.0.1:{port}")
    monkeypatch.setenv("VAULT_ROLE_ID", ROLE_ID)
    monkeypatch.setenv("VAULT_SECRET_ID", SECRET_ID)
    monkeypatch.setenv("VAULT_BOOT_WAIT_SECONDS", "3")
    monkeypatch.setenv("ANTHROPIC_API_KEY", _envval("ANTHROPIC_API_KEY"))
    report, clock = _run()
    assert report.state == "unreachable"
    assert sum(clock.sleeps) == 3.0
    assert os.environ["ANTHROPIC_API_KEY"] == _envval("ANTHROPIC_API_KEY")


def test_server_error_is_retried_like_unreachable(role_env) -> None:
    vault, _ = role_env
    _fill(vault, ["GOOGLE_MAPS_API_KEY"])
    vault.login_script = [(500, {"error": "boom"}), (502, {"error": "boom"})]
    report, clock = _run()
    assert clock.sleeps == [1.0, 2.0] and report.state == "ok"


def test_missing_key_falls_back_and_unset_names_are_critical(role_env, monkeypatch, all_logs) -> None:
    vault, _ = role_env
    monkeypatch.setenv("TWILIO_ACCOUNT_SID", _envval("TWILIO_ACCOUNT_SID"))
    report, _ = _run()
    assert report.state == "ok"
    assert os.environ["TWILIO_ACCOUNT_SID"] == _envval("TWILIO_ACCOUNT_SID")
    assert report.kept_env == ["TWILIO_ACCOUNT_SID"]
    assert "STRIPE_SECRET_KEY" in report.missing and "TWILIO_ACCOUNT_SID" not in report.missing
    crit = _messages(all_logs, logging.CRITICAL)
    line = [m for m in crit if m.startswith("no value from the vault or the environment")]
    assert len(line) == 1 and "STRIPE_SECRET_KEY" in line[0] and "TWILIO_ACCOUNT_SID" not in line[0]


def test_empty_vault_value_counts_as_missing(role_env, monkeypatch) -> None:
    vault, _ = role_env
    vault.values[KEY_PREFIX + "RESEND_API_KEY"] = "   "
    monkeypatch.setenv("RESEND_API_KEY", _envval("RESEND_API_KEY"))
    report, _ = _run()
    assert os.environ["RESEND_API_KEY"] == _envval("RESEND_API_KEY")
    assert "RESEND_API_KEY" in report.kept_env


def test_login_401_is_critical_and_falls_back(role_env, monkeypatch, all_logs) -> None:
    vault, _ = role_env
    _fill(vault)
    vault.login_default = (401, {"error": "bad credentials"})
    monkeypatch.setenv("ANTHROPIC_API_KEY", _envval("ANTHROPIC_API_KEY"))
    report, clock = _run()
    assert report.state == "refused"
    assert vault.gets() == [] and len(vault.logins()) == 1 and clock.sleeps == []
    assert os.environ["ANTHROPIC_API_KEY"] == _envval("ANTHROPIC_API_KEY")
    crit = _messages(all_logs, logging.CRITICAL)
    assert len([m for m in crit if "refused the role login (HTTP 401)" in m]) == 1


def test_get_401_relogs_once_then_gives_up(role_env, monkeypatch, all_logs) -> None:
    vault, _ = role_env
    _fill(vault)
    vault.valid_tokens = set()  # every token is rejected
    monkeypatch.setenv("SECRET_KEY", _envval("SECRET_KEY"))
    report, _ = _run()
    assert report.state == "refused"
    assert len(vault.logins()) == 2  # the first login and one re-login
    assert len(vault.gets()) == 2
    assert os.environ["SECRET_KEY"] == _envval("SECRET_KEY")
    crit = _messages(all_logs, logging.CRITICAL)
    assert len([m for m in crit if "HTTP 401" in m]) == 1
    assert vault_role._TOKEN.value == ""


def test_get_401_then_relogin_succeeds(role_env) -> None:
    vault, _ = role_env
    _fill(vault)
    vault.valid_tokens = {TOKEN_2}
    vault.login_script = [(200, {"access_token": TOKEN, "expires_in": 900})]
    vault.login_default = (200, {"access_token": TOKEN_2, "expires_in": 900})
    report, _ = _run()
    assert report.state == "ok" and report.from_vault == list(VAULT_ROLE_SECRET_NAMES)
    assert len(vault.logins()) == 2


def test_403_outside_scope_is_one_critical_and_falls_back(role_env, monkeypatch, all_logs) -> None:
    vault, _ = role_env
    _fill(vault)
    vault.get_status = {KEY_PREFIX + "STRIPE_SECRET_KEY": 403, KEY_PREFIX + "SMTP_PASSWORD": 403}
    monkeypatch.setenv("STRIPE_SECRET_KEY", _envval("STRIPE_SECRET_KEY"))
    report, _ = _run()
    assert report.forbidden == ["STRIPE_SECRET_KEY", "SMTP_PASSWORD"]
    assert os.environ["STRIPE_SECRET_KEY"] == _envval("STRIPE_SECRET_KEY")
    assert os.environ["ANTHROPIC_API_KEY"] == _val("ANTHROPIC_API_KEY")
    crit = [m for m in _messages(all_logs, logging.CRITICAL) if "HTTP 403" in m]
    assert len(crit) == 1 and "STRIPE_SECRET_KEY" in crit[0] and "SMTP_PASSWORD" in crit[0]


def test_redirect_is_not_followed(role_env) -> None:
    vault, _ = role_env
    _fill(vault)
    vault.get_script = [302]
    report, _ = _run()
    assert not any(p.startswith("/redirected") for (m, p, h, b) in vault.requests)
    assert VAULT_ROLE_SECRET_NAMES[0] not in report.from_vault


def test_vault_value_overrides_env(role_env, monkeypatch) -> None:
    vault, _ = role_env
    _fill(vault)
    monkeypatch.setenv("ANTHROPIC_API_KEY", _envval("ANTHROPIC_API_KEY"))
    monkeypatch.setenv("ADMIN_PASSWORD", _envval("ADMIN_PASSWORD"))
    report, _ = _run()
    assert os.environ["ANTHROPIC_API_KEY"] == _val("ANTHROPIC_API_KEY")
    assert os.environ["ADMIN_PASSWORD"] == _val("ADMIN_PASSWORD")
    assert "ANTHROPIC_API_KEY" in report.from_vault


@pytest.mark.parametrize("name", sorted(PROTECTED_NAMES))
def test_protected_name_keeps_env_on_mismatch(role_env, monkeypatch, all_logs, name) -> None:
    vault, _ = role_env
    _fill(vault)
    monkeypatch.setenv(name, _envval(name))
    report, _ = _run()
    assert os.environ[name] == _envval(name)
    assert name in report.kept_env and name in report.protected_kept and name not in report.from_vault
    crit = [m for m in _messages(all_logs, logging.CRITICAL) if "differs from the environment" in m]
    # FLASK_SECRET_KEY alone is also the effective SECRET_KEY, so that one is kept as well.
    expected = {name, "SECRET_KEY"} if name == "FLASK_SECRET_KEY" else {name}
    assert len(crit) == len(expected) and all(any(f"for {n} " in m for m in crit) for n in expected)


@pytest.mark.parametrize("name", sorted(PROTECTED_NAMES))
def test_protected_name_with_equal_or_empty_env_takes_the_vault(role_env, monkeypatch, all_logs, name) -> None:
    vault, _ = role_env
    _fill(vault)
    monkeypatch.setenv(name, _val(name))
    report, _ = _run()
    assert name in report.from_vault
    assert not [m for m in _messages(all_logs, logging.CRITICAL) if f"for {name} differs" in m]


def test_secret_key_compares_against_the_flask_secret_key_alias(role_env, monkeypatch, all_logs) -> None:
    """Config.secret_key_raw is SECRET_KEY or FLASK_SECRET_KEY: filling SECRET_KEY from the
    vault while only FLASK_SECRET_KEY is set would change the session key."""
    vault, _ = role_env
    _fill(vault, ["SECRET_KEY"])
    monkeypatch.setenv("FLASK_SECRET_KEY", _envval("FLASK_SECRET_KEY"))
    report, _ = _run()
    assert not (os.environ.get("SECRET_KEY") or "")
    assert os.environ["FLASK_SECRET_KEY"] == _envval("FLASK_SECRET_KEY")
    assert "SECRET_KEY" in report.protected_kept
    from backend.config import Config

    assert Config.secret_key_raw() == _envval("FLASK_SECRET_KEY")


def test_token_refreshes_sixty_seconds_before_expiry(role_env) -> None:
    vault, _ = role_env
    _fill(vault, ["GOOGLE_MAPS_API_KEY"])
    clock = FakeClock(0.0)
    _run(clock)
    assert len(vault.logins()) == 1
    clock.t = 839.0  # 900 - 60 - 1: still cached
    _run(clock)
    assert len(vault.logins()) == 1
    clock.t = 840.0  # inside the 60 s margin: refreshed
    _run(clock)
    assert len(vault.logins()) == 2


def test_short_lived_token_is_still_used_once(role_env) -> None:
    vault, _ = role_env
    _fill(vault, ["GOOGLE_MAPS_API_KEY"])
    vault.login_default = (200, {"access_token": TOKEN, "expires_in": 30})
    report, _ = _run()
    assert "GOOGLE_MAPS_API_KEY" in report.from_vault


def test_short_ttl_refreshes_at_half_its_lifetime_not_once_per_key(role_env) -> None:
    """expires_in 30: the margin is min(60, 30 / 2) = 15 s, so one login serves every key."""
    vault, _ = role_env
    _fill(vault)
    vault.login_default = (200, {"access_token": TOKEN, "expires_in": 30})
    clock = FakeClock(0.0)
    report, _ = _run(clock)
    assert report.from_vault == list(VAULT_ROLE_SECRET_NAMES)
    assert len(vault.logins()) == 1
    clock.t = 14.0  # still cached
    _run(clock)
    assert len(vault.logins()) == 1
    clock.t = 15.0  # inside the 15 s margin: refreshed
    _run(clock)
    assert len(vault.logins()) == 2


class _Recorder(BaseHTTPRequestHandler):
    hits: list[str] = []

    def log_message(self, *a):
        pass

    def _any(self):
        _Recorder.hits.append(self.path)
        self.send_response(502)
        self.send_header("Content-Length", "0")
        self.end_headers()

    do_GET = do_POST = do_CONNECT = _any


def test_proxy_environment_is_ignored(role_env, monkeypatch) -> None:
    """Role credentials and the token never go to an HTTP(S)_PROXY from the environment."""
    vault, _ = role_env
    _fill(vault, ["GOOGLE_MAPS_API_KEY"])
    _Recorder.hits = []
    proxy = ThreadingHTTPServer(("127.0.0.1", 0), _Recorder)
    t = threading.Thread(target=proxy.serve_forever, kwargs={"poll_interval": 0.02}, daemon=True)
    t.start()
    try:
        purl = f"http://127.0.0.1:{proxy.server_address[1]}"
        for var in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
            monkeypatch.setenv(var, purl)
        monkeypatch.setenv("NO_PROXY", "")
        monkeypatch.setenv("no_proxy", "")
        report, _ = _run()
    finally:
        proxy.shutdown()
        proxy.server_close()
    assert _Recorder.hits == []
    assert "GOOGLE_MAPS_API_KEY" in report.from_vault and len(vault.logins()) == 1


def test_dripping_server_is_cut_off_at_the_hard_limit(monkeypatch, all_logs) -> None:
    """The urllib timeout is per read: a server sending 1 byte a second never trips it.
    The join on the worker thread ends the load anyway, and nothing is applied."""
    import time as _time

    stop = threading.Event()

    class Drip(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _drip(self):
            try:
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", "100000")
                self.end_headers()
                self.wfile.flush()
                while not stop.is_set():
                    self.wfile.write(b" ")
                    self.wfile.flush()
                    stop.wait(1.0)
            except OSError:
                pass

        do_GET = do_POST = _drip

    server = ThreadingHTTPServer(("127.0.0.1", 0), Drip)
    server.daemon_threads = True
    t = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.02}, daemon=True)
    t.start()
    try:
        monkeypatch.setenv("VAULT_ADDR", f"http://127.0.0.1:{server.server_address[1]}")
        monkeypatch.setenv("VAULT_ROLE_ID", ROLE_ID)
        monkeypatch.setenv("VAULT_SECRET_ID", SECRET_ID)
        monkeypatch.setenv("VAULT_BOOT_WAIT_SECONDS", "1")
        monkeypatch.setattr(vault_role, "HARD_DEADLINE_EXTRA_SECONDS", 1.0)
        monkeypatch.setenv("ANTHROPIC_API_KEY", _envval("ANTHROPIC_API_KEY"))
        before = dict(os.environ)
        t0 = _time.monotonic()
        report = load_role_secrets()  # real clock and sleep
        elapsed = _time.monotonic() - t0
        assert elapsed < 2.0 + 1.0, elapsed  # budget 1 + extra 1, plus a small margin
        assert report.state == "timeout" and report.from_vault == []
        assert dict(os.environ) == before
        assert any("did not finish within 2s" in m for m in _messages(all_logs, logging.WARNING))
        # max_wait_seconds caps the whole load, the extra included.
        t0 = _time.monotonic()
        report = load_role_secrets(max_wait_seconds=0.5)
        assert _time.monotonic() - t0 < 0.5 + 1.0
        assert report.state == "timeout"
    finally:
        stop.set()
        server.shutdown()
        server.server_close()


def test_bad_vault_addr_scheme_is_skipped(monkeypatch, all_logs) -> None:
    monkeypatch.setenv("VAULT_ADDR", "ftp://somewhere")
    monkeypatch.setenv("VAULT_ROLE_ID", ROLE_ID)
    monkeypatch.setenv("VAULT_SECRET_ID", SECRET_ID)
    report, _ = _run()
    assert report.state == "error"
    assert any("not an http(s) URL" in m for m in _messages(all_logs, logging.CRITICAL))


# ── never raises ─────────────────────────────────────────────────────────────────


class _Boom(Exception):
    pass


def test_loader_never_raises(role_env, monkeypatch, all_logs) -> None:
    vault, _ = role_env
    _fill(vault)

    def bad_clock():
        raise _Boom("response body with " + TOKEN)

    report = load_role_secrets(clock=bad_clock, sleep=lambda s: None)
    assert report.state == "error"
    crit = _messages(all_logs, logging.CRITICAL)
    assert any("_Boom" in m for m in crit) and not any(TOKEN in m for m in crit)

    class BadOpener:
        def open(self, *a, **k):
            raise _Boom("x")

    report, _ = _run(opener=BadOpener())
    assert report.state == "error"  # login gave an unexpected answer

    monkeypatch.setattr(vault_role, "_apply", lambda *a: (_ for _ in ()).throw(_Boom()))
    assert load_kmac_vault_secrets() == []


# ── nothing secret in any log record ─────────────────────────────────────────────


def test_no_secret_role_id_secret_id_token_or_address_in_any_log(role_env, monkeypatch, all_logs, capsys) -> None:
    vault, addr = role_env
    _fill(vault)
    for n in PROTECTED_NAMES:
        monkeypatch.setenv(n, _envval(n))
    monkeypatch.setenv("ANTHROPIC_API_KEY", _envval("ANTHROPIC_API_KEY"))
    vault.get_status = {KEY_PREFIX + "SMTP_PASSWORD": 403, KEY_PREFIX + "RESEND_API_KEY": 404}
    vault.get_script = [503, 302]
    _run()
    vault_role.reset_token_cache()
    vault.valid_tokens = set()
    _run()
    vault_role.reset_token_cache()
    vault.login_default = (401, {"error": "nope " + SECRET_ID})
    _run()
    vault.login_default = (503, {"error": "vault is sealed"})
    _run()

    forbidden = [ROLE_ID, SECRET_ID, TOKEN, TOKEN_2, addr, addr.split("//", 1)[1]]
    forbidden += [_val(n) for n in VAULT_ROLE_SECRET_NAMES] + [_envval(n) for n in VAULT_ROLE_SECRET_NAMES]
    assert all_logs.records
    for rec in all_logs.records:
        text = rec.getMessage() + " " + str(rec.exc_text or "") + " " + str(rec.args or "")
        for f in forbidden:
            assert f not in text, (rec.name, rec.levelname)
        assert rec.exc_info is None
    out = capsys.readouterr()
    for f in forbidden:
        assert f not in out.out and f not in out.err


def test_summary_reaches_stderr_without_logging_handlers(role_env, monkeypatch, capsys) -> None:
    """gunicorn --preload imports backend.main before any logging setup: the INFO summary
    must still reach the service log."""
    vault, _ = role_env
    _fill(vault, ["GOOGLE_MAPS_API_KEY"])
    monkeypatch.setattr(vault_role._log, "hasHandlers", lambda: False)
    _run()
    err = capsys.readouterr().err
    assert err.count("vault_role: vault role load (ok)") == 1
    assert _val("GOOGLE_MAPS_API_KEY") not in err


# ── the static-token path is unchanged ───────────────────────────────────────────


def test_static_token_path_unchanged(fake_vault, monkeypatch) -> None:
    vault, addr = fake_vault
    for env_name, key in VAULT_SECRET_MAP.items():
        vault.values[key] = "static-" + key
    monkeypatch.setenv("VAULT_ADDR", addr)
    monkeypatch.setenv("VAULT_TOKEN", STATIC_TOKEN)
    monkeypatch.setenv("KMAC_VAULT_AUTO", "1")
    monkeypatch.setenv("ADMIN_PASSWORD", "already-set")  # only empty vars are filled
    loaded = load_kmac_vault_secrets()
    expected = [n for n in VAULT_SECRET_MAP if n != "ADMIN_PASSWORD"]
    assert loaded == expected
    assert os.environ["ADMIN_PASSWORD"] == "already-set"
    for n in expected:
        assert os.environ[n] == "static-" + VAULT_SECRET_MAP[n]
    assert vault.logins() == []
    assert all(h.get("Authorization") == f"Bearer {STATIC_TOKEN}" for (m, p, h, b) in vault.requests)
    assert "/get/Dealer:site_admin_password" not in vault.gets()
    # static_token=False leaves the static path alone
    for n in expected:
        monkeypatch.delenv(n)
    n_req = len(vault.requests)
    assert load_kmac_vault_secrets(static_token=False) == []
    assert len(vault.requests) == n_req


def test_static_token_path_off_with_kmac_vault_auto_zero(fake_vault, monkeypatch) -> None:
    vault, addr = fake_vault
    monkeypatch.setenv("VAULT_ADDR", addr)
    monkeypatch.setenv("VAULT_TOKEN", STATIC_TOKEN)
    monkeypatch.setenv("KMAC_VAULT_AUTO", "0")
    assert load_kmac_vault_secrets() == []
    assert vault.requests == []


def test_static_token_retries_on_railway(fake_vault, monkeypatch) -> None:
    """On Railway the static path retries VAULT_LOAD_RETRIES times, 2 s apart, as before."""
    vault, addr = fake_vault
    monkeypatch.setenv("VAULT_ADDR", addr)
    monkeypatch.setenv("VAULT_TOKEN", "not-a-valid-token")
    monkeypatch.setenv("RAILWAY_ENVIRONMENT", "test")
    monkeypatch.setenv("VAULT_LOAD_RETRIES", "3")
    sleeps: list[float] = []
    monkeypatch.setattr(kmac_vault.time, "sleep", sleeps.append)
    # Keep the static path on the fake vault only: no Railway or Docker host lookups.
    monkeypatch.setattr(kmac_vault, "_vault_addrs", lambda: [addr])
    assert load_kmac_vault_secrets() == []
    assert sleeps == [2.0, 2.0]


# ── the boot seam ────────────────────────────────────────────────────────────────


def test_web_main_loads_the_vault_before_config_and_the_rest() -> None:
    text = (ROOT / "backend/main.py").read_text(encoding="utf-8")
    call = text.index("\nload_kmac_vault_secrets()")
    assert text.index("load_project_dotenv()") < call < text.index("from backend.config import Config")
    assert call < text.index("from flask import Flask")


def test_vault_modules_import_nothing_that_reads_secrets() -> None:
    for mod in ("backend/utils/vault_role.py", "backend/utils/kmac_vault.py", "gunicorn.conf.py"):
        text = (ROOT / mod).read_text(encoding="utf-8")
        imports = re.findall(r"^\s*(?:from|import) (backend\.[\w.]+)", text, re.M)
        assert set(imports) <= {"backend.utils.runtime_env", "backend.utils", "backend.main",
                                "backend.utils.aggregate_warmup"}, (mod, imports)


def test_web_entrypoint_skips_the_probe_with_the_role_login() -> None:
    text = (ROOT / "scripts/docker-entrypoint-web.sh").read_text(encoding="utf-8")
    role = text.index('if [ -n "${VAULT_ROLE_ID:-}" ]; then')
    probe = text.index('elif [ "${KMAC_VAULT_AUTO}" != "0" ]; then')
    assert role < probe < text.index("load_kmac_vault_secrets") < text.index("bootstrap_site_admin")
    for name in ("VAULT_SECRET_ID", "VAULT_ROLE_ID=", "echo \"$"):
        assert name not in text.replace('"${VAULT_ROLE_ID:-}"', "")


@pytest.mark.parametrize("path", ["scripts/docker-entrypoint-scanner-scheduler.sh", "scripts/docker-entrypoint-jobs-worker.sh"])
def test_loop_entrypoints_skip_the_probe_with_the_role_login(path) -> None:
    text = (ROOT / path).read_text(encoding="utf-8")
    assert text.index('if [ -n "${VAULT_ROLE_ID:-}" ]; then') < text.index('python3 -c "from backend.utils.kmac_vault')


def test_bootstrap_and_fleet_scan_load_through_the_same_function() -> None:
    boot = (ROOT / "scripts/bootstrap_site_admin.py").read_text(encoding="utf-8")
    main_body = boot[boot.index("def main() -> int:"):]
    assert main_body.index("_load_vault_role_secrets()") < main_body.index("init_users_db()")
    assert "load_kmac_vault_secrets(static_token=False, max_wait_seconds=BOOTSTRAP_VAULT_WAIT_CAP_S)" in boot
    fleet = (ROOT / "backend/scripts/fleet_scan.py").read_text(encoding="utf-8")
    tail = fleet[fleet.index('if __name__ == "__main__":'):]
    assert tail.index("_load_vault_role_secrets()") < tail.index("sys.exit(main())")
    assert "load_kmac_vault_secrets(static_token=False)" in fleet
    # scanner-nightly runs railway_scan_fleet.sh, which execs the fleet scan module.
    assert "exec python3 -m backend.scripts.fleet_scan" in (ROOT / "scripts/railway_scan_fleet.sh").read_text()
    for loop in ("scripts/scanner_worker_loop.py", "scripts/scanner_scheduler_loop.py"):
        assert "load_kmac_vault_secrets()" in (ROOT / loop).read_text(encoding="utf-8")


def test_bootstrap_site_admin_uses_the_vault_admin_password(role_env) -> None:
    """End to end: the admin bootstrap's vault step (a subprocess before gunicorn) sees the vault value.

    Runs the bootstrap module's own load step in a fresh interpreter, then reports only whether
    ADMIN_PASSWORD now holds the vault value (never the value itself); no users database is opened.
    """
    vault, addr = role_env
    vault.values[KEY_PREFIX + "ADMIN_PASSWORD"] = "vault-admin-password-123"
    env = {
        **os.environ,
        "PYTHONPATH": str(ROOT),
        "ADMIN_PASSWORD": "env-admin-password-456",
        "FLASK_ENV": "development",
        "VAULT_BOOT_WAIT_SECONDS": "2",
    }
    code = (
        "import os, runpy, sys\n"
        "mod = runpy.run_path(sys.argv[1], run_name='bootstrap_probe')\n"
        "mod['_load_vault_role_secrets']()\n"
        "print('VAULT_VALUE_APPLIED' if os.environ.get('ADMIN_PASSWORD') == sys.argv[2] else 'NOT_APPLIED')\n"
    )
    proc = subprocess.run(
        [sys.executable, "-c", code, str(ROOT / "scripts/bootstrap_site_admin.py"), "vault-admin-password-123"],
        capture_output=True, text=True, env=env, check=False, timeout=120,
    )
    assert proc.returncode == 0, proc.stderr[-2000:]
    assert "VAULT_VALUE_APPLIED" in proc.stdout
    assert len(vault.logins()) == 1
    assert "/get/dealershipscanner:ADMIN_PASSWORD" in vault.gets()
    for secret in ("vault-admin-password-123", ROLE_ID, SECRET_ID, TOKEN):
        assert secret not in proc.stdout and secret not in proc.stderr