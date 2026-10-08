"""P2A.3: ``TESTS_BLOCK_NETWORK=1`` refuses non-loopback connects (backend/tests/conftest.py).

The in-process tests install their own ``NetworkGuard`` on their own
``MonkeyPatch``, recording into a stand-in item, so they behave the same whether
or not the session guard is active (CI sets the variable, a dev shell may not)
and never add a ``network_blocked`` property to their own junit case. The
end-to-end test runs a small pytest session in a subprocess with the conftest
loaded as a plugin, to prove the autouse fixture itself: on with the variable,
untouched without it.
"""

from __future__ import annotations

import errno
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import textwrap
import types
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Iterator

import pytest

from backend.tests.conftest import (
    NETWORK_BLOCKED_PROPERTY,
    TESTS_BLOCK_NETWORK_ENV,
    NetworkBlocked,
    NetworkGuard,
    destination_allowed,
    network_guard_enabled,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
FAKE_NODEID = "fake/test_module.py::test_reaches_out"


@pytest.fixture
def guard() -> Iterator[NetworkGuard]:
    g = NetworkGuard()
    g.current = types.SimpleNamespace(nodeid=FAKE_NODEID, user_properties=[])
    mp = pytest.MonkeyPatch()
    g.install(mp)
    try:
        yield g
    finally:
        mp.undo()


# ---------------------------------------------------------------------------
# Refused: public addresses, by IP or by name
# ---------------------------------------------------------------------------


def test_connect_to_public_ip_is_refused_and_counted(guard: NetworkGuard) -> None:
    with pytest.raises(NetworkBlocked) as excinfo:
        socket.create_connection(("8.8.8.8", 53), timeout=2)
    assert excinfo.value.errno == errno.ECONNREFUSED
    assert TESTS_BLOCK_NETWORK_ENV in str(excinfo.value)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(2)
    try:
        with pytest.raises(NetworkBlocked):
            sock.connect(("8.8.8.8", 53))
        assert sock.connect_ex(("8.8.8.8", 53)) == errno.ECONNREFUSED
    finally:
        sock.close()

    assert guard.attempts == {FAKE_NODEID: ["8.8.8.8:53"] * 3}
    # One junit property per distinct destination, not per attempt.
    assert guard.current.user_properties == [(NETWORK_BLOCKED_PROPERTY, "8.8.8.8:53")]


def test_refusal_is_an_oserror_callers_already_handle() -> None:
    # urllib3 wraps an OSError from connect into NewConnectionError, urllib
    # into URLError: code under test sees an unreachable host, not a crash.
    assert issubclass(NetworkBlocked, ConnectionRefusedError)
    assert issubclass(NetworkBlocked, OSError)


def test_public_hostname_is_refused_without_a_dns_lookup(
    guard: NetworkGuard, monkeypatch: pytest.MonkeyPatch
) -> None:
    def no_dns(*_a: object, **_kw: object) -> None:
        raise AssertionError("the guard resolved a name it should refuse outright")

    monkeypatch.setattr(socket, "getaddrinfo", no_dns)
    with pytest.raises(NetworkBlocked):
        socket.create_connection(("example.com", 443), timeout=2)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        with pytest.raises(NetworkBlocked):
            sock.connect(("example.com", 80))
    finally:
        sock.close()
    assert guard.attempts == {FAKE_NODEID: ["example.com:443", "example.com:80"]}


@pytest.mark.parametrize(
    ("family", "address", "allowed"),
    [
        (socket.AF_INET, ("127.0.0.1", 80), True),
        (socket.AF_INET, ("127.8.9.10", 80), True),
        (socket.AF_INET, ("localhost", 5432), True),
        (socket.AF_INET, ("LOCALHOST.", 5432), True),
        (socket.AF_INET, ("api.localhost", 80), True),
        (socket.AF_INET, ("0.0.0.0", 80), True),
        (socket.AF_INET, ("", 80), True),
        (socket.AF_INET6, ("::1", 80, 0, 0), True),
        (socket.AF_INET6, ("::ffff:127.0.0.1", 80, 0, 0), True),
        (socket.AF_INET6, ("::", 80, 0, 0), True),
        (socket.AF_INET, ("8.8.8.8", 53), False),
        (socket.AF_INET, ("192.168.1.89", 15432), False),
        (socket.AF_INET, ("10.0.0.5", 5432), False),
        (socket.AF_INET, ("169.254.169.254", 80), False),
        (socket.AF_INET, ("example.com", 443), False),
        (socket.AF_INET, ("localhost.example.com", 443), False),
        (socket.AF_INET6, ("2001:4860:4860::8888", 53, 0, 0), False),
        (socket.AF_INET6, ("::ffff:8.8.8.8", 53, 0, 0), False),
        (socket.AF_INET6, ("fe80::1%lo0", 80, 0, 0), False),
    ],
)
def test_destination_table(family: int, address: tuple, allowed: bool) -> None:
    assert destination_allowed(family, address) is allowed


def test_unix_socket_family_is_always_allowed() -> None:
    if not hasattr(socket, "AF_UNIX"):
        pytest.skip("platform has no AF_UNIX")
    assert destination_allowed(socket.AF_UNIX, "/var/run/postgresql/.s.PGSQL.5432") is True


# ---------------------------------------------------------------------------
# Allowed: loopback TCP and unix sockets still connect
# ---------------------------------------------------------------------------


def test_localhost_connects_still_work(guard: NetworkGuard) -> None:
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("127.0.0.1", 0))
    server.listen(4)
    port = server.getsockname()[1]
    opened: list[socket.socket] = []
    try:
        opened.append(socket.create_connection(("127.0.0.1", port), timeout=2))
        # "localhost" may resolve to ::1 first; create_connection falls back to
        # 127.0.0.1, and both go through the guarded connect.
        opened.append(socket.create_connection(("localhost", port), timeout=2))
        raw = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        opened.append(raw)
        raw.settimeout(2)
        raw.connect(("127.0.0.1", port))
        probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        opened.append(probe)
        probe.settimeout(2)
        assert probe.connect_ex(("127.0.0.1", port)) == 0
    finally:
        for s in opened:
            s.close()
        server.close()
    assert guard.attempts == {}
    assert guard.current.user_properties == []


def test_unix_socket_connects_still_work(guard: NetworkGuard) -> None:
    if not hasattr(socket, "AF_UNIX"):
        pytest.skip("platform has no AF_UNIX")
    # Short dir: AF_UNIX paths are capped at ~104 bytes on macOS, and pytest's
    # tmp_path under /private/var/folders is often longer than that.
    short_dir = tempfile.mkdtemp(prefix="ng", dir="/tmp" if os.path.isdir("/tmp") else None)
    path = os.path.join(short_dir, "s")
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        server.bind(path)
        server.listen(1)
        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            client.settimeout(2)
            client.connect(path)
        finally:
            client.close()
    finally:
        server.close()
        shutil.rmtree(short_dir, ignore_errors=True)
    assert guard.attempts == {}


def test_undo_restores_the_previous_socket_functions() -> None:
    before = (vars(socket.socket).get("connect"), vars(socket.socket).get("connect_ex"), socket.create_connection)
    mp = pytest.MonkeyPatch()
    NetworkGuard().install(mp)
    assert socket.create_connection is not before[2]
    mp.undo()
    after = (vars(socket.socket).get("connect"), vars(socket.socket).get("connect_ex"), socket.create_connection)
    assert after == before


# ---------------------------------------------------------------------------
# The switch: only "1" turns it on
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "enabled"),
    [(None, False), ("", False), ("0", False), ("true", False), ("1", True), (" 1 ", True)],
)
def test_only_one_enables_the_guard(monkeypatch: pytest.MonkeyPatch, value: str | None, enabled: bool) -> None:
    if value is None:
        monkeypatch.delenv(TESTS_BLOCK_NETWORK_ENV, raising=False)
    else:
        monkeypatch.setenv(TESTS_BLOCK_NETWORK_ENV, value)
    assert network_guard_enabled() is enabled


# ---------------------------------------------------------------------------
# End to end: the autouse fixture in a real pytest session
# ---------------------------------------------------------------------------

_INNER_TEST = textwrap.dedent(
    """
    import os
    import socket

    import pytest

    # The vPIC stub would import backend.enrichment; it is not under test here.
    pytestmark = pytest.mark.real_vpic_client


    def test_public_connect():
        if os.environ.get("TESTS_BLOCK_NETWORK") == "1":
            with pytest.raises(ConnectionRefusedError) as excinfo:
                socket.create_connection(("8.8.8.8", 53), timeout=2)
            assert type(excinfo.value).__name__ == "NetworkBlocked"
        else:
            # Unset: nothing is patched, and this branch never opens a socket.
            assert "connect" not in vars(socket.socket)
            assert "connect_ex" not in vars(socket.socket)
            assert socket.create_connection.__module__ == "socket"


    def test_stays_local():
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(("127.0.0.1", 0))
        server.listen(1)
        try:
            socket.create_connection(server.getsockname(), timeout=2).close()
        finally:
            server.close()
    """
)


def _run_inner_session(tmp_path: Path, block: bool) -> tuple[subprocess.CompletedProcess, Path]:
    (tmp_path / "test_inner.py").write_text(_INNER_TEST, encoding="utf-8")
    ini = tmp_path / "pytest.ini"
    ini.write_text(
        "[pytest]\nmarkers =\n    real_vpic_client: opt out of the vPIC stub\n",
        encoding="utf-8",
    )
    junit = tmp_path / "inner.xml"
    env = {k: v for k, v in os.environ.items() if not k.startswith("PYTEST_")}
    env.pop(TESTS_BLOCK_NETWORK_ENV, None)
    if block:
        env[TESTS_BLOCK_NETWORK_ENV] = "1"
    proc = subprocess.run(
        [
            sys.executable, "-m", "pytest",
            "-p", "backend.tests.conftest",
            "-p", "no:cacheprovider",
            "-c", str(ini),
            "--rootdir", str(tmp_path),
            "-q",
            f"--junitxml={junit}",
            str(tmp_path / "test_inner.py"),
        ],
        cwd=str(REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=90,
    )
    return proc, junit


def _junit_properties(junit: Path) -> dict[str, list[tuple[str, str]]]:
    out: dict[str, list[tuple[str, str]]] = {}
    for case in ET.parse(junit).getroot().iter("testcase"):
        out[case.get("name", "")] = [
            (p.get("name", ""), p.get("value", "")) for p in case.iter("property")
        ]
    return out


def test_autouse_fixture_refuses_and_reports_when_env_is_set(tmp_path: Path) -> None:
    proc, junit = _run_inner_session(tmp_path, block=True)
    output = proc.stdout + proc.stderr
    assert proc.returncode == 0, output
    assert "NETWORK-BLOCKED CONNECTS: 1 in 1 test(s)" in output, output
    assert "test_inner.py::test_public_connect  [1x: 8.8.8.8:53]" in output, output
    assert "test_stays_local" not in output.split("NETWORK-BLOCKED CONNECTS", 1)[1], output
    props = _junit_properties(junit)
    assert props["test_public_connect"] == [(NETWORK_BLOCKED_PROPERTY, "8.8.8.8:53")]
    assert props["test_stays_local"] == []


def test_autouse_fixture_changes_nothing_when_env_is_unset(tmp_path: Path) -> None:
    proc, junit = _run_inner_session(tmp_path, block=False)
    output = proc.stdout + proc.stderr
    assert proc.returncode == 0, output
    assert "NETWORK-BLOCKED" not in output and "NETWORK GUARD" not in output, output
    assert all(not p for p in _junit_properties(junit).values())
