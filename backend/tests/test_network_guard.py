"""P2A.3: ``TESTS_BLOCK_NETWORK=1`` refuses non-loopback connects (backend/tests/conftest.py).

The in-process tests install their own ``NetworkGuard`` on their own
``MonkeyPatch``, recording into a stand-in item, so they behave the same whether
or not the session guard is active (CI sets the variable, a dev shell may not)
and never add a ``network_blocked`` property to their own junit case. The
end-to-end test runs a small pytest session in a subprocess with the conftest
loaded as a plugin, to prove the autouse fixture itself: on with the variable,
untouched without it.

curl_cffi (the scanner's main HTTP client) goes through libcurl's C sockets, so
the socket patch cannot see it; its own cases prove the curl_cffi layer refuses
before libcurl runs (``libcurl_sealed`` replaces libcurl's transfer with a
tripwire underneath the guard) and that loopback requests still work. They skip
only where curl_cffi is not installed; it is in requirements.txt.
"""

from __future__ import annotations

import asyncio
import errno
import http.server
import importlib.util
import io
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import textwrap
import threading
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
    url_destination,
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
# curl_cffi: libcurl connects in C, below the socket patch
# ---------------------------------------------------------------------------


@pytest.fixture
def cffi() -> types.ModuleType:
    pytest.importorskip("curl_cffi")  # in requirements.txt; skip only where absent
    from curl_cffi import requests as cffi_requests

    return cffi_requests


@pytest.fixture
def libcurl_sealed(cffi: types.ModuleType) -> Iterator[tuple[NetworkGuard, list[str]]]:
    """A guard installed over a libcurl whose transfers are tripwires.

    Everything libcurl does on the wire - the DNS lookup, the connect - happens
    inside ``Curl.perform`` (sync) or after ``AsyncCurl.add_handle`` (async).
    Both are replaced before the guard wraps them, so a refused request that
    reached libcurl shows up in ``reached`` instead of on the network.
    """
    from curl_cffi.aio import AsyncCurl
    from curl_cffi.curl import Curl

    reached: list[str] = []

    def tripwire_perform(curl: object, *_a: object, **_kw: object) -> None:
        reached.append("Curl.perform")
        raise AssertionError("a refused request reached libcurl's perform")

    def tripwire_add_handle(acurl: object, curl: object) -> None:
        reached.append("AsyncCurl.add_handle")
        raise AssertionError("a refused request reached libcurl's multi handle")

    mp = pytest.MonkeyPatch()
    mp.setattr(Curl, "perform", tripwire_perform)
    mp.setattr(AsyncCurl, "add_handle", tripwire_add_handle)
    g = NetworkGuard()
    g.current = types.SimpleNamespace(nodeid=FAKE_NODEID, user_properties=[])
    g.install(mp)
    try:
        yield g, reached
    finally:
        mp.undo()


def test_curl_cffi_requests_to_public_hosts_are_refused_before_libcurl(
    cffi: types.ModuleType, libcurl_sealed: tuple[NetworkGuard, list[str]]
) -> None:
    guard, reached = libcurl_sealed
    assert guard.curl_cffi_guarded is True

    # The shape the scanner uses (chain.py's impersonate stage, net/client.py).
    with pytest.raises(cffi.exceptions.ConnectionError) as excinfo:
        cffi.get("https://d.example/vdp", impersonate="chrome", timeout=5, proxies=None)
    error = excinfo.value
    assert error.code == 7  # CURLE_COULDNT_CONNECT
    assert isinstance(error.__cause__, NetworkBlocked)
    assert TESTS_BLOCK_NETWORK_ENV in str(error)
    assert isinstance(error, OSError)  # callers catching OSError still do

    session = cffi.Session(base_url="https://api.example.com/")
    try:
        with pytest.raises(cffi.exceptions.RequestException):
            session.post("/v1/items", data=b"x")  # relative: joined with base_url
        with pytest.raises(cffi.exceptions.ConnectionError):
            session.request("GET", "http://192.0.2.1/")  # TEST-NET-1: a real connect would hang
    finally:
        session.close()

    assert reached == []
    assert guard.attempts == {
        FAKE_NODEID: [
            "d.example:443 (curl_cffi)",
            "api.example.com:443 (curl_cffi)",
            "192.0.2.1:80 (curl_cffi)",
        ]
    }
    assert guard.current.user_properties == [
        (NETWORK_BLOCKED_PROPERTY, "d.example:443 (curl_cffi)"),
        (NETWORK_BLOCKED_PROPERTY, "api.example.com:443 (curl_cffi)"),
        (NETWORK_BLOCKED_PROPERTY, "192.0.2.1:80 (curl_cffi)"),
    ]


def test_curl_cffi_async_requests_are_refused_before_libcurl(
    cffi: types.ModuleType, libcurl_sealed: tuple[NetworkGuard, list[str]]
) -> None:
    guard, reached = libcurl_sealed

    async def fetch() -> None:
        async with cffi.AsyncSession() as session:
            await session.get("https://d.example/vdp", timeout=5)

    with pytest.raises(cffi.exceptions.ConnectionError) as excinfo:
        asyncio.run(fetch())
    assert isinstance(excinfo.value.__cause__, NetworkBlocked)
    assert reached == []
    assert guard.attempts == {FAKE_NODEID: ["d.example:443 (curl_cffi)"]}


def test_curl_handle_backstop_refuses_direct_users_and_public_proxies(
    cffi: types.ModuleType, libcurl_sealed: tuple[NetworkGuard, list[str]]
) -> None:
    from curl_cffi import Curl, CurlError, CurlOpt

    guard, reached = libcurl_sealed
    curl = Curl()
    try:
        curl.setopt(CurlOpt.URL, b"https://d.example/vdp")
        with pytest.raises(CurlError) as excinfo:
            curl.perform()
        assert excinfo.value.code == 7
        assert isinstance(excinfo.value.__cause__, NetworkBlocked)
        dup = curl.duphandle()  # libcurl copies the URL, so the guard does too
        try:
            with pytest.raises(CurlError):
                dup.perform()
        finally:
            dup.close()
        # A loopback URL behind a public proxy: the proxy is the first hop.
        curl.reset()
        curl.setopt(CurlOpt.URL, b"http://127.0.0.1:9/")
        curl.setopt(CurlOpt.PROXY, b"http://203.0.113.7:3128")
        with pytest.raises(CurlError):
            curl.perform()
        # reset() clears libcurl's proxy, so the guard forgets it too: a stale
        # loopback proxy must not wave a public URL through.
        curl.reset()
        curl.setopt(CurlOpt.PROXY, b"http://127.0.0.1:3128")
        curl.reset()
        curl.setopt(CurlOpt.URL, b"https://d.example/vdp")
        with pytest.raises(CurlError):
            curl.perform()
    finally:
        curl.close()

    # Through a Session the proxy is left to the backstop, which maps to the
    # same ConnectionError callers catch (sync perform and async add_handle).
    with pytest.raises(cffi.exceptions.ConnectionError):
        cffi.get("http://127.0.0.1:9/", proxy="http://203.0.113.7:3128", timeout=5)

    async def fetch_via_proxy() -> None:
        async with cffi.AsyncSession(proxy="socks5h://203.0.113.7") as session:
            await session.get("http://127.0.0.1:9/", timeout=5)

    with pytest.raises(cffi.exceptions.ConnectionError):
        asyncio.run(fetch_via_proxy())

    assert reached == []
    assert guard.attempts == {
        FAKE_NODEID: [
            "d.example:443 (curl_cffi)",
            "d.example:443 (curl_cffi)",
            "203.0.113.7:3128 (curl_cffi proxy)",
            "d.example:443 (curl_cffi)",
            "203.0.113.7:3128 (curl_cffi proxy)",
            "203.0.113.7:1080 (curl_cffi proxy)",
        ]
    }


class _OkHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802 - http.server's name
        body = b"ok"
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args: object) -> None:
        pass


@pytest.fixture
def loopback_http() -> Iterator[int]:
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _OkHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server.server_address[1]
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_curl_cffi_loopback_requests_still_work(
    cffi: types.ModuleType, guard: NetworkGuard, loopback_http: int
) -> None:
    from curl_cffi import Curl, CurlOpt

    port = loopback_http
    assert guard.curl_cffi_guarded is True
    assert cffi.get(f"http://127.0.0.1:{port}/a", timeout=5).text == "ok"
    with cffi.Session(base_url=f"http://127.0.0.1:{port}/") as session:
        assert session.get("b", timeout=5).status_code == 200

    async def fetch() -> str:
        async with cffi.AsyncSession() as session:
            return (await session.get(f"http://127.0.0.1:{port}/c", timeout=5)).text

    assert asyncio.run(fetch()) == "ok"

    curl = Curl()
    body = io.BytesIO()
    try:
        curl.setopt(CurlOpt.URL, f"http://127.0.0.1:{port}/d".encode())
        curl.setopt(CurlOpt.WRITEDATA, body)
        curl.perform()
    finally:
        curl.close()
    assert body.getvalue() == b"ok"
    assert guard.attempts == {}
    assert guard.current.user_properties == []


@pytest.mark.parametrize(
    ("url", "destination"),
    [
        ("https://d.example/vdp", "d.example:443"),
        (b"http://192.0.2.1/", "192.0.2.1:80"),
        ("http://[2001:db8::1]:8080/x", "[2001:db8::1]:8080"),
        ("d.example/vdp", "d.example:80"),  # no scheme: libcurl guesses http
        ("socks5h://203.0.113.7", "203.0.113.7:1080"),
        ("http://192.168.1.89:15432/", "192.168.1.89:15432"),
        ("http://[::1", "http://[::1"),  # does not parse: cannot be proven local
        ("http://127.0.0.1:5000/", None),
        ("http://localhost/", None),
        ("HTTP://LOCALHOST.:8080/", None),
        ("http://[::1]:8080/", None),
        ("http://0.0.0.0:8000/", None),
        ("file:///etc/hosts", None),
        # urlsplit sees no host, but libcurl requests "no-host" (DNS and all):
        # a host the guard cannot read is refused, not waved through.
        ("http:///no-host", "http:///no-host"),
        ("http://:80/x", "http://:80/x"),
        ("", None),
        (None, None),
    ],
)
def test_url_destination_table(url: object, destination: str | None) -> None:
    assert url_destination(url) == destination


def test_undo_restores_curl_cffi(cffi: types.ModuleType) -> None:
    from curl_cffi.aio import AsyncCurl
    from curl_cffi.curl import Curl

    def snapshot() -> list[object]:
        return [
            vars(cffi.Session).get("request"),
            vars(cffi.AsyncSession).get("request"),
            *(vars(Curl).get(name) for name in ("setopt", "reset", "duphandle", "perform")),
            vars(AsyncCurl).get("add_handle"),
        ]

    before = snapshot()
    mp = pytest.MonkeyPatch()
    NetworkGuard().install(mp)
    during = snapshot()
    assert all(b is not d for b, d in zip(before, during))
    mp.undo()
    assert snapshot() == before


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


    def test_curl_public():
        cffi = pytest.importorskip("curl_cffi.requests")
        if os.environ.get("TESTS_BLOCK_NETWORK") == "1":
            with pytest.raises(cffi.exceptions.ConnectionError) as excinfo:
                cffi.get("https://d.example/vdp", timeout=2)
            assert type(excinfo.value.__cause__).__name__ == "NetworkBlocked"
        else:
            # Unset: curl_cffi is untouched, and this branch sends nothing.
            assert not hasattr(cffi.Session.request, "__wrapped__")
            assert not hasattr(cffi.AsyncSession.request, "__wrapped__")


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

_HAVE_CURL_CFFI = importlib.util.find_spec("curl_cffi") is not None


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
    expected = 2 if _HAVE_CURL_CFFI else 1
    assert f"NETWORK-BLOCKED CONNECTS: {expected} in {expected} test(s)" in output, output
    assert "test_inner.py::test_public_connect  [1x: 8.8.8.8:53]" in output, output
    assert "test_stays_local" not in output.split("NETWORK-BLOCKED CONNECTS", 1)[1], output
    props = _junit_properties(junit)
    assert props["test_public_connect"] == [(NETWORK_BLOCKED_PROPERTY, "8.8.8.8:53")]
    assert props["test_stays_local"] == []
    if _HAVE_CURL_CFFI:
        assert "test_inner.py::test_curl_public  [1x: d.example:443 (curl_cffi)]" in output, output
        assert props["test_curl_public"] == [(NETWORK_BLOCKED_PROPERTY, "d.example:443 (curl_cffi)")]


def test_autouse_fixture_changes_nothing_when_env_is_unset(tmp_path: Path) -> None:
    proc, junit = _run_inner_session(tmp_path, block=False)
    output = proc.stdout + proc.stderr
    assert proc.returncode == 0, output
    assert "NETWORK-BLOCKED" not in output and "NETWORK GUARD" not in output, output
    assert all(not p for p in _junit_properties(junit).values())
