"""Shared pytest hooks for DealershipScanner backend tests.

Two isolation failures happened here for real, and both are structural rather
than a mistake in any one test:

1. ``test_dictionary_catalog.test_find_epa_csv_jeep`` redirected
   ``dictionary_paths`` and then called ``rebuild_catalog()``. ``dictionary_catalog``
   does ``from .dictionary_paths import INDEX_DIR, MANIFEST_PATH, CATALOG_DB_PATH``
   at import time, so it holds its OWN references: the rebuild wrote through to
   the REAL ``backend/dictionary/index`` and rebuilt it from a one-row tmp
   fixture (manifest ``entry_count`` 14,598 -> 1), gutting the EPA catalog for
   every process until someone re-ran ``rebuild_catalog()``. Patching that one
   test does not close the hole: the same trap is set for the next test that
   calls any dictionary writer.

2. The autouse fixture below clears ``INVENTORY_DATABASE_URL``, so anything that
   reads the fleet (e.g. ``trim_ladder._inventory_rung_evidence``) sees no
   inventory, returns ``()``, and the test under it fails for a reason that has
   nothing to do with what it asserts.

So this file provides, in order:

* ``_vpic_offline`` (autouse) - NHTSA vPIC decodes fail the way an unreachable
  vPIC fails, so no test reaches vpic.nhtsa.dot.gov; ``@pytest.mark.real_vpic_client``
  opts out. ``fake_dns`` resolves public names from a table for the SSRF guards.
* ``_network_guard`` (session, autouse) - with ``TESTS_BLOCK_NETWORK=1`` (CI sets
  it) every connect to a non-loopback address is refused with ``NetworkBlocked``
  and counted per test; the terminal summary lists the tests that tried
  ("NETWORK-BLOCKED CONNECTS") and the junit XML carries a ``network_blocked``
  property per destination. Unset, nothing is patched.
* ``_forbid_real_dictionary_writes`` (session, autouse) - makes writing anywhere
  under the real dictionary tree raise ``RealDictionaryWriteBlocked``, no matter
  which module's namespace holds the path. The patches are in-process, so a test
  that shells out to a script can still write; that is why the same fixture
  hashes ``index/manifest.json`` and ``index/dictionary_catalog.db`` before and
  after the session and fails the run if the bytes moved.
* ``scratch_dictionary_root`` - an isolated dictionary tree with EVERY imported
  copy of every dictionary path constant redirected at it, so writers are safe
  to call.
* ``sqlite_inventory`` - an isolated SQLite ``cars`` table a test can seed, so
  fleet-backed code paths have a real database to read.
* ``app_factory`` - the one way a test gets a freshly reloaded ``backend.main``
  with every user database in its own tmp dir (21 hand-rolled ``_fresh_app``
  copies used to drift on which keys they set).
* asset-gated skip accounting - the brochure PDFs are gitignored and parts of
  ``backend/dictionary`` exist only on dev machines, so a class of tests skips
  silently on CI and a regression in what they cover has no failing test
  anywhere. Every such skip is now counted and printed in the terminal summary
  ("ASSET-GATED SKIPS"), and with ``REQUIRE_LOCAL_ASSETS=1`` (dev machines, the
  asset-bearing CI job) an asset-gated skip becomes a hard failure so the
  golden-document pins cannot quietly stop running where the documents exist.
"""

from __future__ import annotations

import builtins
import errno
import hashlib
import importlib
import io
import ipaddress
import os
import socket
import sqlite3
import sys
import types
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Callable, Iterator

import pytest


@pytest.fixture(autouse=True)
def _inventory_sqlite_tests_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    """Default tests to SQLite inventory unless they set INVENTORY_DATABASE_URL.

    setenv(""), never delenv: this fixture runs after the root one, and a
    deleted key used to be refilled from .env by the next importlib.reload of
    backend.main (load_project_dotenv), pointing the test at the real database.
    An empty value stays empty and is falsy everywhere the URL is read.
    """
    monkeypatch.setenv("INVENTORY_DATABASE_URL", "")
    monkeypatch.setenv("DATABASE_URL", "")
    monkeypatch.setenv("INVENTORY_SQLITE_TESTS", "1")


# ---------------------------------------------------------------------------
# NHTSA vPIC is offline by default
# ---------------------------------------------------------------------------
#
# ``upsert_vehicles`` -> ``post_write.sync_incomplete_and_backfill_specs`` ->
# ``spec_structured_backfill.apply_structured_spec_backfill_for_car`` ->
# ``nhtsa_vpic.fetch_decode_vin_values_extended`` decodes any well-formed VIN with
# fillable slots, and without a ``get_json`` its ``_default_get`` closure calls
# ``urllib.request.urlopen`` on vpic.nhtsa.dot.gov. ~20 upsert tests reached
# NHTSA that way (2026-10-05, measured with a socket-blocking plugin): their
# results depended on the network and on what vPIC returned that day.
#
# The stub swaps the ``urllib`` that ``nhtsa_vpic`` looks up at call time for a
# namespace whose ``request.urlopen`` raises ``URLError``. Every caller, however
# it imported ``fetch_decode_vin_values_extended``, then gets exactly what an
# unreachable vPIC gives it: ``(None, None, "url_error")``. A ``get_json`` passed
# by a test still wins (it never touches urllib). The batch decoder
# ``vpic_facts.fetch_vpic_batch`` (``requests.post``) raises ConnectionError the
# same way. A test that drives the real HTTP client with its own mock of
# ``urllib.request.urlopen`` / ``requests.post`` opts out with
# ``@pytest.mark.real_vpic_client``.

VPIC_OFFLINE_MARKER = "real_vpic_client"


class VpicOffline(urllib.error.URLError):
    """Raised in place of the vPIC HTTP call while tests run offline."""


def _vpic_offline_urlopen(*_args: Any, **_kwargs: Any) -> Any:
    raise VpicOffline("vPIC is stubbed offline in tests (mark real_vpic_client to opt out)")


def _vpic_offline_batch(vins: Any) -> Any:
    import requests

    raise requests.exceptions.ConnectionError(
        "vPIC batch is stubbed offline in tests (mark real_vpic_client to opt out)"
    )


_VPIC_OFFLINE_URLLIB = types.SimpleNamespace(
    request=types.SimpleNamespace(
        Request=urllib.request.Request,
        urlopen=_vpic_offline_urlopen,
    ),
    error=urllib.error,
)


@pytest.fixture(autouse=True)
def _vpic_offline(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> None:
    if request.node.get_closest_marker(VPIC_OFFLINE_MARKER):
        return
    from backend.enrichment import nhtsa_vpic, vpic_facts

    monkeypatch.setattr(nhtsa_vpic, "urllib", _VPIC_OFFLINE_URLLIB)
    monkeypatch.setattr(vpic_facts, "fetch_vpic_batch", _vpic_offline_batch)


# ---------------------------------------------------------------------------
# Fake DNS for the SSRF host guards
# ---------------------------------------------------------------------------
#
# ``outbound_url.destination_host_blocked_after_dns`` resolves the host with
# ``socket.getaddrinfo``; the web-research href filter and the dev-scanner URL
# validator go through it. Tests that feed it public names take this fixture so
# they resolve from a table instead of the network. Unknown names get a public
# address (example.com's), names in ``fake_dns.table`` get what the test put
# there, and loopback names still go to the real resolver.

_FAKE_DNS_PUBLIC_IP = "93.184.215.14"


class FakeDns:
    def __init__(self, real: Callable[..., Any]) -> None:
        self.table: dict[str, str | None] = {}
        self.lookups: list[str] = []
        self._real = real

    def getaddrinfo(self, host: Any, port: Any, *args: Any, **kwargs: Any) -> Any:
        name = host.decode() if isinstance(host, bytes) else str(host or "")
        name = name.strip().lower().rstrip(".")
        if name in ("", "localhost") or name.endswith(".localhost"):
            return self._real(host, port, *args, **kwargs)
        try:
            ipaddress.ip_address(name)
            return self._real(host, port, *args, **kwargs)
        except ValueError:
            pass
        self.lookups.append(name)
        ip = self.table.get(name, _FAKE_DNS_PUBLIC_IP)
        if ip is None:
            raise socket.gaierror(socket.EAI_NONAME, "fake_dns: unknown host")
        fam = socket.AF_INET6 if ":" in ip else socket.AF_INET
        sockaddr = (ip, port or 0, 0, 0) if fam == socket.AF_INET6 else (ip, port or 0)
        return [(fam, socket.SOCK_STREAM, 6, "", sockaddr)]


@pytest.fixture
def fake_dns(monkeypatch: pytest.MonkeyPatch) -> FakeDns:
    dns = FakeDns(socket.getaddrinfo)
    monkeypatch.setattr(socket, "getaddrinfo", dns.getaddrinfo)
    return dns


# ---------------------------------------------------------------------------
# Network guard: TESTS_BLOCK_NETWORK=1 refuses non-loopback connects
# ---------------------------------------------------------------------------
#
# The vPIC stub and ``fake_dns`` above close the two network paths we knew
# about. Anything else a test reaches (a scanner fetch nobody stubbed, a
# geocoder, an OEM endpoint) goes to the real internet, so a result can depend
# on what a remote host said that day, and on CI it can hang or flake. With
# ``TESTS_BLOCK_NETWORK=1`` (CI sets it) every socket connect to a non-loopback
# destination is refused with ``NetworkBlocked``: a ``ConnectionRefusedError``
# (ECONNREFUSED), so the code under test sees what a firewalled host gives it.
# ``connect_ex`` returns ECONNREFUSED instead of raising, as the real one does.
# Each refused attempt is counted against the running test: the terminal
# summary lists those tests ("NETWORK-BLOCKED CONNECTS"), and every distinct
# destination becomes a ``network_blocked`` property on the test's junit case.
# That list is class (f) of the CI failure ledger.
#
# Allowed: 127.0.0.0/8, ::1 (also as ::ffff:127.x), the unspecified address
# (0.0.0.0, ::, "" - a connect there lands on this host), the localhost names,
# and every non-IP family, i.e. unix sockets. A private LAN address is the
# network (the mini is 192.168.1.89), so it is refused. A hostname that is not a
# localhost name is refused WITHOUT being resolved: it cannot be proven local
# without DNS. Not covered: name resolution itself (``getaddrinfo`` is not a
# connect; ``fake_dns`` covers the tests that need it) and connects made while
# test modules are imported, before the session fixture is set up.
#
# Unset, or any value other than "1": nothing is patched at all.

TESTS_BLOCK_NETWORK_ENV = "TESTS_BLOCK_NETWORK"
NETWORK_BLOCKED_PROPERTY = "network_blocked"
_LOOPBACK_NAMES = frozenset({"", "localhost", "localhost.localdomain", "ip6-localhost", "ip6-loopback"})


class NetworkBlocked(ConnectionRefusedError):
    """A test connected to a non-loopback address while ``TESTS_BLOCK_NETWORK=1``."""


def network_guard_enabled() -> bool:
    return (os.environ.get(TESTS_BLOCK_NETWORK_ENV) or "").strip() == "1"


def host_is_loopback(host: Any) -> bool:
    """True when *host* (an IP literal or a name) is this machine without DNS."""
    if isinstance(host, (bytes, bytearray)):
        host = bytes(host).decode("ascii", "replace")
    name = str(host if host is not None else "").strip().lower().rstrip(".")
    if name.startswith("[") and name.endswith("]"):
        name = name[1:-1]
    if name in _LOOPBACK_NAMES or name.endswith(".localhost"):
        return True
    try:
        ip = ipaddress.ip_address(name)
    except ValueError:
        return False  # a name we cannot prove local without resolving it
    mapped = getattr(ip, "ipv4_mapped", None)
    if mapped is not None:
        ip = mapped
    return ip.is_loopback or ip.is_unspecified


def destination_allowed(family: Any, address: Any) -> bool:
    """Whether a ``connect(address)`` on a socket of *family* stays on this host."""
    if family not in (socket.AF_INET, socket.AF_INET6):
        return True  # AF_UNIX and the other local families
    if not isinstance(address, tuple) or not address:
        return True  # malformed: let the real connect raise its own error
    return host_is_loopback(address[0])


def describe_destination(address: Any) -> str:
    if isinstance(address, tuple) and len(address) >= 2:
        host = address[0]
        if isinstance(host, (bytes, bytearray)):
            host = bytes(host).decode("ascii", "replace")
        host = str(host)
        return f"[{host}]:{address[1]}" if ":" in host else f"{host}:{address[1]}"
    return repr(address)


class NetworkGuard:
    """Refuses non-loopback connects and remembers which test tried them.

    ``current`` is the pytest item running now (set by the
    ``pytest_runtest_protocol`` wrapper below); ``attempts`` maps its nodeid to
    every destination it was refused, repeats included.
    """

    def __init__(self) -> None:
        self.attempts: dict[str, list[str]] = {}
        self.current: Any = None
        self.installed = False

    def _refused(self, destination: str) -> NetworkBlocked:
        item = self.current
        nodeid = getattr(item, "nodeid", None) or "<outside a test>"
        seen = self.attempts.setdefault(nodeid, [])
        if destination not in seen and item is not None:
            item.user_properties.append((NETWORK_BLOCKED_PROPERTY, destination))
        seen.append(destination)
        return NetworkBlocked(
            errno.ECONNREFUSED,
            f"{TESTS_BLOCK_NETWORK_ENV}=1 refused a connect to {destination}: tests "
            "must not reach the network. Stub the call (the vPIC stub and "
            "fake_dns in backend/tests/conftest.py show how) or connect to loopback.",
        )

    def install(self, mp: pytest.MonkeyPatch) -> None:
        """Patch ``socket.socket.connect``/``connect_ex`` and ``socket.create_connection``.

        The class attributes are patched, so every socket - including
        ``ssl.SSLSocket`` and the ones urllib3/asyncio create - goes through
        them. What is wrapped is whatever is installed now, so a second guard
        stacks on the first and ``mp.undo()`` restores it.
        """
        real_connect = socket.socket.connect
        real_connect_ex = socket.socket.connect_ex
        real_create_connection = socket.create_connection
        guard = self

        def connect(sock: socket.socket, address: Any) -> None:
            if not destination_allowed(sock.family, address):
                raise guard._refused(describe_destination(address))
            return real_connect(sock, address)

        def connect_ex(sock: socket.socket, address: Any) -> int:
            if not destination_allowed(sock.family, address):
                guard._refused(describe_destination(address))
                return errno.ECONNREFUSED
            return real_connect_ex(sock, address)

        def create_connection(address: Any, *args: Any, **kwargs: Any) -> socket.socket:
            # Checked before the real one resolves the name, so a refused host
            # costs no DNS lookup; an allowed one is re-checked per resolved
            # address by ``connect`` above.
            if isinstance(address, tuple) and address and not host_is_loopback(address[0]):
                raise guard._refused(describe_destination(address))
            return real_create_connection(address, *args, **kwargs)

        mp.setattr(socket.socket, "connect", connect)
        mp.setattr(socket.socket, "connect_ex", connect_ex)
        mp.setattr(socket, "create_connection", create_connection)
        self.installed = True


_NETWORK_GUARD = NetworkGuard()


@pytest.fixture(scope="session", autouse=True)
def _network_guard() -> Iterator[NetworkGuard | None]:
    """Session-wide, so module- and class-scoped fixtures are covered too."""
    if not network_guard_enabled():
        yield None
        return
    mp = pytest.MonkeyPatch()
    _NETWORK_GUARD.install(mp)
    try:
        yield _NETWORK_GUARD
    finally:
        mp.undo()


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_protocol(item: Any, nextitem: Any):  # noqa: ANN201 - pytest hook
    """Attribute refused connects to the test whose setup/call/teardown made them."""
    _NETWORK_GUARD.current = item
    yield
    _NETWORK_GUARD.current = None


def _network_guard_summary(terminalreporter: Any) -> None:
    if not _NETWORK_GUARD.installed:
        return
    attempts = _NETWORK_GUARD.attempts
    total = sum(len(v) for v in attempts.values())
    if not attempts:
        terminalreporter.section(
            f"NETWORK GUARD ({TESTS_BLOCK_NETWORK_ENV}=1): no test tried to reach the network",
            sep="=",
        )
        return
    terminalreporter.section(
        f"NETWORK-BLOCKED CONNECTS: {total} in {len(attempts)} test(s) "
        f"({TESTS_BLOCK_NETWORK_ENV}=1)",
        sep="=",
    )
    for nodeid, dests in attempts.items():
        unique = list(dict.fromkeys(dests))
        shown = ", ".join(unique[:3]) + (f", +{len(unique) - 3} more" if len(unique) > 3 else "")
        terminalreporter.line(f"  {nodeid}  [{len(dests)}x: {shown}]")
    terminalreporter.line(
        "  (each was refused with NetworkBlocked; stub these calls - they are "
        "class (f) of the CI failure ledger)"
    )


# ---------------------------------------------------------------------------
# 0. Asset-gated skips are counted, and on asset-bearing machines they fail
# ---------------------------------------------------------------------------
#
# The controlled vocabulary of skip reasons that mean "a local corpus asset is
# missing" (gitignored brochure PDFs, the backend/dictionary tree, brochure_text
# corpus files, dev-only epa_master rows). Matched as substrings against the
# skip reason. Reasons here are ON-DISK asset gates; reasons in the ENV list
# below are environment gates (a database the machine does not run) — both are
# counted, only the asset gates are escalated by REQUIRE_LOCAL_ASSETS.

_ASSET_GATE_SKIP_REASONS: tuple[str, ...] = (
    "not present",              # "brochure PDF not present", "dictionary not present", …
    "not on disk",              # "2026_Toyota_RAV4_Brochure.pdf not on disk", …
    "not built",                # "overlay not built", "catalog db not built"
    "run process_brochure_queue first",
    "epa_master has no",        # data-content gates in test_knowledge_engine_specs
)

_ENV_GATE_SKIP_REASONS: tuple[str, ...] = (
    "DQ_INVARIANTS_LIVE_DB",    # needs a real Postgres, not a file on disk
)

REQUIRE_LOCAL_ASSETS_ENV = "REQUIRE_LOCAL_ASSETS"


def _skip_reason_of(report: Any) -> str:
    longrepr = getattr(report, "longrepr", None)
    if isinstance(longrepr, tuple) and len(longrepr) == 3:
        message = str(longrepr[2])
    else:
        message = str(longrepr or "")
    return message.split("Skipped: ", 1)[-1].strip()


def _matches(reason: str, patterns: tuple[str, ...]) -> bool:
    return any(p in reason for p in patterns)


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: Any, call: Any):  # noqa: ANN201 - pytest hook
    outcome = yield
    report = outcome.get_result()
    if report.when not in ("setup", "call") or not report.skipped:
        return
    reason = _skip_reason_of(report)
    if not _matches(reason, _ASSET_GATE_SKIP_REASONS):
        return
    if (os.environ.get(REQUIRE_LOCAL_ASSETS_ENV) or "").strip() == "1":
        report.outcome = "failed"
        report.longrepr = (
            f"{item.nodeid}: asset-gated skip ({reason!r}) with "
            f"{REQUIRE_LOCAL_ASSETS_ENV}=1.\nThis machine claims to hold the "
            "local corpus, so the golden-document test refusing to run IS the "
            "failure — restore the asset or unset the variable."
        )


def pytest_terminal_summary(terminalreporter: Any, exitstatus: int, config: Any) -> None:
    _network_guard_summary(terminalreporter)
    _asset_gate_summary(terminalreporter)


def _asset_gate_summary(terminalreporter: Any) -> None:
    gated: list[tuple[str, str]] = []
    for report in terminalreporter.stats.get("skipped", ()):
        reason = _skip_reason_of(report)
        if _matches(reason, _ASSET_GATE_SKIP_REASONS + _ENV_GATE_SKIP_REASONS):
            gated.append((report.nodeid, reason))
    if not gated:
        return
    terminalreporter.section(f"ASSET-GATED SKIPS: {len(gated)}", sep="=")
    for nodeid, reason in gated:
        terminalreporter.line(f"  {nodeid}  [{reason}]")
    terminalreporter.line(
        f"  (these run nowhere without the local corpus; set "
        f"{REQUIRE_LOCAL_ASSETS_ENV}=1 on asset-bearing machines to make an "
        "asset-gated skip fail instead)"
    )


# ---------------------------------------------------------------------------
# 1. The real dictionary tree is read-only for the whole test session
# ---------------------------------------------------------------------------


class RealDictionaryWriteBlocked(RuntimeError):
    """A test tried to write inside the real ``backend/dictionary`` tree.

    Take the ``scratch_dictionary_root`` fixture and call the writer against
    that instead. Redirecting ``dictionary_paths`` by hand is not enough -- see
    the module docstring.
    """


def _real_dictionary_root() -> Path | None:
    """The dictionary root this process would really use, or None if absent.

    Read off ``dictionary_paths`` itself rather than recomputed here, so an env
    override (``DICTIONARY_ROOT``) or a layout change cannot leave the guard
    protecting a directory nothing writes to.
    """
    try:
        from backend.enrichment import dictionary_paths
    except Exception:  # pragma: no cover - dictionary package not importable
        return None
    root = Path(dictionary_paths.DICTIONARY_ROOT).resolve()
    return root if root.is_dir() else None


def _guarded_prefixes(root: Path) -> tuple[str, ...]:
    """Absolute paths whose subtree is write-protected during the session."""
    out = [str(root)]
    try:
        from backend.enrichment import dictionary_paths

        # CATALOG_DB_PATH honours DICTIONARY_CATALOG_DB_PATH and can sit outside
        # the root; the catalog DB is half of what got destroyed, so cover it.
        db = Path(dictionary_paths.CATALOG_DB_PATH).resolve()
        if not str(db).startswith(str(root) + os.sep):
            out.append(str(db))
    except Exception:  # pragma: no cover
        pass
    return tuple(out)


def _is_write_mode(mode: Any) -> bool:
    m = mode if isinstance(mode, str) else "r"
    return any(c in m for c in "wxa+")


def _install_dictionary_write_guard(mp: pytest.MonkeyPatch, prefixes: tuple[str, ...]) -> None:
    """Deny every write under *prefixes*, whichever module holds the path.

    Patched at the syscall-adjacent layer (``open``/``os``/``sqlite3``) instead
    of at ``dictionary_catalog``'s writers, because the hazard is that a path
    constant has been copied into some other module's namespace -- the guard has
    to be blind to who is calling.
    """

    def _blocked(path: Any) -> str | None:
        try:
            p = os.path.abspath(os.fspath(path))
        except TypeError:  # fd, or something that is not a path
            return None
        for prefix in prefixes:
            if p == prefix or p.startswith(prefix + os.sep):
                return p
        return None

    def _deny(path: str, op: str) -> None:
        raise RealDictionaryWriteBlocked(
            f"{op} would write the real dictionary tree at {path}.\n"
            "Tests must not mutate backend/dictionary -- rebuilding it from a "
            "fixture once cost the live EPA catalog 14,598 entries. Use the "
            "`scratch_dictionary_root` fixture (it redirects every imported copy "
            "of every dictionary path constant), or read-only APIs."
        )

    real_io_open = io.open
    real_builtins_open = builtins.open
    real_os_open = os.open
    real_mkdir = os.mkdir
    real_sqlite_connect = sqlite3.connect

    def guarded_io_open(file: Any, mode: str = "r", *a: Any, **kw: Any):
        if _is_write_mode(mode):
            hit = _blocked(file)
            if hit:
                _deny(hit, f"open(mode={mode!r})")
        return real_io_open(file, mode, *a, **kw)

    def guarded_builtins_open(file: Any, mode: str = "r", *a: Any, **kw: Any):
        if _is_write_mode(mode):
            hit = _blocked(file)
            if hit:
                _deny(hit, f"open(mode={mode!r})")
        return real_builtins_open(file, mode, *a, **kw)

    def guarded_os_open(path: Any, flags: int, *a: Any, **kw: Any):
        writeish = flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND)
        if writeish:
            hit = _blocked(path)
            if hit:
                _deny(hit, "os.open")
        return real_os_open(path, flags, *a, **kw)

    def guarded_mkdir(path: Any, *a: Any, **kw: Any):
        # ``INDEX_DIR.mkdir(parents=True, exist_ok=True)`` on the existing real
        # index is a no-op and stays legal; creating a NEW directory in the tree
        # is a write.
        hit = _blocked(path)
        if hit and not os.path.isdir(hit):
            _deny(hit, "mkdir")
        return real_mkdir(path, *a, **kw)

    def _guard_unary(real: Callable[..., Any], op: str) -> Callable[..., Any]:
        def wrapper(path: Any, *a: Any, **kw: Any):
            hit = _blocked(path)
            if hit:
                _deny(hit, op)
            return real(path, *a, **kw)

        return wrapper

    def _guard_binary(real: Callable[..., Any], op: str) -> Callable[..., Any]:
        def wrapper(src: Any, dst: Any, *a: Any, **kw: Any):
            hit = _blocked(dst) or _blocked(src)
            if hit:
                _deny(hit, op)
            return real(src, dst, *a, **kw)

        return wrapper

    def guarded_sqlite_connect(database: Any = ":memory:", *a: Any, **kw: Any):
        """Reads of the real catalog DB keep working; writes cannot.

        Downgraded to a ``mode=ro`` URI rather than refused outright because
        ``find_epa_csv`` reads this database on ordinary lookups -- refusing the
        connection would break every EPA test instead of protecting it. A write
        on the returned connection raises sqlite3's "attempt to write a readonly
        database".
        """
        target = database
        if isinstance(database, str) and database.startswith("file:"):
            # Already a URI: strip it back to a path so a caller-supplied
            # ``mode=rw`` cannot walk past the guard.
            target = database[5:].split("?", 1)[0]
        hit = _blocked(target) if not isinstance(database, int) else None
        if hit:
            kw = dict(kw)
            kw["uri"] = True
            return real_sqlite_connect(f"file:{hit}?mode=ro", *a, **kw)
        return real_sqlite_connect(database, *a, **kw)

    mp.setattr(io, "open", guarded_io_open)
    mp.setattr(builtins, "open", guarded_builtins_open)
    mp.setattr(os, "open", guarded_os_open)
    mp.setattr(os, "mkdir", guarded_mkdir)
    mp.setattr(os, "remove", _guard_unary(os.remove, "os.remove"))
    mp.setattr(os, "unlink", _guard_unary(os.unlink, "os.unlink"))
    mp.setattr(os, "rmdir", _guard_unary(os.rmdir, "os.rmdir"))
    mp.setattr(os, "truncate", _guard_unary(os.truncate, "os.truncate"))
    mp.setattr(os, "rename", _guard_binary(os.rename, "os.rename"))
    mp.setattr(os, "replace", _guard_binary(os.replace, "os.replace"))
    mp.setattr(sqlite3, "connect", guarded_sqlite_connect)


def _index_fingerprint(root: Path) -> dict[str, str]:
    """Content hash of the two files the 2026-07-31 incident destroyed.

    Hashing bytes on disk on purpose: asking the catalog how many entries it
    thinks it has would be checking the system against its own bookkeeping.
    """
    out: dict[str, str] = {}
    for rel in ("index/manifest.json", "index/dictionary_catalog.db"):
        p = root / rel
        if not p.is_file():
            out[rel] = "absent"
            continue
        h = hashlib.sha256()
        with io.open(p, "rb") as fh:  # bound before the guard patches io.open
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                h.update(chunk)
        out[rel] = f"{p.stat().st_size}:{h.hexdigest()}"
    return out


@pytest.fixture(scope="session", autouse=True)
def _forbid_real_dictionary_writes() -> Iterator[None]:
    """Session-wide: the real dictionary tree is readable and not writable."""
    root = _real_dictionary_root()
    if root is None:
        yield
        return
    before = _index_fingerprint(root)
    mp = pytest.MonkeyPatch()
    _install_dictionary_write_guard(mp, _guarded_prefixes(root))
    try:
        yield
    finally:
        mp.undo()
    after = _index_fingerprint(root)
    if after != before:
        changed = [k for k in before if before[k] != after.get(k)]
        pytest.fail(
            "the real dictionary index changed during this test session: "
            f"{changed}\nbefore={before}\nafter={after}\n"
            "Something wrote it from outside this process, or through an API the "
            "guard does not cover. Rebuild with "
            "`python -c 'from backend.enrichment.dictionary_catalog import rebuild_catalog; rebuild_catalog()'`.",
            pytrace=False,
        )


# ---------------------------------------------------------------------------
# 2. An isolated dictionary tree that writers may actually be pointed at
# ---------------------------------------------------------------------------

def _dictionary_path_constants() -> tuple[str, ...]:
    """Every ``Path`` constant ``dictionary_paths`` publishes, read at runtime.

    Discovered rather than listed: a hand-maintained list going stale when a new
    constant is added is the same drift that let ``dictionary_catalog``'s private
    copies escape the redirect in the first place.
    """
    from backend.enrichment import dictionary_paths

    return tuple(
        name
        for name in dir(dictionary_paths)
        if name.isupper() and isinstance(getattr(dictionary_paths, name, None), Path)
    )


def _dictionary_path_map(root: Path) -> dict[str, Path]:
    """Old absolute path (str) -> its equivalent under *root*."""
    from backend.enrichment import dictionary_paths

    real_root = Path(dictionary_paths.DICTIONARY_ROOT).resolve()
    mapping: dict[str, Path] = {}
    for name in _dictionary_path_constants():
        old_abs = Path(getattr(dictionary_paths, name)).resolve()
        try:
            rel = old_abs.relative_to(real_root)
        except ValueError:
            # e.g. CATALOG_DB_PATH pointed outside the tree by env.
            mapping[str(old_abs)] = root / "index" / old_abs.name
            continue
        mapping[str(old_abs)] = root / rel
    mapping[str(real_root)] = root
    return mapping


def redirect_dictionary_paths(mp: pytest.MonkeyPatch, root: Path) -> list[str]:
    """Point every *imported copy* of every dictionary path constant at *root*.

    Walks ``sys.modules`` because the copies are what matter: a module that did
    ``from ...dictionary_paths import INDEX_DIR`` keeps its own binding, and
    patching ``dictionary_paths`` alone leaves that module writing production.

    Returns ``"module.ATTR"`` for each redirection, so a caller can assert the
    module it is about to exercise was actually covered.
    """
    mapping = _dictionary_path_map(root)
    names = _dictionary_path_constants()
    redirected: list[str] = []
    for mod_name, module in list(sys.modules.items()):
        if module is None:
            continue
        if not (mod_name.startswith("backend.") or mod_name == "backend"):
            continue
        for attr in names:
            value = getattr(module, attr, None)
            if not isinstance(value, Path):
                continue
            new = mapping.get(str(value.resolve()))
            if new is None:
                continue
            mp.setattr(module, attr, new, raising=False)
            redirected.append(f"{mod_name}.{attr}")
    return redirected


def _invalidate_dictionary_caches() -> None:
    """Drop every memo that could carry a path across the redirect boundary."""
    dc = sys.modules.get("backend.enrichment.dictionary_catalog")
    if dc is not None:
        dc.invalidate_catalog_cache()


@pytest.fixture
def scratch_dictionary_root(tmp_path: Path) -> Iterator[Path]:
    """An empty, isolated dictionary tree that dictionary writers may write to.

    Redirects the env vars AND every already-imported copy of every dictionary
    path constant, then invalidates the catalog caches on the way in and on the
    way out (after the redirect is undone, so no scratch path survives the test).
    """
    root = tmp_path / "dictionary"
    for sub in ("index", "epa", "options/raw", "options/stubs", "curated", "derived"):
        (root / sub).mkdir(parents=True, exist_ok=True)

    # Own MonkeyPatch, not the fixture: teardown ordering has to be
    # undo-then-invalidate, and the shared monkeypatch fixture undoes after us.
    mp = pytest.MonkeyPatch()
    mp.setenv("DICTIONARY_ROOT", str(root))
    mp.setenv("DICTIONARY_CATALOG_DB_PATH", str(root / "index" / "dictionary_catalog.db"))
    redirect_dictionary_paths(mp, root)
    _invalidate_dictionary_caches()
    try:
        yield root
    finally:
        mp.undo()
        _invalidate_dictionary_caches()


# ---------------------------------------------------------------------------
# 3. A usable inventory database
# ---------------------------------------------------------------------------


class SqliteInventory:
    """Isolated ``cars`` table for tests that need the fleet to be non-empty."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def get_conn(self):
        """A connection to this inventory, through ``inventory_db.get_conn``."""
        from backend.db import inventory_db as inv_db

        return inv_db.get_conn()

    def add_cars(self, rows: list[dict[str, Any]]) -> int:
        """Insert *rows* (any subset of ``cars`` columns; ``vin`` auto-filled)."""
        inserted = 0
        conn = sqlite3.connect(str(self.path))
        try:
            existing = {r[1] for r in conn.execute("PRAGMA table_info(cars)")}
            for i, row in enumerate(rows):
                data = dict(row)
                data.setdefault("vin", f"TESTVIN{os.getpid():06d}{id(self) % 10000:04d}{i:05d}")
                data.setdefault("listing_active", 1)
                unknown = set(data) - existing
                if unknown:
                    raise KeyError(f"cars has no column(s): {sorted(unknown)}")
                cols = ",".join(data)
                marks = ",".join("?" for _ in data)
                conn.execute(f"INSERT INTO cars ({cols}) VALUES ({marks})", tuple(data.values()))
                inserted += 1
            conn.commit()
        finally:
            conn.close()
        # Re-checked here, not only at fixture setup: the module under test is
        # usually imported by the test body, i.e. after the fixture ran.
        _assert_inventory_cache_list_is_complete()
        _clear_inventory_derived_caches()
        return inserted


# Cached readers of the inventory DB. A memo taken before the fixture seeds rows
# says "the fleet is empty" for the rest of the session, which is how a seeded
# test still reads zero rows. Enumerated rather than swept (a blanket clear would
# also drop caches whose staleness a test may be asserting on) and checked for
# completeness below, so a new cached reader cannot go unnoticed.
_INVENTORY_CACHE_READERS: dict[str, tuple[str, ...]] = {
    # The cached readers live in the ``evidence`` submodule of the
    # ``trim_ladder`` package, not in its facade ``__init__``. Key on the module
    # that defines them: the AST check below reads ``module.__file__``, and the
    # facade's file contains no function bodies at all.
    "backend.enrichment.trim_ladder.evidence": (
        "_active_make_spellings",
        "_active_trims_by_make_year",
    ),
}


def _assert_inventory_cache_list_is_complete() -> None:
    """Fail loudly if a listed module grew a cached inventory reader we do not clear.

    Reads the source, not the module's own bookkeeping: any ``lru_cache``d
    function whose body mentions ``db_conn``/``inventory_db`` must be listed.

    Scope, stated plainly: this only checks the modules that are keys of
    ``_INVENTORY_CACHE_READERS``. A cached inventory reader added to some other
    module is NOT detected here -- add that module as a key when a test starts
    depending on it.
    """
    import ast

    for mod_name, listed in _INVENTORY_CACHE_READERS.items():
        module = sys.modules.get(mod_name)
        if module is None or not getattr(module, "__file__", None):
            continue
        tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"))
        found = {
            node.name
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef)
            and any("lru_cache" in ast.unparse(d) for d in node.decorator_list)
            and ("db_conn" in ast.dump(node) or "inventory_db" in ast.dump(node))
        }
        missing = found - set(listed)
        if missing:
            raise AssertionError(
                f"{mod_name} has cached inventory readers the `sqlite_inventory` "
                f"fixture does not clear: {sorted(missing)}. Add them to "
                "_INVENTORY_CACHE_READERS in backend/tests/conftest.py, or seeded "
                "rows will be invisible to whatever memoised first."
            )


def _clear_inventory_derived_caches() -> None:
    for mod_name, attrs in _INVENTORY_CACHE_READERS.items():
        module = sys.modules.get(mod_name)
        if module is None:
            continue
        for attr in attrs:
            clear = getattr(getattr(module, attr, None), "cache_clear", None)
            if clear is not None:
                clear()


@pytest.fixture
def sqlite_inventory(tmp_path: Path) -> Iterator[SqliteInventory]:
    """A real, empty, isolated inventory DB wired into ``backend.db.inventory_db``.

    Without this, ``INVENTORY_DATABASE_URL`` is blank under pytest and the root
    conftest points ``DB_PATH`` at the shared dev ``inventory.db``, so anything
    fleet-backed (``_inventory_rung_evidence`` -> ``resolve_trim_ladder``) reads
    zero rows and the test fails for a reason it never meant to test.
    """
    from backend.db import inventory_db as inv_db

    path = tmp_path / "inventory.db"
    mp = pytest.MonkeyPatch()
    mp.setenv("INVENTORY_SQLITE_TESTS", "1")
    mp.setenv("INVENTORY_DATABASE_URL", "")
    mp.setenv("INVENTORY_DB_PATH", str(path))
    mp.setattr(inv_db, "DB_PATH", str(path), raising=False)
    inv_db.init_inventory_db()
    _assert_inventory_cache_list_is_complete()
    _clear_inventory_derived_caches()
    try:
        yield SqliteInventory(path)
    finally:
        mp.undo()
        _clear_inventory_derived_caches()


# ---------------------------------------------------------------------------
# 4. One app factory: a freshly reloaded backend.main on tmp databases
# ---------------------------------------------------------------------------

PRODUCTION_TEST_SECRET_KEY = "pytest-secret-key-do-not-use-in-deployment"
PRODUCTION_TEST_ADMIN_PASSWORD = "pytest-admin-bootstrap-do-not-use-in-deployment"


def app_env_defaults(tmp_path: Path) -> dict[str, str]:
    """The complete env every reloaded ``backend.main`` starts from.

    Every user-facing database lives under *tmp_path*; encryption keys and proxy
    trust are blanked with ``""`` (not deleted), so nothing a shell exported can
    leak into the app under test.
    """
    return {
        "FLASK_ENV": "development",
        "MFA_DELIVERY_MODE": "log",
        "USERS_DB_PATH": str(tmp_path / "users_test.db"),
        "DEV_USERS_DB_PATH": str(tmp_path / "dev_users_test.db"),
        "DEALER_PORTAL_DB_PATH": str(tmp_path / "dealer_portal_test.db"),
        "USERS_DB_ENCRYPTION_KEY": "",
        "DEV_USERS_DB_ENCRYPTION_KEY": "",
        "TRUST_PROXY_HEADERS": "",
    }


@pytest.fixture
def app_factory(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Callable[..., Any]:
    """``app_factory(**env) -> backend.main``, reloaded under a complete test env.

    Starts from :func:`app_env_defaults`, then applies the caller's overrides: a
    string value is set, ``None`` deletes the key. ``production=True`` switches to
    ``FLASK_ENV=production`` with the throwaway secret/admin password and the
    SQLCipher test keys (``apply_production_credential_encryption_env``) applied
    before the overrides. Returns the module; ``.app`` is the Flask app.
    """

    def _make(*, production: bool = False, **env: Any):
        values: dict[str, Any] = app_env_defaults(tmp_path)
        if production:
            values["FLASK_ENV"] = "production"
            values["SECRET_KEY"] = PRODUCTION_TEST_SECRET_KEY
            values["ADMIN_PASSWORD"] = PRODUCTION_TEST_ADMIN_PASSWORD
        values.update(env)
        for key, value in values.items():
            if value is None:
                monkeypatch.delenv(key, raising=False)
            else:
                monkeypatch.setenv(key, str(value))
        if production and "USERS_DB_ENCRYPTION_KEY" not in env:
            from conftest import apply_production_credential_encryption_env

            apply_production_credential_encryption_env(monkeypatch)
        import backend.main as main

        importlib.reload(main)
        # Process caches that used to live on backend.main (and so were reset by
        # the reload) now live in their owning route modules; reset them too.
        from backend.routes import fuel_api, home_dashboard

        home_dashboard._reco_cache.clear()
        fuel_api._live_gas_prices_cache = None
        fuel_api._live_gas_prices_cache_mtime = None
        return main

    return _make
