"""backend.db.connect: the one inventory DSN / connection helper (audit 2026-10-01, datascripts F8).

Golden-first: the legacy copies are frozen below VERBATIM (modulo taking the repo root
as an argument) as they stood at b1578241d, and every scenario of the env x .env matrix
is run through both. ``new == legacy`` everywhere except the documented change classes,
and the change counts are pinned so a later edit to the precedence cannot slip through.
"""

from __future__ import annotations

import itertools
import io
import os
import re
import sys
import tokenize
from pathlib import Path

import pytest

from backend.db import connect as dbc

REPO_ROOT = Path(__file__).resolve().parents[2]
_KEYS = ("INVENTORY_DATABASE_URL", "DATABASE_URL")


# ---------------------------------------------------------------------------
# Frozen legacy copies (b1578241d)
# ---------------------------------------------------------------------------


def legacy_a(root: Path) -> str:
    """The 21 identical ``_dsn()`` copies (add_dealership, image_batch, recheck_msrp, ...)."""
    dsn = (os.environ.get("INVENTORY_DATABASE_URL") or os.environ.get("DATABASE_URL") or "").strip()
    if dsn:
        return dsn
    env = root / ".env"
    if env.exists():
        m = re.search(r"^INVENTORY_DATABASE_URL=(.+)$", env.read_text(), re.M)
        if m:
            return m.group(1).strip().strip("'\"")
    raise SystemExit("INVENTORY_DATABASE_URL is not set")


def legacy_b_fetch_oem(root: Path) -> str:
    """fetch_oem_brochures._inventory_dsn: ignored DATABASE_URL."""
    dsn = os.environ.get("INVENTORY_DATABASE_URL")
    if dsn:
        return dsn.strip().strip("'\"")
    env_path = root / ".env"
    if env_path.is_file():
        match = re.search(
            r"^INVENTORY_DATABASE_URL=(.+)$", env_path.read_text(encoding="utf-8"), re.M
        )
        if match:
            return match.group(1).strip().strip("'\"")
    raise SystemExit("INVENTORY_DATABASE_URL not set and not found in .env")


def legacy_c_trim_coverage(root: Path) -> str:
    """trim_coverage_report._inventory_dsn: ignored DATABASE_URL, no strip, exported .env value."""
    dsn = os.environ.get("INVENTORY_DATABASE_URL")
    if dsn:
        return dsn
    env_path = root / ".env"
    if env_path.exists():
        match = re.search(
            r"^INVENTORY_DATABASE_URL=(.+)$", env_path.read_text(encoding="utf-8"), re.M
        )
        if match:
            dsn = match.group(1).strip().strip("'\"")
            os.environ["INVENTORY_DATABASE_URL"] = dsn
            return dsn
    raise SystemExit("INVENTORY_DATABASE_URL not set and not found in .env")


def legacy_d_dq(root: Path) -> str:
    """data_quality_invariants._inventory_url: ignored DATABASE_URL, RuntimeError."""
    url = os.environ.get("INVENTORY_DATABASE_URL")
    if url:
        return url.strip().strip("'\"")
    env_path = root / ".env"
    if env_path.exists():
        m = re.search(r"^INVENTORY_DATABASE_URL=(.+)$", env_path.read_text(), re.M)
        if m:
            return m.group(1).strip().strip("'\"")
    raise RuntimeError("INVENTORY_DATABASE_URL is not set and is not in .env")


LEGACY = {
    "A": legacy_a,
    "B": legacy_b_fetch_oem,
    "C": legacy_c_trim_coverage,
    "D": legacy_d_dq,
}

SHELL_INV = [None, "", "postgresql://shell-inv/db", "  'postgresql://shell-q/db'  "]
SHELL_DB = [None, "postgresql://shell-db/db"]
DOT_INV = [None, "postgresql://dotenv-inv/db", '"postgresql://dotenv-q/db"']
DOT_DB = [None, "postgresql://dotenv-db/db"]
SCENARIOS = list(itertools.product(SHELL_INV, SHELL_DB, DOT_INV, DOT_DB))

# Changed answers, per legacy variant, out of 48 scenarios (all are bug fixes):
#  * dotenv_db_only   -- .env has only DATABASE_URL: legacy raised, now resolves it
#                        (same INV-then-DB precedence after .env is loaded).
#  * quoted_shell     -- a quoted/padded shell value was passed to psycopg verbatim
#                        (A kept the quotes, C kept quotes and spaces); now stripped.
#  * shell_db_ignored -- B/C/D ignored an exported DATABASE_URL and fell to .env or
#                        raised; now DATABASE_URL is honoured, as in A and inventory_pg.
EXPECTED_CHANGE_COUNTS = {"A": 14, "B": 14, "C": 26, "D": 14}


def _blank(v: str | None) -> bool:
    return v is None or not v.strip()


def _change_class(variant: str, si, sd, di, dd) -> str | None:
    if _blank(si) and _blank(sd) and di is None and dd is not None:
        return "dotenv_db_only"
    if si is not None and si.strip() != si.strip().strip("'\"") and variant in ("A", "C"):
        return "quoted_shell"
    if si is not None and si != si.strip() and variant == "C":
        return "quoted_shell"
    if variant in ("B", "C", "D") and _blank(si) and not _blank(sd):
        return "shell_db_ignored"
    return None


def _expected_new(si, sd, di, dd) -> str | None:
    """The documented precedence, computed independently of the module."""
    for v in (si, sd):
        if not _blank(v):
            return v.strip().strip("'\"")
    for v in (di, dd):
        if v is not None:
            return v.strip().strip("'\"")
    return None


def _set_scenario(tmp_path: Path, si, sd, di, dd) -> Path:
    for k in _KEYS:
        os.environ.pop(k, None)
    if si is not None:
        os.environ["INVENTORY_DATABASE_URL"] = si
    if sd is not None:
        os.environ["DATABASE_URL"] = sd
    root = tmp_path / f"s{abs(hash((si, sd, di, dd)))}"
    root.mkdir(exist_ok=True)
    lines = []
    if di is not None:
        lines.append(f"INVENTORY_DATABASE_URL={di}")
    if dd is not None:
        lines.append(f"DATABASE_URL={dd}")
    env = root / ".env"
    if lines:
        env.write_text("\n".join(lines) + "\n")
    elif env.exists():
        env.unlink()
    return root


def _outcome(fn):
    try:
        return ("dsn", fn())
    except BaseException as exc:  # noqa: BLE001 -- SystemExit is the legacy contract
        kind = "RuntimeError" if isinstance(exc, RuntimeError) else (
            "SystemExit" if isinstance(exc, SystemExit) else type(exc).__name__
        )
        return ("error", kind)


@pytest.fixture
def isolated_env(monkeypatch: pytest.MonkeyPatch):
    for k in _KEYS:
        monkeypatch.delenv(k, raising=False)  # registers restore of the pinned ""
    yield monkeypatch


def _use_dotenv_file(monkeypatch: pytest.MonkeyPatch, root: Path) -> None:
    from dotenv import load_dotenv

    monkeypatch.setattr(
        dbc, "_load_project_dotenv", lambda: load_dotenv(root / ".env", override=False)
    )


def test_golden_parity_against_every_legacy_copy(isolated_env, tmp_path: Path) -> None:
    changes: dict[str, list[tuple]] = {v: [] for v in LEGACY}
    for si, sd, di, dd in SCENARIOS:
        root = _set_scenario(tmp_path, si, sd, di, dd)
        _use_dotenv_file(isolated_env, root)
        new = _outcome(dbc.inventory_dsn)
        want = _expected_new(si, sd, di, dd)
        assert new == (("dsn", want) if want else ("error", "RuntimeError")), (si, sd, di, dd, new)
        for variant, legacy in LEGACY.items():
            _set_scenario(tmp_path, si, sd, di, dd)
            old = _outcome(lambda: legacy(root))
            if variant != "D" and old == ("error", "SystemExit") and new[0] == "error":
                continue  # InventoryDsnMissing is a SystemExit too
            if old != new:
                cls = _change_class(variant, si, sd, di, dd)
                assert cls, f"unexplained change {variant} {(si, sd, di, dd)}: {old} -> {new}"
                changes[variant].append(((si, sd, di, dd), old, new, cls))
    assert {v: len(c) for v, c in changes.items()} == EXPECTED_CHANGE_COUNTS, changes


# ---------------------------------------------------------------------------
# Precedence
# ---------------------------------------------------------------------------


def test_inventory_url_beats_database_url(isolated_env) -> None:
    isolated_env.setenv("INVENTORY_DATABASE_URL", "postgresql://inv/db")
    isolated_env.setenv("DATABASE_URL", "postgresql://db/db")
    assert dbc.inventory_dsn() == "postgresql://inv/db"


def test_database_url_when_inventory_blank(isolated_env) -> None:
    isolated_env.setenv("INVENTORY_DATABASE_URL", "   ")
    isolated_env.setenv("DATABASE_URL", "postgresql://db/db")
    assert dbc.inventory_dsn() == "postgresql://db/db"


def test_env_wins_and_dotenv_is_not_loaded(isolated_env) -> None:
    calls = []
    isolated_env.setattr(dbc, "_load_project_dotenv", lambda: calls.append(1))
    isolated_env.setenv("DATABASE_URL", "postgresql://db/db")
    assert dbc.inventory_dsn() == "postgresql://db/db"
    assert calls == []


def test_dotenv_fills_a_blank_exported_key(isolated_env, tmp_path: Path) -> None:
    isolated_env.setenv("INVENTORY_DATABASE_URL", "")
    (tmp_path / ".env").write_text("INVENTORY_DATABASE_URL='postgresql://dot/db'\n")
    _use_dotenv_file(isolated_env, tmp_path)
    assert dbc.inventory_dsn() == "postgresql://dot/db"


def test_blank_keys_are_put_back_when_dotenv_has_nothing(isolated_env) -> None:
    isolated_env.setenv("INVENTORY_DATABASE_URL", "")
    isolated_env.setenv("DATABASE_URL", "")
    isolated_env.setattr(dbc, "_load_project_dotenv", lambda: None)
    assert dbc.inventory_dsn(require=False) is None
    assert os.environ["INVENTORY_DATABASE_URL"] == ""
    assert os.environ["DATABASE_URL"] == ""


def test_real_loader_is_load_project_dotenv(isolated_env) -> None:
    from backend.utils import project_env

    calls = []
    isolated_env.setattr(project_env, "load_project_dotenv", lambda **kw: calls.append(kw))
    assert dbc.inventory_dsn(require=False) is None
    assert calls == [{}]


def test_pytest_session_never_reaches_the_dev_dotenv() -> None:
    """Root conftest pins both URLs blank and disables .env: nothing resolves."""
    assert os.environ.get("INVENTORY_DATABASE_URL") == ""
    assert dbc.inventory_dsn(require=False) is None
    assert os.environ.get("INVENTORY_DATABASE_URL") == ""


def test_missing_is_systemexit_and_runtimeerror(isolated_env) -> None:
    isolated_env.setattr(dbc, "_load_project_dotenv", lambda: None)
    with pytest.raises(SystemExit):
        dbc.inventory_dsn()
    with pytest.raises(RuntimeError):
        dbc.inventory_dsn()
    assert issubclass(dbc.InventoryDsnMissing, Exception)


def test_export_fills_inventory_url_only_when_blank(isolated_env) -> None:
    isolated_env.setenv("DATABASE_URL", "postgresql://db/db")
    assert dbc.inventory_dsn(export=True) == "postgresql://db/db"
    assert os.environ["INVENTORY_DATABASE_URL"] == "postgresql://db/db"
    isolated_env.setenv("INVENTORY_DATABASE_URL", "postgresql://inv/db")
    dbc.inventory_dsn(export=True)
    assert os.environ["INVENTORY_DATABASE_URL"] == "postgresql://inv/db"


def test_no_export_by_default(isolated_env) -> None:
    isolated_env.setenv("DATABASE_URL", "postgresql://db/db")
    dbc.inventory_dsn()
    assert "INVENTORY_DATABASE_URL" not in os.environ


# ---------------------------------------------------------------------------
# connect / connection
# ---------------------------------------------------------------------------


class _FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def execute(self, sql, *a):
        self.conn.statements.append(sql)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _FakeConn:
    def __init__(self, dsn, kwargs):
        self.dsn, self.kwargs = dsn, kwargs
        self.statements: list[str] = []
        self.events: list[str] = []

    def cursor(self):
        return _FakeCursor(self)

    def commit(self):
        self.events.append("commit")

    def rollback(self):
        self.events.append("rollback")

    def close(self):
        self.events.append("close")


@pytest.fixture
def fake_psycopg(isolated_env):
    made: list[_FakeConn] = []

    class _Psycopg:
        @staticmethod
        def connect(dsn, **kwargs):
            made.append(_FakeConn(dsn, kwargs))
            return made[-1]

    isolated_env.setitem(sys.modules, "psycopg", _Psycopg)
    isolated_env.setenv("INVENTORY_DATABASE_URL", "postgresql://inv/db")
    return made


def test_connect_default_is_transactional_with_timeout(fake_psycopg) -> None:
    conn = dbc.connect()
    assert conn.dsn == "postgresql://inv/db"
    assert conn.kwargs == {"connect_timeout": 15}
    assert conn.statements == [] and conn.events == []


def test_connect_autocommit_and_timeout(fake_psycopg) -> None:
    conn = dbc.connect(autocommit=True, timeout=20)
    assert conn.kwargs == {"autocommit": True, "connect_timeout": 20}


def test_connect_timeout_none_passes_no_kwargs(fake_psycopg) -> None:
    """Callers that used bare ``psycopg.connect(url)`` keep exactly that call."""
    conn = dbc.connect(timeout=None)
    assert conn.kwargs == {}


def test_connect_read_only_is_server_enforced(fake_psycopg) -> None:
    conn = dbc.connect(read_only=True)
    assert conn.statements == ["SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY"]
    assert conn.events == ["commit"]


def test_connect_read_only_autocommit_needs_no_commit(fake_psycopg) -> None:
    conn = dbc.connect(autocommit=True, read_only=True)
    assert conn.statements == ["SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY"]
    assert conn.events == []


def test_connect_explicit_dsn_wins(fake_psycopg) -> None:
    assert dbc.connect(dsn="postgresql://other/db").dsn == "postgresql://other/db"


def test_connection_commits_and_closes(fake_psycopg) -> None:
    with dbc.connection() as conn:
        pass
    assert conn.events == ["commit", "close"]


def test_connection_rolls_back_on_error(fake_psycopg) -> None:
    with pytest.raises(ValueError):
        with dbc.connection() as conn:
            raise ValueError("boom")
    assert conn.events == ["rollback", "close"]


def test_connection_autocommit_only_closes(fake_psycopg) -> None:
    with dbc.connection(autocommit=True) as conn:
        pass
    assert conn.events == ["close"]


# ---------------------------------------------------------------------------
# Guard: no script may grow its own DSN helper or .env parser again
# ---------------------------------------------------------------------------

_DEF_DSN = re.compile(r"^\s*def _dsn\(", re.M)
_REGEX_ENV_LINE = re.compile(r"""re\.\w+\(\s*r?["']\^?[A-Z][A-Z0-9_]*=""")


def _script_files() -> list[Path]:
    out: list[Path] = []
    for base in ("backend/scripts", "scripts"):
        out.extend(sorted((REPO_ROOT / base).rglob("*.py")))
    return out


def _violations(src: str) -> list[str]:
    found = []
    if _DEF_DSN.search(src):
        found.append("defines def _dsn(")
    if _REGEX_ENV_LINE.search(src):
        found.append("parses KEY=value with a regex")
    try:
        for tok in tokenize.generate_tokens(io.StringIO(src).readline):
            if tok.type == tokenize.STRING:
                lit = tok.string.lstrip("rRbBuUfF")
                if lit in ('".env"', "'.env'"):
                    found.append(f"opens .env by path (line {tok.start[0]})")
                    break
    except (tokenize.TokenError, IndentationError, SyntaxError):
        pass
    return found


def test_guard_detects_the_legacy_shape() -> None:
    legacy = (
        "def _dsn() -> str:\n"
        "    env = _REPO_ROOT / \".env\"\n"
        "    m = re.search(r\"^INVENTORY_DATABASE_URL=(.+)$\", env.read_text(), re.M)\n"
    )
    assert len(_violations(legacy)) == 3
    assert _violations("url = re.search(\n    r'^INVENTORY_DATABASE_URL=(.+)$', open('.env').read())\n")


def test_no_script_defines_dsn_or_parses_dotenv() -> None:
    files = _script_files()
    assert len(files) > 50  # the scan actually saw the scripts
    bad = {
        str(p.relative_to(REPO_ROOT)): v
        for p in files
        if (v := _violations(p.read_text(encoding="utf-8", errors="replace")))
    }
    assert bad == {}, (
        "use backend.db.connect.inventory_dsn()/connect() instead of a private DSN "
        f"helper or a hand-rolled .env parser: {bad}"
    )
