"""Cloud sessions: scripts/cloud, the SessionStart hook and docs/CLOUD.md. No network, no installs.

docs/CLOUD.md is the owner's guide to running Claude Code on the web against this repo. What
it relies on fails quietly: a CPU torch install that falls back to the multi-GB CUDA wheel, a
venv that is not Python 3.12, a hook that also fires on the owner's Mac, a pgeocode cache that
is cold when the first test imports backend.db.geo, a Postgres that is not the one CI runs. This
module pins what must match CI (.github/workflows/ci.yml is read, not copied) and what must stay
true of the hook and the guide. It runs only what is safe to run: ``--help``, ``--check``
(read-only), the hook's own guard, the argument errors, and single functions of setup.sh against
fake trees and stub commands. It never runs an install.

The guide goes to a public repository, so it is scanned for addresses, private host names and
secret shapes the way a release scans its diff (docs/RELEASING.md, release checklist step 4).
"""

from __future__ import annotations

import json
import os
import re
import shutil
import stat
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SETUP = REPO_ROOT / "scripts" / "cloud" / "setup.sh"
WITH_PG = REPO_ROOT / "scripts" / "cloud" / "with_pg.sh"
PG_TEST_UP = REPO_ROOT / "scripts" / "pg_test_up.sh"
PG_HARNESS = REPO_ROOT / "backend" / "tests" / "pg_harness.py"
PRE_PUSH = REPO_ROOT / "scripts" / "git-hooks" / "pre-push"
SETTINGS = REPO_ROOT / ".claude" / "settings.json"
DOC = REPO_ROOT / "docs" / "CLOUD.md"
CI_YML = REPO_ROOT / ".github" / "workflows" / "ci.yml"
# Excluded from the git greps below: a name this file lists must not vouch for itself.
THIS_FILE = "backend/tests/test_cloud_setup.py"

CLOUD_FILES = (SETUP, WITH_PG, SETTINGS, DOC)

needs_bash = pytest.mark.skipif(shutil.which("bash") is None, reason="bash not available")


# --- helpers ------------------------------------------------------------------


def _text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _code(path: Path) -> str:
    """The script without comment-only lines, so prose cannot trip or mask a check."""
    return "\n".join(line for line in _text(path).splitlines() if not line.lstrip().startswith("#"))


def _const(name: str) -> str:
    m = re.search(rf'^{name}="([^"]*)"$', _text(SETUP), re.M)
    assert m, f"{name} is not a plain assignment in setup.sh"
    return m.group(1)


def _function(name: str) -> str:
    """Body of a top-level shell function (up to the closing brace at column 0)."""
    m = re.search(rf"^{name}\(\) \{{.*?^\}}", _text(SETUP), re.M | re.S)
    assert m, f"function {name} not found in setup.sh"
    return m.group(0)


def _ci_runs() -> str:
    return "\n".join(re.findall(r"^\s+run:\s*(.+)$", _text(CI_YML), re.M))


def _run(argv, env, cwd=None, timeout=60, input=None):
    return subprocess.run(argv, env=env, cwd=cwd, capture_output=True, text=True, timeout=timeout, input=input)


def _clean_env(tmp_path: Path, **extra: str) -> dict:
    """A minimal environment: nothing from the developer's shell or CI leaks into the script."""
    home = tmp_path / "home"
    home.mkdir(exist_ok=True)
    # Docker's Rosetta emulation (an amd64 container on Apple silicon) caches under $HOME/.cache/rosetta
    # for every process it runs. Make it exist up front; _tree() ignores what lands in it.
    (home / ".cache" / "rosetta").mkdir(parents=True, exist_ok=True)
    scratch = tmp_path / "tmp"
    scratch.mkdir(exist_ok=True)
    env = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "HOME": str(home),
        "TMPDIR": str(scratch),
        "LC_ALL": "C",
        "DS_CLOUD_VENV": str(tmp_path / "venv"),
        "DS_CLOUD_LOG_DIR": str(tmp_path / "logs"),
        "PGEOCODE_DATA_DIR": str(tmp_path / "pgeocode"),
    }
    env.update(extra)
    return env


def _tree(path: Path) -> list[str]:
    """Every path under *path* (relative), for "the script wrote nothing" checks."""
    rels = (p.relative_to(path).as_posix() for p in path.rglob("*"))
    return sorted(r for r in rels if not r.startswith("home/.cache/rosetta/"))


def _sourced(tmp_path: Path, body: str, *args: str, **env_extra: str) -> subprocess.CompletedProcess:
    """Run *body* in a bash that has sourced setup.sh (so its functions exist and nothing has run).

    setup.sh turns on ``set -euo pipefail``, which the sourcing shell keeps: start *body* with
    ``set +e`` when it calls a function that is meant to fail.
    """
    return _run(
        ["bash", "-c", f'source "$1"; shift; {body}', "caller", str(SETUP), *args],
        env=_clean_env(tmp_path, **env_extra),
    )


def _executable(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return path


# --- the scripts exist and are well formed -----------------------------------------


@pytest.mark.parametrize("script", [SETUP, WITH_PG], ids=lambda p: p.name)
def test_scripts_are_executable_strict_bash(script):
    assert script.is_file()
    assert os.access(script, os.X_OK), f"{script.name} lost its executable bit"
    text = _text(script)
    assert text.startswith("#!/usr/bin/env bash\n")
    assert re.search(r"^set -euo pipefail$", text, re.M)


@needs_bash
@pytest.mark.parametrize("script", [SETUP, WITH_PG], ids=lambda p: p.name)
def test_scripts_parse(script):
    r = _run(["bash", "-n", str(script)], env=os.environ.copy())
    assert r.returncode == 0, r.stderr


@pytest.mark.skipif(not Path("/bin/bash").exists(), reason="no /bin/bash")
@pytest.mark.parametrize("script", [SETUP, WITH_PG], ids=lambda p: p.name)
def test_scripts_parse_with_the_system_bash(script):
    # macOS ships bash 3.2, the oldest the header promises (--check and --help run on the Mac).
    r = _run(["/bin/bash", "-n", str(script)], env=os.environ.copy())
    assert r.returncode == 0, r.stderr


@pytest.mark.skipif(shutil.which("shellcheck") is None, reason="shellcheck not installed")
@pytest.mark.parametrize("script", [SETUP, WITH_PG], ids=lambda p: p.name)
def test_scripts_are_shellcheck_clean(script):
    r = _run(["shellcheck", "--severity=warning", str(script)], env=os.environ.copy())
    assert r.returncode == 0, r.stdout + r.stderr


# --- setup.sh matches CI --------------------------------------------------------------


def test_python_is_312_like_ci():
    assert _const("PYTHON_MINOR") == "3.12"
    # Every python3.N the script names is 3.12: no stray fallback to another minor.
    assert set(re.findall(r"python3\.(\d+)", _code(SETUP))) <= {"12"}
    versions = set(re.findall(r'python-version:\s*"?([0-9.]+)"?', _text(CI_YML)))
    assert versions == {"3.12"}, "CI moved off 3.12: move PYTHON_MINOR and the doc with it"


def test_cpu_torch_index_is_the_one_ci_uses():
    ci = re.search(r"pip install torch --index-url (\S+)", _ci_runs())
    assert ci, "CI no longer installs CPU torch from an index"
    assert _const("TORCH_INDEX") == ci.group(1) == "https://download.pytorch.org/whl/cpu"
    assert 'torch --index-url "$TORCH_INDEX"' in _function("torch_install")


def test_torch_comes_before_the_requirements_and_gates_them():
    body = _function("step_python")
    assert body.index('"cpu torch"') < body.index('"requirements"')
    install = _function("reqs_install")
    # The CUDA wheel is the failure mode: no requirements install unless CPU torch is in.
    assert install.index("torch_ok") < install.index("-r ")


def test_requirements_install_is_the_ci_command():
    ci = re.search(r"pip install ((?:-r \S+ ?)+)", _ci_runs())
    assert ci
    ci_files = re.findall(r"-r (\S+)", ci.group(1))
    assert ci_files == ["requirements.txt", "requirements-test.txt"]
    ours = re.findall(r'-r "\$ROOT/([^"]+)"', _function("reqs_install"))
    assert ours == ci_files


def test_ruff_pin_is_the_one_ci_uses():
    ci = re.search(r"pip install (ruff==[0-9.]+)", _ci_runs())
    assert ci, "CI no longer pins ruff"
    assert _const("RUFF_PIN") == ci.group(1)


def test_requirements_digest_covers_every_requirements_file_and_the_index():
    body = _function("reqs_digest")
    assert '"$ROOT"/requirements*.txt' in body and "$TORCH_INDEX" in body


def test_node_check_expects_node_20_like_ci():
    assert _const("NODE_DIR") == "/opt/node20/bin"
    node = re.search(r'node-version:\s*"?(\d+)"?', _text(CI_YML))
    assert node and node.group(1) == "20"
    assert "v20.*" in _function("node_ok")


def test_pgeocode_is_seeded_from_geonames_then_the_github_mirror():
    urls = _const("PGEOCODE_URLS").split()
    assert urls[0].startswith("https://download.geonames.org/")
    assert urls[1].startswith("https://raw.githubusercontent.com/")
    assert all("{country}" in u for u in urls)
    body = _function("pgeocode_seed")
    # One source per try, fetched by curl and handed to pgeocode as a file: pgeocode's own fallback
    # only catches HTTP errors, and urllib has its own CA setup behind a cloud proxy.
    assert "curl -fsSL" in body and "--connect-timeout" in body
    assert "as_uri()" in body and "pgeocode.DOWNLOAD_URL" in body
    # The cache is the library's default, so no environment variable is needed in a session.
    assert "PGEOCODE_DATA_DIR" not in _code(SETUP)


def test_git_wiring_matches_the_release_rules():
    code = _code(SETUP)
    assert "core.hooksPath scripts/git-hooks" in code
    assert PRE_PUSH.is_file() and os.access(PRE_PUSH, os.X_OK)
    # The pre-push hook compares a new branch with refs/remotes/<remote>/main and skips quietly without it.
    assert "refs/remotes/$remote/main" in _text(PRE_PUSH)
    assert "+refs/heads/main:refs/remotes/origin/main" in code


def test_postgres_matches_ci_and_the_harness():
    major = _const("PG_MAJOR")
    assert major == "18"
    image = re.search(r"image:\s*pgvector/pgvector:\S+-pg(\d+)", _text(CI_YML))
    assert image and image.group(1) == major, "CI's pg major moved: move PG_MAJOR and the doc with it"
    assert _const("PG_LIB_ROOT") == "/usr/lib/postgresql" and _const("PG_SHARE_ROOT") == "/usr/share/postgresql"
    # The fallback majors are ones the harness accepts, and the one installed is among them.
    floor = re.search(r"< (\d+)000:\s*\n\s*raise PgUnavailable\(\"CREATE DATABASE", _text(PG_HARNESS))
    assert floor, "the harness no longer states a minimum server version"
    fallback = _const("PG_FALLBACK_MAJORS").split()
    assert fallback and all(int(m) >= int(floor.group(1)) // 10 and int(m) < int(major) for m in fallback)
    assert _const("PG_FALLBACK_INSTALL") in fallback
    # with_pg.sh walks the same majors in the same order (it does not source setup.sh).
    loop = re.search(r"for m in ([0-9 ]+); do", _text(WITH_PG))
    assert loop and loop.group(1).split() == [major, *fallback]
    helper = _text(PG_TEST_UP)
    assert "PG_TEST_BIN" in helper and "PG_TEST_EXT" in helper
    env_lines = _function("env_file_write")
    assert "export PG_TEST_BIN=" in env_lines and "export PG_TEST_EXT=" in env_lines
    body = _function("pg_install")
    for package in ('"postgresql-$PG_MAJOR"', '"postgresql-client-$PG_MAJOR"', '"postgresql-$PG_MAJOR-pgvector"'):
        assert package in body, package
    assert "create_main_cluster = false" in _function("pg_no_default_cluster")
    assert "pg_no_default_cluster" in body and "pg_no_default_cluster" in _function("pgfb_install")


def test_pgdg_source_is_signed_and_the_key_fingerprint_is_pinned():
    assert re.fullmatch(r"[0-9A-F]{40}", _const("PGDG_KEY_FPR"))
    # Plain http on purpose (see the comment on the constant): apt verifies every file against the key.
    assert _const("PGDG_URL").startswith("http://apt.postgresql.org/")
    assert _const("PGDG_KEY_URL").startswith("https://www.postgresql.org/")
    # The key normally comes with Ubuntu's postgresql-common, so the allowed domains need no key host.
    assert _const("PGDG_KEY") == "/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc"
    body = _function("pg_install")
    assert "signed-by=$PGDG_KEY" in body
    assert '"$fpr" != "$PGDG_KEY_FPR"' in body
    assert "postgresql-common" in body
    # Reachability is checked before apt's sources are touched, so a blocked host leaves apt healthy.
    assert body.index("InRelease") < body.index('"$PGDG_LIST"')


# --- nothing here touches data or secrets ---------------------------------------------


def test_setup_never_connects_to_a_database_or_names_one():
    forbidden = [
        r"DATABASE_URL", r"PGVECTOR_URL", r"postgres(ql)?://", r"PGPASSWORD", r"\bcreatedb\b", r"\bdropdb\b",
        r"\bpg_dump\b", r"\bpg_restore\b", r"rlwy\.net", r"railway", r"15432", r"\bcars(_cleanroom)?\b",
    ]
    for script in (SETUP, WITH_PG):
        code = _code(script)
        for pattern in forbidden:
            assert not re.search(pattern, code, re.I), f"{script.name}: {pattern}"


def test_with_pg_hands_the_dsn_only_to_the_command_after_the_cluster_is_up():
    code = _code(WITH_PG)
    assert "pg_test_up.sh" in code
    # The one assignment is the line that runs the command; the holder only echoes what pg_test_up.sh exported.
    assigns = re.findall(r"^.*TEST_PG_ADMIN_DSN=.*$", code, re.M)
    assert len(assigns) == 1, assigns
    assert 'TEST_PG_ADMIN_DSN="$dsn" SCHEMA_PARITY_PG_DSN="$dsn" REQUIRE_PG=1 "$@"' in assigns[0], assigns
    assert "runuser -u postgres" in code  # initdb and pg_ctl refuse root
    # The cluster ends through pg_test_up.sh's own exit path (`--` mode plus a stop file), never by a signal:
    # bash defers a trap until the foreground `sleep 3600` of the no-command mode ends.
    assert 'pg_test_up.sh" -- bash -c' in code and '"$STOP"' in code
    # The postgres user gets the Postgres paths spelled out; nothing else of this environment.
    assert '"PG_TEST_BIN=$PG_TEST_BIN"' in code and '"PG_TEST_EXT=$PG_TEST_EXT"' in code


@pytest.mark.parametrize("path", CLOUD_FILES, ids=lambda p: p.name)
def test_no_secret_shapes_in_the_cloud_files(path):
    text = _text(path)
    shapes = [
        r"AIza[0-9A-Za-z_-]{35}", r"sk-ant-[0-9A-Za-z_-]{20,}", r"sk_live_[0-9A-Za-z]{16,}", r"AKIA[0-9A-Z]{16}",
        r"gh[pousr]_[0-9A-Za-z]{36}", r"-----BEGIN [A-Z ]*PRIVATE KEY-----",
        r"postgres(ql)?://[^:/@ ]+:[^@/ ]+@", r"eyJ[0-9A-Za-z_-]{10,}\.eyJ[0-9A-Za-z_-]{10,}",
    ]
    for shape in shapes:
        assert not re.search(shape, text), f"{path.name}: {shape}"


# --- what runs safely --------------------------------------------------------------------


@needs_bash
def test_help_names_the_modes_and_exits_zero(tmp_path):
    r = _run(["bash", str(SETUP), "--help"], env=_clean_env(tmp_path))
    assert r.returncode == 0, r.stderr
    for needle in ("--check", "--hook", "--skip", "--only", "docs/CLOUD.md", "exit 1 when a required", "item is missing"):
        assert needle in r.stdout, needle


@needs_bash
@pytest.mark.parametrize("argv", [["--no-such-option"], ["--only", "nope"], ["--skip=git,nope"]])
def test_bad_arguments_exit_2_before_doing_anything(tmp_path, argv):
    r = _run(["bash", str(SETUP), *argv], env=_clean_env(tmp_path))
    assert r.returncode == 2, (r.stdout, r.stderr)
    assert _tree(tmp_path / "tmp") == [] and not (tmp_path / "venv").exists()


@needs_bash
def test_hook_mode_does_nothing_outside_a_cloud_session(tmp_path):
    env = _clean_env(tmp_path)
    before = _tree(tmp_path)
    r = _run(["bash", str(SETUP), "--hook"], env=env)
    assert r.returncode == 0
    assert r.stdout == "" and r.stderr == ""  # a hook's stdout is added to the session's context
    assert _tree(tmp_path) == before


@needs_bash
def test_hook_mode_never_fails_the_session(tmp_path):
    # In a cloud session (the variable is set) with a log directory that cannot be created, the setup
    # stops before any step runs. A SessionStart hook must still exit 0, and say what happened.
    # (Off Linux it only reports that there is nothing to install; the exit code is the same.)
    env = _clean_env(tmp_path, CLAUDE_CODE_REMOTE="true", DS_CLOUD_LOG_DIR="/dev/null/cannot-exist")
    r = _run(["bash", str(SETUP), "--hook"], env=env)
    assert r.returncode == 0, (r.stdout, r.stderr)
    assert r.stdout.startswith("cloud-setup: "), r.stdout
    assert not (tmp_path / "venv").exists()


@needs_bash
def test_check_mode_reports_every_gap_and_changes_nothing(tmp_path):
    hooks_before = (
        subprocess.run(["git", "-C", str(REPO_ROOT), "config", "--get", "core.hooksPath"], capture_output=True, text=True).stdout
        if shutil.which("git")
        else ""
    )
    link_before = (REPO_ROOT / ".venv").exists()
    env = _clean_env(tmp_path)
    before = _tree(tmp_path)

    r = _run(["bash", str(SETUP), "--check"], env=env, cwd=tmp_path)

    # The venv path is under tmp_path and absent, so the Python side is missing: exit 1.
    assert r.returncode == 1, (r.stdout, r.stderr)
    for label in ("git hooks", "python venv", "cpu torch", "requirements", "ruff", "pgeocode table", "node 20", "postgres 18"):
        assert label in r.stdout, label
    assert re.search(r"^\s*missing\s+python venv\s+no venv at ", r.stdout, re.M)
    assert "required item(s) missing" in r.stdout
    # Read-only: the scratch tree, the repo's .venv link and core.hooksPath are as they were.
    assert _tree(tmp_path) == before
    assert (REPO_ROOT / ".venv").exists() == link_before
    if shutil.which("git"):
        hooks_after = subprocess.run(
            ["git", "-C", str(REPO_ROOT), "config", "--get", "core.hooksPath"], capture_output=True, text=True
        ).stdout
        assert hooks_after == hooks_before


@needs_bash
def test_check_with_only_limits_the_report(tmp_path):
    r = _run(["bash", str(SETUP), "--check", "--only", "node"], env=_clean_env(tmp_path))
    assert r.returncode == 0, r.stdout  # node is optional, so a missing Node 20 is not a failure
    assert "node 20" in r.stdout and "python venv" not in r.stdout


@needs_bash
def test_sourcing_defines_the_functions_and_runs_nothing(tmp_path):
    env = _clean_env(tmp_path)
    before = _tree(tmp_path)
    # The path is $1, not $0: a script is "run" (not sourced) exactly when $0 is its own path.
    script = 'source "$1" && declare -F step_python step_pg ensure >/dev/null && echo sourced'
    r = _run(["bash", "-c", script, "caller", str(SETUP)], env=env)
    assert r.returncode == 0 and r.stdout.strip() == "sourced", (r.stdout, r.stderr)
    assert _tree(tmp_path) == before


@needs_bash
def test_requirements_digest_decides_when_pip_runs_again(tmp_path):
    root = tmp_path / "repo"
    root.mkdir()
    (root / "requirements.txt").write_text("-r requirements-web.txt\n", encoding="utf-8")
    (root / "requirements-web.txt").write_text("flask>=3\n", encoding="utf-8")

    def digest() -> str:
        r = _sourced(tmp_path, 'ROOT="$1"; reqs_digest', str(root))
        assert r.returncode == 0, r.stderr
        assert re.fullmatch(r"[0-9a-f]{64}", r.stdout.strip()), r.stdout
        return r.stdout.strip()

    first = digest()
    assert digest() == first  # stable while nothing changes: the install is skipped
    (root / "requirements-web.txt").write_text("flask>=3.1\n", encoding="utf-8")
    changed = digest()
    assert changed != first  # a requirement moved on a branch: pip runs again
    (root / "requirements-extra.txt").write_text("x\n", encoding="utf-8")
    assert digest() != changed  # any requirements*.txt counts, as the Dockerfiles' glob does


@needs_bash
@pytest.mark.parametrize(
    "skip,only,step,enabled",
    [
        ("", "", "pg", True),
        ("pg", "", "pg", False),
        ("pg", "", "python", True),
        ("", "git,node", "node", True),
        ("", "git,node", "pg", False),
        ("node", "git,node", "node", False),  # --skip wins over --only
    ],
)
def test_step_filter(tmp_path, skip, only, step, enabled):
    r = _sourced(tmp_path, 'SKIP="$1"; ONLY="$2"; want "$3"', skip, only, step)
    assert (r.returncode == 0) is enabled, (r.stdout, r.stderr)


@needs_bash
def test_with_pg_needs_a_command_after_double_dash(tmp_path):
    for argv in ([], ["true"]):
        r = _run(["bash", str(WITH_PG), *argv], env=_clean_env(tmp_path))
        assert r.returncode == 2 and "usage" in r.stderr, (argv, r.stderr)


# --- the Postgres choice: CI's major first, then the image's own ----------------------------


def _fake_pg(root: Path, major: int, *, pgvector=True, dblink=True, binaries=True, version="16.15") -> None:
    """A Debian-layout PostgreSQL <major> under *root*: lib/<major>/bin and share/<major>/extension."""
    bindir = root / "lib" / str(major) / "bin"
    extdir = root / "share" / str(major) / "extension"
    bindir.mkdir(parents=True, exist_ok=True)
    extdir.mkdir(parents=True, exist_ok=True)
    if binaries:
        for name in ("initdb", "pg_ctl", "pg_isready", "psql"):
            _executable(bindir / name, "#!/bin/sh\n")
        _executable(bindir / "postgres", f"#!/bin/sh\necho 'postgres (PostgreSQL) {version} (Test build)'\n")
    if pgvector:
        (extdir / "vector.control").write_text("", encoding="utf-8")
    if dblink:
        (extdir / "dblink.control").write_text("", encoding="utf-8")


_PG_PRELUDE = 'set +e; PG_LIB_ROOT="$1/lib"; PG_SHARE_ROOT="$1/share"; '


@needs_bash
def test_pg_is_not_ok_on_an_empty_machine(tmp_path):
    root = tmp_path / "pg"
    root.mkdir()
    r = _sourced(tmp_path, _PG_PRELUDE + 'pg_ok; echo "rc=$? detail=$DETAIL"; pg_pick_major; echo "pick=$?"', str(root))
    assert "rc=1 detail=no PostgreSQL 18 server and client binaries" in r.stdout, r.stdout
    assert "pick=1" in r.stdout


@needs_bash
def test_pg_18_wins_over_a_fallback_major(tmp_path):
    root = tmp_path / "pg"
    _fake_pg(root, 18, version="18.6")
    _fake_pg(root, 16)
    r = _sourced(tmp_path, _PG_PRELUDE + 'pg_ok; echo "rc=$? detail=$DETAIL"; echo "pick=$(pg_pick_major)"', str(root))
    assert "rc=0 detail=PostgreSQL 18.6 with pgvector and dblink" in r.stdout, r.stdout
    assert "pick=18" in r.stdout


@needs_bash
def test_pg_18_without_pgvector_does_not_count_and_the_first_fallback_is_picked(tmp_path):
    root = tmp_path / "pg"
    _fake_pg(root, 18, pgvector=False)
    _fake_pg(root, 15)
    _fake_pg(root, 16, version="16.15")
    r = _sourced(
        tmp_path,
        _PG_PRELUDE + 'pg_ok; echo "rc=$? detail=$DETAIL"; pgfb_ok; echo "fb=$? detail=$DETAIL"; echo "pick=$(pg_pick_major)"',
        str(root),
    )
    assert "rc=1 detail=pgvector is missing from" in r.stdout, r.stdout
    # 17 is absent, so 16 is the first complete fallback, ahead of 15; the report says it is not parity.
    assert "fb=0 detail=PostgreSQL 16.15 with pgvector and dblink; NOT CI parity (CI and production run 18)" in r.stdout
    assert "pick=16" in r.stdout


@needs_bash
def test_a_fallback_major_without_dblink_is_not_usable(tmp_path):
    root = tmp_path / "pg"
    _fake_pg(root, 16, dblink=False)
    r = _sourced(tmp_path, _PG_PRELUDE + 'pgfb_ok; echo "fb=$? detail=$DETAIL"', str(root))
    assert "fb=1 detail=no PostgreSQL 17, 16, 15 with pgvector either" in r.stdout, r.stdout


@needs_bash
def test_check_mode_reports_the_fallback_only_when_18_is_missing(tmp_path):
    body = _PG_PRELUDE + 'CHECK_ONLY=1; REPORT="$(mktemp)"; step_pg; sort "$REPORT" | cut -d"|" -f2-3; rm -f "$REPORT"'
    only16 = tmp_path / "only16"
    _fake_pg(only16, 16)
    r = _sourced(tmp_path, body, str(only16))
    assert r.stdout.splitlines() == ["optional|postgres 18 + pgvector", "ok|postgres fallback"], r.stdout

    both = tmp_path / "both"
    _fake_pg(both, 18, version="18.6")
    _fake_pg(both, 16)
    r = _sourced(tmp_path, body, str(both))
    assert r.stdout.splitlines() == ["ok|postgres 18 + pgvector"], r.stdout  # the fallback is not even looked at

    none = tmp_path / "none"
    none.mkdir()
    r = _sourced(tmp_path, body, str(none))
    assert r.stdout.splitlines() == ["optional|postgres 18 + pgvector", "optional|postgres fallback"], r.stdout


@needs_bash
def test_session_env_file_gets_each_line_once_and_names_the_picked_postgres(tmp_path):
    root = tmp_path / "pg"
    _fake_pg(root, 16)
    envfile = tmp_path / "envfile"
    envfile.write_text("export OTHER_HOOK=1\n", encoding="utf-8")  # another hook's line stays
    body = (
        _PG_PRELUDE
        + f'VENV=/opt/ds-venv; NODE_DIR={tmp_path}/no-node; CLAUDE_ENV_FILE="{envfile}"; env_file_write; env_file_write'
    )
    r = _sourced(tmp_path, body, str(root))
    assert r.returncode == 0, r.stderr
    assert envfile.read_text(encoding="utf-8").splitlines() == [
        "export OTHER_HOOK=1",
        'export PATH="/opt/ds-venv/bin:$PATH"',  # $PATH stays literal: expanded when the file is run
        f'export PG_TEST_BIN="{root}/lib/16/bin"',
        f'export PG_TEST_EXT="{root}/share/16/extension"',
    ]
    # With Node 20 on the image it comes right after the venv.
    node = tmp_path / "node" / "bin"
    node.mkdir(parents=True)
    envfile2 = tmp_path / "envfile2"
    r = _sourced(
        tmp_path,
        _PG_PRELUDE + f'VENV=/opt/ds-venv; NODE_DIR={node}; CLAUDE_ENV_FILE="{envfile2}"; env_file_write',
        str(tmp_path / "empty"),
    )
    assert r.returncode == 0, r.stderr
    assert envfile2.read_text(encoding="utf-8").splitlines() == [f'export PATH="/opt/ds-venv/bin:{node}:$PATH"']


# --- the pgeocode cache is seeded from a reachable source --------------------------------------

_GEONAMES_ROWS = (
    "US\t28173\tWaxhaw\tNorth Carolina\tNC\tUnion\t179\t\t\t34.8847\t-80.7574\t4\n"
    "US\t28202\tCharlotte\tNorth Carolina\tNC\tMecklenburg\t119\t\t\t35.2271\t-80.8431\t4\n"
)

_STUB_CURL = """#!/bin/bash
# Stand-in for curl: the GeoNames host is "blocked", the GitHub mirror serves the fixture.
args=("$@")
url="${args[$# - 1]}"
out=""
for ((i = 0; i < $#; i++)); do [ "${args[i]}" = "-o" ] && out="${args[i + 1]}"; done
echo "$url" >> "$STUB_CURL_LOG"
case "$url" in
  *download.geonames.org*) exit 22 ;;
  *raw.githubusercontent.com*) [ "$STUB_MIRROR_UP" = 1 ] || exit 22; cp "$STUB_FIXTURE" "$out" ;;
  *) exit 22 ;;
esac
"""


def _seed(tmp_path: Path, mirror_up: bool) -> tuple[subprocess.CompletedProcess, Path, Path]:
    pytest.importorskip("pgeocode")
    fixture = tmp_path / "US.txt"
    fixture.write_text(_GEONAMES_ROWS, encoding="utf-8")
    stubs = tmp_path / "stubs"
    _executable(stubs / "curl", _STUB_CURL)
    # A wrapper, not a symlink: a symlinked interpreter outside its venv loses the venv's packages.
    _executable(tmp_path / "venv" / "bin" / "python", f'#!/bin/sh\nexec "{sys.executable}" "$@"\n')
    curl_log = tmp_path / "curl.log"
    step_log = tmp_path / "step.log"
    r = _sourced(
        tmp_path,
        f'set +e; VENV="{tmp_path / "venv"}"; STEP_LOG="{step_log}"; pgeocode_seed; echo "rc=$? fixerr=${{FIXERR:-}}"',
        PATH=f"{stubs}{os.pathsep}{os.environ.get('PATH', '')}",
        STUB_CURL_LOG=str(curl_log),
        STUB_MIRROR_UP="1" if mirror_up else "0",
        STUB_FIXTURE=str(fixture),
    )
    return r, curl_log, tmp_path / "pgeocode"


@needs_bash
def test_pgeocode_seed_falls_back_to_the_mirror_when_geonames_is_blocked(tmp_path):
    r, curl_log, cache = _seed(tmp_path, mirror_up=True)
    assert "rc=0" in r.stdout, (r.stdout, r.stderr)
    tried = curl_log.read_text(encoding="utf-8").split()
    assert tried[0] == "https://download.geonames.org/export/zip/US.zip"  # the CI source comes first
    assert tried[1] == "https://raw.githubusercontent.com/symerio/postal-codes-data/master/data/geonames/US.txt"
    # pgeocode itself parsed the downloaded file and wrote the cache the library reloads offline.
    assert (cache / "US.txt").is_file() and (cache / "US-index.txt").is_file()
    assert "Waxhaw" in (cache / "US.txt").read_text(encoding="utf-8")


@needs_bash
def test_pgeocode_seed_names_the_hosts_when_every_source_fails(tmp_path):
    r, curl_log, cache = _seed(tmp_path, mirror_up=False)
    assert "rc=1" in r.stdout and "download.geonames.org" in r.stdout, r.stdout
    assert len(curl_log.read_text(encoding="utf-8").split()) == 2
    assert not cache.exists()  # nothing half-written for a later import to trust


# --- the SessionStart hook ---------------------------------------------------------------


@pytest.fixture(scope="module")
def settings() -> dict:
    return json.loads(_text(SETTINGS))


def _hook(settings: dict) -> dict:
    groups = settings["hooks"]["SessionStart"]
    assert len(groups) == 1 and len(groups[0]["hooks"]) == 1
    return groups[0]["hooks"][0]


def test_settings_wire_only_the_remote_session_start_hook(settings):
    # Project settings apply to local sessions too: nothing else (env, permissions, other hooks) may ride along.
    assert set(settings) == {"hooks"}
    assert set(settings["hooks"]) == {"SessionStart"}
    # No matcher: the hook also runs on /clear, compaction and a fork. Each of them can bring a fresh
    # CLAUDE_ENV_FILE, and the venv has to be on that session's PATH again (the script is quiet and quick).
    assert set(settings["hooks"]["SessionStart"][0]) == {"hooks"}
    hook = _hook(settings)
    assert hook["type"] == "command"
    assert "CLAUDE_CODE_REMOTE" in hook["command"] and "scripts/cloud/setup.sh" in hook["command"]
    assert hook["command"].rstrip().endswith("--hook")
    # A cold first session installs for minutes; the default timeout (600 s) would cut it off.
    assert isinstance(hook["timeout"], int) and hook["timeout"] >= 600


@pytest.mark.skipif(shutil.which("sh") is None, reason="sh not available")
def test_hook_command_is_a_no_op_locally_and_runs_the_script_in_the_cloud(settings, tmp_path):
    project = tmp_path / "project"
    (project / "scripts" / "cloud").mkdir(parents=True)
    marker = tmp_path / "ran"
    fake = project / "scripts" / "cloud" / "setup.sh"
    fake.write_text(f'#!/usr/bin/env bash\necho "$@" > "{marker}"\n', encoding="utf-8")
    command = _hook(settings)["command"]
    base = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "CLAUDE_PROJECT_DIR": str(project)}

    for remote in (None, "", "false", "1"):
        env = dict(base)
        if remote is not None:
            env["CLAUDE_CODE_REMOTE"] = remote
        r = _run(["sh", "-c", command], env=env)
        assert r.returncode == 0 and not marker.exists(), f"CLAUDE_CODE_REMOTE={remote!r} must not run the script"

    r = _run(["sh", "-c", command], env={**base, "CLAUDE_CODE_REMOTE": "true"})
    assert r.returncode == 0, r.stderr
    assert marker.read_text(encoding="utf-8").strip() == "--hook"


# --- docs/CLOUD.md ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def doc() -> str:
    return _text(DOC)


def _section(text: str, heading: str) -> str:
    start = text.index(heading)
    end = text.find("\n## ", start + len(heading))
    return text[start:] if end == -1 else text[start:end]


def _fences(text: str) -> list[str]:
    return re.findall(r"^```[a-z]*\n(.*?)^```", text, flags=re.M | re.S)


def test_doc_covers_the_owner_steps(doc):
    for needle in (
        "Claude GitHub App", "/web-setup", "Network access", "Setup script", "Environment variables",
        "phase/N37a", "chore/cloud-setup", "scripts/bump_version.sh", "claude/", "CLAUDE_CODE_REMOTE",
        "scripts/cloud/setup.sh", "scripts/cloud/with_pg.sh", "History rewrite", "download.pytorch.org",
        "download-r2.pytorch.org", "apt.postgresql.org", "download.geonames.org", "Not confirmed",
    ):
        assert needle in doc, needle
    # Start branch: never main.
    assert re.search(r"never `?main`?", doc, re.I)


def test_doc_uses_the_current_network_access_names_and_says_what_the_image_has(doc):
    section = _section(doc, "## 2. Create the environment")
    # The environment dialog's levels today are None, Limited and Full, with Allow package managers
    # and an Allowed domains list. "Custom" survives only as the parenthetical naming the old wording.
    for needle in ("Limited", "Allow package managers", "Allowed domains", "None of the hosts below is on it"):
        assert needle in section, needle
    assert section.count("Custom") == 1 and "Older versions" in section
    # The image already has PostgreSQL 16 and Redis 7, and the guide says why neither is used as is.
    assert "PostgreSQL 16 and Redis 7.0" in section
    # Each host the setup needs is in the table, and the table says what happens without it.
    table = [line for line in section.splitlines() if line.startswith("| `")]
    hosts = {h for line in table for h in re.findall(r"`([a-z*][a-z0-9.*-]+\.[a-z]+)`", line.split("|")[1])}
    assert {"download.pytorch.org", "download-r2.pytorch.org", "apt.postgresql.org", "download.geonames.org"} <= hosts
    assert all(line.count("|") == 4 for line in table)


def test_doc_warns_that_cloud_flag_uploads_a_bundle_without_the_app(doc):
    section = _section(doc, "## 1. Connect GitHub")
    assert "bundle" in section and "Claude GitHub App" in section
    assert "Auto-fix" in section and "per pull request" in section
    assert "bundle" in _section(doc, "## 9. History rewrite")


def _slug(heading: str) -> str:
    return re.sub(r"[^\w\- ]", "", heading.strip().lower()).replace(" ", "-")


def test_every_in_page_link_resolves(doc):
    anchors = set()
    in_fence = False
    for line in doc.splitlines():
        if line.lstrip().startswith("```"):
            in_fence = not in_fence
            continue
        m = None if in_fence else re.match(r"^#{1,6} (.+)$", line)
        if m:
            anchors.add(_slug(m.group(1)))
    links = set(re.findall(r"\]\(#([^)]+)\)", doc))
    assert links, "expected in-page links"
    assert not links - anchors, sorted(links - anchors)


def test_doc_is_safe_for_a_public_repository(doc):
    checks = {
        "an IPv4 address": r"\b(?:\d{1,3}\.){3}\d{1,3}\b",
        "an email address": r"[\w.+-]+@[\w-]+\.[\w.-]+",
        "a UUID (Railway project and service ids look like this)": r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b",
        "a private or deploy host": r"\.(ts\.net|railway\.app|railway\.internal|rlwy\.net|lan|local)\b|kmac-vault|sarraficars|tailnet|tailscale",
        "a long hex token": r"\b[0-9a-f]{32,}\b",
    }
    for what, pattern in checks.items():
        m = re.search(pattern, doc, re.I)
        assert not m, f"CLOUD.md contains {what}: {m.group(0)!r}"


def _setup_script_block(doc: str) -> str:
    section = _section(doc, "## 2. Create the environment")
    blocks = [b for b in _fences(section) if "scripts/cloud/setup.sh" in b]
    assert len(blocks) == 1, "expected exactly one Setup script block in section 2"
    return blocks[0]


def test_the_setup_script_block_parses_and_never_fails_the_session(doc):
    block = _setup_script_block(doc)
    assert block.startswith("#!/bin/bash\n")
    assert re.search(r"^REF=\S+", block, re.M)
    assert '--branch "$REF"' in block and "--skip git,env" in block
    assert block.rstrip().endswith("exit 0")  # a non-zero setup script stops the session from starting
    assert "set -e" not in block  # one failed step must not end the script early
    if shutil.which("bash"):
        r = subprocess.run(["bash", "-n"], input=block, capture_output=True, text=True, timeout=30)
        assert r.returncode == 0, r.stderr


def test_the_setup_script_block_clones_a_branch_the_doc_names_as_a_start_branch(doc):
    ref = re.search(r"^REF=(\S+)", _setup_script_block(doc), re.M).group(1)
    assert ref in {"chore/cloud-setup", "phase/N37a"}
    assert f"`{ref}`" in doc


def _run_block(tmp_path: Path, clone_works: bool, inside_a_checkout: bool) -> tuple[subprocess.CompletedProcess, Path, Path]:
    """Run the pasted setup script with a stub git and a stub setup.sh; its /opt path is redirected."""
    block = _setup_script_block(_text(DOC))
    block = block.replace("/opt/ds-cloud-src", str(tmp_path / "src"))  # never touch the real path
    ran = tmp_path / "ran"
    clone_dir = tmp_path / "checkout"
    git_log = tmp_path / "git.log"
    if inside_a_checkout:
        _executable(clone_dir / "scripts" / "cloud" / "setup.sh", f'#!/usr/bin/env bash\necho "$@" > "{ran}"\n')
    stub_git = """#!/bin/bash
echo "$@" >> "@LOG@"
case "$1" in
  rev-parse) @TOPLEVEL@ ;;
  clone) @CLONE@ ;;
  -C) if [ "$3" = sparse-checkout ]; then
        mkdir -p "$2/scripts/cloud"
        printf '#!/usr/bin/env bash\\necho "$@" > "@RAN@"\\n' > "$2/scripts/cloud/setup.sh"
        exit 0
      fi
      exit 1 ;;
esac
"""
    stub_git = (
        stub_git.replace("@LOG@", str(git_log))
        .replace("@RAN@", str(ran))
        .replace("@TOPLEVEL@", f'echo "{clone_dir}"' if inside_a_checkout else "exit 128")
        .replace("@CLONE@", 'mkdir -p "${@: -1}"; exit 0' if clone_works else "exit 128")
    )
    stubs = tmp_path / "stubs"
    _executable(stubs / "git", stub_git)
    r = _run(
        ["bash"], env={"PATH": f"{stubs}{os.pathsep}{os.environ.get('PATH', '/usr/bin:/bin')}", "HOME": str(tmp_path)},
        cwd=tmp_path, input=block,
    )
    return r, ran, git_log


@needs_bash
def test_the_setup_script_block_uses_the_session_clone_when_there_is_one(tmp_path):
    r, ran, git_log = _run_block(tmp_path, clone_works=True, inside_a_checkout=True)
    assert r.returncode == 0, r.stderr
    assert ran.read_text(encoding="utf-8").strip() == "--skip git,env"
    assert "clone" not in git_log.read_text(encoding="utf-8")  # nothing was fetched


@needs_bash
def test_the_setup_script_block_fetches_the_scripts_when_there_is_no_clone(tmp_path):
    r, ran, git_log = _run_block(tmp_path, clone_works=True, inside_a_checkout=False)
    assert r.returncode == 0, r.stderr
    assert ran.read_text(encoding="utf-8").strip() == "--skip git,env"
    log = git_log.read_text(encoding="utf-8")
    assert "--branch chore/cloud-setup" in log and "https://github.com/asarrafi47/DealershipScanner.git" in log
    assert "sparse-checkout set scripts" in log


@needs_bash
def test_the_setup_script_block_exits_zero_when_nothing_can_be_fetched(tmp_path):
    r, ran, _ = _run_block(tmp_path, clone_works=False, inside_a_checkout=False)
    assert r.returncode == 0, r.stderr
    assert "the SessionStart hook will do the setup" in r.stdout
    assert not ran.exists()


def test_variables_the_doc_sets_are_read_by_the_code(doc):
    section = _section(doc, "## 2. Create the environment")
    env_blocks = [b for b in _fences(section) if "BASH_DEFAULT_TIMEOUT_MS=" in b]
    assert len(env_blocks) == 1, "expected one environment variables block in section 2"
    names = set(re.findall(r"^([A-Z][A-Z0-9_]+)=", env_blocks[0], re.M))
    assert {"BASH_DEFAULT_TIMEOUT_MS", "BASH_MAX_TIMEOUT_MS"} <= names
    # The two Claude Code knobs are not ours; every other name must be one the code reads, or it is a typo.
    ours = sorted(n for n in names if not n.startswith("BASH_"))
    assert ours
    if shutil.which("git") is None:
        pytest.skip("git not available")
    out = subprocess.run(
        ["git", "grep", "-h", "-o", "-w", "-E", "|".join(ours), "--", "backend", "run.py", "scripts", f":(exclude){THIS_FILE}"],
        cwd=REPO_ROOT, capture_output=True, text=True,
    )
    assert out.returncode in (0, 1), out.stderr
    assert set(out.stdout.split()) == set(ours), f"not found in the code: {sorted(set(ours) - set(out.stdout.split()))}"


def test_the_never_set_list_names_real_variables(doc):
    section = _section(doc, "## 2. Create the environment")
    m = re.search(r"Do not set[^\n]*\n(.*?)(?=\n## |\n### |\Z)", section, re.S | re.I)
    assert m, "section 2 has no 'Do not set' list"
    names = sorted(set(re.findall(r"`([A-Z][A-Z0-9_]{3,})`", m.group(1))))
    assert {"INVENTORY_DATABASE_URL", "DATABASE_URL"} <= set(names)
    if shutil.which("git") is None:
        pytest.skip("git not available")
    out = subprocess.run(
        [
            "git", "grep", "-h", "-o", "-w", "-E", "|".join(names),
            "--", "backend", "scripts", "deploy", ".env.example", "run.py", f":(exclude){THIS_FILE}",
        ],
        cwd=REPO_ROOT, capture_output=True, text=True,
    )
    assert out.returncode in (0, 1), out.stderr
    unknown = sorted(set(names) - set(out.stdout.split()))
    assert not unknown, f"named in the 'Do not set' list but not found anywhere in the repo (typo?): {unknown}"
