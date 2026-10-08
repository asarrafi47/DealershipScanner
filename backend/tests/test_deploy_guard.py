"""Guarded Railway deploys: deploy_web.sh and deploy_scanner_nightly.sh (P2B.2, D-REL10).

Every test drives the real scripts, copied into a throwaway git repository
under ``tmp_path`` with a bare "origin" reached over ``file://``. No network,
no real remote, no real Railway:

- ``railway`` on PATH is a fake that records its argv, its working directory
  and the stage it was handed. PATH holds only that fake directory plus
  ``/usr/bin:/bin:/usr/sbin:/sbin`` (git is linked into the fake directory),
  so a real ``railway`` CLI (Homebrew, npm) can never be reached, and HOME is
  a fresh directory, so even a real CLI would find no Railway login;
- git runs with an empty global/system config and a local identity, like
  test_release_tooling.py;
- the scripts run under whatever ``bash`` /usr/bin/env finds first: 3.2 on
  macOS, 5.x on the CI runner. Both must work.

Covered: a tagged main passes ``--dry-run`` and the stage equals the tag's
tracked files (names, bytes, modes) plus the must-ship allowlist plus
BUILD_COMMIT/BUILD_TAG; every refusal (dirty tree, HEAD != main on origin,
missing, lightweight, misplaced or unpushed tag) exits 1 without staging or
calling railway, with and without ``--dry-run``; the ALLOW_UNRELEASED_DEPLOY=1
override; a real (fake-railway) deploy's exact argv; the allowlist rules; the
build files surviving every ignore file.
"""

from __future__ import annotations

import os
import shutil
import stat
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
DEPLOY_DIR = REPO_ROOT / "deploy" / "railway"
LIB = DEPLOY_DIR / "_guarded_deploy.sh"
WEB = "deploy_web.sh"
NIGHTLY = "deploy_scanner_nightly.sh"
SCRIPTS = (WEB, NIGHTLY)
SERVICE = {WEB: "web", NIGHTLY: "scanner-nightly"}
VERSION = "1.6.0"
TAG = f"v{VERSION}"
BUILD_FILES = {"BUILD_COMMIT", "BUILD_TAG"}

FAKE_RAILWAY = """#!/bin/sh
# Fake railway CLI for test_deploy_guard.py: record argv, cwd and the stage.
log="$FAKE_RAILWAY_LOG"
{
  printf 'CALL'
  for a in "$@"; do printf '\\t%s' "$a"; done
  printf '\\n'
  printf 'CWD\\t%s\\n' "$(pwd -P)"
} >> "$log"
if [ "$1" = "up" ] && [ -d "$2" ]; then
  (cd "$2" && find . \\( -type f -o -type l \\) | sed 's|^\\./||' | LC_ALL=C sort) > "$log.files"
  cp "$2/BUILD_COMMIT" "$log.build_commit"
fi
exit 0
"""


# --- helpers ------------------------------------------------------------------


class Repo:
    def __init__(self, path: Path, env: dict):
        self.path = path
        self.env = env

    def run(self, *argv: str, check: bool = True, env: dict | None = None) -> subprocess.CompletedProcess:
        proc = subprocess.run(
            list(argv), cwd=self.path, env=env or self.env, capture_output=True, text=True, timeout=60
        )
        if check and proc.returncode != 0:
            raise AssertionError(
                f"{argv} exited {proc.returncode}\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
            )
        return proc

    def git(self, *args: str, check: bool = True) -> subprocess.CompletedProcess:
        return self.run("git", *args, check=check)

    def sha(self, rev: str = "HEAD") -> str:
        return self.git("rev-parse", rev).stdout.strip()

    def write(self, rel: str, text: str) -> None:
        target = self.path / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8")

    def commit(self, message: str, files: dict) -> str:
        for rel, text in files.items():
            self.write(rel, text)
        self.git("add", "-A")
        self.git("commit", "-q", "-m", message)
        return self.sha()

    def release(self, version: str) -> None:
        """Annotated tag v<version> at HEAD, pushed with main."""
        self.git("tag", "-a", f"v{version}", "-m", f"Release {version}")
        self.git("push", "-q", "origin", "main", f"v{version}")

    def deploy(self, script: str, *args: str, extra_env: dict | None = None) -> subprocess.CompletedProcess:
        env = dict(self.env)
        env.update(extra_env or {})
        return self.run(str(self.path / "deploy" / "railway" / script), *args, check=False, env=env)

    def tracked(self, ref: str) -> dict[str, bytes]:
        names = self.git("ls-tree", "-r", "--name-only", "-z", ref).stdout.split("\0")
        out = {}
        for name in filter(None, names):
            out[name] = subprocess.run(
                ["git", "cat-file", "blob", f"{ref}:{name}"],
                cwd=self.path, env=self.env, capture_output=True, check=True, timeout=60,
            ).stdout
        return out


def stage_files(stage: Path) -> dict[str, bytes]:
    """Every file and symlink under *stage*: relative path -> content (link target for links)."""
    out = {}
    for root, dirs, files in os.walk(stage):
        for name in files + [d for d in dirs if (Path(root) / d).is_symlink()]:
            path = Path(root) / name
            rel = path.relative_to(stage).as_posix()
            out[rel] = os.readlink(path).encode() if path.is_symlink() else path.read_bytes()
    return out


def railway_calls(log: Path) -> list[list[str]]:
    if not log.exists():
        return []
    return [line.split("\t")[1:] for line in log.read_text(encoding="utf-8").splitlines() if line.startswith("CALL")]


def lib_allowlist() -> list[str]:
    """MUST_SHIP_ALLOWLIST as bash sees it (sourcing the lib only defines things)."""
    proc = subprocess.run(
        ["bash", "-c", '. "$1"; printf "%s\\n" ${MUST_SHIP_ALLOWLIST[@]+"${MUST_SHIP_ALLOWLIST[@]}"}', "_", str(LIB)],
        capture_output=True, text=True, timeout=30, check=True,
    )
    return [line for line in proc.stdout.splitlines() if line]


@pytest.fixture
def env(tmp_path: Path) -> dict:
    home = tmp_path / "home"
    home.mkdir()
    fake_bin = tmp_path / "fakebin"
    fake_bin.mkdir()
    railway = fake_bin / "railway"
    railway.write_text(FAKE_RAILWAY, encoding="utf-8")
    railway.chmod(0o755)
    git = shutil.which("git")
    assert git, "git is required"
    (fake_bin / "git").symlink_to(git)

    path = os.pathsep.join([str(fake_bin), "/usr/bin", "/bin", "/usr/sbin", "/sbin"])
    # The only railway reachable from this PATH is the fake.
    assert shutil.which("railway", path=path) == str(railway)

    env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(("GIT_", "RAILWAY_")) and k not in {"ALLOW_UNRELEASED_DEPLOY", "SERVICE"}
    }
    env.update(
        HOME=str(home),
        PATH=path,
        TMPDIR=str(tmp_path / "tmp"),
        GIT_CONFIG_NOSYSTEM="1",
        GIT_CONFIG_GLOBAL=os.devnull,
        GIT_TERMINAL_PROMPT="0",
        FAKE_RAILWAY_LOG=str(tmp_path / "railway.log"),
        LC_ALL="C",
        LANG="C",
    )
    (tmp_path / "tmp").mkdir()
    return env


def _identity(repo: Repo) -> None:
    repo.git("config", "user.name", "Deploy Guard Test")
    repo.git("config", "user.email", "deploy-guard@example.invalid")
    repo.git("config", "commit.gpgsign", "false")
    repo.git("config", "tag.gpgsign", "false")


def make_repo(tmp_path: Path, env: dict, allowlist: tuple[str, ...] = ()) -> Repo:
    """origin.git plus a work clone shaped like the real repo, released as v1.6.0.

    With *allowlist*, the copied lib carries those MUST_SHIP_ALLOWLIST entries.
    """
    origin = tmp_path / "origin.git"
    subprocess.run(["git", "init", "-q", "--bare", "-b", "main", str(origin)], env=env, check=True,
                   capture_output=True, timeout=60)
    work = tmp_path / "work"
    work.mkdir()
    repo = Repo(work, env)
    repo.git("init", "-q", "-b", "main")
    _identity(repo)
    repo.git("remote", "add", "origin", origin.as_uri())

    dest = work / "deploy" / "railway"
    dest.mkdir(parents=True)
    for name in (WEB, NIGHTLY, "railway.scanner-nightly.json"):
        shutil.copy2(DEPLOY_DIR / name, dest / name)
    lib = LIB.read_text(encoding="utf-8")
    if allowlist:
        entries = " ".join(f'"{e}"' for e in allowlist)
        assert lib.count("MUST_SHIP_ALLOWLIST=()") == 1
        lib = lib.replace("MUST_SHIP_ALLOWLIST=()", f"MUST_SHIP_ALLOWLIST=({entries})")
    (dest / LIB.name).write_text(lib, encoding="utf-8")
    shutil.copy2(REPO_ROOT / "railway.toml", work / "railway.toml")

    repo.write("scripts/run.sh", "#!/bin/sh\necho run\n")
    (work / "scripts" / "run.sh").chmod(0o755)
    (work / "link_to_app").symlink_to("app.py")
    repo.commit(
        "initial",
        {
            "VERSION": VERSION + "\n",
            "app.py": "x = 1\n",
            "assets/file with space.txt": "spaces survive\n",
            ".gitignore": ".env\nlocal_only/\n",
        },
    )
    repo.release(VERSION)
    # Never tracked, never shipped.
    repo.write(".env", "SECRET=do-not-ship\n")
    repo.write("local_only/cache.bin", "scratch\n")
    return repo


def expected_stage(repo: Repo, script: str, ref: str, extra: tuple[str, ...] = ()) -> set[str]:
    names = set(repo.tracked(ref)) | set(extra) | BUILD_FILES
    if script == NIGHTLY:
        names = (names - {"railway.toml"}) | {"railway.json"}
    return names


@pytest.fixture
def repo(tmp_path: Path, env: dict) -> Repo:
    return make_repo(tmp_path, env)


def assert_refused(proc: subprocess.CompletedProcess, log: Path, stage: Path | None, *needles: str) -> None:
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert "REFUSED" in proc.stderr
    assert "railway was not called" in proc.stderr
    for needle in needles:
        assert needle in proc.stderr, proc.stderr
    assert railway_calls(log) == []
    if stage is not None:
        assert not stage.exists()


# --- the happy path -----------------------------------------------------------


def test_real_allowlist_is_empty_per_p0a3() -> None:
    """P0A.3 found no non-git file the web image needs; the stage adds nothing."""
    assert lib_allowlist() == []


@pytest.mark.parametrize("script", SCRIPTS)
def test_dry_run_tagged_main_stages_exactly_the_tag(repo: Repo, tmp_path: Path, script: str) -> None:
    stage = tmp_path / "stage"
    proc = repo.deploy(script, "--dry-run", "--keep-stage", str(stage))
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert railway_calls(Path(repo.env["FAKE_RAILWAY_LOG"])) == []
    assert "dry run: railway was not called." in proc.stdout
    assert "DEPLOYING UNRELEASED" not in proc.stderr

    files = stage_files(stage)
    assert set(files) == expected_stage(repo, script, f"refs/tags/{TAG}", tuple(lib_allowlist()))
    tracked = repo.tracked(f"refs/tags/{TAG}")
    for name, blob in tracked.items():
        if script == NIGHTLY and name == "railway.toml":
            continue
        assert files[name] == blob, name
    assert files["BUILD_COMMIT"] == (repo.sha() + "\n").encode()
    assert len(repo.sha()) == 40
    assert files["BUILD_TAG"] == f"{TAG}\n".encode()
    assert os.access(stage / "scripts" / "run.sh", os.X_OK)
    assert (stage / "link_to_app").is_symlink()
    assert ".env" not in files and "local_only/cache.bin" not in files

    if script == NIGHTLY:
        assert files["railway.json"] == tracked["deploy/railway/railway.scanner-nightly.json"]
    else:
        assert files["railway.toml"] == (REPO_ROOT / "railway.toml").read_bytes()

    # The plan: service, commit, release, file count, the exact railway command.
    assert f"service   {SERVICE[script]}" in proc.stdout
    assert f"commit    {repo.sha()}" in proc.stdout
    assert f"release   {TAG}" in proc.stdout
    assert f"({len(files)} files" in proc.stdout
    assert f"railway up {stage.resolve()} --path-as-root --service {SERVICE[script]} --detach" in proc.stdout
    assert "untracked path(s)" not in proc.stdout  # .env and local_only/ are ignored, not untracked


@pytest.mark.parametrize("script", SCRIPTS)
def test_dry_run_without_keep_stage_removes_its_stage(repo: Repo, script: str) -> None:
    tmpdir = Path(repo.env["TMPDIR"])
    proc = repo.deploy(script, "--dry-run")
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert list(tmpdir.iterdir()) == []
    assert railway_calls(Path(repo.env["FAKE_RAILWAY_LOG"])) == []


def test_untracked_file_is_noted_and_never_staged(repo: Repo, tmp_path: Path) -> None:
    repo.write("notes/draft.md", "work in progress\n")
    stage = tmp_path / "stage"
    proc = repo.deploy(WEB, "--dry-run", "--keep-stage", str(stage))
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "1 untracked path(s)" in proc.stdout
    assert "notes/draft.md" not in stage_files(stage)


@pytest.mark.parametrize("script", SCRIPTS)
def test_deploy_calls_railway_once_with_the_stage(repo: Repo, script: str) -> None:
    log = Path(repo.env["FAKE_RAILWAY_LOG"])
    proc = repo.deploy(script)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    calls = railway_calls(log)
    assert len(calls) == 1
    argv = calls[0]
    assert argv[0] == "up" and argv[2:] == ["--path-as-root", "--service", SERVICE[script], "--detach"]
    stage = Path(argv[1])
    cwd = [line.split("\t", 1)[1] for line in log.read_text(encoding="utf-8").splitlines() if line.startswith("CWD")]
    assert [Path(c).resolve() for c in cwd] == [repo.path.resolve()]  # where the Railway link lives
    uploaded = set(Path(f"{log}.files").read_text(encoding="utf-8").splitlines())
    assert uploaded == expected_stage(repo, script, f"refs/tags/{TAG}")
    assert Path(f"{log}.build_commit").read_text(encoding="utf-8") == repo.sha() + "\n"
    assert not stage.exists()  # the temporary stage is cleaned up
    assert f"deployed {repo.sha()} ({TAG}) to {SERVICE[script]}" in proc.stdout


def test_deploy_with_keep_stage_leaves_the_uploaded_stage(repo: Repo, tmp_path: Path) -> None:
    stage = tmp_path / "kept"
    proc = repo.deploy(WEB, "--keep-stage", str(stage))
    assert proc.returncode == 0, proc.stdout + proc.stderr
    calls = railway_calls(Path(repo.env["FAKE_RAILWAY_LOG"]))
    assert [Path(c[1]).resolve() for c in calls] == [stage.resolve()]
    assert (stage / "BUILD_COMMIT").read_text(encoding="utf-8") == repo.sha() + "\n"


# --- refusals -----------------------------------------------------------------


@pytest.mark.parametrize("script", SCRIPTS)
@pytest.mark.parametrize("dirt", ["modified", "staged-new", "deleted"])
def test_refuses_dirty_tree(repo: Repo, tmp_path: Path, script: str, dirt: str) -> None:
    if dirt == "modified":
        repo.write("app.py", "x = 2\n")
    elif dirt == "staged-new":
        repo.write("new_module.py", "y = 1\n")
        repo.git("add", "new_module.py")
    else:
        (repo.path / "app.py").unlink()
    stage = tmp_path / "stage"
    proc = repo.deploy(script, "--dry-run", "--keep-stage", str(stage))
    assert_refused(proc, Path(repo.env["FAKE_RAILWAY_LOG"]), stage, "tracked changes")


@pytest.mark.parametrize("script", SCRIPTS)
def test_refuses_dirty_tree_without_dry_run(repo: Repo, script: str) -> None:
    repo.write("app.py", "x = 2\n")
    proc = repo.deploy(script)
    assert_refused(proc, Path(repo.env["FAKE_RAILWAY_LOG"]), None, "tracked changes")
    assert list(Path(repo.env["TMPDIR"]).iterdir()) == []


@pytest.mark.parametrize("script", SCRIPTS)
def test_refuses_head_ahead_of_origin_main(repo: Repo, tmp_path: Path, script: str) -> None:
    """A tagged, pushed release commit that main on origin does not point at."""
    repo.commit("next", {"VERSION": "1.6.1\n", "app.py": "x = 3\n"})
    repo.git("tag", "-a", "v1.6.1", "-m", "Release 1.6.1")
    repo.git("push", "-q", "origin", "v1.6.1")  # the tag, but not main
    stage = tmp_path / "stage"
    proc = repo.deploy(script, "--dry-run", "--keep-stage", str(stage))
    assert_refused(proc, Path(repo.env["FAKE_RAILWAY_LOG"]), stage, "is not main on origin")
    assert "tag v1.6.1" not in proc.stderr  # the tag itself was fine; only main refused


@pytest.mark.parametrize("script", SCRIPTS)
def test_refuses_when_origin_main_moved_on_even_if_local_ref_is_stale(
    repo: Repo, tmp_path: Path, env: dict, script: str
) -> None:
    """origin/main in the clone still equals HEAD; the live remote does not."""
    other = Repo(tmp_path / "other", env)
    subprocess.run(["git", "clone", "-q", (tmp_path / "origin.git").as_uri(), str(other.path)],
                   env=env, check=True, capture_output=True, timeout=60)
    _identity(other)
    other.commit("someone else's work", {"other.py": "z = 1\n"})
    other.git("push", "-q", "origin", "main")
    assert repo.sha("refs/remotes/origin/main") == repo.sha()  # stale local view
    stage = tmp_path / "stage"
    proc = repo.deploy(script, "--dry-run", "--keep-stage", str(stage))
    assert_refused(proc, Path(repo.env["FAKE_RAILWAY_LOG"]), stage, "is not main on origin", other.sha())


@pytest.mark.parametrize("script", SCRIPTS)
def test_refuses_missing_tag(repo: Repo, tmp_path: Path, script: str) -> None:
    repo.git("push", "-q", "origin", f":refs/tags/{TAG}")
    repo.git("tag", "-d", TAG)
    stage = tmp_path / "stage"
    proc = repo.deploy(script, "--dry-run", "--keep-stage", str(stage))
    assert_refused(proc, Path(repo.env["FAKE_RAILWAY_LOG"]), stage, f"tag {TAG} is missing")


@pytest.mark.parametrize("script", SCRIPTS)
def test_refuses_lightweight_tag(repo: Repo, tmp_path: Path, script: str) -> None:
    repo.git("tag", "-d", TAG)
    repo.git("tag", TAG)
    repo.git("push", "-q", "-f", "origin", TAG)
    stage = tmp_path / "stage"
    proc = repo.deploy(script, "--dry-run", "--keep-stage", str(stage))
    assert_refused(proc, Path(repo.env["FAKE_RAILWAY_LOG"]), stage, "lightweight")


@pytest.mark.parametrize("script", SCRIPTS)
def test_refuses_tag_on_another_commit(repo: Repo, tmp_path: Path, script: str) -> None:
    """VERSION was not bumped: main moved on, but v1.6.0 still names the old commit."""
    repo.commit("unreleased change", {"app.py": "x = 4\n"})
    repo.git("push", "-q", "origin", "main")
    stage = tmp_path / "stage"
    proc = repo.deploy(script, "--dry-run", "--keep-stage", str(stage))
    assert_refused(proc, Path(repo.env["FAKE_RAILWAY_LOG"]), stage, f"tag {TAG} points at", "not HEAD")


@pytest.mark.parametrize("script", SCRIPTS)
def test_refuses_tag_not_pushed(repo: Repo, tmp_path: Path, script: str) -> None:
    repo.git("push", "-q", "origin", f":refs/tags/{TAG}")
    stage = tmp_path / "stage"
    proc = repo.deploy(script, "--dry-run", "--keep-stage", str(stage))
    assert_refused(proc, Path(repo.env["FAKE_RAILWAY_LOG"]), stage, f"tag {TAG} is not on origin")


def test_refuses_when_origin_is_unreachable(repo: Repo, tmp_path: Path) -> None:
    repo.git("remote", "set-url", "origin", (tmp_path / "gone.git").as_uri())
    stage = tmp_path / "stage"
    proc = repo.deploy(WEB, "--dry-run", "--keep-stage", str(stage))
    assert_refused(proc, Path(repo.env["FAKE_RAILWAY_LOG"]), stage, "could not read main from origin")


# --- the ALLOW_UNRELEASED_DEPLOY=1 override -----------------------------------


@pytest.mark.parametrize("script", SCRIPTS)
def test_override_ships_committed_head_with_a_loud_warning(repo: Repo, tmp_path: Path, script: str) -> None:
    hotfix = repo.commit("mid-fleet hotfix", {"app.py": "x = 5\n"})  # no tag, not on origin main
    repo.write("app.py", "x = 6  # uncommitted\n")  # dirty as well
    stage = tmp_path / "stage"
    proc = repo.deploy(script, "--dry-run", "--keep-stage", str(stage),
                       extra_env={"ALLOW_UNRELEASED_DEPLOY": "1"})
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "ALLOW_UNRELEASED_DEPLOY=1: DEPLOYING UNRELEASED CODE" in proc.stderr
    assert "tracked changes" in proc.stderr and "is not main on origin" in proc.stderr
    assert "release   GUARD OVERRIDDEN by ALLOW_UNRELEASED_DEPLOY=1 (BUILD_TAG=unreleased)" in proc.stdout
    files = stage_files(stage)
    assert set(files) == expected_stage(repo, script, hotfix)
    assert files["app.py"] == b"x = 5\n"  # the commit, never the work tree
    assert files["BUILD_COMMIT"] == (hotfix + "\n").encode()
    assert files["BUILD_TAG"] == b"unreleased\n"
    assert railway_calls(Path(repo.env["FAKE_RAILWAY_LOG"])) == []


def test_override_keeps_build_tag_when_head_carries_the_tag(repo: Repo, tmp_path: Path) -> None:
    """Tagged locally, neither main nor the tag pushed: the content is still v1.6.1's."""
    repo.commit("next", {"VERSION": "1.6.1\n"})
    repo.git("tag", "-a", "v1.6.1", "-m", "Release 1.6.1")
    stage = tmp_path / "stage"
    proc = repo.deploy(NIGHTLY, "--dry-run", "--keep-stage", str(stage),
                       extra_env={"ALLOW_UNRELEASED_DEPLOY": "1"})
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert (stage / "BUILD_TAG").read_text(encoding="utf-8") == "v1.6.1\n"


@pytest.mark.parametrize("value", ["true", "yes", "0", " 1"])
def test_override_needs_the_exact_value_1(repo: Repo, tmp_path: Path, value: str) -> None:
    repo.write("app.py", "x = 7\n")
    stage = tmp_path / "stage"
    proc = repo.deploy(WEB, "--dry-run", "--keep-stage", str(stage), extra_env={"ALLOW_UNRELEASED_DEPLOY": value})
    assert_refused(proc, Path(repo.env["FAKE_RAILWAY_LOG"]), stage, "only the exact value 1 overrides")


def test_override_real_deploy_calls_railway(repo: Repo) -> None:
    hotfix = repo.commit("hotfix", {"app.py": "x = 8\n"})
    log = Path(repo.env["FAKE_RAILWAY_LOG"])
    proc = repo.deploy(NIGHTLY, extra_env={"ALLOW_UNRELEASED_DEPLOY": "1"})
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "DEPLOYING UNRELEASED CODE TO 'scanner-nightly'" in proc.stderr
    assert len(railway_calls(log)) == 1
    assert Path(f"{log}.build_commit").read_text(encoding="utf-8") == hotfix + "\n"


# --- usage, allowlist, service and ignore-file rules --------------------------


@pytest.mark.parametrize("script", SCRIPTS)
def test_keep_stage_must_be_new_or_empty(repo: Repo, tmp_path: Path, script: str) -> None:
    stage = tmp_path / "stage"
    stage.mkdir()
    (stage / "leftover").write_text("x", encoding="utf-8")
    proc = repo.deploy(script, "--dry-run", "--keep-stage", str(stage))
    assert proc.returncode == 2
    assert "is not empty" in proc.stderr
    assert railway_calls(Path(repo.env["FAKE_RAILWAY_LOG"])) == []


@pytest.mark.parametrize("args", [["--bogus"], ["--keep-stage"], ["--keep-stage="]])
def test_usage_errors_exit_2(repo: Repo, args: list) -> None:
    proc = repo.deploy(WEB, *args)
    assert proc.returncode == 2
    assert railway_calls(Path(repo.env["FAKE_RAILWAY_LOG"])) == []


def test_help_exits_0_without_railway(repo: Repo) -> None:
    proc = repo.deploy(NIGHTLY, "--help")
    assert proc.returncode == 0
    assert "--dry-run" in proc.stdout and "--keep-stage DIR" in proc.stdout
    assert railway_calls(Path(repo.env["FAKE_RAILWAY_LOG"])) == []


def test_nightly_refuses_service_web(repo: Repo) -> None:
    proc = repo.deploy(NIGHTLY, "--dry-run", extra_env={"SERVICE": "web"})
    assert proc.returncode == 2
    assert "refusing SERVICE=web" in proc.stderr
    assert railway_calls(Path(repo.env["FAKE_RAILWAY_LOG"])) == []


def test_nightly_honours_service_override(repo: Repo) -> None:
    proc = repo.deploy(NIGHTLY, extra_env={"SERVICE": "scanner-nightly-canary"})
    assert proc.returncode == 0, proc.stdout + proc.stderr
    (argv,) = railway_calls(Path(repo.env["FAKE_RAILWAY_LOG"]))
    assert argv[2:] == ["--path-as-root", "--service", "scanner-nightly-canary", "--detach"]


@pytest.mark.parametrize("script", SCRIPTS)
def test_allowlisted_untracked_path_is_staged(tmp_path: Path, env: dict, script: str) -> None:
    repo = make_repo(tmp_path, env, allowlist=("local_only/cache.bin",))
    stage = tmp_path / "stage"
    proc = repo.deploy(script, "--dry-run", "--keep-stage", str(stage))
    assert proc.returncode == 0, proc.stdout + proc.stderr
    files = stage_files(stage)
    assert set(files) == expected_stage(repo, script, f"refs/tags/{TAG}", ("local_only/cache.bin",))
    assert files["local_only/cache.bin"] == b"scratch\n"
    assert "allowlist 1 path(s)" in proc.stdout


@pytest.mark.parametrize(
    ("entry", "message"),
    [("app.py", "is tracked"), ("missing.bin", "is missing from the work tree"), ("../outside", "repo-relative")],
)
def test_bad_allowlist_entry_stops_before_railway(tmp_path: Path, env: dict, entry: str, message: str) -> None:
    repo = make_repo(tmp_path, env, allowlist=(entry,))
    proc = repo.deploy(WEB)
    assert proc.returncode == 2
    assert message in proc.stderr
    assert railway_calls(Path(env["FAKE_RAILWAY_LOG"])) == []


def test_build_files_survive_every_ignore_file(tmp_path: Path, env: dict) -> None:
    """railway up honours .gitignore and .railwayignore in the stage, and Docker
    honours .dockerignore (root-anchored, so a root name matches the same way).

    Each real ignore file is the only exclude source of an empty scratch repo,
    so a developer's own .git/info/exclude cannot mask or fake a match.
    """
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    subprocess.run(["git", "init", "-q", str(scratch)], env=env, check=True, capture_output=True, timeout=30)
    for ignore_file in (".gitignore", ".railwayignore", ".dockerignore"):
        proc = subprocess.run(
            ["git", "-c", f"core.excludesFile={REPO_ROOT / ignore_file}", "check-ignore", "--no-index",
             "-v", "BUILD_COMMIT", "BUILD_TAG", "VERSION", "probe.db"],
            cwd=scratch, env=env, capture_output=True, text=True, timeout=30,
        )
        # probe.db proves the file was read (every ignore file drops *.db).
        assert proc.returncode == 0, proc.stderr
        matched = {line.split("\t")[-1] for line in proc.stdout.splitlines()}
        assert matched == {"probe.db"}, f"{ignore_file} ignores a build file: {proc.stdout}"


def test_no_git_archive_attributes_drop_files() -> None:
    """The stage equals `git ls-files` of the tag only while no export-ignore/subst exists."""
    names = subprocess.run(
        ["git", "ls-files", "-z", "*.gitattributes", ".gitattributes"],
        cwd=REPO_ROOT, capture_output=True, text=True, timeout=30, check=True,
    ).stdout.split("\0")
    for name in filter(None, names):
        text = (REPO_ROOT / name).read_text(encoding="utf-8")
        assert "export-ignore" not in text and "export-subst" not in text, name


def test_lib_refuses_direct_execution() -> None:
    proc = subprocess.run(["bash", str(LIB)], capture_output=True, text=True, timeout=30)
    assert proc.returncode == 2
    assert "is sourced by" in proc.stderr


def test_script_modes() -> None:
    for name in SCRIPTS:
        assert (DEPLOY_DIR / name).stat().st_mode & stat.S_IXUSR, name
    assert not LIB.stat().st_mode & stat.S_IXUSR  # sourced, never run
