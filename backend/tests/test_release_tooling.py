"""Release tooling: bump_version.sh, the pre-push hook and the release guard (P2A.4).

Everything runs against throwaway git repositories under ``tmp_path``: a work
clone holding copies of the real ``scripts/bump_version.sh``,
``scripts/git-hooks/pre-push`` and ``scripts/release_guard.py`` (the layout the
real repo has, so the hook finds the guard the same way), plus a bare
"origin" reached over ``file://``. No network, no real remote.

git runs with an empty global/system config (``GIT_CONFIG_GLOBAL=/dev/null``,
``GIT_CONFIG_NOSYSTEM=1``) and a local ``user.name``/``user.email``: the CI
runner has neither, and a developer's own config (signing, hooksPath) must
not leak in. ``python3`` on PATH is a shim to this interpreter, because the
hook calls whatever ``python3`` is on PATH.

Covered:

- bump_version.sh: patch/minor/major/explicit, the [Unreleased] conversion,
  staging of VERSION and CHANGELOG.md, refusals;
- the hook on feature branches (unchanged by P2A.4): same-VERSION refused,
  bump-only accepted, tags and deletions skipped, a new branch compared with
  origin/main;
- the hook on main: the release guard, based on the sha the remote reported
  (not a stale local origin/main);
- D-REL3 (a) (P2B.1): no branch-name exemption (phase/* and ci/* bump like any
  branch; a shakedown re-uses a bump only under a new branch name), and a
  ``<branch>:main`` push is judged by the release guard, not the branch rule;
- the guard itself: semver rise, CHANGELOG section parsing, containment,
  all-zero base, unresolvable refs, Python 3.9 / stdlib-only source;
- the CI ``release-guard`` job: id, needs, if, fetch, and its base selection
  replayed end to end on simulated PR and push-to-main checkouts.
"""

from __future__ import annotations

import ast
import datetime as dt
import os
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
BUMP = REPO_ROOT / "scripts" / "bump_version.sh"
HOOK = REPO_ROOT / "scripts" / "git-hooks" / "pre-push"
GUARD = REPO_ROOT / "scripts" / "release_guard.py"
CI_YML = REPO_ROOT / ".github" / "workflows" / "ci.yml"

ZERO = "0" * 40
GUARD_IF = "github.ref == 'refs/heads/main' || github.event_name == 'pull_request'"


# --- helpers ------------------------------------------------------------------


def changelog(unreleased: str = "", sections: tuple = ()) -> str:
    """A keep-a-changelog file: title, [Unreleased], then ``(version, body)`` sections."""
    parts = ["# Changelog", "", "## [Unreleased]", ""]
    if unreleased:
        parts += [unreleased, ""]
    for version, body in sections:
        parts += [f"## [{version}] - 2026-10-01", ""]
        if body:
            parts += [body, ""]
    return "\n".join(parts).rstrip("\n") + "\n"


class Repo:
    def __init__(self, path: Path, env: dict):
        self.path = path
        self.env = env

    def run(self, *argv: str, check: bool = True) -> subprocess.CompletedProcess:
        proc = subprocess.run(
            list(argv), cwd=self.path, env=self.env, capture_output=True, text=True, timeout=60
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

    def read(self, rel: str) -> str:
        return (self.path / rel).read_text(encoding="utf-8")

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

    def push(self, *args: str) -> subprocess.CompletedProcess:
        return self.git("push", "origin", *args, check=False)

    def bump(self, kind: str = "patch") -> subprocess.CompletedProcess:
        return self.run(str(self.path / "scripts" / "bump_version.sh"), kind, check=False)

    def guard(self, base: str, head: str = "HEAD") -> subprocess.CompletedProcess:
        return self.run(sys.executable, str(GUARD), "--base", base, "--head", head, check=False)


@pytest.fixture
def git_env(tmp_path: Path) -> dict:
    home = tmp_path / "home"
    home.mkdir()
    shim_dir = tmp_path / "bin"
    shim_dir.mkdir()
    shim = shim_dir / "python3"
    shim.write_text(f'#!/bin/sh\nexec "{sys.executable}" "$@"\n', encoding="utf-8")
    shim.chmod(0o755)
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    env.update(
        HOME=str(home),
        PATH=f"{shim_dir}{os.pathsep}{os.environ.get('PATH', '')}",
        GIT_CONFIG_NOSYSTEM="1",
        GIT_CONFIG_GLOBAL=os.devnull,
        GIT_TERMINAL_PROMPT="0",
        # The guard must not depend on the locale to read a UTF-8 CHANGELOG.md.
        LC_ALL="C",
        LANG="C",
    )
    return env


def _configure_identity(repo: Repo) -> None:
    repo.git("config", "user.name", "Release Tooling Test")
    repo.git("config", "user.email", "release-tooling@example.invalid")
    repo.git("config", "commit.gpgsign", "false")
    repo.git("config", "tag.gpgsign", "false")


def init_repo(path: Path, env: dict, version: str = "1.5.3", notes: str = "- First release.") -> Repo:
    """A repo shaped like the real one: VERSION, CHANGELOG.md and the release scripts."""
    path.mkdir(parents=True)
    repo = Repo(path, env)
    repo.git("init", "-q", "-b", "main")
    _configure_identity(repo)
    (path / "scripts" / "git-hooks").mkdir(parents=True)
    shutil.copy2(BUMP, path / "scripts" / "bump_version.sh")
    shutil.copy2(HOOK, path / "scripts" / "git-hooks" / "pre-push")
    shutil.copy2(GUARD, path / "scripts" / "release_guard.py")
    repo.commit(
        "initial",
        {
            "VERSION": version + "\n",
            "CHANGELOG.md": changelog(sections=((version, notes),)),
            "app.py": "x = 0\n",
        },
    )
    return repo


def init_bare(path: Path, env: dict) -> Path:
    subprocess.run(
        ["git", "init", "-q", "--bare", "-b", "main", str(path)],
        env=env, check=True, capture_output=True,
    )
    # GitHub serves any reachable commit by sha; CI's release-guard job fetches
    # github.event.before that way.
    subprocess.run(
        ["git", "-C", str(path), "config", "uploadpack.allowAnySHA1InWant", "true"],
        env=env, check=True, capture_output=True,
    )
    return path


@pytest.fixture
def pushed(tmp_path: Path, git_env: dict) -> Repo:
    """Work clone at 1.5.3, main pushed to a bare origin, then the hook installed."""
    remote = init_bare(tmp_path / "origin.git", git_env)
    repo = init_repo(tmp_path / "work", git_env)
    repo.git("remote", "add", "origin", remote.as_uri())
    repo.git("push", "-q", "origin", "main")
    repo.git("config", "core.hooksPath", "scripts/git-hooks")
    return repo


def remote_sha(repo: Repo, ref: str) -> str:
    out = repo.git("ls-remote", "origin", ref).stdout.split()
    return out[0] if out else ""


def release_files(version: str, notes: str = "- Fixed a thing.", older=(("1.5.3", "- First release."),)) -> dict:
    return {
        "VERSION": version + "\n",
        "CHANGELOG.md": changelog(sections=((version, notes), *older)),
    }


def _today_candidates() -> set:
    today = dt.date.today()
    return {today.isoformat(), (today - dt.timedelta(days=1)).isoformat()}


# --- bump_version.sh ------------------------------------------------------------


@pytest.mark.parametrize(
    "kind,expected",
    [("patch", "1.5.4"), ("minor", "1.6.0"), ("major", "2.0.0"), ("1.7.0", "1.7.0")],
)
def test_bump_sets_version_converts_unreleased_and_stages_both(tmp_path, git_env, kind, expected):
    repo = init_repo(tmp_path / "r", git_env)
    base = repo.commit(
        "notes",
        {"CHANGELOG.md": changelog("### Fixed\n- A fix.", (("1.5.3", "- First release."),))},
    )

    proc = repo.bump(kind)

    assert proc.returncode == 0, proc.stderr
    assert f"VERSION 1.5.3 -> {expected}" in proc.stdout
    assert repo.read("VERSION") == expected + "\n"
    text = repo.read("CHANGELOG.md")
    heads = [line for line in text.splitlines() if line.startswith("## [")]
    assert heads[0] == "## [Unreleased]"
    assert heads[1].startswith(f"## [{expected}] - ")
    assert heads[1].split(" - ", 1)[1] in _today_candidates()
    assert heads[2].startswith("## [1.5.3]")
    # The Unreleased notes now sit under the new version; a fresh empty Unreleased is on top.
    assert text.startswith(
        f"# Changelog\n\n## [Unreleased]\n\n{heads[1]}\n\n### Fixed\n- A fix.\n\n## [1.5.3]"
    )
    staged = set(repo.git("diff", "--cached", "--name-only").stdout.split())
    assert staged == {"VERSION", "CHANGELOG.md"}
    assert repo.git("diff", "--name-only").stdout.strip() == ""

    # What the script produces from filled notes is releasable.
    repo.git("commit", "-q", "-m", "bump")
    assert repo.guard(base).returncode == 0


def test_bump_refuses_the_same_version_and_a_bad_argument(tmp_path, git_env):
    repo = init_repo(tmp_path / "r", git_env)
    before = repo.read("CHANGELOG.md")

    same = repo.bump("1.5.3")
    assert same.returncode == 1
    assert "VERSION already 1.5.3" in same.stderr

    bad = repo.bump("bogus")
    assert bad.returncode == 2
    assert "usage:" in bad.stderr

    assert repo.read("VERSION") == "1.5.3\n"
    assert repo.read("CHANGELOG.md") == before
    assert repo.git("status", "--porcelain").stdout == ""


def test_bump_of_an_empty_unreleased_is_not_releasable(tmp_path, git_env):
    repo = init_repo(tmp_path / "r", git_env)
    base = repo.sha()
    assert repo.bump("patch").returncode == 0
    repo.git("commit", "-q", "-m", "bump")

    proc = repo.guard(base)
    assert proc.returncode == 1
    assert "empty '## [1.5.4]' section" in proc.stderr


def test_bump_without_an_unreleased_heading_prepends_the_version(tmp_path, git_env):
    repo = init_repo(tmp_path / "r", git_env)
    base = repo.commit(
        "no unreleased", {"CHANGELOG.md": "# Changelog\n\n## [1.5.3] - 2026-10-01\n\n- First.\n"}
    )
    assert repo.bump("patch").returncode == 0
    text = repo.read("CHANGELOG.md")
    first = text.splitlines()[0]
    assert first.startswith("## [1.5.4] - ")
    assert text.split("\n", 1)[1] == "\n# Changelog\n\n## [1.5.3] - 2026-10-01\n\n- First.\n"
    repo.git("commit", "-q", "-m", "bump")
    # The prepended heading is followed by the "# Changelog" title, not notes:
    # the guard reads that as an empty section.
    proc = repo.guard(base)
    assert proc.returncode == 1
    assert "empty '## [1.5.4]' section" in proc.stderr


# --- pre-push hook: feature branches (unchanged by P2A.4) ------------------------


def test_hook_is_executable():
    assert os.access(HOOK, os.X_OK)


def test_hook_refuses_a_same_version_push_and_accepts_it_once_bumped(pushed):
    repo = pushed
    repo.git("checkout", "-q", "-b", "feat")
    assert repo.push("feat").returncode == 0  # same commit as origin/main
    repo.commit("work", {"app.py": "x = 1\n"})

    refused = repo.push("feat")
    assert refused.returncode != 0
    assert "pre-push: VERSION is still 1.5.3" in refused.stderr
    assert remote_sha(repo, "refs/heads/feat") != repo.sha()

    assert repo.bump("patch").returncode == 0
    repo.git("commit", "-q", "-m", "bump")
    accepted = repo.push("feat")
    assert accepted.returncode == 0, accepted.stderr
    assert remote_sha(repo, "refs/heads/feat") == repo.sha()


def test_hook_accepts_a_bump_only_push(pushed):
    repo = pushed
    repo.git("checkout", "-q", "-b", "feat")
    assert repo.push("feat").returncode == 0

    # VERSION and CHANGELOG.md only.
    assert repo.bump("patch").returncode == 0
    repo.git("commit", "-q", "-m", "bump")
    assert repo.push("feat").returncode == 0

    # CHANGELOG.md alone, VERSION unchanged: still nothing but release files.
    repo.commit("notes", {"CHANGELOG.md": repo.read("CHANGELOG.md") + "\n- A late note.\n"})
    proc = repo.push("feat")
    assert proc.returncode == 0, proc.stderr
    assert remote_sha(repo, "refs/heads/feat") == repo.sha()


def test_hook_skips_tags_and_deletions(pushed):
    repo = pushed
    repo.git("checkout", "-q", "-b", "feat")
    assert repo.push("feat").returncode == 0
    repo.commit("unbumped work", {"app.py": "x = 2\n"})
    repo.git("tag", "t1")

    tag = repo.push("refs/tags/t1")
    assert tag.returncode == 0, tag.stderr
    assert remote_sha(repo, "refs/tags/t1") == repo.sha()

    deletion = repo.push("--delete", "feat")
    assert deletion.returncode == 0, deletion.stderr
    assert remote_sha(repo, "refs/heads/feat") == ""

    # Control: the same unbumped commit pushed as a branch is refused.
    assert repo.push("feat").returncode != 0


def test_hook_compares_a_new_branch_with_origin_main(pushed):
    repo = pushed
    main_short = repo.git("rev-parse", "--short", "origin/main").stdout.strip()
    repo.git("checkout", "-q", "-b", "feat")
    repo.commit("work", {"app.py": "x = 3\n"})

    refused = repo.push("feat")
    assert refused.returncode != 0
    assert f"same as {main_short} on origin" in refused.stderr

    # Today's behaviour: with no local origin/main there is nothing to compare with.
    repo.git("update-ref", "-d", "refs/remotes/origin/main")
    accepted = repo.push("feat")
    assert accepted.returncode == 0, accepted.stderr


def test_hook_on_a_feature_branch_only_requires_a_different_version(pushed):
    # The release guard is for main only: a feature branch may even go down,
    # with no CHANGELOG section, exactly as before P2A.4.
    repo = pushed
    repo.git("checkout", "-q", "-b", "feat")
    repo.commit("lower", {"app.py": "x = 4\n", "VERSION": "1.5.2\n"})
    proc = repo.push("feat")
    assert proc.returncode == 0, proc.stderr
    assert "release_guard" not in proc.stderr + proc.stdout


# --- pre-push hook: main gets the release guard -------------------------------------


def test_hook_refuses_a_main_push_with_a_lower_version(pushed):
    repo = pushed
    old = repo.sha()
    repo.commit(
        "downgrade",
        {"app.py": "x = 5\n", **release_files("1.5.2", older=(("1.5.3", "- First release."),))},
    )
    proc = repo.push("main")
    assert proc.returncode != 0
    assert "VERSION 1.5.2" in proc.stderr and "does not rise above 1.5.3" in proc.stderr
    assert "pre-push: refused the push to main" in proc.stderr
    assert remote_sha(repo, "refs/heads/main") == old


def test_hook_refuses_a_main_push_with_the_same_version(pushed):
    repo = pushed
    repo.commit("work", {"app.py": "x = 6\n"})
    proc = repo.push("main")
    assert proc.returncode != 0
    assert "does not rise above 1.5.3" in proc.stderr


def test_hook_main_push_needs_a_filled_changelog_section(pushed):
    repo = pushed
    repo.write("app.py", "x = 7\n")
    assert repo.bump("patch").returncode == 0  # empty [Unreleased] -> empty [1.5.4]
    repo.git("add", "-A")
    repo.git("commit", "-q", "-m", "release without notes")

    empty = repo.push("main")
    assert empty.returncode != 0
    assert "empty '## [1.5.4]' section" in empty.stderr

    text = repo.read("CHANGELOG.md")
    heading = next(line for line in text.splitlines() if line.startswith("## [1.5.4]"))
    repo.commit("notes", {"CHANGELOG.md": text.replace(heading, heading + "\n\n### Fixed\n- x is 7.")})
    accepted = repo.push("main")
    assert accepted.returncode == 0, accepted.stderr
    assert "release_guard: OK" in accepted.stdout + accepted.stderr
    assert remote_sha(repo, "refs/heads/main") == repo.sha()


def test_hook_judges_main_against_the_remote_sha_not_a_stale_origin_main(pushed):
    repo = pushed
    v153 = repo.sha()
    repo.commit("release 1.5.4", {"app.py": "x = 8\n", **release_files("1.5.4")})
    assert repo.push("main").returncode == 0

    # Local origin/main goes stale (1.5.3); the remote really is at 1.5.4.
    repo.git("update-ref", "refs/remotes/origin/main", v153)
    repo.commit("work", {"app.py": "x = 9\n"})  # still 1.5.4
    proc = repo.push("main")
    assert proc.returncode != 0
    assert "does not rise above 1.5.4" in proc.stderr


def test_hook_refuses_a_main_push_over_a_remote_main_it_has_not_fetched(pushed, tmp_path, git_env):
    repo = pushed
    other = Repo(tmp_path / "other", git_env)
    subprocess.run(
        ["git", "clone", "-q", repo.git("remote", "get-url", "origin").stdout.strip(), str(other.path)],
        env=git_env, check=True, capture_output=True,
    )
    _configure_identity(other)
    other.commit("their release", {"app.py": "x = 10\n", **release_files("1.5.4")})
    other.git("push", "-q", "origin", "main")  # no hook installed in this clone

    repo.commit("our release", {"app.py": "x = 11\n", **release_files("1.5.5")})
    proc = repo.push("--force", "main")
    assert proc.returncode != 0
    assert "fetch it first" in proc.stderr
    assert remote_sha(repo, "refs/heads/main") == other.sha()


def test_hook_main_push_that_creates_main_needs_only_the_section(tmp_path, git_env):
    for name, notes, ok in (("filled", "- First release.", True), ("empty", "", False)):
        remote = init_bare(tmp_path / f"{name}.git", git_env)
        repo = init_repo(tmp_path / name, git_env, notes=notes)
        repo.git("remote", "add", "origin", remote.as_uri())
        repo.git("config", "core.hooksPath", "scripts/git-hooks")
        proc = repo.push("main")
        assert (proc.returncode == 0) is ok, (name, proc.stderr)


# --- D-REL3 (a): every push bumps; main also gets the release guard (P2B.1) ---------
#
# Owner decision 2026-10-08: option (a), not (b). The hook keeps "VERSION must
# change" on every branch, with no exemption by branch name, and adds the release
# guard for pushes to main. These pin that for the D-REL2 trunk model: phase/*
# branches, the P2A.5 ci/shakedown-<n> branches, and the `git push origin
# <branch>:main` fast-forward that P2B.3 releases with.


@pytest.mark.parametrize("branch", ["phase/3", "ci/shakedown-1"])
def test_d_rel3_phase_and_ci_branches_need_a_new_version_like_any_branch(pushed, branch):
    repo = pushed
    repo.git("checkout", "-q", "-b", branch)
    repo.commit("work", {"app.py": "x = 20\n"})

    refused = repo.push(branch)
    assert refused.returncode != 0
    assert "pre-push: VERSION is still 1.5.3" in refused.stderr
    assert remote_sha(repo, f"refs/heads/{branch}") == ""

    # A bump with an empty [Unreleased] leaves an empty [1.5.4] section. The
    # release guard would refuse that; on a non-main branch it does not run.
    assert repo.bump("patch").returncode == 0
    repo.git("commit", "-q", "-m", "bump")
    accepted = repo.push(branch)
    assert accepted.returncode == 0, accepted.stderr
    assert "release_guard" not in accepted.stdout + accepted.stderr
    assert remote_sha(repo, f"refs/heads/{branch}") == repo.sha()


def test_d_rel3_ci_shakedown_reuses_the_bump_only_under_a_new_branch_name(pushed):
    # P2A.5: the bumped integration HEAD goes to a NEW ci/shakedown-<n>. A new
    # branch is compared with origin/main, whose VERSION is older, so it needs
    # no extra bump. Re-pushing the same name with more work would need one,
    # which is why every iteration takes a new name.
    repo = pushed
    repo.git("checkout", "-q", "-b", "integration")
    repo.commit("work, bumped", {"app.py": "x = 21\n", "VERSION": "1.5.4\n"})
    first = repo.push("HEAD:refs/heads/ci/shakedown-1")
    assert first.returncode == 0, first.stderr

    repo.commit("ci fix", {"app.py": "x = 22\n"})  # still 1.5.4
    again = repo.push("HEAD:refs/heads/ci/shakedown-1")
    assert again.returncode != 0
    assert "pre-push: VERSION is still 1.5.4" in again.stderr

    fresh = repo.push("HEAD:refs/heads/ci/shakedown-2")
    assert fresh.returncode == 0, fresh.stderr
    assert remote_sha(repo, "refs/heads/ci/shakedown-2") == repo.sha()


def test_d_rel3_a_branch_pushed_to_main_is_judged_by_the_release_guard(pushed):
    # P2B.3 releases with `git push origin <branch>:main`. The hook keys on the
    # remote ref, so the branch's own rule (VERSION differs) is not enough for
    # main: VERSION must rise and CHANGELOG.md needs a filled section.
    repo = pushed
    main_before = remote_sha(repo, "refs/heads/main")
    repo.git("checkout", "-q", "-b", "phase/3")
    repo.commit("work", {"app.py": "x = 23\n"})

    unbumped = repo.push("phase/3:main")
    assert unbumped.returncode != 0
    assert "does not rise above 1.5.3" in unbumped.stderr

    assert repo.bump("patch").returncode == 0  # empty [Unreleased] -> empty [1.5.4]
    repo.git("commit", "-q", "-m", "bump, no notes")
    assert repo.push("phase/3").returncode == 0  # the branch rule is satisfied

    no_notes = repo.push("phase/3:main")
    assert no_notes.returncode != 0
    assert "empty '## [1.5.4]' section" in no_notes.stderr
    assert "pre-push: refused the push to main" in no_notes.stderr
    assert remote_sha(repo, "refs/heads/main") == main_before

    text = repo.read("CHANGELOG.md")
    heading = next(line for line in text.splitlines() if line.startswith("## [1.5.4]"))
    repo.commit("notes", {"CHANGELOG.md": text.replace(heading, heading + "\n\n### Fixed\n- x is 23.")})
    assert repo.push("phase/3").returncode == 0  # CHANGELOG-only: bump-only path

    released = repo.push("phase/3:main")
    assert released.returncode == 0, released.stderr
    assert "release_guard: OK" in released.stdout + released.stderr
    assert remote_sha(repo, "refs/heads/main") == repo.sha() == remote_sha(repo, "refs/heads/phase/3")


# --- release_guard.py -------------------------------------------------------------


@pytest.mark.parametrize(
    "base_v,head_v,ok",
    [
        ("1.5.2", "1.5.3", True),
        ("1.5.2", "1.5.2", False),
        ("1.5.9", "1.5.10", True),  # numeric, not string, comparison
        ("1.5.10", "1.5.9", False),
        ("1.9.9", "2.0.0", True),
        ("1.5.3", "1.4.9", False),
        ("0.2.0", "1.5.3", True),
    ],
)
def test_guard_requires_a_semver_rise(tmp_path, git_env, base_v, head_v, ok):
    repo = init_repo(tmp_path / "r", git_env, version=base_v)
    base = repo.sha()
    repo.commit(
        "release",
        {"app.py": "x = 1\n", **release_files(head_v, older=((base_v, "- Older."),))},
    )
    proc = repo.guard(base)
    assert (proc.returncode == 0) is ok, proc.stderr
    if not ok:
        assert proc.returncode == 1
        assert f"does not rise above {base_v}" in proc.stderr


_SECTION_CASES = {
    "filled": (changelog(sections=(("1.5.4", "- Fixed a thing."), ("1.5.3", "- Older."))), None),
    "filled_last_in_file": (
        "# Changelog\n\n## [1.5.3] - 2026-10-01\n\n- Older.\n\n## [1.5.4] - 2026-10-08\n- Last.\n",
        None,
    ),
    "non_ascii_notes": (
        changelog(sections=(("1.5.4", "- Café prices – fixed."), ("1.5.3", "- Older."))),
        None,
    ),
    "empty": (changelog(sections=(("1.5.4", ""), ("1.5.3", "- Older."))), "empty"),
    "only_a_subheading": (
        changelog(sections=(("1.5.4", "### Added"), ("1.5.3", "- Older."))),
        "empty",
    ),
    "ended_by_the_title": (
        "## [1.5.4] - 2026-10-08\n\n# Changelog\n\n## [1.5.3] - 2026-10-01\n\n- Older.\n",
        "empty",
    ),
    "missing": (changelog(sections=(("1.5.3", "- Older."),)), "no '## [1.5.4]' section"),
    "longer_version_only": (
        changelog(sections=(("1.5.40", "- Other."), ("1.5.3", "- Older."))),
        "no '## [1.5.4]' section",
    ),
}


@pytest.mark.parametrize("case", sorted(_SECTION_CASES))
def test_guard_requires_a_filled_changelog_section(tmp_path, git_env, case):
    text, failure = _SECTION_CASES[case]
    repo = init_repo(tmp_path / "r", git_env)
    base = repo.sha()
    repo.commit("release", {"app.py": "x = 1\n", "VERSION": "1.5.4\n", "CHANGELOG.md": text})
    proc = repo.guard(base)
    if failure is None:
        assert proc.returncode == 0, proc.stderr
        assert "release_guard: OK: VERSION 1.5.3 -> 1.5.4" in proc.stdout
    else:
        assert proc.returncode == 1, proc.stdout
        assert failure in proc.stderr


def test_guard_passes_a_head_already_contained_in_the_base(tmp_path, git_env):
    repo = init_repo(tmp_path / "r", git_env)
    older = repo.sha()
    newer = repo.commit("release", {"app.py": "x = 1\n", **release_files("1.5.4")})
    for head in (older, newer):
        proc = repo.guard(newer, head)
        assert proc.returncode == 0, proc.stderr
        assert "already contained" in proc.stdout


@pytest.mark.parametrize("version", ["1.5", "v1.5.4", "1.5.4-rc1", ""])
def test_guard_refuses_a_malformed_version(tmp_path, git_env, version):
    repo = init_repo(tmp_path / "r", git_env)
    base = repo.sha()
    repo.commit("bad", {"app.py": "x = 1\n", **release_files(version or "1.5.4"), "VERSION": version + "\n"})
    proc = repo.guard(base)
    assert proc.returncode == 1
    assert "is not MAJOR.MINOR.PATCH" in proc.stderr


def test_guard_refuses_a_missing_version_file(tmp_path, git_env):
    repo = init_repo(tmp_path / "r", git_env)
    base = repo.sha()
    repo.git("rm", "-q", "VERSION")
    repo.git("commit", "-q", "-m", "drop VERSION")
    proc = repo.guard(base)
    assert proc.returncode == 1
    assert "VERSION file missing" in proc.stderr


def test_guard_with_an_all_zero_base_checks_only_the_section(tmp_path, git_env):
    filled = init_repo(tmp_path / "filled", git_env, version="0.1.0")
    assert filled.guard(ZERO).returncode == 0
    empty = init_repo(tmp_path / "empty", git_env, version="0.1.0", notes="")
    proc = empty.guard(ZERO)
    assert proc.returncode == 1
    assert "empty '## [0.1.0]' section" in proc.stderr


def test_guard_exits_2_when_a_ref_does_not_resolve(tmp_path, git_env):
    repo = init_repo(tmp_path / "r", git_env)
    proc = repo.guard("1" * 40)
    assert proc.returncode == 2
    assert "fetch it first" in proc.stderr
    assert repo.guard("HEAD", head="no-such-branch").returncode == 2
    usage = repo.run(sys.executable, str(GUARD), check=False)  # --base is required
    assert usage.returncode == 2


def test_guard_judges_the_commit_not_the_working_tree(tmp_path, git_env):
    repo = init_repo(tmp_path / "r", git_env)
    base = repo.sha()
    repo.commit("work", {"app.py": "x = 1\n"})  # committed: still 1.5.3
    for rel, text in release_files("1.5.4").items():
        repo.write(rel, text)  # bumped only in the working tree
    proc = repo.guard(base)
    assert proc.returncode == 1
    assert "does not rise above 1.5.3" in proc.stderr


def test_guard_is_stdlib_only_and_python39_compatible():
    # Hooks run whatever python3 is on PATH; macOS's /usr/bin/python3 is 3.9.
    source = GUARD.read_text(encoding="utf-8")
    tree = ast.parse(source, feature_version=(3, 9))
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported |= {alias.name.split(".")[0] for alias in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module != "__future__":
            imported.add((node.module or "").split(".")[0])
    assert imported, "no imports parsed"
    assert imported <= set(sys.stdlib_module_names), imported - set(sys.stdlib_module_names)
    # Annotations are never evaluated at runtime, so 3.10+ typing syntax cannot slip in.
    assert "from __future__ import annotations" in source


# --- CI release-guard job -----------------------------------------------------------


@pytest.fixture(scope="module")
def ci_jobs() -> dict:
    return yaml.safe_load(CI_YML.read_text(encoding="utf-8"))["jobs"]


@pytest.fixture(scope="module")
def guard_job(ci_jobs) -> dict:
    return ci_jobs["release-guard"]


def _run_steps(job: dict) -> list:
    return [s for s in job.get("steps") or [] if "run" in s]


def test_release_guard_job_is_named_exactly_release_guard(ci_jobs):
    assert "release-guard" in ci_jobs
    # No display name: the check shows (and is required) as "release-guard".
    assert ci_jobs["release-guard"].get("name", "release-guard") == "release-guard"


def test_release_guard_needs_lint_and_judges_only_main(guard_job):
    needs = guard_job["needs"]
    assert (needs if isinstance(needs, list) else [needs]) == ["lint"]
    # D-REL3: pushes to main and PRs to main only; branch pushes are not judged.
    assert guard_job["if"] == GUARD_IF
    assert guard_job["timeout-minutes"] == 5
    assert "permissions" not in guard_job  # inherits contents: read
    assert "continue-on-error" not in guard_job


def test_release_guard_fetches_main_then_runs_the_script(guard_job):
    steps = guard_job["steps"]
    assert (steps[0].get("uses") or "").split("@")[0] == "actions/checkout"
    runs = _run_steps(guard_job)
    assert len(runs) == 2, runs
    fetch, check = runs
    assert shlex.split(fetch["run"]) == [
        "git", "fetch", "--no-tags", "--depth=50", "origin",
        "+refs/heads/main:refs/remotes/origin/main",
    ]
    assert "python3 scripts/release_guard.py --base" in check["run"]
    assert check["env"] == {
        "EVENT_NAME": "${{ github.event_name }}",
        "BEFORE_SHA": "${{ github.event.before }}",
    }


def _replay(job: dict, workdir: Path, env: dict, event: str, before: str = "") -> subprocess.CompletedProcess:
    """Run the job's ``run`` steps the way the runner does (bash -eo pipefail)."""
    bash = shutil.which("bash")
    shell = [bash, "--noprofile", "--norc", "-eo", "pipefail", "-c"] if bash else ["sh", "-ec"]
    step_env = {**env, "EVENT_NAME": event, "BEFORE_SHA": before}
    proc = None
    for step in _run_steps(job):
        proc = subprocess.run(
            [*shell, step["run"]],
            cwd=workdir, env=step_env, capture_output=True, text=True, timeout=60,
        )
        if proc.returncode != 0:
            return proc
    assert proc is not None, "release-guard has no run steps"
    return proc


def _ci_checkout(tmp_path: Path, env: dict, remote: Path, ref: str, name: str) -> Repo:
    """What actions/checkout@v4 leaves behind: a depth-1 fetch of one ref, detached."""
    ci = Repo(tmp_path / name, env)
    ci.path.mkdir()
    ci.git("init", "-q")
    ci.git("remote", "add", "origin", remote.as_uri())
    ci.git("fetch", "-q", "--no-tags", "--depth=1", "origin", f"+{ref}:refs/remotes/ci/head")
    ci.git("checkout", "-q", "--detach", "refs/remotes/ci/head")
    return ci


@pytest.mark.parametrize("bumped", [True, False])
def test_release_guard_job_on_a_pull_request(tmp_path, git_env, guard_job, bumped):
    remote = init_bare(tmp_path / "origin.git", git_env)
    work = init_repo(tmp_path / "work", git_env)
    work.git("remote", "add", "origin", remote.as_uri())
    work.git("push", "-q", "origin", "main")
    work.git("checkout", "-q", "-b", "pr")
    files = {"app.py": "x = 1\n", **(release_files("1.5.4") if bumped else {})}
    work.commit("pr work", files)
    # GitHub runs PR workflows on refs/pull/<n>/merge: the PR merged into main.
    work.git("checkout", "-q", "--detach", "main")
    work.git("merge", "-q", "--no-ff", "-m", "merge pr", "pr")
    work.git("push", "-q", "origin", "HEAD:refs/pull/1/merge")

    ci = _ci_checkout(tmp_path, git_env, remote, "refs/pull/1/merge", "ci")
    proc = _replay(guard_job, ci.path, git_env, "pull_request")
    assert (proc.returncode == 0) is bumped, proc.stdout + proc.stderr


@pytest.mark.parametrize("bumped", [True, False])
def test_release_guard_job_on_a_push_to_main(tmp_path, git_env, guard_job, bumped):
    remote = init_bare(tmp_path / "origin.git", git_env)
    work = init_repo(tmp_path / "work", git_env)
    work.git("remote", "add", "origin", remote.as_uri())
    work.git("push", "-q", "origin", "main")
    before = work.sha()
    work.commit("one", {"app.py": "x = 1\n"})
    work.commit("two", {"app.py": "x = 2\n", **(release_files("1.5.4") if bumped else {})})
    work.git("push", "-q", "origin", "main")

    ci = _ci_checkout(tmp_path, git_env, remote, "refs/heads/main", "ci")
    proc = _replay(guard_job, ci.path, git_env, "push", before)
    assert (proc.returncode == 0) is bumped, proc.stdout + proc.stderr
    if not bumped:
        assert "does not rise above 1.5.3" in proc.stderr
        # Why the job compares with github.event.before: after the push,
        # origin/main IS HEAD, so it would wave every main push through.
        trivial = ci.guard("origin/main")
        assert trivial.returncode == 0 and "already contained" in trivial.stdout


def test_release_guard_job_on_dispatch_checks_only_the_section(tmp_path, git_env, guard_job):
    remote = init_bare(tmp_path / "origin.git", git_env)
    work = init_repo(tmp_path / "work", git_env)
    work.git("remote", "add", "origin", remote.as_uri())
    work.git("push", "-q", "origin", "main")
    ci = _ci_checkout(tmp_path, git_env, remote, "refs/heads/main", "ci")
    proc = _replay(guard_job, ci.path, git_env, "workflow_dispatch")
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "no earlier main to compare with" in proc.stdout
