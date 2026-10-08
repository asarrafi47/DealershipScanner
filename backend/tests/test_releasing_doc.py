"""docs/RELEASING.md matches the tooling it documents (remediation P2B.7).

The release checklist is only useful while its commands work. These tests
read the doc and the scripts as text (no git, no network, no Railway) and pin:

- every repo path the doc names exists;
- every flag the doc passes to deploy_web.sh / deploy_scanner_nightly.sh is
  one deploy/railway/_guarded_deploy.sh accepts, and the plan lines the doc
  quotes are lines the script prints;
- bump_version.sh, release_guard.py and migrate are called with arguments
  those scripts accept;
- the four CI job ids the doc names are exactly the jobs in ci.yml;
- every numbered checklist step carries a "Verify" with a command;
- none of the docs this unit rewrote tells anyone to deploy from main or to
  connect the GitHub repo to Railway.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
RELEASING = REPO_ROOT / "docs" / "RELEASING.md"
GUARD_LIB = REPO_ROOT / "deploy" / "railway" / "_guarded_deploy.sh"
DEPLOY_SCRIPTS = ("deploy_web.sh", "deploy_scanner_nightly.sh")
BUMP = REPO_ROOT / "scripts" / "bump_version.sh"
RELEASE_GUARD = REPO_ROOT / "scripts" / "release_guard.py"
MIGRATE = REPO_ROOT / "backend" / "scripts" / "migrate.py"
CI_YML = REPO_ROOT / ".github" / "workflows" / "ci.yml"
CI_JOBS = {"lint", "pytest", "pytest-integration", "release-guard"}
# Docs rewritten by P2B.7 to drop "deploy from main" (plan Accept: grep -rn "deploy from .main").
REWRITTEN_DOCS = (
    RELEASING,
    REPO_ROOT / "deploy" / "railway" / "README.md",
    REPO_ROOT / "docs" / "RAILWAY_SCANNING.md",
    REPO_ROOT / "RESUME_HERE.md",
)

_REPO_PATH = re.compile(r"(?<![\w./-])((?:deploy|scripts|backend|docs|migrations|\.github)/[\w.*/-]+)")


@pytest.fixture(scope="module")
def doc() -> str:
    return RELEASING.read_text(encoding="utf-8")


def _args_after(text: str, command: str) -> list[list[str]]:
    """The tokens that follow each use of ``command`` on its line (up to a comment,
    backtick or shell operator)."""
    uses = []
    for line in text.splitlines():
        for match in re.finditer(re.escape(command) + r"((?:[ \t]+[^\s#`|;&]+)*)", line):
            uses.append(match.group(1).split())
    return uses


def _flags_after(text: str, command: str) -> list[str]:
    """Every ``-x`` / ``--x`` token that follows ``command`` on the same line."""
    return [tok for args in _args_after(text, command) for tok in args if tok.startswith("-")]


def _guard_options() -> set[str]:
    """Option names in guarded_deploy()'s ``case "$1" in`` block."""
    lib = GUARD_LIB.read_text(encoding="utf-8")
    block = lib[lib.index('case "$1" in') : lib.index("esac", lib.index('case "$1" in'))]
    options = set()
    for arm in re.findall(r"^\s+(-[^)\n]*)\)", block, flags=re.MULTILINE):
        options.update(part.strip() for part in arm.split("|"))
    options.discard("--keep-stage=*")
    return options


def test_every_repo_path_in_the_doc_exists(doc):
    paths = {p.rstrip(".,:;)") for p in _REPO_PATH.findall(doc)}
    assert paths, "expected the doc to name repo paths"
    missing = [
        rel
        for rel in sorted(paths)
        if not (list(REPO_ROOT.glob(rel)) if "*" in rel else (REPO_ROOT / rel).exists())
    ]
    for root_file in ("railway.toml", "Dockerfile.web", "Dockerfile.scanner", "VERSION", "CHANGELOG.md"):
        assert root_file in doc
        assert (REPO_ROOT / root_file).is_file(), root_file
    assert not missing, f"RELEASING.md names paths that do not exist: {missing}"


def test_deploy_flags_are_ones_the_guard_accepts(doc):
    accepted = _guard_options()
    assert {"--dry-run", "--keep-stage", "-h", "--help"} <= accepted
    for name in DEPLOY_SCRIPTS:
        script = REPO_ROOT / "deploy" / "railway" / name
        assert script.is_file()
        assert "_guarded_deploy.sh" in script.read_text(encoding="utf-8")
        used = _flags_after(doc, name)
        assert "--dry-run" in used, name
        assert set(used) <= accepted, (name, sorted(set(used) - accepted))
        # --keep-stage always gets a directory argument.
        for args in _args_after(doc, name):
            for i, tok in enumerate(args):
                if tok == "--keep-stage":
                    assert i + 1 < len(args) and not args[i + 1].startswith("-"), args


def test_quoted_guard_output_and_override_match_the_script(doc):
    lib = GUARD_LIB.read_text(encoding="utf-8")
    for printed in (
        "(annotated, on origin; HEAD == origin main)",
        "dry run: railway was not called.",
        "GUARD OVERRIDDEN",
        "--path-as-root --service",
    ):
        assert printed in doc, printed
        assert printed in lib, printed
    assert 'railway up "$stage" --path-as-root --service "$service" --detach' in lib
    assert "railway up <stage> --path-as-root --service web --detach" in doc
    assert '"${ALLOW_UNRELEASED_DEPLOY:-}" = "1"' in lib
    assert "ALLOW_UNRELEASED_DEPLOY=1 deploy/railway/deploy_scanner_nightly.sh" in doc
    nightly = (REPO_ROOT / "deploy" / "railway" / "deploy_scanner_nightly.sh").read_text(encoding="utf-8")
    assert 'if [ "$SERVICE" = "web" ]; then' in nightly
    assert "railway.scanner-nightly.json" in nightly and "deploy/railway/railway.scanner-nightly.json" in doc


def test_bump_guard_and_migrate_arguments_exist(doc):
    bump = BUMP.read_text(encoding="utf-8")
    levels = set(re.findall(r"^\s+(major|minor|patch)\)", bump, flags=re.MULTILINE))
    assert levels == {"major", "minor", "patch"}
    used_levels = re.findall(r"scripts/bump_version\.sh (\w+)", doc)
    assert used_levels and set(used_levels) <= levels, used_levels

    guard_flags = set(re.findall(r'add_argument\(\s*"(--[\w-]+)"', RELEASE_GUARD.read_text(encoding="utf-8")))
    used_guard = _flags_after(doc, "scripts/release_guard.py")
    assert used_guard and set(used_guard) <= guard_flags, (used_guard, guard_flags)

    migrate_flags = set(re.findall(r'add_argument\(\s*"(--[\w-]+)"', MIGRATE.read_text(encoding="utf-8")))
    used_migrate = _flags_after(doc, "python -m backend.scripts.migrate")
    assert used_migrate and set(used_migrate) <= migrate_flags, (used_migrate, migrate_flags)
    # The doc quotes the runner's "up to date" log line.
    assert "database is up to date" in MIGRATE.read_text(encoding="utf-8")
    assert "database is up to date" in doc


def test_ci_job_ids_in_the_doc_are_the_ci_jobs(doc):
    jobs = set(yaml.safe_load(CI_YML.read_text(encoding="utf-8"))["jobs"])
    assert jobs == CI_JOBS
    for job in CI_JOBS:
        assert f"`{job}`" in doc, job


def test_hook_install_command_matches_the_hook(doc):
    hook = (REPO_ROOT / "scripts" / "git-hooks" / "pre-push").read_text(encoding="utf-8")
    assert "git config core.hooksPath scripts/git-hooks" in hook
    assert "git config core.hooksPath scripts/git-hooks" in doc
    assert "scripts/release_guard.py" in hook and 'remote_ref" = "refs/heads/main"' in hook


def test_every_checklist_step_has_a_verify_command(doc):
    section = doc[doc.index("## Release checklist") :]
    section = section[: section.index("\n## ", 1)]
    steps = re.split(r"^\d+\. \*\*", section, flags=re.MULTILINE)[1:]
    assert len(steps) >= 10
    for number, step in enumerate(steps, start=1):
        assert "Verify" in step, f"step {number} has no Verify"
        verify = step[step.index("Verify") :]
        assert "`" in verify, f"step {number}: Verify names no command"


@pytest.mark.parametrize("path", REWRITTEN_DOCS, ids=lambda p: p.name)
def test_no_rewritten_doc_says_deploy_from_main(path):
    text = path.read_text(encoding="utf-8")
    assert not re.search(r"deploy from .main", text), path
    assert not re.search(r"connect the GitHub repo", text, flags=re.IGNORECASE), path
