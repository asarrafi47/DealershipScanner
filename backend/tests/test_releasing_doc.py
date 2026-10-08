"""docs/RELEASING.md matches the tooling it documents (remediation P2B.7).

The release checklist is only useful while its commands work. These tests
read the doc and the scripts as text (no git, no network, no Railway) and pin:

- every repo path the doc names exists;
- every flag the doc passes to deploy_web.sh / deploy_scanner_nightly.sh is
  one deploy/railway/_guarded_deploy.sh accepts, and the plan lines the doc
  quotes are lines the script prints;
- bump_version.sh, release_guard.py and migrate are called with arguments
  those scripts accept;
- the four CI job ids the doc names (the required checks) are jobs in ci.yml;
- every numbered checklist step carries a "Verify" with a command;
- none of the docs this unit rewrote tells anyone to deploy from main or to
  connect the GitHub repo to Railway;
- every passage that tells the reader to redeploy or to set a Railway variable
  also names the SCAN_FLEET hazard on scanner-nightly (P0A.1), and the
  Railway snapshot in the checklist fails closed;
- the Rollback section says that a Railway rollback restores the target
  deployment's custom variables as well as its image (Railway's
  deployment-actions docs), so SCAN_FLEET=0 cannot make a scanner-nightly
  rollback safe, and no passage in RELEASING.md or RAILWAY_SCANNING.md pairs a
  rollback with SCAN_FLEET without saying so.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
RELEASING = REPO_ROOT / "docs" / "RELEASING.md"
RAILWAY_SCANNING = REPO_ROOT / "docs" / "RAILWAY_SCANNING.md"
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
    RAILWAY_SCANNING,
    REPO_ROOT / "RESUME_HERE.md",
)
# Railway's docs (https://docs.railway.com/guides/deployment-actions, "Rollback"), verbatim.
RAILWAY_ROLLBACK_QUOTE = (
    "Both the Docker image and custom variables are restored during the rollback process."
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
    """Every ``-x`` / ``--x`` token that follows ``command`` on the same line, without
    a shell quote that closes around it (``sh -c '... --dry-run'``)."""
    return [tok.strip("'\"") for args in _args_after(text, command) for tok in args if tok.startswith("-")]


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
    # A subset check: later units (coverage map, pip-audit, drift check) may add
    # jobs. These four are the ones the release checklist and the ruleset require.
    jobs = set(yaml.safe_load(CI_YML.read_text(encoding="utf-8"))["jobs"])
    assert CI_JOBS <= jobs, sorted(CI_JOBS - jobs)
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


def _passages(text: str) -> list[str]:
    """Top-level list items (with their indented paragraphs and code blocks) and
    plain paragraphs, each joined onto one line. Table rows are dropped."""
    passages: list[str] = []
    current: list[str] = []
    in_item = blank = False

    def flush() -> None:
        if current:
            passages.append(" ".join(current))
            current.clear()

    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            blank = True
            if not in_item:
                flush()
            continue
        if stripped.startswith("|"):
            flush()
            in_item = blank = False
            continue
        if re.match(r"(?:[-*]|\d+\.)\s", line):
            flush()
            in_item = True
        elif blank and not line.startswith(" "):
            flush()
            in_item = False
        blank = False
        current.append(stripped)
    flush()
    return passages


def test_redeploy_and_variable_set_advice_names_the_scan_fleet_hazard(doc):
    """A redeploy or a deploying variable change on scanner-nightly starts a fleet
    run while SCAN_FLEET=1 is set (P0A.1). Every passage that gives that advice
    must carry the warning."""
    advice = re.compile(r"railway redeploy(?! --from-source)|railway variable set")
    hits = [p for p in _passages(doc) if advice.search(p)]
    assert hits, "expected the doc to explain how to apply a variable change"
    for passage in hits:
        assert "SCAN_FLEET" in passage, passage[:200]
    source_policy = doc[doc.index("## Railway source policy") : doc.index("## Migrations")]
    assert "SCAN_FLEET=1" in source_policy and "89c13ad52" in source_policy
    assert "railway variable set SCAN_FLEET=0 -s scanner-nightly --skip-deploys" in source_policy


def test_railway_snapshot_fails_closed(doc):
    section = doc[doc.index("## Release checklist") :]
    assert "railway_snapshot() {" in section
    assert 'echo "SNAPSHOT-FAILED $s" >&2; return 1' in section
    assert "assert ids" in section
    assert "railway_snapshot > /tmp/railway_deploys_before.txt && echo snapshot-ok" in section
    # The whole list, sorted: a new id shows in the diff whatever order the CLI uses,
    # and a full page (possibly cut off) fails closed.
    assert "--limit 1000 --json" in section and "sorted(" in section
    assert "assert ids and len(ids) < 1000" in section
    after = section[section.index("railway_snapshot > /tmp/railway_deploys_after.txt") :]
    after = after[: after.index("```")]
    assert "&& echo no-new-deployment" in after
    assert after.count("grep -c .") == 2


def _one_line(text: str) -> str:
    return " ".join(text.split())


def test_release_log_grep_matches_the_table_format(doc):
    assert 'grep -nF "| $V |" docs/RELEASING.md' in doc
    log = doc[doc.index("## Release log") :]
    assert "| version |" in log
    # Version cells are bare (no leading v), which is what the step 14 grep looks for.
    assert "| 1.5.0 |" in log and "| v1.5" not in log


def test_rollback_section_says_variables_are_restored(doc):
    """A Railway rollback runs with the target deployment's variable snapshot, so
    the recipe must say so, re-apply today's web variables, and keep
    scanner-nightly away from a rollback to a SCAN_FLEET=1 snapshot."""
    section = doc[doc.index("## Rollback") : doc.index("## Railway source policy")]
    flat = _one_line(section)
    assert RAILWAY_ROLLBACK_QUOTE in flat
    assert "https://docs.railway.com/guides/deployment-actions" in flat
    assert "retention" in flat

    web = section[section.index("### web") : section.index("### scanner-nightly")]
    assert "--skip-deploys" in web and "railway redeploy -s web -y" in web
    assert "vars-match" in web and "shasum" in web
    # The fingerprint never writes a value: only a hash and the names.
    for line in web.splitlines():
        if "railway variable list -s web --kv" in line and "> /tmp/" in line:
            assert "shasum" in line or "cut -d= -f1" in line, line

    nightly = _one_line(section[section.index("### scanner-nightly") :])
    assert "Do not use a Railway rollback on scanner-nightly" in nightly
    assert "90a3d2a0" in nightly and "SCAN_FLEET=1" in nightly
    assert "ALLOW_UNRELEASED_DEPLOY=1 deploy/railway/deploy_scanner_nightly.sh" in nightly
    assert "railway variable list -s scanner-nightly --kv" in nightly


_ROLLBACK = re.compile(r"\broll(?:s|ed|ing)?[ -]?back\b|\brollback", re.IGNORECASE)


@pytest.mark.parametrize("path", (RELEASING, RAILWAY_SCANNING), ids=lambda p: p.name)
def test_no_passage_treats_scan_fleet_off_as_rollback_protection(path):
    """Setting SCAN_FLEET=0 protects against deploys that use the current
    variables, never against a rollback, which restores the target's snapshot.
    Any passage that pairs a rollback with SCAN_FLEET must say that."""
    hits = [p for p in _passages(path.read_text(encoding="utf-8")) if _ROLLBACK.search(p) and "SCAN_FLEET" in p]
    assert hits, f"expected {path.name} to warn about scanner-nightly rollbacks"
    for passage in hits:
        assert re.search(r"restor|snapshot|revert", passage), passage[:300]
