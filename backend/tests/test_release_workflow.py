"""The release workflow pushes main with the deploy key, never the GITHUB_TOKEN.

The ``protect-main`` ruleset (deletion, non_fast_forward, update on
``refs/heads/main``) has one bypass actor, a DeployKey. A push from the
default ``GITHUB_TOKEN`` (github-actions[bot]) is refused, and a personal
repository cannot add the Actions app to the bypass list. So the ``release``
job of ``.github/workflows/release.yml`` checks out with
``ssh-key: ${{ secrets.RELEASE_DEPLOY_KEY }}`` and pushes over SSH.

This module pins that, offline, so a later edit cannot quietly send the main
push back through the token:

- only the ``release`` job (push) and the ``check`` job (read-only ``ssh -T``)
  see ``RELEASE_DEPLOY_KEY``; no workflow-level env carries it;
- the release job checks the secret first, checks out over SSH, needs no
  token write permission, and keeps the guard, the ancestry check, the
  existing-tag check, the annotated tag and ``main-released``;
- no push uses a token URL or forces;
- the check step pins GitHub's published ed25519 host key (fingerprint
  verified here), uses StrictHostKeyChecking=yes, warns when unset, removes
  the key file, and never pushes;
- the workflow uses only ``actions/*`` actions.
"""

from __future__ import annotations

import base64
import hashlib
import re
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
RELEASE_YML = REPO_ROOT / ".github" / "workflows" / "release.yml"

SECRET = "RELEASE_DEPLOY_KEY"
SECRET_EXPR = "${{ secrets.RELEASE_DEPLOY_KEY }}"
# https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/githubs-ssh-key-fingerprints
GITHUB_ED25519_FINGERPRINT = "+DiY3wvvV6TuJJhbpZisF/zLDA0zPMSvHdkr4UvCOqU"
TOKEN_MARKERS = ("GITHUB_TOKEN", "github.token", "secrets.GITHUB_TOKEN", "x-access-token", "https://github.com")


@pytest.fixture(scope="module")
def raw() -> str:
    return RELEASE_YML.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def wf(raw) -> dict:
    return yaml.safe_load(raw)


@pytest.fixture(scope="module")
def jobs(wf) -> dict:
    return wf["jobs"]


def _dump(obj) -> str:
    return yaml.safe_dump(obj, sort_keys=False)


def _checkout(job: dict) -> dict:
    steps = [s for s in job["steps"] if str(s.get("uses", "")).startswith("actions/checkout@")]
    assert len(steps) == 1, "expected exactly one checkout step"
    return steps[0]


def _release_script(job: dict) -> str:
    runs = [s["run"] for s in job["steps"] if "git push" in s.get("run", "")]
    assert len(runs) == 1, "expected exactly one step that pushes"
    return runs[0]


def _key_step(job: dict) -> dict:
    steps = [s for s in job["steps"] if SECRET in _dump(s)]
    assert len(steps) == 1, "check: exactly one step may see the deploy key"
    return steps[0]


def test_only_release_and_check_reference_the_deploy_key(wf, jobs):
    users = {name for name, job in jobs.items() if SECRET in _dump(job)}
    assert users == {"release", "check"}
    for key in ("env", "defaults", "on", True):
        assert SECRET not in _dump(wf.get(key, {})), f"workflow-level {key!r} must not carry the key"


def test_release_job_checks_out_over_ssh_with_the_deploy_key(jobs):
    co = _checkout(jobs["release"])
    assert co["uses"] == "actions/checkout@v4"
    w = co["with"]
    assert w["ssh-key"] == SECRET_EXPR
    assert w["ref"] == "${{ inputs.ref }}"
    assert w["fetch-depth"] == 0
    # The release step pushes through the key checkout persisted for this job.
    assert w.get("persist-credentials", True) is True
    assert "token" not in w


def test_release_job_fails_early_when_the_secret_is_empty(jobs):
    steps = jobs["release"]["steps"]
    first = steps[0]
    assert first["env"][SECRET] == SECRET_EXPR
    assert f'test -n "${SECRET}"' in first["run"]
    assert "exit 1" in first["run"]
    assert steps.index(_checkout(jobs["release"])) > 0


def test_release_job_token_has_no_write_permission(jobs):
    perms = jobs["release"].get("permissions", {"contents": "read"})
    assert "write" not in _dump(perms)
    assert "write" not in _dump(jobs["check"].get("permissions", {}))


def test_the_secret_reaches_release_steps_only_through_checkout_and_the_check(jobs):
    for step in jobs["release"]["steps"][1:]:
        if step is _checkout(jobs["release"]):
            continue
        assert SECRET not in _dump(step), f"step {step.get('name')!r} must not see the key"


def test_release_push_goes_to_origin_without_a_token_or_force(jobs, raw):
    script = _release_script(jobs["release"])
    pushes = [ln.strip() for ln in script.splitlines() if "git push" in ln]
    assert pushes == ['git push origin HEAD:refs/heads/main', 'git push origin "v$V"']
    for marker in TOKEN_MARKERS:
        assert marker not in _dump(jobs["release"]), marker
    assert "remote set-url" not in script and "insteadOf" not in script
    for ln in raw.splitlines():
        if "git push" in ln:
            assert not re.search(r"(\s-f\b|--force|\s\+\S)", ln), ln


def test_release_script_keeps_guard_ancestry_tag_and_final_check(jobs):
    script = _release_script(jobs["release"])
    for needle in (
        "python3 scripts/release_guard.py --base origin/main",
        "git merge-base --is-ancestor origin/main HEAD",
        'git ls-remote --exit-code --tags origin "refs/tags/v$V"',
        'git config user.name "github-actions[bot]"',
        'git tag -a "v$V" -m "Release $V"',
        "echo main-released",
    ):
        assert needle in script, needle
    order = [script.index(n) for n in (
        "release_guard.py", "merge-base --is-ancestor", "ls-remote --exit-code",
        "git push origin HEAD:refs/heads/main", "git tag -a", 'git push origin "v$V"', "main-released",
    )]
    assert order == sorted(order)


def test_check_step_is_read_only_and_pins_githubs_host_key(jobs):
    step = _key_step(jobs["check"])
    run = step["run"]
    assert step["env"][SECRET] == SECRET_EXPR
    assert "git push" not in run and "ssh-keyscan" not in run
    assert "StrictHostKeyChecking=yes" in run
    assert "ssh -T" in run and "git@github.com" in run
    assert "::warning::" in run and "exit 0" in run
    assert "chmod 600" in run and "$RUNNER_TEMP" in run
    assert re.search(r"trap 'rm -f [^']*\$key", run)
    assert "deploy key authenticates as" in run
    assert "set -x" not in run and 'echo "$RELEASE_DEPLOY_KEY' not in run

    m = re.search(r"github\.com ssh-ed25519 (AAAA[0-9A-Za-z+/=]+)", run)
    assert m, "pinned github.com ed25519 host key missing"
    fp = base64.b64encode(hashlib.sha256(base64.b64decode(m.group(1))).digest()).decode().rstrip("=")
    assert fp == GITHUB_ED25519_FINGERPRINT


def test_workflow_uses_only_first_party_actions(raw):
    uses = re.findall(r"uses:\s*(\S+)", raw)
    assert uses and all(u.startswith("actions/") for u in uses), uses
