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
- the workflow uses only ``actions/*`` actions, each pinned by a full commit
  SHA with its version in a comment (ci.yml too);
- production secrets are set at step level only (no workflow- or job-level
  env), and every job that reads one runs in the ``production`` environment;
- ``BACKUP_PASSPHRASE`` is gone; the dump is encrypted to the age public key in
  ``vars.RELEASE_BACKUP_PUBKEY``, the job fails first when it is unset, and the
  only artifact uploaded is the ``.age`` ciphertext, never a ``*.dump``;
- the Railway CLI is installed at an exact version with ``--ignore-scripts`` in
  steps that hold no secret, and its binary is checked against a pinned SHA-256
  that matches ``deploy/railway/cli/package.json``;
- no step prints Railway status or logs, migrate hides its target, and
  dispatch inputs reach shell scripts only through env;
- a Dependabot config covers pip, npm and github-actions.
"""

from __future__ import annotations

import base64
import hashlib
import json
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
    assert re.fullmatch(r"actions/checkout@[0-9a-f]{40}", co["uses"])
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


# --- PR B: release workflow hardening ------------------------------------------

WORKFLOWS = REPO_ROOT / ".github" / "workflows"
PROD_SECRETS = ("RAILWAY_TOKEN", "RELEASE_DEPLOY_KEY", "BACKUP_PASSPHRASE")
SECRET_REF = re.compile(r"\$\{\{\s*secrets\.([A-Za-z0-9_]+)\s*\}\}")


def _steps(jobs):
    for jname, job in jobs.items():
        for step in job.get("steps", []):
            yield jname, step


def test_no_secret_at_workflow_or_job_level(wf, jobs):
    for key in ("env", "defaults"):
        assert not SECRET_REF.search(_dump(wf.get(key, {}))), f"workflow-level {key} carries a secret"
    for name, job in jobs.items():
        for key in ("env", "defaults", "container", "services"):
            assert not SECRET_REF.search(_dump(job.get(key, {}))), f"{name}.{key} carries a secret"


def test_secrets_appear_only_in_step_env_or_the_release_checkout(jobs):
    release_co = _checkout(jobs["release"])
    for jname, step in _steps(jobs):
        body = {k: v for k, v in step.items() if k != "env"}
        if step is release_co:
            assert SECRET_REF.findall(_dump(body)) == [SECRET]
            continue
        assert not SECRET_REF.search(_dump(body)), f"{jname}/{step.get('name')}: secret outside step env"
        assert "secrets." not in step.get("run", ""), f"{jname}/{step.get('name')}: secret inlined in run"


def test_jobs_that_read_secrets_run_in_the_production_environment(jobs):
    for name, job in jobs.items():
        if SECRET_REF.search(_dump(job)):
            assert job.get("environment") == "production", name


def test_railway_token_only_on_steps_that_call_railway(jobs):
    for jname, step in _steps(jobs):
        env = step.get("env") or {}
        if "RAILWAY_TOKEN" in env:
            assert env["RAILWAY_TOKEN"] == "${{ secrets.RAILWAY_TOKEN }}"
            run = step.get("run", "")
            assert "railway " in run or "deploy/railway/deploy_" in run, f"{jname}/{step.get('name')}"
            assert "npm install" not in run and "pip install" not in run, f"{jname}/{step.get('name')}"
        if "uses" in step:
            assert "RAILWAY_TOKEN" not in _dump(step), f"{jname}: an action must not see RAILWAY_TOKEN"


def test_backup_passphrase_is_gone(raw):
    assert "BACKUP_PASSPHRASE" not in raw
    assert "openssl enc" not in raw


def test_backup_fails_first_without_the_public_key(jobs):
    steps = jobs["backup"]["steps"]
    first = steps[0]
    assert first["env"]["RELEASE_BACKUP_PUBKEY"] == "${{ vars.RELEASE_BACKUP_PUBKEY }}"
    assert "::error::" in first["run"] and "exit 1" in first["run"]
    assert "age1" in first["run"]
    assert not SECRET_REF.search(_dump(first))


def test_backup_encrypts_to_the_public_key_and_uploads_only_ciphertext(jobs):
    steps = jobs["backup"]["steps"]
    dump = [s for s in steps if "pg_dump" in s.get("run", "")]
    assert len(dump) == 1
    run = dump[0]["run"]
    assert "age -R" in run and "prod_pre_migrate.dump.age" in run
    assert 'rm -f "$plain"' in run and "trap " in run
    assert "$RUNNER_TEMP/prod.dump" in run
    uploads = [s for s in steps if str(s.get("uses", "")).startswith("actions/upload-artifact@")]
    assert len(uploads) == 1
    w = uploads[0]["with"]
    assert w["path"] == "prod_pre_migrate.dump.age"
    assert int(w["retention-days"]) <= 7
    assert w["if-no-files-found"] == "error"


def test_no_artifact_upload_of_a_plain_dump_anywhere():
    for path in WORKFLOWS.glob("*.yml"):
        for jname, step in _steps(yaml.safe_load(path.read_text(encoding="utf-8"))["jobs"]):
            if str(step.get("uses", "")).startswith("actions/upload-artifact@"):
                p = str(step["with"]["path"])
                assert not re.search(r"\.(dump|sql|enc|gz|tar)\b", p) or p.endswith(".age"), f"{path.name}/{jname}: {p}"
                if "dump" in p:
                    assert path.name == "release.yml" and p.endswith(".age"), p


def test_deploy_key_still_used_only_by_release_and_check(jobs):
    # Re-stated next to the environment change: release keeps pushing over SSH.
    users = {name for name, job in jobs.items() if SECRET in _dump(job)}
    assert users == {"release", "check"}
    assert jobs["release"]["environment"] == "production"
    assert _checkout(jobs["release"])["with"]["ssh-key"] == SECRET_EXPR


def test_every_action_is_pinned_by_full_sha_with_a_version_comment():
    for path in WORKFLOWS.glob("*.yml"):
        for ln in path.read_text(encoding="utf-8").splitlines():
            m = re.search(r"uses:\s*(\S+)(.*)$", ln)
            if not m:
                continue
            ref, rest = m.group(1), m.group(2)
            assert ref.startswith("actions/"), f"{path.name}: third-party action {ref}"
            assert re.fullmatch(r"[\w.-]+/[\w.-]+@[0-9a-f]{40}", ref), f"{path.name}: {ref} not pinned by SHA"
            assert re.fullmatch(r"\s*# v\d+\.\d+\.\d+", rest), f"{path.name}: {ref} lacks '# vX.Y.Z'"


def test_railway_cli_is_pinned_and_installed_without_scripts(wf, jobs, raw):
    env = wf["env"]
    version = str(env["RAILWAY_CLI_VERSION"])
    assert re.fullmatch(r"\d+\.\d+\.\d+", version)
    assert re.fullmatch(r"[0-9a-f]{64}", str(env["RAILWAY_CLI_SHA256"]))
    pkg = json.loads((REPO_ROOT / "deploy" / "railway" / "cli" / "package.json").read_text(encoding="utf-8"))
    assert pkg["dependencies"]["@railway/cli"] == version, "bump RAILWAY_CLI_VERSION and RAILWAY_CLI_SHA256 together"
    installs = [ln.strip() for ln in raw.splitlines() if "npm install" in ln or "npm i " in ln]
    assert installs, "no Railway CLI install found"
    for ln in installs:
        assert "--ignore-scripts" in ln and '"@railway/cli@${RAILWAY_CLI_VERSION}"' in ln, ln
    for jname, step in _steps(jobs):
        run = step.get("run", "")
        if "npm install" in run:
            assert not SECRET_REF.search(_dump(step)), f"{jname}: install step must hold no secret"
            assert 'sha256sum -c' in run and "${RAILWAY_CLI_SHA256}" in run, jname
            assert run.index("sha256sum -c") < run.index("tar -xzf"), jname
    calls = {name for name, job in jobs.items() if "railway " in _dump(job)}
    for name in calls:
        assert "npm install" in _dump(jobs[name]), f"{name} calls railway without the pinned install"


def test_logs_stay_quiet(jobs, raw):
    assert "railway logs" not in raw
    assert "pg_restore -l prod.dump | head" not in raw
    for jname, step in _steps(jobs):
        run = step.get("run", "")
        for ln in run.splitlines():
            if re.search(r"(^|[;&|]\s*)railway status\b", ln.strip()):
                assert ">/dev/null 2>&1" in ln, f"{jname}: railway status output must be hidden"
        if "backend.scripts.migrate" in run:
            assert (step.get("env") or {}).get("MIGRATE_HIDE_TARGET") == "1", jname
        assert "set -x" not in run
        assert "DATABASE_PUBLIC_URL" not in run or "echo $DATABASE" not in run.replace('"', "")


def test_dispatch_inputs_reach_scripts_only_through_env(jobs):
    for jname, step in _steps(jobs):
        assert "${{ inputs." not in step.get("run", ""), f"{jname}/{step.get('name')}: inline input"


def test_check_job_compares_rulesets_read_only(jobs):
    steps = [s for s in jobs["check"]["steps"] if "check_release_protection.py" in s.get("run", "")]
    assert len(steps) == 1
    assert not SECRET_REF.search(_dump(steps[0]))
    assert "--strict" not in steps[0]["run"]
    assert "write" not in _dump(jobs["check"].get("permissions", {}))


def test_dependabot_covers_pip_npm_and_actions():
    cfg = yaml.safe_load((REPO_ROOT / ".github" / "dependabot.yml").read_text(encoding="utf-8"))
    assert cfg["version"] == 2
    ecos = {u["package-ecosystem"]: u for u in cfg["updates"]}
    assert {"pip", "npm", "github-actions"} <= set(ecos)
    for u in cfg["updates"]:
        assert u.get("target-branch") == "dev", u["package-ecosystem"]
        assert (REPO_ROOT / u["directory"].lstrip("/")).is_dir(), u["directory"]
    assert (REPO_ROOT / ecos["npm"]["directory"].lstrip("/") / "package.json").is_file()


def test_gate_job_holds_no_secret_and_every_secret_job_needs_it(wf, jobs):
    gate = jobs["gate"]
    assert "environment" not in gate
    assert not SECRET_REF.search(_dump(gate)) and "vars." not in _dump(gate)
    run = gate["steps"][0]["run"]
    assert "^(dev|main|hotfix/" in run and "exit 1" in run
    for name, job in jobs.items():
        if name == "gate":
            continue
        needs = job.get("needs", [])
        needs = [needs] if isinstance(needs, str) else needs
        assert "gate" in needs, f"{name} must need gate"
    assert "inputs.ref" in wf["run-name"] and "github.actor" in wf["run-name"]


def test_deploy_and_scan_refuse_a_tag_that_is_not_main(jobs):
    for name in ("deploy", "scan"):
        runs = [s["run"] for s in jobs[name]["steps"] if 'git checkout --detach "v$V"' in s.get("run", "")]
        assert len(runs) == 1, name
        run = runs[0]
        assert 'git rev-parse "v$V^{commit}"' in run and "git rev-parse origin/main" in run, name
        assert run.index("git rev-parse origin/main") < run.index('git checkout --detach "v$V"'), name


def test_release_fails_when_main_is_not_at_head_after_the_push(jobs):
    script = _release_script(jobs["release"])
    assert 'main on origin is not at HEAD after the push"; exit 1' in script
    assert "&& echo main-released" not in script


PGDG_FPR = "B97B0AFCAA1A47F044F244A07FCC7D46ACCC4CF8"


def test_backup_checks_the_pgdg_key_fingerprint_before_apt(jobs):
    tools = next(s["run"] for s in jobs["backup"]["steps"] if "apt-get install" in s.get("run", ""))
    assert "set -euo pipefail" in tools
    check = tools.index(f'test "$fpr" = "{PGDG_FPR}"')
    assert check < tools.index("apt-get update")
    # Captured with a throwaway GNUPGHOME and an awk that reads all input: no
    # early-exiting reader in a pipe that pipefail could turn into a failure.
    assert "GNUPGHOME=" in tools and "--show-keys --with-colons" in tools
    assert "grep -q '^fpr" not in tools
    assert "exit}" not in tools
    assert re.search(r"apt-get install -y -qq postgresql-client-18 age\b", tools)


PUBLIC_TEXT = (
    RELEASE_YML,
    REPO_ROOT / "docs" / "RELEASING.md",
    REPO_ROOT / "docs" / "scan_loop" / "OWNER_SETUP.md",
)


@pytest.mark.parametrize("path", PUBLIC_TEXT, ids=lambda p: p.name)
def test_public_release_text_names_no_internal_finding_ids(path):
    text = path.read_text(encoding="utf-8")
    assert not re.search(r"\b[A-Z]\d+-\d+\b", text), path.name
