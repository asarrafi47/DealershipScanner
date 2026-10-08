"""The CI workflow runs what the remediation plan says it runs (P2A.2).

.github/workflows/ci.yml is easy to break without noticing: a narrowed
trigger, a renamed job or a skipped job reads as "green" on GitHub. This
module pins the parts later phases rely on:

- triggers: every branch push, PRs to main, manual dispatch, no tag runs;
- ``permissions: contents: read`` and the per-ref concurrency group;
- the job ids ``lint``, ``pytest`` and ``pytest-integration`` (required checks
  on main from P2B.5), ``needs: lint``, the timeouts, and no job-level ``if:``
  on them (a skipped job reports success and would mask a red run);
- the exact ruff pin;
- the install: CPU torch first (D-TC5), then requirements.txt plus
  requirements-test.txt;
- the offline selection, the network guard, node, and the junit artifact.

PyYAML parses the bare key ``on`` as the boolean ``True`` (YAML 1.1), hence
``d.get("on", d.get(True))``. pyyaml is declared in requirements-test.txt, so
the import below is a hard dependency: this module fails, never skips, when
it is missing.
"""

from __future__ import annotations

import fnmatch
import re
import shlex
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
CI_YML = REPO_ROOT / ".github" / "workflows" / "ci.yml"
TEST_REQUIREMENTS = REPO_ROOT / "requirements-test.txt"

REQUIRED_JOBS = ("lint", "pytest", "pytest-integration")
RUFF_VERSION = "0.15.22"
PYTEST_VERSION = "9.1.1"
TIMEOUT_MINUTES = {"lint": 5, "pytest": 30, "pytest-integration": 15}
OFFLINE_SELECTION = "not integration and not slow and not pg"
CPU_TORCH_INDEX = "https://download.pytorch.org/whl/cpu"
JUNIT_PATH = "reports/offline.xml"


@pytest.fixture(scope="module")
def workflow() -> dict:
    return yaml.safe_load(CI_YML.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def triggers(workflow) -> dict:
    on = workflow.get("on", workflow.get(True))
    assert isinstance(on, dict), f"ci.yml `on:` must be a mapping, got {on!r}"
    return on


@pytest.fixture(scope="module")
def jobs(workflow) -> dict:
    return workflow["jobs"]


def _steps(job: dict) -> list[dict]:
    return job.get("steps") or []


def _runs(job: dict) -> list[str]:
    return [s["run"] for s in _steps(job) if "run" in s]


def _index(job: dict, predicate) -> int:
    for i, step in enumerate(_steps(job)):
        if predicate(step):
            return i
    raise AssertionError("no step matches")


def _run_index(job: dict, needle: str) -> int:
    return _index(job, lambda s: needle in (s.get("run") or ""))


def _uses_index(job: dict, action: str) -> int:
    return _index(job, lambda s: (s.get("uses") or "").split("@")[0] == action)


def _needs(job: dict) -> list[str]:
    needs = job.get("needs") or []
    return [needs] if isinstance(needs, str) else list(needs)


def _pytest_args(job: dict) -> list[str]:
    """The arguments after ``python -m pytest backend/tests`` in the job's one pytest step."""
    cmds = [r for r in _runs(job) if "-m pytest" in r]
    assert len(cmds) == 1, f"expected one pytest step, found {cmds}"
    argv = shlex.split(cmds[0])
    assert argv[:4] == ["python", "-m", "pytest", "backend/tests"], argv
    return argv[4:]


def _opt(argv: list[str], flag: str) -> str:
    """Value of ``-m X`` or ``--flag=X`` style options."""
    for i, arg in enumerate(argv):
        if arg == flag:
            return argv[i + 1]
        if arg.startswith(flag + "="):
            return arg.split("=", 1)[1]
    raise AssertionError(f"{flag} missing from {argv}")


# --- triggers, permissions, concurrency -----------------------------------


def test_yaml_on_key_is_read_either_way():
    # The premise of d.get("on", d.get(True)): PyYAML turns a bare `on:` into True.
    assert yaml.safe_load("on:\n  push:\n") == {True: {"push": None}}


def test_push_runs_on_every_branch_and_never_on_tags(triggers):
    push = triggers["push"]
    assert push == {"branches": ["**"]}, push
    # A tags / tags-ignore filter next to `branches` would start tag runs.
    assert not {"tags", "tags-ignore", "branches-ignore", "paths", "paths-ignore"} & set(push)


def test_pull_requests_to_main_and_dispatch(triggers):
    assert triggers["pull_request"] == {"branches": ["main"]}
    assert "workflow_dispatch" in triggers
    assert set(triggers) == {"push", "pull_request", "workflow_dispatch"}


def test_permissions_are_read_only(workflow):
    assert workflow["permissions"] == {"contents": "read"}


def test_concurrency_groups_by_pr_or_ref_and_keeps_main_runs(workflow):
    conc = workflow["concurrency"]
    assert conc["group"] == "ci-${{ github.event.pull_request.number || github.ref }}"
    assert conc["cancel-in-progress"] == "${{ github.ref != 'refs/heads/main' }}"


# --- jobs -------------------------------------------------------------------


def test_required_job_ids_exist(jobs):
    missing = [j for j in REQUIRED_JOBS if j not in jobs]
    assert not missing, f"required check job ids renamed or removed: {missing}"


@pytest.mark.parametrize("job_id", REQUIRED_JOBS)
def test_required_jobs_are_never_skipped_by_an_if(jobs, job_id):
    # A skipped job reports success on its SHA and would mask a red run.
    assert "if" not in jobs[job_id], f"{job_id} has a job-level if: {jobs[job_id]['if']!r}"


@pytest.mark.parametrize("job_id", ["pytest", "pytest-integration"])
def test_test_jobs_need_lint(jobs, job_id):
    assert _needs(jobs[job_id]) == ["lint"]


@pytest.mark.parametrize("job_id,minutes", sorted(TIMEOUT_MINUTES.items()))
def test_timeouts(jobs, job_id, minutes):
    assert jobs[job_id]["timeout-minutes"] == minutes


@pytest.mark.parametrize("job_id", REQUIRED_JOBS)
def test_python_312(jobs, job_id):
    i = _uses_index(jobs[job_id], "actions/setup-python")
    assert str(_steps(jobs[job_id])[i]["with"]["python-version"]) == "3.12"


def test_ruff_is_pinned_exactly(jobs):
    runs = _runs(jobs["lint"])
    pins = re.findall(r"\bruff==([0-9][0-9.]*)\b", "\n".join(runs))
    assert pins == [RUFF_VERSION], f"lint must install ruff=={RUFF_VERSION}, found {pins}"
    assert any(shlex.split(r)[:3] == ["ruff", "check", "."] for r in runs), runs


# --- install (D-TC5) --------------------------------------------------------


@pytest.mark.parametrize("job_id", ["pytest", "pytest-integration"])
def test_cpu_torch_installs_before_the_requirements(jobs, job_id):
    job = jobs[job_id]
    torch_i = _run_index(job, "pip install torch")
    torch_argv = shlex.split(_steps(job)[torch_i]["run"])
    assert _opt(torch_argv, "--index-url") == CPU_TORCH_INDEX

    req_i = _run_index(job, "-r requirements.txt")
    req_argv = shlex.split(_steps(job)[req_i]["run"])
    assert req_argv[:2] == ["pip", "install"]
    files = [req_argv[i + 1] for i, a in enumerate(req_argv) if a == "-r"]
    assert files == ["requirements.txt", "requirements-test.txt"], req_argv
    # Torch first, or pip resolves sentence-transformers to the CUDA wheel.
    assert torch_i < req_i


@pytest.mark.parametrize("job_id", ["pytest", "pytest-integration"])
def test_pip_cache_covers_every_requirements_file(jobs, job_id):
    job = jobs[job_id]
    cfg = _steps(job)[_uses_index(job, "actions/setup-python")]["with"]
    assert cfg["cache"] == "pip"
    pattern = cfg["cache-dependency-path"]
    for name in ("requirements.txt", "requirements-test.txt"):
        assert fnmatch.fnmatch(name, pattern), (name, pattern)


def test_requirements_test_pins_exact_versions():
    pins: dict[str, str] = {}
    for raw in TEST_REQUIREMENTS.read_text(encoding="utf-8").splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        m = re.fullmatch(r"([A-Za-z0-9][A-Za-z0-9._-]*)==([0-9][0-9A-Za-z.]*)", line)
        assert m, f"requirements-test.txt: not an exact pin: {raw!r}"
        pins[m.group(1).lower()] = m.group(2)
    assert set(pins) == {"pytest", "pytest-timeout", "pyyaml"}, pins
    assert pins["pytest"] == PYTEST_VERSION


# --- offline run ------------------------------------------------------------


def test_offline_selection_and_options(jobs):
    argv = _pytest_args(jobs["pytest"])
    assert _opt(argv, "-m") == OFFLINE_SELECTION
    assert _opt(argv, "-p") == "no:cacheprovider"
    assert _opt(argv, "--timeout") == "300"
    assert _opt(argv, "--durations") == "40"
    assert _opt(argv, "--junitxml") == JUNIT_PATH


def test_offline_run_blocks_the_network(jobs):
    job = jobs["pytest"]
    step = _steps(job)[_run_index(job, "-m pytest")]
    env = {**(job.get("env") or {}), **(step.get("env") or {})}
    assert str(env.get("TESTS_BLOCK_NETWORK")) == "1", env


def test_node_is_set_up_before_the_offline_run(jobs):
    job = jobs["pytest"]
    node_i = _uses_index(job, "actions/setup-node")
    assert str(_steps(job)[node_i]["with"]["node-version"]) == "20"
    assert node_i < _run_index(job, "-m pytest")


def test_junit_report_is_uploaded_even_when_tests_fail(jobs):
    job = jobs["pytest"]
    up_i = _uses_index(job, "actions/upload-artifact")
    step = _steps(job)[up_i]
    assert step.get("if") == "always()"
    assert step["with"]["path"] == JUNIT_PATH
    assert up_i > _run_index(job, "-m pytest")


def test_integration_tier_runs_by_marker_and_lists_skips(jobs):
    argv = _pytest_args(jobs["pytest-integration"])
    assert _opt(argv, "-m") == "integration"
    assert "-rs" in argv
