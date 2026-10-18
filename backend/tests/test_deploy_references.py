"""Deploy files only reference things that exist (remediation P15B.6, NEXT_PHASES N4).

The dealer_jobs worker and scheduler lost their Railway services on 2026-10-08 (D-REL1 (a)).
P15B.6 removed what was left of them in the repo:

- Dockerfile.scanner-worker, Dockerfile.scanner-scheduler and their per-service Railway tomls;
- the ``scanner-worker`` / ``scanner-scheduler`` services in deploy/docker-compose.yml, with the
  ``scanner_node_modules`` volume and ``SCANNER_BROWSER_PROFILE_DIR``;
- the old worker entrypoint name: the script the discovery image still runs is now
  scripts/docker-entrypoint-jobs-worker.sh.

The compose Postgres moved from postgres:16-alpine to the pgvector image at prod's major
(PG 18.6 with vector 0.8.6; D-TC3), because migrations/V001 runs ``CREATE EXTENSION vector``.

These tests read files as text (no Docker, no network). P18A.7 extends this file when it
deletes the k8s-era manifests (D-IF9).
"""

from __future__ import annotations

import json
import re
import shlex
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE = REPO_ROOT / "deploy" / "docker-compose.yml"
THIS_FILE = "backend/tests/test_deploy_references.py"

DELETED_FILES = (
    "Dockerfile.scanner-worker",
    "Dockerfile.scanner-scheduler",
    "deploy/railway/railway.scanner-worker.toml",
    "deploy/railway/railway.scanner-scheduler.toml",
)
# Names only history may still mention (docs/ and CHANGELOG.md record what was removed).
DELETED_NAMES = (
    "Dockerfile.scanner-worker",
    "Dockerfile.scanner-scheduler",
    "railway.scanner-worker.toml",
    "railway.scanner-scheduler.toml",
    "scanner_node_modules",
    "SCANNER_BROWSER_PROFILE_DIR",
    "docker-entrypoint-scanner-worker",
)
# NEXT_PHASES_PLAN N4 exit gate:
#   grep -rn "scanner-worker\|scanner-scheduler" deploy Dockerfile* docker-compose.yml
# Files allowed to match, each with the reason and the unit that removes it.
EXIT_GATE_ALLOWED = {
    # A Kubernetes node label (nodeSelector scanner-worker=true), not the deleted service.
    # The whole k8s-era deploy/car-scanner/ directory is deleted by P18A.7 (D-IF9).
    "deploy/car-scanner/cronjob.yaml",
}
# Prod runs PG 18.6 with pgvector 0.8.6 (NEXT_PHASES_PLAN Section 2.5); D-TC3 = match prod.
PROD_PG_MAJOR = 18
PGVECTOR_IMAGE = re.compile(r"^pgvector/pgvector:0\.8\.\d+-pg(?P<major>\d+)(?:-[a-z]+)?$")
EXPECTED_SERVICES = {"postgres", "web"}


def _git_grep(*args: str) -> str:
    if shutil.which("git") is None:
        pytest.skip("git not available")
    inside = subprocess.run(
        ["git", "rev-parse", "--is-inside-work-tree"],
        cwd=REPO_ROOT, capture_output=True, text=True, check=False,
    )
    if inside.returncode != 0 or inside.stdout.strip() != "true":
        pytest.skip("not a git checkout")
    hits = subprocess.run(
        ["git", "grep", "-n", "-I", *args],
        cwd=REPO_ROOT, capture_output=True, text=True, check=False,
    )
    # git grep: 0 = matches, 1 = no match, anything else = error.
    assert hits.returncode in (0, 1), hits.stderr
    return hits.stdout


@pytest.fixture(scope="module")
def compose() -> dict:
    data = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    assert isinstance(data, dict) and isinstance(data.get("services"), dict)
    return data


def _mounts(service: dict) -> list[tuple[str, str]]:
    """(source, target) for every short-syntax volume entry of a service."""
    out = []
    for entry in service.get("volumes") or []:
        assert isinstance(entry, str), f"long-syntax volume not handled here: {entry!r}"
        parts = entry.split(":")
        assert len(parts) >= 2, entry
        out.append((parts[0], parts[1]))
    return out


def _is_named_volume(source: str) -> bool:
    return not source.startswith((".", "/", "~", "$"))


# ---------------------------------------------------------------------------
# 1. The dead worker/scheduler files are gone and nothing executable names them
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("rel", DELETED_FILES)
def test_dead_worker_scheduler_file_is_deleted(rel):
    assert not (REPO_ROOT / rel).exists(), rel


def test_no_reference_to_deleted_names_outside_history():
    patterns = [arg for name in DELETED_NAMES for arg in ("-e", name)]
    out = _git_grep(
        "-F", *patterns, "--", ".",
        ":(exclude)docs/**",
        ":(exclude)CHANGELOG.md",
        f":(exclude){THIS_FILE}",
    )
    assert out == "", f"references to deleted deploy files or settings remain:\n{out}"


def test_exit_gate_grep_matches_only_allowed_files():
    out = _git_grep(
        "-E", "scanner-worker|scanner-scheduler", "--", "deploy", "Dockerfile*", "docker-compose.yml",
    )
    files = {line.split(":", 1)[0] for line in out.splitlines() if line}
    unexpected = sorted(files - EXIT_GATE_ALLOWED)
    assert not unexpected, f"worker/scheduler names outside history: {unexpected}\n{out}"


# ---------------------------------------------------------------------------
# 2. docker-compose: Postgres with pgvector at prod's major, no scanner services
# ---------------------------------------------------------------------------


def test_compose_runs_only_postgres_and_web(compose):
    assert set(compose["services"]) == EXPECTED_SERVICES


def test_compose_postgres_is_pinned_pgvector_at_prod_major(compose):
    image = compose["services"]["postgres"]["image"]
    m = PGVECTOR_IMAGE.match(image)
    assert m, f"compose Postgres must be a pinned 0.8.x pgvector image, got {image!r}"
    assert int(m.group("major")) == PROD_PG_MAJOR, image


def test_compose_postgres_mount_uses_the_pg18_data_layout(compose):
    """PG 18+ images exit when anything is mounted at /var/lib/postgresql/data.

    Their PGDATA is /var/lib/postgresql/18/docker and the VOLUME is /var/lib/postgresql
    (docker-library/postgres PR 1259), so the data volume mounts there.
    """
    targets = [target.rstrip("/") for _, target in _mounts(compose["services"]["postgres"])]
    assert "/var/lib/postgresql" in targets
    assert "/var/lib/postgresql/data" not in targets


def test_compose_named_volumes_are_declared_and_used(compose):
    declared = set((compose.get("volumes") or {}).keys())
    used = {
        source
        for service in compose["services"].values()
        for source, _ in _mounts(service)
        if _is_named_volume(source)
    }
    assert used <= declared, f"undeclared named volumes: {sorted(used - declared)}"
    assert declared <= used, f"declared but unused volumes: {sorted(declared - used)}"


def test_compose_build_files_and_entrypoints_exist(compose):
    for name, service in compose["services"].items():
        build = service.get("build")
        if isinstance(build, dict):
            context = (COMPOSE.parent / build.get("context", ".")).resolve()
            dockerfile = context / build.get("dockerfile", "Dockerfile")
            assert dockerfile.is_file(), f"{name}: {dockerfile} missing"
        for key in ("entrypoint", "command"):
            value = service.get(key)
            words = value if isinstance(value, list) else shlex.split(value or "")
            for word in words:
                if word.startswith("/app/"):
                    assert (REPO_ROOT / word.removeprefix("/app/")).exists(), f"{name}: {word}"


def test_deploy_scripts_name_only_compose_services(compose):
    services = set(compose["services"])
    call = re.compile(r"docker compose(?:\s+-f\s+\S+)?\s+(build|up|exec)\s+(.*)$")
    seen = 0
    for script in ("deploy/up.sh", "deploy/restart.sh"):
        for line in (REPO_ROOT / script).read_text(encoding="utf-8").splitlines():
            m = call.search(line)
            if not m:
                continue
            verb, rest = m.groups()
            words = shlex.split(rest.split(">")[0].rstrip("\\"))
            names, skip = [], False
            for word in words:
                if skip:
                    skip = False
                elif word in ("--scale", "-f", "--file"):
                    skip = True
                elif not word.startswith("-"):
                    names.append(word)
            if verb == "exec":
                names = names[:1]
            for name in names:
                seen += 1
                assert name in services, f"{script}: '{line.strip()}' names unknown service {name!r}"
    assert seen, "expected deploy/up.sh and deploy/restart.sh to call docker compose"


# ---------------------------------------------------------------------------
# 3. Dockerfiles and Railway configs point at files that exist
# ---------------------------------------------------------------------------


def _dockerfiles() -> list[Path]:
    files = sorted(REPO_ROOT.glob("Dockerfile*"))
    assert files, "expected Dockerfiles at the repo root"
    return files


@pytest.mark.parametrize("dockerfile", _dockerfiles(), ids=lambda p: p.name)
def test_dockerfile_scripts_exist(dockerfile):
    text = dockerfile.read_text(encoding="utf-8")
    for rel in sorted(set(re.findall(r"/app/(scripts/[\w./-]+)", text))):
        assert (REPO_ROOT / rel).is_file(), f"{dockerfile.name} names missing {rel}"


def test_railway_configs_name_existing_dockerfiles():
    # Since N4.1 the expected Railway settings live in deploy/railway/service_settings.json
    # (config-as-code files are gone); any railway.* file that comes back is checked too.
    settings = json.loads((REPO_ROOT / "deploy" / "railway" / "service_settings.json").read_text())
    builds = [
        (f"service_settings.json:{name}", entry["serviceInstance"]["dockerfilePath"])
        for name, entry in settings["services"].items()
    ]
    configs = [REPO_ROOT / "railway.toml", *sorted((REPO_ROOT / "deploy" / "railway").glob("railway.*"))]
    for config in configs:
        if not config.is_file():
            continue
        for path in re.findall(r"""dockerfilePath["']?\s*[=:]\s*["']([^"']+)["']""", config.read_text()):
            builds.append((config.name, path))
    assert builds, "expected at least one Railway service build to check"
    for source, path in builds:
        assert (REPO_ROOT / path).is_file(), f"{source} builds missing {path}"
