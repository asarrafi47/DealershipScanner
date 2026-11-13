"""Railway service settings replace config-as-code (N4.1, D-N24).

deploy/railway/service_settings.json holds the settings the deleted root
railway.toml (web) and deploy/railway/railway.scanner-nightly.json
(scanner-nightly) used to apply; deploy/railway/service_settings.py reads the
live values with one read-only ``railway api`` query. These tests pin:

- the expected settings equal the two old files, as they stood at the 1.5.6
  release (164096b33), plus the "must stay unset" keys (no cron, no config-file
  path, no healthcheck on the scanner job); the old build.builder = DOCKERFILE
  is carried by a non-null dockerfilePath (the instance builder field reads
  RAILPACK on Dockerfile-built services, P0A.3);
- the ids are the ones the P0A.1 audit recorded in docs/SCANNING_OPS_LOG.md;
- the helper only ever sends a plain query, and its printed owner-pack commands
  carry exactly the expected values;
- the ``check`` command's exit codes against a stub ``railway`` (no network, no
  real CLI: PATH holds only the stub directory and the system bin directories).

The deploy-script side of the check (refusals, ``--no-settings-check``, the
config-as-code refusal) is in test_deploy_guard.py.
"""

from __future__ import annotations

import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
DEPLOY_DIR = REPO_ROOT / "deploy" / "railway"
SETTINGS_JSON = DEPLOY_DIR / "service_settings.json"
HELPER = DEPLOY_DIR / "service_settings.py"
OPS_LOG = REPO_ROOT / "docs" / "SCANNING_OPS_LOG.md"
_UUID = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"

# The deleted config-as-code files, verbatim from 164096b33 (Release 1.5.6). Frozen
# here because CI checks out without history.
OLD_RAILWAY_TOML = """\
# DealershipScanner (Sarrafi Cars) — Railway deployment
# Docs: deploy/railway/README.md

[build]
builder = "DOCKERFILE"
dockerfilePath = "Dockerfile.web"

[deploy]
healthcheckPath = "/health"
healthcheckTimeout = 300
restartPolicyType = "ON_FAILURE"
restartPolicyMaxRetries = 5
# Secrets: Railway kmac-vault (same project). Wire with deploy/railway/sync-vault-to-railway.sh
numReplicas = 1
"""
OLD_SCANNER_NIGHTLY_JSON = """\
{
  "$schema": "https://railway.com/railway.schema.json",
  "build": {
    "builder": "DOCKERFILE",
    "dockerfilePath": "Dockerfile.scanner"
  },
  "deploy": {
    "restartPolicyType": "NEVER",
    "numReplicas": 1,
    "limitOverride": {
      "containers": {
        "cpu": 16,
        "memoryBytes": 17179869184
      }
    }
  }
}
"""
# Settings the old files left at Railway's default (unset); the new file says so explicitly.
MUST_BE_UNSET = {
    "web": {"cronSchedule", "railwayConfigFile"},
    "scanner-nightly": {"cronSchedule", "railwayConfigFile", "healthcheckPath"},
}


def _load_helper():
    spec = importlib.util.spec_from_file_location("railway_service_settings", HELPER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def helper():
    return _load_helper()


@pytest.fixture(scope="module")
def settings() -> dict:
    return json.loads(SETTINGS_JSON.read_text(encoding="utf-8"))


def _old_file_settings(config: dict) -> tuple[dict, dict | None]:
    """(serviceInstance fields, limits) that a config-as-code file applied.

    build.builder = DOCKERFILE is not a checked field: it is what a set
    dockerfilePath means (see the module docstring).
    """
    fields = dict(config.get("build", {}))
    assert fields.pop("builder") == "DOCKERFILE"
    assert fields.get("dockerfilePath"), "a Dockerfile build names its Dockerfile"
    deploy = dict(config.get("deploy", {}))
    limit = deploy.pop("limitOverride", None)
    fields.update(deploy)
    limits = dict(limit["containers"]) if limit else None
    return fields, limits


@pytest.mark.parametrize(
    ("service", "old"),
    [("web", tomllib.loads(OLD_RAILWAY_TOML)), ("scanner-nightly", json.loads(OLD_SCANNER_NIGHTLY_JSON))],
)
def test_expected_settings_equal_the_old_config_files(settings, service, old) -> None:
    entry = settings["services"][service]
    old_fields, old_limits = _old_file_settings(old)
    new_fields = entry["serviceInstance"]
    for key, value in old_fields.items():
        assert key in new_fields, f"{service}: {key} from the old file is not checked"
        assert new_fields[key] == value and type(new_fields[key]) is type(value), (service, key)
    extra = set(new_fields) - set(old_fields)
    assert extra == MUST_BE_UNSET[service], (service, sorted(extra))
    assert all(new_fields[key] is None for key in extra)
    assert entry.get("limits") == old_limits


def test_ids_are_the_audited_ones(settings) -> None:
    """environment, web and scanner-nightly ids from the P0A.1 per-service table."""
    log = OPS_LOG.read_text(encoding="utf-8")
    env = re.search(r"environment `production` \(`(" + _UUID + r")`", log)
    row = re.search(
        r"^\| service id \| `(" + _UUID + r")` \| `(" + _UUID + r")` \| `(" + _UUID + r")`", log, flags=re.MULTILINE
    )
    assert env and row, "P0A.1 ids not found in docs/SCANNING_OPS_LOG.md"
    assert "| | web | Postgres | scanner-nightly |" in log  # the column order of that table
    assert settings["environmentId"] == env.group(1)
    assert settings["services"]["web"]["serviceId"] == row.group(1)
    assert settings["services"]["scanner-nightly"]["serviceId"] == row.group(3)
    # web-cards (the nightly card builder) is a third service; its id is deliberately not in the repository.
    assert set(settings["services"]) == {"web", "scanner-nightly", "web-cards"}
    assert "serviceId" not in settings["services"]["web-cards"]


def test_every_expected_field_is_readable(helper, settings, monkeypatch) -> None:
    monkeypatch.setenv("RAILWAY_WEB_CARDS_SERVICE_ID", "00000000-0000-4000-8000-0000000000c1")
    for service in settings["services"]:
        entry = helper.service_entry(settings, service)
        assert set(entry["serviceInstance"]) <= helper.READABLE_FIELDS


# --- the query is read-only ---------------------------------------------------


@pytest.mark.parametrize("service", ["web", "scanner-nightly"])
def test_query_is_a_plain_query_for_the_expected_fields(helper, settings, service) -> None:
    entry = helper.service_entry(settings, service)
    doc = helper.build_query(entry)
    assert doc.startswith("query N41ServiceSettings(")
    assert "mutation" not in doc.lower() and "subscription" not in doc.lower()
    selected = re.search(r"serviceId: \$serviceId\) \{ ([^}]*) \}", doc).group(1).split()
    assert selected == ["serviceName", *sorted(entry["serviceInstance"])]
    assert ("serviceInstanceLimits(" in doc) == (service == "scanner-nightly")
    argv = helper.railway_api_argv(doc, helper.query_variables(settings, entry))
    assert argv[:3] == ["railway", "api", doc] and argv[3] == "--variables"
    assert json.loads(argv[4]) == {"environmentId": settings["environmentId"], "serviceId": entry["serviceId"]}


@pytest.mark.parametrize(
    "doc",
    [
        'mutation { serviceInstanceUpdate(serviceId: "x", environmentId: "y", input: {}) }',
        'query Q { a } mutation M { serviceInstanceRedeploy(serviceId: "x", environmentId: "y") }',
        'subscription { deploymentLogs(deploymentId: "x") { message } }',
        "{ me { id } }",  # anonymous shorthand: only an explicit query is sent
    ],
)
def test_anything_but_a_plain_query_is_refused_before_railway_runs(helper, doc, monkeypatch) -> None:
    monkeypatch.setenv("PATH", "/nonexistent")
    with pytest.raises(helper.SettingsError, match="not a plain query"):
        helper.assert_read_only(doc)
    with pytest.raises(helper.SettingsError, match="not a plain query"):
        helper.run_query(["railway", "api", doc, "--variables", "{}"])


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (lambda s: s["services"]["web"]["serviceInstance"].update(startCmd="x"), "unknown fields"),
        (lambda s: s["services"]["web"].update(serviceId="web"), "serviceId is not a UUID"),
        (lambda s: s.update(environmentId="production"), "environmentId is not a UUID"),
        (lambda s: s["services"]["scanner-nightly"].update(limits={"cpu": 16}), "limits must be"),
        (lambda s: s["services"]["scanner-nightly"].update(limits={"cpu": 16, "memoryBytes": "16G"}), "limits must be"),
        (lambda s: s["services"]["web"].update(serviceInstance={}), "serviceInstance is empty"),
    ],
)
def test_bad_expected_settings_are_rejected(helper, settings, change, message) -> None:
    broken = json.loads(json.dumps(settings))
    change(broken)
    with pytest.raises(helper.SettingsError, match=message):
        data = helper.parse_settings(json.dumps(broken))
        for service in data["services"]:
            helper.service_entry(data, service)


@pytest.mark.parametrize("text", ["", "not json", "[]", '{"environmentId": "x"}'])
def test_unreadable_expected_settings_are_rejected(helper, text) -> None:
    with pytest.raises(helper.SettingsError, match="expected settings"):
        helper.parse_settings(text)


def test_unknown_service_is_rejected(helper, settings) -> None:
    with pytest.raises(helper.SettingsError, match="no expected settings for service 'scanner-worker'"):
        helper.service_entry(settings, "scanner-worker")


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ({"containers": {"cpu": 16, "memoryBytes": 17179869184}}, {"cpu": 16, "memoryBytes": 17179869184}),
        ({"cpu": 8, "memoryBytes": 1024}, {"cpu": 8, "memoryBytes": 1024}),
        ({"vCPUs": 16, "memoryGB": 16}, {"cpu": 16, "memoryBytes": 17179869184}),
        ('{"vCPUs": 2, "memoryGB": 0.5}', {"cpu": 2, "memoryBytes": 536870912}),
        ({"containers": {"vCPUs": 4, "memoryGB": 1}}, {"cpu": 4, "memoryBytes": 1073741824}),
        (None, None),
        ({}, None),
        ({"cpu": True, "memoryBytes": 1}, None),
        ({"cpu": 16, "memoryBytes": 1, "region": "us-west2"}, None),
        ("not json", None),
        ([16, 16], None),
    ],
)
def test_limits_normalization(helper, raw, expected) -> None:
    assert helper.normalize_limits(raw) == expected


# --- owner-pack commands -------------------------------------------------------


def _run_helper(*args: str, env: dict | None = None, stdin: str | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-I", str(HELPER), *args], capture_output=True, text=True, timeout=60, env=env, input=stdin
    )


def _split_railway_api(line: str) -> tuple[str, dict]:
    import shlex

    argv = shlex.split(line)
    assert argv[:2] == ["railway", "api"] and argv[3] == "--variables" and len(argv) == 5, argv
    return argv[2], json.loads(argv[4])


@pytest.mark.parametrize("service", ["web", "scanner-nightly"])
def test_print_update_carries_exactly_the_documented_expected_values(helper, settings, service) -> None:
    proc = _run_helper("print-update", service)
    assert proc.returncode == 0, proc.stderr
    doc, variables = _split_railway_api(proc.stdout.strip())
    assert doc.startswith("mutation N41ServiceSettingsUpdate(") and "serviceInstanceUpdate(" in doc
    entry = settings["services"][service]
    documented = {k: v for k, v in entry["serviceInstance"].items() if k in helper.DOCUMENTED_UPDATE_FIELDS}
    assert variables == {
        "environmentId": settings["environmentId"],
        "serviceId": entry["serviceId"],
        "input": documented,
    }
    left_out = ["railwayConfigFile"] + (["limits"] if entry.get("limits") else [])
    assert f"not in this mutation (set separately, see the pack): {', '.join(left_out)}" in proc.stderr


def test_print_update_with_undocumented_adds_the_config_path(settings) -> None:
    proc = _run_helper("print-update", "web", "--with-undocumented")
    assert proc.returncode == 0, proc.stderr
    _, variables = _split_railway_api(proc.stdout.strip())
    assert variables["input"] == settings["services"]["web"]["serviceInstance"]
    assert proc.stderr == ""


@pytest.mark.parametrize("service", ["web", "scanner-nightly"])
def test_print_query_is_the_command_the_check_runs(helper, settings, service) -> None:
    proc = _run_helper("print-query", service)
    assert proc.returncode == 0, proc.stderr
    doc, variables = _split_railway_api(proc.stdout.strip())
    entry = helper.service_entry(settings, service)
    assert doc == helper.build_query(entry)
    assert variables == helper.query_variables(settings, entry)


# --- `check` against a stub railway --------------------------------------------

STUB = """#!/bin/sh
printf '%s\\n' "$*" | tr '\\n' ' ' >> "$STUB_LOG"; printf '\\n' >> "$STUB_LOG"
[ -n "$STUB_STDERR" ] && printf '%s\\n' "$STUB_STDERR" >&2
printf '%s' "$STUB_BODY"
exit "${STUB_EXIT:-0}"
"""


@pytest.fixture
def stub_env(tmp_path: Path) -> dict:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "railway").write_text(STUB, encoding="utf-8")
    (bin_dir / "railway").chmod(0o755)
    path = os.pathsep.join([str(bin_dir), "/usr/bin", "/bin"])
    assert shutil.which("railway", path=path) == str(bin_dir / "railway")
    return {"PATH": path, "HOME": str(tmp_path), "STUB_LOG": str(tmp_path / "railway.log"), "LC_ALL": "C"}


def _body(settings: dict, service: str, **overrides) -> str:
    entry = settings["services"][service]
    data = {"serviceInstance": {"serviceName": service, **entry["serviceInstance"], **overrides}}
    if entry.get("limits"):
        data["serviceInstanceLimits"] = {"containers": entry["limits"]}
    return json.dumps({"data": data})


@pytest.mark.parametrize(
    ("overrides", "stub_exit", "expected_rc", "needle"),
    [
        ({}, 0, 0, "all 9 checked settings match"),
        ({"restartPolicyMaxRetries": 10}, 0, 1, "1 setting(s) differ"),
        ({}, 1, 2, "CANNOT CHECK: railway api exited 1"),
    ],
)
def test_check_exit_codes(settings, stub_env, overrides, stub_exit, expected_rc, needle) -> None:
    env = dict(stub_env, STUB_BODY=_body(settings, "web", **overrides), STUB_EXIT=str(stub_exit))
    proc = _run_helper("check", "web", env=env)
    assert proc.returncode == expected_rc, proc.stdout + proc.stderr
    assert needle in proc.stdout
    calls = Path(env["STUB_LOG"]).read_text(encoding="utf-8").splitlines()
    assert len(calls) == 1 and calls[0].startswith("api query N41ServiceSettings(")


def test_check_reads_settings_from_stdin(settings, stub_env) -> None:
    """The deploy scripts pipe the deployed ref's file in with --settings-file -."""
    edited = json.loads(json.dumps(settings))
    edited["services"]["web"]["serviceInstance"]["restartPolicyMaxRetries"] = 10
    env = dict(stub_env, STUB_BODY=_body(edited, "web"))
    proc = _run_helper("check", "web", "--settings-file", "-", env=env, stdin=json.dumps(edited))
    assert proc.returncode == 0, proc.stdout
    proc = _run_helper("check", "web", env=env)  # the repo file still expects 5
    assert proc.returncode == 1 and "restartPolicyMaxRetries" in proc.stdout


def test_check_without_railway_cannot_check(stub_env) -> None:
    env = dict(stub_env, PATH="/usr/bin:/bin")
    if shutil.which("railway", path=env["PATH"]):
        pytest.skip("a system railway CLI is installed in /usr/bin or /bin")
    proc = _run_helper("check", "web", env=env)
    assert proc.returncode == 2
    assert "CANNOT CHECK: the railway CLI is not on PATH" in proc.stdout


def test_deprecation_notice_is_reported(settings, stub_env) -> None:
    notice = "Config as Code (railway.json / railway.toml) is deprecated. Run `railway config migrate`."
    env = dict(stub_env, STUB_BODY=_body(settings, "web"), STUB_STDERR=notice)
    proc = _run_helper("check", "web", env=env)
    assert proc.returncode == 0, proc.stdout
    assert f"warning   the railway CLI printed a deprecation notice: {notice}" in proc.stdout
