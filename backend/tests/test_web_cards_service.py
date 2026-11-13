"""The web-cards Railway service: the nightly listings-card builder (code and docs only).

Static checks in the style of test_railway_service_settings.py; nothing here reaches Railway,
a database or the network. The deploy script's behavior against a fake railway is covered by
test_deploy_guard.py (it runs for deploy_web_cards.sh too); the checks below pin what is
specific to this service:

- ``deploy/railway/railway.web-cards.json`` is the declared service: a cron job built from
  Dockerfile.web, started with the builder module, 08:30 UTC daily, no health check, no
  restart loop;
- ``service_settings.json`` carries the same values for the guard, without a Railway id;
- ``deploy_web_cards.sh`` deploys the SAME tag as the web service;
- the container entrypoint runs a start command instead of the web server;
- docs/RELEASING.md names the owner steps and the log line to look for;
- nothing new carries a secret, a database address or a Railway id.
"""

from __future__ import annotations

import importlib
import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
DEPLOY = REPO / "deploy" / "railway"
CONFIG = DEPLOY / "railway.web-cards.json"
SETTINGS = DEPLOY / "service_settings.json"
SCRIPT = DEPLOY / "deploy_web_cards.sh"
ENTRYPOINT = REPO / "scripts" / "docker-entrypoint-web.sh"
RELEASING = REPO / "docs" / "RELEASING.md"
START = "python -m backend.scripts.build_listings_grid_cards"
_UUID = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"


@pytest.fixture(scope="module")
def config() -> dict:
    return json.loads(CONFIG.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def entry() -> dict:
    return json.loads(SETTINGS.read_text(encoding="utf-8"))["services"]["web-cards"]


# ── the declared service ────────────────────────────────────────────────────────────


def test_config_is_a_cron_job_built_from_dockerfile_web(config) -> None:
    assert config["build"] == {"builder": "DOCKERFILE", "dockerfilePath": "Dockerfile.web"}
    assert (REPO / "Dockerfile.web").is_file()
    assert config["deploy"]["startCommand"] == START
    assert config["deploy"]["numReplicas"] == 1


def test_config_runs_daily_at_0830_utc(config) -> None:
    fields = config["deploy"]["cronSchedule"].split()
    assert fields == ["30", "8", "*", "*", "*"], "minute 30, hour 8, every day (Railway cron is UTC)"


def test_config_has_no_health_check_and_no_restart_loop(config) -> None:
    deploy = config["deploy"]
    assert not any(k.startswith("healthcheck") for k in deploy), "a cron job exits; a health check would fail it"
    assert not any(k.startswith("healthcheck") for k in config["build"])
    assert deploy["restartPolicyType"] == "NEVER"
    assert "restartPolicyMaxRetries" not in deploy


def test_config_matches_the_settings_the_guard_compares(config, entry) -> None:
    """The reference file and service_settings.json cannot drift apart."""
    inst = entry["serviceInstance"]
    assert inst["dockerfilePath"] == config["build"]["dockerfilePath"]
    for key in ("startCommand", "cronSchedule", "restartPolicyType", "numReplicas"):
        assert inst[key] == config["deploy"][key], key
    assert inst["healthcheckPath"] is None
    assert inst["railwayConfigFile"] is None, "Railway stops reading config files on 2026-12-01"
    assert entry["replaces"] == "deploy/railway/railway.web-cards.json"


def test_settings_entry_has_no_railway_id(entry) -> None:
    assert "serviceId" not in entry
    assert entry["serviceIdEnv"] == "RAILWAY_WEB_CARDS_SERVICE_ID"


def test_start_command_names_a_real_module_with_a_main_and_the_done_line() -> None:
    module_name = START.split()[-1]
    module = importlib.import_module(module_name)
    assert callable(module.main)
    source = Path(module.__file__).read_text(encoding="utf-8")
    assert 'print(f"done in ' in source, "the line the owner looks for in the Railway log"


def test_start_command_is_not_what_the_web_image_runs_by_default() -> None:
    assert "gunicorn" not in START
    assert 'ENTRYPOINT ["/app/scripts/docker-entrypoint-web.sh"]' in (REPO / "Dockerfile.web").read_text(encoding="utf-8")


# ── the entrypoint runs a command instead of the web server ─────────────────────────


@pytest.mark.skipif(shutil.which("sh") is None, reason="needs a POSIX sh")
@pytest.mark.parametrize(
    ("args", "expected"),
    [
        (["echo", "ran", "this"], "ran this"),
        (["echo one; echo two"], "one\ntwo"),  # a start command arrives as ONE string
    ],
)
def test_entrypoint_runs_a_given_command_and_skips_the_web_boot(args, expected, tmp_path) -> None:
    proc = subprocess.run(["sh", str(ENTRYPOINT), *args], capture_output=True, text=True, timeout=30, cwd=tmp_path)
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip() == expected
    # No vault check, no migration, no admin bootstrap, no gunicorn.
    assert "WARN" not in proc.stderr and "gunicorn" not in proc.stdout


def test_entrypoint_command_branch_comes_before_every_side_effect() -> None:
    text = ENTRYPOINT.read_text(encoding="utf-8")
    branch = text.index('if [ "$#" -gt 0 ]')
    for marker in ("load_kmac_vault_secrets", "backend.scripts.migrate", "bootstrap_site_admin", "exec gunicorn"):
        assert branch < text.index(marker), marker


def test_entrypoint_without_a_command_still_ends_in_gunicorn() -> None:
    text = ENTRYPOINT.read_text(encoding="utf-8")
    assert len(re.findall(r"^exec gunicorn ", text, re.MULTILINE)) == 1 and "backend.main:app" in text


# ── the deploy script ───────────────────────────────────────────────────────────────


def test_script_is_executable_and_uses_the_shared_guard() -> None:
    assert SCRIPT.stat().st_mode & 0o111
    text = SCRIPT.read_text(encoding="utf-8")
    assert '. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_guarded_deploy.sh"' in text
    assert "guarded_deploy deploy_web_cards.sh" in text
    assert 'SERVICE="${SERVICE:-web-cards}"' in text


def test_script_deploys_the_release_tag_like_the_web_script() -> None:
    """The card revision must match web's: both ship ``git archive v$(cat VERSION)`` (the guard)."""
    text = SCRIPT.read_text(encoding="utf-8")
    assert "SAME tagged release as the web service" in text
    lib = (DEPLOY / "_guarded_deploy.sh").read_text(encoding="utf-8")
    assert 'git -C "$repo" archive --format=tar "$ref"' in lib
    web = (DEPLOY / "deploy_web.sh").read_text(encoding="utf-8")
    assert "guarded_deploy deploy_web.sh web" in web


@pytest.mark.skipif(shutil.which("bash") is None, reason="needs bash")
@pytest.mark.parametrize("service", ["web", "scanner-nightly"])
def test_script_refuses_to_target_the_other_services(service) -> None:
    proc = subprocess.run(
        ["bash", str(SCRIPT), "--dry-run"],
        env={"PATH": "/usr/bin:/bin", "SERVICE": service, "HOME": "/nonexistent"},
        capture_output=True, text=True, timeout=30,
    )
    assert proc.returncode == 2
    assert f"refusing SERVICE={service}" in proc.stderr


def test_script_help_names_the_id_variable() -> None:
    assert "RAILWAY_WEB_CARDS_SERVICE_ID" in SCRIPT.read_text(encoding="utf-8")


# ── docs/RELEASING.md ───────────────────────────────────────────────────────────────


def _section() -> str:
    text = RELEASING.read_text(encoding="utf-8")
    start = text.index("## The card builder and the web-cards service")
    end = text.index("\n## ", start + 10)
    return text[start:end]


def test_releasing_says_to_run_the_builder_after_a_card_revision_change() -> None:
    section = _section()
    assert "python -m backend.scripts.build_listings_grid_cards" in section
    assert "CARD_REV" in section and "150 to 170 s" in section
    assert "08:30 UTC" in section


def test_releasing_lists_the_owner_steps_in_order() -> None:
    section = _section()
    needles = [
        "1. **Create the service.**",
        "2. **Copy the database variables from web with reference variables.**",
        "3. **Apply the service settings**",
        "4. **Deploy with the script**",
        "deploy/railway/deploy_web_cards.sh --dry-run",
        "5. **Trigger the first run**",
        "6. **Check the log**",
        "grep '^done in'",
    ]
    positions = [section.index(n) for n in needles]
    assert positions == sorted(positions), "the steps must appear in the order the owner does them"
    assert "${{Postgres." in section, "database variables are copied as reference variables, never as values"
    assert "RAILWAY_WEB_CARDS_SERVICE_ID" in section


def test_releasing_checklist_points_at_the_section() -> None:
    text = RELEASING.read_text(encoding="utf-8")
    assert "the-card-builder-and-the-web-cards-service" in text.split("## The card builder", 1)[0]


# ── nothing secret ──────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("path", [CONFIG, SCRIPT])
def test_new_files_carry_no_id_secret_or_database_address(path) -> None:
    text = path.read_text(encoding="utf-8")
    assert not re.search(_UUID, text)
    assert not re.search(r"postgres(ql)?://|rlwy\.net|railway\.internal|token|password|secret", text, re.IGNORECASE)


def test_the_releasing_section_carries_no_id_secret_or_database_address() -> None:
    section = _section()
    assert not re.search(_UUID, section)
    assert not re.search(r"postgres(ql)?://[^\s`]*@|rlwy\.net|[A-Za-z0-9]{32,}", section)


# ── the guard's settings check for this service ─────────────────────────────────────


def _helper():
    import importlib.util

    spec = importlib.util.spec_from_file_location("railway_service_settings_wc", DEPLOY / "service_settings.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_check_needs_the_id_from_the_environment_and_names_the_variable(monkeypatch) -> None:
    helper = _helper()
    settings = json.loads(SETTINGS.read_text(encoding="utf-8"))
    monkeypatch.delenv("RAILWAY_WEB_CARDS_SERVICE_ID", raising=False)
    with pytest.raises(helper.SettingsError, match="export RAILWAY_WEB_CARDS_SERVICE_ID="):
        helper.service_entry(settings, "web-cards")
    monkeypatch.setenv("RAILWAY_WEB_CARDS_SERVICE_ID", "not-a-uuid")
    with pytest.raises(helper.SettingsError, match="export RAILWAY_WEB_CARDS_SERVICE_ID="):
        helper.service_entry(settings, "web-cards")


def test_check_reads_the_exported_id_and_compares_eight_settings(monkeypatch) -> None:
    helper = _helper()
    settings = json.loads(SETTINGS.read_text(encoding="utf-8"))
    sid = "00000000-0000-4000-8000-0000000000c1"
    monkeypatch.setenv("RAILWAY_WEB_CARDS_SERVICE_ID", sid.upper())
    entry = helper.service_entry(settings, "web-cards")
    assert entry["serviceId"] == sid
    assert helper.query_variables(settings, entry)["serviceId"] == sid
    live = {"serviceInstance": {"serviceName": "web-cards", **entry["serviceInstance"]}}
    rows = helper.compare("web-cards", entry, live)
    assert len(rows) == 8 and all(r[3] for r in rows)
    assert "all 8 checked settings match" in RELEASING.read_text(encoding="utf-8")
    # One drifted setting is a mismatch: a restart loop, or a health check, would be caught.
    for field, bad in (("restartPolicyType", "ON_FAILURE"), ("cronSchedule", "0 * * * *"), ("healthcheckPath", "/health")):
        drifted = {"serviceInstance": {**live["serviceInstance"], field: bad}}
        assert [r[0] for r in helper.compare("web-cards", entry, drifted) if not r[3]] == [field]


def test_a_malformed_id_variable_name_is_refused() -> None:
    helper = _helper()
    settings = json.loads(SETTINGS.read_text(encoding="utf-8"))
    settings["services"]["web-cards"]["serviceIdEnv"] = "lower; rm -rf"
    with pytest.raises(helper.SettingsError, match="not a variable name"):
        helper.service_entry(settings, "web-cards")


def test_the_other_services_still_need_their_id_in_the_file() -> None:
    helper = _helper()
    settings = json.loads(SETTINGS.read_text(encoding="utf-8"))
    for name in ("web", "scanner-nightly"):
        assert re.fullmatch(_UUID, helper.service_entry(settings, name)["serviceId"])
        broken = json.loads(json.dumps(settings))
        del broken["services"][name]["serviceId"]
        with pytest.raises(helper.SettingsError, match="serviceId is not a UUID"):
            helper.service_entry(broken, name)
