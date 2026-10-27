"""Home-lane runner (P15A.2): guards refuse, the dry run lists the roster, no hardcoded paths."""
from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
LANE_DIR = REPO / "deploy" / "home_lane"
GOOD_DSN = "postgresql://user:s3cret@db.example.com:5432/cars"


@pytest.fixture()
def clone(tmp_path):
    """A throwaway tree with the runner at deploy/home_lane/ and a two-id roster."""
    root = tmp_path / "lane-clone"
    (root / "deploy" / "home_lane").mkdir(parents=True)
    (root / "deploy" / "railway").mkdir(parents=True)
    shutil.copy(LANE_DIR / "run_home_lane.sh", root / "deploy" / "home_lane")
    (root / "deploy" / "railway" / "home_lane_dealers.txt").write_text(
        "# evidence comment\nhome1-com  # 403 from Railway 2026-11-02, ok from home 2026-11-03\nhome2-com\nbanned-com\n")
    (root / "deploy" / "railway" / "do_not_scan.txt").write_text("banned-com # no\n")
    (root / ".prod_lane_clone").write_text("")
    return root


def _run(root, *args, dsn=GOOD_DSN, env_file=None, extra=None):
    env = {"PATH": os.environ["PATH"], "HOME": str(root.parent / "home")}
    if dsn is not None:
        env["HOME_LANE_DATABASE_URL"] = dsn
    if env_file is not None:
        env["HOME_LANE_ENV_FILE"] = str(env_file)
    env.update(extra or {})
    return subprocess.run(["bash", str(root / "deploy" / "home_lane" / "run_home_lane.sh"), *args],
                          capture_output=True, text=True, env=env, timeout=60)


def test_scripts_parse():
    for name in ("run_home_lane.sh", "install_launchd.sh"):
        assert subprocess.run(["bash", "-n", str(LANE_DIR / name)]).returncode == 0


def test_dry_run_lists_roster_minus_do_not_scan_and_hides_credentials(clone):
    r = _run(clone, "--dry-run")
    assert r.returncode == 0, r.stderr
    assert "home1-com" in r.stdout and "home2-com" in r.stdout
    assert "banned-com" not in r.stdout
    assert "db.example.com" in r.stdout
    assert "s3cret" not in r.stdout + r.stderr


@pytest.mark.parametrize("dsn", [
    "postgresql://u:p@localhost:5432/cars",
    "postgresql://u:p@127.0.0.1/cars",
    "postgresql://u:p@[::1]:5432/cars",
    "postgresql://u:p@db.example.com:15432/cars",
])
def test_refuses_local_targets(clone, dsn):
    r = _run(clone, "--dry-run", dsn=dsn)
    assert r.returncode == 2 and "REFUSED" in r.stderr
    assert "p@" not in r.stdout + r.stderr


def test_allow_local_override(clone):
    r = _run(clone, "--dry-run", dsn="postgresql://u:p@localhost/cars", extra={"HOME_LANE_ALLOW_LOCAL": "1"})
    assert r.returncode == 0, r.stderr


def test_refuses_main_checkout_without_marker(clone):
    (clone / ".prod_lane_clone").unlink()
    r = _run(clone, "--dry-run")
    assert r.returncode == 2 and "prod-lane clone" in r.stderr


def test_refuses_world_readable_env_file(clone, tmp_path):
    f = tmp_path / "home_lane.env"
    f.write_text(f"HOME_LANE_DATABASE_URL={GOOD_DSN}\n")
    f.chmod(0o644)
    r = _run(clone, "--dry-run", dsn=None, env_file=f)
    assert r.returncode == 2 and "readable" in r.stderr
    f.chmod(0o600)
    r = _run(clone, "--dry-run", dsn=None, env_file=f)
    assert r.returncode == 0, r.stderr


def test_refuses_without_any_dsn(clone, tmp_path):
    r = _run(clone, "--dry-run", dsn=None, env_file=tmp_path / "missing.env")
    assert r.returncode == 2


def test_refuses_env_file_inside_repo(clone):
    f = clone / "home_lane.env"
    f.write_text(f"HOME_LANE_DATABASE_URL={GOOD_DSN}\n")
    f.chmod(0o600)
    r = _run(clone, "--dry-run", dsn=None, env_file=f)
    assert r.returncode == 2 and "inside the repo" in r.stderr


def test_runner_exports_the_lane_environment():
    text = (LANE_DIR / "run_home_lane.sh").read_text()
    for needle in ('SCANNER_EGRESS_TAG=""', 'SCANNER_HTTP_PROXY=""', 'SCANNER_ALLOW_BROWSER=""',
                   "PROJECT_DOTENV_DISABLE=1", "SCAN_LANE=home", "SCAN_POST_STEPS=0",
                   "${HOME_LANE_SHARDS:-2}", "--no-discover"):
        assert needle in text, needle


def test_install_renders_template_without_loading(clone, tmp_path):
    shutil.copy(LANE_DIR / "install_launchd.sh", clone / "deploy" / "home_lane")
    shutil.copy(LANE_DIR / "com.sarraficars.home-lane.plist.template", clone / "deploy" / "home_lane")
    agents = tmp_path / "agents"
    r = subprocess.run(["bash", str(clone / "deploy" / "home_lane" / "install_launchd.sh")],
                       capture_output=True, text=True, timeout=60,
                       env={"PATH": os.environ["PATH"], "HOME": "/home/x", "HOME_LANE_AGENT_DIR": str(agents)})
    assert r.returncode == 0, r.stderr
    out = (agents / "com.sarraficars.home-lane.plist").read_text()
    assert "__" not in out and str(clone) in out and "<integer>2</integer>" in out
    assert "not loaded" in r.stdout


def test_no_hardcoded_user_paths():
    for p in LANE_DIR.iterdir():
        assert "/Users/" not in p.read_text(), p.name
