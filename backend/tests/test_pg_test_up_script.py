"""N37.1: scripts/pg_test_up.sh refuses to run next to real data (no server needed)."""

import os
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "pg_test_up.sh"


def _run(env_extra):
    env = {k: v for k, v in os.environ.items() if k not in ("TEST_PG_ADMIN_DSN", "PGDATABASE", "PGPORT")}
    env.update(env_extra)
    return subprocess.run(["bash", str(SCRIPT), "--", "true"], env=env, capture_output=True, text=True, timeout=60)


@pytest.mark.parametrize(
    "env",
    [
        {"TEST_PG_ADMIN_DSN": "postgresql://localhost/cars"},
        {"TEST_PG_ADMIN_DSN": "host=localhost port=15432 dbname=postgres"},
        {"PGDATABASE": "cars"},
        {"PGPORT": "15432"},
    ],
)
def test_script_refuses_protected_targets(env):
    r = _run(env)
    assert r.returncode == 1
    assert "refusing" in r.stderr
