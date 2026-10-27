"""N11.1a: scripts/cleanroom/prep_conv_db.sh refusals and call shape, against stub
psql / pg_dump binaries (no Postgres, no network). The real run is owner-gated."""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
SCRIPTS = REPO / "scripts" / "cleanroom"
BASH = shutil.which("bash")
pytestmark = pytest.mark.skipif(BASH is None, reason="bash not available")

PSQL_STUB = r'''#!/bin/sh
echo "psql $*" >> "$STUB_LOG"
case "$*" in
  *shobj_description*) echo "$STUB_NOTE";;
  *"FROM pg_database WHERE datname="*)
    n=$(echo "$*" | sed "s/.*datname='\([a-z0-9_]*\)'.*/\1/")
    case " $STUB_EXISTS " in *" $n "*) echo 1;; *) echo 0;; esac;;
  *"WITH u AS"*) echo "$STUB_CARS";;
  *"count(*) FROM cars"*) echo "$STUB_CARS";;
  *"count(*) FROM public."*) echo 7;;
  *) cat >> "$STUB_LOG.stdin";;
esac
'''


@pytest.fixture
def sandbox(tmp_path: Path):
    repo = tmp_path / "repo"
    shutil.copytree(SCRIPTS, repo / "scripts" / "cleanroom")
    bindir = tmp_path / "bin"
    bindir.mkdir()
    (bindir / "psql").write_text(PSQL_STUB)
    (bindir / "pg_dump").write_text('#!/bin/sh\necho "pg_dump $*" >> "$STUB_LOG"\necho "-- dump"\n')
    for f in bindir.iterdir():
        f.chmod(0o755)
    log = tmp_path / "calls.log"
    env = {k: v for k, v in os.environ.items() if not k.startswith(("PG", "INVENTORY_", "CR_"))}
    env.update(PATH=f"{bindir}{os.pathsep}{env.get('PATH', '')}", GIT_CEILING_DIRECTORIES=str(tmp_path),
               STUB_LOG=str(log), STUB_EXISTS="cars cars_cleanroom cars_cr_conv", STUB_NOTE="", STUB_CARS="1234",
               CR_ALLOW_ART_OUTSIDE_RUNS="1")

    def run(*args, **over):
        e = dict(env, **over)
        return subprocess.run([BASH, str(repo / "scripts" / "cleanroom" / "prep_conv_db.sh"), *args],
                              capture_output=True, text=True, timeout=60, env=e)

    return {"run": run, "log": log, "tmp": tmp_path}


def _calls(log: Path) -> str:
    return log.read_text() if log.exists() else ""


@pytest.mark.parametrize("db", ["cars", "cars_cleanroom", "cars_rehearsal", "cars_cr_rc", "cars_cr_x", "mydb", ""])
def test_refuses_every_target_that_is_not_the_conv_db(sandbox, db):
    p = sandbox["run"]("--db", db, "--art", str(sandbox["tmp"] / "art"))
    assert p.returncode == 2 and "not a convergence DB" in p.stderr
    assert _calls(sandbox["log"]) == ""


def test_refuses_source_equal_target_and_bad_shift(sandbox):
    art = str(sandbox["tmp"] / "art")
    assert "same DB" in sandbox["run"]("--db", "cars_cr_conv", "--source", "cars_cr_conv", "--art", art).stderr
    assert "shift-days" in sandbox["run"]("--db", "cars_cr_conv", "--shift-days", "0", "--art", art).stderr
    assert _calls(sandbox["log"]) == ""


def test_refuses_art_outside_the_run_root_before_any_db_call(sandbox):
    p = sandbox["run"]("--db", "cars_cr_conv", "--art", str(sandbox["tmp"] / "art"), CR_ALLOW_ART_OUTSIDE_RUNS="")
    assert p.returncode == 2 and "CR_ALLOW_ART_OUTSIDE_RUNS" in p.stderr
    assert _calls(sandbox["log"]) == ""


def test_refuses_missing_db_second_run_and_empty_cars(sandbox):
    art = str(sandbox["tmp"] / "art")
    p = sandbox["run"]("--db", "cars_cr_conv", "--art", art, STUB_EXISTS="cars")
    assert p.returncode == 2 and "does not exist" in p.stderr
    p = sandbox["run"]("--db", "cars_cr_conv", "--art", art, STUB_NOTE="prep_conv_db: shifted 5 rows")
    assert p.returncode == 2 and "already prepared" in p.stderr
    p = sandbox["run"]("--db", "cars_cr_conv", "--art", art, STUB_CARS="0")
    assert p.returncode == 2 and "no cars rows" in p.stderr
    assert "COMMENT ON DATABASE" not in _calls(sandbox["log"]) and "UPDATE" not in _calls(sandbox["log"])


def test_happy_path_replaces_reference_tables_then_shifts_and_logs(sandbox):
    art = sandbox["tmp"] / "art"
    p = sandbox["run"]("--db", "cars_cr_conv", "--art", str(art))
    assert p.returncode == 0 and "PREP OK" in p.stdout, p.stdout + p.stderr
    calls = _calls(sandbox["log"])
    stdin = Path(str(sandbox["log"]) + ".stdin").read_text()
    # the table list comes from build_db.sh, the replace is one stream with the dump of the source
    build = (SCRIPTS / "build_db.sh").read_text()
    block = re.search(r'COPY_TABLES="([^"]+)"', build).group(1).split()
    assert "dealer_recipes" in block and len(block) > 10
    assert stdin.splitlines()[0] == "SET session_replication_role = replica;"
    for t in block:
        assert f"DELETE FROM public.{t};" in stdin
        assert f"-t public.{t}" in calls
    assert "-d cars --data-only" in calls and "-- dump" in stdin
    # the shift: 20 days, once, and the finished prep is recorded on the DB
    assert calls.count("scraped_at - interval '20 days'") == 1
    assert "COMMENT ON DATABASE cars_cr_conv IS 'prep_conv_db: shifted 1234 rows by 20 days" in calls
    assert not re.search(r"psql .*-d (cars|cars_cleanroom)\b[^_]", calls.replace("pg_dump", ""))
    info = json.loads((art / "build" / "prep_conv_db.json").read_text())
    assert info["scraped_at_rows_shifted"] == 1234 and info["scraped_at_shift_days"] == 20
    assert info["dealer_recipes_rows"] == 7 and info["source"] == "cars"
