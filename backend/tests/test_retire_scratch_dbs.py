"""N28.5a: retire_scratch_dbs.sh guards, dry-run default and drop gates (psql/pg_dump/dropdb stubbed)."""

import os
import re
import stat
import subprocess
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[2] / "scripts" / "cleanroom"

_STUB = r"""#!/usr/bin/env bash
echo "$(basename "$0") $*" >> "$STUB_LOG"
case "$(basename "$0")" in
  psql)
    all="$*"
    if [[ "$all" == *pg_database_size* ]]; then printf '%s\n' "$STUB_DBS"
    elif [[ "$all" == *"count(*) FROM pg_database"* ]]; then echo 0
    elif [[ "$all" == *"-d cars_draft_retire_chk"* ]]; then printf '%s\n' "${STUB_COUNTS_RESTORED:-$STUB_COUNTS}"
    else printf '%s\n' "$STUB_COUNTS"; fi;;
  pg_dump) while [ $# -gt 0 ]; do [ "$1" = -f ] && echo dumpbytes > "$2"; shift; done;;
esac
exit 0
"""


@pytest.fixture
def sb(tmp_path):
    bindir = tmp_path / "bin"
    bindir.mkdir()
    for n in ("psql", "pg_dump", "pg_restore", "createdb", "dropdb"):
        p = bindir / n
        p.write_text(_STUB)
        p.chmod(p.stat().st_mode | stat.S_IXUSR)
    log = tmp_path / "log"
    log.write_text("")
    backups = tmp_path / "backups"
    env = {
        **os.environ,
        "PATH": f"{bindir}:{os.environ['PATH']}",
        "STUB_LOG": str(log),
        "STUB_DBS": "cars_cleanroom|900\ncars_cr_x|100\ncars_rehearsal|500",
        "STUB_COUNTS": "cars=5\ndealerships=2",
        "RETIRE_BACKUP_DIR": str(backups),
        "RETIRE_DECISIONS_LOG": str(tmp_path / "decisions.md"),
    }

    def run(*args, **extra):
        return subprocess.run(
            [str(SCRIPTS / "retire_scratch_dbs.sh"), *args],
            env={**env, **extra}, capture_output=True, text=True, timeout=60,
        )

    return {"run": run, "log": log, "backups": backups, "tmp": tmp_path}


def _calls(sb, prog):
    return [ln for ln in sb["log"].read_text().splitlines() if ln.startswith(prog + " ")]


def test_name_regex_matches_build_db():
    rx = r"\^\(cars_cr_\[a-z0-9_\]\+\|cars_rehearsal\)\$"
    assert re.search(rx, (SCRIPTS / "build_db.sh").read_text())
    assert re.search(rx, (SCRIPTS / "retire_scratch_dbs.sh").read_text())


def test_list_shows_sizes_and_gates(sb):
    p = sb["run"]("list")
    assert p.returncode == 0, p.stderr
    assert "cars_cr_x" in p.stdout and "100" in p.stdout and "none" in p.stdout
    assert _calls(sb, "dropdb") == []


@pytest.mark.parametrize("name", ["cars", "cars_prod", "foo", "cars_cr_BAD", "cars_draft_retire_chk"])
def test_guard_refuses_bad_names(sb, name):
    for cmd in ("dump", "drop"):
        p = sb["run"](cmd, "--db", name, "--execute")
        assert p.returncode == 2 and "REFUSED" in p.stderr
    assert _calls(sb, "dropdb") == [] and _calls(sb, "pg_dump") == []


def test_cleanroom_needs_logged_decision(sb):
    p = sb["run"]("dump", "--db", "cars_cleanroom", "--execute")
    assert p.returncode == 2 and "decisions.md" in p.stderr
    (sb["tmp"] / "decisions.md").write_text("- cars_cleanroom retire: approved\n")
    p = sb["run"]("dump", "--db", "cars_cleanroom")
    assert p.returncode == 0 and "dry run" in p.stdout


def test_dump_dry_run_does_nothing(sb):
    p = sb["run"]("dump", "--db", "cars_cr_x")
    assert p.returncode == 0 and "dry run" in p.stdout
    assert _calls(sb, "pg_dump") == [] and not sb["backups"].exists()


def test_dump_execute_verifies_and_writes_marker(sb):
    p = sb["run"]("dump", "--db", "cars_cr_x", "--execute")
    assert p.returncode == 0, p.stderr
    dumps = list(sb["backups"].glob("scratch_cars_cr_x_*.dump"))
    assert len(dumps) == 1 and (Path(str(dumps[0]) + ".verified")).exists()
    assert "cars=5" in Path(str(dumps[0]) + ".verified").read_text()
    # the only dropdb is the throwaway check DB
    assert _calls(sb, "dropdb") == [_calls(sb, "dropdb")[0]] and "cars_draft_retire_chk" in _calls(sb, "dropdb")[0]
    assert "verified" in sb["run"]("list").stdout


def test_dump_count_mismatch_writes_no_marker(sb):
    p = sb["run"]("dump", "--db", "cars_cr_x", "--execute", STUB_COUNTS_RESTORED="cars=4\ndealerships=2")
    assert p.returncode != 0 and "FAILED" in p.stderr
    assert list(sb["backups"].glob("*.verified")) == []


def _verified_dump(sb):
    assert sb["run"]("dump", "--db", "cars_cr_x", "--execute").returncode == 0
    sb["log"].write_text("")


def test_drop_without_dump_or_ok_never_drops(sb):
    p = sb["run"]("drop", "--db", "cars_cr_x", "--execute")
    assert p.returncode == 0 and "NOT dropping" in p.stdout and "verified dump" in p.stdout
    assert _calls(sb, "dropdb") == []


def test_drop_needs_owner_flag_and_execute(sb):
    _verified_dump(sb)
    p = sb["run"]("drop", "--db", "cars_cr_x", "--execute")
    assert "owner OK flag" in p.stdout and _calls(sb, "dropdb") == []
    (sb["backups"] / "retire_ok").mkdir()
    (sb["backups"] / "retire_ok" / "cars_cr_x").write_text("ok")
    p = sb["run"]("drop", "--db", "cars_cr_x")
    assert "unmet: --execute" in p.stdout and _calls(sb, "dropdb") == []


def test_drop_all_gates_met_drops_only_named_db(sb):
    _verified_dump(sb)
    (sb["backups"] / "retire_ok").mkdir()
    (sb["backups"] / "retire_ok" / "cars_cr_x").write_text("ok")
    p = sb["run"]("drop", "--db", "cars_cr_x", "--execute")
    assert p.returncode == 0, p.stderr
    assert _calls(sb, "dropdb") == ["dropdb -h /tmp -p 5432 cars_cr_x"]


def test_tampered_dump_blocks_drop(sb):
    _verified_dump(sb)
    (sb["backups"] / "retire_ok").mkdir()
    (sb["backups"] / "retire_ok" / "cars_cr_x").write_text("ok")
    next(sb["backups"].glob("scratch_cars_cr_x_*.dump")).write_text("changed")
    p = sb["run"]("drop", "--db", "cars_cr_x", "--execute")
    assert "verified dump" in p.stdout and _calls(sb, "dropdb") == []


def test_prefix_named_db_dump_does_not_satisfy_gate(sb):
    stubs = {"STUB_DBS": "cars_cr_a|1\ncars_cr_a_b|2"}
    assert sb["run"]("dump", "--db", "cars_cr_a_b", "--execute", **stubs).returncode == 0
    (sb["backups"] / "retire_ok").mkdir()
    (sb["backups"] / "retire_ok" / "cars_cr_a").write_text("ok")
    sb["log"].write_text("")
    p = sb["run"]("drop", "--db", "cars_cr_a", "--execute", **stubs)
    assert "verified dump" in p.stdout and _calls(sb, "dropdb") == []
    assert "unverified" not in sb["run"]("list", **stubs).stdout.split("cars_cr_a ")[1].split("\n")[0]


def test_marker_for_other_db_rejected(sb):
    _verified_dump(sb)
    d = next(sb["backups"].glob("scratch_cars_cr_x_*.dump"))
    m = Path(str(d) + ".verified")
    m.write_text(m.read_text().replace("db=cars_cr_x", "db=cars_cr_y"))
    (sb["backups"] / "retire_ok").mkdir()
    (sb["backups"] / "retire_ok" / "cars_cr_x").write_text("ok")
    p = sb["run"]("drop", "--db", "cars_cr_x", "--execute")
    assert "verified dump" in p.stdout and _calls(sb, "dropdb") == []


def test_row_count_drift_blocks_drop(sb):
    _verified_dump(sb)
    (sb["backups"] / "retire_ok").mkdir()
    (sb["backups"] / "retire_ok" / "cars_cr_x").write_text("ok")
    p = sb["run"]("drop", "--db", "cars_cr_x", "--execute", STUB_COUNTS="cars=6\ndealerships=2")
    assert "row counts differ" in p.stdout and _calls(sb, "dropdb") == []
