"""Scan-loop tests: configuration (card L4). Request-schema tests are added by later cards."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from backend.scanner.loop import config as loop_config
from backend.scanner.loop.config import CEILINGS, DEFAULTS, LoopConfig, load_loop_config

REPO_ROOT = Path(__file__).resolve().parents[2]

EXAMPLE = (
    '{"schema":1,"enabled":true,"kinds":["probe","validate","rescan","apply"],"dealers_per_cycle":6,'
    '"cycles_per_day":4,"attempts_per_dealer":3,"applies_per_dealer_per_day":1,"cloud_run":{"dealers":8,'
    '"agents":20,"max_fixes":4,"workflows_at_once":1,"routines":3,"weekly_share_stop":0.6},'
    '"stale_after_hours":36}'
)


def test_config_defaults_match_the_card() -> None:
    assert DEFAULTS.enabled is True
    assert DEFAULTS.kinds == ("probe", "validate", "rescan", "apply")
    assert DEFAULTS.dealers_per_cycle == 6
    assert DEFAULTS.cycles_per_day == 4
    assert DEFAULTS.attempts_per_dealer == 3
    assert DEFAULTS.applies_per_dealer_per_day == 1
    assert DEFAULTS.stale_after_hours == 36
    cloud = DEFAULTS.cloud_run
    assert (cloud.dealers, cloud.agents, cloud.max_fixes) == (8, 20, 4)
    assert (cloud.workflows_at_once, cloud.routines, cloud.weekly_share_stop) == (1, 3, 0.6)
    assert DEFAULTS.idle is False


def test_config_ceilings_are_twice_the_defaults() -> None:
    assert CEILINGS == {
        "dealers_per_cycle": 12,
        "cycles_per_day": 8,
        "attempts_per_dealer": 6,
        "applies_per_dealer_per_day": 2,
    }


def test_config_fixed_limits() -> None:
    assert loop_config.MAX_REQUEST_BYTES == 4096
    assert loop_config.MAX_PROPOSAL_BYTES == 33792
    assert loop_config.MAX_STATUS_BYTES == 16384
    assert loop_config.MAX_CONFIG_BYTES == 1024
    assert loop_config.MAX_DEALERS_PER_REQUEST == 8
    assert loop_config.MAX_READ_FILES == 50
    assert loop_config.REASON_MAX_CHARS == 160
    assert loop_config.DIGEST_MAX_BYTES == 6144
    assert loop_config.DEALER_ID_RE.match("autosavvy-com")
    assert not loop_config.DEALER_ID_RE.match("-bad")
    assert not loop_config.DEALER_ID_RE.match("a" * 81)
    assert loop_config.ENV_DATA_TOKEN == "LOOP_DATA_TOKEN"


def test_config_example_from_bundle_equals_defaults() -> None:
    assert load_loop_config(EXAMPLE) == DEFAULTS
    assert load_loop_config(EXAMPLE.encode()) == DEFAULTS


def test_config_is_frozen() -> None:
    with pytest.raises(Exception):
        DEFAULTS.enabled = False  # type: ignore[misc]


def test_config_clamps_above_ceiling_and_below_zero() -> None:
    assert load_loop_config('{"dealers_per_cycle": 99}').dealers_per_cycle == 12
    assert load_loop_config('{"dealers_per_cycle": -1}').dealers_per_cycle == 0
    assert load_loop_config('{"dealers_per_cycle": -1}').enabled is True
    assert load_loop_config('{"cycles_per_day": 9}').cycles_per_day == 8
    assert load_loop_config('{"attempts_per_dealer": 7}').attempts_per_dealer == 6
    assert load_loop_config('{"applies_per_dealer_per_day": 3}').applies_per_dealer_per_day == 2
    assert load_loop_config('{"dealers_per_cycle": 12}').dealers_per_cycle == 12


@pytest.mark.parametrize(
    "raw",
    [
        '{"dealers_per_cycle": true}',
        '{"cycles_per_day": "4"}',
        '{"dealers_per_cycle": 2.5}',
        '{"enabled": "yes"}',
        '{"kinds": "probe"}',
        '{"kinds": ["probe", "delete"]}',
        '{"kinds": ["probe", 3]}',
        '{"cloud_run": []}',
        '{"cloud_run": {"dealers": true}}',
        '{"cloud_run": {"weekly_share_stop": "0.6"}}',
        '{"stale_after_hours": "36"}',
        '{"dealers_per_cycle": NaN}',
        "",
        "   ",
        "{not json",
        "[1, 2]",
        '"text"',
        "42",
        "null",
    ],
)
def test_config_wrong_type_or_unreadable_gives_disabled(raw: str) -> None:
    cfg = load_loop_config(raw)
    assert cfg.enabled is False
    assert cfg.idle is True


def test_config_missing_file_gives_disabled() -> None:
    cfg = load_loop_config(None)
    assert cfg.enabled is False
    assert cfg.idle is True


def test_config_non_text_input_gives_disabled() -> None:
    assert load_loop_config(123).enabled is False  # type: ignore[arg-type]
    assert load_loop_config(b"\xff\xfe").enabled is False


def test_config_oversize_file_gives_disabled() -> None:
    pad = " " * (1025 - len('{"enabled":true}'))
    over = '{"enabled":true}' + pad
    assert len(over.encode()) == 1025
    assert load_loop_config(over).enabled is False
    exact = over[:1024]
    assert len(exact.encode()) == 1024
    assert load_loop_config(exact).enabled is True


def test_config_unknown_keys_are_ignored() -> None:
    cfg = load_loop_config('{"unlimited": true, "dealers_per_cycle": 5, "cloud_run": {"future": 1}}')
    assert cfg.enabled is True
    assert cfg.dealers_per_cycle == 5
    assert cfg.cloud_run == DEFAULTS.cloud_run


def test_config_disabled_flag_is_idle() -> None:
    cfg = load_loop_config('{"enabled": false}')
    assert cfg.enabled is False
    assert cfg.idle is True


def test_config_kinds_subset_is_kept_in_canonical_order() -> None:
    assert load_loop_config('{"kinds": ["rescan", "probe"]}').kinds == ("probe", "rescan")
    assert load_loop_config('{"kinds": []}').kinds == ()


def test_config_cloud_run_values_are_read_and_bounded() -> None:
    cfg = load_loop_config('{"cloud_run": {"dealers": 5, "weekly_share_stop": 7, "agents": -3}}')
    assert cfg.cloud_run.dealers == 5
    assert cfg.cloud_run.weekly_share_stop == 1.0
    assert cfg.cloud_run.agents == 0
    assert cfg.cloud_run.max_fixes == 4


def test_config_idle_result_never_looks_unlimited() -> None:
    cfg = load_loop_config("{broken")
    assert isinstance(cfg, LoopConfig)
    assert cfg.dealers_per_cycle <= CEILINGS["dealers_per_cycle"]


def test_config_does_not_mutate_or_depend_on_input_object() -> None:
    raw = json.dumps({"dealers_per_cycle": 3})
    assert load_loop_config(raw) == load_loop_config(raw)


def test_loop_package_imports_only_the_standard_library() -> None:
    code = (
        "import sys\n"
        "sys.path.insert(0, sys.argv[1])\n"
        "before = set(sys.modules)\n"
        "import backend.scanner.loop, backend.scanner.loop.config\n"
        "new = sorted(set(sys.modules) - before)\n"
        "std = set(sys.stdlib_module_names) | set(sys.builtin_module_names)\n"
        "bad = [m for m in new if m.split('.')[0] not in std and not m.startswith('backend')]\n"
        "assert not bad, bad\n"
        "extra = [m for m in new if m.startswith('backend') and not m.startswith('backend.scanner.loop')"
        " and m not in ('backend', 'backend.scanner')]\n"
        "assert not extra, extra\n"
    )
    proc = subprocess.run(
        [sys.executable, "-I", "-c", code, str(REPO_ROOT)], capture_output=True, text=True, timeout=60
    )
    assert proc.returncode == 0, proc.stderr[-500:]


def test_loop_config_source_reads_no_environment_or_clock() -> None:
    import ast

    tree = ast.parse((REPO_ROOT / "backend/scanner/loop/config.py").read_text(encoding="utf-8"))
    imported = set()
    attrs = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(a.name.split(".")[0] for a in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.add((node.module or "").split(".")[0])
        elif isinstance(node, ast.Attribute):
            attrs.add(node.attr)
    assert not imported & {"os", "time", "datetime", "sys", "pathlib", "subprocess"}
    assert not attrs & {"environ", "getenv", "now", "utcnow", "time", "open", "read_text"}
