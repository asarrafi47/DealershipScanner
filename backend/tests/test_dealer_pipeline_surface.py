"""Pins the public surface of ``backend.scripts.dealer_pipeline`` across the F11 split.

The module is the Railway fleet's production entrypoint (Dockerfile.scanner ->
scripts/railway_scan_fleet.sh -> fleet_scan -> ``python -m
backend.scripts.dealer_pipeline``) and a library for 15 import sites. Moving its
code into ``backend/scanner/pipeline/`` must not change: the names importable
from it, its argparse surface (option strings, defaults, help text), or the
subprocess command lines it builds (argv, cwd, env). No network, no real
subprocess: ``subprocess.run`` is a recorder.
"""
from __future__ import annotations

import argparse
import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

from backend.scripts import dealer_pipeline as dp
from backend.tests.pipeline_patch import patch_pipeline

REPO_ROOT = Path(__file__).resolve().parents[2]

# Every top-level name the pre-split module defined or imported for others to
# use. In-repo users: tests (assess, reconcile_dealer, route_verdict, ...,
# monkeypatched stubs), discovery_probe (dealers_from_db),
# unstale_host_blocked_recipes (_rows, get_conn, dealers_from_db).
SURFACE = [
    "load_project_dotenv", "get_conn", "ROOT", "LOG_ROOT", "PROCESS_DOC", "INCOMPLETE_FLOOR", "DISCREPANCY_FLOOR",
    "FIELD_FLOOR", "BASELINE_DAYS", "RECONCILE_MIN_SHARE", "ROW_FLOOR", "MIN_ROWS_UNKNOWN", "KEY_FIELDS",
    "SECONDARY_FIELDS", "HTTP_ONLY_ENV", "chromium_process_count", "wait_for_db", "_assess_conn", "_conn_alive",
    "_rows", "load_manifest_dealers", "dealers_from_db", "dealer_from_manifest", "ensure_recipe",
    "_default_lock_path", "LOCK_FILE", "_lock_holder_alive", "run_discovery_capture", "wait_for_scanner_lock",
    "run_http_only_scan", "vpic_for_dealers", "_log_append", "_log_write_once", "log_discovery", "_pct",
    "_one_condition_ok", "reconcile_dealer", "log_scan_run", "write_instructions_if_first_success",
    "record_timing", "_timing_text", "VIN_OWNER_CONFLICT_REASON_MIN", "assess", "_assess", "verify_accuracy",
    "LIFECYCLE_VERDICTS", "SCANNED_VERDICTS", "FAILING_VERDICTS", "LIFECYCLE_OK", "_AUTH_ERROR_MARKERS",
    "_NO_URL_SYNTHS", "_scan_hints", "_set_scan_hints", "_learning_append", "route_verdict",
    "lifecycle_attempted_today", "validate_live_recipes", "_lifecycle_block", "run_lifecycle",
    "scan_retry_batch", "run_lifecycle_pass", "triage_table", "platform_cluster_lines", "write_needs_discovery",
    "main", "run", "write_slow_dealers",
]

CONSTANTS = {
    "PROCESS_DOC": "docs/NETWORK_SCAN_PROCESS.md",
    "INCOMPLETE_FLOOR": 0.05, "DISCREPANCY_FLOOR": 0.05, "FIELD_FLOOR": 0.90, "BASELINE_DAYS": 30,
    "RECONCILE_MIN_SHARE": 0.60, "ROW_FLOOR": 0.50, "MIN_ROWS_UNKNOWN": 20,
    "KEY_FIELDS": ("price", "trim", "exterior_color"),
    "SECONDARY_FIELDS": ("interior_color", "engine_description", "transmission", "drivetrain", "fuel_type",
                         "body_style", "description", "stock_number", "msrp", "gallery_8plus"),
    "HTTP_ONLY_ENV": {"SCANNER_HTTP_ONLY": "1", "SCANNER_VDP_DB_MERGE": "0"},
    "VIN_OWNER_CONFLICT_REASON_MIN": 10,
    "LIFECYCLE_VERDICTS": ("no_recipe", "no_rows"), "SCANNED_VERDICTS": ("ok", "thin", "inaccurate"),
    "FAILING_VERDICTS": ("no_recipe", "no_rows", "error"), "LIFECYCLE_OK": ("resynth_ok", "capture_ok"),
    "_AUTH_ERROR_MARKERS": ("401", "403", "auth", "forbidden", "unauthori"),
    "_NO_URL_SYNTHS": ("not_in_manifest_or_db",),
}

# (option strings, dest, default, type, action class, required, help)
ARGS = [
    (("--dealers",), "dealers", "", None, "_StoreAction", False, "comma-separated dealer ids"),
    (("--manifest",), "manifest", str(REPO_ROOT / "dealers.json"), None, "_StoreAction", False, None),
    (("--limit",), "limit", 0, int, "_StoreAction", False, "first N manifest dealers (after sharding)"),
    (("--shard-index",), "shard_index", 0, int, "_StoreAction", False, None),
    (("--shard-count",), "shard_count", 1, int, "_StoreAction", False, None),
    (("--out",), "out", None, None, "_StoreAction", True, "output directory"),
    (("--batch",), "batch", 4, int, "_StoreAction", False, "dealers per scanner process (= dealer concurrency)"),
    (("--scan-timeout",), "scan_timeout", 3600, int, "_StoreAction", False, "seconds per scanner batch"),
    (("--lock-wait",), "lock_wait", 5400, int, "_StoreAction", False, "seconds to wait for another scanner run to finish"),
    (("--skip-scan",), "skip_scan", False, None, "_StoreTrueAction", False, "assess the latest run since --since instead of scanning"),
    (("--since",), "since", "", None, "_StoreAction", False, "with --skip-scan: ISO timestamp of the run to assess"),
    (("--force-synth",), "force_synth", False, None, "_StoreTrueAction", False, "re-synthesize recipes even when one exists"),
    (("--no-vpic",), "no_vpic", False, None, "_StoreTrueAction", False, None),
    (("--no-reconcile",), "no_reconcile", False, None, "_StoreTrueAction", False, "report but do not retire rows the run did not return"),
    (("--no-discover",), "no_discover", False, None, "_StoreTrueAction", False,
     "do not run the discovery browser capture for dealers whose recipe synthesis failed (default: run it once per dealer per day, in its own process)"),
    (("--no-lifecycle",), "no_lifecycle", False, None, "_StoreTrueAction", False,
     "skip the recipe lifecycle after the main batches (force re-synth -> discovery capture -> retry batch for no_recipe / no_rows / stale / rejected dealers; once per dealer per day)"),
]


def test_every_used_name_is_importable_from_the_facade():
    missing = [n for n in SURFACE if not hasattr(dp, n)]
    assert missing == []
    for name, value in CONSTANTS.items():
        assert getattr(dp, name) == value, name


def test_paths_resolve_to_the_repo_root():
    assert dp.ROOT == REPO_ROOT
    assert (dp.ROOT / "scanner.py").is_file()


def test_from_imports_used_by_scripts_still_work():
    from backend.scripts.dealer_pipeline import _rows, dealers_from_db, get_conn as pipeline_conn  # noqa: F401
    from backend.db.inventory_db import get_conn

    assert pipeline_conn is get_conn


class _Captured(Exception):
    pass


def _parser(monkeypatch) -> argparse.ArgumentParser:
    seen: list[argparse.ArgumentParser] = []

    def _capture(self, *a, **k):
        seen.append(self)
        raise _Captured

    monkeypatch.setattr(argparse.ArgumentParser, "parse_args", _capture)
    with pytest.raises(_Captured):
        dp.main()
    return seen[0]


def test_argparse_surface(monkeypatch):
    ap = _parser(monkeypatch)
    assert ap.description == dp.__doc__
    assert ap.formatter_class is argparse.RawDescriptionHelpFormatter
    got = [(tuple(a.option_strings), a.dest, a.default, a.type, type(a).__name__, a.required, a.help)
           for a in ap._actions if a.dest != "help"]
    assert got == ARGS


def test_docstring_names_the_steps():
    doc = dp.__doc__ or ""
    assert doc.startswith("Mass HTTP-only dealer pipeline: recipe -> scan -> NHTSA heal -> assess -> triage.")
    assert "python -m backend.scripts.dealer_pipeline --dealers a,b,c --out workspace/pipeline/run1" in doc


def _py() -> str:
    venv = REPO_ROOT / ".venv" / "bin" / "python"
    return str(venv) if venv.exists() else sys.executable


def test_http_only_scan_command_line(monkeypatch, tmp_path):
    calls = []

    def _run(cmd, **kw):
        calls.append((cmd, kw))
        return subprocess.CompletedProcess(cmd, 3)

    monkeypatch.setattr(subprocess, "run", _run)
    monkeypatch.delenv("DEALERS_FROM_SCANNABLE", raising=False)
    rc = dp.run_http_only_scan(["a-com", "b-com"], concurrency=2, log_path=tmp_path / "logs" / "scanner.log", timeout_sec=77)
    assert rc == 3
    (cmd, kw), = calls
    assert cmd == [_py(), "scanner.py", "--dealer-id", "a-com,b-com", "--dealer-concurrency", "2", "--scan-only"]
    assert kw["cwd"] == str(REPO_ROOT)
    assert kw["timeout"] == 77 and kw["stderr"] is subprocess.STDOUT
    env = kw["env"]
    assert env["SCANNER_HTTP_ONLY"] == "1" and env["SCANNER_VDP_DB_MERGE"] == "0" and env["DEALERS_FROM_SCANNABLE"] == "1"


def test_discovery_capture_command_line(monkeypatch, tmp_path):
    root = tmp_path / "dealer_logs"
    patch_pipeline(monkeypatch, "LOG_ROOT", root)
    real = importlib.util.find_spec
    monkeypatch.setattr(importlib.util, "find_spec", lambda name, *a: object() if name == "playwright" else real(name, *a))
    monkeypatch.setenv("SCANNER_ALLOW_BROWSER", "1")
    calls = []

    def _run(cmd, **kw):
        calls.append((cmd, kw))
        return subprocess.CompletedProcess(cmd, 0, stdout="one\ntwo\nthree\n", stderr="")

    monkeypatch.setattr(subprocess, "run", _run)
    out = dp.run_discovery_capture("cap-com", timeout_sec=123)
    (cmd, kw), = calls
    assert cmd == [_py(), "-m", "backend.scripts.discovery_probe", "--no-paths", "--browser-capture", "--dealers", "cap-com"]
    assert kw["cwd"] == str(REPO_ROOT) and kw["timeout"] == 123 and kw["capture_output"] is True and kw["text"] is True
    assert kw["env"]["DEALER_LOGS_ROOT"] == str(root) and "SCANNER_ALLOW_BROWSER" not in kw["env"]
    assert out["rc"] == 0 and out["stdout_tail"] == ["two", "three"] and out["recipes_after"] == 0
    assert list((root / "cap-com").glob(".capture_*"))
    # second call the same UTC day: marker spent, no subprocess
    assert dp.run_discovery_capture("cap-com")["skipped"] == "already captured today"
    assert len(calls) == 1
