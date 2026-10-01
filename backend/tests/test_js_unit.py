"""Run the frontend helper unit tests (backend/tests/js/*.test.js) under pytest.

The JS tests are plain ``node:test`` files that load pure helper scripts from
frontend/static into a node:vm context with a tiny window/document stub; they
need only node (no npm install, no network). Skipped when node is not on PATH.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

JS_TEST_DIR = Path(__file__).resolve().parent / "js"


@pytest.mark.skipif(shutil.which("node") is None, reason="node not installed")
def test_js_unit_suite_passes() -> None:
    files = sorted(str(p) for p in JS_TEST_DIR.glob("*.test.js"))
    assert files, f"no *.test.js files under {JS_TEST_DIR}"
    proc = subprocess.run(
        [shutil.which("node"), "--test", *files],
        capture_output=True,
        text=True,
        timeout=120,
        cwd=str(JS_TEST_DIR),
    )
    assert proc.returncode == 0, proc.stdout[-6000:] + proc.stderr[-2000:]
