"""Regression: backend.scraping.cli must import through the package path.

Bare ``from scraping...`` / ``from intelligence...`` imports only resolve when
``backend/`` is on sys.path, so they raised ModuleNotFoundError under
``python -m backend.scraping`` (audit 2026-10-01, B12).
"""
from __future__ import annotations

import ast
import importlib
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRAPING_DIR = REPO_ROOT / "backend" / "scraping"


def _imported_modules(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return [
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module and node.level == 0
    ]


def test_cli_and_fixture_tests_use_package_imports():
    for name in ("cli.py", "fixture_tests.py"):
        for module in _imported_modules(SCRAPING_DIR / name):
            top = module.split(".")[0]
            assert top not in {"scraping", "intelligence"}, f"{name}: bare import {module}"


def test_cli_lazy_imports_resolve():
    for module in _imported_modules(SCRAPING_DIR / "cli.py"):
        if module.startswith("backend."):
            importlib.import_module(module)


def test_fixture_test_runs_as_module():
    proc = subprocess.run(
        [sys.executable, "-m", "backend.scraping", "--fixture-test"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert proc.returncode == 0, proc.stderr
    assert "all passed" in proc.stdout
