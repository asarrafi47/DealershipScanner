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


# Source-level guards for page scripts that are IIFEs over the live DOM (not loadable in
# the node:vm helper harness).
STATIC = Path(__file__).resolve().parents[2] / "frontend" / "static"
TEMPLATES = Path(__file__).resolve().parents[2] / "frontend" / "templates"


def test_sticker_onerror_hides_preview_only() -> None:
    """B-038: a failed sticker image must not hide the wrap holding parsed options."""
    src = (STATIC / "car_packages.js").read_text()
    body = src.split("stickerImg.onerror = function () {", 1)[1].split("};", 1)[0]
    assert "hideWindowStickerBlock" not in body
    assert "done(false)" in body


def test_listings_rerender_keeps_page_for_poll_and_deep_link() -> None:
    """B-039: poll refetch and ?page=N boot render must not reset to page 1."""
    src = (STATIC / "main.js").read_text()
    core = src.split("function renderResultsNowCore(opts)", 1)[1].split("let _filterRenderFrame", 1)[0]
    assert "opts.keepPage" in core and "_bootKeepPage" in core
    assert "resetPage: true" in core  # filter changes still reset
    assert "_bootKeepPage = _listingsPage > 1" in src
    assert src.count("__DS_runFilterRenderKeepPage()") == 2  # both inventory polls


def test_depreciation_card_gated_on_real_price() -> None:
    """B-040: no invented $20,000 forecast for call-for-price / payment-listed cars."""
    html = (TEMPLATES / "car.html").read_text()
    head, rest = html.split('<div id="depreciation-forecast-card"', 1)
    assert "{% if car.price and car.price > 0 and not car.payment_listed %}" in head[-400:]
    assert "20000" not in rest.split("</div>", 1)[0]
    assert "{% endif %}" in rest.split("</ul>", 1)[1][:120]
