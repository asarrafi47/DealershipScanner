"""VDP gallery hang guards (network drain + JS evaluate timeouts)."""

from __future__ import annotations

import asyncio

import pytest

from backend.scanner.vdp import (
    GALLERY_COLLECT_URLS_JS,
    _drain_pending_tasks,
    _vdp_gallery_loop_max_sec,
    _vdp_response_text_timeout_sec,
)


def test_drain_pending_tasks_cancels_stragglers():
    async def _run() -> None:
        async def slow() -> None:
            await asyncio.sleep(30)

        pending = [asyncio.create_task(slow())]
        await _drain_pending_tasks(pending, timeout_sec=0.15)
        assert pending == []

    asyncio.run(_run())


def test_gallery_js_avoids_background_style_scan():
    """Regression: background-image / getComputedStyle harvest pulled marketing tiles and hung carousel."""
    assert "getComputedStyle" not in GALLERY_COLLECT_URLS_JS
    assert "document.querySelectorAll(bsel)" not in GALLERY_COLLECT_URLS_JS
    assert "isLikelyJunkUrl" in GALLERY_COLLECT_URLS_JS


def test_gallery_loop_has_wall_clock_default():
    assert _vdp_gallery_loop_max_sec() >= 30.0
    assert _vdp_response_text_timeout_sec() >= 1.0
