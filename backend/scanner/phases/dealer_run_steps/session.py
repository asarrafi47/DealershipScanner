"""The scan's browser session: none under HTTP-only (the default).

A context + page open only when the orchestrator launched a browser, which it
does only with ``SCANNER_ALLOW_BROWSER=1`` (discovery captures); the page then
reaches the recovery chain's page strategies. Kept because that path is live.
"""
from __future__ import annotations

import logging
import os
from typing import Any

from backend.scanner.phases.dealer_run_steps.state import DealerRun
from backend.scanner.phases.nav import get_rotating_ua

logger = logging.getLogger("scanner")


def _http_only() -> bool:
    """No browser at all (the default): inventory comes from recipe replay, per-car
    data from VDP recipes + HTTP-first. Only ``SCANNER_ALLOW_BROWSER=1`` (discovery
    captures) turns this off — see backend/scanner/browser_gate.py."""
    from backend.scanner.browser_gate import http_only

    return http_only()


async def open_session(run: DealerRun, browser: Any) -> None:
    ctx_opts: dict[str, Any] = {"viewport": {"width": 1920, "height": 1080}}
    _ua = (os.environ.get("SCANNER_USER_AGENT") or "").strip() or get_rotating_ua()
    ctx_opts["user_agent"] = _ua
    if browser is not None and not _http_only():
        from backend.scanner.browser_gate import require_browser

        require_browser("dealer_run.new_context")
        logger.info("Warmup UA [%s]: %s", run.name, _ua[:130])
        run.context = await browser.new_context(**ctx_opts)
        run.page = await run.context.new_page()
    else:
        # HTTP-only: no context, no page. The recipe replay + HTTP-first detail
        # pass are the whole scan; recovery runs only its HTTP-safe strategies.
        run.page = None
    run.result["http_only"] = True
    # Warmup, dead-domain URL discovery, the maintenance-page probe and the
    # site profiler were browser phases; they live in discovery now
    # (backend/scanner/discovery_capture.py, docs/HTTP_ONLY_SCANS_PLAN.md).
