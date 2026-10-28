"""The scan's session start: no browser, ever.

Scans are HTTP-only (docs/HTTP_ONLY_SCANS_PLAN.md): inventory comes from recipe
replay and per-car data from VDP recipes + HTTP-first. The headless browser lives
only in discovery (``discovery_probe --browser-capture``).
"""
from __future__ import annotations

from backend.scanner.phases.dealer_run_steps.state import DealerRun


async def open_session(run: DealerRun) -> None:
    # No context, no page: the recipe replay + HTTP-first detail pass are the
    # whole scan; recovery runs only its HTTP-safe strategies.
    run.page = None
    run.result["http_only"] = True
    # Warmup, dead-domain URL discovery, the maintenance-page probe and the
    # site profiler were browser phases; they live in discovery now
    # (backend/scanner/discovery_capture.py, docs/HTTP_ONLY_SCANS_PLAN.md).
