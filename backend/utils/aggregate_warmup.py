"""Warm the fleet aggregates before the web worker starts serving (web speed P6).

``gunicorn.conf.py`` calls :func:`warm_blocking` from ``post_fork``. A worker does not
accept connections until that hook returns, so Railway's ``/health`` check cannot pass
(and the first visitor cannot arrive) before the aggregates are loaded -- the first car
page after a deploy then costs what any other car page costs.

Never fatal and never long: each step is isolated (a failing one is logged and the
request path keeps its inline cold fallback), and the wait is capped. The cap must stay
well under the gunicorn worker timeout (120 s in ``scripts/docker-entrypoint-web.sh``)
and the Railway health-check window (300 s); a step still running at the cap simply
finishes on its daemon thread.

Environment: ``AGGREGATE_WARM=0`` turns it off; ``AGGREGATE_WARM_CAP_S`` (default 45,
at most 90) is the longest the worker waits.
"""

from __future__ import annotations

import importlib
import logging
import os
import threading
import time

_log = logging.getLogger(__name__)

DEFAULT_CAP_S = 45.0
MAX_CAP_S = 90.0

# (label, module, function). The six values the README groups as "the five aggregates":
# rarity counts, price plausibility, deal-score bands, public count, featured cars and
# the dealer fallback URLs.
WARM_STEPS: tuple[tuple[str, str, str], ...] = (
    ("rarity counts", "backend.utils.rarity_score", "warm_inventory_counts"),
    ("price plausibility", "backend.utils.price_plausibility", "warm"),
    ("deal-score bands", "backend.intelligence.deal_score_cache", "warm"),
    ("public listings count", "backend.db.repositories.listings_repo", "warm_public_listings_count"),
    ("featured cars", "backend.db.repositories.listings_repo", "warm_featured_cars"),
    ("fallback dealer urls", "backend.db.repositories.grid_cards_repo", "warm_fallback_dealer_urls"),
)


def warm_cap_seconds() -> float:
    raw = os.environ.get("AGGREGATE_WARM_CAP_S")
    try:
        cap = float(raw) if raw not in (None, "") else DEFAULT_CAP_S
    except ValueError:
        cap = DEFAULT_CAP_S
    return max(0.0, min(cap, MAX_CAP_S))


def warm_enabled() -> bool:
    return os.environ.get("AGGREGATE_WARM", "1").strip().lower() not in ("0", "false", "no", "off")


def warm_aggregates(steps=WARM_STEPS, *, deadline: float | None = None) -> dict[str, str]:
    """Run every step in order; returns ``{label: "ok" | "skipped" | "failed: <type>"}``.

    A step that raises is logged and does not stop the others. After ``deadline`` (a
    ``time.monotonic()`` value) no further step starts.
    """
    out: dict[str, str] = {}
    for label, module, func in steps:
        if deadline is not None and time.monotonic() >= deadline:
            out[label] = "skipped"
            continue
        t0 = time.perf_counter()
        try:
            getattr(importlib.import_module(module), func)()
            out[label] = "ok"
            _log.info("aggregate warm-up: %s ready (%.2fs)", label, time.perf_counter() - t0)
        except Exception as exc:
            out[label] = f"failed: {type(exc).__name__}"
            _log.warning("aggregate warm-up: %s failed; it loads on first use", label, exc_info=True)
    return out


def warm_blocking(cap_s: float | None = None, steps=WARM_STEPS) -> dict[str, str] | None:
    """Warm in a thread and wait at most ``cap_s``. Returns the step results, or None when
    disabled or when the cap passed first (the thread keeps going in the background)."""
    if not warm_enabled():
        return None
    cap = warm_cap_seconds() if cap_s is None else max(0.0, cap_s)
    result: dict[str, dict[str, str]] = {}
    started = time.monotonic()

    def _run() -> None:
        result["steps"] = warm_aggregates(steps, deadline=started + cap)

    t = threading.Thread(target=_run, name="aggregate-warmup", daemon=True)
    t.start()
    t.join(cap)
    if t.is_alive():
        _log.warning("aggregate warm-up still running after %.0fs; serving anyway", cap)
        return None
    return result.get("steps")
