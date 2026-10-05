"""Process-local job manager for the on-demand window-sticker / packages fetch.

Moved verbatim out of ``backend/routes/cars_pages.py`` (monolith audit
2026-10-01, W5). Import these names from this module; ``cars_pages`` imports
only the ones its route uses.

What lives here is the state machine only (budget/cooldown/TTL/poll knobs, the
in-flight thread registry, the panel snapshot/cache dicts, the status
vocabulary and the budgeted runner). The ``/packages/ensure`` route and its
payload builders stay in ``cars_pages`` because the tests spy on them as
``cars_pages`` module globals.

The state is per process: with ``GUNICORN_WORKERS=1`` there is one registry; a
scale-out would give each worker its own (unchanged by this move).
"""

from __future__ import annotations

import logging
import os
import threading
import time

# Same logger name as before the move, so log routing/filters are unchanged.
logger = logging.getLogger("backend.routes.cars_pages")


# --- window sticker: honest state + a deadline on the on-demand fetch --------
#
# ``ensure_listing_packages_for_car`` is the on-demand OEM/listing sticker path
# and must keep working exactly as it does (see the project rule: no batch
# harvesting, no new brands).  What it must NOT do is hold an HTTP request open
# while it re-scrapes a dealer VDP.  Measured on this branch, car 136451 (a 2024
# Altima with no sticker source at all): the inner call takes 28.0s cold / 12.2s
# warm and stores nothing, *every single time* — nothing about the row changes,
# so the next view pays it again.
#
# Five separate things were wrong; all five are fixed here.  (1)-(3) came first;
# (4)-(5) are the user-visible regressions that first round introduced.
#
# 1. Only the inner call had a budget, not the request.  After ``join(budget)``
#    the route still did ``get_car_by_id`` + ``prepare_car_detail_context`` +
#    the sticker probes *while the worker kept running*, and that worker starves
#    them: the same tail measured 0.04s idle and 10.5s alongside a live worker
#    (window_sticker_available alone: 0.00s -> 5.57s).  So a 6s budget produced
#    13-17s responses.  Fix: build the panel payload BEFORE starting the worker,
#    and on timeout return that snapshot without touching anything contended.
# 2. Repeat views re-paid the whole budget forever, because the single flight
#    only covers a worker that is still alive.  Fix: a per-car cooldown — one
#    pipeline run per car per ``CAR_PACKAGES_ENSURE_COOLDOWN_SECONDS`` — plus a
#    short TTL on the read-only panel build.  Later views answer from stored
#    state and launch nothing; when a worker finishes it drops both, so anything
#    it stored shows up on the very next view.
# 3. The status flipped between calls on the same car, because the payload was
#    read off a row a background worker was concurrently rewriting.  Fix: while
#    a worker is in flight for a car, replay the panel *content* captured before
#    it started, field for field.  (The fetch-state fields are deliberately not
#    replayed — see (5); they are re-derived on every response.)
#
# 4. The budget silently *dropped* the sticker for the shopper.  The response
#    said ``window_sticker_status="fetching"`` and nothing else, and
#    ``car_packages.js`` calls this endpoint exactly once per page load — so the
#    server-side guarantee "it shows on the very next view" had no next view.
#    Measured on this branch before the fix, car 91583 (fresh process, premium
#    session, GET /car/91583 then the ensure XHR): the XHR answered at 6.01s with
#    ``fetching`` and the worker stored a real Monroney PDF 18.9s later that the
#    shopper never saw.  Fix: the response now carries an explicit, machine-
#    readable retry contract (``window_sticker_fetch_in_flight`` /
#    ``window_sticker_should_retry`` / ``window_sticker_retry_after_seconds``)
#    so the client can poll, and stops the moment the server says to.
# 5. ``window_sticker_status`` lied about work being in flight.  It said
#    "fetching" whenever a sticker *source* was known and nothing was stored —
#    including when the pipeline had already run and found nothing (car 125964,
#    measured: ``window_sticker_ensure_skipped="cooldown"`` together with
#    ``window_sticker_status="fetching"``, no thread alive).  Fix: "fetching"
#    now means exactly "a worker thread for this car is alive as this response
#    is built"; the "a source exists but nothing is stored and nothing is
#    running" case is its own state, ``pending``.
_PACKAGES_ENSURE_DEFAULT_BUDGET = 6.0
_PACKAGES_ENSURE_DEFAULT_COOLDOWN = 900.0
_PACKAGES_ENSURE_DEFAULT_PANEL_TTL = 60.0
# How soon a client should re-POST while a fetch is genuinely in flight.
_PACKAGES_ENSURE_DEFAULT_POLL = 2.0
# Bound the bookkeeping dicts; a fleet-sized cap with a coarse prune is enough
# (correctness never depends on a hit — a miss just re-runs the pipeline).
_PACKAGES_ENSURE_TRACK_MAX = 20_000

_packages_ensure_inflight: dict[int, threading.Thread] = {}
# Panel payload captured just before the in-flight worker for that car started.
_packages_ensure_snapshot: dict[int, dict] = {}
# Short-lived cache of the read-only panel build, per car: ``{car_id: (t, payload)}``.
# Building it is 0.01-0.05s idle but was measured at 10.7s while the listings
# prewarm/rebuild saturates the process, and that rebuild fires on a 60s token —
# so a repeat view must not have to build it again. Invalidated as soon as a
# worker for that car finishes, so a sticker it stored shows up on the next view.
_packages_ensure_panel_cache: dict[int, tuple[float, dict]] = {}
# Monotonic timestamp of the last pipeline run per car (set at launch, refreshed
# on completion so the cooldown is measured from when the work actually ended).
_packages_ensure_attempted_at: dict[int, float] = {}
_packages_ensure_lock = threading.Lock()

# Outcome of an ensure attempt, as seen by the request thread. Reported to the
# client as ``window_sticker_ensure_outcome`` — diagnostics, not a UI signal.
ENSURE_COMPLETED = "completed"
ENSURE_TIMEOUT = "timeout"
ENSURE_INFLIGHT = "inflight"
ENSURE_COOLDOWN = "cooldown"

# Panel states the client can render without guessing.
#   ready       — something is stored, show it.
#   fetching    — a worker thread for this car is alive right now. This is the
#                 ONLY state that means work is happening; it is never returned
#                 unless ``window_sticker_fetch_in_flight`` is true.
#   pending     — a sticker source exists but nothing is stored and nothing is
#                 running. Either the pipeline has not run for this car yet, or
#                 it ran and came back empty. Not a spinner.
#   unavailable — this vehicle has no sticker source at all.
#   hidden      — the panel is not shown for this car.
STICKER_STATUS_READY = "ready"
STICKER_STATUS_FETCHING = "fetching"
STICKER_STATUS_PENDING = "pending"
STICKER_STATUS_UNAVAILABLE = "unavailable"
STICKER_STATUS_HIDDEN = "hidden"

# Panel *content*: a function of (row, stored sticker files) alone. For an
# unchanged row every call returns the same values for these, including while a
# background worker is rewriting the row (that window replays a snapshot).
PACKAGES_ENSURE_PANEL_FIELDS = (
    "show_window_sticker_ui",
    "window_sticker_available",
    "window_sticker_visual_available",
    "window_sticker_source_known",
    "window_sticker_oem_url",
    "window_sticker_view_url",
    "window_sticker_preview_url",
    "packages_panel_has_content",
    "listing_photo_detected_equipment",
)

# Fetch *state*: deliberately live, re-derived on every response. These describe
# what is happening right now, so they may differ between two calls for the same
# car — that is the point.
PACKAGES_ENSURE_FETCH_FIELDS = (
    "window_sticker_status",
    "window_sticker_fetch_in_flight",
    "window_sticker_should_retry",
    "window_sticker_retry_after_seconds",
    "window_sticker_ensure_outcome",
    "window_sticker_fetch_pending",
)

# Every 200 from ``/packages/ensure`` carries all of these. (The rest of the
# payload is diagnostics from the pipeline run — ``stored``, ``analyzed``,
# ``listing_description_reason``, … — only present on the view that actually ran
# it. Don't drive UI off those.)
PACKAGES_ENSURE_RESPONSE_FIELDS = PACKAGES_ENSURE_PANEL_FIELDS + PACKAGES_ENSURE_FETCH_FIELDS


def _packages_ensure_budget_seconds() -> float:
    """Wall-clock budget for the whole ``/packages/ensure`` request (0 disables the deadline)."""
    raw = (os.getenv("CAR_PACKAGES_ENSURE_BUDGET_SECONDS") or "").strip()
    if not raw:
        return _PACKAGES_ENSURE_DEFAULT_BUDGET
    try:
        return max(0.0, float(raw))
    except ValueError:
        return _PACKAGES_ENSURE_DEFAULT_BUDGET


def _packages_ensure_cooldown_seconds() -> float:
    """How long after a pipeline run for a car before another view may launch one (0 disables)."""
    raw = (os.getenv("CAR_PACKAGES_ENSURE_COOLDOWN_SECONDS") or "").strip()
    if not raw:
        return _PACKAGES_ENSURE_DEFAULT_COOLDOWN
    try:
        return max(0.0, float(raw))
    except ValueError:
        return _PACKAGES_ENSURE_DEFAULT_COOLDOWN


def _packages_ensure_panel_ttl_seconds() -> float:
    """How long a read-only panel build may be reused for a car (0 disables the cache)."""
    raw = (os.getenv("CAR_PACKAGES_ENSURE_PANEL_TTL_SECONDS") or "").strip()
    if not raw:
        return _PACKAGES_ENSURE_DEFAULT_PANEL_TTL
    try:
        return max(0.0, float(raw))
    except ValueError:
        return _PACKAGES_ENSURE_DEFAULT_PANEL_TTL


def _packages_ensure_poll_seconds() -> float:
    """How long the client should wait before re-POSTing while a fetch is in flight."""
    raw = (os.getenv("CAR_PACKAGES_ENSURE_POLL_SECONDS") or "").strip()
    if not raw:
        return _PACKAGES_ENSURE_DEFAULT_POLL
    try:
        return max(0.25, float(raw))
    except ValueError:
        return _PACKAGES_ENSURE_DEFAULT_POLL


def _prune_packages_ensure_tracking() -> None:
    """Caller holds ``_packages_ensure_lock``."""
    if len(_packages_ensure_attempted_at) > _PACKAGES_ENSURE_TRACK_MAX:
        _packages_ensure_attempted_at.clear()
    if len(_packages_ensure_snapshot) > _PACKAGES_ENSURE_TRACK_MAX:
        _packages_ensure_snapshot.clear()
    if len(_packages_ensure_panel_cache) > _PACKAGES_ENSURE_TRACK_MAX:
        _packages_ensure_panel_cache.clear()


def _packages_ensure_inflight_snapshot(car_id: int) -> dict | None:
    """Payload captured before the still-running worker for *car_id* started, if any."""
    with _packages_ensure_lock:
        t = _packages_ensure_inflight.get(int(car_id))
        if t is None or not t.is_alive():
            return None
        snap = _packages_ensure_snapshot.get(int(car_id))
    return dict(snap) if snap is not None else None


def _packages_ensure_worker_alive(car_id: int) -> bool:
    """True iff a pipeline thread for *car_id* is alive at this instant."""
    with _packages_ensure_lock:
        t = _packages_ensure_inflight.get(int(car_id))
    return bool(t is not None and t.is_alive())


def _packages_ensure_cooldown_remaining(car_id: int, cooldown: float) -> float:
    """Seconds until another pipeline run may be launched for *car_id* (0 = now)."""
    if cooldown <= 0:
        return 0.0
    with _packages_ensure_lock:
        last = _packages_ensure_attempted_at.get(int(car_id))
    if last is None:
        return 0.0
    return max(0.0, cooldown - (time.monotonic() - last))


def _packages_ensure_in_cooldown(car_id: int, cooldown: float) -> bool:
    return _packages_ensure_cooldown_remaining(car_id, cooldown) > 0.0


def _window_sticker_status(
    *,
    show_sticker_ui: bool,
    sticker_ready: bool,
    sticker_visual: bool,
    has_source: bool,
    fetch_in_flight: bool = False,
) -> str:
    """
    Collapse the sticker flags into one state string for the panel.

    ``fetch_in_flight`` is the only thing that can produce
    :data:`STICKER_STATUS_FETCHING`: a known sticker source with nothing stored
    and no worker running is :data:`STICKER_STATUS_PENDING`, not "fetching".
    """
    if not show_sticker_ui:
        return STICKER_STATUS_HIDDEN
    if sticker_ready or sticker_visual:
        return STICKER_STATUS_READY
    if fetch_in_flight:
        return STICKER_STATUS_FETCHING
    if has_source:
        return STICKER_STATUS_PENDING
    return STICKER_STATUS_UNAVAILABLE


def _run_packages_ensure_with_budget(
    car_id: int,
    *,
    allow_vision: bool,
    budget: float | None = None,
    snapshot: dict | None = None,
) -> tuple[dict | None, str]:
    """
    Run the on-demand packages/sticker pipeline, but wait at most *budget* seconds.

    Returns ``(status, outcome)``; ``status`` is only non-``None`` for
    :data:`ENSURE_COMPLETED`. On :data:`ENSURE_TIMEOUT` the worker keeps running
    so a later view finds the result already stored; *snapshot* (the panel
    payload as it looked before the worker started) is registered for that
    window so concurrent views get a consistent answer instead of a torn read.
    """
    from backend.enrichment.listing_packages_service import ensure_listing_packages_for_car

    car_id = int(car_id)
    if budget is None:
        budget = _packages_ensure_budget_seconds()
    if budget <= 0:
        # Deadline disabled — run it inline (dev/debug and the unit tests).
        try:
            return ensure_listing_packages_for_car(
                car_id, allow_vision_fallback=allow_vision, refetch_description=True
            ), ENSURE_COMPLETED
        finally:
            with _packages_ensure_lock:
                _packages_ensure_panel_cache.pop(car_id, None)
                _packages_ensure_attempted_at[car_id] = time.monotonic()
                _prune_packages_ensure_tracking()

    box: dict[str, dict] = {}

    def _work() -> None:
        try:
            box["status"] = ensure_listing_packages_for_car(
                car_id, allow_vision_fallback=allow_vision, refetch_description=True
            )
        except Exception:
            logger.exception("packages ensure failed for car %s", car_id)
        finally:
            with _packages_ensure_lock:
                _packages_ensure_inflight.pop(car_id, None)
                _packages_ensure_snapshot.pop(car_id, None)
                # Whatever it stored must be visible to the very next view.
                _packages_ensure_panel_cache.pop(car_id, None)
                # Measure the cooldown from when the work ended, not started.
                _packages_ensure_attempted_at[car_id] = time.monotonic()

    with _packages_ensure_lock:
        existing = _packages_ensure_inflight.get(car_id)
        if existing is not None and existing.is_alive():
            # Someone is already fetching this VIN — don't start a second one.
            return None, ENSURE_INFLIGHT
        t = threading.Thread(target=_work, name=f"packages-ensure-{car_id}", daemon=True)
        _packages_ensure_inflight[car_id] = t
        if snapshot is not None:
            _packages_ensure_snapshot[car_id] = dict(snapshot)
        _packages_ensure_attempted_at[car_id] = time.monotonic()
        _prune_packages_ensure_tracking()
        t.start()

    t.join(budget)
    if t.is_alive():
        return None, ENSURE_TIMEOUT
    return box.get("status"), ENSURE_COMPLETED
