"""Queue onboarding is paused (N3.10, N3.10b): one answer for every web entry point.

No worker has consumed ``dealer_jobs`` since the worker and scheduler services were
deleted on 2026-10-08 (SCANNING_OPS_LOG.md), so a job queued from the web app waits
forever. Owner decision D-IF3 (a) retires queue onboarding and hands it to
dealer-discovery. Every web path that would insert a ``dealer_jobs`` row answers 409
with ``ONBOARDING_PAUSED_MESSAGE`` and enqueues nothing:

- ``POST /admin/dealers`` (the hub form; plain text, N3.10);
- ``POST /api/admin/dealer-onboard`` (Find dealers "Request scrape");
- ``POST /api/admin/dealer-jobs/<id>/retry`` and ``.../smart-retry``.

The queue code itself stays: P15A.3 fixes or removes the path for good, and this
module goes with it. The Find dealers page and the hub read the same flag to hide
"Request scrape" and the Restart buttons. The hub POST answers 409 whatever the
flag says: N3.10 removed its form and its enqueue call.
"""

from __future__ import annotations

from flask import jsonify

# A constant, not an env knob: turning it off without a dealer_jobs consumer would
# only queue jobs that never run again. P15A.3 owns the change.
QUEUE_ONBOARDING_PAUSED = True

ONBOARDING_PAUSED_MESSAGE = "Onboarding is paused; use dealer-discovery (see P15A.3)"
ONBOARDING_PAUSED_ERROR = "onboarding_paused"


def onboarding_paused_json():
    """``(response, 409)`` for the JSON endpoints: ``error`` is the code, ``message`` the line."""
    return (
        jsonify({"ok": False, "error": ONBOARDING_PAUSED_ERROR, "message": ONBOARDING_PAUSED_MESSAGE}),
        409,
    )
