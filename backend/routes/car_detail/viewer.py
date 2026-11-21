"""Step 1: who is looking — record the view and read the saved-car flag."""

from __future__ import annotations

from flask import request

from backend.db.inventory_db import is_car_saved
from backend.db.user_history_db import record_car_view
from backend.routes.home_dashboard import _invalidate_reco_cache


def is_prefetch_request() -> bool:
    """True for a speculative fetch the shopper has not navigated to (yet).

    * ``X-DS-Prefetch: 1``: nav_perf.js's fetch() fallback (browsers without
      speculation rules).
    * ``Sec-Purpose: prefetch`` / ``prefetch;prerender`` (and the legacy
      ``Purpose: prefetch``): a speculation-rules prefetch or prerender, or a
      ``<link rel=prefetch>``. The value is a structured-header list whose first
      item is the token ``prefetch``; parameters such as ``;anonymous-client-ip``
      may follow."""
    if request.headers.get("X-DS-Prefetch") == "1":
        return True
    for name in ("Sec-Purpose", "Purpose"):
        raw = (request.headers.get(name) or "").strip().lower()
        if raw.split(";", 1)[0].split(",", 1)[0].strip() == "prefetch":
            return True
    return False


def record_view_and_saved_state(uid, car_id: int) -> bool:
    """Record a signed-in viewer's car view (best effort); return ``car_is_saved``."""
    car_is_saved = False
    if uid:
        # A browser prefetch (nav_perf.js's header, or a speculation-rules prefetch's
        # Sec-Purpose) is not a view: it must not enter Recently viewed or the
        # recommendations, nor reset the home caches. Car pages are never prerendered
        # (frontend/static/speculation-rules.json): a prerender runs the page now and
        # its later activation would never reach the server to record the view.
        if not is_prefetch_request():
            try:
                record_car_view(int(uid), car_id)
                _invalidate_reco_cache(int(uid))
            except Exception:
                pass
        try:
            car_is_saved = is_car_saved(int(uid), car_id)
        except Exception:
            pass
    return car_is_saved
