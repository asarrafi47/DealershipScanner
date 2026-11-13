"""Step 1: who is looking — record the view and read the saved-car flag."""

from __future__ import annotations

from flask import request

from backend.db.inventory_db import is_car_saved
from backend.db.user_history_db import record_car_view
from backend.routes.home_dashboard import _invalidate_reco_cache


def record_view_and_saved_state(uid, car_id: int) -> bool:
    """Record a signed-in viewer's car view (best effort); return ``car_is_saved``."""
    car_is_saved = False
    if uid:
        # A browser prefetch (header sent by nav_perf.js) is not a view: it must not enter
        # Recently viewed or the recommendations, nor reset the home caches.
        if request.headers.get("X-DS-Prefetch") != "1":
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
