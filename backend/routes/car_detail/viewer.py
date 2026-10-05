"""Step 1: who is looking — record the view and read the saved-car flag."""

from __future__ import annotations

from backend.db.inventory_db import is_car_saved
from backend.db.user_history_db import record_car_view
from backend.routes.home_dashboard import _invalidate_reco_cache


def record_view_and_saved_state(uid, car_id: int) -> bool:
    """Record a signed-in viewer's car view (best effort); return ``car_is_saved``."""
    car_is_saved = False
    if uid:
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
