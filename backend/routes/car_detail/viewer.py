"""Step 1: who is looking — record the view and read the saved-car flag."""

from __future__ import annotations

from backend.routes.home_dashboard import _invalidate_reco_cache


def record_view_and_saved_state(main, uid, car_id: int) -> bool:
    """Record a signed-in viewer's car view (best effort); return ``car_is_saved``."""
    car_is_saved = False
    if uid:
        try:
            main.record_car_view(int(uid), car_id)
            _invalidate_reco_cache(int(uid))
        except Exception:
            pass
        try:
            car_is_saved = main.is_car_saved(int(uid), car_id)
        except Exception:
            pass
    return car_is_saved
