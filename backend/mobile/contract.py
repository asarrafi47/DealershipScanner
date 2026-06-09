"""Canonical list of HTTP routes used by the Sarrafi Cars iOS app.

Keep in sync with ``ios/docs/API_CONTRACT.md``. Contract tests assert these
endpoints exist on the Flask app (see ``backend/tests/test_mobile_api_contract.py``).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

Method = Literal["GET", "POST"]


@dataclass(frozen=True, slots=True)
class MobileRoute:
    method: Method
    path: str
    endpoint: str
    """Flask view name (``request.endpoint``)."""
    auth: Literal["public", "session", "session_csrf"] = "public"
    notes: str = ""


# Paths use Flask rule syntax (<int:car_id>, etc.).
MOBILE_API_ROUTES: tuple[MobileRoute, ...] = (
    MobileRoute("GET", "/api/auth/csrf", "api_auth_csrf"),
    MobileRoute("GET", "/api/auth/me", "api_auth_me", auth="session"),
    MobileRoute(
        "POST",
        "/api/auth/login",
        "api_auth_login",
        auth="session_csrf",
        notes="SEC-077",
    ),
    MobileRoute(
        "POST",
        "/api/auth/register",
        "api_auth_register",
        auth="session_csrf",
        notes="SEC-086; same users.db as /register",
    ),
    MobileRoute(
        "POST",
        "/api/auth/logout",
        "api_auth_logout",
        auth="session_csrf",
        notes="SEC-077",
    ),
    MobileRoute("GET", "/api/listings/cars", "api_listings_cars"),
    MobileRoute("GET", "/api/listings/filter-options", "api_listings_filter_options"),
    MobileRoute("GET", "/api/listings/geo-coords", "api_listings_geo_coords"),
    MobileRoute(
        "POST",
        "/api/session/listings-geo",
        "api_session_listings_geo",
        auth="session_csrf",
    ),
    MobileRoute(
        "POST",
        "/api/search/smart",
        "api_search_smart",
        auth="session_csrf",
        notes="SEC-040 rate limit",
    ),
    MobileRoute("GET", "/api/zip-coords", "api_zip_coords"),
    MobileRoute("GET", "/api/dealer-locator", "api_dealer_locator"),
    MobileRoute("GET", "/api/saved-cars", "api_saved_cars", auth="session"),
    MobileRoute("GET", "/api/cars/<int:car_id>", "api_car_detail"),
    MobileRoute(
        "POST",
        "/api/cars/<int:car_id>/save",
        "api_toggle_save",
        auth="session_csrf",
    ),
)
