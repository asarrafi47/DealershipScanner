"""Per-dealership RESEARCH page (additive, read-only).

New surface at ``GET /dealership/<dealer_key>``. This is intentionally separate
from the car SEARCH / listings flow and must not affect it. It shows one
dealer's header, inventory grid, Leaflet location map, contact block, and
navigate-there deep links, plus an empty placeholder section for a later
"Current Deals & Specials" phase.

``dealer_key`` accepts either the hostname-derived ``cars.dealer_id`` (e.g.
``tustintoyota-com``) or the numeric ``dealerships.id``. The ``dealerships`` <->
``cars`` join is by host: ``dealerships.website_url`` host (www-stripped, dots
turned to dashes) matches ``cars.dealer_id``.
"""

from __future__ import annotations

import sqlite3
from urllib.parse import urlparse

from flask import abort, render_template

# Cap for the server-rendered inventory grid. The true total count is shown
# separately; this only bounds how many cards we render on the page.
_GRID_CAP = 60


def _host_to_dealer_id(url: str | None) -> str | None:
    """``https://www.longotoyota.com/`` -> ``longotoyota-com`` (matches cars.dealer_id)."""
    if not url:
        return None
    host = urlparse(url if "://" in url else "http://" + url).netloc.lower().strip()
    if host.startswith("www."):
        host = host[4:]
    host = host.split(":")[0].strip()
    if not host:
        return None
    return host.replace(".", "-")


def _fetch_dealership_by_id(dealership_id: int) -> dict | None:
    from backend.db.inventory_db import get_conn

    conn = get_conn()
    try:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, name, website_url, oem_brand, platform,
                   google_rating, google_review_count,
                   street_address, city, state, zip_code,
                   latitude, longitude, phone
            FROM dealerships WHERE id = ?
            """,
            (dealership_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def _find_dealership_by_dealer_id(dealer_id: str) -> dict | None:
    """Match a dealerships row to a cars.dealer_id via website_url host."""
    from backend.db.inventory_db import get_conn

    conn = get_conn()
    try:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, name, website_url, oem_brand, platform,
                   google_rating, google_review_count,
                   street_address, city, state, zip_code,
                   latitude, longitude, phone
            FROM dealerships
            WHERE website_url IS NOT NULL AND website_url <> ''
            """
        )
        for row in cur.fetchall():
            if _host_to_dealer_id(row["website_url"]) == dealer_id:
                return dict(row)
        return None
    finally:
        conn.close()


def _dealer_inventory(dealer_id: str) -> dict:
    """Return counts and a capped, serialized grid of active cars for a dealer."""
    from backend.db.inventory_db import get_conn
    from backend.db.repositories.listings_repo import serialize_car_for_listings_grid

    conn = get_conn()
    try:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN LOWER(COALESCE(condition, '')) = 'new' THEN 1 ELSE 0 END) AS new_count
            FROM cars
            WHERE dealer_id = ? AND COALESCE(listing_active, 1) = 1
            """,
            (dealer_id,),
        )
        crow = cur.fetchone()
        total = int(crow["total"] or 0) if crow else 0
        new_count = int(crow["new_count"] or 0) if crow else 0

        cur.execute(
            """
            SELECT * FROM cars
            WHERE dealer_id = ? AND COALESCE(listing_active, 1) = 1
            ORDER BY
                CASE WHEN LOWER(COALESCE(condition, '')) = 'new' THEN 0 ELSE 1 END,
                COALESCE(price, 0) DESC
            LIMIT ?
            """,
            (dealer_id, _GRID_CAP),
        )
        rows = [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()

    cars = []
    dealer_name_from_car = None
    dealer_url_from_car = None
    for r in rows:
        try:
            card = serialize_car_for_listings_grid(r)
        except Exception:
            continue
        cars.append(card)
        if not dealer_name_from_car and card.get("dealer_name"):
            dealer_name_from_car = card.get("dealer_name")
        if not dealer_url_from_car and card.get("dealer_url"):
            dealer_url_from_car = card.get("dealer_url")

    return {
        "total": total,
        "new_count": new_count,
        "used_count": max(total - new_count, 0),
        "shown": len(cars),
        "capped": total > len(cars),
        "cars": cars,
        "dealer_name_from_car": dealer_name_from_car,
        "dealer_url_from_car": dealer_url_from_car,
    }


def _resolve_dealer(dealer_key: str) -> tuple[dict | None, str | None]:
    """(dealership_row | None, dealer_id | None) from a numeric id or a dealer_id string."""
    key = (dealer_key or "").strip()
    if not key:
        return None, None
    if key.isdigit():
        row = _fetch_dealership_by_id(int(key))
        if not row:
            return None, None
        return row, _host_to_dealer_id(row.get("website_url"))
    # Treat as cars.dealer_id (hostname-derived); find its dealerships row by host.
    return _find_dealership_by_dealer_id(key), key


def dealership_research_page(dealer_key: str):
    dealership, dealer_id = _resolve_dealer(dealer_key)

    inventory = _dealer_inventory(dealer_id) if dealer_id else None
    has_inventory = bool(inventory and inventory["total"])

    # Require *something* to show: a registry row or real inventory. Otherwise 404
    # rather than render an empty shell.
    if not dealership and not has_inventory:
        abort(404)

    # Header/contact fields: prefer the registry row, fall back to values derived
    # from the dealer's own car listings.
    name = (dealership or {}).get("name") or (inventory or {}).get("dealer_name_from_car") or dealer_id
    website_url = (dealership or {}).get("website_url") or (inventory or {}).get("dealer_url_from_car")

    lat = (dealership or {}).get("latitude")
    lon = (dealership or {}).get("longitude")
    try:
        lat = float(lat) if lat is not None else None
        lon = float(lon) if lon is not None else None
    except (TypeError, ValueError):
        lat = lon = None
    has_geo = lat is not None and lon is not None

    address_parts = [
        (dealership or {}).get("street_address"),
        (dealership or {}).get("city"),
        (dealership or {}).get("state"),
        (dealership or {}).get("zip_code"),
    ]
    full_address = ", ".join(p for p in address_parts if p) or None

    # Navigate-there deep links: prefer precise geo, fall back to the address text.
    if has_geo:
        nav_dest = f"{lat},{lon}"
        gmaps_url = f"https://www.google.com/maps/dir/?api=1&destination={nav_dest}"
        apple_url = f"https://maps.apple.com/?daddr={nav_dest}"
        waze_url = f"https://waze.com/ul?ll={nav_dest}&navigate=yes"
    elif full_address:
        from urllib.parse import quote_plus

        enc = quote_plus(full_address)
        gmaps_url = f"https://www.google.com/maps/dir/?api=1&destination={enc}"
        apple_url = f"https://maps.apple.com/?daddr={enc}"
        waze_url = f"https://waze.com/ul?q={enc}&navigate=yes"
    else:
        gmaps_url = apple_url = waze_url = None

    return render_template(
        "dealership.html",
        dealer_key=dealer_key,
        dealer_id=dealer_id,
        dealership=dealership,
        name=name,
        website_url=website_url,
        phone=(dealership or {}).get("phone"),
        oem_brand=(dealership or {}).get("oem_brand"),
        google_rating=(dealership or {}).get("google_rating"),
        google_review_count=(dealership or {}).get("google_review_count"),
        full_address=full_address,
        lat=lat,
        lon=lon,
        has_geo=has_geo,
        nav_gmaps_url=gmaps_url,
        nav_apple_url=apple_url,
        nav_waze_url=waze_url,
        inventory=inventory or {"total": 0, "new_count": 0, "used_count": 0, "cars": [], "capped": False},
    )


def register(app) -> None:
    """Attach the dealership research page route (additive; bare endpoint name)."""
    app.add_url_rule(
        "/dealership/<dealer_key>",
        view_func=dealership_research_page,
        methods=["GET"],
    )
