"""Sync dealer-portal vehicles into the public ``cars`` table.

Dealer-portal vehicles (``backend/db/dealer_portal_db.py`` / the
``dealer_vehicles`` table) are managed by dealer self-service users under
``/inventory``, but previously never appeared on ``/listings`` or a car page:
``search_cars`` (``backend/db/repositories/search_repo.py``) and the listings
grid (``backend/db/repositories/listings_repo.py``) both read only from
``cars`` — a completely separate table with its own VIN space.

This module upserts one dealer-portal vehicle into ``cars`` through the SAME
path the scanner uses (``backend.scanner.database.upsert_vehicles``), so a
portal listing gets identical field cleaning / data-quality scoring /
``listing_active`` bookkeeping to a scanned row, rather than a hand-rolled
INSERT that would drift from that logic over time.

Call :func:`sync_portal_vehicle_to_public_listings` after any dealer-portal
create/update (including gallery changes) and
:func:`remove_portal_vehicle_from_public_listings` after a portal vehicle is
deleted. Both are best-effort and never raise — the portal-side save (the
dealer's own source of truth) has already succeeded by the time either is
called, and a sync failure must not undo or block that.

Safety: ``cars.vin`` is UNIQUE and shared with every scanned dealer. If a
portal user's VIN collides with an already-scanned car, upserting would
silently overwrite that car's real dealer attribution. :func:`_vin_is_safe_to_sync`
refuses the sync in that case (logged) rather than clobbering scanned data —
the portal vehicle still saves fine, it just does not surface publicly under
someone else's VIN. The sync returns :data:`SYNC_VIN_TAKEN` then, so the route
tells the seller (card S19 (5)).

Public posting switch (card S19 (1), ``DESIGN.md`` section 23): while
``MARKETPLACE_POSTING_ENABLED`` is off (the default), only **grandfathered**
accounts are copied to ``cars``: an account with at least one ``dealer_vehicles``
row created before :data:`GRANDFATHER_CUTOFF`. Every other account keeps its cars
private (saved in ``dealer_vehicles`` only) and any public row it already has for
that VIN is retired on its next save.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin

from backend.config import Config

_log = logging.getLogger(__name__)

# Namespace prefix for cars.dealer_id on portal-sourced rows, so they can
# never collide with a real scanned rooftop's dealer_id and are easy to spot.
DEALER_PORTAL_DEALER_ID_PREFIX = "dealer-portal-"

# Card S19: an account that had a dealer_vehicles row before this moment is
# "grandfathered" (its cars keep reaching the public site while public posting
# is off). Fixed at the start of the day the card was built, before its release,
# so accounts made while the old open sign-up was still live are NOT included.
# backend/scripts/list_grandfathered_sellers.py lists them by id for the owner.
GRANDFATHER_CUTOFF = datetime(2026, 11, 15, 0, 0, 0, tzinfo=timezone.utc)
# The same moment as text for the ``cars`` timestamp columns (ISO text; midnight UTC, so any
# timestamp of an earlier day sorts below it and any of that day or later does not).
GRANDFATHER_CUTOFF_DAY = GRANDFATHER_CUTOFF.date().isoformat()

# sync_portal_vehicle_to_public_listings results
SYNC_PUBLISHED = "published"
SYNC_PRIVATE = "private"  # public posting is off and the account is not grandfathered
SYNC_VIN_TAKEN = "vin_taken"  # the VIN is already listed by someone else
SYNC_SKIPPED = "skipped"  # no VIN
SYNC_FAILED = "failed"  # a read or write failed (logged); nothing was published

_TRUE = ("1", "true", "yes", "on")


def marketplace_posting_enabled() -> bool:
    """``MARKETPLACE_POSTING_ENABLED`` (environment only, default off)."""
    return (os.environ.get("MARKETPLACE_POSTING_ENABLED") or "").strip().lower() in _TRUE


def _parse_created_at(raw: Any) -> datetime | None:
    if isinstance(raw, datetime):
        dt = raw
    else:
        text = str(raw or "").strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _vehicle_rows_say_grandfathered(uid: int) -> bool:
    from backend.db import dealer_portal_db as ddb

    for row in ddb.list_vehicles_for_user(uid):
        dt = _parse_created_at(row.get("created_at"))
        if dt is not None and dt < GRANDFATHER_CUTOFF:
            return True
    return False


def _public_rows_say_grandfathered(uid: int) -> bool:
    """A ``cars`` row of this account first seen before the cut-off (live or retired).

    ``dealer_vehicles`` rows can be deleted by the seller (a sold car); the public row is only
    ever retired, never deleted, so it keeps the account's history. Postgres only (production);
    the offline SQLite store answers False.
    """
    from backend.db import inventory_pg
    from backend.db.repositories.base_repo import db_conn

    if not inventory_pg.is_inventory_postgres():
        return False
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT 1 FROM cars WHERE dealer_id = %s "
            "AND COALESCE(NULLIF(first_seen_at, ''), scraped_at) < %s LIMIT 1",
            (dealer_portal_dealer_id(uid), GRANDFATHER_CUTOFF_DAY),
        )
        return cur.fetchone() is not None


def grandfather_status(user_id: int) -> bool | None:
    """True when the account had a ``dealer_vehicles`` row before :data:`GRANDFATHER_CUTOFF`
    (seen from its saved rows or from its public rows); False when not; None when the stores
    could not be read (the caller then changes nothing)."""
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return False
    if uid <= 0:
        return False
    try:
        return _vehicle_rows_say_grandfathered(uid) or _public_rows_say_grandfathered(uid)
    except Exception:
        _log.exception("dealer-portal sync: could not read the grandfather rule for user_id=%s", uid)
        return None


def is_grandfathered_account(user_id: int) -> bool:
    """True only when the account is known to be grandfathered (a read failure is False)."""
    return grandfather_status(user_id) is True


def account_may_publish(user_id: int) -> bool | None:
    """May this account's cars be copied to the public ``cars`` table right now?
    None when the grandfather rule could not be read."""
    if marketplace_posting_enabled():
        return True
    return grandfather_status(user_id)


def dealer_portal_dealer_id(user_id: int) -> str:
    return f"{DEALER_PORTAL_DEALER_ID_PREFIX}{int(user_id)}"


def is_dealer_portal_dealer_id(dealer_id: Any) -> bool:
    return isinstance(dealer_id, str) and dealer_id.startswith(DEALER_PORTAL_DEALER_ID_PREFIX)


def _absolute_gallery_urls(gallery: list[str]) -> list[str]:
    """Best-effort absolute URLs for dealer-uploaded photos.

    Dealer-portal uploads are served from this app at a relative path
    (``/dealer-uploads/<user_id>/<vehicle_id>/<file>``). The scanner's upsert
    path only accepts an ``http(s)`` ``image_url`` as the grid thumbnail
    (anything else falls back to a placeholder), so a relative path is
    resolved against ``PUBLIC_BASE_URL`` when that is configured. Without it,
    the gallery is still stored (the car page can use it), just not as the
    grid thumbnail.
    """
    base = Config.public_base_url()
    if not base:
        return list(gallery)
    prefix = base if base.endswith("/") else base + "/"
    out = []
    for u in gallery:
        if isinstance(u, str) and u.startswith("/"):
            out.append(urljoin(prefix, u.lstrip("/")))
        else:
            out.append(u)
    return out


def _dealer_display_name(user_id: int) -> str:
    """Best-effort dealer name for the listing card: org name, else username."""
    try:
        from backend.db.users_db import get_org, get_user_profile
    except ImportError:
        return f"Dealer portal seller #{user_id}"
    try:
        user = get_user_profile(user_id) or {}
    except Exception:
        user = {}
    org_id = user.get("org_id")
    if org_id:
        try:
            org = get_org(int(org_id))
        except Exception:
            org = None
        name = (org or {}).get("name")
        if name:
            return str(name)
    return user.get("username") or f"Dealer portal seller #{user_id}"


def _vin_owner(vin: str, user_id: int) -> str:
    """``free`` (no row), ``own`` (this caller's portal row), ``taken`` (a scanned
    listing or another portal user) or ``error`` (could not read)."""
    try:
        from backend.db.repositories.cars_repo import get_car_by_vin

        existing = get_car_by_vin(vin)
    except Exception:
        _log.exception("dealer-portal sync: could not check existing row for vin=%s", vin)
        return "error"
    if existing is None:
        return "free"
    if existing.get("dealer_id") == dealer_portal_dealer_id(user_id):
        return "own"
    return "taken"


def _vin_is_safe_to_sync(vin: str, user_id: int) -> bool:
    """Refuse to sync onto a VIN owned by a scanned listing OR another portal user.

    Matches the row's ``dealer_id`` to THIS caller's own portal dealer_id, not
    just "any dealer-portal-* id" — otherwise portal user B could silently
    overwrite portal user A's listing by entering A's VIN. A read failure fails
    closed (skip sync) rather than risk overwriting a row we could not inspect.
    """
    return _vin_owner(vin, user_id) in ("free", "own")


def sync_portal_vehicle_to_public_listings(user_id: int, vehicle: dict[str, Any]) -> str:
    """Upsert one dealer-portal vehicle into the public ``cars`` table.

    Call after create/update/photo-gallery changes so the vehicle appears on
    ``/listings`` and its car page. Never raises. Returns one of the ``SYNC_*``
    values so the route can tell the seller what happened.
    """
    vin = str(vehicle.get("vin") or "").strip()
    if not vin:
        return SYNC_SKIPPED
    may = account_may_publish(user_id)
    if may is None:
        return SYNC_FAILED  # could not read the rule: change nothing on the public site
    if not may:
        # Public posting is off and this account is not grandfathered: the car stays
        # private. A public row this account already has for the VIN is retired so an
        # edit can never leave a stale or newly changed car on the public site.
        remove_portal_vehicle_from_public_listings(vin, user_id)
        return SYNC_PRIVATE
    try:
        owner = _vin_owner(vin, user_id)
        if owner == "error":
            return SYNC_FAILED
        if owner == "taken":
            _log.warning(
                "dealer-portal sync: vin=%s (user_id=%s) collides with an existing "
                "listing owned by someone else; skipping public sync to avoid "
                "overwriting another dealer's data",
                vin, user_id,
            )
            return SYNC_VIN_TAKEN

        gallery = vehicle.get("gallery") or []
        if not isinstance(gallery, list):
            gallery = []
        abs_gallery = _absolute_gallery_urls([str(u) for u in gallery if u])

        row: dict[str, Any] = {
            "vin": vin,
            "title": vehicle.get("title") or None,
            "year": vehicle.get("year"),
            "make": vehicle.get("make"),
            "model": vehicle.get("model"),
            "trim": vehicle.get("trim"),
            "price": vehicle.get("price"),
            "mileage": vehicle.get("mileage"),
            "transmission": vehicle.get("transmission"),
            "drivetrain": vehicle.get("drivetrain"),
            "fuel_type": vehicle.get("fuel_type"),
            "exterior_color": vehicle.get("exterior_color"),
            "interior_color": vehicle.get("interior_color"),
            "cylinders": vehicle.get("cylinders"),
            "body_style": vehicle.get("body_style"),
            "engine_description": vehicle.get("engine_description"),
            "stock_number": vehicle.get("stock_number"),
            "description": vehicle.get("notes"),
            "gallery": abs_gallery,
            "image_url": abs_gallery[0] if abs_gallery else None,
            "dealer_name": _dealer_display_name(user_id),
            "dealer_id": dealer_portal_dealer_id(user_id),
        }

        from backend.scanner.database import upsert_vehicles

        upsert_vehicles([row])
    except Exception:
        _log.exception(
            "dealer-portal sync: failed to upsert vin=%s (user_id=%s) into cars",
            vin, user_id,
        )
        return SYNC_FAILED
    return SYNC_PUBLISHED


def remove_portal_vehicle_from_public_listings(vin: str, user_id: int) -> None:
    """Retire (not delete) the public ``cars`` row for a deleted portal vehicle.

    Mirrors how the scanner retires a VIN it no longer sees
    (``listing_active`` / ``listing_removed_at``) rather than deleting the
    row outright — deleting would lose any price-history / attribution rows
    keyed off ``cars.id``. Only touches the row stamped with THIS caller's
    own portal dealer_id (exact match, not the generic ``dealer-portal-*``
    prefix) — otherwise portal user B deleting their own (colliding) VIN
    could retire portal user A's real listing. Never raises.
    """
    v = str(vin or "").strip()
    if not v:
        return
    try:
        from datetime import datetime, timezone

        from backend.db.repositories.base_repo import db_conn

        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        with db_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                "UPDATE cars SET listing_active = 0, listing_removed_at = ? "
                "WHERE UPPER(TRIM(vin)) = ? AND dealer_id = ?",
                (now, v.upper(), dealer_portal_dealer_id(user_id)),
            )
            conn.commit()
    except Exception:
        _log.exception("dealer-portal sync: failed to retire vin=%s from cars", v)


def retire_portal_listings_for_user(user_id: int) -> None:
    """Retire every public ``cars`` row stamped with this portal user's dealer_id.

    Used when the account is deleted (their ``dealer_vehicles`` rows are gone,
    so nothing else would ever retire the mirrored listings) and when it is
    suspended (card S19 (4); un-suspending does not republish). Matches the exact
    ``dealer-portal-<id>`` only, so scanner rows and other sellers are never
    touched. Never raises.
    """
    try:
        from datetime import datetime, timezone

        from backend.db.repositories.base_repo import db_conn

        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        with db_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                "UPDATE cars SET listing_active = 0, listing_removed_at = ? "
                "WHERE dealer_id = ? AND listing_active = 1",
                (now, dealer_portal_dealer_id(user_id)),
            )
            conn.commit()
    except Exception:
        _log.exception("dealer-portal sync: failed to retire cars for user=%s", user_id)
