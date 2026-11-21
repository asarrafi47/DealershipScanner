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
someone else's VIN.
"""
from __future__ import annotations

import logging
import os
from typing import Any
from urllib.parse import urljoin

from backend.config import Config

_log = logging.getLogger(__name__)

# S19: until phone-verified private sellers exist (S22), a car saved in the portal is copied to the
# public ``cars`` table only for accounts that already had a vehicle before this release, or when
# the owner turns posting on with the environment variable below (environment only, default off).
MARKETPLACE_POSTING_ENV = "MARKETPLACE_POSTING_ENABLED"
# An account is grandfathered when it had at least one ``dealer_vehicles`` row created before this
# instant. The PR that ships S19 lists those accounts by id for the owner to check.
GRANDFATHER_CUTOFF = "2026-11-21T00:00:00+00:00"

POSTING_HELD_MESSAGE = "Posting cars publicly opens soon. Your car is saved."
VIN_COLLISION_MESSAGE = "This VIN is already listed on the site; it was saved but not published."

# What :func:`sync_portal_vehicle_to_public_listings` did, so the page can tell the seller.
SYNC_PUBLISHED = "published"
SYNC_HELD = "held"  # saved, not copied: posting is not open to this account yet
SYNC_COLLISION = "collision"  # saved, not copied: the VIN is already listed by someone else
SYNC_SKIPPED = "skipped"  # nothing to do (no VIN)
SYNC_FAILED = "failed"


def marketplace_posting_enabled() -> bool:
    return (os.environ.get(MARKETPLACE_POSTING_ENV) or "").strip().lower() in ("1", "true", "yes", "on")


def is_grandfathered(user_id: int) -> bool:
    """True when the account had a ``dealer_vehicles`` row before :data:`GRANDFATHER_CUTOFF`.

    Fails closed: an unreadable portal database means "not grandfathered" (logged).
    """
    try:
        from backend.db import dealer_portal_db as ddb

        cutoff = GRANDFATHER_CUTOFF[:19]
        return any(str(v.get("created_at") or "9999")[:19] < cutoff for v in ddb.list_vehicles_for_user(int(user_id)))
    except Exception:
        _log.exception("dealer-portal sync: could not check grandfathering for user=%s", user_id)
        return False


def may_post_publicly(user_id: int) -> bool:
    return marketplace_posting_enabled() or is_grandfathered(user_id)

# Namespace prefix for cars.dealer_id on portal-sourced rows, so they can
# never collide with a real scanned rooftop's dealer_id and are easy to spot.
DEALER_PORTAL_DEALER_ID_PREFIX = "dealer-portal-"


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


def _vin_sync_state(vin: str, user_id: int) -> str:
    """``"ok"``, ``"collision"`` (the VIN is someone else's row) or ``"unreadable"`` (could not check)."""
    try:
        from backend.db.repositories.cars_repo import get_car_by_vin

        existing = get_car_by_vin(vin)
    except Exception:
        # Read failure: fail closed (skip sync) rather than risk overwriting
        # a row we could not inspect.
        _log.exception("dealer-portal sync: could not check existing row for vin=%s", vin)
        return "unreadable"
    if existing is None or existing.get("dealer_id") == dealer_portal_dealer_id(user_id):
        return "ok"
    return "collision"


def _vin_is_safe_to_sync(vin: str, user_id: int) -> bool:
    """Refuse to sync onto a VIN owned by a scanned listing OR another portal user.

    Matches the row's ``dealer_id`` to THIS caller's own portal dealer_id, not
    just "any dealer-portal-* id" — otherwise portal user B could silently
    overwrite portal user A's listing by entering A's VIN.
    """
    return _vin_sync_state(vin, user_id) == "ok"


def sync_portal_vehicle_to_public_listings(user_id: int, vehicle: dict[str, Any]) -> str:
    """Upsert one dealer-portal vehicle into the public ``cars`` table.

    Call after create/update/photo-gallery changes so the vehicle appears on
    ``/listings`` and its car page. Never raises. Returns one of the ``SYNC_*``
    values so the caller can tell the seller what happened.
    """
    vin = str(vehicle.get("vin") or "").strip()
    if not vin:
        return SYNC_SKIPPED
    try:
        if not may_post_publicly(user_id):
            return SYNC_HELD
        state = _vin_sync_state(vin, user_id)
        if state != "ok":
            _log.warning(
                "dealer-portal sync: vin=%s (user_id=%s) collides with an existing "
                "listing owned by someone else; skipping public sync to avoid "
                "overwriting another dealer's data",
                vin, user_id,
            )
            return SYNC_COLLISION if state == "collision" else SYNC_FAILED

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
        return SYNC_PUBLISHED
    except Exception:
        _log.exception(
            "dealer-portal sync: failed to upsert vin=%s (user_id=%s) into cars",
            vin, user_id,
        )
        return SYNC_FAILED


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
    so nothing else would ever retire the mirrored listings). Never raises.
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
