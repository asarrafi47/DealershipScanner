"""
AI vehicle-narration API.

Exposes ``GET /api/car/<int:car_id>/narrate``, which loads a car by id and
returns a grounded, AI-generated 2-3 sentence description produced by
``backend.utils.vehicle_narrator.narrate_vehicle``. The narrator is routed
through the provider layer (Claude in prod, local Ollama in dev) and only
narrates fields present on the row -- it never invents specs or prices.

Card C3: the route needs a signed-in account (the same check as the chat, so a signed-out
caller gets the existing login response), the ``X-CSRF-Token`` header (a GET carries the Lax
session cookie on a cross-site link, so without the header another site could make a signed-in
browser spend the chat budget), and a model call (a cache miss) runs under the chat gate
(``guard(kind="narrate", weight=1)``), sharing the shopper's chat allowance.

Responses:
    200 -> {"ok": true,  "description": "<prose>"}
    401 -> {"ok": false, "error": "login_required"}     # 403 premium_required with billing on
    403 -> {"ok": false, "error": "csrf_required"}
    404 -> {"ok": false, "error": "car not found"}
    429 -> {"ok": false, "error": "rate_limited"} or a chat-limit refusal (error, message, resets_at)
    500 -> {"ok": false, "error": "narration_failed"}   # detail is logged, not returned
"""
from __future__ import annotations

import hashlib
import logging
from collections import OrderedDict

from flask import Blueprint, jsonify, request
from werkzeug.exceptions import HTTPException

from backend.billing import access as paid_access
from backend.billing.catalog import FEATURE_AI_CAR_CHAT
from backend.chat_budget import ChatDenied
from backend.chat_budget import web as chat_web
from backend.db.inventory_db import get_car_by_id
from backend.utils.client_ip import client_ip
from backend.utils.csrf import validate_csrf_header
from backend.utils.vehicle_narrator import narrate_vehicle

_log = logging.getLogger(__name__)

ai_narrate_bp = Blueprint("ai_narrate_bp", __name__)


def _client_ip() -> str:
    return client_ip(request)

# Per-worker LRU cache: narration is deterministic given the row's content, so we
# cache by car_id + a hash of the narratable fields — a car edit changes the hash
# and re-generates; unchanged cars return instantly without hitting the model.
_CACHE_MAX = 2000
_cache: "OrderedDict[tuple, str]" = OrderedDict()
_CACHE_FIELDS = (
    "year", "make", "model", "trim", "body_style", "price", "msrp", "mileage",
    "exterior_color", "interior_color", "engine_description", "cylinders",
    "fuel_type", "transmission", "drivetrain", "mpg_city", "mpg_highway", "is_cpo",
)


def _cache_key(car_id: int, row: dict) -> tuple:
    blob = "|".join(str(row.get(k)) for k in _CACHE_FIELDS)
    return (car_id, hashlib.md5(blob.encode("utf-8")).hexdigest()[:16])


@ai_narrate_bp.route("/api/car/<int:car_id>/narrate", methods=["GET"])
def narrate_car(car_id: int):
    """Return an AI-generated description for the car with ``car_id``."""
    ok, err = paid_access.check_feature(FEATURE_AI_CAR_CHAT)
    if not ok:
        status = 401 if err == paid_access.LOGIN_REQUIRED else 403
        return jsonify(paid_access.denied_json(FEATURE_AI_CAR_CHAT, err)), status
    try:
        validate_csrf_header()
    except HTTPException:
        return jsonify({"ok": False, "error": "csrf_required"}), 403
    # Per-minute burst guard (the one function every chat route uses; global cap default 300).
    if not chat_web.burst_allowed(chat_web.BURST_NARRATE, _client_ip()):
        return jsonify({"ok": False, "error": "rate_limited"}), 429
    try:
        row = get_car_by_id(car_id, include_inactive=False)
        if row is None:
            return jsonify({"ok": False, "error": "car not found"}), 404

        # Normalize sqlite3.Row / mapping-like rows to a plain dict.
        try:
            row_dict = dict(row)
        except (TypeError, ValueError):
            row_dict = {k: row[k] for k in row.keys()}

        key = _cache_key(car_id, row_dict)
        cached = _cache.get(key)
        if cached is not None:
            _cache.move_to_end(key)
            return jsonify({"ok": True, "description": cached, "cached": True})

        # Only a cache miss calls the model, so only a cache miss is counted.
        try:
            with chat_web.open_guard(chat_web.KIND_NARRATE, listing_id=None, weight=1):
                description = narrate_vehicle(row_dict)
        except ChatDenied as denied:
            return chat_web.denial_response(denied.denial)
        _cache[key] = description
        _cache.move_to_end(key)
        while len(_cache) > _CACHE_MAX:
            _cache.popitem(last=False)
        return jsonify({"ok": True, "description": description})
    except Exception:  # noqa: BLE001 - surface as generic JSON 500
        # Log the detail server-side; never return the raw exception message to
        # an unauthenticated client (it can leak internal/provider details).
        _log.exception("narration failed for car_id=%s", car_id)
        return jsonify({"ok": False, "error": "narration_failed"}), 500
