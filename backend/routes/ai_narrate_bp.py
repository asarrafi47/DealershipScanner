"""
AI vehicle-narration API.

Exposes ``GET /api/car/<int:car_id>/narrate``, which loads a car by id and
returns a grounded, AI-generated 2-3 sentence description produced by
``backend.utils.vehicle_narrator.narrate_vehicle``. The narrator is routed
through the provider layer (Claude in prod, local Ollama in dev) and only
narrates fields present on the row -- it never invents specs or prices.

Responses:
    200 -> {"ok": true,  "description": "<prose>"}
    404 -> {"ok": false, "error": "car not found"}
    500 -> {"ok": false, "error": "<message>"}
"""
from __future__ import annotations

import hashlib
import logging
from collections import OrderedDict

from flask import Blueprint, jsonify, request

from backend.db.inventory_db import get_car_by_id
from backend.utils.car_chat_policy import car_chat_rate_limits
from backend.utils.client_ip import client_ip
from backend.utils.ip_rate_limit import allow_request
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
    # Rate-limit: this triggers a paid model call in prod, so cap per-IP and
    # globally to prevent cost-amplification / DoS on an otherwise public GET.
    ip = _client_ip()
    _pair, rpm_ip, rpm_global = car_chat_rate_limits()
    if rpm_global > 0 and not allow_request(
        "narrate:global", max_events=rpm_global, window_seconds=60.0
    ):
        return jsonify({"ok": False, "error": "rate_limited"}), 429
    if not allow_request(f"narrate:ip:{ip}", max_events=max(rpm_ip, 30), window_seconds=60.0):
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

        description = narrate_vehicle(row_dict)
        _cache[key] = description
        _cache.move_to_end(key)
        while len(_cache) > _CACHE_MAX:
            _cache.popitem(last=False)
        return jsonify({"ok": True, "description": description})
    except Exception as exc:  # noqa: BLE001 - surface as JSON 500
        _log.exception("narration failed for car_id=%s", car_id)
        return jsonify({"ok": False, "error": str(exc)}), 500
