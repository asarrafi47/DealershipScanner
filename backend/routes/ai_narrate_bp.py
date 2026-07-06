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

import logging

from flask import Blueprint, jsonify

from backend.db.inventory_db import get_car_by_id
from backend.utils.vehicle_narrator import narrate_vehicle

_log = logging.getLogger(__name__)

ai_narrate_bp = Blueprint("ai_narrate_bp", __name__)


@ai_narrate_bp.route("/api/car/<int:car_id>/narrate", methods=["GET"])
def narrate_car(car_id: int):
    """Return an AI-generated description for the car with ``car_id``."""
    try:
        row = get_car_by_id(car_id, include_inactive=False)
        if row is None:
            return jsonify({"ok": False, "error": "car not found"}), 404

        # Normalize sqlite3.Row / mapping-like rows to a plain dict.
        try:
            row_dict = dict(row)
        except (TypeError, ValueError):
            row_dict = {k: row[k] for k in row.keys()}

        description = narrate_vehicle(row_dict)
        return jsonify({"ok": True, "description": description})
    except Exception as exc:  # noqa: BLE001 - surface as JSON 500
        _log.exception("narration failed for car_id=%s", car_id)
        return jsonify({"ok": False, "error": str(exc)}), 500
