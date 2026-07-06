"""
Global AI assistant endpoint for the sidebar chatbot.

POST /api/ai/chat  {message, car_id?}

Works from any page (unlike the car-page-only /api/car/<id>/chat). When the user
is viewing a car, the frontend passes its id so the assistant answers with that
car as context ("is this a good deal?"); otherwise it answers general car-shopping
questions. Gated to paid/dev users via the ai_car_chat entitlement, same as the
existing car chat. CSRF is enforced globally by main.py's before_request.
"""
from __future__ import annotations

import logging

from flask import Blueprint, jsonify, request, session

from backend.billing.catalog import FEATURE_AI_CAR_CHAT
from backend.billing.entitlements import require_feature
from backend.utils.ip_rate_limit import allow_request

logger = logging.getLogger("ai_chat")

ai_chat_bp = Blueprint("ai_chat_bp", __name__)

_MAX_MESSAGE = 2000

_GENERAL_SYSTEM = (
    "You are a concise, honest car-shopping assistant for the Sarrafi Cars "
    "marketplace. Answer the user's question helpfully in 1-4 sentences. If the "
    "question is about a specific vehicle's details you were not given, say you'd "
    "need them to open that listing. Do not invent specific prices, specs, or "
    "inventory you have not been shown."
)


def _client_ip() -> str:
    fwd = (request.headers.get("X-Forwarded-For") or "").split(",")[0].strip()
    return fwd or (request.remote_addr or "unknown")


@ai_chat_bp.route("/api/ai/chat", methods=["POST"])
def api_ai_chat():
    ok, err = require_feature(session, FEATURE_AI_CAR_CHAT)
    if not ok:
        return jsonify({"ok": False, "error": err or "feature_unavailable"}), 403

    ip = _client_ip()
    if not allow_request(f"aichat:ip:{ip}", max_events=20, window_seconds=60.0):
        return jsonify({"ok": False, "error": "rate_limited"}), 429

    body = request.get_json(silent=True) or {}
    message = (body.get("message") or "").strip()
    if not message:
        return jsonify({"ok": False, "error": "message_required"}), 400
    if len(message) > _MAX_MESSAGE:
        return jsonify({"ok": False, "error": "message_too_long"}), 400

    car_id = body.get("car_id")
    context_car = None
    if car_id not in (None, "", "null"):
        try:
            from backend.db.inventory_db import get_car_by_id

            row = get_car_by_id(int(car_id), include_inactive=False)
            if row:
                context_car = dict(row)
        except (TypeError, ValueError, Exception):
            context_car = None

    try:
        if context_car is not None:
            from backend.intelligence.ai.agent import run_car_page_chat

            out = run_car_page_chat(context_car, message, allow_web_research=False)
            reply = (out.get("reply") or "").strip()
            if out.get("error") and not reply:
                return jsonify({"ok": False, "error": out["error"]}), 502
            return jsonify({"ok": True, "reply": reply, "context": "car",
                            "car_id": context_car.get("id")})
        # No car in view — general assistant answer.
        from backend.utils.llm_client import complete

        reply = complete(message, system=_GENERAL_SYSTEM, temperature=0.4, max_tokens=400).strip()
        return jsonify({"ok": True, "reply": reply, "context": "general"})
    except Exception as e:
        logger.warning("ai chat failed: %s", str(e)[:200])
        return jsonify({"ok": False, "error": "assistant_unavailable"}), 502
