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
from backend.utils.car_chat_policy import (
    car_chat_rate_limits,
    car_chat_user_daily_limit,
    web_research_playwright_allowed,
)
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

# Narrow task the local model handles reliably: rewrite a fuzzy shopping request
# into a concrete search phrase, or NONE. The phrase is re-parsed by the local
# parser, so no hallucinated filter ever reaches results; NONE routes to a normal
# conversational answer (so questions are never misclassified as searches).
_REWRITE_PROMPT = (
    'A car shopper said: "{msg}"\n\n'
    "If they are looking for a vehicle, rewrite it as a short concrete search "
    "phrase using standard filters: make, model, body type "
    "(SUV/sedan/coupe/truck/van/convertible/hatchback/wagon/minivan), drivetrain "
    "(AWD/FWD/RWD), fuel (gas/hybrid/electric/diesel), a max price like "
    "'under $30,000', a max mileage, or a year. Translate lifestyle to filters: "
    "family -> SUV or minivan; sporty/fast -> coupe or convertible; good on gas -> "
    "hybrid; cheap / first car / budget -> a low max price.\n"
    "If it is NOT a car search (a question, greeting, or chit-chat), reply with "
    "exactly NONE.\n\n"
    "Output only the rewritten phrase, or NONE."
)


def _client_ip() -> str:
    fwd = (request.headers.get("X-Forwarded-For") or "").split(",")[0].strip()
    return fwd or (request.remote_addr or "unknown")


# Filter keys that signal the user is searching inventory (not asking a question).
_SEARCH_SIGNAL_KEYS = frozenset({
    "make", "model", "max_price", "max_mileage", "body_style", "drivetrain",
    "fuel_type", "min_year", "max_year", "exterior_color", "cylinders", "transmission",
})
_QUESTION_STARTS = ("how ", "what ", "why ", "is ", "are ", "does ", "do ", "can ",
                    "should ", "which ", "who ", "when ", "tell me", "explain")


def _looks_like_search(filters: dict, message: str) -> bool:
    """Non-empty structured filters + not phrased purely as a question → a search."""
    if not any(k in filters for k in _SEARCH_SIGNAL_KEYS):
        return False
    m = message.strip().lower()
    if any(k in filters for k in ("max_price", "max_mileage")) or m.startswith(
        ("show", "find", "list", "search", "looking for", "i want", "i need")
    ):
        return True
    return not m.startswith(_QUESTION_STARTS)


def _summarize_filters(filters: dict) -> str:
    """Human phrase of what was understood, e.g. 'BMW X5, under $50,000, AWD'."""
    parts: list[str] = []

    def _first(v):
        return v[0] if isinstance(v, list) and v else v

    yr_lo, yr_hi = filters.get("min_year"), filters.get("max_year")
    if yr_lo and yr_hi and yr_lo == yr_hi:
        parts.append(str(yr_lo))
    elif yr_lo:
        parts.append(f"{yr_lo}+")
    if filters.get("make"):
        parts.append(str(_first(filters["make"])))
    if filters.get("model"):
        parts.append(str(_first(filters["model"])))
    if filters.get("body_style"):
        parts.append(str(_first(filters["body_style"])).split("/")[0])
    if filters.get("drivetrain"):
        parts.append(str(_first(filters["drivetrain"])))
    if filters.get("fuel_type"):
        parts.append(str(filters["fuel_type"]))
    if filters.get("exterior_color"):
        parts.append(str(_first(filters["exterior_color"])))
    if filters.get("max_price"):
        try:
            parts.append(f"under ${int(float(filters['max_price'])):,}")
        except (TypeError, ValueError):
            pass
    if filters.get("max_mileage"):
        try:
            parts.append(f"under {int(float(filters['max_mileage'])):,} mi")
        except (TypeError, ValueError):
            pass
    return ", ".join(parts)


@ai_chat_bp.route("/api/ai/chat", methods=["POST"])
def api_ai_chat():
    ok, err = require_feature(session, FEATURE_AI_CAR_CHAT)
    if not ok:
        return jsonify({"ok": False, "error": err or "feature_unavailable"}), 403

    # Rate limits mirror the car-page chat: global, per-IP, and per-user/day.
    ip = _client_ip()
    rpm_pair, rpm_ip, rpm_global = car_chat_rate_limits()
    if rpm_global > 0 and not allow_request("aichat:global", max_events=rpm_global, window_seconds=60.0):
        return jsonify({"ok": False, "error": "rate_limited"}), 429
    if not allow_request(f"aichat:ip:{ip}", max_events=rpm_ip, window_seconds=60.0):
        return jsonify({"ok": False, "error": "rate_limited"}), 429
    daily_limit = car_chat_user_daily_limit()
    if daily_limit > 0:
        uid = session.get("user_id")
        daily_key = f"aichat:daily:user:{int(uid)}" if uid else f"aichat:daily:ip:{ip}"
        if not allow_request(daily_key, max_events=daily_limit, window_seconds=86400.0):
            return jsonify({"ok": False, "error": "user_chat_limit_reached"}), 429

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
            cid = int(car_id)
        except (TypeError, ValueError):
            cid = None
        if cid is not None:
            try:
                from backend.db.inventory_db import get_car_by_id

                row = get_car_by_id(cid, include_inactive=False)
                if row:
                    context_car = dict(row)
            except Exception as e:
                logger.warning("ai chat car lookup failed (id=%s): %s", cid, str(e)[:150])

    try:
        if context_car is not None:
            from backend.intelligence.ai.agent import run_car_page_chat

            # Honor the same web-research policy as the car-page chat: when the row
            # doesn't answer it, look it up (the "go find out" fallback).
            allow_web = web_research_playwright_allowed(session.get("user_id"))
            out = run_car_page_chat(context_car, message, allow_web_research=allow_web)
            reply = (out.get("reply") or "").strip()
            if out.get("error") and not reply:
                return jsonify({"ok": False, "error": out["error"]}), 502
            return jsonify({"ok": True, "reply": reply, "context": "car",
                            "car_id": context_car.get("id")})
        # No car in view — search intent first (proven local parser), else a general answer.
        try:
            from backend.utils.query_parser import parse_natural_query

            filters = parse_natural_query(message) or {}
        except Exception:
            filters = {}
        if _looks_like_search(filters, message):
            from urllib.parse import quote

            summary = _summarize_filters(filters)
            reply = (f"Here are matching listings — {summary}." if summary
                     else "Here are matching listings.")
            return jsonify({
                "ok": True, "context": "search", "reply": reply,
                "search": {"url": "/listings?q=" + quote(message), "summary": summary,
                           "filters": filters, "q": message},
            })

        # Parser found nothing concrete. Try to rewrite fuzzy intent into a search
        # phrase (narrow task); only if that yields real filters is it a search —
        # otherwise answer conversationally, so questions are never mis-routed.
        from backend.utils.llm_client import complete

        rewrite = complete(_REWRITE_PROMPT.format(msg=message[:500]),
                           temperature=0.0, max_tokens=48).strip().strip('"').strip()
        if rewrite and not rewrite.upper().startswith("NONE"):
            try:
                rf = parse_natural_query(rewrite) or {}
            except Exception:
                rf = {}
            if any(k in rf for k in _SEARCH_SIGNAL_KEYS):
                from urllib.parse import quote

                summary = _summarize_filters(rf)
                reply = (f"Here are matching listings — {summary}." if summary
                         else "Here are matching listings.")
                return jsonify({
                    "ok": True, "context": "search", "reply": reply,
                    "search": {"url": "/listings?q=" + quote(rewrite), "summary": summary,
                               "filters": rf, "q": rewrite},
                })

        reply = complete(message, system=_GENERAL_SYSTEM, temperature=0.4, max_tokens=300).strip()
        return jsonify({"ok": True, "reply": reply, "context": "general"})
    except Exception as e:
        logger.warning("ai chat failed: %s", str(e)[:200])
        return jsonify({"ok": False, "error": "assistant_unavailable"}), 502
