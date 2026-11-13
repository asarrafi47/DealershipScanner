"""``POST /webhooks/twilio/verify``: Twilio's delivery-status callbacks (DESIGN.md section 17).

* Answers 404 while ``AUTH_PHONE_LOGIN_ENABLED`` is off.
* Verifies ``X-Twilio-Signature`` with the ``twilio`` request validator against the **public** URL
  (``PUBLIC_BASE_URL`` + path + query), not the URL this server saw behind its proxies. A missing or
  bad signature (or no public URL configured) gets 403 and nothing changes. CSRF does not apply:
  the signature is the proof (listed in ``backend/web/security.py``).
* Hands a normalised event to a ``SendEventSink``. The table that holds send rows arrives with
  card S5 and its sink with S6; until a sink is installed events are acknowledged and dropped.
* Unknown events get 200 and are ignored; repeats of one event id apply once.
* Logs an event name, a status and a result. Never a body, a number, a code or a header.
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Protocol

from flask import Blueprint, abort, jsonify, request

from backend.auth.phone import config
from backend.config import Config

_log = logging.getLogger("auth_phone")

bp = Blueprint("twilio_webhook", __name__)

MAX_BODY_BYTES = 64 * 1024

# Delivery outcomes we act on, by the spelling Twilio uses (lower case).
_STATUS_ALIASES = {
    "sent": "sent",
    "delivered": "delivered",
    "failed": "failed",
    "undelivered": "undelivered",
    "verification.delivered": "delivered",
    "verification.failed": "failed",
    "verification.sent": "sent",
}

RESULT_APPLIED = "applied"
RESULT_DUPLICATE = "duplicate"
RESULT_IGNORED = "ignored"


@dataclass(frozen=True)
class WebhookEvent:
    event_id: str  # the vendor's id for this delivery event (idempotency key)
    provider_ref: str | None  # the vendor's verification or message id: how the send row is found
    status: str  # sent | delivered | failed | undelivered
    cost_cents: float | None  # only when the event carries a price


class SendEventSink(Protocol):
    def apply(self, event: WebhookEvent) -> str:
        """Update the send row's outcome and cost estimate. Returns a RESULT_* string. Must be
        idempotent by ``event.event_id`` (the durable version is a unique index)."""
        ...


class NullSendEventSink:
    """Used until the send table exists (S5) and its sink is wired (S6): acknowledge, store nothing."""

    def apply(self, event: WebhookEvent) -> str:
        return RESULT_IGNORED


class MemorySendEventSink:
    """For tests: remembers applied events by id."""

    def __init__(self) -> None:
        self.events: dict[str, WebhookEvent] = {}

    def apply(self, event: WebhookEvent) -> str:
        if event.event_id in self.events:
            return RESULT_DUPLICATE
        self.events[event.event_id] = event
        return RESULT_APPLIED


_sink: SendEventSink = NullSendEventSink()
_state_lock = threading.Lock()
_seen_ids: OrderedDict[str, None] = OrderedDict()
_SEEN_MAX = 10000
bad_signature_count = 0


def set_sink(sink: SendEventSink | None) -> None:
    global _sink
    _sink = sink if sink is not None else NullSendEventSink()


def reset_state() -> None:
    """Forget seen event ids and the bad-signature count (tests)."""
    global bad_signature_count
    with _state_lock:
        _seen_ids.clear()
        bad_signature_count = 0


def _first_sight(event_id: str) -> bool:
    with _state_lock:
        if event_id in _seen_ids:
            return False
        _seen_ids[event_id] = None
        while len(_seen_ids) > _SEEN_MAX:
            _seen_ids.popitem(last=False)
        return True


def _forget(event_id: str) -> None:
    with _state_lock:
        _seen_ids.pop(event_id, None)


def public_url_for_request() -> str | None:
    """The exact URL Twilio called: the configured public base, this path, this query string."""
    base = Config.public_base_url().rstrip("/")
    if not base:
        return None
    url = base + request.path
    query = request.query_string.decode("utf-8", "replace")
    return url + ("?" + query if query else "")


def signature_is_valid(url: str, payload: Any, signature: str) -> bool:
    from twilio.request_validator import RequestValidator

    for secret in config.twilio_webhook_secrets():
        try:
            if RequestValidator(secret).validate(url, payload, signature):
                return True
        except Exception:  # noqa: BLE001 - a malformed payload is simply not valid
            return False
    return False


def _price_to_cents(value: Any) -> float | None:
    try:
        return round(abs(float(value)) * 100, 3)
    except (TypeError, ValueError):
        return None


def parse_event(fields: dict[str, Any]) -> WebhookEvent | None:
    """A WebhookEvent from a form body or a JSON body, or None when it is not a delivery event.

    The field names are Twilio's classic status-callback names plus a JSON shape with the event in
    ``data``; confirm against a real callback on the dev service (PR notes).
    """
    data = fields.get("data") if isinstance(fields.get("data"), dict) else fields

    def pick(*names: str) -> str | None:
        for name in names:
            for source in (fields, data):
                value = source.get(name)
                if isinstance(value, str) and value.strip():
                    return value.strip()
        return None

    raw_status = pick("MessageStatus", "SmsStatus", "status", "type")
    status = _STATUS_ALIASES.get((raw_status or "").lower())
    ref = pick("VerificationSid", "verification_sid", "MessageSid", "SmsSid", "sid")
    event_id = pick("EventSid", "event_sid", "event_id", "id")
    if status is None or ref is None:
        return None
    if event_id is None:
        event_id = "%s:%s" % (ref, status)  # one status per message is one event
    return WebhookEvent(event_id=event_id, provider_ref=ref, status=status,
                        cost_cents=_price_to_cents(pick("Price", "price")))


def _short(value: str | None) -> str:
    return hashlib.sha256((value or "").encode("utf-8")).hexdigest()[:8]


@bp.route("/webhooks/twilio/verify", methods=["POST"])
def twilio_verify_webhook():
    global bad_signature_count
    if not config.enabled():
        abort(404)
    if request.content_length is not None and request.content_length > MAX_BODY_BYTES:
        return jsonify({"ok": False}), 413

    url = public_url_for_request()
    signature = request.headers.get("X-Twilio-Signature") or ""
    is_json = bool(request.mimetype and request.mimetype.endswith("json"))
    raw = request.get_data(cache=True, as_text=False) or b""
    payload: Any = raw.decode("utf-8", "replace") if is_json else request.form.to_dict(flat=True)
    if not url or not signature or not signature_is_valid(url, payload, signature):
        with _state_lock:
            bad_signature_count += 1
        _log.warning("event=webhook_bad_signature")
        return jsonify({"ok": False}), 403

    fields: dict[str, Any]
    if is_json:
        try:
            parsed = json.loads(raw.decode("utf-8", "replace") or "{}")
        except ValueError:
            parsed = {}
        fields = parsed if isinstance(parsed, dict) else {}
    else:
        fields = payload
    event = parse_event(fields)
    if event is None:
        _log.info("event=webhook_ignored")
        return jsonify({"ok": True})

    if not _first_sight(event.event_id):
        _log.info("event=webhook_event status=%s result=%s ref=%s", event.status, RESULT_DUPLICATE, _short(event.provider_ref))
        return jsonify({"ok": True})
    try:
        result = _sink.apply(event)
    except Exception:  # noqa: BLE001 - let the vendor retry; log no text
        _forget(event.event_id)
        _log.error("event=webhook_sink_error status=%s", event.status)
        return jsonify({"ok": False}), 500
    _log.info("event=webhook_event status=%s result=%s ref=%s", event.status, result, _short(event.provider_ref))
    return jsonify({"ok": True})
