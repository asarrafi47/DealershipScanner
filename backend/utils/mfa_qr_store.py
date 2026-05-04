"""Store pending QR MFA login attempts: Redis when REDIS_URL is set; in-memory in dev or when MFA_QR_INMEMORY=1."""

from __future__ import annotations

import json
import os
import secrets
import time
import threading
from typing import Any, Optional

from backend.utils.runtime_env import is_production_env

_ATTEMPT_TTL = int((os.environ.get("MFA_QR_ATTEMPT_TTL_SECONDS") or "120") or 120)
_APPROVED_TTL = int((os.environ.get("MFA_QR_APPROVED_TTL_SECONDS") or "120") or 120)
_REDIS_URL = (os.environ.get("REDIS_URL") or "").strip()
_INMEM_FLAG = (os.environ.get("MFA_QR_INMEMORY") or "").strip().lower() in ("1", "true", "yes", "on")
_KEY_PREFIX = "mfa_qr:attempt"
_LOCK = threading.Lock()
_MEM: dict[str, dict[str, Any]] = {}
_rclient: Any = None
_connect_failed = False


def is_redis_mfa_qr_configured() -> bool:
    return bool(_REDIS_URL)


def mfa_qr_channel_available() -> bool:
    """App may offer QR sign-in: Redis, or in-memory in non-production, or explicit MFA_QR_INMEMORY in prod (tests)."""
    if is_redis_mfa_qr_configured():
        return True
    if is_production_env():
        return _INMEM_FLAG
    return True


def _use_inmemory() -> bool:
    if is_redis_mfa_qr_configured():
        return False
    if is_production_env():
        return _INMEM_FLAG
    return True


def _get_redis():
    global _rclient, _connect_failed
    if _rclient is not None or _connect_failed:
        return _rclient
    if not is_redis_mfa_qr_configured():
        return None
    try:
        import redis
    except ImportError:  # pragma: no cover
        _connect_failed = True
        return None
    try:
        _rclient = redis.from_url(
            _REDIS_URL, decode_responses=True, health_check_interval=30
        )
        _rclient.ping()
    except Exception:  # noqa: BLE001
        _rclient = None
        _connect_failed = True
    return _rclient


def _k(attempt_id: str) -> str:
    return f"{_KEY_PREFIX}:{attempt_id}"


def _mem_get(attempt_id: str) -> Optional[dict[str, Any]]:
    with _LOCK:
        rec = _MEM.get(attempt_id)
        if not rec:
            return None
        if float(rec.get("exp", 0)) < time.time():
            _MEM.pop(attempt_id, None)
            return None
    return rec.get("data")  # type: ignore[return-value]


def _mem_set(attempt_id: str, data: dict[str, Any], ttl: int) -> None:
    with _LOCK:
        _MEM[attempt_id] = {"data": data, "exp": time.time() + float(max(1, ttl))}


def _mem_delete(attempt_id: str) -> None:
    with _LOCK:
        _MEM.pop(attempt_id, None)


def mfa_qr_create_attempt(
    *, user_id: int, mfa_intent: str, stream: str
) -> Optional[str]:
    """Create a new pending attempt. Returns unguessable token, or None if store unavailable."""
    attempt_id = secrets.token_urlsafe(32)
    payload = {
        "user_id": int(user_id),
        "mfa_intent": (mfa_intent or "").strip() or "general",
        "state": "pending",
        "form_nonce": "",
        "stream": (stream or "app").strip().lower() or "app",
    }
    r = _get_redis() if (not _use_inmemory()) else None
    if r:
        try:
            r.set(_k(attempt_id), json.dumps(payload), ex=max(1, _ATTEMPT_TTL))
        except Exception:  # noqa: BLE001
            return None
        return attempt_id
    if not _use_inmemory():
        return None
    _mem_set(attempt_id, payload, _ATTEMPT_TTL)
    return attempt_id


def mfa_qr_get(attempt_id: str) -> Optional[dict[str, Any]]:
    if not (attempt_id or "").strip():
        return None
    r = _get_redis() if (not _use_inmemory()) else None
    if r:
        try:
            raw = r.get(_k(attempt_id))
        except Exception:  # noqa: BLE001
            return None
        if not raw:
            return None
        try:
            d = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            return None
        if isinstance(d, dict):
            return d
        return None
    return _mem_get(attempt_id.strip())


def mfa_qr_set_form_nonce(attempt_id: str) -> Optional[str]:
    """Set a new form nonce for GET /mfa/qr-confirm (one POST per view). Returns None on failure."""
    d = mfa_qr_get(attempt_id) or None
    if not d or (d.get("state") or "") != "pending":
        return None
    n = secrets.token_hex(20)
    d = dict(d)
    d["form_nonce"] = n
    r = _get_redis() if (not _use_inmemory()) else None
    if r:
        try:
            pttl = r.pttl(_k(attempt_id))
            ttl = max(1, int((pttl / 1000) or _ATTEMPT_TTL) if pttl and pttl > 0 else _ATTEMPT_TTL)
            r.set(_k(attempt_id), json.dumps(d), ex=ttl)
        except Exception:  # noqa: BLE001
            return None
        return n
    with _LOCK:
        rec = _MEM.get(attempt_id)
        if not rec or float(rec.get("exp", 0)) < time.time():
            return None
    _mem_set(attempt_id, d, _ATTEMPT_TTL)
    return n


def mfa_qr_approve(attempt_id: str, ap_nonce: str) -> bool:
    """Device confirms: pending + matching nonce. Marks approved, extends TTL. Returns success."""
    d0 = mfa_qr_get(attempt_id) or None
    if not d0 or (d0.get("state") or "") != "pending":
        return False
    if not ap_nonce or ap_nonce != (d0.get("form_nonce") or ""):
        return False
    d = dict(d0)
    d["state"] = "approved"
    r = _get_redis() if (not _use_inmemory()) else None
    if r:
        try:
            r.set(_k(attempt_id), json.dumps(d), ex=max(1, _APPROVED_TTL))
        except Exception:  # noqa: BLE001
            return False
        return True
    _mem_set(attempt_id, d, _APPROVED_TTL)
    return True


def mfa_qr_consume_approved(attempt_id: str, user_id: int) -> bool:
    """One-time: approved attempt matches user; delete record. Returns True if finalization is allowed."""
    d = mfa_qr_get(attempt_id) or None
    if not d or (d.get("state") or "") != "approved":
        return False
    try:
        uid = int(d.get("user_id") or 0)
    except (TypeError, ValueError):
        return False
    if uid != int(user_id):
        return False
    r = _get_redis() if (not _use_inmemory()) else None
    if r:
        try:
            r.delete(_k(attempt_id))
        except Exception:  # noqa: BLE001
            return False
    else:
        _mem_delete(attempt_id)
    return True