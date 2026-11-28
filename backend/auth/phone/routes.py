"""Phone sign-in pages and JSON twins (card S8, DESIGN.md section 15).

Routes (all answer 404 while ``AUTH_PHONE_LOGIN_ENABLED`` is off; the flag is read on every request):

    GET  /login/phone            the page: number, Turnstile, consent box, "Text me a code"
    POST /login/phone            start (form CSRF token, Turnstile token, consent)
    GET  /login/phone/code       the code page
    POST /login/phone/code       check the code, sign in
    POST /login/phone/resend     another code (its own Turnstile token)
    POST /login/phone/change     "Use a different number"
    POST /api/auth/phone/start   JSON twins for the iOS app (header X-CSRF-Token)
    POST /api/auth/phone/verify
    POST /api/auth/phone/resend

With the flag on, ``GET /login`` is this page too (``pages.login_page`` calls :func:`render_login_entry`)
and the old email and password form moves to ``/staff/login`` (``pages.staff_login_page``).

Rules this module keeps:

* Every sentence a person reads is the service's own (``AuthPhoneService`` returns it as ``.sentence``);
  nothing is derived from a reason code. Known and unknown numbers get the same words and the same status.
* The phone number is never stored, logged or put in a URL. The Flask session holds only the challenge's
  public id, the per-browser nonce, the service's "we sent a code to the number ending NN" sentence and
  the time it was sent. A page that fails does not echo the number back.
* The nonce is minted here before the first send and reused, so a wrong-code lock keyed by nonce survives
  a new start; the device hash is ``None`` unless the request really carried a valid ``sc_dev`` cookie
  (the service counts cookieless sends separately), derived the same way in start, resend and check.
* After an approved code: ``AuthPhoneService.sign_in`` finds or makes the account, ``is_phone_eligible``
  refuses admins, dealers, staff and suspended accounts with the "wrong code" sentence (no hint that the
  number is tied to an account), then ``session.clear()`` and ``finalize_app_session(auth_method="phone")``.
* Nothing here talks to a vendor except through the service; a failure of the service, the database or
  the provider answers "not available right now" and logs only the exception class (``event=route_error``).
"""

from __future__ import annotations

import logging
import math
import secrets
import threading
import time
from dataclasses import dataclass
from typing import Any

from flask import abort, jsonify, make_response, redirect, render_template, request, session, url_for

from backend.auth.phone import config as phone_config
from backend.auth.phone import consent, numbers, registry, turnstile
from backend.auth.phone import service as ps
from backend.auth.phone.endpoints import PHONE_FLAG_ENDPOINTS
from backend.config import Config
from backend.utils.client_ip import phone_client_ip
from backend.utils.device_id import read_device_id
from backend.utils.ip_rate_limit import allow_request
from backend.web.request_scope import NO_SERVER_TIMING_KEY
from backend.web.security import allow_turnstile_csp

_log = logging.getLogger("auth_phone")

# The phone answers carry no Server-Timing header (the staff form keeps it).
_NO_TIMING_ENDPOINTS = PHONE_FLAG_ENDPOINTS - {"staff_login_page"}

# Flask session keys (a cookie: signed, not encrypted, so nothing sensitive goes in).
_K_CID = "phone_cid"  # the open challenge's public id
_K_NONCE = "phone_nonce"  # this browser's binding nonce
_K_MSG = "phone_sent_msg"  # "We sent a 6-digit code to the number ending 23."
_K_SENT_AT = "phone_sent_at"  # epoch seconds of the last send

TURNSTILE_FIELD = "cf-turnstile-response"
SESSION_EXPIRED_MESSAGE = "Your session expired — please log in again."

# A "retry" from sign_in means another tab is still making the account (it hashes a random password,
# about a quarter of a second at the production cost, longer on a busy worker); it is safe to ask
# again. The code is already spent, so giving up costs the person a new text: wait up to ~2.5 s.
_SIGN_IN_TRIES = 6
_SIGN_IN_PAUSE_S = 0.5
_sleep = time.sleep  # tests replace it so the retry path does not wait

# Review R7: every start and resend answer is padded to this floor, so its timing does not tell a number
# with an account (no line-type lookup) from one without. Above the slowest normal path (Turnstile, then
# Lookup, then the Verify start); a Turnstile or vendor timeout may still run past it.
START_MIN_SECONDS = 2.5
_monotonic = time.monotonic  # tests replace both so the floor costs no real time
_pad_sleep = time.sleep


def _padded(fn):
    """Run ``fn`` and wait until :data:`START_MIN_SECONDS` have passed since it started (review R7)."""
    import functools

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        started = _monotonic()
        try:
            return fn(*args, **kwargs)
        finally:
            left = START_MIN_SECONDS - (_monotonic() - started)
            if left > 0:
                _pad_sleep(left)

    return wrapper


# Reasons after which the open challenge is over (the person must ask for a new code).
_CHALLENGE_ENDED = frozenset({ps.R_NO_CHALLENGE, ps.R_EXPIRED, ps.R_TOO_MANY})

# JSON error word and HTTP status by the SENTENCE, never by the reason: several reasons share a
# sentence and must answer identically (known and unknown numbers, and every refusal that reads alike).
_ERROR_CODES = {
    ps.COULD_NOT_SEND: "could_not_send",
    ps.BUSY: "busy",
    ps.SELLER_BUSY: "busy",
    ps.UNAVAILABLE: "unavailable",
    ps.OPTED_OUT: "opted_out",
    ps.CONSENT_REQUIRED: "consent_required",
    ps.NOT_A_PERSON: "turnstile_failed",
    ps.RELOAD: "reload",
    ps.RESEND_TOO_SOON: "too_soon",
    ps.CODE_FAILED: "code_failed",
    ps.CODE_LOCKED: "code_locked",
    ps.CODE_RETRY: "try_again",
    ps.TRY_AGAIN: "try_again",
    ps.START_AGAIN: "start_again",
    ps.CANCELLED: "cancelled",
}
_STATUS_TOO_MANY = frozenset({ps.BUSY, ps.SELLER_BUSY, ps.TRY_AGAIN, ps.CODE_LOCKED, ps.RESEND_TOO_SOON})
_STATUS_DOWN = frozenset({ps.UNAVAILABLE, ps.OPTED_OUT, ps.CODE_RETRY})


def _http_status(sentence: str) -> int:
    if sentence in _STATUS_TOO_MANY:
        return 429
    if sentence in _STATUS_DOWN:
        return 503
    return 400


def _html_status(sentence: str) -> int:
    """A failed form keeps HTTP 200 (like the password form) unless the answer is "busy" or "down"."""
    status = _http_status(sentence)
    return status if status in (429, 503) else 200


def _error_code(sentence: str) -> str:
    return _ERROR_CODES.get(sentence, "error")


# ---------------------------------------------------------------- the service seam

_svc_lock = threading.Lock()
_svc: Any = None


class _LiveTurnstile:
    """Asks whatever verifier is installed now (``turnstile.set_verifier`` in tests), not the one
    that existed when the service was built."""

    def verify(self, token: str | None, ip: str | None) -> Any:
        return turnstile.verify_turnstile(token, ip)


def get_service() -> Any:
    """The one ``AuthPhoneService`` of this process. A failure to build it is raised, never cached."""
    global _svc
    with _svc_lock:
        if _svc is None:
            _svc = ps.AuthPhoneService(registry.get_provider(), _LiveTurnstile())
        return _svc


def set_service(service: Any) -> None:
    """Use ``service`` (tests: a stub, or one built over a fake provider)."""
    global _svc
    with _svc_lock:
        _svc = service


def reset_service() -> None:
    set_service(None)


# ---------------------------------------------------------------- small helpers


def _require_enabled() -> None:
    if not phone_config.enabled():
        abort(404)


def _guard(call):
    """Run ``call()``; on any failure answer "not available right now" and log the exception class only
    (a driver error can carry a number or a statement). Fails closed, never a 500."""
    try:
        return call(), None
    except Exception as exc:
        _log.warning("event=route_error reason=%s", type(exc).__name__)
        return None, ps.UNAVAILABLE


def _ip_throttled() -> bool:
    """A cheap per-address request limit in front of the Turnstile call and the database work, like the
    password form has (``RATE_LIMIT_LOGIN_PER_MIN`` per minute, one bucket for start, resend and check).
    It is in-process (and in Redis when ``REDIS_URL`` is set), so a flood of unsolved or made-up
    requests is turned away before it costs a Cloudflare call or a database row."""
    return not allow_request(
        f"phone:{phone_client_ip(request)}", max_events=Config.RATE_LIMIT_LOGIN_PER_MIN, window_seconds=60.0
    )


def _device_hash() -> str | None:
    """Hash of the device id in a valid ``sc_dev`` cookie the request really carried, else None."""
    device_id = read_device_id(request)
    return numbers.hash_device(device_id) if device_id else None


def _browser_nonce() -> str:
    nonce = session.get(_K_NONCE)
    if not isinstance(nonce, str) or len(nonce) < 8:
        nonce = secrets.token_urlsafe(18)
        session[_K_NONCE] = nonce
    return nonce


def _challenge_ids() -> tuple[str | None, str | None]:
    cid, nonce = session.get(_K_CID), session.get(_K_NONCE)
    if isinstance(cid, str) and cid and isinstance(nonce, str) and nonce:
        return cid, nonce
    return None, None


def _remember_challenge(outcome: Any, nonce: str) -> None:
    session.permanent = True  # a person may leave the browser to read the text message
    session[_K_CID] = outcome.public_id
    session[_K_NONCE] = outcome.session_nonce or nonce
    session[_K_MSG] = outcome.sentence
    session[_K_SENT_AT] = time.time()


def _drop_challenge() -> None:
    for key in (_K_CID, _K_MSG, _K_SENT_AT):
        session.pop(key, None)


def _resend_remaining() -> int:
    total = phone_config.resend_wait_s()
    sent_at = session.get(_K_SENT_AT)
    if not isinstance(sent_at, (int, float)):
        return 0
    return max(0, min(total, math.ceil(total - (time.time() - sent_at))))


def _body(text: str, status: int = 200):
    resp = make_response(text, status)
    resp.headers["Cache-Control"] = "no-store"
    return resp


def _no_store(resp):
    resp.headers["Cache-Control"] = "no-store"
    return resp


@dataclass
class _Result:
    ok: bool
    sentence: str = ""
    phone_page: bool = False  # the challenge is over: show the number page
    drop: bool = False  # forget the session's challenge once the answer is rendered
    row: dict | None = None  # the signed-in user (verify only)


# ---------------------------------------------------------------- operations shared by HTML and JSON


@_padded
def _do_start(phone: str, consent_ok: bool, token: str) -> _Result:
    if _ip_throttled():
        return _Result(False, ps.TRY_AGAIN, phone_page=True)
    nonce = _browser_nonce()
    out, err = _guard(
        lambda: get_service().start(
            phone[:64],
            consent=consent_ok,
            turnstile_token=token[:4096],
            ip=phone_client_ip(request),
            device_hash=_device_hash(),
            session_nonce=nonce,
            purpose="login",
        )
    )
    # No challenge exists after a refused start, so every failure shows the number page again.
    if err:
        return _Result(False, err, phone_page=True)
    if not out.ok or not out.public_id:
        return _Result(False, out.sentence or ps.UNAVAILABLE, phone_page=True)
    _remember_challenge(out, nonce)
    return _Result(True, out.sentence)


@_padded
def _do_resend(token: str) -> _Result:
    cid, nonce = _challenge_ids()
    if not cid:
        return _Result(False, ps.START_AGAIN, phone_page=True, drop=True)
    if _ip_throttled():
        return _Result(False, ps.TRY_AGAIN)
    out, err = _guard(
        lambda: get_service().resend(
            cid, nonce, turnstile_token=token[:4096], ip=phone_client_ip(request), device_hash=_device_hash()
        )
    )
    if err:
        return _Result(False, err)
    if out.ok:
        session[_K_MSG] = out.sentence
        session[_K_SENT_AT] = time.time()
        return _Result(True, out.sentence)
    ended = out.reason == ps.R_NO_CHALLENGE
    return _Result(False, out.sentence, phone_page=ended, drop=ended)


def _do_verify(code: str) -> _Result:
    cid, nonce = _challenge_ids()
    if not cid:
        return _Result(False, ps.START_AGAIN, phone_page=True, drop=True)
    if _ip_throttled():
        return _Result(False, ps.TRY_AGAIN)
    approved, err = _guard(
        lambda: get_service().check(
            cid, nonce, code[:32], device_hash=_device_hash(), ip=phone_client_ip(request)
        )
    )
    if err:
        return _Result(False, err)
    if not approved.approved:
        ended = approved.reason in _CHALLENGE_ENDED
        return _Result(False, approved.sentence, phone_page=ended, drop=ended)
    return _finish_login(approved)


def _refuse(reason: str, sentence: str) -> _Result:
    """No session. The code is spent, so the challenge is over, but the answer reads like a wrong code."""
    _log.warning("event=login_refused reason=%s purpose=login", reason)
    return _Result(False, sentence, drop=True)


def _finish_login(approved: Any) -> _Result:
    from backend.auth.phone.accounts import PhoneAccounts, is_phone_eligible, purge_phone_identity
    from backend.auth.session import finalize_app_session
    from backend.db.users_db import _user_row_is_active, get_user_profile

    if approved.purpose != "login":
        return _refuse("purpose", ps.START_AGAIN)

    def _sign_in():
        out = None
        for attempt in range(_SIGN_IN_TRIES):
            out = get_service().sign_in(approved, PhoneAccounts())
            if out.ok or out.reason != ps.R_RETRY:
                break
            if attempt + 1 < _SIGN_IN_TRIES:
                _sleep(_SIGN_IN_PAUSE_S)
        return out

    out, err = _guard(_sign_in)
    if err:
        return _Result(False, err, drop=True)
    if not out.ok and out.reason == "ineligible":
        # The service refused the stored account before any login stamp or dormant detach (review R8).
        return _refuse("ineligible", ps.CODE_FAILED)
    if not out.ok or not out.user_id:
        return _refuse("retry_exhausted", ps.TRY_AGAIN)

    row, err = _guard(lambda: get_user_profile(int(out.user_id)))
    if err:
        return _Result(False, err, drop=True)
    if row is None:
        # The number still points at an account that no longer exists (the purge after an admin delete
        # failed, or the process died between the two steps). Users.id is never reused, so the dead id
        # cannot belong to anyone else: release the stale identity and make a fresh account, once.
        _log.warning("event=identity_orphan_healed")
        purge_phone_identity(int(out.user_id))
        out, err = _guard(_sign_in)
        if err:
            return _Result(False, err, drop=True)
        if not out.ok or not out.user_id:
            return _refuse("retry_exhausted", ps.TRY_AGAIN)
        row, err = _guard(lambda: get_user_profile(int(out.user_id)))
        if err:
            return _Result(False, err, drop=True)
        if row is None:
            return _refuse("finalize_failed", ps.CODE_FAILED)
    if not _user_row_is_active(row):
        return _refuse("inactive", ps.CODE_FAILED)
    if not is_phone_eligible(row):
        return _refuse("ineligible", ps.CODE_FAILED)

    session.clear()
    if not finalize_app_session(int(row["id"]), auth_method="phone"):
        session.clear()
        return _refuse("finalize_failed", ps.CODE_FAILED)
    return _Result(True, row=row)


def _do_change() -> None:
    cid, nonce = _challenge_ids()
    if cid:
        _guard(lambda: get_service().cancel(cid, nonce))
    _drop_challenge()


# ---------------------------------------------------------------- HTML rendering


def _consent_context() -> dict[str, str]:
    return {
        "consent_lead": consent.CONSENT_LEAD,
        "consent_terms": consent.CONSENT_TERMS,
        "consent_and": consent.CONSENT_AND,
        "consent_privacy": consent.CONSENT_PRIVACY,
        "consent_tail": consent.CONSENT_TAIL,
        "terms_path": consent.TERMS_PATH,
        "privacy_path": consent.PRIVACY_PATH,
    }


def _render_phone_page(error: str | None = None, status: int = 200):
    """login_phone.html. Context: ``error``, ``turnstile_site_key``, ``support_contact``,
    ``staff_login_url`` and the ``consent_*`` pieces of :func:`_consent_context`."""
    allow_turnstile_csp()
    html = render_template(
        "login_phone.html",
        error=error,
        turnstile_site_key=phone_config.turnstile_site_key(),
        support_contact=consent.support_contact(),
        staff_login_url=url_for("staff_login_page"),
        **_consent_context(),
    )
    return _body(html, status)


def _render_code_page(error: str | None = None, status: int = 200):
    """login_phone_code.html. Context: ``error``, ``sent_message``, ``resend_wait_s`` (seconds left
    before "Send again" works), ``turnstile_site_key``, ``support_contact``."""
    allow_turnstile_csp()
    html = render_template(
        "login_phone_code.html",
        error=error,
        sent_message=session.get(_K_MSG) or "",
        resend_wait_s=_resend_remaining(),
        turnstile_site_key=phone_config.turnstile_site_key(),
        support_contact=consent.support_contact(),
    )
    return _body(html, status)


def _failed_page(res: _Result):
    """The page that shows a failed result. Rendered first, then the session is cleaned, so a refused
    sign-in looks exactly like a wrong code (same page, same sentence, same status)."""
    status = _html_status(res.sentence)
    page = _render_phone_page(res.sentence, status) if res.phone_page else _render_code_page(res.sentence, status)
    if res.drop:
        _drop_challenge()
    return page


def render_login_entry():
    """``/login`` while the flag is on: the phone page. A POST (an old form) never reads a password."""
    if request.method == "POST":
        return redirect(url_for("login_page"), 303)
    error = None
    if request.args.get("_error", "") == "session_expired":
        error = SESSION_EXPIRED_MESSAGE
    else:
        error = session.pop("oauth_error", None)
    return _render_phone_page(error)


# ---------------------------------------------------------------- HTML views


def login_phone_page():
    _require_enabled()
    error = SESSION_EXPIRED_MESSAGE if request.args.get("_error", "") == "session_expired" else None
    return _render_phone_page(error)


def login_phone_start():
    _require_enabled()
    form = request.form
    res = _do_start(
        form.get("phone") or "",
        (form.get("consent") or "").strip().lower() in ("1", "on"),
        form.get(TURNSTILE_FIELD) or "",
    )
    if res.ok:
        return _no_store(redirect(url_for("login_phone_code_page"), 303))
    return _failed_page(res)


def login_phone_code_page():
    _require_enabled()
    cid, _nonce = _challenge_ids()
    if not cid:
        return _no_store(redirect(url_for("login_phone_page"), 303))
    return _render_code_page()


def login_phone_code_verify():
    _require_enabled()
    res = _do_verify(request.form.get("code") or "")
    if res.ok:
        from backend.auth.session import post_login_redirect

        return _no_store(post_login_redirect())
    return _failed_page(res)


def login_phone_resend():
    _require_enabled()
    res = _do_resend(request.form.get(TURNSTILE_FIELD) or "")
    if res.ok:
        return _no_store(redirect(url_for("login_phone_code_page"), 303))
    return _failed_page(res)


def login_phone_change():
    _require_enabled()
    _do_change()
    return _no_store(redirect(url_for("login_phone_page"), 303))


# ---------------------------------------------------------------- JSON twins


def _json_body() -> dict | None:
    data = request.get_json(silent=True)
    return data if isinstance(data, dict) else None


def _json_fail(sentence: str, status: int | None = None):
    body = {"ok": False, "error": _error_code(sentence), "message": sentence}
    return _no_store(jsonify(body)), (status or _http_status(sentence))


def _json_bad_request():
    return _no_store(jsonify({"ok": False, "error": "json_object"})), 400


def _text(value: Any) -> str:
    return value if isinstance(value, str) else ""


def _json_result(res: _Result):
    if res.drop:
        _drop_challenge()
    return _json_fail(res.sentence)


def api_auth_phone_start():
    _require_enabled()
    data = _json_body()
    if data is None:
        return _json_bad_request()
    res = _do_start(_text(data.get("phone")), data.get("consent") is True, _text(data.get("turnstile_token")))
    if not res.ok:
        return _json_result(res)
    return _no_store(
        jsonify({"ok": True, "message": res.sentence, "resend_after_s": phone_config.resend_wait_s()})
    )


def api_auth_phone_verify():
    _require_enabled()
    data = _json_body()
    if data is None:
        return _json_bad_request()
    res = _do_verify(_text(data.get("code")))
    if not res.ok:
        return _json_result(res)
    from backend.auth.pages import _auth_user_payload

    return _no_store(jsonify({"ok": True, "user": _auth_user_payload(res.row)}))


def api_auth_phone_resend():
    _require_enabled()
    data = _json_body()
    if data is None:
        return _json_bad_request()
    res = _do_resend(_text(data.get("turnstile_token")))
    if not res.ok:
        return _json_result(res)
    return _no_store(
        jsonify({"ok": True, "message": res.sentence, "resend_after_s": phone_config.resend_wait_s()})
    )


def _no_server_timing() -> None:
    """Keep ``Server-Timing`` off the phone answers. Its ``db_connect`` count differs between a number
    that has an account (no Lookup, no lookup-cache rows) and one that has not, which would tell a
    visitor which numbers are registered (DESIGN.md section 14)."""
    if (request.endpoint or "") in _NO_TIMING_ENDPOINTS:
        request.environ[NO_SERVER_TIMING_KEY] = True


def register(app) -> None:
    """Attach the routes with bare endpoint names (the CSRF lists in ``backend/web/security.py`` match them)."""
    app.before_request(_no_server_timing)
    add = app.add_url_rule
    add("/login/phone", endpoint="login_phone_page", view_func=login_phone_page, methods=["GET"])
    add("/login/phone", endpoint="login_phone_start", view_func=login_phone_start, methods=["POST"])
    add("/login/phone/code", endpoint="login_phone_code_page", view_func=login_phone_code_page, methods=["GET"])
    add("/login/phone/code", endpoint="login_phone_code_verify", view_func=login_phone_code_verify, methods=["POST"])
    add("/login/phone/resend", endpoint="login_phone_resend", view_func=login_phone_resend, methods=["POST"])
    add("/login/phone/change", endpoint="login_phone_change", view_func=login_phone_change, methods=["POST"])
    add("/api/auth/phone/start", endpoint="api_auth_phone_start", view_func=api_auth_phone_start, methods=["POST"])
    add("/api/auth/phone/verify", endpoint="api_auth_phone_verify", view_func=api_auth_phone_verify, methods=["POST"])
    add("/api/auth/phone/resend", endpoint="api_auth_phone_resend", view_func=api_auth_phone_resend, methods=["POST"])
