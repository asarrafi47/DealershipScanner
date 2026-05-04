"""QR-code second factor: scan opens approval URL; Redis-backing + SocketIO realtime + Segno for PNG."""

from __future__ import annotations

import os

from typing import Any

from flask import (
    Blueprint,
    Response,
    abort,
    current_app,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from flask_socketio import join_room

from backend.utils.client_ip import client_ip
from backend.utils.csrf import validate_csrf_form
from backend.utils.ip_rate_limit import allow_request
from backend.utils.mfa_action_log import log_mfa_action
from backend.utils.mfa_qr_store import (
    mfa_qr_approve,
    mfa_qr_channel_available,
    mfa_qr_consume_approved,
    mfa_qr_get,
    mfa_qr_set_form_nonce,
)
from backend.utils.qr_segno import png_bytes

bp = Blueprint("mfa_qr", __name__)

_MFA_RPM = int((os.environ.get("RATE_LIMIT_MFA_QR_PER_MIN") or "30") or 30)


def mfa_qr_base_url() -> str:
    b = (os.environ.get("PUBLIC_BASE_URL") or os.environ.get("MFA_QR_BASE_URL") or "").strip()
    if b:
        return b.rstrip("/")
    return (request.url_root or "").rstrip("/")


@bp.get("/mfa/qr-wait")
def mfa_qr_wait():
    from backend.main import _mfa_landing_url, _require_mfa_pending_user  # late import

    if not mfa_qr_channel_available():
        return render_template("mfa_qr_error.html", error="Phone QR sign-in is not configured."), 503
    uid = _require_mfa_pending_user()
    if not uid:
        return redirect(_mfa_landing_url())
    m = (session.get("mfa_pending_method") or "").strip().lower()
    if m != "qr":
        return redirect(_mfa_landing_url())
    aid = (session.get("mfa_qr_attempt_id") or "").strip()
    if not aid or not mfa_qr_get(aid):
        return render_template("mfa_qr_error.html", error="This sign-in request expired. Start over."), 410
    urec = mfa_qr_get(aid) or {}
    if int(urec.get("user_id") or 0) != int(uid) or (urec.get("stream") or "app") != "app":
        return render_template("mfa_qr_error.html", error="Invalid sign-in state."), 400
    return render_template("mfa_qr_wait.html", attempt_id=aid, img_url=url_for("mfa_qr.mfa_qr_approve_png", a=aid))


@bp.get("/mfa/qr-approve-png")
def mfa_qr_approve_png():
    """PNG of URL to /mfa/qr-confirm/<token>; session must own the attempt (desktop only)."""
    from backend.main import _require_mfa_pending_user  # late import

    if not mfa_qr_channel_available():
        return Response(status=404)
    uid = _require_mfa_pending_user()
    m = (session.get("mfa_pending_method") or "").strip().lower()
    if not uid or m != "qr":
        return Response(status=404)
    aid = (request.args.get("a") or "").strip() or (request.args.get("attempt_id") or "").strip()
    if (session.get("mfa_qr_attempt_id") or "").strip() != aid or not aid:
        return Response(status=404)
    d = mfa_qr_get(aid) or None
    if not d or int(d.get("user_id") or 0) != int(uid) or (d.get("state") or "") not in (
        "pending",
        "approved",
    ):
        return Response(status=404)
    url = f"{mfa_qr_base_url()}{url_for('mfa_qr.mfa_qr_confirm_get', attempt_id=aid)}"
    raw = png_bytes(data=url, box_size=4)
    if not raw:
        return Response(status=500)
    return Response(raw, mimetype="image/png", headers={"Cache-Control": "no-store, private"})


@bp.get("/mfa/qr-confirm/<path:attempt_id>")
def mfa_qr_confirm_get(attempt_id: str):
    """Open on the phone: approve sign-in. No app session required — token in URL is the secret."""
    aid = (attempt_id or "").strip()
    if not aid or len(aid) > 300:
        abort(404)
    st = mfa_qr_get(aid) or None
    if not st or (st.get("state") or "") != "pending" or (st.get("stream") or "app") != "app":
        return render_template("mfa_qr_error.html", error="This link expired or is invalid.", title="Link invalid"), 404
    nonce = mfa_qr_set_form_nonce(aid)
    if not nonce:
        return render_template("mfa_qr_error.html", error="This link expired or is invalid.", title="Link invalid"), 404
    return render_template("mfa_qr_confirm.html", attempt_id=aid, ap_nonce=nonce, site_name="DealershipScanner")


@bp.post("/mfa/qr-confirm/<path:attempt_id>")
def mfa_qr_confirm_post(attempt_id: str):
    from backend.main import _mfa_landing_url  # late import for confirm_ok template (anonymous session → login)

    aid = (attempt_id or "").strip()
    ip = client_ip(request)
    if not allow_request(
        f"mfa_qr_confirm_post:{ip}", max_events=_MFA_RPM, window_seconds=60.0
    ):
        return render_template("mfa_qr_error.html", error="Too many attempts. Try again shortly."), 429
    st = mfa_qr_get(aid) or None
    if not st or (st.get("state") or "") != "pending" or (st.get("stream") or "app") != "app":
        return render_template("mfa_qr_error.html", error="This request expired or was already used."), 410
    if not mfa_qr_approve(aid, (request.form.get("ap_nonce") or "").strip()):
        log_mfa_action(
            event="mfa_qr.confirm_bad_nonce",
            surface="app",
            fields={"attempt_id_prefix": (aid[:8] if len(aid) >= 8 else aid) + "...", "client_ip": ip},
        )
        return render_template("mfa_qr_error.html", error="Request invalid. Open the page again and tap Approve."), 400
    sio = current_app.extensions.get("socketio")
    if sio:
        try:
            sio.emit("mfa_qr_approved", {"ok": True}, to=_room(aid), namespace="/")
        except Exception as e:  # noqa: BLE001
            log_mfa_action(
                event="mfa_qr.socket_emit_failed",
                surface="app",
                fields={"error": (str(e) or "")[:200]},
            )
    u = mfa_qr_get(aid) or {}
    log_mfa_action(
        event="mfa_qr.approved",
        surface="app",
        fields={"user_id": int(u.get("user_id") or 0), "client_ip": ip},
    )
    return render_template("mfa_qr_confirm_ok.html", title="Signed in on other device", next_login_url=_mfa_landing_url())


@bp.post("/mfa/qr/complete")
def mfa_qr_complete():
    from backend.main import (  # late import
        _finalize_app_session,
        _mfa_landing_url,
        _post_mfa_success_redirect,
        _require_mfa_pending_user,
    )

    wants_json = (request.headers.get("X-MFA-QR-JSON") or "").strip().lower() in ("1", "true", "yes", "on")
    validate_csrf_form()
    uid = _require_mfa_pending_user()
    m = (session.get("mfa_pending_method") or "").strip().lower()
    if not uid or m != "qr":
        if not wants_json:
            return render_template("mfa_qr_error.html", error="Session expired. Start sign-in again.", title="Sign-in"), 400
        return jsonify({"ok": False, "error": "no_pending"}), 400
    aid = (request.form.get("attempt_id") or (session.get("mfa_qr_attempt_id") or "")).strip()
    if (session.get("mfa_qr_attempt_id") or "").strip() != aid or not aid:
        if not wants_json:
            return render_template("mfa_qr_error.html", error="Request invalid. Try again from the 2FA screen.", title="Sign-in"), 400
        return jsonify({"ok": False, "error": "mismatch"}), 400
    ip = client_ip(request)
    if not allow_request(
        f"mfa_qr_complete:{ip}", max_events=_MFA_RPM, window_seconds=60.0
    ):
        if not wants_json:
            return render_template("mfa_qr_error.html", error="Too many attempts. Try again in a minute.", title="Sign-in"), 429
        return jsonify({"ok": False, "error": "rate_limited"}), 429
    if not mfa_qr_consume_approved(aid, int(uid)):
        if not wants_json:
            return (
                render_template("mfa_qr_error.html", error="Not approved on the phone yet, or this code expired. Try again."),
                400,
            )
        return jsonify({"ok": False, "error": "not_ready_or_expired"}), 400
    session.pop("mfa_qr_attempt_id", None)
    if not _finalize_app_session(int(uid)):
        nxt = _mfa_landing_url()
    else:
        r = _post_mfa_success_redirect()
        nxt = (r.headers or {}).get("Location") or getattr(r, "location", None) or "/"
    log_mfa_action(
        event="mfa_qr.complete",
        surface="app",
        fields={"user_id": int(uid), "client_ip": ip},
    )
    if not wants_json:
        return redirect(nxt, code=302)
    return jsonify({"ok": True, "next": nxt})


def _room(aid: str) -> str:
    return f"mqra_{aid}"


def register_mfa_qr_socketio(socketio) -> None:
    @socketio.on("mfa_qr_subscribe", namespace="/")
    def on_mfa_qr_sub(data: Any) -> None:  # noqa: ANN401
        from backend.main import _require_mfa_pending_user  # late import

        aid = str((data or {}).get("attempt_id") or "")
        if not aid or (session.get("mfa_qr_attempt_id") or "") != aid:
            return
        m = (session.get("mfa_pending_method") or "").strip().lower()
        if m != "qr":
            return
        u = mfa_qr_get(aid) or None
        puid = _require_mfa_pending_user()
        if not u or not puid or int(u.get("user_id") or 0) != int(puid) or (u.get("stream") or "app") != "app":
            return
        st = (u.get("state") or "") in ("pending", "approved")
        if not st:
            return
        try:
            join_room(_room(aid), namespace="/")
        except Exception:  # noqa: BLE001
            pass
        log_mfa_action(
            event="mfa_qr.socket_subscribed",
            surface="app",
            fields={"pending_user_id": puid, "client_ip": client_ip(request)},
        )
