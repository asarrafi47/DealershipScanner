from __future__ import annotations

import logging
import os
import smtplib
from email.message import EmailMessage

from backend.utils.mfa_action_log import log_mfa_action
from backend.utils.runtime_env import is_production_env

_log = logging.getLogger(__name__)


def _send_mode() -> str:
    # "smtp" (default) | "log" (dev fallback) | "test" (returns code to caller)
    return (os.environ.get("MFA_DELIVERY_MODE") or "smtp").strip().lower()


def _email_plain_body(code: str) -> str:
    return f"Your verification code is: {code}\n\nIt expires in 10 minutes.\n"


def _send_resend(
    *, to_email: str, code: str, mfa_log_surface: str, subject: str | None = None
) -> None:
    import resend  # type: ignore[import-untyped]

    api_key = (os.environ.get("RESEND_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("RESEND_API_KEY is not set.")
    resend.api_key = api_key
    from_addr = (
        (os.environ.get("RESEND_FROM") or os.environ.get("SMTP_FROM") or "").strip()
    )
    if not from_addr:
        raise RuntimeError("Set RESEND_FROM (or SMTP_FROM) to a Resend-verified from address.")
    resend.Emails.send(
        {
            "from": from_addr,
            "to": [to_email],
            "subject": subject or "Your DealershipScanner verification code",
            "text": _email_plain_body(code),
        }
    )
    log_mfa_action(
        event="delivery.email",
        surface=mfa_log_surface,
        fields={
            "delivery_mode": "resend",
            "result": "resend_sent",
            "to_email": to_email,
        },
    )


def send_email_code(
    *, to_email: str, code: str, mfa_log_surface: str = "app"
) -> None:
    mode = _send_mode()
    if mode in ("log", "test"):
        _log.info("[mfa] email code to=%s code=%s", to_email, code)
        log_mfa_action(
            event="delivery.email",
            surface=mfa_log_surface,
            fields={
                "delivery_mode": mode,
                "result": "log_or_test",
                "to_email": to_email,
            },
        )
        return

    ep = (os.environ.get("MFA_EMAIL_PROVIDER") or "auto").strip().lower()
    rkey = (os.environ.get("RESEND_API_KEY") or "").strip()

    if ep in ("auto", "resend"):
        if rkey:
            try:
                _send_resend(to_email=to_email, code=code, mfa_log_surface=mfa_log_surface)
            except Exception as e:  # noqa: BLE001
                log_mfa_action(
                    event="delivery.email",
                    surface=mfa_log_surface,
                    fields={
                        "delivery_mode": "resend",
                        "result": "error",
                        "detail": type(e).__name__,
                        "error": (str(e) or "")[:500],
                        "to_email": to_email,
                    },
                )
                if isinstance(e, (RuntimeError, OSError)):
                    raise
                raise RuntimeError("Resend could not send the message.") from e
            return
        if ep == "resend" and is_production_env():
            log_mfa_action(
                event="delivery.email",
                surface=mfa_log_surface,
                fields={"delivery_mode": "resend", "result": "error", "detail": "resend_key_missing"},
            )
            raise RuntimeError("MFA_EMAIL_PROVIDER=resend but RESEND_API_KEY is not set.")
        if ep == "resend" and not is_production_env():
            _log.info("[mfa] RESEND_API_KEY missing; dev fallback: log to console")
            _log.info("[mfa] email code to=%s code=%s", to_email, code)
            log_mfa_action(
                event="delivery.email",
                surface=mfa_log_surface,
                fields={
                    "delivery_mode": "resend",
                    "result": "resend_key_missing_log_fallback",
                    "to_email": to_email,
                },
            )
            return

    # SMTP (legacy) — also used when MFA_EMAIL_PROVIDER=auto and Resend is not configured
    host = (os.environ.get("SMTP_HOST") or "").strip()
    port = int(os.environ.get("SMTP_PORT") or 587)
    user = (os.environ.get("SMTP_USERNAME") or "").strip()
    pw = (os.environ.get("SMTP_PASSWORD") or "").strip()
    from_addr = (os.environ.get("SMTP_FROM") or user or "").strip()
    if not host or not from_addr:
        if is_production_env():
            log_mfa_action(
                event="delivery.email",
                surface=mfa_log_surface,
                fields={
                    "delivery_mode": "smtp",
                    "result": "error",
                    "detail": "smtp_not_configured",
                    "to_email": to_email,
                },
            )
            raise RuntimeError("Email is not configured. Set RESEND_API_KEY+RESEND_FROM, or SMTP_HOST+SMTP_FROM.")
        _log.info("[mfa] email not configured; falling back to log mode")
        _log.info("[mfa] email code to=%s code=%s", to_email, code)
        log_mfa_action(
            event="delivery.email",
            surface=mfa_log_surface,
            fields={
                "delivery_mode": "smtp",
                "result": "smtp_fallback_to_log",
                "detail": "missing_smtp_host_or_from",
                "to_email": to_email,
            },
        )
        return

    msg = EmailMessage()
    msg["Subject"] = "Your DealershipScanner verification code"
    msg["From"] = from_addr
    msg["To"] = to_email
    msg.set_content(_email_plain_body(code))

    try:
        with smtplib.SMTP(host, port, timeout=15) as s:
            s.starttls()
            if user and pw:
                s.login(user, pw)
            s.send_message(msg)
    except (OSError, smtplib.SMTPException) as e:  # noqa: BLE001
        log_mfa_action(
            event="delivery.email",
            surface=mfa_log_surface,
            fields={
                "delivery_mode": "smtp",
                "result": "error",
                "detail": f"smtp_{type(e).__name__}",
                "error": str(e)[:500],
                "to_email": to_email,
            },
        )
        raise
    log_mfa_action(
        event="delivery.email",
        surface=mfa_log_surface,
        fields={
            "delivery_mode": "smtp",
            "result": "smtp_sent",
            "smtp_port": port,
            "to_email": to_email,
        },
    )
