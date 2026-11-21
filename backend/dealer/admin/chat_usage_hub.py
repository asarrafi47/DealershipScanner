"""Site-admin chat usage page at ``/admin/chat-usage`` (card C5, ``docs/auth_sms/CHAT_LIMITS.md`` section 8).

Shows today's chat use against the budget, the last 14 days, the 20 busiest accounts today (id and
username only) and the refusals by kind, then the chat settings with where each value comes from and a
form to change them, plus the two switches an admin reaches for in a hurry: pause the chat, pause the
texts. Plain headings and tables; no phone number, no message text and no secret appears here.

Every change is validated before any is written, refused with a plain sentence when it is bad, written
with the admin's id (``app_settings.updated_by``) and logged once with the old and the new value.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from flask import flash, redirect, render_template, request, session, url_for

from backend import settings_store
from backend.dealer.admin.routes import _session_profile, store_admin_bp
from backend.utils.csrf import validate_csrf_form
from backend.utils.roles import is_admin_role

_audit = logging.getLogger("chat_usage_admin")
_log = logging.getLogger(__name__)

# Per-subject limits: 0 means "no limit", so the form wants the "unlimited" tick to set it.
PER_SUBJECT_KEYS = (
    "chat.user_per_hour",
    "chat.user_per_day",
    "chat.listing_per_day",
    "chat.phone_per_day",
    "chat.device_per_day",
    "chat.ip_per_day",
    "chat.ip_net_per_day",
)
# Money and token values: the form also refuses anything above the env-only hard ceiling.
SWITCHES = {
    "pause_chat": ("chat.enabled", False, "Chat is paused."),
    "resume_chat": ("chat.enabled", True, "Chat is back on."),
    "pause_texts": ("auth_phone.sends_paused", True, "Text messages are paused."),
    "resume_texts": ("auth_phone.sends_paused", False, "Text messages are back on."),
}
DENIAL_LABELS = (
    ("user", "Account"),
    ("phone", "Phone"),
    ("device", "Device"),
    ("ip", "IP address"),
    ("listing", "Listing"),
    ("global", "Site budget"),
    ("global_share", "New-account or open share of the budget"),
)
SOURCE_LABELS = {
    "row": "settings row",
    "env": "environment",
    "default": "default",
    "ceiling": "hard ceiling (the value is above the environment's maximum)",
}
UNAVAILABLE = "Usage figures are not available right now."
_FIELD = "s__"
_UNLIMITED = "unlimited__"


def _require_site_admin():
    p = _session_profile()
    if not p or not is_admin_role(p.get("role")):
        return None
    return p


# ---------------------------------------------------------------------------- settings table


def editable_keys() -> list[str]:
    """The settings this page edits: every ``chat.*`` setting."""
    return [k for k in settings_store.SPECS if k.startswith("chat.")]


def _text(spec: settings_store.Spec, value: Any) -> str:
    if spec.kind == "pcts":
        return ", ".join(str(v) for v in value)
    if spec.kind == "bool":
        return "yes" if value else "no"
    return str(value)


def settings_rows() -> list[dict[str, Any]]:
    """One row per editable setting: current value, where it comes from, and its limits."""
    current = settings_store.all_settings()
    rows = []
    for key in editable_keys():
        spec = settings_store.SPECS[key]
        cur = current[key]
        rows.append({
            "key": key,
            "text": _text(spec, cur["value"]),
            "source": SOURCE_LABELS.get(cur["source"], cur["source"]),
            "env": spec.env,
            "kind": spec.kind,
            "ceiling": settings_store.hard_ceiling(key),
            "per_subject": key in PER_SUBJECT_KEYS,
            "field": _FIELD + key,
            "unlimited_field": _UNLIMITED + key,
            "unlimited_now": key in PER_SUBJECT_KEYS and cur["value"] == 0,
        })
    return rows


def _canonical(spec: settings_store.Spec, value: Any) -> str:
    """The text stored in ``app_settings`` for a parsed value (``settings_store.parse`` reads it back)."""
    if spec.kind == "bool":
        return "true" if value else "false"
    if spec.kind == "pcts":
        return ",".join(str(v) for v in value)
    return str(value)


def _kind_help(spec: settings_store.Spec) -> str:
    if spec.kind == "bool":
        return "yes or no"
    if spec.kind == "tz":
        return "a time zone name such as America/New_York"
    if spec.kind == "pcts":
        return "percentages from 1 to 99 separated by commas, such as 50, 80"
    return f"a whole number from {spec.min_value} to {spec.max_value}"


def plan_edits(form: Any) -> tuple[list[tuple[str, str, str]], list[str]]:
    """Check the submitted form. Returns ``(changes, errors)``; ``changes`` are ``(key, old, new)`` texts.

    Nothing is written here. A single error means no change at all is applied.
    """
    changes: list[tuple[str, str, str]] = []
    errors: list[str] = []
    current = settings_store.all_settings()
    for key in editable_keys():
        spec = settings_store.SPECS[key]
        raw = form.get(_FIELD + key)
        unlimited = bool(form.get(_UNLIMITED + key)) and key in PER_SUBJECT_KEYS
        if raw is None and not unlimited:
            continue
        raw = (raw or "").strip()
        if unlimited:
            raw = "0"
        old = _text(spec, current[key]["value"])
        if raw == old and not unlimited:
            continue
        if raw == "" and not unlimited:
            continue
        value = settings_store.parse(spec, raw)
        if value is None:
            errors.append(f"{key}: that is not a valid value. It needs {_kind_help(spec)}.")
            continue
        if key in PER_SUBJECT_KEYS and value == 0 and not unlimited:
            errors.append(f'{key}: 0 means no limit. Tick "unlimited" to set it.')
            continue
        cap = settings_store.hard_ceiling(key)
        if cap is not None and isinstance(value, int) and value > cap:
            errors.append(f"{key}: that is above {cap}, the most the server's environment allows.")
            continue
        new = _canonical(spec, value)
        if new == _canonical(spec, current[key]["value"]):
            continue
        changes.append((key, _canonical(spec, current[key]["value"]), new))
    return changes, errors


def apply_changes(changes: list[tuple[str, str, str]], admin_id: int) -> None:
    """Write each change with the admin's id and log it once. Raises if the database refuses."""
    from backend.db.repositories import chat_usage_repo as repo

    for key, old, new in changes:
        repo.set_app_setting(key, new, updated_by=admin_id)
        _audit.info("event=setting_changed admin=%s key=%s old=%s new=%s", admin_id, key, old, new)
    settings_store.reset_cache()


# ---------------------------------------------------------------------------- usage figures


def _cents(micro: int) -> float:
    return micro / 10_000.0


def _pct(used: float, budget: float) -> int:
    return int(round(100.0 * used / budget)) if budget > 0 else 0


def build_snapshot(now: datetime | None = None) -> dict[str, Any]:
    """Today so far, the last 14 days, the 20 busiest accounts and the refusals. Never raises."""
    try:
        return _snapshot(now or datetime.now(timezone.utc))
    except Exception as exc:  # the table or the database may be unavailable: say so, do not 500
        _log.warning("chat usage page: figures unavailable (%s)", type(exc).__name__)
        return {"error": UNAVAILABLE}


def _snapshot(now: datetime) -> dict[str, Any]:
    from backend.db.repositories import chat_usage_repo as repo

    tz = settings_store.get_str("chat.reset_tz")
    start, nxt = repo.day_start(now, tz), repo.next_day(now, tz)
    today = repo.day_summary(day_start_utc=start, next_day_utc=nxt)
    budget_cents = settings_store.get_int("chat.budget_usd_per_day_cents")
    budget_tokens = settings_store.get_int("chat.budget_tokens_per_day")
    used_tokens = today["tokens_in"] + today["tokens_out"]
    dollars_pct = _pct(_cents(today["cost_micro"]), budget_cents)
    tokens_pct = _pct(used_tokens, budget_tokens)
    if not settings_store.get_bool("chat.enabled"):
        status = "paused by an admin"
    elif budget_cents == 0 or budget_tokens == 0 or max(dollars_pct, tokens_pct) >= 100:
        status = "stopped by the budget until the next reset"
    else:
        status = "on"

    days = []
    day_end = start
    for _ in range(14):
        day_begin = repo.day_start(day_end - timedelta(seconds=1), tz)
        s = repo.day_summary(day_start_utc=day_begin, next_day_utc=day_end)
        days.append({
            "day": day_begin.astimezone(repo._zone(tz)).strftime("%a %Y-%m-%d"),
            "messages": s["messages"], "tokens": s["tokens_in"] + s["tokens_out"],
            "dollars": s["cost_micro"] / 1_000_000.0, "refusals": sum(s["denied"].values()),
        })
        day_end = day_begin

    top = []
    for row in repo.read_day_totals(now=now, tz_name=tz, kind="user", limit=20):
        username = ""
        try:
            from backend.db.users_db import get_user_profile

            username = str((get_user_profile(int(row["subject_key"])) or {}).get("username") or "")
        except (TypeError, ValueError):
            continue
        top.append({"id": row["subject_key"], "username": username, "messages": row["messages"]})

    denied = [{"label": label, "count": today["denied"].get(kind, 0)} for kind, label in DENIAL_LABELS]
    return {
        "error": None,
        "status": status,
        "messages": today["messages"],
        "tokens_in": today["tokens_in"], "tokens_out": today["tokens_out"],
        "dollars": today["cost_micro"] / 1_000_000.0,
        "dollars_pct": dollars_pct, "tokens_pct": tokens_pct,
        "budget_dollars": budget_cents / 100.0, "budget_tokens": budget_tokens,
        "next_reset": nxt.astimezone(repo._zone(tz)).strftime("%Y-%m-%d %H:%M %Z"),
        "days": days, "top": top, "denied": denied,
    }


# ---------------------------------------------------------------------------- the page


@store_admin_bp.route("/chat-usage", methods=["GET", "POST"])
def admin_chat_usage_hub():
    admin = _require_site_admin()
    if not admin:
        flash("Chat usage requires a site admin account.", "error")
        return redirect(url_for("store_admin.admin_site_hub"))
    admin_id = int(session.get("user_id") or 0)

    if request.method == "POST":
        validate_csrf_form()
        action = (request.form.get("action") or "").strip().lower()
        try:
            if action in SWITCHES:
                key, on, done = SWITCHES[action]
                spec = settings_store.SPECS[key]
                old = _canonical(spec, settings_store.get(key))
                apply_changes([(key, old, _canonical(spec, on))], admin_id)
                flash(done, "success")
            elif action == "save":
                changes, errors = plan_edits(request.form)
                if errors:
                    for e in errors:
                        flash(e, "error")
                    flash("Nothing was changed.", "error")
                elif not changes:
                    flash("No change to save.", "info")
                else:
                    apply_changes(changes, admin_id)
                    flash("Saved " + ", ".join(k for k, _, _ in changes) + ".", "success")
            else:
                flash("Unknown action.", "error")
        except Exception:
            _log.exception("chat usage page: could not save")
            flash("The change could not be saved. Nothing was changed.", "error")
        return redirect(url_for("store_admin.admin_chat_usage_hub"))

    return render_template(
        "admin/chat_usage.html",
        usage=build_snapshot(),
        rows=settings_rows(),
        chat_on=settings_store.get_bool("chat.enabled"),
        texts_paused=settings_store.get_bool("auth_phone.sends_paused"),
    )
