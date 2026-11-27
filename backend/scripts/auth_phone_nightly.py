"""Yesterday's phone-code totals, retention purge, and an exit code for alerts (card S16,
docs/auth_sms/DESIGN.md sections 16 and 17).

    python -m backend.scripts.auth_phone_nightly [--dry-run] [--no-purge]

Prints yesterday's totals (the day in ``chat.reset_tz``, the same day the send caps use): texts
sent, approvals (challenges approved, the ``check_ok`` count), their ratio, the cost estimate from
the app's own send rows, and refusals by reason. Then it deletes rows past retention
(``backend/auth/phone/retention.py``) and reports ``reserving`` identities older than an hour.
``--dry-run`` counts what the purge would delete and deletes nothing.

    0  under every alert level
    2  an alert level: sends reached the alert threshold (``SMS_ALERT_SEND_THRESHOLD``, unset
       means half the send cap), or approvals were under 50 percent of 20 or more texts
    3  a cap was hit: a ``blocked_cap`` refusal, the send cap reached, or the spend cap reached
    1  the job could not run (no database address, or the database unreachable), or the purge
       failed on a day with no alert

On 2 and 3 it also emails ``OWNER_ALERT_EMAIL`` when that is set (one email per run). Lines carry
counts and dollars only: no number, code, hash or database address.

Railway cron (owner step, card S18): ``python -m backend.scripts.auth_phone_nightly``.
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Callable
from zoneinfo import ZoneInfo

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_ALERT = 2
EXIT_CAP = 3

LOW_APPROVAL_MIN_SENDS = 20
LOW_APPROVAL_PCT = 50


def _out(line: str) -> None:
    print(f"auth_phone_nightly: {line}", flush=True)


def _dollars(cents: Decimal | int) -> str:
    return f"${Decimal(cents) / 100:,.2f}"


@dataclass(frozen=True)
class Levels:
    send_cap: int
    spend_cap_cents: int
    alert_threshold: int


def verdict(totals, levels: Levels) -> tuple[int, str]:
    """The exit code and the one-line reason, from yesterday's totals. Pure."""
    if (totals.outcomes.get("blocked_cap", 0) > 0
            or (levels.send_cap and totals.sends >= levels.send_cap)
            or (levels.spend_cap_cents and totals.cost_cents >= levels.spend_cap_cents)):
        return EXIT_CAP, "cap_hit"
    reasons = []
    if levels.alert_threshold and totals.sends >= levels.alert_threshold:
        reasons.append("cap_50pct")
    pct = totals.approval_pct
    if totals.sends >= LOW_APPROVAL_MIN_SENDS and pct is not None and pct < LOW_APPROVAL_PCT:
        reasons.append("low_approvals")
    if reasons:
        return EXIT_ALERT, ", ".join(reasons)
    return EXIT_OK, "under every alert level"


def _levels() -> tuple[Levels, str]:
    from backend.auth.phone import config as phone_config
    from backend.auth.phone import limits as lim

    limits = lim.load_limits()
    threshold = phone_config._int("SMS_ALERT_SEND_THRESHOLD", limits.daily_send_cap // 2, minimum=0)
    return Levels(limits.daily_send_cap, limits.daily_spend_cap_cents, threshold), limits.reset_tz


def _send_email(subject: str, text: str) -> None:
    from backend.utils.mfa_delivery import send_transactional_email

    to = (os.environ.get("OWNER_ALERT_EMAIL") or "").strip()
    send_transactional_email(to_email=to, subject=subject, text=text, log_surface="auth_phone_nightly")


def _have_dsn() -> bool:
    from backend.db.connect import inventory_dsn

    return bool(inventory_dsn(require=False))


def run(*, now: datetime | None = None, purge: bool = True, dry_run: bool = False,
        send_email: Callable[[str, str], None] | None = None) -> int:
    from backend.auth.phone import limits as lim
    from backend.auth.phone import retention

    if not _have_dsn():
        _out("no database address set (INVENTORY_DATABASE_URL or DATABASE_URL); nothing done")
        return EXIT_ERROR
    now = now or datetime.now(timezone.utc)
    levels, tz = _levels()
    today = lim.day_start(now, tz)
    yesterday = lim.day_start(today - timedelta(hours=1), tz)
    label = yesterday.astimezone(ZoneInfo(tz)).date().isoformat()
    try:
        t = retention.day_totals(since=yesterday, until=today)
        stale = retention.stale_reserving(now=now)
    except Exception as exc:
        _out(f"could not read the totals ({type(exc).__name__}); nothing deleted")
        return EXIT_ERROR

    pct = t.approval_pct
    lines = [
        f"day {label} ({tz})",
        f"sends {t.sends:,} of {levels.send_cap:,} (alert at {levels.alert_threshold:,})",
        f"approvals {t.approvals:,} ({'no texts' if pct is None else f'{pct}% of texts'})",
        f"cost estimate {_dollars(t.cost_cents)} of {_dollars(levels.spend_cap_cents)}",
        "blocked " + (", ".join(f"{k} {v:,}" for k, v in t.blocked.items()) if t.blocked else "none"),
        f"reserving identities older than one hour {stale:,}",
    ]
    code, why = verdict(t, levels)
    lines.append(f"result: {why}")
    for line in lines:
        _out(line)

    if purge or dry_run:
        try:
            counts = retention.purge(now=now, dry_run=dry_run)
            verb = "would delete" if dry_run else "deleted"
            _out(f"{verb} " + ", ".join(f"{k} {v:,}" for k, v in counts.items()))
        except Exception as exc:
            _out(f"could not delete old rows ({type(exc).__name__})")
            if code == EXIT_OK:
                code = EXIT_ERROR

    if code in (EXIT_ALERT, EXIT_CAP) and (os.environ.get("OWNER_ALERT_EMAIL") or "").strip():
        sender = send_email or _send_email
        try:
            sender(f"Phone codes {label}: {why}", "\n".join(lines) + "\n")
            _out("alert email sent")
        except Exception as exc:  # the exit code still carries the alert
            _out(f"alert email failed ({type(exc).__name__})")
    return code


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    parser.add_argument("--dry-run", action="store_true", help="count what the purge would delete; delete nothing")
    parser.add_argument("--no-purge", action="store_true", help="report only; no purge and no count")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        from dotenv import load_dotenv

        load_dotenv()
    except ImportError:
        pass
    return run(purge=not args.no_purge, dry_run=args.dry_run and not args.no_purge)


if __name__ == "__main__":
    sys.exit(main())
