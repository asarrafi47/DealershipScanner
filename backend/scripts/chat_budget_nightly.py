"""Yesterday's AI chat totals, retention, and an exit code for alerts (card C6, CHAT_LIMITS.md section 9).

    python -m backend.scripts.chat_budget_nightly

Prints yesterday's totals (in the reset time zone ``chat.reset_tz``): messages, tokens, dollars
and denials by kind, then deletes counter rows older than 35 days. The exit code tells a
scheduled job whether the owner should hear about it:

    0  under every alert level
    2  an alert level (``chat.alert_pcts``, default 50 and 80) was passed
    3  the day's budget was reached (dollars or tokens, or the site budget refused a message)
    1  the job could not run (database unreachable); nothing was deleted

On 2 and 3 it also emails the address in ``OWNER_ALERT_EMAIL`` when that is set (one email per
run, so one per day). Lines carry counts, percentages and dollars only: no message text, name,
phone number, hash or address, and numbers use thousands separators (no digit run of 7 or more).
The percentages use today's settings, which is what the owner sees on the admin page.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Callable

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_ALERT = 2
EXIT_BUDGET = 3

# Subject kinds as the owner reads them (the global rows are the site budget).
_KIND_NAMES = {"global": "budget", "user": "account", "listing": "car", "phone": "phone", "device": "device", "ip": "address"}


def _out(line: str) -> None:
    print(f"chat_budget_nightly: {line}", flush=True)


def _send_email(subject: str, text: str) -> None:
    from backend.utils.mfa_delivery import send_transactional_email

    to = (os.environ.get("OWNER_ALERT_EMAIL") or "").strip()
    send_transactional_email(to_email=to, subject=subject, text=text, log_surface="chat_budget_nightly")


def run(*, now: datetime | None = None, purge: bool = True,
        send_email: Callable[[str, str], None] | None = None) -> int:
    from backend import settings_store as ss
    from backend.chat_budget.alerts import budget_pct, dollars
    from backend.db.repositories import chat_usage_repo as repo

    now = now or datetime.now(timezone.utc)
    tz = ss.get_str("chat.reset_tz")
    today = repo.day_start(now, tz)
    yesterday = repo.day_start(today - timedelta(hours=1), tz)
    label = yesterday.astimezone(repo._zone(tz)).date().isoformat()
    try:
        s = repo.day_summary(day_start_utc=yesterday, next_day_utc=today)
    except Exception as exc:
        _out(f"could not read the totals ({type(exc).__name__}); nothing deleted")
        return EXIT_ERROR

    tokens = s["tokens_in"] + s["tokens_out"]
    cents_limit = ss.get_int("chat.budget_usd_per_day_cents")
    tokens_limit = ss.get_int("chat.budget_tokens_per_day")
    pct = budget_pct(tokens, s["cost_micro"], tokens_limit=tokens_limit, cents_limit=cents_limit)
    denied: dict[str, int] = {}
    for kind, n in s["denied"].items():
        name = _KIND_NAMES.get(kind, "other")
        denied[name] = denied.get(name, 0) + n
    budget_denials = denied.get("budget", 0)
    levels = sorted(int(x) for x in (ss.get("chat.alert_pcts") or ()))
    passed = [lv for lv in levels if pct >= lv]

    lines = [
        f"day {label} ({tz})",
        f"messages {s['messages']:,}",
        f"tokens {tokens:,} (in {s['tokens_in']:,}, out {s['tokens_out']:,})",
        f"dollars {dollars(s['cost_micro'])} of {dollars(cents_limit * 10_000)} ({pct}% of the budget)",
        "denied " + (", ".join(f"{k} {v:,}" for k, v in sorted(denied.items())) if denied else "none"),
    ]
    if pct >= 100 or budget_denials > 0:
        code, verdict = EXIT_BUDGET, "the budget was reached"
    elif passed:
        code, verdict = EXIT_ALERT, f"passed the {passed[-1]}% alert level"
    else:
        code, verdict = EXIT_OK, "under every alert level"
    lines.append(f"result: {verdict}")
    for line in lines:
        _out(line)

    if purge:
        try:
            n = repo.delete_older_than(now=now, days=repo.RETENTION_DAYS)
            _out(f"deleted {n:,} rows older than {repo.RETENTION_DAYS} days")
        except Exception as exc:
            _out(f"could not delete old rows ({type(exc).__name__})")

    if code != EXIT_OK and (os.environ.get("OWNER_ALERT_EMAIL") or "").strip():
        sender = send_email or _send_email
        try:
            sender(f"Chat budget {label}: {verdict}", "\n".join(lines) + "\n")
            _out("alert email sent")
        except Exception as exc:  # the exit code still carries the alert
            _out(f"alert email failed ({type(exc).__name__})")
    return code


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--no-purge", action="store_true", help="report only; delete nothing")
    args = parser.parse_args(argv)
    try:
        from dotenv import load_dotenv

        load_dotenv()
    except ImportError:
        pass
    return run(purge=not args.no_purge)


if __name__ == "__main__":
    sys.exit(main())
