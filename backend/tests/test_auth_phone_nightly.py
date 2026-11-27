"""S16: the nightly phone-code job offline (argument parsing, alert levels, exit codes).

The database is faked: ``retention`` functions are replaced, so no Postgres is needed. The
Postgres tier is ``test_auth_phone_retention_pg.py``. Test numbers: fictional +1 202 555 01xx.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from backend import settings_store as ss
from backend.auth.phone import retention
from backend.auth.phone.retention import DayTotals
from backend.scripts import auth_phone_nightly as nightly

NOW = datetime(2026, 11, 15, 8, 30, tzinfo=timezone.utc)
LEVELS = nightly.Levels(send_cap=30, spend_cap_cents=300, alert_threshold=15)
FAKE_DSN = "postgresql://someone:not-a-real-secret@db.invalid:5432/cars"


@pytest.fixture(autouse=True)
def settings(monkeypatch):
    for spec in ss.SPECS.values():
        monkeypatch.delenv(spec.env, raising=False)
    for name in ("OWNER_ALERT_EMAIL", "SMS_ALERT_SEND_THRESHOLD"):
        monkeypatch.delenv(name, raising=False)
    ss.set_loader(lambda: {})
    yield
    ss.set_loader(None)


def _t(sends=0, approvals=0, cost=0, **outcomes) -> DayTotals:
    outs = {"sent": sends, **outcomes} if sends else dict(outcomes)
    return DayTotals(sends=sends, approvals=approvals, cost_cents=Decimal(cost), outcomes=outs)


# ---------------------------------------------------------------- arguments


def test_parse_args_defaults_and_flags():
    a = nightly.parse_args([])
    assert (a.dry_run, a.no_purge) == (False, False)
    a = nightly.parse_args(["--dry-run"])
    assert a.dry_run is True
    assert nightly.parse_args(["--no-purge"]).no_purge is True
    with pytest.raises(SystemExit):
        nightly.parse_args(["--delete-everything"])


def test_main_passes_the_flags_to_run(monkeypatch):
    seen = []
    monkeypatch.setattr(nightly, "run", lambda **kw: seen.append(kw) or 0)
    monkeypatch.setattr("dotenv.load_dotenv", lambda *a, **k: None, raising=False)
    nightly.main(["--dry-run"])
    nightly.main([])
    nightly.main(["--no-purge", "--dry-run"])
    assert seen == [
        {"purge": True, "dry_run": True},
        {"purge": True, "dry_run": False},
        {"purge": False, "dry_run": False},
    ]


# ---------------------------------------------------------------- alert levels


@pytest.mark.parametrize(
    "totals,code,why",
    [
        (_t(), nightly.EXIT_OK, "under every alert level"),
        (_t(14, 14, 84), nightly.EXIT_OK, "under every alert level"),
        (_t(15, 15, 90), nightly.EXIT_ALERT, "cap_50pct"),
        (_t(10, 10, 60, blocked_cap=1), nightly.EXIT_CAP, "cap_hit"),
        (_t(30, 30, 180), nightly.EXIT_CAP, "cap_hit"),
        (_t(5, 5, 300), nightly.EXIT_CAP, "cap_hit"),  # spend cap reached
        (_t(19, 0, 0), nightly.EXIT_ALERT, "cap_50pct"),  # under 20 texts: the ratio is not judged
    ],
)
def test_verdict_levels(totals, code, why):
    assert nightly.verdict(totals, LEVELS) == (code, why)


def test_low_approvals_need_20_texts_and_under_half():
    high = nightly.Levels(send_cap=100, spend_cap_cents=10_000, alert_threshold=50)
    assert nightly.verdict(_t(20, 9), high) == (nightly.EXIT_ALERT, "low_approvals")
    assert nightly.verdict(_t(20, 10), high) == (nightly.EXIT_OK, "under every alert level")
    assert nightly.verdict(_t(19, 0), high) == (nightly.EXIT_OK, "under every alert level")
    assert nightly.verdict(_t(20, 9), LEVELS) == (nightly.EXIT_ALERT, "cap_50pct, low_approvals")


def test_blocked_lists_only_refusals():
    t = _t(3, 2, blocked_cap=1, blocked_voip=2, provider_error=1)
    assert t.blocked == {"blocked_cap": 1, "blocked_voip": 2}
    assert _t().approval_pct is None


# ---------------------------------------------------------------- run()


class _Fake:
    def __init__(self, monkeypatch, totals, *, fail_read=False, fail_purge=False):
        self.purges = []
        self.mails = []

        def day_totals(**kw):
            if fail_read:
                raise RuntimeError(f"could not connect to {FAKE_DSN}")
            return totals

        def purge(**kw):
            if fail_purge:
                raise RuntimeError(f"lost {FAKE_DSN}")
            self.purges.append(kw)
            return {"challenges": 1, "sends": 2, "wrong_tries": 0, "lookup_cache": 0}

        monkeypatch.setattr(retention, "day_totals", day_totals)
        monkeypatch.setattr(retention, "purge", purge)
        monkeypatch.setattr(retention, "stale_reserving", lambda **kw: 0)
        monkeypatch.setenv("INVENTORY_DATABASE_URL", FAKE_DSN)


def test_refuses_to_run_without_a_database_address(monkeypatch, capsys):
    monkeypatch.setenv("INVENTORY_DATABASE_URL", "")
    monkeypatch.setenv("DATABASE_URL", "")
    monkeypatch.setattr("backend.db.connect._load_project_dotenv", lambda: None)
    called = []
    monkeypatch.setattr(retention, "purge", lambda **kw: called.append(kw))
    assert nightly.run(now=NOW) == nightly.EXIT_ERROR
    assert "no database address set" in capsys.readouterr().out
    assert called == []


def test_unreadable_totals_exit_1_delete_nothing_and_hide_the_address(monkeypatch, capsys):
    fake = _Fake(monkeypatch, _t(), fail_read=True)
    assert nightly.run(now=NOW) == nightly.EXIT_ERROR
    out = capsys.readouterr().out
    assert "could not read the totals (RuntimeError)" in out
    assert fake.purges == []
    assert "not-a-real-secret" not in out and "db.invalid" not in out


def test_quiet_day_exits_0_and_purges(monkeypatch, capsys):
    fake = _Fake(monkeypatch, _t(3, 3, 18))
    assert nightly.run(now=NOW) == nightly.EXIT_OK
    out = capsys.readouterr().out
    assert "deleted challenges 1, sends 2, wrong_tries 0, lookup_cache 0" in out
    assert "cost estimate $0.18 of $3.00" in out
    assert fake.purges == [{"now": NOW, "dry_run": False}]
    assert FAKE_DSN not in out


def test_dry_run_asks_the_purge_to_count_only(monkeypatch, capsys):
    fake = _Fake(monkeypatch, _t())
    assert nightly.run(now=NOW, dry_run=True) == nightly.EXIT_OK
    assert fake.purges == [{"now": NOW, "dry_run": True}]
    assert "would delete" in capsys.readouterr().out


def test_no_purge_skips_the_purge(monkeypatch):
    fake = _Fake(monkeypatch, _t())
    assert nightly.run(now=NOW, purge=False) == nightly.EXIT_OK
    assert fake.purges == []


def test_failed_purge_on_a_quiet_day_exits_1_and_keeps_an_alert_code(monkeypatch, capsys):
    _Fake(monkeypatch, _t(), fail_purge=True)
    assert nightly.run(now=NOW) == nightly.EXIT_ERROR
    out = capsys.readouterr().out
    assert "could not delete old rows (RuntimeError)" in out and "db.invalid" not in out
    _Fake(monkeypatch, _t(15, 15), fail_purge=True)
    assert nightly.run(now=NOW) == nightly.EXIT_ALERT


def test_alert_emails_the_owner_once_and_quiet_days_do_not(monkeypatch, capsys):
    monkeypatch.setenv("OWNER_ALERT_EMAIL", "owner@example.com")
    mails = []
    _Fake(monkeypatch, _t(3, 3))
    assert nightly.run(now=NOW, send_email=lambda s, t: mails.append((s, t))) == nightly.EXIT_OK
    assert mails == []
    _Fake(monkeypatch, _t(10, 10, blocked_cap=2))
    assert nightly.run(now=NOW, send_email=lambda s, t: mails.append((s, t))) == nightly.EXIT_CAP
    assert len(mails) == 1
    subject, text = mails[0]
    assert subject == "Phone codes 2026-11-14: cap_hit"
    assert "blocked blocked_cap 2" in text and "+1202" not in text


def test_alert_threshold_follows_the_variable(monkeypatch, capsys):
    monkeypatch.setenv("SMS_ALERT_SEND_THRESHOLD", "5")
    _Fake(monkeypatch, _t(5, 5))
    assert nightly.run(now=NOW, purge=False) == nightly.EXIT_ALERT
    assert "sends 5 of 30 (alert at 5)" in capsys.readouterr().out
