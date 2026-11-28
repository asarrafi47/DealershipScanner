"""S16: the read-only /dev page of today's phone-code sends against the caps (DESIGN.md section 17).

Plain text, no JavaScript, dev-console sign-in, 404 while phone sign-in is off. The totals are
faked (no database); test numbers are in the fictional +1 202 555 01xx range.
"""

from __future__ import annotations

import re
from decimal import Decimal

import pytest

from backend import settings_store as ss
from backend.auth import session as auth_session
from backend.auth.phone import retention
from backend.auth.phone.retention import DayTotals

URL = "/dev/phone/caps"
DIGITS7 = re.compile(r"\d{7,}")


@pytest.fixture
def main(app_factory, monkeypatch):
    for spec in ss.SPECS.values():
        monkeypatch.delenv(spec.env, raising=False)
    monkeypatch.delenv("SMS_ALERT_SEND_THRESHOLD", raising=False)
    monkeypatch.delenv("AUTH_PHONE_LOGIN_ENABLED", raising=False)
    app = app_factory()
    ss.set_loader(lambda: {})
    totals = DayTotals(
        sends=4,
        approvals=3,
        cost_cents=Decimal("24.000"),
        outcomes={"sent": 4, "blocked_cap": 1, "blocked_voip": 2},
        pools={"known": 2, "new": 1, "seller": 1},
    )
    monkeypatch.setattr(retention, "day_totals", lambda **kw: totals)
    monkeypatch.setattr(retention, "spend_since", lambda since, **kw: Decimal("130"))
    yield app
    ss.set_loader(None)


def _dev_client(main):
    c = main.app.test_client()
    with c.session_transaction() as s:
        s["admin_user_id"] = 1
        s["admin_username"] = "operator"
        s[auth_session.DEV_AUTH_AT] = auth_session._now()
    return c


def test_page_is_404_while_phone_sign_in_is_off_even_for_the_operator(main):
    assert _dev_client(main).get(URL).status_code == 404


def test_page_needs_the_dev_sign_in(main, monkeypatch):
    monkeypatch.setenv("AUTH_PHONE_LOGIN_ENABLED", "1")
    r = main.app.test_client().get(URL)
    assert r.status_code in (301, 302)
    assert "/dev/login" in r.headers["Location"]


def test_page_shows_todays_sends_against_the_caps_in_plain_text(main, monkeypatch):
    monkeypatch.setenv("AUTH_PHONE_LOGIN_ENABLED", "1")
    r = _dev_client(main).get(URL)
    assert r.status_code == 200
    assert r.mimetype == "text/plain"
    body = r.get_data(as_text=True)
    assert "Sending is on." in body
    assert "Texts sent: 4 of 30 (alert at 15)." in body
    assert "Cost estimate: $0.24 of $3.00." in body
    assert "Shopper texts: 3 of 27 (known 2, new 1 of 15, resend 0 of 6)." in body
    assert "Seller and dealer texts: 1 of 3." in body
    assert "Refused: blocked_cap 1, blocked_voip 2." in body
    assert "Prepaid: $1.30 spent of $20.00 since the start (no top-up date set); texts stop at $18.00 (90%)." in body
    assert "<script" not in body.lower() and "<" not in body
    assert not DIGITS7.search(body) and "+1202" not in body


def test_page_says_when_sending_is_paused_and_when_totals_fail(main, monkeypatch):
    monkeypatch.setenv("AUTH_PHONE_LOGIN_ENABLED", "1")
    monkeypatch.setenv("AUTH_PHONE_SENDS_PAUSED", "1")
    body = _dev_client(main).get(URL).get_data(as_text=True)
    assert "Sending is paused." in body

    def boom(**kw):
        raise RuntimeError("postgresql://someone:secret@db.invalid/cars")

    monkeypatch.setattr(retention, "day_totals", boom)
    body = _dev_client(main).get(URL).get_data(as_text=True)
    assert body == "Could not read today's sends (RuntimeError).\n"
