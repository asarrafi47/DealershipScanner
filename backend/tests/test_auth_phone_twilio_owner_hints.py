"""Owner hints on the Twilio adapter's provider_error line: the console fix, never the number.

A trial account texting an unverified number (21608), a restricted API key without Verify or Lookup
permission (8021, bare 403), bad credentials (20003, 401) and a suspended or empty account (20005,
402) all used to log only a status and a code. The line now names the cause for the owner; the
reason code (and so the shopper's sentence) is unchanged. Fictional numbers only.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest
import requests
from twilio.base.exceptions import TwilioRestException
from twilio.base.version import Version
from twilio.http.response import Response

from backend.auth.phone import provider as p
from backend.auth.phone import twilio_verify as tv
from backend.tests.test_auth_phone_twilio_verify import FakeClient

NUMBER = "+15555550123"
SERVICE_SID = "VAfeedfacefeedfacefeedfacefeedface"
SECRET = "not-a-real-secret-0123456789abcdef"
VENDOR_MSG = "Trial accounts cannot send to unverified number %s (service %s, key %s)" % (NUMBER, SERVICE_SID, SECRET)


def rest(status, code):
    return TwilioRestException(status, "https://verify.twilio.test/%s/%s" % (SERVICE_SID, NUMBER), VENDOR_MSG, code)


def adapter():
    fc = FakeClient()
    return fc, tv.TwilioVerifyProvider(fc, SERVICE_SID, cache=None, hash_fn=lambda e: "hash:" + e[-4:])


def provider_line(caplog):
    (line,) = [r.getMessage() for r in caplog.records if "event=provider_error" in r.getMessage()]
    return line


HINTED = [
    (403, 21608, tv.HINT_TRIAL_UNVERIFIED),
    (400, 21608, tv.HINT_TRIAL_UNVERIFIED),
    (403, 8021, tv.HINT_KEY_PERMISSION),
    (401, 8021, tv.HINT_KEY_PERMISSION),
    (403, None, tv.HINT_KEY_PERMISSION),
    (403, 403, tv.HINT_KEY_PERMISSION),  # a non-JSON 403 body: the client copies the status into the code
    (401, 20003, tv.HINT_AUTH_FAILED),
    (401, None, tv.HINT_AUTH_FAILED),
    (401, 401, tv.HINT_AUTH_FAILED),
    (400, 20005, tv.HINT_ACCOUNT_INACTIVE),
    (403, 20005, tv.HINT_ACCOUNT_INACTIVE),
    (402, None, tv.HINT_ACCOUNT_INACTIVE),
]
UNHINTED = [(403, 60410), (400, 60200), (404, 20404), (429, 60203), (429, 60212), (429, None), (500, None),
            (503, 503), (None, None)]


@pytest.mark.parametrize("status,code,hint", HINTED)
def test_owner_hint_table(status, code, hint):
    assert tv.owner_hint(status, code) == hint


@pytest.mark.parametrize("status,code", UNHINTED)
def test_no_hint_for_causes_the_owner_cannot_fix_in_the_console(status, code):
    assert tv.owner_hint(status, code) is None


@pytest.mark.parametrize("op", ["start", "check", "lookup"])
@pytest.mark.parametrize("status,code,hint", HINTED)
def test_every_op_logs_the_hint_and_keeps_its_reason(op, status, code, hint, caplog):
    caplog.set_level(logging.DEBUG)
    fc, prov = adapter()
    # What the same error gave before hints existed: the reason must not move.
    expected = {
        "start": tv.TwilioVerifyProvider._start_reason,
        "check": tv.TwilioVerifyProvider._check_reason,
        "lookup": lambda e: p.LINE_LOOKUP_FAILED,
    }[op](rest(status, code))
    fc.queues[op].append(rest(status, code))
    if op == "start":
        result = prov.start(NUMBER, locale="en")
    elif op == "check":
        result = prov.check(NUMBER, code="482913", provider_ref="VEabc")
    else:
        result = prov.lookup(NUMBER)
    assert result.reason == expected
    line = provider_line(caplog)
    assert line.endswith(
        "op=%s reason=%s http_status=%s vendor_code=%s hint=%s" % (op, expected, status, code, hint)
    ), line
    for needle in (NUMBER, "5555550123", SERVICE_SID, SECRET, "Trial accounts", "twilio.test"):
        assert needle not in caplog.text + repr(result), needle


def test_a_restricted_key_without_lookup_permission_fails_closed_and_says_why(caplog):
    """Fail closed is right (DESIGN.md 6.7: an unlookupable number may be VoIP), but the owner must be able
    to see that the key needs Lookup permission: until then every new number is refused."""
    caplog.set_level(logging.DEBUG)
    fc, prov = adapter()
    fc.queues["lookup"].extend([rest(403, 8021), rest(401, 20003)])
    for expected_hint in (tv.HINT_KEY_PERMISSION, tv.HINT_AUTH_FAILED):
        caplog.clear()
        info = prov.lookup(NUMBER)
        assert (info.ok, info.reason) == (False, p.LINE_LOOKUP_FAILED)
        assert "op=lookup reason=lookup_failed" in provider_line(caplog)
        assert provider_line(caplog).endswith("hint=%s" % expected_hint)


def test_a_real_client_html_403_still_gets_the_permission_hint(caplog):
    caplog.set_level(logging.DEBUG)
    exc = Version.exception("GET", "https://lookups.twilio.test/v2/PhoneNumbers/%s" % NUMBER,
                            Response(403, "<html><body>Forbidden %s</body></html>" % NUMBER), "Unable to fetch record")
    fc, prov = adapter()
    fc.queues["lookup"].append(exc)
    prov.lookup(NUMBER)
    line = provider_line(caplog)
    assert line.endswith("http_status=403 vendor_code=403 hint=twilio_key_permission")
    assert "5555550123" not in caplog.text and "Forbidden" not in caplog.text


@pytest.mark.parametrize("exc", [requests.ReadTimeout("x"), requests.ConnectionError("x"), OSError("x"), ValueError("x")])
def test_non_vendor_failures_carry_no_hint(exc, caplog):
    caplog.set_level(logging.DEBUG)
    fc, prov = adapter()
    fc.queues["start"].extend([exc, SimpleNamespace(sid="VEdup")])
    prov.start(NUMBER, locale="en")
    assert "hint=" not in provider_line(caplog)


@pytest.mark.parametrize("status,code", UNHINTED)
def test_unhinted_vendor_errors_keep_the_old_line(status, code, caplog):
    caplog.set_level(logging.DEBUG)
    fc, prov = adapter()
    fc.queues["check"].append(rest(status, code))
    result = prov.check(NUMBER, code="482913", provider_ref="VEabc")
    assert provider_line(caplog).endswith(
        "op=check reason=%s http_status=%s vendor_code=%s" % (result.reason, status, code)
    )
