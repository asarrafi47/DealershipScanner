"""RFC 9457 errors from Twilio are vendor errors too (TwilioServiceException is not a TwilioRestException).

Before: a problem-details body (``type``, ``title``, ``status``, ``code``) fell through to "unexpected
exception": a 429 became provider_error instead of throttled, a 404 on check ended the challenge as
unknown instead of expired, a 404 on Lookup became lookup_failed, and the log line dropped status and
code. Fictional numbers only.
"""

from __future__ import annotations

import json
import logging

import pytest
from twilio.base.exceptions import TwilioRestException, TwilioServiceException
from twilio.base.version import Version
from twilio.http.response import Response

from backend.auth.phone import provider as p
from backend.auth.phone import twilio_verify as tv
from backend.tests.test_auth_phone_twilio_verify import FakeClient

NUMBER = "+15555550123"
SERVICE_SID = "VAfeedfacefeedfacefeedfacefeedface"
DETAIL = "Max send attempts reached for %s on service %s" % (NUMBER, SERVICE_SID)


def problem(status, code):
    """Built the way the Twilio client builds it, from an RFC 9457 response body."""
    body = json.dumps({"type": "https://www.twilio.test/docs/errors/%s" % code, "title": "Too Many Requests",
                       "status": status, "code": code, "detail": DETAIL, "instance": "/v2/x/%s" % NUMBER})
    exc = Version.exception("POST", "https://verify.twilio.test/v2/%s" % SERVICE_SID, Response(status, body), "x")
    assert isinstance(exc, TwilioServiceException) and not isinstance(exc, TwilioRestException)
    return exc


def adapter():
    fc = FakeClient()
    return fc, tv.TwilioVerifyProvider(fc, SERVICE_SID, cache=None, hash_fn=lambda e: "hash:" + e[-4:])


def provider_line(caplog):
    (line,) = [r.getMessage() for r in caplog.records if "event=provider_error" in r.getMessage()]
    return line


@pytest.mark.parametrize("status,code,start,check,lookup", [
    (429, 60203, p.START_THROTTLED, p.CHECK_THROTTLED, p.LINE_LOOKUP_FAILED),
    (429, 60202, p.START_THROTTLED, p.CHECK_TOO_MANY, p.LINE_LOOKUP_FAILED),
    (404, 20404, p.START_PROVIDER_ERROR, p.CHECK_EXPIRED, p.LINE_INVALID_NUMBER),
    (400, 60200, p.START_INVALID_NUMBER, p.CHECK_WRONG_CODE, p.LINE_LOOKUP_FAILED),
    (401, 20003, p.START_UNAVAILABLE, p.CHECK_UNKNOWN, p.LINE_LOOKUP_FAILED),
    (403, 60410, p.START_BLOCKED, p.CHECK_UNKNOWN, p.LINE_LOOKUP_FAILED),
    (503, 20503, p.START_UNAVAILABLE, p.CHECK_UNKNOWN, p.LINE_LOOKUP_FAILED),
])
def test_rfc9457_errors_map_like_legacy_ones(status, code, start, check, lookup, caplog):
    caplog.set_level(logging.DEBUG)
    for op, want in (("start", start), ("check", check), ("lookup", lookup)):
        caplog.clear()
        fc, prov = adapter()
        fc.queues[op].append(problem(status, code))
        if op == "start":
            got = prov.start(NUMBER, locale="en")
        elif op == "check":
            got = prov.check(NUMBER, code="482913", provider_ref="VEabc")
        else:
            got = prov.lookup(NUMBER)
        assert got.reason == want, op
        # The same reason a legacy TwilioRestException with these numbers gets.
        fc2, prov2 = adapter()
        fc2.queues[op].append(TwilioRestException(status, "u", "m", code))
        legacy = {"start": lambda: prov2.start(NUMBER, locale="en"),
                  "check": lambda: prov2.check(NUMBER, code="482913", provider_ref="VEabc"),
                  "lookup": lambda: prov2.lookup(NUMBER)}[op]()
        assert legacy.reason == got.reason, op
        line = [r.getMessage() for r in caplog.records if "event=provider_error" in r.getMessage()][0]
        assert "op=%s reason=%s http_status=%d vendor_code=%d" % (op, want, status, code) in line
        assert "5555550123" not in caplog.text and SERVICE_SID not in caplog.text and "Max send" not in caplog.text


@pytest.mark.parametrize("status,code", [(True, False), (False, True)])
def test_a_bool_status_or_code_is_not_an_integer(status, code, caplog):
    caplog.set_level(logging.DEBUG)
    fc, prov = adapter()
    fc.queues["start"].append(TwilioRestException(status, "u", "m", code))
    assert prov.start(NUMBER, locale="en").reason == p.START_PROVIDER_ERROR
    assert provider_line(caplog).endswith("http_status=None vendor_code=None")
