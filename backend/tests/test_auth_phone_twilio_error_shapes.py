"""Twilio error shapes on the real-provider path, without calling Twilio.

Every failure Twilio (or the network under it) can hand the adapter is raised by a fake client, and
for each one we pin: the reason code, the one log line (event, op, reason, integer status and vendor
code; never the number, the vendor text, the service SID or a credential), the sentence the service
would show (generic, and the same whether or not the number has an account), and that nothing which
may already have produced a text is retried. Fictional numbers only (+1 555 555 01xx, +1 202 555 01xx).
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
import requests
from twilio.base.exceptions import TwilioRestException
from twilio.base.version import Version
from twilio.http.response import Response

from backend.auth.phone import provider as p
from backend.auth.phone import service as svc
from backend.auth.phone import twilio_verify as tv
from backend.auth.phone.turnstile import TurnstileResult
from backend.tests.test_auth_phone_twilio_verify import FakeClient

NUMBER = "+15555550123"  # adapter-level tests: the adapter never parses the number
SVC_NUMBER = "+12025550147"  # service-level tests: must pass libphonenumber's US check
SERVICE_SID = "VAfeedfacefeedfacefeedfacefeedface"
ACCOUNT_SID = "ACdeadbeefdeadbeefdeadbeefdeadbeef"
KEY_SID = "SKcafebabecafebabecafebabecafebabe"
SECRET = "not-a-real-secret-0123456789abcdef"
CODE = "482913"
VENDOR_MSG = (
    "Unable to create record: The number %s is unverified. Trial accounts cannot send messages to "
    "unverified numbers; verify %s at twilio.com/user/account/phone-numbers/verified (service %s, "
    "account %s, key %s:%s)" % (NUMBER, SVC_NUMBER, SERVICE_SID, ACCOUNT_SID, KEY_SID, SECRET)
)
HTML_BODY = (
    "<html><head><title>503 Service Temporarily Unavailable</title></head><body>"
    "<center><h1>503 Service Temporarily Unavailable</h1></center><!-- %s %s --></body></html>"
    % (NUMBER, SERVICE_SID)
)
URI = "https://verify.twilio.test/v2/Services/%s/Verifications?To=%s" % (SERVICE_SID, NUMBER)

# Things that must never appear in a log line or a result, in any form.
FORBIDDEN = (
    NUMBER, "5555550123", SVC_NUMBER, "2025550147", SERVICE_SID, ACCOUNT_SID, KEY_SID, SECRET, CODE,
    "unverified", "Trial accounts", "twilio.com", "twilio.test", "<html", "Unavailable</", "Unable to create",
)


def rest(status, code, msg=VENDOR_MSG):
    return TwilioRestException(status, URI, msg, code, "POST")


class _NewConnectionError(Exception):
    """Stands in for urllib3's NewConnectionError: the adapter matches it by class name."""


_NewConnectionError.__name__ = "NewConnectionError"


# (id, exception factory, http_status logged, vendor_code logged, start reason, check reason, lookup reason)
SHAPES = [
    ("403-21608-trial-unverified", lambda: rest(403, 21608), 403, 21608,
     p.START_UNAVAILABLE, p.CHECK_UNKNOWN, p.LINE_LOOKUP_FAILED),
    ("401-20003-bad-credentials", lambda: rest(401, 20003), 401, 20003,
     p.START_UNAVAILABLE, p.CHECK_UNKNOWN, p.LINE_LOOKUP_FAILED),
    ("401-bare", lambda: rest(401, None), 401, None,
     p.START_UNAVAILABLE, p.CHECK_UNKNOWN, p.LINE_LOOKUP_FAILED),
    ("403-8021-restricted-key", lambda: rest(403, 8021), 403, 8021,
     p.START_UNAVAILABLE, p.CHECK_UNKNOWN, p.LINE_LOOKUP_FAILED),
    ("401-8021-restricted-key", lambda: rest(401, 8021), 401, 8021,
     p.START_UNAVAILABLE, p.CHECK_UNKNOWN, p.LINE_LOOKUP_FAILED),
    ("400-60200-invalid-parameter", lambda: rest(400, 60200), 400, 60200,
     p.START_INVALID_NUMBER, p.CHECK_WRONG_CODE, p.LINE_LOOKUP_FAILED),
    ("404-60200-invalid-parameter", lambda: rest(404, 60200), 404, 60200,
     p.START_INVALID_NUMBER, p.CHECK_EXPIRED, p.LINE_INVALID_NUMBER),
    ("429-60202-max-check-attempts", lambda: rest(429, 60202), 429, 60202,
     p.START_THROTTLED, p.CHECK_TOO_MANY, p.LINE_LOOKUP_FAILED),
    ("429-60203-max-send-attempts", lambda: rest(429, 60203), 429, 60203,
     p.START_THROTTLED, p.CHECK_THROTTLED, p.LINE_LOOKUP_FAILED),
    ("429-60212-concurrent-requests", lambda: rest(429, 60212), 429, 60212,
     p.START_THROTTLED, p.CHECK_THROTTLED, p.LINE_LOOKUP_FAILED),
    ("404-20404-not-found", lambda: rest(404, 20404), 404, 20404,
     p.START_PROVIDER_ERROR, p.CHECK_EXPIRED, p.LINE_INVALID_NUMBER),
    ("429-bare", lambda: rest(429, None), 429, None,
     p.START_THROTTLED, p.CHECK_THROTTLED, p.LINE_LOOKUP_FAILED),
    ("html-body-no-status-no-code", lambda: rest(None, None, HTML_BODY), None, None,
     p.START_PROVIDER_ERROR, p.CHECK_UNKNOWN, p.LINE_LOOKUP_FAILED),
    ("timeout", lambda: requests.Timeout(VENDOR_MSG), None, None,
     p.START_UNAVAILABLE, p.CHECK_NETWORK_ERROR, p.LINE_LOOKUP_FAILED),
    ("read-timeout", lambda: requests.ReadTimeout(VENDOR_MSG), None, None,
     p.START_UNAVAILABLE, p.CHECK_NETWORK_ERROR, p.LINE_LOOKUP_FAILED),
    ("connect-timeout", lambda: requests.ConnectTimeout(VENDOR_MSG), None, None,
     p.START_UNAVAILABLE, p.CHECK_NETWORK_ERROR, p.LINE_LOOKUP_FAILED),
    ("connection-error-after-send", lambda: requests.ConnectionError(VENDOR_MSG), None, None,
     p.START_NETWORK_ERROR, p.CHECK_NETWORK_ERROR, p.LINE_LOOKUP_FAILED),
    ("connection-error-refused", lambda: requests.ConnectionError(_NewConnectionError(VENDOR_MSG)), None, None,
     p.START_NETWORK_ERROR, p.CHECK_NETWORK_ERROR, p.LINE_LOOKUP_FAILED),
    ("oserror", lambda: OSError(VENDOR_MSG), None, None,
     p.START_NETWORK_ERROR, p.CHECK_NETWORK_ERROR, p.LINE_LOOKUP_FAILED),
]
IDS = [s[0] for s in SHAPES]

# Shapes that provably never reached Twilio: the only ones start may retry (once).
CONNECT_FAILURES = {"connect-timeout", "connection-error-refused"}

LINE_RE = re.compile(
    r"event=provider_error provider=twilio_verify op=(?P<op>\w+) reason=(?P<reason>\w+) "
    r"http_status=(?P<status>\d+|None) vendor_code=(?P<code>\d+|None)"
)


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.delenv("REDIS_URL", raising=False)
    monkeypatch.setenv("PHONE_HASH_PEPPER", "t" * 40)


def adapter():
    fc = FakeClient()
    return fc, tv.TwilioVerifyProvider(fc, SERVICE_SID, cache=None, hash_fn=lambda e: "hash:" + e[-4:])


def vendor_calls(fc, kind):
    return [c for c in fc.calls if c[0] == kind]


def provider_lines(caplog):
    return [r.getMessage() for r in caplog.records if r.name == "auth_phone" and "event=provider_error" in r.getMessage()
            and "provider=twilio_verify" in r.getMessage()]


def assert_line(line, *, op, reason, status, code):
    m = LINE_RE.search(line)
    assert m, line
    assert (m["op"], m["reason"]) == (op, reason)
    assert m["status"] == ("None" if status is None else str(int(status)))
    assert m["code"] == ("None" if code is None else str(int(code)))


def assert_clean(text):
    for needle in FORBIDDEN:
        assert needle not in text, needle


# ---- adapter: reason, log line, retries ------------------------------------------------------

@pytest.mark.parametrize("shape", SHAPES, ids=IDS)
def test_start_error_shape(shape, caplog):
    name, make_exc, status, code, start_reason, _, _ = shape
    caplog.set_level(logging.DEBUG)
    fc, prov = adapter()
    # Queue a second answer that would succeed: any retry that is not allowed would turn it into a send.
    fc.queues["start"].extend([make_exc(), make_exc() if name in CONNECT_FAILURES else SimpleNamespace(sid="VEdup")])
    result = prov.start(NUMBER, locale="en")
    assert (result.ok, result.reason, result.provider_ref) == (False, start_reason, None)
    expected_calls = 2 if name in CONNECT_FAILURES else 1
    assert len(vendor_calls(fc, "start")) == expected_calls == result.attempts
    lines = provider_lines(caplog)
    assert len(lines) == expected_calls
    for line in lines:
        assert_line(line, op="start", reason=start_reason, status=status, code=code)
    assert_clean(caplog.text + repr(result))


@pytest.mark.parametrize("shape", SHAPES, ids=IDS)
def test_check_error_shape(shape, caplog):
    _, make_exc, status, code, _, check_reason, _ = shape
    caplog.set_level(logging.DEBUG)
    fc, prov = adapter()
    fc.queues["check"].extend([make_exc(), SimpleNamespace(status="approved")])
    result = prov.check(NUMBER, code=CODE, provider_ref="VEabc")
    assert (result.approved, result.reason) == (False, check_reason)
    assert len(vendor_calls(fc, "check")) == 1  # a check is never retried by the adapter
    (line,) = provider_lines(caplog)
    assert_line(line, op="check", reason=check_reason, status=status, code=code)
    assert_clean(caplog.text + repr(result))


@pytest.mark.parametrize("shape", SHAPES, ids=IDS)
def test_lookup_error_shape(shape, caplog):
    _, make_exc, status, code, _, _, lookup_reason = shape
    caplog.set_level(logging.DEBUG)
    fc, prov = adapter()
    fc.queues["lookup"].extend([make_exc()])
    info = prov.lookup(NUMBER)
    assert (info.ok, info.reason, info.line_type) == (False, lookup_reason, None)  # every failure refuses
    assert len(vendor_calls(fc, "lookup")) == 1
    (line,) = provider_lines(caplog)
    assert_line(line, op="lookup", reason=lookup_reason, status=status, code=code)
    assert_clean(caplog.text + repr(info))


@pytest.mark.parametrize("exc", [requests.ReadTimeout("x"), requests.Timeout("x"), requests.ConnectionError("x")])
def test_no_retry_after_a_read_timeout_or_reset_even_when_the_retry_would_succeed(exc):
    """The text may already be on its way: a second create would be a second paid text."""
    fc, prov = adapter()
    fc.queues["start"].extend([exc, SimpleNamespace(sid="VEsecond")])
    result = prov.start(NUMBER, locale="en")
    assert (result.ok, result.attempts) == (False, 1)
    assert len(vendor_calls(fc, "start")) == 1
    assert fc.queues["start"]  # the second answer was never asked for


def test_a_real_twilio_html_error_body_becomes_status_and_code_only(caplog):
    """What the Twilio client itself raises when the body is not JSON (a proxy or load balancer page):
    status and code are the HTTP status, the message carries the HTML."""
    caplog.set_level(logging.DEBUG)
    exc = Version.exception("POST", URI, Response(503, HTML_BODY), "Unable to create record")
    assert isinstance(exc, TwilioRestException) and (exc.status, exc.code) == (503, 503)
    fc, prov = adapter()
    fc.queues["start"].append(exc)
    result = prov.start(NUMBER, locale="en")
    assert result.reason == p.START_UNAVAILABLE
    (line,) = provider_lines(caplog)
    assert_line(line, op="start", reason=p.START_UNAVAILABLE, status=503, code=503)
    assert_clean(caplog.text + repr(result))


@pytest.mark.parametrize("status,code", [("429", "60203"), (429.0, 60203.0), (b"429", None), (None, "x")])
def test_odd_status_and_code_types_never_crash_and_log_as_none(status, code, caplog):
    caplog.set_level(logging.DEBUG)
    fc, prov = adapter()
    fc.queues["start"].append(rest(status, code))
    fc.queues["check"].append(rest(status, code))
    fc.queues["lookup"].append(rest(status, code))
    assert prov.start(NUMBER, locale="en").ok is False
    assert prov.check(NUMBER, code=CODE, provider_ref="VEabc").approved is False
    assert prov.lookup(NUMBER).ok is False
    lines = provider_lines(caplog)
    assert len(lines) == 3
    for line in lines:
        m = LINE_RE.search(line)
        assert m and (m["status"], m["code"]) == ("None", "None"), line
    assert_clean(caplog.text)


# ---- service: what the shopper sees -----------------------------------------------------------

class OkTurnstile:
    def verify(self, token, ip):
        return TurnstileResult(True, "ok")


class FakeRepo:
    """Just the repository calls a failed send or check makes. No database."""

    T0 = datetime(2026, 11, 16, 17, 0, tzinfo=timezone.utc)

    def __init__(self, known: bool):
        self.known = known
        self.outcomes: list[str] = []
        self.finished: list[str] = []
        self.refunds = 0
        self.lookups: dict = {}

    # start
    SendRow = staticmethod(lambda **kw: kw)

    def wrong_try_state(self, key, *, now):
        return SimpleNamespace(locked=False)

    def recent_vendor_outcomes(self, *, limit):
        return []

    def find_identity_by_phone(self, e164):
        return SimpleNamespace(state="active", user_id=7) if self.known else None

    def reserve_send(self, row, *, now, decide):
        return SimpleNamespace(send_id=1)

    def record_send(self, row, *, now):
        self.outcomes.append(row.get("outcome"))

    def set_send_outcome(self, send_id, outcome, *, est_cost_cents):
        self.outcomes.append(outcome)

    # check
    def fetch_challenge(self, public_id, nonce):
        return SimpleNamespace(
            status="open", phone_e164=SVC_NUMBER, phone_hash="h" * 64, purpose="login",
            expires_at=self.T0 + timedelta(minutes=5), provider_ref="VEabc", created_at=self.T0,
        )

    def reserve_attempt(self, public_id, nonce, *, now, max_attempts):
        return 1

    def refund_attempt(self, public_id, nonce):
        self.refunds += 1

    def finish_challenge(self, public_id, status, *, now, session_nonce):
        self.finished.append(status)

    def add_wrong_try(self, key, *, now, max_tries, lock_minutes):
        return SimpleNamespace(locked=False)


class DictCache:
    def __init__(self):
        self.d = {}

    def get(self, h):
        return self.d.get(h)

    def put(self, h, info, ttl_s):
        self.d[h] = info


def service_for(known: bool, *, start_exc=None, check_exc=None, lookup_exc=None):
    fc, prov = adapter()
    fc.queues["lookup"].append(lookup_exc if lookup_exc is not None else
                               SimpleNamespace(valid=True, country_code="US", line_type_intelligence={"type": "mobile"}))
    if start_exc is not None:
        fc.queues["start"].extend([start_exc, start_exc])
    if check_exc is not None:
        fc.queues["check"].append(check_exc)
    repo = FakeRepo(known)
    service = svc.AuthPhoneService(
        prov, OkTurnstile(), repo, lambda: FakeRepo.T0, svc.ServiceConfig(),
        lookup_cache=DictCache(), limits_loader=lambda: svc.lim.Limits(), sends_paused=lambda: False,
    )
    return service, repo, fc


def do_start(service):
    return service.start(SVC_NUMBER, consent=True, turnstile_token="t", ip="10.0.0.1", device_hash="d" * 64,
                         session_nonce="n" * 24)


# Sentences a failed send may show; none of them says anything about an account.
GENERIC_START = {svc.UNAVAILABLE, svc.COULD_NOT_SEND, svc.OPTED_OUT}
GENERIC_CHECK = {svc.CODE_RETRY, svc.CODE_FAILED, svc.START_AGAIN}


def test_the_generic_sentences_never_mention_accounts():
    for sentence in GENERIC_START | GENERIC_CHECK:
        low = sentence.lower()
        for word in ("account", "sign up", "signed up", "registered", "new user", "welcome back", "exists"):
            assert word not in low, (sentence, word)


@pytest.mark.parametrize("shape", SHAPES, ids=IDS)
def test_start_failure_reads_the_same_for_known_and_unknown_numbers(shape, caplog):
    caplog.set_level(logging.DEBUG)
    answers = []
    for known in (True, False):
        service, repo, fc = service_for(known, start_exc=shape[1]())
        out = do_start(service)
        assert out.ok is False and out.public_id is None
        answers.append((out.ok, out.reason, out.sentence, out.retry_after_s))
        assert len(vendor_calls(fc, "lookup")) == (0 if known else 1)  # known numbers skip Lookup (DESIGN.md 6.7)
        assert "sent" not in repo.outcomes
    assert answers[0] == answers[1]
    assert answers[0][2] in GENERIC_START
    assert_clean(caplog.text)


@pytest.mark.parametrize("shape", SHAPES, ids=IDS)
def test_check_failure_reads_generic_and_retries_only_when_the_code_was_not_judged(shape, caplog):
    caplog.set_level(logging.DEBUG)
    service, repo, fc = service_for(False, check_exc=shape[1]())
    out = service.check("pub", "n" * 24, CODE, device_hash="d" * 64)
    assert out.approved is False and out.phone_e164 is None
    assert out.sentence in GENERIC_CHECK
    if shape[5] in (p.CHECK_NETWORK_ERROR, p.CHECK_THROTTLED):
        assert (out.sentence, repo.refunds, repo.finished) == (svc.CODE_RETRY, 1, [])  # try again, attempt refunded
    assert len(vendor_calls(fc, "check")) == 1
    assert_clean(caplog.text)


@pytest.mark.parametrize("shape", SHAPES, ids=IDS)
def test_lookup_failure_for_a_new_number_refuses_with_a_generic_sentence(shape, caplog):
    """Lookup runs only for numbers without an account, and fails closed (DESIGN.md 6.7): nothing is sent."""
    caplog.set_level(logging.DEBUG)
    service, repo, fc = service_for(False, lookup_exc=shape[1]())
    out = do_start(service)
    assert out.ok is False
    assert out.sentence in (svc.UNAVAILABLE, svc.COULD_NOT_SEND)
    assert vendor_calls(fc, "start") == []
    expected = "blocked_cap" if shape[6] == p.LINE_LOOKUP_FAILED else "blocked_voip"
    assert repo.outcomes == [expected]
    assert_clean(caplog.text)
