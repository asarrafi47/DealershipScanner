"""Tiered network observer: scoring, classification, sniffing, ledger, and handler flow."""

from __future__ import annotations

import asyncio
import json

import pytest

from backend.scanner.network_observer import (
    Classification,
    NetworkObserver,
    ObserverLedger,
    TIER_CAPTURE,
    TIER_FINGERPRINT,
    TIER_IGNORE,
    best_vehicle_list_score,
    classify_payload,
    looks_like_vin,
    parse_sniffed_json,
    schema_hash,
    score_vehicle_list,
)

DEALER = "https://www.bmwdealer.com"


def _vin(i: int = 0) -> str:
    return ("1HGCM82633A%06d" % i)[:17]


def _vehicle(i: int = 0, vin_key: str = "vin") -> dict:
    return {
        vin_key: _vin(i),
        "year": 2024,
        "make": "BMW",
        "model": "X5",
        "price": 61999,
        "stockNumber": f"B{i:04d}",
    }


# ── looks_like_vin ───────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("value", "expect"),
    [
        ("1HGCM82633A004352", True),
        ("1hgcm82633a004352", True),  # case-insensitive
        ("1HGCM82633A00435", False),  # 16 chars
        ("1HGCM82633A0043521", False),  # 18 chars
        ("IHGCM82633A004352", False),  # I not in VIN alphabet
        ("11111111111111111", False),  # no letters
        ("ABCDEFGHJKLMNPRST", False),  # no digits
        (12345678901234567, False),  # not a string
        (None, False),
    ],
)
def test_looks_like_vin(value, expect):
    assert looks_like_vin(value) is expect


# ── scoring ──────────────────────────────────────────────────────────────────


def test_score_qualifies_with_nonstandard_vin_key():
    # The legacy qualifier requires a literal `vin` key; scoring must not.
    lst = [_vehicle(i, vin_key="vinNumber") for i in range(5)]
    s = score_vehicle_list(lst)
    assert s.vin_items == 5
    assert s.qualifies


def test_score_qualifies_with_nested_vin():
    lst = [{"vehicle": {"identifiers": {"vinCode": _vin(i)}}, "price": 30000, "year": 2023}
           for i in range(4)]
    s = score_vehicle_list(lst)
    assert s.vin_items == 4


def test_score_rejects_non_vehicle_list():
    lst = [{"label": "Financing", "url": "/finance"}, {"label": "Service", "url": "/service"}]
    s = score_vehicle_list(lst)
    assert s.vin_items == 0
    assert not s.qualifies


def test_best_vehicle_list_score_finds_nested_list():
    body = {"data": {"searchResults": {"items": [_vehicle(i, "vehicleVin") for i in range(6)]}}}
    s = best_vehicle_list_score(body)
    assert s.vin_items == 6
    assert s.qualifies


# ── classification ───────────────────────────────────────────────────────────


def test_classify_capture_legacy_key():
    body = {"inventory": [_vehicle(i) for i in range(4)]}
    cls = classify_payload(f"{DEALER}/api/inventory", body, DEALER)
    assert cls.tier == TIER_CAPTURE
    assert cls.reason == "legacy"


def test_classify_capture_by_score_only():
    # `vinNumber` key: fails legacy qualifier, passes scoring.
    body = {"results": [_vehicle(i, vin_key="vinNumber") for i in range(5)]}
    cls = classify_payload(f"{DEALER}/api/search", body, DEALER)
    assert cls.tier == TIER_CAPTURE
    assert cls.reason == "score"


def test_classify_score_capture_disabled(monkeypatch):
    monkeypatch.setenv("SCANNER_SCORE_INTERCEPT", "0")
    body = {"results": [_vehicle(i, vin_key="vinNumber") for i in range(5)]}
    cls = classify_payload(f"{DEALER}/api/search", body, DEALER)
    assert cls.tier == TIER_FINGERPRINT  # still worth learning from


def test_classify_denied_vehicle_payload_becomes_fingerprint():
    body = {"inventory": [_vehicle(i) for i in range(4)]}
    cls = classify_payload("https://unknown-inventory-cdn.io/feed", body, DEALER)
    assert cls.tier == TIER_FINGERPRINT
    assert cls.reason == "url_denied_vehicle_payload"
    assert cls.url_allowed is False


def test_classify_near_miss_single_vin():
    body = {"widget": {"featured": [{"vin": _vin(1)}]}}
    cls = classify_payload(f"{DEALER}/api/widget", body, DEALER)
    # 1 VIN row without inventory container keys: not captured, but fingerprinted.
    assert cls.tier in (TIER_FINGERPRINT, TIER_CAPTURE)
    assert cls.tier == TIER_FINGERPRINT


def test_classify_ignore_unrelated_json():
    body = {"session": "abc", "consent": True}
    cls = classify_payload(f"{DEALER}/api/consent", body, DEALER)
    assert cls.tier == TIER_IGNORE


# ── sniffed JSON parsing ─────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("text", "expect"),
    [
        ('{"a": 1}', {"a": 1}),
        (')]}\',\n{"a": 1}', {"a": 1}),  # anti-XSSI prefix
        ("for(;;);{\"a\": 1}", {"a": 1}),
        ('callbackFn({"a": 1});', {"a": 1}),  # JSONP
        ("jQuery1234_5678([1, 2])", [1, 2]),
        ('"just a string"', None),  # scalar JSON rejected
        ("not json at all", None),
        ("", None),
    ],
)
def test_parse_sniffed_json(text, expect):
    assert parse_sniffed_json(text) == expect


# ── schema hash / ledger ─────────────────────────────────────────────────────


def test_schema_hash_stable_across_values():
    a = {"inventory": [{"vin": _vin(1), "price": 1}], "total": 10}
    b = {"inventory": [{"vin": _vin(2), "price": 2}], "total": 99}
    assert schema_hash(a) == schema_hash(b)


def test_schema_hash_differs_across_shapes():
    a = {"inventory": [{"vin": _vin(1)}]}
    b = {"specials": [{"offer": "x"}]}
    assert schema_hash(a) != schema_hash(b)


def test_ledger_fingerprint_dedup_and_json():
    led = ObserverLedger(dealer_id="d1", dealer_name="Dealer", inv_path="/cars")
    from backend.scanner.network_observer import PayloadFingerprint

    fp = PayloadFingerprint(
        host="x.io", path="/feed", method="GET", content_type="application/json",
        schema_hash="abc", top_keys=["inventory"], approx_bytes=100, best_list_len=4,
        vin_items=4, score=0.7, reason="url_denied_vehicle_payload", url_allowed=False,
        sniffed=False,
    )
    led.add_fingerprint(fp)
    led.add_fingerprint(fp)
    assert len(led.fingerprints) == 1
    assert next(iter(led.fingerprints.values())).occurrences == 2
    out = led.to_json()
    json.dumps(out)  # serializable
    assert out["fingerprints"][0]["host"] == "x.io"


# ── observer end-to-end with fake responses ──────────────────────────────────


class FakeRequest:
    def __init__(self, method="GET", post_data=None, resource_type="xhr", headers=None):
        self.method = method
        self.post_data = post_data
        self.resource_type = resource_type
        self.headers = headers or {}

    async def all_headers(self):
        return dict(self.headers)


class FakeResponse:
    def __init__(self, url, body=None, text=None, content_type="application/json",
                 request=None, fail_first_read=False):
        self.url = url
        self.headers = {"content-type": content_type}
        self._body = body
        self._text = text if text is not None else (json.dumps(body) if body is not None else "")
        self.request = request or FakeRequest()
        self._fail_next = fail_first_read

    async def json(self):
        if self._fail_next:
            self._fail_next = False
            raise RuntimeError("Response disposed")
        return self._body

    async def text(self):
        if self._fail_next:
            self._fail_next = False
            raise RuntimeError("Response disposed")
        return self._text

    async def finished(self):
        return None


def _make_observer(records=None, found=None):
    records = records if records is not None else []
    found = found if found is not None else {"value": False}
    obs = NetworkObserver(
        dealer_base_url=DEALER, dealer_id="d1", dealer_name="Dealer",
        path="/new-inventory/", records=records, found_data=found,
    )
    return obs, records, found


def test_observer_captures_inventory_json():
    obs, records, found = _make_observer()
    body = {"inventory": [_vehicle(i) for i in range(4)], "pageInfo": {"totalCount": 44}}
    resp = FakeResponse(f"{DEALER}/api/inventory", body=body,
                        request=FakeRequest(method="POST", post_data='{"page":1}'))
    asyncio.run(obs.handle_response(resp))
    assert records == [(f"{DEALER}/api/inventory", body)]
    assert found["value"] is True
    (ep,) = obs.ledger.endpoints.values()
    assert ep.method == "POST"
    assert ep.post_data_sample == '{"page":1}'
    assert ep.total_count == 44


def test_observer_sniffs_mislabeled_json():
    obs, records, found = _make_observer()
    body = {"vehicles": [_vehicle(i) for i in range(3)]}
    resp = FakeResponse(f"{DEALER}/inventory/feed", text=json.dumps(body),
                        content_type="text/html",
                        request=FakeRequest(resource_type="fetch"))
    asyncio.run(obs.handle_response(resp))
    assert len(records) == 1
    assert records[0][1] == body
    assert obs.ledger.sniffed_json == 1


def test_observer_does_not_sniff_documents():
    obs, records, _ = _make_observer()
    body = {"vehicles": [_vehicle(i) for i in range(3)]}
    resp = FakeResponse(f"{DEALER}/inventory/", text=json.dumps(body),
                        content_type="text/html",
                        request=FakeRequest(resource_type="document"))
    asyncio.run(obs.handle_response(resp))
    assert records == []


def test_observer_sniff_disabled(monkeypatch):
    monkeypatch.setenv("SCANNER_SNIFF_NONJSON", "0")
    obs, records, _ = _make_observer()
    body = {"vehicles": [_vehicle(i) for i in range(3)]}
    resp = FakeResponse(f"{DEALER}/inventory/feed", text=json.dumps(body),
                        content_type="text/plain",
                        request=FakeRequest(resource_type="xhr"))
    asyncio.run(obs.handle_response(resp))
    assert records == []


def test_observer_retries_body_read_after_failure():
    obs, records, _ = _make_observer()
    body = {"inventory": [_vehicle(i) for i in range(4)]}
    resp = FakeResponse(f"{DEALER}/api/inventory", body=body, fail_first_read=True)
    asyncio.run(obs.handle_response(resp))
    assert len(records) == 1


def test_observer_counts_denied_and_fingerprints_vehicle_payload():
    obs, records, found = _make_observer()
    body = {"inventory": [_vehicle(i) for i in range(4)]}
    resp = FakeResponse("https://unknown-inventory-cdn.io/feed", body=body)
    asyncio.run(obs.handle_response(resp))
    assert records == []
    assert found["value"] is False
    assert obs.url_denied == 1
    assert obs.ledger.denied_hosts == {"unknown-inventory-cdn.io": 1}
    (fp,) = obs.ledger.fingerprints.values()
    assert fp.reason == "url_denied_vehicle_payload"
    assert fp.vin_items == 4


def test_observer_ignores_unrelated_json():
    obs, records, _ = _make_observer()
    resp = FakeResponse(f"{DEALER}/api/consent", body={"ok": True})
    asyncio.run(obs.handle_response(resp))
    assert records == []
    assert obs.ledger.ignored == 1


def test_observer_handler_never_raises():
    obs, _, _ = _make_observer()

    class Broken:
        url = f"{DEALER}/x"

        @property
        def headers(self):
            raise RuntimeError("boom")

    asyncio.run(obs.handle_response(Broken()))  # must swallow


def test_observer_save_ledger(tmp_path, monkeypatch):
    import backend.scanner.network_observer as no

    monkeypatch.setattr(no, "WORKSPACE_DEBUG_DIR", tmp_path)
    obs, _, _ = _make_observer()
    body = {"inventory": [_vehicle(i) for i in range(4)]}
    asyncio.run(obs.handle_response(FakeResponse(f"{DEALER}/api/inventory", body=body)))
    out = obs.save_ledger()
    assert out is not None
    data = json.loads((tmp_path / out.split("/")[-1]).read_text())
    assert data["dealer_id"] == "d1"
    assert len(data["endpoints"]) == 1


def test_observer_save_ledger_skips_when_empty(tmp_path, monkeypatch):
    import backend.scanner.network_observer as no

    monkeypatch.setattr(no, "WORKSPACE_DEBUG_DIR", tmp_path)
    obs, _, _ = _make_observer()
    assert obs.save_ledger() is None


# ── tracked tasks / drain / capture event ─────────────────────────────────────


class SlowResponse(FakeResponse):
    """FakeResponse whose body read takes *delay* seconds."""

    def __init__(self, *args, delay=0.05, **kwargs):
        super().__init__(*args, **kwargs)
        self._delay = delay

    async def json(self):
        await asyncio.sleep(self._delay)
        return await super().json()


def test_on_response_tracks_task_and_drain_lands_payload():
    obs, records, found = _make_observer()
    body = {"inventory": [_vehicle(i) for i in range(4)]}

    async def run():
        obs.on_response(SlowResponse(f"{DEALER}/api/inventory", body=body, delay=0.05))
        assert records == []  # body read still in flight
        still = await obs.drain(2.0)
        assert still == 0
        assert not obs._pending

    asyncio.run(run())
    assert len(records) == 1
    assert found["value"] is True


def test_drain_timeout_reports_stragglers():
    obs, records, _ = _make_observer()
    body = {"inventory": [_vehicle(i) for i in range(4)]}

    async def run():
        obs.on_response(SlowResponse(f"{DEALER}/api/inventory", body=body, delay=0.5))
        still = await obs.drain(0.05)
        assert still == 1
        await obs.drain(2.0)  # let it finish so the loop closes cleanly

    asyncio.run(run())
    assert len(records) == 1


def test_drain_noop_when_idle():
    obs, _, _ = _make_observer()
    assert asyncio.run(obs.drain(0.1)) == 0


def test_capture_event_set_on_capture_only():
    obs, _, _ = _make_observer()
    assert not obs.capture_event.is_set()
    asyncio.run(obs.handle_response(FakeResponse(f"{DEALER}/api/consent", body={"ok": True})))
    assert not obs.capture_event.is_set()
    body = {"inventory": [_vehicle(i) for i in range(4)]}
    asyncio.run(obs.handle_response(FakeResponse(f"{DEALER}/api/inventory", body=body)))
    assert obs.capture_event.is_set()


class ClosedPageResponse(FakeResponse):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.read_attempts = 0

    async def json(self):
        self.read_attempts += 1
        raise RuntimeError("Target page, context or browser has been closed")


def test_body_read_page_closed_skips_retry():
    obs, records, _ = _make_observer()
    resp = ClosedPageResponse(f"{DEALER}/api/inventory", body={})
    asyncio.run(obs.handle_response(resp))
    assert records == []
    assert resp.read_attempts == 1  # no 0.4s sleep-and-retry on a dead page
    assert obs.ledger.body_read_failures == 1


# ── auth header capture (endpoint replay recipes) ─────────────────────────────


def test_extract_auth_headers_filters_and_caps():
    from backend.scanner.network_observer import extract_auth_headers

    headers = {
        "X-Typesense-Api-Key": "ts_key_123",
        "x-algolia-api-key": "alg_key",
        "x-algolia-application-id": "APP1",
        "Authorization": "Bearer abc",
        "Cookie": "session=SECRET",
        "content-type": "application/json",
        "accept": "*/*",
        "x-csrf-token": "tok" * 200,
    }
    out = extract_auth_headers(headers)
    assert out["x-typesense-api-key"] == "ts_key_123"
    assert out["x-algolia-api-key"] == "alg_key"
    assert out["x-algolia-application-id"] == "APP1"
    assert out["authorization"] == "Bearer abc"
    assert "cookie" not in out
    assert "content-type" not in out
    assert len(out["x-csrf-token"]) == 300  # value cap


def test_captured_endpoint_records_auth_headers():
    obs, records, _ = _make_observer()
    body = {"inventory": [_vehicle(i) for i in range(4)]}
    req = FakeRequest(method="POST", post_data='{"searches":[]}',
                      headers={"x-typesense-api-key": "ts_key_123", "cookie": "no"})
    resp = FakeResponse(f"{DEALER}/api/inventory", body=body, request=req)
    asyncio.run(obs.handle_response(resp))
    assert len(records) == 1
    (ep,) = obs.ledger.endpoints.values()
    assert ep.auth_headers == {"x-typesense-api-key": "ts_key_123"}
    # dedup keeps the first non-empty auth_headers
    asyncio.run(obs.handle_response(FakeResponse(f"{DEALER}/api/inventory", body=body,
                                                 request=FakeRequest(method="POST"))))
    (ep,) = obs.ledger.endpoints.values()
    assert ep.occurrences == 2
    assert ep.auth_headers == {"x-typesense-api-key": "ts_key_123"}
