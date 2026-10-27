"""P12A.5 / N18: VDP prefetch on the fetch policy (profile rotation, per-VIN attempt ledger,
gap-first budget, Carfax not required on new cars, candidates_with_gap). Fakes only."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any

import pytest

from backend.db import vdp_attempts as va
from backend.scanner.net import client as nc
from backend.scanner.vdp import prefetch as pf
from backend.scanner.vdp import vdp_recipes as vr

HOST = "dealer.p12a5.test"
URL = f"https://{HOST}/used/car-1.htm"
BIG = "<html><body>" + "x" * 3000 + "</body></html>"
CHALLENGE = "<html><title>Just a moment...</title></html>"


class Resp:
    def __init__(self, status: int, text: str = BIG, ctype: str = "text/html"):
        self.status_code, self.text, self.headers = status, text, {"content-type": ctype}


class FakeCffi:
    """curl_cffi.requests stand-in: a status script per impersonation profile."""

    def __init__(self, by_profile: dict[str, list[Resp]] | None = None, default: Resp | None = None):
        self.by_profile = by_profile or {}
        self.default = default
        self.calls: list[dict[str, Any]] = []

    def _go(self, url: str, kw: dict[str, Any]) -> Resp:
        self.calls.append({"url": url, **kw})
        queue = self.by_profile.get(kw.get("impersonate"))
        if queue:
            return queue.pop(0)
        assert self.default is not None, f"unscripted call {kw.get('impersonate')}"
        return self.default

    def get(self, url, **kw):
        return self._go(url, kw)

    def request(self, method, url, **kw):
        return self._go(url, kw)

    @property
    def profiles(self) -> list[str]:
        return [c["impersonate"] for c in self.calls]


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    for name in ("SCANNER_HTTP_PROXY", "HTTPS_PROXY", "HTTP_PROXY"):
        monkeypatch.setenv(name, "")
    nc.HOST_PROFILES.clear()
    pf._SLOW_HOSTS.discard(HOST)
    pf._LAST_STATUS.clear()
    pf._LAST_ERROR.clear()
    pf._LAST_PROFILE.clear()
    yield
    nc.HOST_PROFILES.clear()
    pf._SLOW_HOSTS.discard(HOST)


def _install(monkeypatch, cffi: FakeCffi) -> None:
    monkeypatch.setattr(nc, "import_curl_cffi", lambda: cffi)


# ── rotation ────────────────────────────────────────────────────────────────

def test_healthy_host_costs_one_request_per_vdp(monkeypatch):
    cffi = FakeCffi(default=Resp(200))
    _install(monkeypatch, cffi)
    assert pf._fetch_html(URL) == BIG
    assert pf._fetch_html(URL.replace("car-1", "car-2")) == BIG
    assert cffi.profiles == ["chrome", "chrome"]
    assert nc.HOST_PROFILES.get(HOST) == "chrome"


def test_first_attempt_rotates_on_403_and_remembers_the_winner(monkeypatch):
    cffi = FakeCffi({"chrome": [Resp(403)], "chrome124": [Resp(200)], "safari17_0": []}, default=Resp(200))
    _install(monkeypatch, cffi)
    assert pf._fetch_html(URL) == BIG
    assert cffi.profiles == ["chrome", "chrome124"]
    assert pf._LAST_PROFILE[URL] == "chrome124"
    # the next VDP starts from the winner: one request
    assert pf._fetch_html(URL.replace("car-1", "car-2")) == BIG
    assert cffi.profiles[2:] == ["chrome124"]


def test_profile_sets_its_own_user_agent_except_chrome(monkeypatch):
    cffi = FakeCffi({"chrome": [Resp(405)]}, default=Resp(200))
    _install(monkeypatch, cffi)
    pf._fetch_html(URL)
    chrome_call, second_call = cffi.calls
    assert chrome_call["headers"]["User-Agent"] == pf._UA
    assert "User-Agent" not in second_call["headers"]
    assert second_call["headers"]["Referer"] == f"https://{HOST}/"


def test_429_and_503_never_rotate(monkeypatch):
    for status in (429, 503):
        nc.HOST_PROFILES.clear()
        cffi = FakeCffi(default=Resp(status))
        _install(monkeypatch, cffi)
        assert pf._fetch_html(URL) is None
        assert cffi.profiles == ["chrome"]
        assert pf._LAST_STATUS[URL] == status


def test_slow_host_gets_exactly_one_request_per_attempt(monkeypatch):
    pf._SLOW_HOSTS.add(HOST)
    cffi = FakeCffi(default=Resp(403))
    _install(monkeypatch, cffi)
    assert pf._fetch_html(URL) is None
    assert pf._fetch_html(URL) is None
    assert cffi.profiles == ["chrome", "chrome"]


def test_host_with_a_winner_does_not_rotate_on_403(monkeypatch):
    nc.HOST_PROFILES.remember(HOST, "safari17_0")
    cffi = FakeCffi(default=Resp(403))
    _install(monkeypatch, cffi)
    assert pf._fetch_html(URL) is None
    assert cffi.profiles == ["safari17_0"]


def test_challenge_page_does_not_rotate_and_is_a_403(monkeypatch):
    cffi = FakeCffi(default=Resp(200, CHALLENGE))
    _install(monkeypatch, cffi)
    assert pf._fetch_html(URL) is None
    assert cffi.profiles == ["chrome"]
    assert pf._LAST_STATUS[URL] == 403
    assert nc.HOST_PROFILES.get(HOST) is None


def test_vdp_json_starts_from_winner_and_rotates_once(monkeypatch):
    recipe = vr.VdpRecipe(dealer_id="d1", url_template="https://api.p12a5.test/{vin}", method="GET")
    api = "https://api.p12a5.test/V1"
    cffi = FakeCffi({"chrome": [Resp(403, "{}", "application/json")]}, default=Resp(200, '{"a": 1}', "application/json"))
    monkeypatch.setattr(nc, "import_curl_cffi", lambda: cffi)
    nc.HOST_PROFILES.clear()
    assert vr._fetch_json(recipe, api, None, "https://api.p12a5.test") == (200, {"a": 1})
    assert cffi.profiles == ["chrome", "chrome124"]
    assert vr._fetch_json(recipe, api, None, "https://api.p12a5.test")[0] == 200
    assert cffi.profiles[2:] == ["chrome124"]
    nc.HOST_PROFILES.clear()


# ── candidate rules ─────────────────────────────────────────────────────────

def _full(**kw):
    base = {"vin": "1HGBH41JXMN109186", "price": 20000, "exterior_color": "Red", "interior_color": "Black",
            "trim": "EX", "description": "d" * 80, "stock_number": "S1", "msrp": 25000,
            "gallery": [f"https://img.test/{i}.jpg" for i in range(15)]}
    base.update(kw)
    return base


def test_carfax_not_required_on_new_cars():
    assert pf._extras_missing(_full(condition="New", carfax_url="")) is False
    assert pf._extras_missing(_full(condition="New", msrp=None)) is True
    assert pf._extras_missing(_full(condition="Used", carfax_url="")) is True
    assert pf._extras_missing(_full(condition="Certified Pre-Owned", carfax_url="")) is True
    assert pf._extras_missing(_full(condition="Used", carfax_url="https://cf/x")) is False
    assert pf._extras_missing(_full(condition="New", stock_number="")) is True


# ── ledger (pure) ───────────────────────────────────────────────────────────

def test_backoff_doubles_after_the_max(monkeypatch):
    monkeypatch.delenv("SCANNER_VDP_ATTEMPT_MAX", raising=False)
    monkeypatch.delenv("SCANNER_VDP_ATTEMPT_BACKOFF_H", raising=False)
    assert [va.backoff_hours(n) for n in (1, 2, 3, 4, 5)] == [0, 0, 72, 144, 288]
    assert va.backoff_hours(30) == 14 * 24


def test_is_deferred_window(monkeypatch):
    monkeypatch.delenv("SCANNER_VDP_ATTEMPT_MAX", raising=False)
    now = datetime(2026, 10, 27, tzinfo=timezone.utc)
    recent = (now - timedelta(hours=10)).isoformat()
    old = (now - timedelta(hours=100)).isoformat()
    assert va.is_deferred(2, recent, now) is False  # under the max: always retried
    assert va.is_deferred(3, recent, now) is True
    assert va.is_deferred(3, old, now) is False  # back-off passed
    assert va.is_deferred(4, old, now) is True  # 144 h now
    assert va.is_deferred(3, "garbage", now) is False


# ── ledger (SQLite harness) and the prefetch pass ───────────────────────────

def test_ledger_roundtrip_and_clears_on_success():
    d = "ledger-p12a5-com"
    v1, v2 = "1HGBH41JXMN100001", "1HGBH41JXMN100002"
    assert va.record(d, [(v1, 403, "blocked"), (v2, 404, "http_404")]) == 2
    assert va.record(d, [(v1, 403, "blocked")]) == 1
    got = va.load(d, [v1, v2, "NOPE"])
    assert got[v1][0] == 2 and got[v2][0] == 1 and "NOPE" not in got
    va.record(d, [], [v1])
    assert list(va.load(d, [v1, v2])) == [v2]
    va.record(d, [], [v2])
    assert va.load(d, [v1, v2]) == {}


def _cars(n, **kw):
    return [{"vin": f"1HGBH41JXMN2{i:05d}", "price": None, "_detail_url": f"https://{HOST}/used/{i}", **kw}
            for i in range(n)]


def _fast(monkeypatch):
    monkeypatch.setattr(pf, "_HTTP_POLITE_DELAY_S", 0)
    monkeypatch.setattr(pf, "_SLOW_HOST_DELAY_S", 0)
    monkeypatch.setattr(pf, "_SLOW_HOST_COOLDOWN_S", 0)
    monkeypatch.setattr(pf, "_http_first_max", lambda *a, **k: 100)


def test_pass_records_failures_and_skips_deferred_cars(monkeypatch):
    _fast(monkeypatch)
    d = "pass-p12a5-com"
    fetched: list[str] = []

    def _fetch(url):
        fetched.append(url)
        pf._LAST_STATUS[url] = 404
        return None

    monkeypatch.setattr(pf, "_fetch_html", _fetch)
    cars = _cars(3)
    for _ in range(3):  # three runs fail every car three times
        asyncio.run(pf.http_prefetch_missing_fields([dict(c) for c in cars], d))
    assert len(fetched) == 9
    stats = asyncio.run(pf.http_prefetch_missing_fields([dict(c) for c in cars], d))
    assert stats["ledger_skipped"] == 3 and stats["candidates"] == 0
    assert len(fetched) == 9  # no fetch at all while the back-off holds
    # a car the ledger has never seen is still fetched
    fresh = _cars(1)
    fresh[0]["vin"] = "1HGBH41JXMN900001"
    asyncio.run(pf.http_prefetch_missing_fields(fresh, d))
    assert len(fetched) == 10


def test_pass_success_clears_the_row(monkeypatch):
    _fast(monkeypatch)
    d = "clear-p12a5-com"
    car = _cars(1)
    va.record(d, [(car[0]["vin"], 403, "blocked")])

    def _ok(url):
        pf._LAST_STATUS[url] = 200
        return BIG

    monkeypatch.setattr(pf, "_fetch_html", _ok)
    asyncio.run(pf.http_prefetch_missing_fields(car, d))
    assert va.load(d, [car[0]["vin"]]) == {}


def test_skipped_host_is_not_counted_as_an_attempt(monkeypatch):
    _fast(monkeypatch)
    d = "skip-p12a5-com"
    monkeypatch.setattr(pf, "_fetch_html", lambda url: None)  # no status: a skipped / silent call
    asyncio.run(pf.http_prefetch_missing_fields(_cars(2), d))
    assert va.load(d, [c["vin"] for c in _cars(2)]) == {}


def test_untried_cars_go_before_retries_inside_a_gap_rank(monkeypatch):
    _fast(monkeypatch)
    monkeypatch.setattr(pf, "_HTTP_CONCURRENCY", 1)
    d = "order-p12a5-com"
    cars = _cars(3)
    va.record(d, [(cars[0]["vin"], 403, "blocked")])
    order: list[str] = []

    def _f(url):
        order.append(url)
        pf._LAST_STATUS[url] = 500
        return None

    monkeypatch.setattr(pf, "_fetch_html", _f)
    asyncio.run(pf.http_prefetch_missing_fields(cars, d))
    assert order[0].endswith("/1") and order[1].endswith("/2")


def test_candidates_with_gap_counts_data_gaps_not_extras(monkeypatch):
    _fast(monkeypatch)
    monkeypatch.setattr(pf, "_fetch_html", lambda url: None)
    gap = {"vin": "1HGBH41JXMN300001", "price": None, "_detail_url": f"https://{HOST}/a"}
    extras = _full(vin="1HGBH41JXMN300002", condition="Used", carfax_url="", _detail_url=f"https://{HOST}/b")
    stats = asyncio.run(pf.http_prefetch_missing_fields([gap, extras]))
    assert stats["candidates"] == 2 and stats["candidates_with_gap"] == 1


def test_405_slows_the_host_like_403(monkeypatch):
    _fast(monkeypatch)
    pf._HOST_COOLDOWN_UNTIL.clear()

    def _f(url):
        pf._LAST_STATUS[url] = 405
        return None

    monkeypatch.setattr(pf, "_fetch_html", _f)
    stats = asyncio.run(pf.http_prefetch_missing_fields(_cars(1)))
    assert stats["statuses"]["405"] >= 1 and stats["slowed_host"] == HOST
    pf._HOST_COOLDOWN_UNTIL.clear()
