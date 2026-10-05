"""Characterization of every scanner HTTP fetcher (audit F-18).

Pins, with ``curl_cffi`` / ``requests`` / urllib stubbed, exactly what each
fetcher puts on the wire (library, function, method, URL, headers, impersonate
profile, proxies, timeout, redirects) and what it returns for a 200, a 403, a
challenge page, a timeout and an arbitrary exception, plus the pacing and retry
sleeps around it.

Written BEFORE ``backend/scanner/net/client.py`` became the shared HTTP layer and
kept unchanged after, so the move is proven behavior-preserving. Several pinned
behaviors look like bugs (replay ignores ``SCANNER_HTTP_PROXY``, VDP prefetch
returns a challenge page as HTML, the chain's requests fetcher passes no proxy);
they are pinned on purpose -- change them deliberately, with this file.

Fetchers covered:
  * ``chain.ImpersonatingFetcher`` / ``chain.RequestsFetcher``
  * ``synth.http._fetch_impersonated`` / ``fetch_dealer_html`` / ``_dep_fetch_page``
    / ``_cosmos_get_json``
  * ``recipes._replay_request`` / ``_replay_impersonated``
  * ``vdp.prefetch._fetch_html``
  * ``vdp.vdp_recipes._fetch_json``
"""
from __future__ import annotations

import io
import json
import socket
import sys
import time
import types
import urllib.error
from typing import Any

import pytest
import requests

from backend.scanner import chain
from backend.scanner import recipes as rec
from backend.scanner.synth import http as synth_http
from backend.scanner.vdp import prefetch as pf
from backend.scanner.vdp import vdp_recipes as vr

PROXY = "http://proxy.test:3128"
URL = "https://dealer.test/inventory/"
BIG = "<html><body>" + ("real inventory " * 300) + "</body></html>"  # > 2000 bytes
CHALLENGE = "<html><title>Just a moment...</title>" + ("x" * 3000) + "</html>"
THIN = "<html>tiny</html>"
PROFILES = ["chrome", "chrome124", "safari17_0"]


class FakeResp:
    def __init__(self, status: int = 200, text: str = "", ctype: str = "text/html; charset=utf-8"):
        self.status_code = status
        self.text = text
        self.headers = {"content-type": ctype}

    def json(self) -> Any:
        return json.loads(self.text)

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(f"{self.status_code} Error")


class FakeUrlResp:
    def __init__(self, body: str, url: str = URL):
        self._body = body.encode()
        self._url = url
        self.headers = types.SimpleNamespace(get_content_charset=lambda: "utf-8")

    def read(self) -> bytes:
        return self._body

    def geturl(self) -> str:
        return self._url


class CurlTimeout(Exception):
    """Stand-in for curl_cffi.requests.exceptions.Timeout (its own hierarchy)."""


class Wire:
    """Ordered record of everything that went out, and a script of what comes back."""

    def __init__(self) -> None:
        self.events: list[Any] = []
        self.script: dict[str, list[Any]] = {"curl_cffi": [], "requests": [], "urllib": []}

    def queue(self, lib: str, *outcomes: Any) -> None:
        self.script[lib].extend(outcomes)

    def _next(self, lib: str) -> Any:
        assert self.script[lib], f"unexpected {lib} call (nothing scripted)"
        out = self.script[lib].pop(0)
        if isinstance(out, BaseException):
            raise out
        return out

    def call(self, lib: str, fn: str, method: str, url: str, kwargs: dict[str, Any]) -> Any:
        self.events.append({"lib": lib, "fn": fn, "method": method, "url": url, "kwargs": dict(kwargs)})
        return self._next(lib)

    def urllib_open(self, req: Any, timeout: float | None = None) -> Any:
        self.events.append({
            "lib": "urllib", "url": req.full_url, "headers": dict(req.header_items()), "timeout": timeout,
        })
        return self._next("urllib")

    def calls(self, lib: str | None = None) -> list[dict[str, Any]]:
        return [e for e in self.events if isinstance(e, dict) and (lib is None or e["lib"] == lib)]


@pytest.fixture
def wire(monkeypatch):
    w = Wire()
    cffi_pkg = types.ModuleType("curl_cffi")
    cffi_req = types.ModuleType("curl_cffi.requests")
    cffi_req.get = lambda url, **kw: w.call("curl_cffi", "get", "GET", url, kw)
    cffi_req.request = lambda method, url, **kw: w.call("curl_cffi", "request", method, url, kw)
    cffi_pkg.requests = cffi_req
    monkeypatch.setitem(sys.modules, "curl_cffi", cffi_pkg)
    monkeypatch.setitem(sys.modules, "curl_cffi.requests", cffi_req)
    monkeypatch.setattr(requests, "get", lambda url, **kw: w.call("requests", "get", "GET", url, kw))
    monkeypatch.setattr(requests, "request", lambda method, url, **kw: w.call("requests", "request", method, url, kw))
    monkeypatch.setattr(synth_http, "open_url", w.urllib_open)
    monkeypatch.setattr(synth_http, "_pace", lambda: w.events.append("pace"))
    monkeypatch.setattr(time, "sleep", lambda s: w.events.append(("sleep", s)))
    for name in ("SCANNER_HTTP_PROXY", "HTTPS_PROXY", "HTTP_PROXY"):
        monkeypatch.setenv(name, "")
    pf._LAST_STATUS.clear()
    pf._LAST_ERROR.clear()
    yield w
    pf._LAST_STATUS.clear()
    pf._LAST_ERROR.clear()


@pytest.fixture
def no_cffi(monkeypatch, wire):
    monkeypatch.setitem(sys.modules, "curl_cffi", None)
    monkeypatch.setitem(sys.modules, "curl_cffi.requests", None)
    return wire


@pytest.fixture
def proxied(monkeypatch, wire):
    monkeypatch.setenv("SCANNER_HTTP_PROXY", PROXY)
    return wire


def _cffi_get(url: str, profile: str, **kw: Any) -> dict[str, Any]:
    return {"lib": "curl_cffi", "fn": "get", "method": "GET", "url": url, "kwargs": {"impersonate": profile, **kw}}


# ════════════════════════════════════════════════════════════════════════════
# chain.ImpersonatingFetcher
# ════════════════════════════════════════════════════════════════════════════

def _chain_call(profile: str, proxies: Any = None) -> dict[str, Any]:
    return _cffi_get(URL, profile, timeout=25.0, proxies=proxies, allow_redirects=True)


def test_chain_impersonating_200(wire):
    wire.queue("curl_cffi", FakeResp(200, BIG))
    assert chain.ImpersonatingFetcher().fetch(URL) == BIG
    assert wire.events == [_chain_call("chrome")]


def test_chain_impersonating_403_rotates_all_profiles_then_raises(wire):
    wire.queue("curl_cffi", FakeResp(403), FakeResp(403), FakeResp(403))
    with pytest.raises(chain.FetchError, match=r"impersonation exhausted for dealer.test \(last status 403\)"):
        chain.ImpersonatingFetcher().fetch(URL)
    assert wire.events == [_chain_call(p) for p in PROFILES]


def test_chain_impersonating_returns_challenge_page_unchecked(wire):
    wire.queue("curl_cffi", FakeResp(200, CHALLENGE))
    assert chain.ImpersonatingFetcher().fetch(URL) == CHALLENGE


def test_chain_impersonating_timeouts_and_errors(wire):
    wire.queue("curl_cffi", CurlTimeout("timed out"), RuntimeError("boom"), CurlTimeout("timed out"))
    with pytest.raises(chain.FetchError, match=r"\(last status None\)"):
        chain.ImpersonatingFetcher().fetch(URL)
    assert wire.events == [_chain_call(p) for p in PROFILES]


def test_chain_impersonating_blank_200_counts_as_failure(wire):
    wire.queue("curl_cffi", FakeResp(200, "   "), FakeResp(200, ""), FakeResp(200, " \n"))
    with pytest.raises(chain.FetchError, match=r"\(last status 200\)"):
        chain.ImpersonatingFetcher().fetch(URL)


def test_chain_impersonating_remembers_winning_profile_per_host(wire):
    f = chain.ImpersonatingFetcher(timeout_s=7.0)
    wire.queue("curl_cffi", FakeResp(403), FakeResp(200, BIG), FakeResp(200, BIG))
    assert f.fetch(URL) == BIG
    assert f.fetch(URL + "?page=2") == BIG
    profiles = [e["kwargs"]["impersonate"] for e in wire.calls()]
    assert profiles == ["chrome", "chrome124", "chrome124"]
    assert {e["kwargs"]["timeout"] for e in wire.calls()} == {7.0}


def test_chain_impersonating_uses_scanner_proxy(proxied):
    proxied.queue("curl_cffi", FakeResp(200, BIG))
    chain.ImpersonatingFetcher().fetch(URL)
    assert proxied.events == [_chain_call("chrome", {"http": PROXY, "https": PROXY})]


def test_chain_impersonating_falls_back_to_https_proxy_env(monkeypatch, wire):
    monkeypatch.setenv("HTTPS_PROXY", "http://std.proxy:1")
    wire.queue("curl_cffi", FakeResp(200, BIG))
    chain.ImpersonatingFetcher().fetch(URL)
    assert wire.calls()[0]["kwargs"]["proxies"] == {"http": "http://std.proxy:1", "https": "http://std.proxy:1"}


def test_chain_impersonating_unavailable_without_curl_cffi(no_cffi):
    assert chain.ImpersonatingFetcher().available() is False


# ════════════════════════════════════════════════════════════════════════════
# chain.RequestsFetcher
# ════════════════════════════════════════════════════════════════════════════

def _requests_chain_call() -> dict[str, Any]:
    return {"lib": "requests", "fn": "get", "method": "GET", "url": URL,
            "kwargs": {"timeout": 20.0, "headers": {"User-Agent": chain._UA}, "allow_redirects": True}}


def test_chain_requests_200(wire):
    wire.queue("requests", FakeResp(200, BIG))
    assert chain.RequestsFetcher().fetch(URL) == BIG
    assert wire.events == [_requests_chain_call()]


def test_chain_requests_403_raises_http_error(wire):
    wire.queue("requests", FakeResp(403, "denied"))
    with pytest.raises(requests.exceptions.HTTPError):
        chain.RequestsFetcher().fetch(URL)


def test_chain_requests_challenge_returned(wire):
    wire.queue("requests", FakeResp(200, CHALLENGE))
    assert chain.RequestsFetcher().fetch(URL) == CHALLENGE


def test_chain_requests_timeout_propagates(wire):
    wire.queue("requests", requests.exceptions.Timeout("slow"))
    with pytest.raises(requests.exceptions.Timeout):
        chain.RequestsFetcher().fetch(URL)


def test_chain_requests_passes_no_proxy_even_with_scanner_proxy(proxied):
    proxied.queue("requests", FakeResp(200, BIG))
    chain.RequestsFetcher().fetch(URL)
    assert proxied.events == [_requests_chain_call()]


# ════════════════════════════════════════════════════════════════════════════
# synth.http._fetch_impersonated
# ════════════════════════════════════════════════════════════════════════════

def _synth_call(profile: str, *, url: str = URL, proxies: Any = None, headers: Any = None,
                timeout: float = 25.0) -> dict[str, Any]:
    return _cffi_get(url, profile, timeout=timeout, proxies=proxies, allow_redirects=True, headers=headers)


def test_synth_impersonated_200(wire):
    wire.queue("curl_cffi", FakeResp(200, BIG))
    assert synth_http._fetch_impersonated(URL) == BIG
    assert wire.events == ["pace", _synth_call("chrome")]


def test_synth_impersonated_403_rotates_with_pacing(wire):
    wire.queue("curl_cffi", FakeResp(403), FakeResp(403), FakeResp(403))
    assert synth_http._fetch_impersonated(URL) is None
    assert wire.events == [x for p in PROFILES for x in ("pace", _synth_call(p))]


def test_synth_impersonated_skips_challenge_and_thin(wire):
    wire.queue("curl_cffi", FakeResp(200, CHALLENGE), FakeResp(200, THIN), FakeResp(200, BIG))
    assert synth_http._fetch_impersonated(URL) == BIG
    assert len(wire.calls()) == 3


def test_synth_impersonated_min_bytes_zero_accepts_thin(wire):
    wire.queue("curl_cffi", FakeResp(200, THIN))
    assert synth_http._fetch_impersonated(URL, min_bytes=0) == THIN


def test_synth_impersonated_errors_return_none(wire):
    wire.queue("curl_cffi", CurlTimeout("t"), RuntimeError("x"), FakeResp(500, BIG))
    assert synth_http._fetch_impersonated(URL) is None
    assert len(wire.calls()) == 3


def test_synth_impersonated_headers_timeout_proxy(proxied):
    proxied.queue("curl_cffi", FakeResp(200, BIG))
    hdrs = {"Referer": "https://dealer.test/"}
    assert synth_http._fetch_impersonated(URL, timeout=9.0, headers=hdrs) == BIG
    assert proxied.events == ["pace", _synth_call("chrome", proxies={"http": PROXY, "https": PROXY},
                                                  headers=hdrs, timeout=9.0)]


def test_synth_impersonated_empty_headers_sent_as_none(wire):
    wire.queue("curl_cffi", FakeResp(200, BIG))
    synth_http._fetch_impersonated(URL, headers={})
    assert wire.calls()[0]["kwargs"]["headers"] is None


def test_synth_impersonated_without_curl_cffi(no_cffi):
    assert synth_http._fetch_impersonated(URL) is None
    assert no_cffi.events == []


# ════════════════════════════════════════════════════════════════════════════
# synth.http.fetch_dealer_html (urllib first, then impersonation)
# ════════════════════════════════════════════════════════════════════════════

def _urllib_call(url: str = URL, timeout: float = 25.0, headers: dict | None = None) -> dict[str, Any]:
    if headers is None:
        headers = {k.capitalize(): v for k, v in synth_http._browser_headers().items()}
    return {"lib": "urllib", "url": url, "headers": headers, "timeout": timeout}


def _http_error(code: int) -> urllib.error.HTTPError:
    return urllib.error.HTTPError(URL, code, "err", {}, io.BytesIO(b""))


def test_fetch_dealer_html_200(wire):
    wire.queue("urllib", FakeUrlResp(BIG))
    assert synth_http.fetch_dealer_html(URL) == BIG
    assert wire.events == ["pace", _urllib_call()]


def test_fetch_dealer_html_403_escalates_to_impersonation(wire):
    wire.queue("urllib", _http_error(403))
    wire.queue("curl_cffi", FakeResp(200, BIG))
    assert synth_http.fetch_dealer_html(URL, timeout=11.0) == BIG
    assert wire.events == ["pace", _urllib_call(timeout=11.0), "pace", _synth_call("chrome", timeout=11.0)]


def test_fetch_dealer_html_retries_transient_with_sleeps(wire):
    wire.queue("urllib", _http_error(503), _http_error(429), FakeUrlResp(BIG))
    assert synth_http.fetch_dealer_html(URL) == BIG
    assert wire.events == ["pace", _urllib_call(), ("sleep", 2.0), "pace", _urllib_call(),
                           ("sleep", 4.0), "pace", _urllib_call()]


def test_fetch_dealer_html_retries_exhausted_then_impersonates(wire):
    wire.queue("urllib", _http_error(503), _http_error(503), _http_error(503))
    wire.queue("curl_cffi", FakeResp(403), FakeResp(403), FakeResp(403))
    assert synth_http.fetch_dealer_html(URL) is None
    assert [e for e in wire.events if isinstance(e, tuple)] == [("sleep", 2.0), ("sleep", 4.0)]
    assert len(wire.calls("curl_cffi")) == 3


def test_fetch_dealer_html_timeout_gives_up_without_impersonation(wire):
    wire.queue("urllib", socket.timeout("timed out"))
    assert synth_http.fetch_dealer_html(URL) is None
    assert wire.calls("curl_cffi") == []


def test_fetch_dealer_html_url_error_gives_up(wire):
    wire.queue("urllib", urllib.error.URLError("refused"))
    assert synth_http.fetch_dealer_html(URL) is None
    assert wire.calls("curl_cffi") == []


def test_fetch_dealer_html_thin_body_escalates(wire):
    wire.queue("urllib", FakeUrlResp(THIN))
    wire.queue("curl_cffi", FakeResp(200, BIG))
    assert synth_http.fetch_dealer_html(URL) == BIG


def test_fetch_dealer_html_challenge_escalates(wire):
    wire.queue("urllib", FakeUrlResp(CHALLENGE))
    wire.queue("curl_cffi", FakeResp(200, CHALLENGE), FakeResp(200, CHALLENGE), FakeResp(200, CHALLENGE))
    assert synth_http.fetch_dealer_html(URL) is None
    assert len(wire.calls("curl_cffi")) == 3


def test_fetch_dealer_html_rejects_non_http(wire):
    assert synth_http.fetch_dealer_html("ftp://dealer.test/") is None
    assert synth_http.fetch_dealer_html("") is None
    assert wire.events == []


def test_dep_fetch_page_403_escalates_with_referer_and_min_bytes_zero(wire):
    wire.queue("urllib", _http_error(403))
    wire.queue("curl_cffi", FakeResp(200, THIN))
    html, final = synth_http._dep_fetch_page(URL)
    assert (html, final) == (THIN, URL)
    hdrs = {**synth_http._browser_headers(), "Referer": "https://dealer.test/", "Sec-Fetch-Site": "same-origin"}
    assert wire.events == ["pace", _urllib_call(headers={k.capitalize(): v for k, v in hdrs.items()}),
                           "pace", _synth_call("chrome", headers=hdrs)]


def test_dep_fetch_page_404_no_escalation(wire):
    wire.queue("urllib", _http_error(404))
    assert synth_http._dep_fetch_page(URL) == (None, URL)
    assert wire.calls("curl_cffi") == []


def test_cosmos_get_json_falls_back_to_impersonation(wire):
    wire.queue("urllib", _http_error(403))
    wire.queue("curl_cffi", FakeResp(200, json.dumps({"a": 1}) + " " * 3000))
    assert synth_http._cosmos_get_json(URL) == {"a": 1}
    assert wire.calls("urllib")[0]["headers"]["Accept"] == "application/json"


# ════════════════════════════════════════════════════════════════════════════
# recipes._replay_request / _replay_impersonated
# ════════════════════════════════════════════════════════════════════════════

BASE = "https://dealer.test"
API = "https://api.dealer.test/inventory"


def _recipe(method: str = "GET", pagination: str = rec.PAGINATION_NONE, auth: dict | None = None) -> rec.EndpointRecipe:
    return rec.EndpointRecipe(
        dealer_id="d1", url=API, method=method, content_type="application/json",
        post_template=None, auth_headers=auth or {}, pagination=pagination,
    )


def _replay_headers(method: str = "GET", auth: dict | None = None) -> dict[str, str]:
    h = {
        "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
        "Accept": "application/json, text/plain, */*",
        "Origin": BASE,
        "Referer": BASE + "/",
        **(auth or {}),
    }
    if method != "GET":
        h["Content-Type"] = "application/json"
    return h


JSON_BODY = json.dumps({"inventory": [{"vin": "1"}]})


def test_replay_get_200_json(wire):
    wire.queue("requests", FakeResp(200, JSON_BODY, "application/json"))
    assert rec._replay_request(_recipe(auth={"X-Key": "k"}), None, BASE + "/") == (200, {"inventory": [{"vin": "1"}]})
    assert wire.events == [{"lib": "requests", "fn": "get", "method": "GET", "url": API,
                            "kwargs": {"headers": _replay_headers(auth={"X-Key": "k"}), "timeout": 20.0}}]


def test_replay_403_clears_via_impersonation_without_proxy(proxied):
    proxied.queue("requests", FakeResp(403))
    proxied.queue("curl_cffi", FakeResp(403), FakeResp(200, JSON_BODY))
    assert rec._replay_request(_recipe(), None, BASE, url=API + "?page=2") == (200, {"inventory": [{"vin": "1"}]})
    assert proxied.calls("curl_cffi") == [
        _cffi_get(API + "?page=2", p, headers=_replay_headers(), timeout=20.0) for p in ("chrome", "chrome124")
    ]


def test_replay_403_impersonation_exhausted_keeps_original_status(wire):
    wire.queue("requests", FakeResp(403))
    wire.queue("curl_cffi", FakeResp(403), FakeResp(503), FakeResp(429))
    assert rec._replay_request(_recipe(), None, BASE) == (403, None)
    assert len(wire.calls("curl_cffi")) == 3


@pytest.mark.parametrize("status", [405, 429])
def test_replay_other_fingerprint_statuses_escalate(wire, status):
    wire.queue("requests", FakeResp(status))
    wire.queue("curl_cffi", FakeResp(200, JSON_BODY))
    assert rec._replay_request(_recipe(), None, BASE)[0] == 200


def test_replay_500_no_escalation(wire):
    wire.queue("requests", FakeResp(500))
    assert rec._replay_request(_recipe(), None, BASE) == (500, None)
    assert wire.calls("curl_cffi") == []


def test_replay_timeout_escalates_and_all_fail(wire):
    wire.queue("requests", requests.exceptions.Timeout("slow"))
    wire.queue("curl_cffi", CurlTimeout("t"), RuntimeError("x"), CurlTimeout("t"))
    assert rec._replay_request(_recipe(), None, BASE) == (0, None)
    assert len(wire.calls("curl_cffi")) == 3


def test_replay_non_requests_exception_propagates(wire):
    wire.queue("requests", RuntimeError("not a RequestException"))
    with pytest.raises(RuntimeError):
        rec._replay_request(_recipe(), None, BASE)


def test_replay_post_body_and_content_type(wire):
    wire.queue("requests", FakeResp(403))
    wire.queue("curl_cffi", FakeResp(200, JSON_BODY))
    body = {"page": 1}
    assert rec._replay_request(_recipe("POST"), body, BASE)[0] == 200
    assert wire.events == [
        {"lib": "requests", "fn": "request", "method": "POST", "url": API,
         "kwargs": {"headers": _replay_headers("POST"), "data": json.dumps(body), "timeout": 20.0}},
        {"lib": "curl_cffi", "fn": "request", "method": "POST", "url": API,
         "kwargs": {"headers": _replay_headers("POST"), "data": json.dumps(body), "impersonate": "chrome",
                    "timeout": 20.0}},
    ]


def test_replay_html_recipe_returns_text_even_for_challenge(wire):
    wire.queue("requests", FakeResp(200, CHALLENGE))
    assert rec._replay_request(_recipe(pagination=rec.PAGINATION_DEP_SRP), None, BASE) == (200, CHALLENGE)


def test_replay_html_recipe_impersonated_returns_text(wire):
    wire.queue("requests", FakeResp(403))
    wire.queue("curl_cffi", FakeResp(200, BIG))
    r = _recipe(pagination=rec.PAGINATION_NONE)
    r.provider_hint = "html_cards"
    assert rec._replay_request(r, None, BASE) == (200, BIG)


def test_replay_non_json_200(wire):
    wire.queue("requests", FakeResp(200, "<html>not json</html>"))
    assert rec._replay_request(_recipe(), None, BASE) == (200, None)


def test_replay_impersonated_non_json_200(wire):
    wire.queue("requests", FakeResp(403))
    wire.queue("curl_cffi", FakeResp(200, "<html>not json</html>"))
    assert rec._replay_request(_recipe(), None, BASE) == (200, None)


def test_replay_json_scalar_is_none(wire):
    wire.queue("requests", FakeResp(200, "42"))
    assert rec._replay_request(_recipe(), None, BASE) == (200, None)


def test_replay_without_curl_cffi(no_cffi):
    no_cffi.queue("requests", FakeResp(403))
    assert rec._replay_request(_recipe(), None, BASE) == (403, None)
    assert rec._replay_impersonated(_recipe(), API, {}, None) == (0, None)


# ════════════════════════════════════════════════════════════════════════════
# vdp.prefetch._fetch_html
# ════════════════════════════════════════════════════════════════════════════

VDP = "https://dealer.test/used/2020-honda-civic-123.htm"


def _prefetch_headers() -> dict[str, str]:
    return {"User-Agent": pf._UA, "Accept": "text/html,application/xhtml+xml", "Referer": "https://dealer.test/",
            "Sec-Fetch-Site": "same-origin", "Sec-Fetch-Mode": "navigate", "Sec-Fetch-Dest": "document"}


def _pf_cffi(proxies: Any = None) -> dict[str, Any]:
    return _cffi_get(VDP, "chrome", headers=_prefetch_headers(), timeout=15.0, proxies=proxies)


def _pf_requests(proxies: Any = None) -> dict[str, Any]:
    return {"lib": "requests", "fn": "get", "method": "GET", "url": VDP,
            "kwargs": {"headers": _prefetch_headers(), "timeout": 15.0, "proxies": proxies}}


def test_prefetch_200_html(wire):
    wire.queue("curl_cffi", FakeResp(200, BIG))
    assert pf._fetch_html(VDP) == BIG
    assert wire.events == [_pf_cffi()]
    assert pf._LAST_STATUS[VDP] == 200


def test_prefetch_returns_challenge_page_as_html(wire):
    wire.queue("curl_cffi", FakeResp(200, CHALLENGE))
    assert pf._fetch_html(VDP) == CHALLENGE


@pytest.mark.parametrize("status", [403, 429, 503])
def test_prefetch_fingerprint_block_no_fallback(wire, status):
    wire.queue("curl_cffi", FakeResp(status))
    assert pf._fetch_html(VDP) is None
    assert wire.events == [_pf_cffi()]
    assert pf._LAST_STATUS[VDP] == status


def test_prefetch_404_falls_back_to_requests(wire):
    wire.queue("curl_cffi", FakeResp(404))
    wire.queue("requests", FakeResp(200, BIG))
    assert pf._fetch_html(VDP) == BIG
    assert wire.events == [_pf_cffi(), _pf_requests()]


def test_prefetch_non_html_200_falls_back(wire):
    wire.queue("curl_cffi", FakeResp(200, "{}", "application/json"))
    wire.queue("requests", FakeResp(200, "{}", "application/json"))
    assert pf._fetch_html(VDP) is None
    assert len(wire.events) == 2


def test_prefetch_cffi_timeout_falls_back_and_notes_error(wire):
    wire.queue("curl_cffi", CurlTimeout("timed out"))
    wire.queue("requests", FakeResp(200, BIG))
    assert pf._fetch_html(VDP) == BIG
    assert pf._LAST_ERROR[VDP] == "CurlTimeout: timed out"
    assert pf._LAST_STATUS[VDP] == 200


def test_prefetch_both_fail(wire):
    wire.queue("curl_cffi", RuntimeError("boom"))
    wire.queue("requests", requests.exceptions.Timeout("slow"))
    assert pf._fetch_html(VDP) is None
    assert pf._LAST_STATUS[VDP] == 0
    assert pf._LAST_ERROR[VDP] == "Timeout: slow"


def test_prefetch_requests_403(wire):
    wire.queue("curl_cffi", FakeResp(404))
    wire.queue("requests", FakeResp(403, BIG))
    assert pf._fetch_html(VDP) is None
    assert pf._LAST_STATUS[VDP] == 403


def test_prefetch_without_curl_cffi(no_cffi):
    no_cffi.queue("requests", FakeResp(200, BIG))
    assert pf._fetch_html(VDP) == BIG
    assert no_cffi.events == [_pf_requests()]


def test_prefetch_proxy_on_both_paths(proxied):
    proxied.queue("curl_cffi", FakeResp(404))
    proxied.queue("requests", FakeResp(200, BIG))
    pf._fetch_html(VDP)
    p = {"http": PROXY, "https": PROXY}
    assert proxied.events == [_pf_cffi(p), _pf_requests(p)]


# ════════════════════════════════════════════════════════════════════════════
# vdp.vdp_recipes._fetch_json
# ════════════════════════════════════════════════════════════════════════════

def _vrecipe(method: str = "GET") -> vr.VdpRecipe:
    return vr.VdpRecipe(dealer_id="d1", url_template=API + "/{vin}", method=method, auth_headers={"X-A": "1"})


def _vr_headers(method: str = "GET") -> dict[str, str]:
    h = {"User-Agent": vr._UA, "Accept": "application/json, text/plain, */*", "Origin": BASE,
         "Referer": BASE + "/", "X-A": "1"}
    if method != "GET":
        h["Content-Type"] = "application/json"
    return h


def _vr_call(lib: str, method: str = "GET", payload: Any = None, proxies: Any = None) -> dict[str, Any]:
    kw: dict[str, Any] = {"headers": _vr_headers(method), "data": payload}
    if lib == "curl_cffi":
        kw["impersonate"] = "chrome"
    kw.update({"timeout": 15.0, "proxies": proxies})
    return {"lib": lib, "fn": "request", "method": method, "url": API + "/V1", "kwargs": kw}


def test_vdp_json_200(wire):
    wire.queue("curl_cffi", FakeResp(200, JSON_BODY, "application/json"))
    assert vr._fetch_json(_vrecipe(), API + "/V1", None, BASE + "/") == (200, {"inventory": [{"vin": "1"}]})
    assert wire.events == [_vr_call("curl_cffi")]


def test_vdp_json_403_no_fallback(wire):
    wire.queue("curl_cffi", FakeResp(403, JSON_BODY))
    assert vr._fetch_json(_vrecipe(), API + "/V1", None, BASE) == (403, None)
    assert len(wire.events) == 1


def test_vdp_json_challenge_page(wire):
    wire.queue("curl_cffi", FakeResp(200, CHALLENGE))
    assert vr._fetch_json(_vrecipe(), API + "/V1", None, BASE) == (200, None)


def test_vdp_json_empty_200(wire):
    wire.queue("curl_cffi", FakeResp(200, ""))
    assert vr._fetch_json(_vrecipe(), API + "/V1", None, BASE) == (200, None)


@pytest.mark.parametrize("exc", [CurlTimeout("t"), RuntimeError("x")])
def test_vdp_json_cffi_error_no_fallback(wire, exc):
    wire.queue("curl_cffi", exc)
    assert vr._fetch_json(_vrecipe(), API + "/V1", None, BASE) == (0, None)
    assert wire.calls("requests") == []


def test_vdp_json_post(proxied):
    proxied.queue("curl_cffi", FakeResp(200, JSON_BODY))
    assert vr._fetch_json(_vrecipe("POST"), API + "/V1", '{"vin":"V1"}', BASE)[0] == 200
    assert proxied.events == [_vr_call("curl_cffi", "POST", '{"vin":"V1"}', {"http": PROXY, "https": PROXY})]


def test_vdp_json_without_curl_cffi(no_cffi):
    no_cffi.queue("requests", FakeResp(200, JSON_BODY), requests.exceptions.Timeout("slow"))
    assert vr._fetch_json(_vrecipe(), API + "/V1", None, BASE) == (200, {"inventory": [{"vin": "1"}]})
    assert vr._fetch_json(_vrecipe(), API + "/V1", None, BASE) == (0, None)
    assert no_cffi.events == [_vr_call("requests"), _vr_call("requests")]
