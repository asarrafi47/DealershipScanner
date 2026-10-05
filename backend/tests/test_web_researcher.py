"""Tests for web research search fallbacks.

Offline: the DuckDuckGo results page and guide pages are canned, and public
hosts resolve through the conftest ``fake_dns`` table (the SSRF guard does a
DNS lookup per candidate URL).
"""

from __future__ import annotations

import io
import urllib.request

import pytest

from backend.utils import web_researcher as wr
from backend.utils.web_researcher import (
    _is_brave_bot_page,
    direct_trim_guide_urls,
    href_is_acceptable_result,
)

_DDG_HTML = """
<a href="//duckduckgo.com/l/?uddg=https%3A%2F%2Fwww.motorpoint.co.uk%2Fguides%2Fbmw&amp;rut=abc">BMW</a>
<a href="//duckduckgo.com/l/?uddg=https%3A%2F%2Fwww.edmunds.com%2Fbmw%2F&amp;rut=def">Edmunds</a>
<a href="//duckduckgo.com/l/?uddg=http%3A%2F%2F127.0.0.1%2Fadmin&amp;rut=ghi">local</a>
"""


class _Resp(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def test_duckduckgo_html_parses_uddg_links(monkeypatch: pytest.MonkeyPatch, fake_dns) -> None:
    requested: list[str] = []

    def fake_urlopen(req, timeout=20):
        requested.append(req.full_url)
        return _Resp(_DDG_HTML.encode())

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    found = wr.duckduckgo_html_result_links("bmw 1 series trims", allowed_hosts=None)
    assert requested and "duckduckgo.com" in requested[0]
    assert found == ["https://www.motorpoint.co.uk/guides/bmw", "https://www.edmunds.com/bmw/"]
    assert href_is_acceptable_result("https://www.motorpoint.co.uk/guides/bmw", allowed_hosts=None)


def test_brave_bot_page_detection():
    assert _is_brave_bot_page("Verifying you're not a bot\nQuick check before you continue")
    assert not _is_brave_bot_page("2015 BMW 1 Series trim levels explained")


def test_direct_trim_guide_urls():
    urls = direct_trim_guide_urls(2015, "BMW", "1 Series")
    assert any("motortrend.com" in u for u in urls)
    assert any("caranddriver.com" in u for u in urls)


def test_direct_guide_fetch(monkeypatch: pytest.MonkeyPatch, fake_dns) -> None:
    """With year/make/model, the direct guide URLs are fetched first over plain HTTP."""
    guide_text = " ".join(["The 2010 Acura MDX comes in base, Technology and Advance trims."] * 4)
    fetched: list[str] = []

    def fake_fetch(url: str, *, timeout_sec: int = 20):
        fetched.append(url)
        return "2010 Acura MDX trims", guide_text

    def no_browser(*_a, **_k):
        raise AssertionError("browser fallback must not run when the HTTP guide fetch succeeds")

    monkeypatch.setattr(wr, "fetch_page_text_http", fake_fetch)
    monkeypatch.setattr(wr, "duckduckgo_html_result_links", lambda *a, **k: [])
    monkeypatch.setattr(wr.WebResearcher, "_brave_search_links", no_browser)
    monkeypatch.setattr(wr.WebResearcher, "_fetch_with_playwright", no_browser)

    r = wr.WebResearcher(max_text_chars=8000)
    out = r.search_and_summarize(
        "2010 acura mdx trim comparison",
        year=2010,
        make="Acura",
        model="MDX",
    )
    assert out is not None
    assert len(out.text) >= 80
    assert fetched == [direct_trim_guide_urls(2010, "Acura", "MDX")[0]]
    assert out.url == fetched[0]
