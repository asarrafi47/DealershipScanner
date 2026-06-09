"""Tests for web research search fallbacks."""

from __future__ import annotations

from backend.utils.web_researcher import (
    _is_brave_bot_page,
    direct_trim_guide_urls,
    href_is_acceptable_result,
)


def test_duckduckgo_html_parses_uddg_links():
    html = """
    <a href="//duckduckgo.com/l/?uddg=https%3A%2F%2Fwww.motorpoint.co.uk%2Fguides%2Fbmw&amp;rut=abc">BMW</a>
    <a href="//duckduckgo.com/l/?uddg=https%3A%2F%2Fwww.edmunds.com%2Fbmw%2F&amp;rut=def">Edmunds</a>
    """
    # Patch fetch by calling parser logic inline
    import re
    from urllib.parse import parse_qs, unquote, urlparse

    found = []
    for m in re.finditer(r'href="(//duckduckgo\.com/l/\?[^"]+)"', html):
        href = "https:" + m.group(1)
        qs = parse_qs(urlparse(href).query)
        target = unquote(qs.get("uddg", [""])[0]).strip()
        if target and href_is_acceptable_result(target, allowed_hosts=None):
            found.append(target)
    assert "https://www.motorpoint.co.uk/guides/bmw" in found
    assert "https://www.edmunds.com/bmw/" in found


def test_brave_bot_page_detection():
    assert _is_brave_bot_page("Verifying you're not a bot\nQuick check before you continue")
    assert not _is_brave_bot_page("2015 BMW 1 Series trim levels explained")


def test_direct_trim_guide_urls():
    urls = direct_trim_guide_urls(2015, "BMW", "1 Series")
    assert any("motortrend.com" in u for u in urls)
    assert any("caranddriver.com" in u for u in urls)


def test_direct_guide_fetch():
    from backend.utils.web_researcher import WebResearcher

    r = WebResearcher(max_text_chars=8000)
    out = r.search_and_summarize(
        "2010 acura mdx trim comparison",
        year=2010,
        make="Acura",
        model="MDX",
    )
    assert out is not None
    assert len(out.text) >= 80
