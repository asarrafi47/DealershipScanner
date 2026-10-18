"""The ``css_url`` Jinja filter (P3.5, security-7).

Image URLs land inside ``style="background-image:url('...')"``. Autoescape turns
``'`` into ``&#39;``, but the HTML parser decodes that back to a quote before
the CSS parser reads the attribute, so a dealer image URL containing ``');``
injected CSS. ``css_url`` admits only http(s) URLs that pass
``normalize_listing_image_url`` and same-origin image paths, percent-encodes the
characters that can end the CSS string, and falls back to the placeholder.
"""

from __future__ import annotations

import re
from html.parser import HTMLParser
from pathlib import Path

import pytest
from flask import Flask
from jinja2 import Undefined

from backend.web.templating import CSS_URL_PLACEHOLDER, _jinja_css_url, register_templating

REPO_ROOT = Path(__file__).resolve().parents[2]
TEMPLATES = REPO_ROOT / "frontend" / "templates"

# Characters that must never reach CSS raw from a stored URL.
_BREAKOUT_CHARS = (
    set("'\"()\\<>") | {chr(c) for c in range(0x00, 0x21)} | {"\x7f", "\x85", "\xa0", "\u2028", "\u2029", "\u3000"}
)

CDN_URLS = [
    "https://pictures.dealer.com/m/maseratiofchattanooga/1234/abcd1234ef5678.jpg?impolicy=downsize&w=568",
    "https://images.dealerinspire.com/photos/1C4RJFBG5MC123456/1.jpg",
    "https://cdn.spincar.com/swipetospin-viewers/dealer/1HGCM82633A004352/20231011/closeup/1.jpg?ts=1697040000",
    "https://img.sm360.ca/ir/w640h480c/images/newcar/ca/2024/toyota/rav4/le/suv/2024_toyota_rav4_le_001.jpg",
    "https://vehicle-photos-published.vauto.com/ab/cd/ef/12-34-56/image-1.jpg",
    "https://res.cloudinary.com/demo/image/upload/c_fill,w_640/v1/cars/x.jpg",
    "https://cdn.example.com/photos/already%20encoded%27name.jpg",
    "https://inv.assets.example.com/2025/Lexus/RX/350h~Premium/photo+1.webp?a=1&b=2#frag",
    "https://cdn.example.com/photos/caf\u00e9-front.jpg",
]

LOCAL_PATHS = [
    "/static/placeholder.svg",
    "/static/placeholder.svg?v=1729200000",
    "/car-images/rick_hendrick_bmw/1HGCM82633A004352/000_9f2c1a.jpg",
    "/dealer-uploads/3/17/Zq8fK2mPq1sV0aLx9cRt.webp",
]

INJECTION_PAYLOADS = [
    "https://cdn.example.com/a.jpg');background:url(https://evil.example/x);/*",
    'https://cdn.example.com/a.jpg");background:red;/*',
    "https://cdn.example.com/a.jpg'><script>alert(1)</script>",
    "https://cdn.example.com/a.jpg\\27);color:red;/*",
    "https://cdn.example.com/a.jpg\n);color:red;/*",
    "https://cdn.example.com/a b\tc\rd\fe\x00f\x7fg\x85h\xa0i\u2028j.jpg",
    "/car-images/d/v/1.jpg');background:url(https://evil.example/x);/*",
    "/static/x.svg');color:red;/*",
    '/dealer-uploads/1/2/a.jpg");color:red;/*',
]

REJECTED = [
    None,
    "",
    "   ",
    "\u2014",
    42,
    Undefined(),
    "javascript:alert(1)",
    "JaVaScRiPt:alert(1)",
    "data:image/png;base64,AAAA",
    "vbscript:msgbox(1)",
    "ftp://cdn.example.com/a.jpg",
    "cdn.example.com/a.jpg",
    "//evil.example/x.jpg",
    "/\\evil.example/x.jpg",
    "/api/admin/operator/incomplete-cars/1",
    "/logout",
    "/car-images/../logout",
    "/static/%2e%2e/admin",
    "/dealer-uploads/1/2/.%2E/x.jpg",
    "https://192.168.1.10/a.jpg",
    "https://10.0.0.5/a.jpg",
    "https://user:pw@cdn.example.com/a.jpg",
    "https://cdn.example.com/\ud800.jpg",
]


@pytest.fixture
def dev_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLASK_ENV", "development")


class _StyleCollector(HTMLParser):
    """The style attribute values a browser's HTML parser would hand to CSS."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.styles: list[str] = []

    def handle_starttag(self, tag, attrs):
        for name, val in attrs:
            if name == "style":
                self.styles.append(val or "")


def _styles(html: str) -> list[str]:
    p = _StyleCollector()
    p.feed(html)
    p.close()
    return p.styles


# One quoted url() token and nothing after it: the CSS a safe value must produce.
_SAFE_CSS = re.compile(r"""^background-image:url\('([^'"()\\\s<>]*)'\)$""")


def _bare_app() -> Flask:
    app = Flask(__name__)
    register_templating(app)
    return app


# --------------------------------------------------------------------------- filter


@pytest.mark.parametrize("url", CDN_URLS)
def test_cdn_urls_pass_through_byte_identical(dev_env, url) -> None:
    assert _jinja_css_url(url) == url


@pytest.mark.parametrize("url", CDN_URLS)
def test_cdn_urls_pass_through_byte_identical_in_production(monkeypatch, url) -> None:
    monkeypatch.setenv("FLASK_ENV", "production")
    assert _jinja_css_url(url) == url


@pytest.mark.parametrize("path", LOCAL_PATHS)
def test_same_origin_image_paths_pass_through(dev_env, path) -> None:
    assert _jinja_css_url(path) == path


@pytest.mark.parametrize("payload", INJECTION_PAYLOADS)
def test_injection_payloads_are_neutralized(dev_env, payload) -> None:
    out = _jinja_css_url(payload)
    assert out != CSS_URL_PLACEHOLDER, "a fixable URL should be encoded, not dropped"
    leaked = sorted(ch for ch in set(out) if ch in _BREAKOUT_CHARS)
    assert not leaked, f"{payload!r} -> {out!r} leaks {leaked!r}"
    # Same URL, only encoded: decoding gives the original back.
    from urllib.parse import unquote

    assert unquote(out) == payload.strip()


def test_injection_payload_exact_encoding(dev_env) -> None:
    out = _jinja_css_url("https://cdn.example.com/a.jpg');background:url(https://evil.example/x);/*")
    assert out == "https://cdn.example.com/a.jpg%27%29;background:url%28https://evil.example/x%29;/*"
    assert _jinja_css_url('/car-images/a b\\c"<>.jpg') == "/car-images/a%20b%5Cc%22%3C%3E.jpg"
    assert _jinja_css_url("https://cdn.example.com/a\x85b\u2028c.jpg") == (
        "https://cdn.example.com/a%C2%85b%E2%80%A8c.jpg"
    )


@pytest.mark.parametrize("value", REJECTED)
def test_rejected_values_fall_back_to_placeholder(dev_env, value) -> None:
    assert _jinja_css_url(value) == CSS_URL_PLACEHOLDER


def test_plain_http_is_dropped_in_production_only(monkeypatch) -> None:
    url = "http://cdn.example.com/a.jpg"
    monkeypatch.setenv("FLASK_ENV", "development")
    assert _jinja_css_url(url) == url
    monkeypatch.setenv("FLASK_ENV", "production")
    assert _jinja_css_url(url) == CSS_URL_PLACEHOLDER


def test_localhost_image_is_dropped_in_production(monkeypatch) -> None:
    monkeypatch.setenv("FLASK_ENV", "production")
    assert _jinja_css_url("https://127.0.0.1/a.jpg") == CSS_URL_PLACEHOLDER


def test_surrounding_whitespace_is_trimmed(dev_env) -> None:
    assert _jinja_css_url("  https://cdn.example.com/a.jpg\n") == "https://cdn.example.com/a.jpg"


def test_returns_plain_str_so_autoescape_still_runs(dev_env) -> None:
    from markupsafe import Markup

    out = _jinja_css_url(Markup("https://cdn.example.com/a.jpg?x=1&y=2"))
    assert type(out) is str


# --------------------------------------------------------------------------- rendered HTML


def test_filter_is_registered_on_the_app() -> None:
    from backend.main import app

    assert app.jinja_env.filters.get("css_url") is _jinja_css_url


_STYLE_TPL = """<div style="background-image:url('{{ u | css_url }}')"></div>"""
_UNFILTERED_TPL = """<div style="background-image:url('{{ u }}')"></div>"""


def test_unfiltered_template_is_injectable(dev_env) -> None:
    """Documents the bug the filter closes: autoescape alone does not stop it."""
    env = _bare_app().jinja_env
    html = env.from_string(_UNFILTERED_TPL).render(u="https://cdn.example.com/a.jpg');color:red;/*")
    (style,) = _styles(html)
    assert "&#39;" in html
    assert style.startswith("background-image:url('https://cdn.example.com/a.jpg');color:red")


@pytest.mark.parametrize(
    "payload",
    INJECTION_PAYLOADS
    + [
        # A literal character reference must reach CSS as text, never as a quote.
        "https://cdn.example.com/a.jpg#&#39;&#41;;color:red;/*",
        "https://cdn.example.com/a.jpg?q=&apos;&rpar;;color:red",
    ],
)
def test_rendered_style_attribute_stays_one_url_token(dev_env, payload) -> None:
    env = _bare_app().jinja_env
    html = env.from_string(_STYLE_TPL).render(u=payload)
    (style,) = _styles(html)
    assert _SAFE_CSS.match(style), f"{payload!r} rendered as CSS {style!r}"


@pytest.mark.parametrize("url", CDN_URLS)
def test_rendered_cdn_url_reaches_css_unchanged(dev_env, url) -> None:
    env = _bare_app().jinja_env
    html = env.from_string(_STYLE_TPL).render(u=url)
    (style,) = _styles(html)
    assert style == f"background-image:url('{url}')"


def test_rendered_placeholder_for_empty_value(dev_env) -> None:
    env = _bare_app().jinja_env
    (style,) = _styles(env.from_string(_STYLE_TPL).render(u=None))
    assert style == f"background-image:url('{CSS_URL_PLACEHOLDER}')"


# --------------------------------------------------------------------------- template scan

_JINJA_EXPR = re.compile(r"\{\{.*?\}\}", re.S)
_JINJA_STMT = re.compile(r"\{%.*?%\}|\{#.*?#\}", re.S)
_STYLE_ATTR = re.compile(r"""\bstyle\s*=\s*(?:"([^"]*)"|'([^']*)')""", re.I)
_URL_TOKEN = re.compile(r"url\s*\(", re.I)
_CSS_URL_EXPR = re.compile(r"\|\s*css_url\s*-?\}\}$")


def _style_url_sites(text: str) -> list[tuple[int, str, list[str]]]:
    """(line, attribute, its Jinja expressions) for every style attribute with url(.

    Jinja expressions are swapped for indexed tokens first so a quote inside
    ``{{ ... }}`` cannot end the attribute match early."""
    exprs: list[str] = []

    def _tok(m: re.Match) -> str:
        exprs.append(m.group(0))
        # Keep the expression's newlines so line numbers stay right.
        return f"\x00{len(exprs) - 1}\x00" + "\n" * m.group(0).count("\n")

    masked = _JINJA_EXPR.sub(_tok, text)
    masked = _JINJA_STMT.sub(lambda m: "\n" * m.group(0).count("\n"), masked)
    sites = []
    for m in _STYLE_ATTR.finditer(masked):
        val = m.group(1) if m.group(1) is not None else m.group(2)
        if not _URL_TOKEN.search(val):
            continue
        line = masked.count("\n", 0, m.start()) + 1
        used = [exprs[int(i)] for i in re.findall(r"\x00(\d+)\x00", val)]
        sites.append((line, val, used))
    return sites


def test_every_style_url_goes_through_css_url() -> None:
    offenders = []
    total = 0
    for path in sorted(TEMPLATES.rglob("*.html")):
        text = path.read_text(encoding="utf-8")
        for line, val, used in _style_url_sites(text):
            total += 1
            rel = f"{path.relative_to(REPO_ROOT)}:{line}"
            if not used:
                offenders.append(f"{rel} (url() without css_url)")
                continue
            for expr in used:
                if not _CSS_URL_EXPR.search(expr):
                    offenders.append(f"{rel} ({expr.strip()} lacks a final | css_url)")
    # home x4, landing, compare, inventory/dashboard, inventory/listings x3.
    assert total >= 10, f"scan found only {total} style url() sites; the regex is broken"
    assert not offenders, "style url() must use the css_url filter: " + ", ".join(offenders)


def test_scan_catches_an_unfiltered_style_url() -> None:
    bad = (
        '<div class="x" style="background-image:url(\'{{ car.gallery[0] if car.gallery '
        'else (car.image_url or "/static/placeholder.svg") }}\')"></div>\n'
        "<span style='background: url(\"{{ u | e }}\")'></span>\n"
        "<i style=\"background-image:url('{{ u | css_url }}')\"></i>\n"
    )
    sites = _style_url_sites(bad)
    assert [line for line, _v, _u in sites] == [1, 2, 3]
    assert not _CSS_URL_EXPR.search(sites[0][2][0])
    assert not _CSS_URL_EXPR.search(sites[1][2][0])
    assert _CSS_URL_EXPR.search(sites[2][2][0])


def test_templates_using_css_url_keep_autoescape_on() -> None:
    # The filter returns a plain str and relies on autoescape for "&".
    for path in sorted(TEMPLATES.rglob("*.html")):
        text = path.read_text(encoding="utf-8")
        if "css_url" in text:
            assert not re.search(r"\{%-?\s*autoescape\s+false", text), path


# --------------------------------------------------------------------------- JS helpers
#
# compare.js (compare tray) and dev.js (incomplete cards) build the same
# style="background-image:url('...')" through innerHTML. new URL().href keeps a
# literal "&#39;" or "&#41;", and the HTML parser decodes it back into a quote or
# paren before CSS reads the style, so the helpers must escape "&" as well as
# percent-encode what can end the CSS string. P16.8 later moves both to CSSOM.

STATIC = REPO_ROOT / "frontend" / "static"
_JS_ORIGIN = "https://sarraficars.example"

_ENTITY_PAYLOADS = [
    "https://cdn.example.com/a.jpg#&#39;);position:fixed;inset:0;background:url(https://evil.example/x)",
    "https://cdn.example.com/a.jpg#&#39;&#41;;color:red;/*",
    "https://cdn.example.com/a.jpg?q=&apos;&rpar;;color:red",
    "https://cdn.example.com/a.jpg#&#x27;&#x29;;color:red",
    "https://cdn.example.com/a.jpg?x=1&quot;&gt;&lt;b&gt;",
    "https://cdn.example.com/a.jpg?q=\\');color:red",
    "https://cdn.example.com/a.jpg?q=x\\",
]
_JS_HTTP_PAYLOADS = [p for p in INJECTION_PAYLOADS if p.startswith("https://")] + _ENTITY_PAYLOADS


def _js_function(path: Path, name: str) -> str:
    src = path.read_text(encoding="utf-8")
    m = re.search(rf"function {name}\([^)]*\) \{{.*?\n    \}}", src, re.S)
    assert m, f"{name} not found in {path.name}"
    return m.group(0)


def _run_node(prelude: str, call: str, inputs: list[str]) -> list[str]:
    import json
    import shutil
    import subprocess

    script = (
        prelude
        + f"\nconst __in = {json.dumps(inputs)};"
        + f"\nprocess.stdout.write(JSON.stringify(__in.map((u) => {call})));"
    )
    proc = subprocess.run([shutil.which("node"), "-e", script], capture_output=True, text=True, timeout=30, check=True)
    return json.loads(proc.stdout)


def _compare_js(inputs: list[str]) -> list[str]:
    prelude = f"const window = {{ location: {{ origin: {_JS_ORIGIN!r} }} }};\n" + _js_function(
        STATIC / "compare.js", "safeCssBackgroundUrl"
    )
    return _run_node(prelude, "safeCssBackgroundUrl(u)", inputs)


def _dev_js(inputs: list[str]) -> list[str]:
    path = STATIC / "dev.js"
    prelude = _js_function(path, "isHttpUrl") + "\n" + _js_function(path, "cssSingleQuotedUrl")
    return _run_node(prelude, "cssSingleQuotedUrl(u)", inputs)


def _node_href(inputs: list[str]) -> list[str]:
    return _run_node("", f"new URL(u, {_JS_ORIGIN!r}).href", inputs)


def _compare_tray_style(safe_img: str) -> str:
    # compare.js syncTray: ' style="background-image:url(\'' + safeImg + "')"
    (style,) = _styles(
        '<span class="compare-tray__slot-thumb"' + " style=\"background-image:url('" + safe_img + "')\"" + "></span>"
    )
    return style


def _dev_card_style(img_quoted: str) -> str:
    # dev.js incomplete card: style="background-image:url('${imgQuoted}')"
    (style,) = _styles(f'<div class="result-image" style="background-image:url(\'{img_quoted}\')"></div>')
    return style


_needs_node = pytest.mark.skipif(__import__("shutil").which("node") is None, reason="node not installed")


@_needs_node
def test_old_compare_tray_helper_was_injectable() -> None:
    """Documents the bug: replacing ' alone misses a literal &#39; in the URL."""
    old = _run_node(
        f"const window = {{ location: {{ origin: {_JS_ORIGIN!r} }} }};",
        "new URL(u, window.location.origin).href.replace(/'/g, '%27')",
        [_ENTITY_PAYLOADS[0]],
    )
    style = _compare_tray_style(old[0])
    assert not _SAFE_CSS.match(style)
    assert "');position:fixed" in style


@_needs_node
def test_compare_tray_style_stays_one_url_token() -> None:
    payloads = _JS_HTTP_PAYLOADS + [p for p in INJECTION_PAYLOADS if p.startswith("/")]
    for payload, out in zip(payloads, _compare_js(payloads), strict=True):
        style = _compare_tray_style(out)
        assert _SAFE_CSS.match(style), f"{payload!r} rendered as CSS {style!r}"


@_needs_node
def test_dev_incomplete_card_style_stays_one_url_token() -> None:
    for payload, out in zip(_JS_HTTP_PAYLOADS, _dev_js(_JS_HTTP_PAYLOADS), strict=True):
        style = _dev_card_style(out)
        assert _SAFE_CSS.match(style), f"{payload!r} rendered as CSS {style!r}"


@_needs_node
def test_js_helpers_pass_cdn_urls_through() -> None:
    hrefs = _node_href(CDN_URLS)
    compare_out = _compare_js(CDN_URLS)
    dev_out = _dev_js(CDN_URLS)
    for url, href, c_out, d_out in zip(CDN_URLS, hrefs, compare_out, dev_out, strict=True):
        if url.isascii():
            assert href == url
        # After HTML parsing, CSS sees the browser's own href, unchanged.
        assert _compare_tray_style(c_out) == f"background-image:url('{href}')"
        assert _dev_card_style(d_out) == f"background-image:url('{href}')"


@_needs_node
def test_js_helpers_reject_non_http() -> None:
    bad = ["javascript:alert(1)", "data:image/png;base64,AAAA", "vbscript:msgbox(1)", ""]
    assert _compare_js(bad) == ["", "", "", ""]
    assert _dev_js(bad) == [CSS_URL_PLACEHOLDER] * 4
