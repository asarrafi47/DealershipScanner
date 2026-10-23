"""Tests for web research search fallbacks.

Offline: the DuckDuckGo results page and guide pages are canned, and public
hosts resolve through the conftest ``fake_dns`` table (the SSRF guard does a
DNS lookup per candidate URL).

P3.8 (D-SEC1 (a)): ``WebResearcher()`` is HTTP-only. With the playwright imports
patched to raise, it touches neither Brave nor the Chromium page fallback;
``allow_browser=True`` reaches the (mocked) fallback; Dockerfile.web installs no
browser.
"""

from __future__ import annotations

import ast
import builtins
import inspect
import io
import os
import re
import urllib.error
import urllib.request
from pathlib import Path

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


# --- P3.8 (D-SEC1 (a)): HTTP-only by default, browser mode opt-in ----------

_REPO_ROOT = Path(__file__).resolve().parents[2]
_GOOD_TEXT = " ".join(["The 2021 BMW 330i has a strong reliability record for its class and segment."] * 4)


@pytest.fixture
def playwright_imports(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Make every ``playwright`` import raise, and record each attempt."""
    attempts: list[str] = []
    real_import = builtins.__import__

    def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
        if level == 0 and (name == "playwright" or name.startswith("playwright.")):
            attempts.append(name)
            raise ImportError(f"P3.8 test: playwright import blocked ({name})")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    return attempts


def _spy_browser_steps(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Record calls to the three browser steps, then run the real methods."""
    calls: list[str] = []
    for name in ("_brave_search_links", "_fetch_with_playwright", "_make_context"):
        original = getattr(wr.WebResearcher, name)

        def spy(self, *a, _name=name, _orig=original, **k):
            calls.append(_name)
            return _orig(self, *a, **k)

        monkeypatch.setattr(wr.WebResearcher, name, spy)
    return calls


def _http_403(url: str, *, timeout_sec: int = 20):
    raise urllib.error.HTTPError(url, 403, "Forbidden", hdrs=None, fp=None)


def _http_too_short(url: str, *, timeout_sec: int = 20):
    return "Access Denied", "Access Denied"


def test_allow_browser_defaults_to_false() -> None:
    param = inspect.signature(wr.WebResearcher).parameters["allow_browser"]
    assert param.default is False
    assert param.kind is inspect.Parameter.KEYWORD_ONLY
    assert wr.WebResearcher().allow_browser is False
    assert wr.WebResearcher(allow_browser=True).allow_browser is True


@pytest.mark.parametrize("http_fetch", [_http_403, _http_too_short], ids=["http_403", "too_short"])
def test_default_researcher_touches_neither_brave_nor_fallback(
    monkeypatch: pytest.MonkeyPatch, playwright_imports: list[str], http_fetch
) -> None:
    """One DuckDuckGo candidate (< 2 triggered Brave) whose HTTP fetch fails or is too
    short (that triggered the Chromium page fallback). HTTP-only reaches neither."""
    calls = _spy_browser_steps(monkeypatch)
    fetched: list[str] = []

    def fetch(url: str, *, timeout_sec: int = 20):
        fetched.append(url)
        return http_fetch(url, timeout_sec=timeout_sec)

    monkeypatch.setattr(wr, "duckduckgo_html_result_links", lambda *a, **k: ["https://www.edmunds.com/bmw/3-series/"])
    monkeypatch.setattr(wr, "fetch_page_text_http", fetch)

    out = wr.WebResearcher(timeout_ms=25_000, max_text_chars=2_000).search_and_summarize(
        "2021 bmw 3 series reliability"
    )
    assert out is None
    assert fetched == ["https://www.edmunds.com/bmw/3-series/"]
    assert calls == []
    assert playwright_imports == []


def test_default_researcher_answers_from_http(monkeypatch: pytest.MonkeyPatch, playwright_imports: list[str]) -> None:
    calls = _spy_browser_steps(monkeypatch)
    monkeypatch.setattr(
        wr,
        "duckduckgo_html_result_links",
        lambda *a, **k: ["https://www.kbb.com/bmw/3-series/", "https://www.jdpower.com/bmw-330i"],
    )

    def fetch(url: str, *, timeout_sec: int = 20):
        if "kbb.com" in url:
            return _http_403(url)
        return "2021 BMW 330i review", _GOOD_TEXT

    monkeypatch.setattr(wr, "fetch_page_text_http", fetch)
    out = wr.WebResearcher().search_and_summarize("2021 bmw 3 series reliability")
    assert out is not None
    assert out.url == "https://www.jdpower.com/bmw-330i"
    assert "reliability record" in out.text
    assert calls == []
    assert playwright_imports == []


def test_default_researcher_refuses_direct_browser_calls(playwright_imports: list[str]) -> None:
    r = wr.WebResearcher()
    with pytest.raises(wr.BrowserModeDisabled):
        r._brave_search_links("2021 bmw 3 series reliability")
    with pytest.raises(wr.BrowserModeDisabled):
        r._fetch_with_playwright("https://www.edmunds.com/bmw/3-series/")
    with pytest.raises(wr.BrowserModeDisabled):
        r._make_context(object())
    assert playwright_imports == []


def test_allow_browser_reaches_mocked_brave_and_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    brave_calls: list[str] = []
    fallback_calls: list[str] = []
    brave_url = "https://www.caredge.com/bmw/3-series/2021/reliability"

    def fake_brave(self, query: str, *, max_results: int = 5):
        brave_calls.append(query)
        return [brave_url]

    def fake_fallback(self, url: str):
        fallback_calls.append(url)
        if url == brave_url:
            return "2021 BMW 3 Series reliability", _GOOD_TEXT
        return "Access Denied", ""

    monkeypatch.setattr(wr, "duckduckgo_html_result_links", lambda *a, **k: ["https://www.edmunds.com/bmw/3-series/"])
    monkeypatch.setattr(wr, "fetch_page_text_http", _http_403)
    monkeypatch.setattr(wr.WebResearcher, "_brave_search_links", fake_brave)
    monkeypatch.setattr(wr.WebResearcher, "_fetch_with_playwright", fake_fallback)

    out = wr.WebResearcher(allow_browser=True).search_and_summarize("2021 bmw 3 series reliability")
    assert brave_calls == ["2021 bmw 3 series reliability"]
    assert fallback_calls == ["https://www.edmunds.com/bmw/3-series/", brave_url]
    assert out is not None and out.url == brave_url


def test_allow_browser_passes_the_gate_into_playwright(playwright_imports: list[str]) -> None:
    """Browser mode gets past the gate: the next thing it does is import Playwright."""
    r = wr.WebResearcher(allow_browser=True)
    assert r._brave_search_links("2021 bmw 3 series reliability") == []  # ImportError -> []
    with pytest.raises(ImportError):
        r._fetch_with_playwright("https://www.edmunds.com/bmw/3-series/")
    assert playwright_imports == ["playwright.sync_api", "playwright.sync_api"]


def _webresearcher_calls(path: Path) -> list[ast.Call]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    out: list[ast.Call] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            fn = node.func
            name = fn.id if isinstance(fn, ast.Name) else getattr(fn, "attr", "")
            if name == "WebResearcher":
                out.append(node)
    return out


# Data and cache trees hold no code; in a dev checkout they can hold millions of files.
_PRUNE_DIRS = frozenset({"tests", "data", "artifacts", "debug", "ZIPs", "__pycache__", "vdp_images", "car_images"})


def _backend_code_files() -> list[Path]:
    out: list[Path] = []
    for root, dirs, files in os.walk(_REPO_ROOT / "backend"):
        dirs[:] = sorted(d for d in dirs if d not in _PRUNE_DIRS and not d.startswith("."))
        out.extend(Path(root) / f for f in sorted(files) if f.endswith(".py"))
    return out


def test_only_the_operator_script_turns_on_browser_mode() -> None:
    """Every WebResearcher(...) outside tests keeps the default, except the operator script."""
    turned_on: list[str] = []
    callers: list[str] = []
    for path in _backend_code_files():
        rel = path.relative_to(_REPO_ROOT).as_posix()
        try:
            if "WebResearcher(" not in path.read_text(encoding="utf-8", errors="replace"):
                continue
        except OSError:
            continue
        for call in _webresearcher_calls(path):
            callers.append(rel)
            for kw in call.keywords:
                if kw.arg is None:
                    turned_on.append(f"{rel}:{call.lineno} (**kwargs)")
                elif kw.arg == "allow_browser" and not (isinstance(kw.value, ast.Constant) and kw.value.value is False):
                    turned_on.append(f"{rel}:{call.lineno}")
    assert "backend/intelligence/ai/agent.py" in callers
    assert [t.split(":")[0] for t in turned_on] == ["backend/scripts/analyze_trim_adds_with_web.py"]


def _dockerfile_instructions(path: Path) -> list[str]:
    """Dockerfile instructions with comment lines dropped and continuations joined."""
    out: list[str] = []
    buf = ""
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.endswith("\\"):
            buf += line[:-1] + " "
            continue
        out.append(buf + line)
        buf = ""
    if buf:
        out.append(buf)
    return out


_CHROMIUM_ONLY_LIBS = (
    "libnss3",
    "libatk1.0-0",
    "libatk-bridge2.0-0",
    "libcups2",
    "libdrm2",
    "libxkbcommon0",
    "libxcomposite1",
    "libxdamage1",
    "libxfixes3",
    "libxrandr2",
    "libgbm1",
    "libasound2",
    "libpango-1.0-0",
    "libpangocairo-1.0-0",
    "libgtk-3-0",
    "libx11-xcb1",
    "libxcb-dri3-0",
)


def test_web_image_installs_no_browser() -> None:
    """Static proof for the image check (the Docker build itself needs the owner's OK)."""
    instructions = _dockerfile_instructions(_REPO_ROOT / "Dockerfile.web")
    body = "\n".join(instructions).lower()
    assert instructions and instructions[0].startswith("FROM ")
    for banned in ("playwright install", "patchright install", "install-deps", "chromium", "ms-playwright"):
        assert banned not in body, f"Dockerfile.web still has {banned!r}"
    packages = set(re.split(r"\s+", body))
    assert not packages & set(_CHROMIUM_ONLY_LIBS), sorted(packages & set(_CHROMIUM_ONLY_LIBS))
    # The rest of the image is unchanged: deps, app code, static precompression, entrypoint.
    assert "pip install --no-cache-dir gunicorn[gthread] -r requirements.txt" in body
    assert "python scripts/build_static_compressed.py --quiet" in body
    assert 'entrypoint ["/app/scripts/docker-entrypoint-web.sh"]' in body
