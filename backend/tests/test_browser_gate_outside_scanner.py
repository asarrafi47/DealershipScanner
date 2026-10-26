"""P16.5: every headless-browser launch site outside backend/scanner is gated.

The scanner's own sites are covered by test_browser_gate_20260926.py. This pins the
operator scripts and the legacy scraping CLI: each must ask ``require_browser`` before
launching, so a stray run without SCANNER_ALLOW_BROWSER=1 fails loudly.
"""
from __future__ import annotations

import asyncio
import re
from pathlib import Path

import pytest

from backend.scanner import browser_gate as bg

_BACKEND = Path(__file__).resolve().parents[1]

# Launch sites that are gated some other way (documented, not forgotten).
_OWN_GATE = {
    # WebResearcher._require_browser: refuses unless built with allow_browser=True;
    # the only allow_browser=True caller is an operator script.
    "utils/web_researcher.py",
}
_LAUNCH = re.compile(r"\.chromium\.launch\(|\.firefox\.launch\(|\.webkit\.launch\(")


def _launch_files():
    for p in sorted(_BACKEND.rglob("*.py")):
        rel = p.relative_to(_BACKEND).as_posix()
        if rel.startswith(("scanner/", "tests/")) or "/." in rel:
            continue
        if _LAUNCH.search(p.read_text(encoding="utf-8", errors="ignore")):
            yield rel


def test_inventory_every_launch_site_is_gated():
    found = list(_launch_files())
    assert found, "inventory found no launch sites; the pattern is broken"
    ungated = []
    for rel in found:
        if rel in _OWN_GATE:
            continue
        src = (_BACKEND / rel).read_text(encoding="utf-8")
        for m in _LAUNCH.finditer(src):
            before = src[: m.start()]
            # the nearest gate must precede each launch within the same function body
            last_def = max(before.rfind("\ndef "), before.rfind("\nasync def "), before.rfind("    def "), before.rfind("    async def "))
            if "require_browser(" not in before[last_def:]:
                ungated.append(f"{rel}@{src[:m.start()].count(chr(10)) + 1}")
    assert not ungated, f"launch sites without require_browser: {ungated}"


def test_expected_sites_in_inventory():
    found = set(_launch_files())
    assert {
        "scripts/image_downloader.py",
        "scripts/car_data_scraper.py",
        "scripts/backfill_dealership_addresses.py",
        "scraping/cli.py",
    } <= found


def test_require_browser_refuses_without_flag(monkeypatch):
    monkeypatch.setenv("SCANNER_ALLOW_BROWSER", "")
    monkeypatch.setenv("SCANNER_HTTP_ONLY", "0")  # does not allow a browser by itself
    with pytest.raises(bg.BrowserForbidden, match="my_caller"):
        bg.require_browser("my_caller")
    monkeypatch.setenv("SCANNER_ALLOW_BROWSER", "1")
    bg.require_browser("my_caller")


def test_backfill_context_refuses_before_importing_playwright(monkeypatch):
    monkeypatch.setenv("SCANNER_ALLOW_BROWSER", "")
    from backend.scripts import backfill_dealership_addresses as m

    cls = m.BrowserFetcher
    obj = cls.__new__(cls)
    with pytest.raises(bg.BrowserForbidden, match="backfill_dealership_addresses"):
        asyncio.run(obj.__aenter__())
