"""HTTP-only scans (docs/HTTP_ONLY_SCANS_PLAN.md, Phase 0): nothing in the scan
path may open a browser unless SCANNER_ALLOW_BROWSER=1 (discovery)."""
from __future__ import annotations

import asyncio

import pytest

from backend.scanner import browser_gate as bg


def test_gate_defaults_to_forbidden(monkeypatch):
    monkeypatch.delenv("SCANNER_ALLOW_BROWSER", raising=False)
    monkeypatch.setenv("SCANNER_HTTP_ONLY", "0")  # legacy switch no longer opens the door
    assert not bg.browser_allowed()
    assert bg.http_only()
    with pytest.raises(bg.BrowserForbidden) as e:
        bg.require_browser("test.site")
    assert "test.site" in str(e.value)
    assert "forbidden" in bg.describe()


def test_gate_opens_only_for_discovery(monkeypatch):
    monkeypatch.setenv("SCANNER_ALLOW_BROWSER", "1")
    assert bg.browser_allowed() and not bg.http_only()
    bg.require_browser("discovery")  # no raise
    assert "allowed" in bg.describe()


def test_dealer_run_http_only_follows_the_gate(monkeypatch):
    from backend.scanner.phases import dealer_run

    monkeypatch.delenv("SCANNER_ALLOW_BROWSER", raising=False)
    assert dealer_run._http_only()
    monkeypatch.setenv("SCANNER_ALLOW_BROWSER", "1")
    assert not dealer_run._http_only()


def test_fetch_chains_have_no_browser_stage_by_default(monkeypatch):
    monkeypatch.delenv("SCANNER_ALLOW_BROWSER", raising=False)
    from backend.scanner import chain
    from backend.scanner.post_scan import gap_fill

    assert [f.name for f in chain.default_fetchers()] == [chain.ImpersonatingFetcher().name, chain.RequestsFetcher().name]
    assert gap_fill._playwright_fetch_html("https://example.com/x") is None
    monkeypatch.setenv("SCANNER_ALLOW_BROWSER", "1")
    assert chain.default_fetchers()[-1].name == chain.PlaywrightFetcher().name


def test_recovery_runs_only_http_safe_strategies_without_a_page(monkeypatch):
    from backend.scanner import inventory_recovery as ir

    monkeypatch.delenv("SCANNER_ALLOW_BROWSER", raising=False)
    assert ir.HTTP_SAFE_STRATEGIES <= set(ir.RECOVERY_STRATEGY_ORDER)
    ctx = ir.RecoveryContext(page=None, base_url="https://x.example", dealer_id="x-example", dealer_name="X", dealer_url="https://x.example",
                             provider="dealer_dot_com", intercept_records=[], path_htmls=[], vehicles=[], parse_fn=lambda raw: [], dealer={})
    assert asyncio.run(ir._html_and_next_data(ctx)) == []


def test_eprocess_without_page_uses_results_api_only(monkeypatch):
    from backend.scanner.scrapers import dealer_eprocess as dep

    calls: list[str] = []

    async def fake_results(base_url, dealer_id, dealer_name, dealer_url):
        calls.append(base_url)
        return [{"vin": "1HGBH41JXMN109186"}]

    monkeypatch.setattr(dep, "_fetch_results_api", fake_results)
    rows = asyncio.run(dep.scrape_dealer_eprocess_from_page(None, "https://d.example", "d-example", "D", "https://d.example"))
    assert rows and calls == ["https://d.example"]


def test_launch_sites_refuse_without_the_gate(monkeypatch):
    monkeypatch.delenv("SCANNER_ALLOW_BROWSER", raising=False)
    from backend.scanner import chain

    with pytest.raises(bg.BrowserForbidden):
        chain.PlaywrightFetcher().fetch("https://example.com/")
