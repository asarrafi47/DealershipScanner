"""Offline unit tests for backend.scanner.chain (ScraperChain framework).

Stub fetchers/extractors only — no network, no real browser.
"""
from __future__ import annotations

import pytest

from backend.scanner.chain import (
    ExtractError,
    Extractor,
    Fetcher,
    FetchError,
    HeuristicVehicleLinkExtractor,
    ScrapedVehicle,
    ScraperChain,
    ScrapeResult,
    default_chain,
)


# ---------------------------------------------------------------------------
# Stubs
# ---------------------------------------------------------------------------


class StubFetcher(Fetcher):
    def __init__(self, name, result=None, exc=None, is_available=True):
        self.name = name
        self.result = result
        self.exc = exc
        self.is_available = is_available
        self.calls = 0

    def available(self):
        return self.is_available

    def fetch(self, url):
        self.calls += 1
        if self.exc is not None:
            raise self.exc
        return self.result


class StubExtractor(Extractor):
    def __init__(self, name, result=None, exc=None, is_available=True):
        self.name = name
        self.result = result if result is not None else []
        self.exc = exc
        self.is_available = is_available
        self.calls = 0

    def available(self):
        return self.is_available

    def extract(self, html, source_url):
        self.calls += 1
        if self.exc is not None:
            raise self.exc
        return self.result


def one_vehicle(title="2022 BMW M3"):
    return [ScrapedVehicle(title=title, vin="WBS8M9C55J5K12345")]


# ---------------------------------------------------------------------------
# Fetch stage
# ---------------------------------------------------------------------------


class TestFetchStage:
    def test_first_success_short_circuits(self):
        f1 = StubFetcher("cheap", result="<html>ok</html>")
        f2 = StubFetcher("heavy", result="<html>never</html>")
        chain = ScraperChain([f1, f2], [])
        html, name = chain.fetch("http://x")
        assert html == "<html>ok</html>"
        assert name == "cheap"
        assert f2.calls == 0

    def test_empty_string_falls_through(self):
        f1 = StubFetcher("cheap", result="")
        f2 = StubFetcher("heavy", result="<html>ok</html>")
        chain = ScraperChain([f1, f2], [])
        html, name = chain.fetch("http://x")
        assert name == "heavy"
        assert f1.calls == 1

    def test_whitespace_only_falls_through(self):
        f1 = StubFetcher("cheap", result="   \n\t ")
        f2 = StubFetcher("heavy", result="<html>ok</html>")
        chain = ScraperChain([f1, f2], [])
        _, name = chain.fetch("http://x")
        assert name == "heavy"

    def test_none_result_falls_through(self):
        f1 = StubFetcher("cheap", result=None)
        f2 = StubFetcher("heavy", result="<html>ok</html>")
        chain = ScraperChain([f1, f2], [])
        _, name = chain.fetch("http://x")
        assert name == "heavy"

    def test_exception_falls_through(self):
        f1 = StubFetcher("cheap", exc=RuntimeError("boom"))
        f2 = StubFetcher("heavy", result="<html>ok</html>")
        chain = ScraperChain([f1, f2], [])
        html, name = chain.fetch("http://x")
        assert name == "heavy"
        assert html == "<html>ok</html>"

    def test_unavailable_skipped_and_not_called(self):
        f1 = StubFetcher("gated", result="<html>never</html>", is_available=False)
        f2 = StubFetcher("heavy", result="<html>ok</html>")
        chain = ScraperChain([f1, f2], [])
        _, name = chain.fetch("http://x")
        assert name == "heavy"
        assert f1.calls == 0

    def test_error_aggregation_message(self):
        f1 = StubFetcher("cheap", exc=RuntimeError("boom"))
        f2 = StubFetcher("heavy", result="")
        chain = ScraperChain([f1, f2], [])
        with pytest.raises(FetchError) as ei:
            chain.fetch("http://x")
        assert str(ei.value) == "cheap: boom; heavy: empty"

    def test_unavailable_does_not_pollute_errors(self):
        f1 = StubFetcher("gated", is_available=False)
        f2 = StubFetcher("cheap", result="")
        chain = ScraperChain([f1, f2], [])
        with pytest.raises(FetchError) as ei:
            chain.fetch("http://x")
        assert str(ei.value) == "cheap: empty"
        assert "gated" not in str(ei.value)

    def test_all_gated_off_raises_no_fetchers_available(self):
        f1 = StubFetcher("a", is_available=False)
        f2 = StubFetcher("b", is_available=False)
        chain = ScraperChain([f1, f2], [])
        with pytest.raises(FetchError, match="no fetchers available"):
            chain.fetch("http://x")

    def test_empty_fetcher_list_raises_no_fetchers_available(self):
        chain = ScraperChain([], [])
        with pytest.raises(FetchError, match="no fetchers available"):
            chain.fetch("http://x")


# ---------------------------------------------------------------------------
# Extract stage
# ---------------------------------------------------------------------------


class TestExtractStage:
    def test_first_nonempty_list_wins(self):
        e1 = StubExtractor("api", result=one_vehicle())
        e2 = StubExtractor("llm", result=one_vehicle("never"))
        chain = ScraperChain([], [e1, e2])
        vehicles, name = chain.extract("<html/>", "http://x")
        assert name == "api"
        assert len(vehicles) == 1
        assert e2.calls == 0

    def test_empty_list_means_fall_through(self):
        e1 = StubExtractor("api", result=[])
        e2 = StubExtractor("llm", result=one_vehicle())
        chain = ScraperChain([], [e1, e2])
        vehicles, name = chain.extract("<html/>", "http://x")
        assert name == "llm"
        assert e1.calls == 1

    def test_exception_falls_through(self):
        e1 = StubExtractor("api", exc=ValueError("bad json"))
        e2 = StubExtractor("llm", result=one_vehicle())
        chain = ScraperChain([], [e1, e2])
        _, name = chain.extract("<html/>", "http://x")
        assert name == "llm"

    def test_error_aggregation_uses_vehicles_wording(self):
        e1 = StubExtractor("api", result=[])
        e2 = StubExtractor("llm", exc=ValueError("bad json"))
        chain = ScraperChain([], [e1, e2])
        with pytest.raises(ExtractError) as ei:
            chain.extract("<html/>", "http://x")
        assert str(ei.value) == "api: 0 vehicles; llm: bad json"

    def test_all_gated_off_raises_no_extractors_available(self):
        e1 = StubExtractor("a", is_available=False)
        chain = ScraperChain([], [e1])
        with pytest.raises(ExtractError, match="no extractors available"):
            chain.extract("<html/>", "http://x")
        assert e1.calls == 0


# ---------------------------------------------------------------------------
# run() — end to end, manual bypass, result_filter, provenance
# ---------------------------------------------------------------------------


class TestRun:
    def test_run_fetches_then_extracts_with_provenance(self):
        f = StubFetcher("cheap", result="<html>page</html>")
        e = StubExtractor("api", result=one_vehicle())
        chain = ScraperChain([f], [e])
        result = chain.run("http://dealer.example/inventory")
        assert isinstance(result, ScrapeResult)
        assert result.fetch_strategy == "cheap"
        assert result.extract_strategy == "api"
        assert result.source_url == "http://dealer.example/inventory"
        assert len(result.vehicles) == 1

    def test_manual_html_bypasses_fetch(self):
        f = StubFetcher("cheap", exc=RuntimeError("must not be called"))
        e = StubExtractor("api", result=one_vehicle())
        chain = ScraperChain([f], [e])
        result = chain.run("http://x", html="<html>pasted</html>")
        assert result.fetch_strategy == "manual"
        assert f.calls == 0

    def test_result_filter_applied_to_winner(self):
        e = StubExtractor("api", result=one_vehicle() + one_vehicle("junk"))
        chain = ScraperChain(
            [StubFetcher("f", result="<html/>")],
            [e],
            result_filter=lambda vs: [v for v in vs if v.title != "junk"],
        )
        result = chain.run("http://x")
        assert [v.title for v in result.vehicles] == ["2022 BMW M3"]

    def test_no_result_filter_passes_through(self):
        e = StubExtractor("api", result=one_vehicle())
        chain = ScraperChain([StubFetcher("f", result="<html/>")], [e])
        result = chain.run("http://x")
        assert len(result.vehicles) == 1

    def test_run_propagates_fetch_error(self):
        chain = ScraperChain([StubFetcher("f", result="")], [StubExtractor("e")])
        with pytest.raises(FetchError):
            chain.run("http://x")

    def test_run_propagates_extract_error(self):
        chain = ScraperChain(
            [StubFetcher("f", result="<html/>")], [StubExtractor("e", result=[])]
        )
        with pytest.raises(ExtractError):
            chain.run("http://x")


# ---------------------------------------------------------------------------
# ScrapedVehicle / factory / heuristic extractor
# ---------------------------------------------------------------------------


class TestScrapedVehicle:
    def test_defaults_allow_partial_construction(self):
        v = ScrapedVehicle()
        assert v.title == ""
        assert v.vin is None
        assert v.raw == {}

    def test_normalized_title(self):
        v = ScrapedVehicle(title="  2022   BMW\tM3 ")
        assert v.normalized_title() == "2022 BMW M3"

    def test_raw_dict_not_shared_between_instances(self):
        a, b = ScrapedVehicle(), ScrapedVehicle()
        a.raw["k"] = 1
        assert b.raw == {}


class TestDefaultChain:
    def test_default_chain_orders_cheap_to_heavy(self):
        chain = default_chain(extractors=[StubExtractor("e")])
        assert [f.name for f in chain.fetchers] == ["requests", "playwright"]

    def test_default_chain_passes_filter(self):
        flt = lambda vs: vs  # noqa: E731
        chain = default_chain(extractors=[], result_filter=flt)
        assert chain.result_filter is flt


class TestHeuristicVehicleLinkExtractor:
    HTML = """
    <html><body>
      <a href="/inventory/used-2021-bmw-m4/">2021 BMW M4 Competition</a>
      <a href="/inventory/used-2021-bmw-m4/">2021 BMW M4 Competition</a>
      <a href="/vehicle/WBS8M9C55J5K12345">2018 BMW M3 CS</a>
      <a href="/about-us/">About Us</a>
      <a href="/inventory/no-text/"><img src="x.jpg"/></a>
    </body></html>
    """

    def test_extracts_dedupes_and_resolves_urls(self):
        ex = HeuristicVehicleLinkExtractor()
        if not ex.available():
            pytest.skip("bs4 not installed")
        vehicles = ex.extract(self.HTML, "https://dealer.example/base/")
        titles = {v.title for v in vehicles}
        assert titles == {"2021 BMW M4 Competition", "2018 BMW M3 CS"}
        assert len(vehicles) == 2  # duplicate anchor deduped, no-text skipped
        by_title = {v.title: v for v in vehicles}
        assert (
            by_title["2021 BMW M4 Competition"].url
            == "https://dealer.example/inventory/used-2021-bmw-m4/"
        )
        assert by_title["2018 BMW M3 CS"].vin == "WBS8M9C55J5K12345"

    def test_lowercase_vin_in_href_is_captured_and_uppercased(self):
        """VDP-href matching is case-insensitive, so VIN capture must be too
        (dealer platforms commonly lowercase VINs in VDP URLs)."""
        ex = HeuristicVehicleLinkExtractor()
        if not ex.available():
            pytest.skip("bs4 not installed")
        html = '<a href="/detail/wbs8m9c55j5k12345">2018 BMW M3 CS</a>'
        vehicles = ex.extract(html, "https://dealer.example/")
        assert len(vehicles) == 1
        assert vehicles[0].vin == "WBS8M9C55J5K12345"


# ---------------------------------------------------------------------------
# Integration seam: gap_fill.fetch_listing_html backed by the chain
# ---------------------------------------------------------------------------

GOOD_HTML = "<html>" + ("x" * 9000) + ' application/ld+json vehiclecondition</html>'
THIN_HTML = "<html>tiny spa shell</html>"


class TestFetchListingHtmlIntegration:
    @pytest.fixture(autouse=True)
    def _chain_enabled(self, monkeypatch):
        monkeypatch.delenv("SCANNER_LISTING_FETCH_CHAIN", raising=False)

    def test_requests_result_accepted_when_sufficient(self, monkeypatch):
        from backend.scanner.post_scan import gap_fill

        monkeypatch.setattr(gap_fill, "_requests_fetch_html", lambda u, **kw: GOOD_HTML)
        monkeypatch.setattr(
            gap_fill,
            "_playwright_fetch_html",
            lambda u, **kw: pytest.fail("playwright must not run"),
        )
        assert gap_fill.fetch_listing_html("https://d.example/vdp") == GOOD_HTML

    def test_thin_requests_result_falls_through_to_playwright(self, monkeypatch):
        from backend.scanner.post_scan import gap_fill

        monkeypatch.setattr(gap_fill, "_requests_fetch_html", lambda u, **kw: THIN_HTML)
        monkeypatch.setattr(
            gap_fill, "_playwright_fetch_html", lambda u, **kw: "<html>rendered</html>"
        )
        assert gap_fill.fetch_listing_html("https://d.example/vdp") == "<html>rendered</html>"

    def test_total_failure_returns_none(self, monkeypatch):
        from backend.scanner.post_scan import gap_fill

        monkeypatch.setattr(gap_fill, "_requests_fetch_html", lambda u, **kw: None)
        monkeypatch.setattr(gap_fill, "_playwright_fetch_html", lambda u, **kw: None)
        assert gap_fill.fetch_listing_html("https://d.example/vdp") is None

    def test_non_http_url_returns_none_without_fetching(self, monkeypatch):
        from backend.scanner.post_scan import gap_fill

        monkeypatch.setattr(
            gap_fill,
            "_requests_fetch_html",
            lambda u, **kw: pytest.fail("must not fetch"),
        )
        assert gap_fill.fetch_listing_html("ftp://nope") is None
        assert gap_fill.fetch_listing_html("") is None

    def test_env_flag_forces_legacy_path(self, monkeypatch):
        from backend.scanner.post_scan import gap_fill

        monkeypatch.setenv("SCANNER_LISTING_FETCH_CHAIN", "0")
        monkeypatch.setattr(gap_fill, "_requests_fetch_html", lambda u, **kw: GOOD_HTML)
        monkeypatch.setattr(
            gap_fill,
            "_playwright_fetch_html",
            lambda u, **kw: pytest.fail("playwright must not run"),
        )
        assert gap_fill.fetch_listing_html("https://d.example/vdp") == GOOD_HTML

    def test_chain_bug_falls_back_to_legacy(self, monkeypatch):
        from backend.scanner.post_scan import gap_fill

        monkeypatch.setattr(
            gap_fill,
            "_fetch_listing_html_via_chain",
            lambda u: (_ for _ in ()).throw(RuntimeError("chain bug")),
        )
        monkeypatch.setattr(gap_fill, "_requests_fetch_html", lambda u, **kw: GOOD_HTML)
        monkeypatch.setattr(gap_fill, "_playwright_fetch_html", lambda u, **kw: None)
        assert gap_fill.fetch_listing_html("https://d.example/vdp") == GOOD_HTML

    def test_works_inside_running_event_loop(self, monkeypatch):
        import asyncio

        from backend.scanner.post_scan import gap_fill

        monkeypatch.setattr(gap_fill, "_requests_fetch_html", lambda u, **kw: GOOD_HTML)
        monkeypatch.setattr(gap_fill, "_playwright_fetch_html", lambda u, **kw: None)

        async def go():
            return gap_fill.fetch_listing_html("https://d.example/vdp")

        assert asyncio.run(go()) == GOOD_HTML


class TestPlaywrightFetcherAvailability:
    def test_available_reflects_import(self):
        from backend.scanner.chain import PlaywrightFetcher

        try:
            import playwright  # noqa: F401

            expected = True
        except ImportError:
            expected = False
        assert PlaywrightFetcher().available() is expected

    def test_injected_page_factory_used(self):
        from contextlib import contextmanager

        from backend.scanner.chain import PlaywrightFetcher

        class FakePage:
            def __init__(self):
                self.goto_calls = []

            def goto(self, url, wait_until=None, timeout=None):
                self.goto_calls.append((url, wait_until, timeout))

            def content(self):
                return "<html>fake</html>"

        page = FakePage()
        seen_ua = []

        @contextmanager
        def factory(ua):
            seen_ua.append(ua)
            yield page

        f = PlaywrightFetcher(timeout_ms=1234, wait_until="domcontentloaded", page_factory=factory)
        assert f.fetch("http://x") == "<html>fake</html>"
        assert page.goto_calls == [("http://x", "domcontentloaded", 1234)]
        assert seen_ua and "Mozilla" in seen_ua[0]
