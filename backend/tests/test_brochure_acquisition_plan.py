"""
Offline checks for ``brochure_acquisition.plan.plan_and_download`` -- the
default mode of ``backend/scripts/fetch_oem_brochures.py`` after the F7 split.

Every collaborator is stubbed where ``plan`` looks it up: no database, no
fetcher, no file written.
"""
from __future__ import annotations

import sys

from backend.enrichment.brochure_acquisition import plan
from backend.scripts import fetch_oem_brochures as fob

_STATS = {
    "corpus_files": 10,
    "active_cars": 100,
    "distinct_vehicles": 3,
    "raw_catalog_key_combos": 4,
    "covered_vehicles": 1,
    "uncovered_vehicles": 2,
    "covered_cars": 40,
    "gap_cars": 60,
    "alias_only_keys": 0,
    "alias_only_cars": 0,
}


def _gap(make: str, model: str, cars: int) -> dict:
    return {
        "group_key": f"2024|{make.lower()}|{model.lower()}",
        "year": 2024,
        "make": make,
        "model": model,
        "cars": cars,
        "variants": [{}],
        "primary_catalog_key": f"2024__{make.lower()}__{model.lower()}",
        "alias_catalog_keys": [],
        "unsupported_reason": "",
    }


class _Fetcher:
    delay = 4.0
    robots_agent = "x"
    user_agent = "x"

    @classmethod
    def for_tier(cls, tier, *, delay, user_agent):
        return cls()

    def get_text(self, url):  # pragma: no cover - never reached
        raise AssertionError("network")


class _Index:
    by_sha: dict = {}


def _stub(monkeypatch, gaps):
    monkeypatch.setattr(plan, "load_gap_list", lambda: (gaps, dict(_STATS)))
    monkeypatch.setattr(plan, "open_content_index", lambda: (_Index(), {}))
    monkeypatch.setattr(plan, "PacedFetcher", _Fetcher)
    monkeypatch.setattr(plan, "RobotsPolicy", lambda *a, **k: object())
    resolved: list[str] = []

    def _resolve(gap, fetcher, robots, page_cache, *, archive=None):
        resolved.append(gap["model"])
        return {
            "status": "found",
            "tier": "oem",
            "doc_url": f"https://example.invalid/{gap['model']}.pdf",
            "discovery_url": "https://example.invalid/",
            "detail": "score=1",
        }

    def _no_download(*a, **k):
        raise AssertionError("dry run downloaded")

    def _no_artifact(*a, **k):
        raise AssertionError("artifact written despite --no-artifact")

    monkeypatch.setattr(plan, "resolve_source", _resolve)
    monkeypatch.setattr(plan, "download_and_extract", _no_download)
    monkeypatch.setattr(plan, "write_gap_artifact", _no_artifact)
    return resolved


def test_dry_run_filters_limits_and_writes_nothing(monkeypatch, capsys):
    gaps = [_gap("Mazda", "CX50", 30), _gap("Honda", "Pilot", 20), _gap("Mazda", "3", 10)]
    resolved = _stub(monkeypatch, gaps)
    monkeypatch.setattr(
        sys, "argv", ["x", "--brand", "MAZDA", "--limit", "1", "--no-artifact"]
    )
    assert fob.main() == 0
    assert resolved == ["CX50"]
    out = capsys.readouterr().out
    assert "DRY RUN (default) - nothing will be written: considering 1 gaps" in out
    assert "WOULD FETCH [oem] https://example.invalid/CX50.pdf" in out
    assert "would fetch 1 document(s); re-run with --download" in out


def test_html_specs_mode_stops_before_pdf_resolution(monkeypatch, capsys):
    resolved = _stub(monkeypatch, [_gap("Honda", "Pilot", 20)])
    seen: list[dict] = []

    def _html(filtered, *, limit, delay, user_agent, download):
        seen.append({"n": len(filtered), "limit": limit, "delay": delay,
                     "download": download})
        return {
            "considered": 1, "discovered": 0, "already_held": 0, "fetched": 0,
            "parsed": 0, "stored": 0, "refused": 0, "overlays": 0, "citations": 0,
            "verified": 0, "verify_failures": {}, "refusals": [],
        }

    monkeypatch.setattr(plan, "fetch_html_specs", _html)
    monkeypatch.setattr(
        sys, "argv", ["x", "--html-specs", "--no-artifact", "--delay", "1"]
    )
    assert fob.main() == 0
    assert seen == [{"n": 1, "limit": 25, "delay": 3.0, "download": False}]
    assert resolved == []
    assert "HTML spec pages: considered=1" in capsys.readouterr().out
