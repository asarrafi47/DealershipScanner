"""Tests for scrape confidence stdout parsing."""

from __future__ import annotations

from backend.scanner.scrape_confidence import merge_result_with_scanner_stdout, parse_scanner_stdout


def test_parse_scanner_stdout_confidence() -> None:
    stdout = (
        "info\n"
        'SCAN_CONFIDENCE:{"level":"high","score":0.95,"reason":"ok","path":"turbo","vehicle_count":42}\n'
        "SCAN_VEHICLE_COUNT:42\n"
    )
    parsed = parse_scanner_stdout(stdout)
    assert parsed["vehicle_count"] == 42
    assert parsed["scrape_confidence"]["level"] == "high"
    assert parsed["scrape_confidence"]["score"] == 0.95


def test_merge_result_with_scanner_stdout() -> None:
    stdout = 'SCAN_CONFIDENCE:{"level":"low","score":0.1,"reason":"none"}\n'
    merged = merge_result_with_scanner_stdout({"log_tail": "x"}, stdout)
    assert merged["scrape_confidence"]["level"] == "low"
    assert merged["log_tail"] == "x"


def test_soft_scrape_failure_onboard_zero() -> None:
    from backend.scanner.scrape_confidence import job_display_status, soft_scrape_failure

    result = {"vehicle_count": 0, "scrape_confidence": {"level": "low", "score": 0.08}}
    assert soft_scrape_failure("onboard", result) is True
    assert job_display_status("done", "onboard", result) == "incomplete"


def test_soft_scrape_failure_success() -> None:
    from backend.scanner.scrape_confidence import job_display_status, soft_scrape_failure

    result = {"vehicle_count": 42, "scrape_confidence": {"level": "high", "score": 0.95}}
    assert soft_scrape_failure("onboard", result) is False
    assert job_display_status("done", "onboard", result) == "done"
