"""Tests for failed job diagnosis (rules + smart retry hints)."""

from __future__ import annotations

from backend.scanner.job_diagnosis import (
    apply_diagnosis_to_payload,
    diagnose_failed_job,
)


def test_diagnose_browser_arch_mismatch() -> None:
    log = "qemu-x86_64: Could not open '/lib64/ld-linux-x86-64.so.2'\nFailed to launch the browser process"
    d = diagnose_failed_job(
        error="exit_1",
        log_tail=log,
        job_type="onboard",
        dealer_id="test-dealer",
        payload={"url": "https://example.com"},
        use_llm=False,
    )
    assert d["category"] == "infrastructure"
    assert d["retry_recommended"] is False
    assert d["retry_strategy"] == "needs_code_fix"
    assert d["retry_actions"] == []
    assert "Playwright" in d["code_fix_hint"]


def test_node_era_strategies_are_gone() -> None:
    from backend.scanner import job_diagnosis as jd

    assert "retry_with_python_scanner" not in jd._STRATEGIES
    d = diagnose_failed_job(
        error="exit_1", log_tail="Error: Cannot find module 'puppeteer'", use_llm=False
    )
    assert d["source"] == "fallback"
    out = jd.apply_diagnosis_to_payload({}, {"retry_actions": [{"type": "use_python_scanner"}]})
    assert "use_python_scanner" not in out
    assert jd._normalize_diagnosis({"retry_actions": [{"type": "use_python_scanner"}]})["retry_actions"] == []


def test_apply_diagnosis_sets_profile_and_env() -> None:
    d = {
        "retry_actions": [
            {"type": "set_profile", "value": "resilient"},
            {"type": "set_env", "key": "SCANNER_EXAMPLE_FLAG", "value": "1"},
        ]
    }
    out = apply_diagnosis_to_payload({"url": "https://x.com"}, d)
    assert out["profile"] == "resilient"
    assert out["retry_env"]["SCANNER_EXAMPLE_FLAG"] == "1"
    assert out["ai_diagnosis"]["retry_strategy"] == d.get("retry_strategy")
