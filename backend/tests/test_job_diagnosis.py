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
    assert d["retry_recommended"] is True
    assert d["retry_strategy"] == "retry_with_env"
    assert any(a.get("key") == "PUPPETEER_EXECUTABLE_PATH" for a in d["retry_actions"])


def test_apply_diagnosis_sets_profile_and_env() -> None:
    d = {
        "retry_actions": [
            {"type": "set_profile", "value": "resilient"},
            {"type": "set_env", "key": "PUPPETEER_EXECUTABLE_PATH", "value": "/chrome"},
        ]
    }
    out = apply_diagnosis_to_payload({"url": "https://x.com"}, d)
    assert out["profile"] == "resilient"
    assert out["retry_env"]["PUPPETEER_EXECUTABLE_PATH"] == "/chrome"
    assert out["ai_diagnosis"]["retry_strategy"] == d.get("retry_strategy")
