"""Tests for failed job diagnosis (rules + smart retry hints)."""

from __future__ import annotations

from backend.scanner.job_diagnosis import (
    apply_diagnosis_to_payload,
    diagnose_failed_job,
)
from backend.scanner.retry_env import filter_retry_env, is_retry_env_key_allowed


def test_diagnose_browser_arch_mismatch(monkeypatch) -> None:
    monkeypatch.setattr(
        "backend.scanner.job_diagnosis._playwright_chromium_path",
        lambda: "/fake/playwright/chrome-linux/chrome",
    )
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


def test_apply_diagnosis_strips_disallowed_env_keys() -> None:
    d = {
        "retry_actions": [
            {"type": "set_env", "key": "INVENTORY_DATABASE_URL", "value": "postgres://evil"},
            {"type": "set_env", "key": "PUPPETEER_EXECUTABLE_PATH", "value": "/chrome"},
        ]
    }
    out = apply_diagnosis_to_payload(
        {"url": "https://x.com", "retry_env": {"PYTHONPATH": "/tmp/evil"}},
        d,
    )
    assert "INVENTORY_DATABASE_URL" not in out.get("retry_env", {})
    assert "PYTHONPATH" not in out.get("retry_env", {})
    assert out["retry_env"]["PUPPETEER_EXECUTABLE_PATH"] == "/chrome"


def test_retry_env_allowlist() -> None:
    assert is_retry_env_key_allowed("PUPPETEER_EXECUTABLE_PATH")
    assert is_retry_env_key_allowed("SCANNER_JOB_TIMEOUT_SEC")
    assert not is_retry_env_key_allowed("INVENTORY_DATABASE_URL")
    assert not is_retry_env_key_allowed("PYTHONPATH")
    assert not is_retry_env_key_allowed("LD_PRELOAD")
    assert not is_retry_env_key_allowed("scanner_job_timeout_sec")

    filtered = filter_retry_env(
        {
            "PUPPETEER_EXECUTABLE_PATH": "/chrome",
            "SCANNER_FAILURE_HAR": "1",
            "INVENTORY_DATABASE_URL": "postgres://x",
            "PYTHONPATH": "/evil",
        }
    )
    assert filtered == {
        "PUPPETEER_EXECUTABLE_PATH": "/chrome",
        "SCANNER_FAILURE_HAR": "1",
    }
