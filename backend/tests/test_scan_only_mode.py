"""SCANNER_SCAN_ONLY / fast mode env defaults."""

from __future__ import annotations

from backend.scanner.scan_efficiency import (
    apply_fast_mode_env_defaults,
    apply_scan_only_env_defaults,
    scanner_scan_only_enabled,
)


def test_scan_only_env_default_disables_post_stages(monkeypatch):
    monkeypatch.delenv("SCANNER_POST_REPAIR", raising=False)
    monkeypatch.setenv("SCANNER_SCAN_ONLY", "1")
    apply_scan_only_env_defaults()
    assert scanner_scan_only_enabled() is True
    import os

    assert os.environ.get("SCANNER_POST_REPAIR") == "0"
    assert os.environ.get("SCANNER_POST_WINDOW_STICKER") == "0"


def test_fast_mode_sets_scan_only(monkeypatch):
    for key in ("SCANNER_SCAN_ONLY", "SCANNER_VDP_EP_MAX"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("SCANNER_FAST_MODE", "1")
    apply_fast_mode_env_defaults()
    import os

    assert os.environ.get("SCANNER_SCAN_ONLY") == "1"
    assert os.environ.get("SCANNER_VDP_EP_MAX") == "0"
