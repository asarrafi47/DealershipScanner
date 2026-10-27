"""P12A.6: the dealer-probe scripts reach curl_cffi through net/client and send the scanner
proxy (fakes only, no network)."""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.scanner.net import client as nc

PROXY = "http://proxy.test:8080"
SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"


@pytest.fixture(autouse=True)
def _proxy(monkeypatch):
    for name in ("HTTPS_PROXY", "HTTP_PROXY"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("SCANNER_HTTP_PROXY", PROXY)


class FakeCffi:
    def __init__(self, statuses, body="<html>autowall</html>"):
        self.calls: list[dict] = []
        self.statuses = list(statuses)
        self.body = body

    def get(self, url, **kwargs):
        self.calls.append({"url": url, **kwargs})
        status = self.statuses.pop(0) if len(self.statuses) > 1 else self.statuses[0]
        if isinstance(status, Exception):
            raise status
        return SimpleNamespace(
            status_code=status, text=self.body, url=url, history=[],
            headers={"server": "cloudflare"},
        )


def test_scripts_have_no_direct_curl_cffi_import():
    for name in ("discovery_probe.py", "audit_unscannable_dealers.py"):
        assert "from curl_cffi" not in (SCRIPTS / name).read_text(), name


def test_discovery_probe_get_is_a_single_chrome_probe_with_proxy_and_profile(monkeypatch):
    from backend.scripts import discovery_probe

    fake = FakeCffi([200])
    monkeypatch.setattr(nc, "import_curl_cffi", lambda: fake)
    rec = discovery_probe._get("https://dealer.example.com/", referer="https://dealer.example.com/")
    (call,) = fake.calls
    assert call["impersonate"] == "chrome"
    assert call["proxies"] == {"http": PROXY, "https": PROXY}
    assert call["headers"]["Referer"] == "https://dealer.example.com/"
    assert rec["profile"] == "chrome" and rec["status"] == 200


def test_discovery_probe_record_only_gains_profile(monkeypatch):
    from backend.scripts import discovery_probe

    monkeypatch.setattr(nc, "import_curl_cffi", lambda: FakeCffi([200]))
    rec = discovery_probe._get("https://dealer.example.com/")
    assert set(rec) == {
        "url", "final_url", "profile", "status", "bytes", "secs", "title", "server", "cf_ray",
        "content_type", "redirects", "text",
    }


def test_discovery_probe_error_record_is_unchanged(monkeypatch):
    from backend.scripts import discovery_probe

    monkeypatch.setattr(nc, "import_curl_cffi", lambda: FakeCffi([RuntimeError("boom")]))
    rec = discovery_probe._get("https://dealer.example.com/")
    assert set(rec) == {"url", "error", "secs"} and rec["error"].startswith("RuntimeError")


def test_audit_probe_rotates_until_200_and_sends_proxy(monkeypatch):
    from backend.scripts import audit_unscannable_dealers as audit

    fake = FakeCffi([403, 200])
    monkeypatch.setattr(nc, "curl_cffi_requests", lambda: fake)
    out = audit._probe_impersonated("https://dealer.example.com/")
    assert [c["impersonate"] for c in fake.calls] == ["chrome", "chrome124"]
    assert all(c["proxies"] == {"http": PROXY, "https": PROXY} for c in fake.calls)
    assert out["status"] == 200 and out["impersonate"] == "chrome124"


def test_audit_probe_returns_last_answer_when_nothing_clears(monkeypatch):
    from backend.scripts import audit_unscannable_dealers as audit

    fake = FakeCffi([403])
    monkeypatch.setattr(nc, "curl_cffi_requests", lambda: fake)
    out = audit._probe_impersonated("https://dealer.example.com/")
    assert len(fake.calls) == len(nc.IMPERSONATE_PROFILES)
    assert out["status"] == 403 and out["impersonate"] == "safari17_0"


def test_audit_probe_error_then_none_without_curl_cffi(monkeypatch):
    from backend.scripts import audit_unscannable_dealers as audit

    monkeypatch.setattr(nc, "curl_cffi_requests", lambda: FakeCffi([RuntimeError("x" * 300)]))
    out = audit._probe_impersonated("https://dealer.example.com/")
    assert out["status"] is None and len(out["error"]) == 120
    monkeypatch.setattr(nc, "curl_cffi_requests", lambda: None)
    assert audit._probe_impersonated("https://dealer.example.com/") is None
