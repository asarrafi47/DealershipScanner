"""SEC-A-002 / SEC-A-018 / B-066: dealer-supplied sticker URL fetch guard."""

import pytest

from backend.scanner.post_scan import window_sticker as ws

VIN = "W1K6G7GB6NA126084"


def test_ipacket_pattern_must_cover_whole_url():
    assert ws.is_listing_sticker_url(f"https://djapi.autoipacket.com/v2/sticker-puller/download/{VIN}?token=a.b")
    # Pattern smuggled into the query of another host (old .search() accepted it).
    evil = f"http://127.0.0.1:8080/x?u=https://autoipacket.com/v2/sticker-puller/download/{VIN}"
    assert not ws.is_listing_sticker_url(evil)
    assert not ws.is_listing_sticker_url(f"http://169.254.169.254/#https://autoipacket.com/v2/sticker-puller/download/{VIN}")


def test_ipacket_host_is_exact_not_suffix():
    assert ws._is_trusted_ipacket_host("djapi.autoipacket.com")
    assert not ws._is_trusted_ipacket_host("evil.autoipacket.com")
    assert not ws._is_trusted_ipacket_host("autoipacket.com.evil.test")


@pytest.mark.parametrize("host", ["dealer.localhost", "inventory.internal", "cdn.127.0.0.1", "dealer.local"])
def test_dealer_hint_hosts_resolving_private_are_not_fetched(monkeypatch, host):
    def boom(*a, **k):
        raise AssertionError("requests.get must not run")

    monkeypatch.setattr("requests.get", boom)
    with pytest.raises(ValueError):
        ws.fetch_listing_sticker(f"http://{host}/window-sticker/{VIN}.pdf", VIN)


class _Resp:
    def __init__(self, status=200, headers=None, body=b"x" * 10):
        self.status_code = status
        self.headers = headers or {}
        self._body = body
        self.closed = False

    @property
    def content(self):
        return self._content

    @property
    def text(self):
        return self._content.decode("utf-8", "replace")

    def raise_for_status(self):
        pass

    def iter_content(self, chunk_size=1):
        for i in range(0, len(self._body), chunk_size):
            yield self._body[i : i + chunk_size]

    def close(self):
        self.closed = True


def _allow_public(monkeypatch):
    monkeypatch.setattr(
        "backend.utils.outbound_url.destination_host_blocked_after_dns",
        lambda h: h in ("localhost", "metadata.internal") or h.startswith("169.254"),
    )


def test_redirect_to_internal_host_is_refused(monkeypatch):
    _allow_public(monkeypatch)
    calls = []

    def fake_get(url, **kw):
        calls.append((url, kw.get("allow_redirects")))
        return _Resp(302, {"location": "http://169.254.169.254/latest/meta-data"})

    monkeypatch.setattr("requests.get", fake_get)
    with pytest.raises(ValueError):
        ws.fetch_listing_sticker(f"https://dealer-cdn.example.com/window-sticker/{VIN}.pdf", VIN)
    assert calls == [(f"https://dealer-cdn.example.com/window-sticker/{VIN}.pdf", False)]


def test_public_redirect_followed_by_hand(monkeypatch):
    _allow_public(monkeypatch)
    seq = [_Resp(301, {"location": "/final.pdf"}), _Resp(200, {"content-type": "text/plain"}, b"hello")]
    urls = []

    def fake_get(url, **kw):
        urls.append(url)
        assert kw["allow_redirects"] is False
        return seq.pop(0)

    monkeypatch.setattr("requests.get", fake_get)
    out = ws.fetch_listing_sticker(f"https://dealer-cdn.example.com/window-sticker/{VIN}.pdf", VIN)
    assert urls[1] == "https://dealer-cdn.example.com/final.pdf"
    assert out["content"] == b"hello"


def test_body_byte_cap(monkeypatch):
    _allow_public(monkeypatch)
    monkeypatch.setattr(ws, "_STICKER_MAX_BYTES", 100)
    monkeypatch.setattr("requests.get", lambda *a, **k: _Resp(200, {}, b"a" * 500))
    with pytest.raises(ValueError):
        ws.fetch_listing_sticker(f"https://dealer-cdn.example.com/window-sticker/{VIN}.pdf", VIN)


def test_vin_in_url_fallback_requires_vin_shape():
    # 32-hex content hash contains a 17-char run that is not a VIN.
    assert ws._vin_token_in_url("https://dealer-cdn.example.com/window-sticker/0123456789abcdef0123456789abcdef.pdf") is None
    assert ws._vin_token_in_url(f"https://dealer-cdn.example.com/s/{VIN}.pdf") == VIN
