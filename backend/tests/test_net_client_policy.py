"""P12A.1: named FetchPolicy values equal today's per-site constants; HostProfileMemory."""
from __future__ import annotations

import threading

from backend.scanner import chain
from backend.scanner.net import client as nc


def test_fingerprint_block_set_defined():
    assert nc.FINGERPRINT_BLOCK_STATUSES == {403, 405, 429, 503}


def test_policy_values_match_site_constants():
    from backend.scanner import recipes
    from backend.scanner.synth import http as synth_http
    from backend.scanner.vdp import prefetch, vdp_recipes

    assert nc.FEED_REPLAY.timeout == recipes._REPLAY_TIMEOUT_S
    assert nc.FEED_REPLAY.block_statuses == recipes._TLS_FINGERPRINT_STATUSES
    assert nc.FEED_REPLAY.profiles == nc.IMPERSONATE_PROFILES
    assert nc.SYNTH_HTML.timeout == 25.0 == nc.SYNTH_JSON.timeout
    assert nc.SYNTH_HTML.min_bytes == synth_http._MIN_REAL_HTML_BYTES
    assert nc.SYNTH_HTML.retry_statuses == synth_http._RETRYABLE_STATUS
    assert nc.SYNTH_HTML.retries == 2 and nc.SYNTH_HTML.backoff_s == 2.0
    assert nc.VDP_HTML.timeout == prefetch._HTTP_FETCH_TIMEOUT_S
    assert nc.VDP_HTML.block_statuses == nc.VDP_FINGERPRINT_BLOCK_STATUSES
    assert nc.VDP_HTML.check_challenge and nc.VDP_HTML.profiles == nc.IMPERSONATE_PROFILES  # P12A.5
    assert nc.VDP_JSON.timeout == vdp_recipes._REPLAY_TIMEOUT_S
    assert nc.CHAIN_IMPERSONATE.timeout == 25.0
    assert nc.CHAIN_IMPERSONATE.profiles == chain.ImpersonatingFetcher.PROFILES
    assert nc.CHAIN_REQUESTS.timeout == 20.0 and not nc.CHAIN_REQUESTS.profiles
    assert nc.STATUS_PROBE.timeout == 20.0


def test_policy_is_frozen():
    import dataclasses

    try:
        nc.FEED_REPLAY.timeout = 1.0  # type: ignore[misc]
    except dataclasses.FrozenInstanceError:
        return
    raise AssertionError("FetchPolicy must be frozen")


def test_memory_order_puts_winner_first():
    m = nc.HostProfileMemory()
    assert m.order("h") == list(nc.IMPERSONATE_PROFILES)
    m.remember("h", "safari17_0")
    assert m.order("h") == ["safari17_0", "chrome", "chrome124"]
    assert m.get("other") is None


def test_chain_fetcher_shares_process_wide_memory():
    a, b = chain.ImpersonatingFetcher(), chain.ImpersonatingFetcher()
    assert a._winning is nc.HOST_PROFILES is b._winning
    a._winning.remember("x.example", "chrome124")
    assert b._winning.get("x.example") == "chrome124"


def test_memory_thread_safe():
    m = nc.HostProfileMemory()

    def work(i: int) -> None:
        for j in range(500):
            m.remember(f"h{i}-{j}", "chrome")
            m.order(f"h{i}-{j}")

    ts = [threading.Thread(target=work, args=(i,)) for i in range(8)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert len(m) == 8 * 500
