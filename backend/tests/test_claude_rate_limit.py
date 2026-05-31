"""Tests for Anthropic vision rate limiter."""
from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

from backend.vision import claude_rate_limit as rl


def test_acquire_vision_slot_blocks_when_bucket_empty(monkeypatch) -> None:
    monkeypatch.setenv("ANTHROPIC_VISION_RPM", "60")
    rl._tokens = 0.0
    rl._last_refill = time.monotonic()
    t0 = time.monotonic()
    rl.acquire_vision_slot(cost=1.0)
    rl.acquire_vision_slot(cost=1.0)
    assert time.monotonic() - t0 >= 0.8


def test_anthropic_messages_create_retries_429(monkeypatch) -> None:
    monkeypatch.setenv("ANTHROPIC_VISION_RPM", "120")
    monkeypatch.setenv("ANTHROPIC_VISION_MAX_RETRIES", "3")
    client = MagicMock()
    err = Exception("429 rate limit")
    err.status_code = 429
    ok = MagicMock()
    client.messages.create.side_effect = [err, ok]
    with patch.object(rl, "acquire_vision_slot"):
        with patch.object(rl.time, "sleep"):
            out = rl.anthropic_messages_create(client, model="m", max_tokens=1, messages=[])
    assert out is ok
    assert client.messages.create.call_count == 2
