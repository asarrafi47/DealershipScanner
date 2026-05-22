"""Package-level parser behavior (unknown provider, etc.)."""

from __future__ import annotations

import pytest

import backend.parsers as parsers


@pytest.fixture(autouse=True)
def clear_unknown_provider_warnings() -> None:
    parsers._warned_unknown_providers.clear()
    yield
    parsers._warned_unknown_providers.clear()


def test_unknown_provider_returns_empty() -> None:
    """Unknown provider tries auto-detect; empty raw data yields no vehicles."""
    assert parsers.parse("not_a_provider", {}, base_url="https://x.com/", dealer_id="d1") == []
    assert parsers.parse("not_a_provider", {}, base_url="https://x.com/", dealer_id="d1") == []
