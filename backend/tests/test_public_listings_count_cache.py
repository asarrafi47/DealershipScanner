"""``public_listings_count`` is cached (landing page ran COUNT(*) per request).

Efficiency review 2026-09-28, recommendation 3: keyed by the grid write
fingerprint with a 300 s TTL; a moving fingerprint (scan in progress) still
keeps the last count for 60 s.
"""
from __future__ import annotations

import pytest

from backend.db.repositories import listings_repo


@pytest.fixture
def counted(monkeypatch):
    calls = {"n": 0}

    def fake_count() -> int:
        calls["n"] += 1
        return 1000 + calls["n"]

    monkeypatch.setattr(listings_repo, "_count_public_listings_uncached", fake_count)
    monkeypatch.setattr(listings_repo, "_public_count_cache", None)
    return calls


def _clock(monkeypatch, start: float):
    now = {"t": start}
    monkeypatch.setattr(listings_repo.time, "time", lambda: now["t"])
    return now


def test_count_is_served_from_cache_within_ttl(counted, monkeypatch):
    monkeypatch.setattr(listings_repo, "_pg_grid_write_fingerprint", lambda: (("cars", 1),))
    now = _clock(monkeypatch, 1_000.0)
    assert listings_repo.public_listings_count() == 1001
    now["t"] += 10
    assert listings_repo.public_listings_count() == 1001
    assert counted["n"] == 1
    now["t"] += listings_repo._PUBLIC_COUNT_TTL_S
    assert listings_repo.public_listings_count() == 1002
    assert counted["n"] == 2


def test_fingerprint_change_recounts_after_the_min_interval(counted, monkeypatch):
    fp = {"v": (("cars", 1),)}
    monkeypatch.setattr(listings_repo, "_pg_grid_write_fingerprint", lambda: fp["v"])
    now = _clock(monkeypatch, 1_000.0)
    assert listings_repo.public_listings_count() == 1001
    # Writes landed (scan): the fingerprint moves, but not a recount per request.
    fp["v"] = (("cars", 2),)
    now["t"] += 5
    assert listings_repo.public_listings_count() == 1001
    assert counted["n"] == 1
    now["t"] += listings_repo._PUBLIC_COUNT_MIN_RECOUNT_S
    assert listings_repo.public_listings_count() == 1002
    assert counted["n"] == 2


def test_legacy_token_change_recounts_immediately(counted, monkeypatch):
    """Off Postgres the token is the DB mtime pair: a different DB is a different count."""
    monkeypatch.setattr(listings_repo, "_pg_grid_write_fingerprint", lambda: None)
    tok = {"v": (1.0, 1.0)}
    monkeypatch.setattr(listings_repo, "_listings_cache_token", lambda: tok["v"])
    _clock(monkeypatch, 1_000.0)
    assert listings_repo.public_listings_count() == 1001
    assert listings_repo.public_listings_count() == 1001
    tok["v"] = (2.0, 1.0)
    assert listings_repo.public_listings_count() == 1002
    assert counted["n"] == 2


def test_clear_inventory_listings_cache_drops_the_count(counted, monkeypatch):
    monkeypatch.setattr(listings_repo, "_pg_grid_write_fingerprint", lambda: (("cars", 1),))
    _clock(monkeypatch, 1_000.0)
    assert listings_repo.public_listings_count() == 1001
    listings_repo.clear_inventory_listings_cache()
    assert listings_repo.public_listings_count() == 1002
