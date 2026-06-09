"""Tests for Google Places rating fetch + dealership cache."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from backend.cron.scan_run_counter import record_scanner_batch_finished
from backend.cron.sync_dealer_google_ratings import backfill_dealer_google_ratings
from backend.discovery.google_place_rating import (
    GooglePlaceRating,
    fetch_place_rating_by_id,
    rating_from_place_dict,
    search_place_rating,
)


def test_rating_from_place_dict_parses_fields() -> None:
    out = rating_from_place_dict(
        {"id": "places/ChIJabc", "rating": 4.6, "userRatingCount": 312}
    )
    assert out is not None
    assert out.place_id == "ChIJabc"
    assert out.rating == 4.6
    assert out.review_count == 312


def test_fetch_place_rating_by_id_uses_place_details() -> None:
    mock_resp = MagicMock()
    mock_resp.raise_for_status.return_value = None
    mock_resp.json.return_value = {
        "id": "places/ChIJxyz",
        "rating": 4.2,
        "userRatingCount": 88,
    }
    sess = MagicMock()
    sess.get.return_value = mock_resp

    out = fetch_place_rating_by_id(
        "ChIJxyz",
        api_key="test-key",
        session=sess,
    )
    assert out is not None
    assert out.rating == 4.2
    assert out.review_count == 88
    sess.get.assert_called_once()
    args, kwargs = sess.get.call_args
    assert args[0].endswith("/places/ChIJxyz")
    assert kwargs["headers"]["X-Goog-FieldMask"] == "id,rating,userRatingCount"


def test_search_place_rating_text_search() -> None:
    mock_resp = MagicMock()
    mock_resp.raise_for_status.return_value = None
    mock_resp.json.return_value = {
        "places": [
            {
                "id": "places/ChIJsearch",
                "rating": 4.8,
                "userRatingCount": 501,
                "displayName": {"text": "Premier Auto"},
            }
        ]
    }
    sess = MagicMock()
    sess.post.return_value = mock_resp

    out = search_place_rating(
        "Premier Auto",
        "Charlotte",
        "NC",
        latitude=35.05,
        longitude=-80.85,
        api_key="test-key",
        session=sess,
    )
    assert out is not None
    assert out.place_id == "ChIJsearch"
    assert out.rating == 4.8
    body = sess.post.call_args.kwargs["json"]
    assert "Premier Auto" in body["textQuery"]
    assert body["locationBias"]["circle"]["center"]["latitude"] == 35.05


def test_backfill_only_missing_dealers(monkeypatch) -> None:
    monkeypatch.setattr(
        "backend.cron.sync_dealer_google_ratings.list_dealers_needing_google_rating",
        lambda limit=25: [
            {
                "id": 3,
                "name": "New Dealer",
                "city": "Raleigh",
                "state": "NC",
                "latitude": 35.8,
                "longitude": -78.6,
                "google_place_id": None,
            }
        ],
    )
    monkeypatch.setattr(
        "backend.cron.sync_dealer_google_ratings.search_place_rating",
        lambda *a, **k: GooglePlaceRating(place_id="ChIJnew", rating=4.0, review_count=42),
    )
    saved: list[tuple] = []

    def _save(dealer_id, *, place_id, rating, review_count):
        saved.append((dealer_id, place_id, rating, review_count))

    monkeypatch.setattr("backend.cron.sync_dealer_google_ratings.save_dealer_google_rating", _save)
    monkeypatch.setattr("backend.cron.sync_dealer_google_ratings.google_maps_api_key", lambda: "key")
    monkeypatch.setattr("backend.cron.sync_dealer_google_ratings.time.sleep", lambda _s: None)

    summary = backfill_dealer_google_ratings(max_dealers=5)
    assert summary["examined"] == 1
    assert summary["resolved"] == 1
    assert saved == [(3, "ChIJnew", 4.0, 42)]


def test_scan_counter_runs_ratings_every_tenth_batch(tmp_path, monkeypatch) -> None:
    counter_path = tmp_path / "scanner_run_counter.json"
    monkeypatch.setattr("backend.cron.scan_run_counter.COUNTER_PATH", counter_path)
    monkeypatch.setenv("DEALER_GOOGLE_RATING_SCAN_INTERVAL", "10")

    flags = []
    for _ in range(10):
        out = record_scanner_batch_finished()
        flags.append(bool(out.get("run_dealer_ratings")))

    assert flags.count(True) == 1
    assert flags[-1] is True
