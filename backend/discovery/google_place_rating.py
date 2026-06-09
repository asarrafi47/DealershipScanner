"""
Google Places rating lookup — fetch once, cache on ``dealerships`` rows.

Uses Places API (New):
- ``GET places/{id}`` when ``google_place_id`` is known (cheapest refresh).
- ``POST places:searchText`` when resolving a dealer for the first time.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any

import requests

logger = logging.getLogger(__name__)

SEARCH_TEXT_URL = "https://places.googleapis.com/v1/places:searchText"
PLACE_DETAIL_FIELD_MASK = "id,rating,userRatingCount"
SEARCH_FIELD_MASK = "places.id,places.rating,places.userRatingCount,places.displayName"


@dataclass(frozen=True)
class GooglePlaceRating:
    place_id: str
    rating: float | None
    review_count: int | None


def google_maps_api_key() -> str | None:
    key = (os.environ.get("GOOGLE_MAPS_API_KEY") or "").strip()
    return key or None


def _normalize_place_id(raw: str | None) -> str:
    pid = (raw or "").strip()
    if pid.startswith("places/"):
        return pid[len("places/") :]
    return pid


def _parse_rating_fields(place: dict[str, Any]) -> tuple[float | None, int | None]:
    rating_raw = place.get("rating")
    count_raw = place.get("userRatingCount")
    rating: float | None
    review_count: int | None
    try:
        rating = float(rating_raw) if rating_raw is not None else None
    except (TypeError, ValueError):
        rating = None
    try:
        review_count = int(count_raw) if count_raw is not None else None
    except (TypeError, ValueError):
        review_count = None
    if rating is not None and not (0 <= rating <= 5):
        rating = None
    if review_count is not None and review_count < 0:
        review_count = None
    return rating, review_count


def rating_from_place_dict(place: dict[str, Any]) -> GooglePlaceRating | None:
    pid = _normalize_place_id(place.get("id") or "")
    if not pid:
        return None
    rating, review_count = _parse_rating_fields(place)
    return GooglePlaceRating(place_id=pid, rating=rating, review_count=review_count)


def fetch_place_rating_by_id(
    place_id: str,
    *,
    api_key: str | None = None,
    session: requests.Session | None = None,
    timeout_s: float = 15.0,
) -> GooglePlaceRating | None:
    """Fetch rating for a known Google place id (Place Details)."""
    pid = _normalize_place_id(place_id)
    if not pid:
        return None
    key = api_key or google_maps_api_key()
    if not key:
        logger.debug("Google place rating skipped — no GOOGLE_MAPS_API_KEY")
        return None

    sess = session or requests.Session()
    url = f"https://places.googleapis.com/v1/places/{pid}"
    headers = {
        "X-Goog-Api-Key": key,
        "X-Goog-FieldMask": PLACE_DETAIL_FIELD_MASK,
    }
    try:
        r = sess.get(url, headers=headers, timeout=timeout_s)
        r.raise_for_status()
        data = r.json()
    except (requests.RequestException, ValueError) as e:
        logger.warning("Google place details failed for %s: %s", pid, e)
        return None
    return rating_from_place_dict(data)


def search_place_rating(
    name: str,
    city: str,
    state: str,
    *,
    latitude: float | None = None,
    longitude: float | None = None,
    api_key: str | None = None,
    session: requests.Session | None = None,
    timeout_s: float = 15.0,
) -> GooglePlaceRating | None:
    """Resolve a dealership via text search and return cached rating fields."""
    name = (name or "").strip()
    city = (city or "").strip()
    state = (state or "").strip().upper()
    if not name:
        return None

    key = api_key or google_maps_api_key()
    if not key:
        logger.debug("Google place rating search skipped — no GOOGLE_MAPS_API_KEY")
        return None

    query_parts = [name, city, state, "car dealer"]
    text_query = " ".join(p for p in query_parts if p).strip()
    body: dict[str, Any] = {
        "textQuery": text_query,
        "includedType": "car_dealer",
        "maxResultCount": 1,
    }
    if latitude is not None and longitude is not None:
        body["locationBias"] = {
            "circle": {
                "center": {"latitude": float(latitude), "longitude": float(longitude)},
                "radius": 5000.0,
            }
        }

    sess = session or requests.Session()
    headers = {
        "X-Goog-Api-Key": key,
        "X-Goog-FieldMask": SEARCH_FIELD_MASK,
        "Content-Type": "application/json",
    }
    try:
        r = sess.post(SEARCH_TEXT_URL, headers=headers, json=body, timeout=timeout_s)
        r.raise_for_status()
        data = r.json()
    except (requests.RequestException, ValueError) as e:
        logger.warning("Google place rating search failed for %r: %s", text_query, e)
        return None

    places = data.get("places") or []
    if not places:
        return None
    return rating_from_place_dict(places[0])
