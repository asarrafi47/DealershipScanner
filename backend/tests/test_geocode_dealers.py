"""Geocoding must be anchored to the dealer, never to their name alone."""

import json

import pytest

from backend.scripts import geocode_dealers as gd

# Torrance CA — where South Bay BMW actually is.
TORRANCE = {"lat": 33.8604, "lon": -118.3514, "zip_code": "90504",
            "city": "Torrance", "state": "CA", "source": "feed_zip"}
SF_LAT, SF_LON = 37.7761, -122.4020  # where the name-only lookup put it


def _nominatim(lat, lon, city="Somewhere", state="CA"):
    return {"lat": str(lat), "lon": str(lon),
            "address": {"city": city, "state": state, "postcode": "94102"}}


def test_nominatim_hit_in_another_metro_is_rejected(monkeypatch):
    """The South Bay BMW regression: same name, 389 miles away."""
    monkeypatch.setattr(gd, "_nominatim_search", lambda q: _nominatim(SF_LAT, SF_LON))
    monkeypatch.setattr(gd.time, "sleep", lambda _s: None)

    assert gd._nominatim_refine("South Bay BMW", "https://southbaybmw.com", TORRANCE) is None


def test_nearby_nominatim_hit_refines_the_centroid(monkeypatch):
    """A hit near the dealer's own ZIP is the street address — keep it."""
    monkeypatch.setattr(gd, "_nominatim_search",
                        lambda q: _nominatim(33.8650, -118.3490, "Torrance", "CA"))
    monkeypatch.setattr(gd.time, "sleep", lambda _s: None)

    out = gd._nominatim_refine("South Bay BMW", "https://southbaybmw.com", TORRANCE)
    assert out is not None
    assert out["source"] == "nominatim_corroborated"
    assert out["lat"] == pytest.approx(33.8650)


def test_unanchored_dealer_is_not_guessed(monkeypatch):
    """No anchor means no coordinates — a gap beats a wrong metro."""
    monkeypatch.setattr(gd, "_google_places_geocode", lambda *a: None)
    monkeypatch.setattr(gd, "_feed_zip_geocode", lambda *a: None)
    monkeypatch.setattr(gd, "_nominatim_search", lambda q: _nominatim(SF_LAT, SF_LON))
    monkeypatch.setattr(gd.time, "sleep", lambda _s: None)

    assert gd.geocode_dealer("Lexus", "https://lexusofmissionviejo.com") is None


def test_places_hit_requires_matching_website_host(monkeypatch):
    """websiteUri is the identity check; a name match alone is not enough."""
    monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "test-key")
    monkeypatch.setattr(gd.time, "sleep", lambda _s: None)
    place = {
        "websiteUri": "https://www.some-other-lexus.com",
        "location": {"latitude": SF_LAT, "longitude": SF_LON},
        "addressComponents": [],
    }
    monkeypatch.setattr(gd, "_places_search", lambda q, k: [place])
    assert gd._google_places_geocode("Lexus", "https://www.lexusofmissionviejo.com") is None

    place["websiteUri"] = "http://lexusofmissionviejo.com/"  # scheme/www differ
    got = gd._google_places_geocode("Lexus", "https://www.lexusofmissionviejo.com")
    assert got is not None and got["source"] == "google_places"


def test_group_feed_zips_are_not_collapsed_to_one_lot(monkeypatch):
    """A group feed carries several lot ZIPs — picking one invents a location."""
    body = json.dumps({"vehicles": [
        {"zip": "90504"}, {"zip": "92008"}, {"zip": "85014"}, {"zip": "37421"},
    ]})
    import backend.scanner.recipes as recipes

    calls = []

    async def fake_fetch(*a, **k):
        calls.append(a)
        return ([("https://feed", body)], {})

    monkeypatch.setattr(recipes, "try_fetch_via_recipes", fake_fetch)
    assert gd._feed_zip_geocode("somedealer", "https://somedealer.com") is None
    assert calls, "guard must reject the mixed ZIPs, not skip the fetch entirely"


def test_single_lot_feed_zip_is_accepted(monkeypatch):
    """One distinct ZIP across the feed is unambiguous — that is the lot."""
    body = json.dumps({"vehicles": [{"dealerZip": "90504"}] * 4})
    import backend.scanner.recipes as recipes

    async def fake_fetch(*a, **k):
        return ([("https://feed", body)], {})

    monkeypatch.setattr(recipes, "try_fetch_via_recipes", fake_fetch)
    out = gd._feed_zip_geocode("somedealer", "https://somedealer.com", "Some Dealer")
    assert out is not None
    assert out["zip_code"] == "90504"
    assert out["source"] == "feed_zip"


def test_group_feed_uses_this_dealers_rows_not_the_majority(monkeypatch):
    """
    The Nissan of Costa Mesa regression: seven rooftops on one feed, and this
    dealer owns the *rarest* ZIP. A majority vote picks a store 60mi away.
    """
    body = json.dumps({"vehicles":
        [{"dealerName": "Nissan of Mission Hills", "dealerCity": "Mission Hills",
          "dealerState": "CA", "dealerZip": "91345"}] * 15
        + [{"dealerName": "Carson Nissan", "dealerCity": "Carson",
            "dealerState": "CA", "dealerZip": "90745"}] * 10
        + [{"dealerName": "Nissan of Costa Mesa", "dealerCity": "Costa Mesa",
            "dealerState": "CA", "dealerZip": "92626"}] * 3
    })
    import backend.scanner.recipes as recipes

    async def fake_fetch(*a, **k):
        return ([("https://feed", body)], {})

    monkeypatch.setattr(recipes, "try_fetch_via_recipes", fake_fetch)
    out = gd._feed_zip_geocode(
        "nissanofcostamesa-com", "https://www.nissanofcostamesa.com",
        "Nissan of Costa Mesa",
    )
    assert out is not None
    assert out["zip_code"] == "92626", "must not take the 15-row Mission Hills majority"


def test_source_rank_orders_anchors_by_strength():
    rank = gd._SOURCE_RANK
    assert rank["google_places"] > rank["registry"] > rank["nominatim_corroborated"]
    assert rank["nominatim_corroborated"] > rank["feed_zip"]
    # An unranked source is untrusted and must never outrank a real anchor.
    assert rank.get("nominatim", 0) == 0
