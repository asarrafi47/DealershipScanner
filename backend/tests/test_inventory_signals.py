"""Unit tests for backend.intelligence.inventory_signals (pure functions only)."""

import json
from datetime import datetime, timezone

from backend.intelligence import inventory_signals as sig

NOW = datetime(2026, 7, 18, tzinfo=timezone.utc)


def _car(**kw):
    return kw


# --- days_on_lot / aging_bucket ------------------------------------------------
def test_days_on_lot_basic():
    car = _car(first_seen_at="2026-07-01T00:00:00Z")
    assert sig.days_on_lot(car, now=NOW) == 17


def test_days_on_lot_missing():
    assert sig.days_on_lot(_car(), now=NOW) is None
    assert sig.days_on_lot(_car(first_seen_at=""), now=NOW) is None
    assert sig.days_on_lot(_car(first_seen_at="not-a-date"), now=NOW) is None


def test_days_on_lot_future_clamps_to_zero():
    car = _car(first_seen_at="2026-07-25T00:00:00Z")
    assert sig.days_on_lot(car, now=NOW) == 0


def test_aging_bucket_boundaries():
    def b(days):
        first = NOW.timestamp() - days * 86400
        car = _car(first_seen_at=datetime.fromtimestamp(first, tz=timezone.utc).isoformat())
        return sig.aging_bucket(car, now=NOW)

    assert b(0) == "fresh"
    assert b(13) == "fresh"
    assert b(14) == "normal"
    assert b(60) == "normal"
    assert b(61) == "stale"
    assert b(90) == "stale"
    assert b(91) == "very_stale"
    assert b(400) == "very_stale"


def test_aging_bucket_unknown():
    assert sig.aging_bucket(_car(), now=NOW) is None
    assert sig.is_stale(_car(first_seen_at="2026-01-01T00:00:00Z"), now=NOW) is True
    assert sig.is_stale(_car(first_seen_at="2026-07-10T00:00:00Z"), now=NOW) is False


# --- price history / drops -----------------------------------------------------
def _prov(*pairs):
    return json.dumps([{"date": d, "price": p} for d, p in pairs])


def test_recent_price_drop_detected():
    car = _car(
        price_provenance_json=_prov(
            ("2026-06-01T00:00:00Z", 30000),
            ("2026-07-10T00:00:00Z", 27000),
        )
    )
    info = sig.recent_price_drop(car, now=NOW)
    assert info["dropped"] is True
    assert info["from_price"] == 30000
    assert info["to_price"] == 27000
    assert info["drop_amount"] == 3000
    assert info["drop_pct"] == 10.0
    assert info["days_ago"] == 8
    assert info["is_recent"] is True
    assert info["num_drops"] == 1
    assert info["peak_price"] == 30000
    assert info["total_drop_from_peak"] == 3000


def test_price_increase_is_not_a_drop():
    car = _car(
        price_provenance_json=_prov(
            ("2026-06-01T00:00:00Z", 20000),
            ("2026-07-10T00:00:00Z", 25000),
        )
    )
    info = sig.recent_price_drop(car, now=NOW)
    assert info["dropped"] is False
    assert info["num_drops"] == 0


def test_multiple_moves_reports_latest_drop_and_counts_all():
    car = _car(
        price_provenance_json=_prov(
            ("2026-06-01T00:00:00Z", 30000),
            ("2026-06-10T00:00:00Z", 28000),  # drop 1
            ("2026-06-20T00:00:00Z", 29000),  # up
            ("2026-07-01T00:00:00Z", 26000),  # drop 2 (latest)
        )
    )
    info = sig.recent_price_drop(car, now=NOW)
    assert info["num_drops"] == 2
    assert info["from_price"] == 29000
    assert info["to_price"] == 26000
    assert info["when"] == "2026-07-01"
    assert info["peak_price"] == 30000


def test_not_recent_drop():
    car = _car(
        price_provenance_json=_prov(
            ("2026-05-01T00:00:00Z", 30000),
            ("2026-05-10T00:00:00Z", 27000),
        )
    )
    info = sig.recent_price_drop(car, now=NOW, recent_within_days=30)
    assert info["dropped"] is True
    assert info["is_recent"] is False


def test_single_or_no_history():
    assert sig.recent_price_drop(_car(), now=NOW)["dropped"] is False
    one = _car(price_provenance_json=_prov(("2026-07-01T00:00:00Z", 25000)))
    assert sig.recent_price_drop(one, now=NOW)["dropped"] is False
    assert sig.recent_price_drop(_car(price_provenance_json="garbage"), now=NOW)["dropped"] is False


def test_price_history_filters_bad_entries():
    car = _car(
        price_provenance_json=_prov(
            ("2026-06-01T00:00:00Z", 0),  # non-positive -> dropped
            ("bad-date", 25000),          # bad date -> dropped
            ("2026-07-01T00:00:00Z", 24000),
        )
    )
    hist = sig.price_history(car)
    assert len(hist) == 1
    assert hist[0][1] == 24000


# --- aggregate helpers ---------------------------------------------------------
def test_aging_distribution_and_stale_share():
    cars = [
        _car(first_seen_at="2026-07-15T00:00:00Z", dealer_name="A"),   # fresh (3d)
        _car(first_seen_at="2026-06-20T00:00:00Z", dealer_name="A"),   # normal (28d)
        _car(first_seen_at="2026-05-01T00:00:00Z", dealer_name="B"),   # stale (78d)
        _car(first_seen_at="2026-01-01T00:00:00Z", dealer_name="B"),   # very_stale
        _car(dealer_name="B"),                                          # unknown
    ]
    dist = sig.aging_distribution(cars, now=NOW)
    assert dist["total"] == 5
    assert dist["buckets"]["fresh"] == 1
    assert dist["buckets"]["normal"] == 1
    assert dist["buckets"]["stale"] == 1
    assert dist["buckets"]["very_stale"] == 1
    assert dist["buckets"]["unknown"] == 1
    assert dist["stale_share"] == round(2 / 5, 4)


def test_stale_share_by_dealer_respects_min_inventory():
    cars = [_car(first_seen_at="2026-01-01T00:00:00Z", dealer_name="Big") for _ in range(12)]
    cars += [_car(first_seen_at="2026-07-15T00:00:00Z", dealer_name="Big") for _ in range(12)]
    cars += [_car(first_seen_at="2026-01-01T00:00:00Z", dealer_name="Tiny")]
    rows = sig.stale_share_by_dealer(cars, now=NOW, min_inventory=10)
    assert [r["dealer"] for r in rows] == ["Big"]
    big = rows[0]
    assert big["total"] == 24
    assert big["stale"] == 12
    assert big["stale_share"] == 0.5


def test_top_price_drops_sorted_by_pct():
    cars = [
        _car(vin="X", price_provenance_json=_prov(("2026-07-01T00:00:00Z", 20000), ("2026-07-10T00:00:00Z", 19000))),  # 5%
        _car(vin="Y", price_provenance_json=_prov(("2026-07-01T00:00:00Z", 40000), ("2026-07-10T00:00:00Z", 30000))),  # 25%
    ]
    top = sig.top_price_drops(cars, now=NOW, limit=5)
    assert [r["vin"] for r in top] == ["Y", "X"]
    assert top[0]["drop_pct"] == 25.0


def test_turn_time_and_fastest_turning_models():
    removed = []
    # Corolla: fast (median ~5d)
    for _ in range(15):
        removed.append(_car(make="Toyota", model="Corolla",
                            first_seen_at="2026-06-01T00:00:00Z",
                            listing_removed_at="2026-06-06T00:00:00Z"))
    # Tundra: slow (median ~40d)
    for _ in range(15):
        removed.append(_car(make="Toyota", model="Tundra",
                            first_seen_at="2026-06-01T00:00:00Z",
                            listing_removed_at="2026-07-11T00:00:00Z"))
    # too few to rank
    removed.append(_car(make="Toyota", model="Rare",
                        first_seen_at="2026-06-01T00:00:00Z",
                        listing_removed_at="2026-06-02T00:00:00Z"))

    assert sig.turn_time_days(removed[0]) == 5
    fastest = sig.fastest_turning_models(removed, min_count=15, limit=5)
    assert fastest[0]["model"] == "Corolla"
    assert fastest[0]["median_turn_days"] == 5
    assert all(r["model"] != "Rare" for r in fastest)
    slowest = sig.fastest_turning_models(removed, min_count=15, limit=5, slowest=True)
    assert slowest[0]["model"] == "Tundra"


def test_turn_time_negative_is_none():
    assert sig.turn_time_days(
        _car(first_seen_at="2026-07-10T00:00:00Z", listing_removed_at="2026-07-01T00:00:00Z")
    ) is None
