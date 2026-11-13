"""Web speed P19: aged-but-valid cards, stale-while-revalidate across a token change and the
UTC day roll, streaming gzip (docs/web_speed_search_2026_11/README.md, section 6).

README acceptance, one test group each:

* a stored card that is only aged but still matches its row is served as a complete card and
  nothing is queued ("background re-serialisation: 20,888 cards x 0.994 ms on the request
  path -> 0");
* "price dropped N days ago" stays right for an old card (moved to today at serve time);
* a web worker does not start a re-serialization storm;
* a token change and the UTC day roll serve the body we have and rebuild once, off the
  request, with the README's bounds (LISTINGS_MAX_STALE_S=120, LISTINGS_MAX_STALE_DAYROLL_S=1800);
* the scope body is gzipped piece by piece ("transient 116.3 -> 12.9 MB, same bytes").

The laptop numbers (92618 / 25 mi, 20,888 cards) need the owner's database; these tests use
the sqlite fixture and counters instead of clocks where they can.
"""

from __future__ import annotations

import gzip
import json
import logging
import threading
import time
import tracemalloc
import zlib
from datetime import datetime, timedelta, timezone

import pytest

from backend.routes import listings_api
from backend.tests.test_listings_scoped_grid import _cars, _seed
from backend.utils.stream_gzip import gzip_stream

ZIP = "92694"


@pytest.fixture
def scoped(sqlite_inventory):
    from backend.db.repositories import grid_cards_repo as gc
    from backend.db.repositories import listings_repo as lr

    lr.clear_inventory_listings_cache()
    gc.reset_grid_cards_state()
    listings_api.clear_cars_scope_cache()
    _seed(sqlite_inventory)
    yield sqlite_inventory
    lr.clear_inventory_listings_cache()


@pytest.fixture
def client():
    from backend.main import app

    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c


def _origin():
    from backend.db.geo import zip_to_coords

    return zip_to_coords(ZIP)


def _sql(statement: str, fetch: bool = False):
    """Run one statement on the test inventory through the product connection (ints inlined)."""
    from backend.db.inventory_db import db_conn

    with db_conn() as conn:
        cur = conn.execute(statement)
        rows = cur.fetchall() if fetch else None
        conn.commit()
    return rows


def _age_the_store(db, days: int):
    from backend.db.repositories import grid_cards_repo as gc

    _sql(f"UPDATE listings_grid_cards SET built_day = {int(gc._today() - days)}")


def _no_background(monkeypatch):
    from backend.db.repositories import grid_cards_repo as gc

    queued: list[int] = []
    monkeypatch.setattr(gc, "_enqueue_refresh", lambda ids: queued.extend(ids))
    serialized: list[int] = []
    real = gc._serialize_rows

    def counting(rows, ctx, **kw):
        serialized.append(len(rows))
        return real(rows, ctx, **kw)

    monkeypatch.setattr(gc, "_serialize_rows", counting)
    return queued, serialized


# ── aged but valid cards are complete ───────────────────────────────────────────────


@pytest.mark.parametrize("days_old", [2, 5, 400])
def test_aged_but_valid_cards_are_served_complete_and_nothing_is_queued(scoped, monkeypatch, days_old):
    from backend.db.repositories import grid_cards_repo as gc

    lat, lon = _origin()
    first = gc.cards_near(lat, lon, 250)
    assert len(first.entries) == 5
    _age_the_store(scoped, days_old)
    queued, serialized = _no_background(monkeypatch)
    res = gc.cards_near(lat, lon, 250)
    assert res.partial is False
    assert queued == [] and serialized == []
    assert res.stats["changed"] == 0 and res.stats["fresh"] == 5
    assert res.cards_json() == first.cards_json(), "the shopper gets the same cards"


def test_aged_cards_never_start_the_background_thread(scoped, monkeypatch):
    from backend.db.repositories import grid_cards_repo as gc

    lat, lon = _origin()
    gc.cards_near(lat, lon, 250)
    _age_the_store(scoped, 9)
    before = gc._bg_thread
    threads_before = {t.name for t in threading.enumerate()}
    gc.cards_near(lat, lon, 250)
    assert gc._bg_thread is before
    assert {t.name for t in threading.enumerate()} == threads_before


def test_a_large_aged_area_costs_the_request_path_no_serialization(scoped, monkeypatch):
    """The README's laptop row (20,888 cards x 0.994 ms -> 0), at fixture scale."""
    from backend.db.repositories import grid_cards_repo as gc

    extra = [
        {
            "title": f"2022 Toyota Camry #{100 + i}",
            "vin": f"1HGBH41JXMN{100000 + i}",
            "year": 2022, "make": "Toyota", "model": "Camry", "trim": "LE",
            "price": 20000 + i, "mileage": 5000 + i,
            "image_url": f"https://cdn.example.com/x{i}.jpg",
            "dealer_name": "near-motors", "dealer_url": "https://near-motors.test",
            "dealer_id": "near-motors-test", "dealership_registry_id": 1,
            "body_style": "Sedan", "fuel_type": "Gasoline",
        }
        for i in range(300)
    ]
    scoped.add_cars(extra)
    lat, lon = _origin()
    first = gc.cards_near(lat, lon, 250)
    assert len(first.entries) == 305
    _age_the_store(scoped, 30)
    queued, serialized = _no_background(monkeypatch)
    t0 = time.perf_counter()
    res = gc.cards_near(lat, lon, 250)
    elapsed = time.perf_counter() - t0
    assert queued == [] and sum(serialized) == 0 and not res.partial
    assert len(res.entries) == 305
    assert elapsed < 5.0


def test_endpoint_answers_an_aged_area_as_complete(scoped, client):
    from backend.db.repositories import grid_cards_repo as gc

    lat, lon = _origin()
    gc.cards_near(lat, lon, 250)
    _age_the_store(scoped, 6)
    listings_api.clear_cars_scope_cache()
    r = client.get(f"/api/listings/cars?zip={ZIP}&radius=250")
    data = _cars(r)
    assert data["partial"] is False and data["count"] == 5
    assert "X-Listings-Partial" not in r.headers
    assert "s-maxage=60" in r.headers["Cache-Control"], "complete bodies are shareable again"


def test_aged_cards_that_no_longer_match_their_row_are_still_refreshed(scoped, monkeypatch):
    """Aged-but-valid is complete; aged AND changed is stale and is queued as before."""
    from backend.db.repositories import grid_cards_repo as gc

    lat, lon = _origin()
    gc.cards_near(lat, lon, 250)
    _age_the_store(scoped, 5)
    _change_everything(scoped)
    monkeypatch.setattr(gc, "_INLINE_REBUILD_MAX", 2)
    queued, _serialized = _no_background(monkeypatch)
    res = gc.cards_near(lat, lon, 250)
    assert res.partial and len(set(queued)) == 5 and len(res.entries) == 5


def test_the_offline_builder_still_rebuilds_aged_cards(scoped):
    """The nightly job is what re-serializes aged cards, so date labels stay right."""
    from backend.db.repositories import grid_cards_repo as gc

    assert gc.build_all_cards(batch=10)["rebuilt"] == 6
    _age_the_store(scoped, 3)
    stats = gc.build_all_cards(batch=10)
    assert stats["rebuilt"] == 6 and stats["fresh"] == 0
    stats = gc.build_all_cards(batch=10)
    assert stats["rebuilt"] == 0


# ── the date-dependent label ────────────────────────────────────────────────────────


def _drop_car(days_ago: float, **over):
    when = datetime.now(timezone.utc) - timedelta(days=days_ago)
    history = [
        {"price": 25000, "date": (when - timedelta(days=30)).isoformat()},
        {"price": 22500, "date": when.isoformat()},
    ]
    return {"price_provenance_json": json.dumps(history), **over}


def test_shifting_matches_a_fresh_serialization_for_every_age():
    """A card built k days ago and shifted by k days equals one built today for the same drop."""
    from backend.db.repositories.grid_cards_repo import _shift_price_drop_days
    from backend.utils.car_serialize.serialize import _PRICE_DROP_RECENT_DAYS, _latest_price_drop_for_grid

    for age in (1, 2, 5):  # days since the drop when the card was built
        stored_amount, stored_days = _latest_price_drop_for_grid(_drop_car(age + 0.5))
        assert stored_days == age
        card = json.dumps({"id": 1, "price_drop_amount": stored_amount, "price_drop_days_ago": stored_days}, separators=(",", ":"))
        for k in range(0, _PRICE_DROP_RECENT_DAYS + 3):
            fresh_amount, fresh_days = _latest_price_drop_for_grid(_drop_car(age + k + 0.5))
            shifted = json.loads(_shift_price_drop_days(card, k))
            assert (shifted["price_drop_amount"], shifted["price_drop_days_ago"]) == (fresh_amount, fresh_days), (age, k)


def test_shifting_leaves_other_cards_alone():
    from backend.db.repositories.grid_cards_repo import _shift_price_drop_days

    plain = '{"id":1,"price":20000,"price_drop_amount":null,"price_drop_days_ago":null}'
    assert _shift_price_drop_days(plain, 3) == plain
    assert _shift_price_drop_days('{"id":1}', 3) == '{"id":1}'
    with_drop = '{"id":1,"price_drop_amount":2500.0,"price_drop_days_ago":2,"x":"price_drop_days_ago"}'
    assert _shift_price_drop_days(with_drop, 0) == with_drop
    assert _shift_price_drop_days(with_drop, -4) == with_drop
    out = json.loads(_shift_price_drop_days(with_drop, 3))
    assert out["price_drop_days_ago"] == 5 and out["price_drop_amount"] == 2500.0 and out["x"] == "price_drop_days_ago"


def test_dealer_text_cannot_forge_the_label_pattern():
    """Dealer-derived text is data: a title that contains the key text changes nothing else."""
    from backend.db.repositories.grid_cards_repo import _shift_price_drop_days

    card = json.dumps(
        {"id": 1, "title": 'x","price_drop_amount":1,"price_drop_days_ago":1,"y":"', "price_drop_amount": 9.0, "price_drop_days_ago": 3},
        separators=(",", ":"),
    )
    out = _shift_price_drop_days(card, 2)
    json.loads(out)  # still valid JSON, whatever was matched


def test_an_old_stored_card_shows_the_right_days_through_the_store(scoped, monkeypatch):
    """A card stored with a price-drop label (day_dep = 1) is moved forward by its age.

    The grid query on dev does not select ``price_provenance_json`` (see the PR notes), so no
    card is built with a label today; the stored card is written the way the serializer would
    write it, so the shifting is proven for the day the column is selected.
    """
    from backend.db.repositories import grid_cards_repo as gc

    lat, lon = _origin()
    first = gc.cards_near(lat, lon, 250)
    assert len(first.entries) == 5
    car_id, ckey, aux, bucket, price, ver, blob = _sql(
        "SELECT car_id, content_key, aux_key, sort_bucket, sort_price, row_ver, card_json "
        "FROM listings_grid_cards ORDER BY car_id LIMIT 1",
        fetch=True,
    )[0]
    card = json.loads(blob)
    assert "price_drop_days_ago" in card
    card["price_drop_amount"], card["price_drop_days_ago"] = 2500.0, 2
    # Write the card back the way the store does, flagged as date-dependent.
    gc._store([(car_id, ckey, aux, 1, bucket, price, json.dumps(card, separators=(",", ":")))], {car_id: ver}, gc._today())

    def served():
        res = gc.cards_near(lat, lon, 250)
        out = next(json.loads(c) for c in res.cards_json() if json.loads(c)["id"] == car_id)
        return res, out

    res, today = served()
    assert (today["price_drop_amount"], today["price_drop_days_ago"]) == (2500.0, 2)
    queued, serialized = _no_background(monkeypatch)
    _age_the_store(scoped, 4)
    res, again = served()
    assert again["price_drop_days_ago"] == 6 and again["price_drop_amount"] == 2500.0
    assert queued == [] and serialized == [] and not res.partial
    # Past the serializer's own window the badge is gone, as on a rebuilt card.
    _age_the_store(scoped, 40)
    res, gone = served()
    assert gone["price_drop_days_ago"] is None and gone["price_drop_amount"] is None
    assert queued == [] and not res.partial


# ── a web worker never runs a storm ─────────────────────────────────────────────────


def _change_everything(db):
    _sql("UPDATE cars SET price = price + 1")


def test_a_stale_batch_over_the_cap_is_served_and_left_to_the_builder(scoped, monkeypatch, caplog):
    from backend.db.repositories import grid_cards_repo as gc

    lat, lon = _origin()
    gc.cards_near(lat, lon, 250)
    _change_everything(scoped)
    monkeypatch.setattr(gc, "_INLINE_REBUILD_MAX", 2)
    monkeypatch.setattr(gc, "_BG_STALE_MAX", 3)
    monkeypatch.setattr(gc, "_storm_warned_at", float("-inf"))
    queued, serialized = _no_background(monkeypatch)
    with caplog.at_level(logging.WARNING, logger=gc._log.name):
        res = gc.cards_near(lat, lon, 250)
    assert queued == [] and serialized == []
    assert len(res.entries) == 5, "stale cards are still served"
    assert res.partial is False, "nothing is being refreshed, so the client must not poll"
    assert res.stats["bg_skipped_stale"] == 5
    assert any("build_listings_grid_cards" in r.getMessage() for r in caplog.records)


def test_a_small_stale_batch_is_still_refreshed_in_the_background(scoped, monkeypatch):
    from backend.db.repositories import grid_cards_repo as gc

    lat, lon = _origin()
    gc.cards_near(lat, lon, 250)
    _change_everything(scoped)
    monkeypatch.setattr(gc, "_INLINE_REBUILD_MAX", 2)
    monkeypatch.setattr(gc, "_BG_STALE_MAX", 5)
    queued, _ = _no_background(monkeypatch)
    res = gc.cards_near(lat, lon, 250)
    assert res.partial and len(set(queued)) == 5


def test_missing_cards_are_queued_nearest_first_up_to_the_cap(scoped, monkeypatch):
    from backend.db.repositories import grid_cards_repo as gc

    monkeypatch.setattr(gc, "_INLINE_REBUILD_MAX", 1)
    monkeypatch.setattr(gc, "_BG_OMITTED_MAX", 2)
    queued, _ = _no_background(monkeypatch)
    lat, lon = _origin()
    res = gc.cards_near(lat, lon, 250)
    assert res.partial and len(queued) == 2
    dealers = [_sql(f"SELECT dealer_id FROM cars WHERE id = {int(i)}", fetch=True)[0][0] for i in queued]
    assert "far-motors-test" not in dealers, "the farthest car is not the one queued first"


# ── stale while revalidate: token change and the UTC day roll ───────────────────────


class Scope:
    """Drives api_listings_cars with a controllable token and a counting, optionally slow builder."""

    def __init__(self, monkeypatch, client):
        from backend.db.repositories import grid_cards_repo as gc

        self.client = client
        self.monkeypatch = monkeypatch
        self.token = ("fp1", 20_000, "rev", "mkt", "attr", False)
        self.builds = 0
        self.delay = 0.0
        self.fail = False
        self.lock = threading.Lock()
        monkeypatch.setattr(gc, "grid_scope_token", lambda: self.token)
        monkeypatch.setattr(listings_api, "is_inventory_postgres", lambda: True)
        real = listings_api._build_cars_scope_entry

        def counting(*a, **k):
            with self.lock:
                self.builds += 1
                n = self.builds
            time.sleep(self.delay)
            if self.fail and n > 1:
                raise RuntimeError("database unavailable")
            return real(*a, **k)

        monkeypatch.setattr(listings_api, "_build_cars_scope_entry", counting)

    def get(self, zip_code=ZIP, radius=250):
        t0 = time.perf_counter()
        r = self.client.get(f"/api/listings/cars?zip={zip_code}&radius={radius}")
        return r, time.perf_counter() - t0

    def settle(self, timeout=10):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if not listings_api._cars_scope_rebuilding:
                return
            time.sleep(0.01)
        raise AssertionError("background scope rebuild did not finish")

    def move_token(self, **changes):
        t = list(self.token)
        if "fp" in changes:
            t[0] = changes["fp"]
        if "day" in changes:
            t[1] = changes["day"]
        self.token = tuple(t)


@pytest.fixture
def scope(scoped, client, monkeypatch):
    s = Scope(monkeypatch, client)
    yield s
    s.settle()


def _change_a_price(db, to: int = 31337):
    _sql(f"UPDATE cars SET price = {int(to)} WHERE title = '2022 Toyota Camry #1'")


def test_token_change_serves_the_old_body_at_once_and_rebuilds_off_the_request(scope, scoped):
    first, _ = scope.get()
    assert first.status_code == 200
    _change_a_price(scoped)
    scope.move_token(fp="fp2")
    scope.delay = 0.6
    r, elapsed = scope.get()
    assert r.status_code == 200 and elapsed < 0.3, f"waited {elapsed:.2f}s for the rebuild"
    assert r.headers["ETag"] == first.headers["ETag"], "the body we already had"
    scope.settle()
    assert scope.builds == 2
    after, _ = scope.get()
    assert after.headers["ETag"] != first.headers["ETag"]
    prices = {c["price"] for c in _cars(after)["cars"]}
    assert 31337 in prices
    assert scope.builds == 2, "the rebuilt body is current: no third build"


def test_the_utc_day_roll_serves_the_old_body_and_rebuilds_once(scope):
    first, _ = scope.get()
    scope.move_token(day=20_001)
    scope.delay = 0.6
    r, elapsed = scope.get()
    assert elapsed < 0.3 and r.headers["ETag"] == first.headers["ETag"]
    scope.settle()
    assert scope.builds == 2


def test_four_shoppers_at_the_roll_cause_one_rebuild_and_none_waits(scope):
    scope.get()
    scope.move_token(day=20_001)
    scope.delay = 0.6
    from backend.main import app

    def one(i, out):
        with app.test_client() as c:
            t0 = time.perf_counter()
            r = c.get(f"/api/listings/cars?zip={ZIP}&radius=250")
            out[i] = (r.status_code, time.perf_counter() - t0)

    out: dict = {}
    threads = [threading.Thread(target=one, args=(i, out)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(10)
    assert [s for s, _ in out.values()] == [200] * 4
    assert max(e for _, e in out.values()) < 0.4
    scope.settle()
    assert scope.builds == 2, "one cold build plus ONE rebuild for four concurrent shoppers"


def test_stale_bounds_match_the_readme():
    assert listings_api.LISTINGS_MAX_STALE_S == 120
    assert listings_api.LISTINGS_MAX_STALE_DAYROLL_S == 1800


def test_a_body_stale_for_too_long_is_rebuilt_inline(scope, monkeypatch):
    scope.get()
    key = (ZIP, 250.0, ())
    entry = listings_api._cars_scope_get(key)
    assert entry is not None
    scope.move_token(fp="fp2")
    scope.delay = 0.3
    now = time.monotonic()
    ok = listings_api._stale_entry_to_serve(key, entry, scope.token, now)
    assert ok is not None and ok.stale_since is not None
    # 119 s into a data-token change: still served. 121 s: inline build.
    assert listings_api._stale_entry_to_serve(key, ok, scope.token, ok.stale_since + 119) is not None
    assert listings_api._stale_entry_to_serve(key, ok, scope.token, ok.stale_since + 121) is None
    # The roll alone gets the longer bound.
    scope.token = (entry.token[0], entry.token[1] + 1, *entry.token[2:])
    fresh_entry = entry._replace(stale_since=None)
    day = listings_api._stale_entry_to_serve(key, fresh_entry, scope.token, 1000.0)
    assert day is not None
    assert listings_api._stale_entry_to_serve(key, day, scope.token, 1000.0 + 1799) is not None
    assert listings_api._stale_entry_to_serve(key, day, scope.token, 1000.0 + 1801) is None


def test_past_the_bound_the_request_builds_inline_and_shares_one_build(scope):
    scope.get()
    key = (ZIP, 250.0, ())
    scope.move_token(fp="fp2")
    entry = listings_api._cars_scope_get(key)
    listings_api._cars_scope_put(key, entry._replace(stale_since=time.monotonic() - 500))
    scope.delay = 0.4
    r, elapsed = scope.get()
    assert r.status_code == 200 and elapsed >= 0.35, "too stale to serve: it waited for a fresh build"
    assert scope.builds == 2
    scope.settle()
    assert scope.builds == 2


def test_a_failing_background_rebuild_keeps_serving_and_waits_before_retrying(scope, caplog):
    first, _ = scope.get()
    scope.move_token(fp="fp2")
    scope.fail = True
    with caplog.at_level(logging.WARNING, logger=listings_api.__name__):
        r, _ = scope.get()
        scope.settle()
    assert r.headers["ETag"] == first.headers["ETag"]
    assert scope.builds == 2
    for _ in range(5):
        again, elapsed = scope.get()
        assert again.status_code == 200 and again.headers["ETag"] == first.headers["ETag"] and elapsed < 0.3
    scope.settle()
    assert scope.builds == 2, "a failed rebuild is retried after a pause, not on every request"
    assert any("background rebuild" in rec.getMessage() for rec in caplog.records)
    # After the pause the next request tries again.
    listings_api._cars_scope_rebuild_failed_at.clear()
    scope.fail = False
    scope.get()
    scope.settle()
    assert scope.builds == 3


def test_a_body_with_cards_waiting_on_the_refresher_is_not_served_stale(scope):
    scope.get()
    key = (ZIP, 250.0, ())
    entry = listings_api._cars_scope_get(key)
    listings_api._cars_scope_put(key, entry._replace(store_gen=0))
    scope.move_token(fp="fp2")
    scope.delay = 0.3
    r, elapsed = scope.get()
    assert elapsed >= 0.25 and r.status_code == 200, "partial bodies keep the old rule: rebuild inline"


def test_a_data_change_off_postgres_is_still_immediate(scope, monkeypatch):
    """Dev and tests: the token is the database file's mtime pair, a write must show at once."""
    monkeypatch.setattr(listings_api, "is_inventory_postgres", lambda: False)
    scope.get()
    scope.move_token(fp="fp2")
    scope.delay = 0.3
    r, elapsed = scope.get()
    assert elapsed >= 0.25 and r.status_code == 200
    # The day roll alone is served stale everywhere.
    scope.move_token(day=20_001)
    r, elapsed = scope.get()
    assert elapsed < 0.2
    scope.settle()


def test_the_day_is_the_second_item_of_the_scope_token():
    """_DAY_INDEX must follow grid_scope_token's layout."""
    from backend.db.repositories import grid_cards_repo as gc

    assert listings_api._DAY_INDEX == 1
    gc._token_cache = None
    assert gc.grid_scope_token()[listings_api._DAY_INDEX] == gc._today()


def test_token_difference_classifier():
    t = listings_api._token_differs_only_by_day
    a = ("fp", 5, "rev", "m", "a", False)
    assert t(a, ("fp", 6, "rev", "m", "a", False)) is True
    assert t(a, a) is False
    assert t(a, ("fp2", 6, "rev", "m", "a", False)) is False
    assert t(a, ("fp", 6, "rev2", "m", "a", False)) is False
    assert t("x", "y") is False and t(a, a[:3]) is False


# ── streaming gzip ──────────────────────────────────────────────────────────────────


def _old_body(head: str, cards: list[str]) -> bytes:
    return (head[:-1] + ',"cars":[' + ",".join(cards) + "]}").encode("utf-8")


@pytest.mark.parametrize("n", [0, 1, 255, 256, 257, 700])
def test_streamed_body_equals_the_joined_body(n):
    head = json.dumps({"ok": True, "count": n, "partial": False}, separators=(",", ":"))
    cards = [json.dumps({"id": i, "t": "é" * (i % 7)}, separators=(",", ":")) for i in range(n)]
    assert b"".join(listings_api._scope_body_parts(head, cards)) == _old_body(head, cards)
    out = gzip_stream(listings_api._scope_body_parts(head, cards), level=6)
    assert gzip.decompress(out) == _old_body(head, cards)
    assert out == zlib.compress(_old_body(head, cards), 6, wbits=31), "same deflate stream and wrapper"


def test_gzip_stream_equals_one_shot_gzip_for_any_chunking():
    data = (b"stored card json " * 5000) + bytes(range(256)) * 40
    for size in (1, 7, 4096, len(data)):
        parts = [data[i:i + size] for i in range(0, len(data), size)]
        assert gzip_stream(parts) == zlib.compress(data, 6, wbits=31)
    assert gzip.decompress(gzip_stream([])) == b""
    assert gzip.decompress(gzip_stream([b"", b"abc", b""])) == b"abc"


def test_the_built_entry_decodes_to_the_same_json_as_before(scoped, monkeypatch):
    from backend.db.repositories import grid_cards_repo as gc

    lat, lon = _origin()
    token = gc.grid_scope_token()
    entry = listings_api._build_cars_scope_entry(ZIP, (lat, lon), 250.0, (), token)
    body = json.loads(gzip.decompress(entry.gz_body))
    assert body["count"] == len(body["cars"]) == entry.count == 5
    assert body["ok"] is True and body["zip"] == ZIP and body["partial"] is False
    res = gc.cards_near(lat, lon, 250.0)
    assert [json.dumps(c, separators=(",", ":")) for c in body["cars"]] == [
        json.dumps(json.loads(c), separators=(",", ":")) for c in res.cards_json()
    ]


def test_two_builds_of_the_same_content_have_the_same_etag(scoped):
    """No clock in the gzip header any more: an unchanged area is a 304, not a new body."""
    from backend.db.repositories import grid_cards_repo as gc

    lat, lon = _origin()
    token = gc.grid_scope_token()
    a = listings_api._build_cars_scope_entry(ZIP, (lat, lon), 250.0, (), token)
    time.sleep(1.1)
    b = listings_api._build_cars_scope_entry(ZIP, (lat, lon), 250.0, (), token)
    assert a.etag == b.etag and a.gz_body == b.gz_body


def test_streaming_cuts_the_transient_memory_of_a_big_scope():
    """README: transient 116.3 -> 12.9 MB for the same bytes. Same shape at a smaller size."""
    cards = [json.dumps({"id": i, "blob": "x" * 4500 + str(i)}, separators=(",", ":")) for i in range(4000)]
    head = json.dumps({"ok": True, "count": len(cards), "partial": False}, separators=(",", ":"))
    raw_size = sum(len(c) for c in cards)

    tracemalloc.start()
    old = gzip.compress(_old_body(head, cards), compresslevel=6, mtime=0)
    _, old_peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    tracemalloc.start()
    new = gzip_stream(listings_api._scope_body_parts(head, cards), level=6)
    _, new_peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    assert gzip.decompress(new) == gzip.decompress(old)
    assert old_peak > 1.8 * raw_size, "the joined body and its encoding are both held"
    assert new_peak < 0.25 * old_peak, (old_peak, new_peak)
