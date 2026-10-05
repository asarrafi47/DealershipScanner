"""The photo-attribution verdicts, as the shopper sees them.

``car_attribution`` records where a car actually is, read off the listing's own
photographs. Only 144 cars could be physically re-filed; the rest have an
unregistered, group-level or ambiguous destination and cannot be moved at all. What
these tests guard is the thing that made the other 2,500 verdicts worth recording:
the site no longer states a dealership it has evidence against, and — the failure
mode that would be worse than the bug — it does not drop a single car to achieve it.
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager

import pytest

from backend.routes import home_dashboard as _home_dashboard_routes

_ATTRIBUTION_DDL = """
CREATE TABLE IF NOT EXISTS car_attribution (
    car_id           INTEGER PRIMARY KEY,
    status           TEXT,
    observed_rooftop TEXT,
    filed_dealer_id  TEXT,
    evidence         TEXT,
    decided_at       TEXT
);
CREATE TABLE IF NOT EXISTS dealer_feed_scope (
    dealer_id  TEXT PRIMARY KEY,
    scope      TEXT,
    decided_at TEXT
);
CREATE TABLE IF NOT EXISTS car_move_log (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    car_id         INTEGER,
    from_dealer_id TEXT,
    to_dealer_id   TEXT,
    reverted_at    TEXT
);
"""

# One car per case, so a failure names the rule it broke.
CARS = [
    # Photos read a plate frame from a different rooftop. The real BMW of Murrieta /
    # Rick Hendrick Naples shape.
    dict(id=1, title="2023 BMW X5 xDrive40i", make="BMW", model="X5", year=2023,
         price=61000, dealer_id="bmwofmurrieta-com", dealer_name="BMW of Murrieta",
         dealer_url="https://www.bmwofmurrieta.com", condition="Used"),
    # Photos name the filed store: nothing to say.
    dict(id=2, title="2022 BMW 330i", make="BMW", model="330i", year=2022,
         price=39000, dealer_id="bmwofmurrieta-com", dealer_name="BMW of Murrieta",
         dealer_url="https://www.bmwofmurrieta.com", condition="Used"),
    # No verdict either way, at a dealer proven to be served its whole group's feed.
    dict(id=3, title="2021 BMW X3", make="BMW", model="X3", year=2021, price=35000,
         dealer_id="bmwofmurrieta-com", dealer_name="BMW of Murrieta",
         dealer_url="https://www.bmwofmurrieta.com", condition="Used"),
    # Same "no evidence" verdict at a normal single-rooftop dealer: unchanged.
    dict(id=4, title="2020 Toyota Camry SE", make="Toyota", model="Camry", year=2020,
         price=21000, dealer_id="othertoyota-com", dealer_name="Other Toyota",
         dealer_url="https://www.othertoyota.com", condition="Used"),
    # No verdict at all — the ~99% of the fleet no photograph has judged.
    dict(id=5, title="2019 Honda Accord EX", make="Honda", model="Accord", year=2019,
         price=18000, dealer_id="othertoyota-com", dealer_name="Other Toyota",
         dealer_url="https://www.othertoyota.com", condition="Used"),
    # Already moved by apply_attribution_moves: cars.dealer_id is now the rooftop the
    # photos named, and the conflicting verdict still points at the OLD filing.
    dict(id=6, title="2023 Chevrolet Tahoe LT", make="Chevrolet", model="Tahoe",
         year=2023, price=64000, dealer_id="rickhendrickchevynaples-com",
         dealer_name="Rick Hendrick Chevrolet Naples",
         dealer_url="https://www.rickhendrickchevynaples.com", condition="Used"),
    # A re-scan changed dealer_id AFTER the verdict — no move was ever applied (its
    # only move-log row is reverted). V011 keeps filed_dealer_id precisely so this
    # drift does not make the row look consistent.
    dict(id=7, title="2022 Porsche Macan S", make="Porsche", model="Macan",
         year=2022, price=58000, dealer_id="hendrickporsche-com",
         dealer_name="Hendrick Porsche",
         dealer_url="https://www.hendrickporsche.com", condition="Used"),
]

VERDICTS = [
    (1, "conflicting", "Rick Hendrick Chevrolet Naples", "bmwofmurrieta-com"),
    (2, "confirmed", "BMW of Murrieta", "bmwofmurrieta-com"),
    (3, "unverified", None, "bmwofmurrieta-com"),
    (4, "unverified", None, "othertoyota-com"),
    (6, "conflicting", "Rick Hendrick Chevrolet Naples", "bmwofmurrieta-com"),
    (7, "conflicting", "Mall of Georgia Mazda", "bmwofmurrieta-com"),
]

SCOPES = [("bmwofmurrieta-com", "group"), ("othertoyota-com", "rooftop")]

# What apply_attribution_moves logged. Car 6's move stands; car 7's was reverted, so
# the dealer it sits at now is scan drift, not a resolved destination.
MOVES = [
    (6, "bmwofmurrieta-com", "rickhendrickchevynaples-com", None),
    (7, "bmwofmurrieta-com", "hendrickporsche-com", "2026-08-01T00:00:00Z"),
]


@pytest.fixture
def fleet(sqlite_inventory):
    """Seven cars, six verdicts and one group-fed dealer in an isolated SQLite db."""
    sqlite_inventory.add_cars([dict(c) for c in CARS])
    conn = sqlite3.connect(str(sqlite_inventory.path))
    try:
        conn.executescript(_ATTRIBUTION_DDL)
        conn.executemany(
            "INSERT INTO car_attribution (car_id, status, observed_rooftop, filed_dealer_id) "
            "VALUES (?,?,?,?)",
            VERDICTS,
        )
        conn.executemany("INSERT INTO dealer_feed_scope (dealer_id, scope) VALUES (?,?)", SCOPES)
        conn.executemany(
            "INSERT INTO car_move_log (car_id, from_dealer_id, to_dealer_id, reverted_at) "
            "VALUES (?,?,?,?)",
            MOVES,
        )
        conn.commit()
    finally:
        conn.close()

    from backend.db.repositories import listings_repo

    # The grid cache and its per-row serialize memo are module-level; a fleet left in
    # either of them is the next test's mystery failure.
    listings_repo.clear_inventory_listings_cache()
    yield sqlite_inventory
    listings_repo.clear_inventory_listings_cache()


def _states():
    from backend.db.repositories.cars_repo import car_attribution_states

    return car_attribution_states()


def test_conflicting_car_is_not_presented_as_confidently_located(fleet):
    """A photograph named another rooftop, so the filed dealership is not a fact."""
    from backend.utils.car_serialize import serialize_car_for_listings_grid

    card = serialize_car_for_listings_grid(dict(CARS[0]), attribution=_states()[1])

    assert card["location_confirmed"] is False
    # The dealer that published the listing is still named — that part is true.
    assert card["dealer_name"] == "BMW of Murrieta"
    # …and the shopper is told what the photos actually showed.
    assert card["observed_rooftop"] == "Rick Hendrick Chevrolet Naples"
    assert "Rick Hendrick Chevrolet Naples" in card["location_note"]


def test_confirmed_car_is_untouched(fleet):
    """Photos named the filed store: the card must be exactly what it was before."""
    from backend.utils.car_serialize import serialize_car_for_listings_grid

    card = serialize_car_for_listings_grid(dict(CARS[1]), attribution=_states()[2])
    plain = serialize_car_for_listings_grid(dict(CARS[1]))

    assert "location_confirmed" not in card
    assert "location_note" not in card
    assert card == plain


def test_no_verdict_at_a_normal_dealer_is_untouched(fleet):
    """``unverified`` is not evidence. Only a group-wide feed makes it doubt.

    Most of the fleet has no photo judgement, so caveating on absence of evidence
    would put "location unconfirmed" on tens of thousands of perfectly correct cards.
    """
    from backend.utils.car_serialize import serialize_car_for_listings_grid

    states = _states()
    unjudged = serialize_car_for_listings_grid(dict(CARS[4]), attribution=states.get(5))
    unverified_single_rooftop = serialize_car_for_listings_grid(
        dict(CARS[3]), attribution=states[4]
    )

    assert "location_confirmed" not in unjudged
    assert "location_confirmed" not in unverified_single_rooftop
    assert states[4]["location_unconfirmed"] is False


def test_unverified_at_a_group_fed_dealer_is_unconfirmed(fleet):
    """No evidence either way, but the feed carries the whole ownership group."""
    from backend.utils.car_serialize import serialize_car_for_listings_grid

    state = _states()[3]
    assert state["location_unconfirmed"] is True

    card = serialize_car_for_listings_grid(dict(CARS[2]), attribution=state)
    assert card["location_confirmed"] is False
    assert card["observed_rooftop"] is None
    assert "ownership group" in card["location_note"]


def test_already_moved_car_keeps_its_new_rooftop(fleet):
    """``apply_attribution_moves`` re-files the car but leaves the verdict alone.

    Reading ``status`` literally would put "location unconfirmed" on the 144 cars we
    have already fixed — the one group whose dealership we are now sure about.
    """
    state = _states()[6]
    assert state["status"] == "conflicting"
    assert state["location_unconfirmed"] is False


def test_rescan_drift_does_not_clear_the_caveat(fleet):
    """A re-scan changing dealer_id is not a deliberate move.

    V011 keeps ``filed_dealer_id`` "so a later re-scan that changes dealer_id does
    not silently make this row look consistent" — yet a bare
    ``filed_dealer_id != dealer_id`` check read exactly that drift as a completed
    move and dropped the caveat. Only a logged, NON-reverted
    ``apply_attribution_moves`` run onto the dealer the car now sits at clears it;
    car 7 drifted to a dealer whose only move-log row was reverted.
    """
    state = _states()[7]
    assert state["status"] == "conflicting"
    assert state["location_unconfirmed"] is True


def test_grid_softens_the_claim_and_hides_nothing(fleet):
    """Every car still ships; only the ones with evidence against them are caveated."""
    from backend.db.repositories import listings_repo

    listings_repo.clear_inventory_listings_cache()
    cards = listings_repo._build_grid_cars_uncached()

    assert sorted(c["id"] for c in cards) == [1, 2, 3, 4, 5, 6, 7]
    unconfirmed = {c["id"] for c in cards if c.get("location_confirmed") is False}
    assert unconfirmed == {1, 3, 7}


def test_home_rails_carry_the_caveat(fleet, monkeypatch):
    """The route paths that serialize OUTSIDE the cached grid (home rails, saved
    cars, smart search) go through ``serialize_cars_for_listings_grid``, which
    batches the verdict read itself — they used to call the per-car serializer
    with no attribution at all and stated the contradicted dealership as fact.
    """
    from backend.routes import home_dashboard as hd

    monkeypatch.setattr(_home_dashboard_routes, "get_recent_viewed_car_ids", lambda uid, limit=12: [1, 2])
    monkeypatch.setattr(
        _home_dashboard_routes, "get_cars_by_ids", lambda ids: [dict(CARS[0]), dict(CARS[1])]
    )

    by_id = {c["id"]: c for c in hd._recently_viewed_for_user(7)}

    assert by_id[1]["location_confirmed"] is False
    assert "Rick Hendrick Chevrolet Naples" in by_id[1]["location_note"]
    assert "location_confirmed" not in by_id[2]


def test_dealer_attribution_resolution_rolls_up_the_same_fleet(fleet):
    """
    The admin resolution view (backend/db/repositories/attribution_repo.py) must
    agree with the per-car reader on this exact fleet: bmwofmurrieta-com is the
    one group-fed dealer, car 1 (conflicting) and car 3 (unverified) are its two
    stuck cars, car 2 (confirmed) is not, and the single-rooftop dealer
    (othertoyota-com) is excluded entirely -- this is the "which stores are
    unlocatable" view, not a fleet-wide caveat count.
    """
    from backend.db.repositories.attribution_repo import dealer_attribution_resolution

    rows = dealer_attribution_resolution()
    by_id = {r["dealer_id"]: r for r in rows}

    assert "othertoyota-com" not in by_id  # rooftop-scoped: not this view's question
    row = by_id["bmwofmurrieta-com"]
    assert row["confirmed"] == 1
    assert row["conflicting"] == 1
    assert row["unverified"] == 1
    assert row["stuck"] == 2  # the conflicting car + the unverified-at-group-feed car
    assert "Rick Hendrick Chevrolet Naples" in row["stuck_why"]
    assert "no gallery evidence" in row["stuck_why"]
    assert row["active_cars"] == 3
    assert row["dealer_name"] == "BMW of Murrieta"

    # sorted worst (stuck) first by default
    assert rows[0]["dealer_id"] == "bmwofmurrieta-com"


def test_dealer_attribution_resolution_empty_when_tables_absent(sqlite_inventory):
    """A fresh DB (car_attribution/dealer_feed_scope never created) must degrade to
    an empty list, not raise -- the admin page shows 'no data yet', never a 500."""
    from backend.db.repositories.attribution_repo import dealer_attribution_resolution

    assert dealer_attribution_resolution() == []


def test_batch_serializer_is_one_verdict_read_per_response(fleet, monkeypatch):
    """``serialize_cars_for_listings_grid`` batches: ONE verdict read, never one per car."""
    import backend.db.repositories.cars_repo as cars_repo
    from backend.db.repositories.listings_repo import serialize_cars_for_listings_grid

    real_states = cars_repo.car_attribution_states
    calls: list[list[int]] = []

    def _counting_states(car_ids=None):
        calls.append(list(car_ids) if car_ids is not None else [])
        return real_states(calls[-1] or None)

    monkeypatch.setattr(cars_repo, "car_attribution_states", _counting_states)

    cards = serialize_cars_for_listings_grid([dict(c) for c in CARS[:5]])

    assert len(calls) == 1
    assert set(calls[0]) == {c["id"] for c in CARS[:5]}
    unconfirmed = {c["id"] for c in cards if c.get("location_confirmed") is False}
    assert unconfirmed == {1, 3}


def test_dealership_page_does_not_count_them_as_confidently_its_own(fleet, monkeypatch):
    """The rooftop's headline inventory number carries the caveat, and loses no car."""
    from backend.db.repositories import listings_repo
    from backend.routes import dealership_page as dp

    listings_repo.clear_inventory_listings_cache()
    # The page reads the dealer's cards from the persisted card store (a dealer-
    # scoped query) -- never the whole-fleet grid, which must not be built here.
    def whole_fleet(*_a, **_k):
        raise AssertionError("dealership page built the whole-fleet grid")

    monkeypatch.setattr("backend.db.inventory_db.listings_grid_serialized_cars", whole_fleet)
    monkeypatch.setattr(listings_repo, "listings_grid_serialized_cars", whole_fleet)

    inv = dp._dealer_inventory("bmwofmurrieta-com")

    assert inv["total"] == 3          # nothing dropped from the rooftop's list
    assert inv["unconfirmed_count"] == 2
    assert inv["confirmed_total"] == 1


def test_states_are_one_query_for_any_number_of_cars(fleet):
    """O(1) statements, not O(N).

    ``/api/listings/cars`` serializes the whole active fleet in one pass; a per-car
    verdict lookup there is tens of thousands of round trips on the hottest read in
    the app, which is why this helper takes ids in bulk in the first place.
    """
    import backend.db.repositories.cars_repo as cars_repo

    real_db_conn = cars_repo.db_conn
    statements: list[str] = []

    class _CountingConn:
        def __init__(self, conn):
            self._conn = conn

        def execute(self, sql, params=None):
            statements.append(sql)
            return self._conn.execute(sql, params)

        def __getattr__(self, name):
            return getattr(self._conn, name)

    @contextmanager
    def _counting_db_conn(**kw):
        with real_db_conn(**kw) as conn:
            yield _CountingConn(conn)

    cars_repo.db_conn = _counting_db_conn
    try:
        one = cars_repo.car_attribution_states([1])
        del statements[:]
        many = cars_repo.car_attribution_states([c["id"] for c in CARS])
        n_many = len(statements)
        del statements[:]
        cars_repo.car_attribution_states()
        n_all = len(statements)
    finally:
        cars_repo.db_conn = real_db_conn

    assert len(one) == 1
    assert len(many) == len(VERDICTS)
    assert n_many == 1
    assert n_all == 1


def test_missing_tables_degrade_to_no_verdict(sqlite_inventory):
    """A dev/test database without the attribution tables must still serve listings.

    The overlay is a caveat layered on what the page already shows. Letting a missing
    table raise would take out the listings grid to protect a footnote.
    """
    from backend.db.repositories.cars_repo import car_attribution_states

    sqlite_inventory.add_cars([dict(CARS[0])])
    assert car_attribution_states() == {}
    assert car_attribution_states([1]) == {}


class TestGroupFedCarsWithNoVerdict:
    """The population `dealer_feed_scope` exists to cover.

    A car at a dealer PROVEN to be served its whole group's inventory, which no photograph
    has judged yet, must not state its location as fact. The first version of the query drove
    from `car_attribution` alone, so only judged cars were covered -- and 1,848 of
    `bmwofmurrieta-com`'s cars had never been judged. Every one of them named a rooftop we
    have positive evidence it is probably not at: 76 of that dealer's photos name other
    stores, across 53 of them, against 23 naming itself.
    """

    def test_unjudged_car_at_a_group_fed_dealer_is_unconfirmed(self):
        from backend.db.repositories.cars_repo import _attribution_location_unconfirmed

        assert _attribution_location_unconfirmed("unverified", group_fed=True, refiled=False)

    def test_unjudged_car_at_a_rooftop_fed_dealer_is_left_alone(self):
        """Most of the fleet is fine and must not be caveated."""
        from backend.db.repositories.cars_repo import _attribution_location_unconfirmed

        assert not _attribution_location_unconfirmed(
            "unverified", group_fed=False, refiled=False
        )

    def test_a_car_we_already_moved_is_not_caveated(self):
        """`apply_attribution_moves` writes `cars` without touching the verdict, so the
        stale `conflicting` row describes where the car USED to be filed."""
        from backend.db.repositories.cars_repo import _attribution_location_unconfirmed

        assert not _attribution_location_unconfirmed(
            "conflicting", group_fed=True, refiled=True
        )

    def test_the_union_branch_filters_by_id_too(self):
        """The id filter is appended to the whole statement; before the union was wrapped in
        a subquery it would have bound to the last branch only."""
        from backend.db.repositories.cars_repo import _ATTRIBUTION_STATE_SQL

        assert "WHERE car_id IN" not in _ATTRIBUTION_STATE_SQL
        assert _ATTRIBUTION_STATE_SQL.strip().endswith("attribution_state")


class TestWordingNeverAssertsWhatIsNotKnown:
    """Each sentence must be true of the car it appears on.

    Car 229310 is a Porsche listed by a BMW franchise, with one image watermarked "BMW of
    Beverly Hills" and another showing a "World Famous BMW" plate frame. The filing is
    contradicted; the photographs disagree on the replacement. Before this split, that car
    fell through to the group-feed sentence, asserting a feed arrangement we had no evidence
    for whenever the dealer was not actually group-fed.
    """

    def test_a_named_rooftop_is_stated(self):
        from backend.utils.car_serialize.attribution import attribution_note

        note = attribution_note({
            "location_unconfirmed": True, "status": "conflicting",
            "observed_rooftop": "Crown Lexus", "group_feed": True,
        })
        assert "Crown Lexus" in note

    def test_no_rooftop_at_a_group_fed_dealer_explains_the_feed(self):
        from backend.utils.car_serialize.attribution import attribution_note

        note = attribution_note({
            "location_unconfirmed": True, "status": "unverified",
            "observed_rooftop": None, "group_feed": True,
        })
        assert "ownership group" in note

    def test_no_rooftop_without_a_group_feed_claims_no_feed(self):
        from backend.utils.car_serialize.attribution import attribution_note

        note = attribution_note({
            "location_unconfirmed": True, "status": "conflicting",
            "observed_rooftop": None, "group_feed": False,
        })
        assert "ownership group" not in note
        assert "do not match" in note

    def test_nothing_to_say_says_nothing(self):
        from backend.utils.car_serialize.attribution import attribution_note

        assert attribution_note({"location_unconfirmed": False}) == ""
        assert attribution_note(None) == ""
