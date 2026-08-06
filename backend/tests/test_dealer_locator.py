"""Dealer locator: center resolution, DB/Google merge, and the nearby-dealer picker."""

from __future__ import annotations

from backend.listings.dealer_locator import (
    find_nearby_dealers,
    resolve_search_center,
    _matches_registry,
)
from backend.listings import nearby_dealers as nd
from backend.listings.dealer_registry_match import (
    dealer_registry_sql_filter,
    registry_id_by_dealer_host,
)


class _FakeCursor:
    """Returns a canned result set per SQL fragment, like the inventory conn wrapper."""

    def __init__(self, results: dict[str, list[tuple]]) -> None:
        self._results = results
        self._rows: list[tuple] = []

    def execute(self, sql, params=None):  # noqa: D102 - test double
        self._rows = []
        for needle, rows in self._results.items():
            if needle in sql:
                self._rows = rows
                break
        return self

    def fetchall(self):  # noqa: D102 - test double
        return self._rows


class _FakeConn:
    def __init__(self, results: dict[str, list[tuple]]) -> None:
        self._results = results

    def cursor(self):  # noqa: D102 - test double
        return _FakeCursor(self._results)

    def close(self) -> None:
        return None


class _FakeDictCursor(_FakeCursor):
    """Postgres' dict row factory: rows come back as ``{column: value}``, not tuples."""

    def __init__(self, results: dict[str, list[tuple]], columns: dict[str, list[str]]) -> None:
        super().__init__(results)
        self._columns = columns
        self._cols: list[str] = []

    def execute(self, sql, params=None):  # noqa: D102 - test double
        super().execute(sql, params)
        self._cols = []
        for needle, cols in self._columns.items():
            if needle in sql:
                self._cols = cols
                break
        return self

    def fetchall(self):  # noqa: D102 - test double
        return [dict(zip(self._cols, row)) for row in self._rows]


class _FakeDictConn(_FakeConn):
    def __init__(self, results, columns) -> None:
        super().__init__(results)
        self._columns = columns

    def cursor(self):  # noqa: D102 - test double
        return _FakeDictCursor(self._results, self._columns)


def test_registry_host_map_survives_a_dict_row_connection() -> None:
    """
    ``search_cars`` hands this a dict-row connection; the picker hands it tuples.

    Unpacking a dict row yields column *names*, so the map silently came back empty
    inside ``search_cars`` on Postgres and the host fallback in the dealer filter never
    fired: Irvine Subaru (registry 479, 41 cars none of which carry a
    ``dealership_registry_id``) counted 41 in the picker and returned 0 when ticked.
    """
    rows = [(479, "https://www.irvinesubaru.com", "https://www.irvinesubaru.com")]
    cols = ["id", "website_url", "dealer_website_url"]
    tuple_map = registry_id_by_dealer_host(_FakeConn({"FROM dealerships": rows}))
    dict_map = registry_id_by_dealer_host(
        _FakeDictConn({"FROM dealerships": rows}, {"FROM dealerships": cols})
    )
    assert tuple_map == {"irvinesubaru.com": 479}
    assert dict_map == tuple_map


def test_dealer_filter_host_patterns_do_not_leak_across_rooftops() -> None:
    """
    A registry row can hold a bare brand domain, so the host match must be anchored.

    Registry id 170 is "Subaru of America" at ``subaru.com``; a plain ``%subaru.com%``
    substring match hands it every ``*subaru.com`` rooftop we scan.
    """
    clause, params = dealer_registry_sql_filter(
        [170],
        {"subaru.com": 170},
        placeholders_fn=lambda xs: ",".join("?" * len(xs)),
    )
    patterns = [p for p in params if isinstance(p, str)]
    assert patterns == ["%//subaru.com", "%//subaru.com/%", "%//www.subaru.com", "%//www.subaru.com/%"]
    assert "dealer_url" in clause

    def like(url: str, pattern: str) -> bool:
        import re as _re

        return bool(_re.fullmatch(pattern.replace("%", ".*"), url))

    assert any(like("https://www.subaru.com/inventory", p) for p in patterns)
    assert any(like("https://subaru.com", p) for p in patterns)
    assert not any(like("https://www.irvinesubaru.com", p) for p in patterns)
    assert not any(like("https://kearnymesasubaru.com/x", p) for p in patterns)


def test_resolve_center_from_zip(monkeypatch) -> None:
    monkeypatch.setattr("backend.db.geo.zip_to_coords", lambda _z: (35.2, -80.8))
    assert resolve_search_center(zip_code="28173") == (35.2, -80.8)


def test_resolve_center_from_city_state(monkeypatch) -> None:
    monkeypatch.setattr(
        "backend.db.dealerships_db.geocode_city_state",
        lambda c, s: (35.0, -80.0) if c == "Charlotte" and s == "NC" else None,
    )
    assert resolve_search_center(city="Charlotte", state="NC") == (35.0, -80.0)


def test_matches_registry_by_host() -> None:
    g = {"name": "Foo Motors", "latitude": 35.0, "longitude": -80.0, "website_url": "https://www.foomotors.com"}
    r = {
        "name": "Foo Auto",
        "latitude": 35.5,
        "longitude": -79.0,
        "website_url": "https://foomotors.com/inventory",
        "dealer_website_url": "",
    }
    assert _matches_registry(g, r) is True


def test_find_nearby_merges_db_and_google(monkeypatch) -> None:
    db_rows = [
        {
            "id": 10,
            "name": "Registry Dealer",
            "city": "Charlotte",
            "state": "NC",
            "latitude": 35.1,
            "longitude": -80.7,
            "distance_miles": 1.0,
            "street_address": "1 Main St",
            "zip_code": "28217",
            "website_url": "https://registry.example",
            "dealer_website_url": "https://registry.example",
        }
    ]

    class FakeCandidate:
        name = "Google Only"
        city = "Charlotte"
        state = "NC"
        street_address = "2 Other St"
        zip_code = "28217"
        latitude = 35.2
        longitude = -80.6
        dealer_website_url = "https://google-only.example"
        website_url = "https://google-only.example"

    monkeypatch.setattr("backend.db.geo.zip_to_coords", lambda _z: (35.15, -80.65))
    monkeypatch.setattr(
        "backend.db.dealerships_db.search_dealerships_by_radius",
        lambda *_a, **_k: db_rows,
    )
    monkeypatch.setattr(
        "backend.listings.nearby_dealers._active_listing_counts",
        lambda ids: {10: 3},
    )
    monkeypatch.setattr(
        "backend.listings.nearby_dealers._active_listing_counts_by_dealer_id",
        lambda ids: {},
    )
    monkeypatch.setattr(
        "backend.listings.nearby_dealers._last_inventory_scraped_at",
        lambda ids: {},
    )
    monkeypatch.setattr(
        "backend.listings.nearby_dealers._last_inventory_scraped_at_by_dealer_id",
        lambda ids: {},
    )
    monkeypatch.setattr(
        "backend.listings.nearby_dealers._last_catalog_scan_at",
        lambda **_k: ({}, {}),
    )
    monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "test-key")
    monkeypatch.setattr(
        "backend.discovery.google_places.fetch_google_places_dealerships",
        lambda *_a, **_k: [FakeCandidate()],
    )

    out = find_nearby_dealers(zip_code="28173", radius_miles=25)
    assert out["ok"] is True
    assert out["total"] == 2
    assert out["in_database_count"] == 1
    names = {d["name"] for d in out["dealers"]}
    assert "Registry Dealer" in names
    assert "Google Only" in names
    reg = next(d for d in out["dealers"] if d["name"] == "Registry Dealer")
    assert reg["in_database"] is True
    assert reg["listing_count"] == 3
    goog = next(d for d in out["dealers"] if d["name"] == "Google Only")
    assert goog["in_database"] is False


def test_find_nearby_attaches_last_synced(monkeypatch) -> None:
    db_rows = [
        {
            "id": 10,
            "name": "Synced Dealer",
            "city": "Chattanooga",
            "state": "TN",
            "latitude": 35.1,
            "longitude": -85.3,
            "distance_miles": 1.0,
            "street_address": "1 Main St",
            "zip_code": "37403",
            "website_url": "https://synced.example",
            "dealer_website_url": "https://synced.example",
        }
    ]

    monkeypatch.setattr("backend.db.geo.zip_to_coords", lambda _z: (35.1, -85.3))
    monkeypatch.setattr(
        "backend.db.dealerships_db.search_dealerships_by_radius",
        lambda *_a, **_k: db_rows,
    )
    monkeypatch.setattr(
        "backend.listings.nearby_dealers._active_listing_counts",
        lambda ids: {10: 1},
    )
    monkeypatch.setattr(
        "backend.listings.nearby_dealers._active_listing_counts_by_dealer_id",
        lambda ids: {},
    )
    monkeypatch.setattr(
        "backend.listings.nearby_dealers._last_inventory_scraped_at",
        lambda ids: {10: "2026-06-10T12:00:00+00:00"},
    )
    monkeypatch.setattr(
        "backend.listings.nearby_dealers._last_inventory_scraped_at_by_dealer_id",
        lambda ids: {},
    )
    monkeypatch.setattr(
        "backend.listings.nearby_dealers._last_catalog_scan_at",
        lambda **_k: ({}, {}),
    )
    monkeypatch.delenv("GOOGLE_MAPS_API_KEY", raising=False)

    out = find_nearby_dealers(zip_code="37403", radius_miles=25, include_google=False)
    assert out["ok"] is True
    dealer = out["dealers"][0]
    assert dealer["last_synced_at"] == "2026-06-10T12:00:00+00:00"


def test_find_nearby_counts_scraped_google_dealer(monkeypatch) -> None:
    class FakeCandidate:
        name = "Ford Store"
        city = "Chattanooga"
        state = "TN"
        street_address = "2 Other St"
        zip_code = "37403"
        latitude = 35.2
        longitude = -85.3
        dealer_website_url = "https://www.mtnviewford.com"
        website_url = "https://www.mtnviewford.com"

    monkeypatch.setattr("backend.db.geo.zip_to_coords", lambda _z: (35.15, -85.3))
    monkeypatch.setattr(
        "backend.db.dealerships_db.search_dealerships_by_radius",
        lambda *_a, **_k: [],
    )
    monkeypatch.setattr(
        "backend.listings.nearby_dealers._active_listing_counts",
        lambda ids: {},
    )
    monkeypatch.setattr(
        "backend.listings.nearby_dealers._active_listing_counts_by_dealer_id",
        lambda ids: {"mtnviewford-com": 383},
    )
    monkeypatch.setattr(
        "backend.listings.nearby_dealers._last_inventory_scraped_at",
        lambda ids: {},
    )
    monkeypatch.setattr(
        "backend.listings.nearby_dealers._last_inventory_scraped_at_by_dealer_id",
        lambda ids: {},
    )
    monkeypatch.setattr(
        "backend.listings.nearby_dealers._last_catalog_scan_at",
        lambda **_k: ({}, {"mtnviewford-com": "2026-06-13T12:00:00+00:00"}),
    )
    monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "test-key")
    monkeypatch.setattr(
        "backend.discovery.google_places.fetch_google_places_dealerships",
        lambda *_a, **_k: [FakeCandidate()],
    )

    out = find_nearby_dealers(zip_code="37403", radius_miles=25)
    assert out["ok"] is True
    dealer = out["dealers"][0]
    assert dealer["in_database"] is False
    assert dealer["listing_count"] == 383


    monkeypatch.setattr(
        "backend.db.dealerships_db.geocode_city_state",
        lambda *_a, **_k: None,
    )
    monkeypatch.setattr("backend.db.geo.zip_to_coords", lambda _z: None)
    out = find_nearby_dealers(city="Nowhereville", state="ZZ", radius_miles=10)
    assert out["ok"] is False
    assert out["error"] == "location_not_found"


def test_rooftop_inventory_folds_url_variants() -> None:
    conn = _FakeConn(
        {
            "FROM cars": [
                ("https://www.villagevw.com", None, "Village Volkswagen", 60),
                ("https://villagevw.com/inventory", 0, "Village Volkswagen", 40),
                ("", None, "No URL", 5),
            ]
        }
    )
    out = nd._rooftop_inventory(conn)
    assert set(out) == {"villagevw.com"}
    assert out["villagevw.com"]["listing_count"] == 100
    assert out["villagevw.com"]["name"] == "Village Volkswagen"
    assert out["villagevw.com"]["by_registry"] == {0: 100}


def test_rooftop_inventory_splits_a_storefront_by_attributed_rooftop() -> None:
    """One site, several stores: the feed's per-vehicle rooftop is kept separate."""
    conn = _FakeConn(
        {
            "FROM cars": [
                ("https://www.nissanofcostamesa.com", 426, "Nissan of Costa Mesa", 304),
                ("https://www.nissanofcostamesa.com", 422, "Nissan of Costa Mesa", 507),
                ("https://www.nissanofcostamesa.com", None, "Nissan of Costa Mesa", 9),
            ]
        }
    )
    out = nd._rooftop_inventory(conn)
    assert out["nissanofcostamesa.com"]["listing_count"] == 820
    assert out["nissanofcostamesa.com"]["by_registry"] == {426: 304, 422: 507, 0: 9}


def test_rooftop_slices_files_cars_under_the_rooftop_that_holds_them() -> None:
    rooftops = {
        "nissanofcostamesa.com": {
            "host": "nissanofcostamesa.com",
            "dealer_url": "https://www.nissanofcostamesa.com",
            "name": "Nissan of Costa Mesa",
            "listing_count": 820,
            "by_registry": {426: 304, 422: 507, 0: 9},
        },
        "newstore.com": {
            "host": "newstore.com",
            "dealer_url": "https://newstore.com",
            "name": "New Store",
            "listing_count": 12,
            "by_registry": {0: 12},
        },
    }
    host_to_registry = {"nissanofcostamesa.com": 426}
    registry_rows = {426: {"name": "Nissan of Costa Mesa"}, 422: {"name": "Carson Nissan"}}

    counts, unregistered = nd._rooftop_slices(rooftops, host_to_registry, registry_rows)
    # The 9 unattributed cars fall back to the storefront that served them.
    assert counts == {426: 313, 422: 507}
    assert unregistered == {"newstore.com": 12}


def test_rooftop_slices_ignores_an_id_with_no_live_registry_row() -> None:
    """A dangling attribution is unfilterable, so those cars go back to the storefront."""
    rooftops = {
        "irvinesubaru.com": {
            "host": "irvinesubaru.com",
            "dealer_url": "https://www.irvinesubaru.com",
            "name": "Irvine Subaru",
            "listing_count": 41,
            "by_registry": {9999: 41},
        }
    }
    counts, unregistered = nd._rooftop_slices(
        rooftops, {"irvinesubaru.com": 479}, {479: {"name": "Irvine Subaru"}}
    )
    assert counts == {479: 41}
    assert unregistered == {}


def _sutherlin_rooftops() -> dict:
    return {
        "sutherlinsubaru.com": {
            "host": "sutherlinsubaru.com",
            "dealer_url": "https://www.sutherlinsubaru.com",
            "name": "Sutherlin Subaru",
            "listing_count": 25,
            "by_registry": {170: 20, 0: 5},
        }
    }


def _sutherlin_registry() -> dict:
    return {
        # Subaru of America's HQ row: it has a site of its own, and that site's host is a
        # substring of the rooftop's, which is the signature of the old %host% backfill.
        170: {
            "name": "Subaru of America",
            "city": "Camden",
            "state": "NJ",
            "latitude": 39.9425652,
            "longitude": -75.1084167,
            "hosts": {"subaru.com"},
        },
        # A group-feed sibling has no site of its own; its stamps must survive untouched.
        422: {"name": "Carson Nissan", "city": "Carson", "state": "CA", "hosts": set()},
        # A cross-brand sibling that does have a site, but not one the storefront's host
        # contains — real attribution, not a substring collision.
        499: {"name": "Fletcher Jones Motorcars", "hosts": {"fjmercedes.com"}},
    }


def test_mis_stamped_pairs_flags_only_a_substring_collision() -> None:
    rooftops = _sutherlin_rooftops()
    rooftops["nissanofcostamesa.com"] = {
        "host": "nissanofcostamesa.com",
        "dealer_url": "https://www.nissanofcostamesa.com",
        "name": "Nissan of Costa Mesa",
        "listing_count": 507,
        "by_registry": {422: 507},
    }
    rooftops["audifletcherjones.com"] = {
        "host": "audifletcherjones.com",
        "dealer_url": "https://www.audifletcherjones.com",
        "name": "Audi Fletcher Jones",
        "listing_count": 686,
        "by_registry": {499: 686},
    }
    assert nd._mis_stamped_pairs(rooftops, _sutherlin_registry()) == [
        ("sutherlinsubaru.com", 170, 20)
    ]


def test_mis_stamped_pairs_ignores_a_stamp_matching_the_rooftops_own_row() -> None:
    rooftops = {
        "irvinesubaru.com": {
            "host": "irvinesubaru.com",
            "dealer_url": "https://www.irvinesubaru.com",
            "name": "Irvine Subaru",
            "listing_count": 41,
            "by_registry": {479: 41},
        }
    }
    registry = {479: {"name": "Irvine Subaru", "hosts": {"irvinesubaru.com"}}}
    assert nd._mis_stamped_pairs(rooftops, registry) == []


def test_apply_stamp_corrections_returns_cars_to_their_storefront() -> None:
    rooftops = _sutherlin_rooftops()
    nd._apply_stamp_corrections(rooftops, [("sutherlinsubaru.com", 170, 20)])
    assert rooftops["sutherlinsubaru.com"]["by_registry"] == {0: 25}


def _sutherlin_world(monkeypatch, *, cleared: list, registered: list) -> None:
    """Wire the Sutherlin fixtures into both the picker and the offline pass."""
    monkeypatch.setattr(nd, "_rooftop_inventory", lambda _c: _sutherlin_world.rooftops)
    monkeypatch.setattr(nd, "_registry_rows_by_id", lambda _c: _sutherlin_world.registry)
    monkeypatch.setattr(
        nd,
        "_rooftop_geopoints",
        lambda _c: {
            "sutherlinsubaru.com": {
                "lat": 35.8743663,
                "lon": -84.4418105,
                "city": "Kingston",
                "state": "TN",
            }
        },
    )
    monkeypatch.setattr(
        nd, "_clear_mis_stamped_registry_ids", lambda pairs: cleared.extend(pairs) or 20
    )

    def fake_register(pending, registry_rows_, taken):
        registered.extend(r["host"] for r, _p in pending)
        return {r["host"]: 901 for r, _p in pending}

    monkeypatch.setattr(nd, "_register_rooftops", fake_register)
    monkeypatch.setattr(
        "backend.listings.dealer_registry_match.registry_id_by_dealer_host",
        lambda _c: {"subaru.com": 170},
    )
    monkeypatch.setattr("backend.db.inventory_db.get_conn", lambda *_a, **_k: _FakeConn({}))


def test_picker_request_corrects_a_mis_stamp_without_writing(monkeypatch) -> None:
    """
    The wrong-metro entry disappears, and the request writes nothing to do it.

    Registry 170 ("Subaru of America", Camden NJ) sat 0.54 miles from ZIP 08103 holding
    20 Tennessee cars. Folding those cars back onto their storefront is what removes
    the Camden entry, and folding them is a pure in-memory operation on the rooftops
    this request already read. ``GET /api/nearby-dealers`` is unauthenticated: it must
    not ``UPDATE cars`` and it must not ``INSERT INTO dealerships``.
    """
    _sutherlin_world.rooftops = _sutherlin_rooftops()
    _sutherlin_world.registry = _sutherlin_registry()
    cleared: list[tuple] = []
    registered: list[str] = []
    _sutherlin_world(monkeypatch, cleared=cleared, registered=registered)

    # Camden NJ: the rooftop is 600+ miles away, so nothing at all should come back.
    assert nd._dealers_with_inventory_near(39.9425652, -75.1084167, 25.0) == []
    # Kingston TN: the rooftop has no registry row of its own yet, so the picker has
    # no id to put on a checkbox and leaves it out rather than minting one. It becomes
    # offerable after the offline pass below runs.
    assert nd._dealers_with_inventory_near(35.8743663, -84.4418105, 25.0) == []
    assert cleared == []
    assert registered == []


def test_offline_pass_clears_the_stamp_and_registers_the_rooftop(monkeypatch) -> None:
    """The writes the picker used to do, now in the pass the backfill script calls.

    The stamp is non-NULL, so ``backfill_dealership_registry_ids`` (``IS NULL OR <=
    0``) can never revisit it — clearing it is what lets the rooftop be re-stamped
    once it has a row of its own.
    """
    _sutherlin_world.rooftops = _sutherlin_rooftops()
    _sutherlin_world.registry = _sutherlin_registry()
    cleared: list[tuple] = []
    registered: list[str] = []
    _sutherlin_world(monkeypatch, cleared=cleared, registered=registered)

    rows_cleared, pairs = nd.repair_mis_stamped_registry_ids()
    assert pairs == [("sutherlinsubaru.com", 170, 20)]
    assert rows_cleared == 20
    assert cleared == [("sutherlinsubaru.com", 170, 20)]
    assert registered == ["sutherlinsubaru.com"]

    # Once registered, the rooftop is what the picker offers, at its own coordinates.
    monkeypatch.setattr(
        "backend.listings.dealer_registry_match.registry_id_by_dealer_host",
        lambda _c: {"subaru.com": 170, "sutherlinsubaru.com": 901},
    )
    _sutherlin_world.registry[901] = {
        "name": "Sutherlin Subaru", "city": "Kingston", "state": "TN", "hosts": set(),
    }
    knoxville = nd._dealers_with_inventory_near(35.8743663, -84.4418105, 25.0)
    assert [(d["id"], d["listing_count"], d["city"]) for d in knoxville] == [
        (901, 25, "Kingston")
    ]


def test_offline_registration_kill_switch(monkeypatch) -> None:
    monkeypatch.setenv("NEARBY_DEALERS_AUTOREGISTER", "0")
    _sutherlin_world.rooftops = _sutherlin_rooftops()
    _sutherlin_world.registry = _sutherlin_registry()
    cleared: list[tuple] = []
    registered: list[str] = []
    _sutherlin_world(monkeypatch, cleared=cleared, registered=registered)

    nd.repair_mis_stamped_registry_ids()
    # The stamp repair still runs; only the INSERT half is suppressed.
    assert cleared == [("sutherlinsubaru.com", 170, 20)]
    assert registered == []


def test_stamp_repair_kill_switch(monkeypatch) -> None:
    monkeypatch.setenv("NEARBY_DEALERS_REPAIR_STAMPS", "0")
    rooftops = _sutherlin_rooftops()
    assert nd._correct_mis_stamped_rooftops(rooftops, _sutherlin_registry()) == []
    assert rooftops["sutherlinsubaru.com"]["by_registry"] == {170: 20, 0: 5}


def test_rooftop_geopoints_only_labels_city_from_trusted_source() -> None:
    conn = _FakeConn(
        {
            "FROM dealer_geopoints": [
                ("https://www.a.com", 35.0, -85.0, "Chattanooga", "tn", "google_places"),
                ("https://www.b.com", 34.0, -84.0, "Somewhere", "GA", "name_match"),
            ]
        }
    )
    out = nd._rooftop_geopoints(conn)
    assert out["a.com"] == {"lat": 35.0, "lon": -85.0, "city": "Chattanooga", "state": "TN"}
    # A name-matched geocode still places the dealer, but must not label its city.
    assert out["b.com"]["city"] == ""
    assert out["b.com"]["lat"] == 34.0


def test_match_unlinked_registry_row_rejects_sibling_brand() -> None:
    registry = {
        7: {
            "name": "Shottenkirk Honda Huntsville",
            "city": "Huntsville",
            "state": "AL",
            "latitude": 34.7,
            "longitude": -86.6,
        }
    }
    point = {"lat": 34.7, "lon": -86.6, "city": "Huntsville", "state": "AL"}
    sibling = {"host": "shottenkirkacura.com", "name": "Shottenkirk Acura Huntsville"}
    assert nd._match_unlinked_registry_row(sibling, point, registry, set()) == 0

    same = {"host": "shottenkirkhonda.com", "name": "Shottenkirk  Honda, Huntsville"}
    assert nd._match_unlinked_registry_row(same, point, registry, set()) == 7
    # Already claimed by another rooftop this pass.
    assert nd._match_unlinked_registry_row(same, point, registry, {7}) == 0


def test_match_unlinked_registry_row_rejects_far_namesake() -> None:
    registry = {
        9: {"name": "Toyota of Cleveland", "city": "Cleveland", "state": "OH",
            "latitude": 41.5, "longitude": -81.7},
    }
    point = {"lat": 35.1, "lon": -84.9, "city": "Cleveland", "state": "TN"}
    rooftop = {"host": "toyotaofcleveland.com", "name": "Toyota of Cleveland"}
    assert nd._match_unlinked_registry_row(rooftop, point, registry, set()) == 0


def test_dealers_with_inventory_near_omits_unregistered_rooftops(monkeypatch) -> None:
    """A scanned rooftop with no registry row is omitted, not registered mid-request.

    This is the behaviour change that took registration off ``GET
    /api/nearby-dealers``: the checkbox filter is keyed on ``dealerships.id``, so a
    rooftop without one cannot be offered, and minting one is a write. Measured on
    the live inventory 2026-08-02, 55 of 213 scanned hosts are in this state and wait
    on ``register_unregistered_rooftops``; none of them fall within 25 miles of ZIP
    37405 or 92694, whose picker results are byte-identical before and after.
    """
    rooftops = {
        "villagevw.com": {
            "host": "villagevw.com",
            "dealer_url": "https://www.villagevw.com",
            "name": "Village Volkswagen of Chattanooga",
            "listing_count": 100,
            "by_registry": {0: 100},
        },
        "faraway.com": {
            "host": "faraway.com",
            "dealer_url": "https://www.faraway.com",
            "name": "Far Away Motors",
            "listing_count": 500,
            "by_registry": {0: 500},
        },
    }
    geopoints = {
        "villagevw.com": {"lat": 35.0638, "lon": -85.1936, "city": "Chattanooga", "state": "TN"},
        "faraway.com": {"lat": 40.0, "lon": -80.0, "city": "Elsewhere", "state": "PA"},
    }
    registered: list[str] = []
    _ids = {"villagevw.com": 900, "faraway.com": 901}

    def fake_register(pending, registry_rows, taken):
        registered.extend(r["host"] for r, _p in pending)
        return {r["host"]: _ids[r["host"]] for r, _p in pending}

    monkeypatch.setattr(nd, "_rooftop_inventory", lambda _c: rooftops)
    monkeypatch.setattr(nd, "_rooftop_geopoints", lambda _c: geopoints)
    monkeypatch.setattr(nd, "_registry_rows_by_id", lambda _c: {})
    monkeypatch.setattr(nd, "_register_rooftops", fake_register)
    monkeypatch.setattr(
        "backend.listings.dealer_registry_match.registry_id_by_dealer_host",
        lambda _c: {},
    )
    monkeypatch.setattr("backend.db.inventory_db.get_conn", lambda *_a, **_k: _FakeConn({}))

    out = nd._dealers_with_inventory_near(35.0842, -85.3167, 25.0)
    assert registered == []
    assert out == []

    # The same two rooftops, once the offline pass has given them ids: the in-radius
    # one is offered at its own geopoint, the far one is filtered out by distance.
    # Ordered by inventory size, not by the caller's ZIP: offline has no ZIP.
    nd.register_unregistered_rooftops()
    assert registered == ["faraway.com", "villagevw.com"]
    monkeypatch.setattr(
        "backend.listings.dealer_registry_match.registry_id_by_dealer_host",
        lambda _c: {"villagevw.com": 900, "faraway.com": 901},
    )
    monkeypatch.setattr(
        nd,
        "_registry_rows_by_id",
        lambda _c: {900: {"name": "Village Volkswagen of Chattanooga"},
                    901: {"name": "Far Away Motors"}},
    )
    out = nd._dealers_with_inventory_near(35.0842, -85.3167, 25.0)
    assert [(d["id"], d["name"], d["listing_count"]) for d in out] == [
        (900, "Village Volkswagen of Chattanooga", 100)
    ]
    assert out[0]["city"] == "Chattanooga"
    assert out[0]["distance_miles"] > 0


def test_dealers_with_inventory_near_merges_hosts_sharing_a_registry_row(monkeypatch) -> None:
    rooftops = {
        "store.com": {
            "host": "store.com",
            "dealer_url": "https://store.com",
            "name": "Store",
            "listing_count": 10,
            "by_registry": {0: 10},
        },
        "store-used.com": {
            "host": "store-used.com",
            "dealer_url": "https://store-used.com",
            "name": "Store Used",
            "listing_count": 4,
            "by_registry": {0: 4},
        },
    }
    geopoints = {
        "store.com": {"lat": 35.0, "lon": -85.0, "city": "Town", "state": "TN"},
        "store-used.com": {"lat": 35.0, "lon": -85.0, "city": "Town", "state": "TN"},
    }
    monkeypatch.setattr(nd, "_rooftop_inventory", lambda _c: rooftops)
    monkeypatch.setattr(nd, "_rooftop_geopoints", lambda _c: geopoints)
    monkeypatch.setattr(nd, "_registry_rows_by_id", lambda _c: {5: {"name": "Store", "city": "Town", "state": "TN"}})
    monkeypatch.setattr(
        "backend.listings.dealer_registry_match.registry_id_by_dealer_host",
        lambda _c: {"store.com": 5, "store-used.com": 5},
    )
    monkeypatch.setattr("backend.db.inventory_db.get_conn", lambda *_a, **_k: _FakeConn({}))

    out = nd._dealers_with_inventory_near(35.0, -85.0, 25.0)
    assert len(out) == 1
    assert out[0]["id"] == 5
    assert out[0]["listing_count"] == 14


def test_dealers_with_inventory_near_splits_a_group_feed(monkeypatch) -> None:
    """
    The checkbox count must be what a dealer filter can actually deliver.

    The storefront serves 820 cars but only 313 are its own; the other 507 are the
    sibling rooftop's, and that rooftop is listed separately at its own coordinates
    instead of being folded into the site that happens to publish it.
    """
    rooftops = {
        "nissanofcostamesa.com": {
            "host": "nissanofcostamesa.com",
            "dealer_url": "https://www.nissanofcostamesa.com",
            "name": "Nissan of Costa Mesa",
            "listing_count": 820,
            "by_registry": {426: 304, 422: 507, 0: 9},
        }
    }
    monkeypatch.setattr(nd, "_rooftop_inventory", lambda _c: rooftops)
    monkeypatch.setattr(
        nd,
        "_rooftop_geopoints",
        lambda _c: {
            "nissanofcostamesa.com": {
                "lat": 33.6795,
                "lon": -117.9171,
                "city": "Costa Mesa",
                "state": "CA",
            }
        },
    )
    monkeypatch.setattr(
        nd,
        "_registry_rows_by_id",
        lambda _c: {
            426: {"name": "Nissan of Costa Mesa", "city": "Costa Mesa", "state": "CA",
                  "latitude": 33.6795, "longitude": -117.9171},
            422: {"name": "Carson Nissan", "city": "Carson", "state": "CA",
                  "latitude": 33.8250, "longitude": -118.2474},
        },
    )
    monkeypatch.setattr(
        "backend.listings.dealer_registry_match.registry_id_by_dealer_host",
        lambda _c: {"nissanofcostamesa.com": 426},
    )
    monkeypatch.setattr("backend.db.inventory_db.get_conn", lambda *_a, **_k: _FakeConn({}))

    out = nd._dealers_with_inventory_near(33.6795, -117.9171, 40.0)
    by_id = {d["id"]: d for d in out}
    assert by_id[426]["listing_count"] == 313
    assert by_id[422]["listing_count"] == 507
    # Carson Nissan has no site of its own — it is placed by its registry coordinates.
    assert by_id[422]["name"] == "Carson Nissan"
    assert by_id[422]["distance_miles"] > by_id[426]["distance_miles"]

    # Outside the sibling's distance, only the storefront's own stock is offered.
    near = nd._dealers_with_inventory_near(33.6795, -117.9171, 5.0)
    assert [(d["id"], d["listing_count"]) for d in near] == [(426, 313)]


def test_total_in_radius_counts_dealers_missing_from_registry_search(monkeypatch) -> None:
    monkeypatch.setattr("backend.db.geo.zip_to_coords", lambda _z: (35.0, -85.0))
    monkeypatch.setattr(
        "backend.db.dealerships_db.search_dealerships_by_radius",
        lambda *_a, **_k: [{"id": 1}],
    )
    monkeypatch.setattr(
        nd,
        "_dealers_with_inventory_near",
        lambda *_a, **_k: [
            {"id": 1, "name": "A", "city": "", "state": "", "distance_miles": 1.0, "listing_count": 3},
            {"id": 2, "name": "B", "city": "", "state": "", "distance_miles": 2.0, "listing_count": 3},
        ],
    )
    out = nd.resolve_nearby_dealers_for_listings(zip_code="37405", radius_miles=25)
    assert out["total_with_inventory"] == 2
    # Registry radius search only knew about id 1; the freshly registered id 2 counts too.
    assert out["total_in_radius"] == 2


def test_resolve_forwards_radius_to_rooftop_search(monkeypatch) -> None:
    """The requested radius must reach the rooftop search, not a hardcoded default."""
    seen: list[float] = []

    monkeypatch.setattr("backend.db.geo.zip_to_coords", lambda _z: (35.0, -85.0))
    monkeypatch.setattr(
        "backend.db.dealerships_db.search_dealerships_by_radius",
        lambda _lat, _lon, radius: [{"id": i} for i in range(1, int(radius) // 10 + 1)],
    )

    def fake_near(lat, lon, radius_miles):
        seen.append(radius_miles)
        return []

    monkeypatch.setattr(nd, "_dealers_with_inventory_near", fake_near)

    small = nd.resolve_nearby_dealers_for_listings(zip_code="37405", radius_miles=25)
    large = nd.resolve_nearby_dealers_for_listings(zip_code="37405", radius_miles=50)
    assert seen == [25.0, 50.0]
    # A wider radius must widen the reported count; it stayed pinned at 1 while the
    # route silently dropped the caller's radius.
    assert large["total_in_radius"] > small["total_in_radius"]


def test_nearby_dealers_api_accepts_radius_miles_alias(monkeypatch) -> None:
    import backend.main as main

    seen: dict[str, float] = {}

    def fake_resolve(*, zip_code, radius_miles, search_query=None, cap=None):
        seen["radius"] = radius_miles
        return {"ok": True, "dealers": []}

    monkeypatch.setattr(
        "backend.listings.nearby_dealers.resolve_nearby_dealers_for_listings", fake_resolve
    )
    with main.app.test_client() as client:
        client.get("/api/nearby-dealers?zip_code=37405&radius_miles=10")
    assert seen["radius"] == 10.0

    with main.app.test_client() as client:
        client.get("/api/nearby-dealers?zip_code=37405&radius=15")
    assert seen["radius"] == 15.0
