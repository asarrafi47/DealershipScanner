"""Round-trip + guard tests for the community backend (comments, dealer ratings).

SQLite isolation mirrors ``test_dealer_reviews.py`` — never touches prod Postgres.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from backend.db import comments_db
from backend.db.comments_db import (
    SCOPE_CAR,
    SCOPE_DEALER,
    CommentError,
    body_to_html,
    count_comments,
    count_recent_comments_by_user,
    create_comment,
    delete_own_comment,
    flag_comment,
    get_comment,
    get_dealer_rating,
    list_comments,
    list_dealerships_needing_rating,
    sanitize_body,
    subject_exists,
    upsert_dealer_rating,
)
from backend.enrichment.dealer_ratings import classify_places_error


@pytest.fixture()
def seeded(sqlite_inventory):
    """A car row and two dealership rows so subject-existence checks are real."""
    conn = sqlite_inventory.get_conn()
    try:
        cur = conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS cars (id INTEGER PRIMARY KEY, vin TEXT)")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS dealerships (
                id INTEGER PRIMARY KEY,
                name TEXT, city TEXT, state TEXT,
                latitude REAL, longitude REAL,
                website_url TEXT, dealer_website_url TEXT,
                is_active INTEGER NOT NULL DEFAULT 1,
                duplicate_of_id INTEGER,
                google_place_id TEXT
            )
            """
        )
        cur.execute("INSERT INTO cars (id, vin) VALUES (101, 'TESTVIN0000000001')")
        cur.execute(
            "INSERT INTO dealerships (id, name, city, state, website_url, is_active) "
            "VALUES (7, 'Bogus Motors', 'Las Vegas', 'NV', 'https://bogusmotors.com', 1)"
        )
        cur.execute(
            "INSERT INTO dealerships (id, name, city, state, website_url, is_active) "
            "VALUES (8, 'Smoky Auto Group', 'Henderson', 'NV', 'https://smokyauto.com', 1)"
        )
        conn.commit()
    finally:
        conn.close()
    return sqlite_inventory


# ---------------------------------------------------------------------------
# Body handling
# ---------------------------------------------------------------------------

def test_sanitize_body_strips_control_chars_and_collapses_blank_lines():
    assert sanitize_body("  hello\x00 world  ") == "hello world"
    assert sanitize_body("a\r\n\n\n\n\nb") == "a\n\nb"


@pytest.mark.parametrize("bad", ["", "   ", None, "x"])
def test_sanitize_body_rejects_empty(bad):
    with pytest.raises(CommentError) as exc:
        sanitize_body(bad)
    assert exc.value.code == "empty_body"


def test_sanitize_body_rejects_overlong():
    with pytest.raises(CommentError) as exc:
        sanitize_body("x" * (comments_db.MAX_BODY_CHARS + 1))
    assert exc.value.code == "body_too_long"


def test_markup_is_stored_verbatim_but_escaped_on_the_way_out(seeded):
    """The raw text stays intact; ``body_html`` is what the UI may render."""
    payload = "<script>alert('x')</script> & 5 < 6"
    row = create_comment(SCOPE_CAR, 101, 1, payload)
    assert row["body"] == payload
    assert "<script>" not in row["body_html"]
    assert "&lt;script&gt;" in row["body_html"]
    assert "&amp;" in row["body_html"]


def test_body_to_html_turns_newlines_into_breaks():
    assert body_to_html("a\nb") == "a<br>b"


# ---------------------------------------------------------------------------
# Comment CRUD
# ---------------------------------------------------------------------------

def test_car_and_dealer_threads_are_independent(seeded):
    create_comment(SCOPE_CAR, 101, 1, "Priced well for the mileage.")
    create_comment(SCOPE_DEALER, 7, 1, "this dealership adds bogus packages, avoid")
    create_comment(
        SCOPE_DEALER, 7, 2, "smells like a las vegas casino in here (cigarettes)"
    )

    car_thread = list_comments(SCOPE_CAR, 101)
    dealer_thread = list_comments(SCOPE_DEALER, 7)
    assert [c["body"] for c in car_thread] == ["Priced well for the mileage."]
    assert len(dealer_thread) == 2
    # Newest first.
    assert dealer_thread[0]["user_id"] == 2
    assert dealer_thread[0]["subject_id"] == 7
    assert count_comments(SCOPE_DEALER, 7) == 2
    # A different dealership sees none of it.
    assert list_comments(SCOPE_DEALER, 8) == []
    assert count_comments(SCOPE_DEALER, 8) == 0


def test_list_comments_on_empty_database_returns_empty(sqlite_inventory):
    """A car page viewed before anyone has commented must not raise."""
    assert list_comments(SCOPE_CAR, 999) == []
    assert count_comments(SCOPE_CAR, 999) == 0


def test_unknown_scope_is_rejected():
    with pytest.raises(CommentError) as exc:
        list_comments("dealership", 1)
    assert exc.value.code == "bad_scope"


def test_delete_is_soft_and_owner_only(seeded):
    mine = create_comment(SCOPE_DEALER, 7, 1, "They quoted one price and charged another.")
    theirs = create_comment(SCOPE_DEALER, 7, 2, "Friendly staff, no games.")

    assert delete_own_comment(SCOPE_DEALER, theirs["id"], 1) == "forbidden"
    assert delete_own_comment(SCOPE_DEALER, 999999, 1) == "not_found"
    assert delete_own_comment(SCOPE_DEALER, mine["id"], 1) == "deleted"

    # Gone from the thread, still on the row (moderation trail survives).
    assert [c["id"] for c in list_comments(SCOPE_DEALER, 7)] == [theirs["id"]]
    row = get_comment(SCOPE_DEALER, mine["id"])
    assert row is not None and row["deleted_at"] is not None
    # Second delete of the same comment is a no-op, not a crash.
    assert delete_own_comment(SCOPE_DEALER, mine["id"], 1) == "not_found"


def test_flagging_hides_a_comment_at_the_threshold(seeded):
    c = create_comment(SCOPE_CAR, 101, 3, "Spam spam spam buy my thing")
    for expected in range(1, comments_db.FLAG_HIDE_THRESHOLD):
        updated = flag_comment(SCOPE_CAR, c["id"])
        assert updated["flag_count"] == expected
        assert updated["is_hidden"] is False
        assert len(list_comments(SCOPE_CAR, 101)) == 1

    final = flag_comment(SCOPE_CAR, c["id"])
    assert final["flag_count"] == comments_db.FLAG_HIDE_THRESHOLD
    assert final["is_hidden"] is True
    assert list_comments(SCOPE_CAR, 101) == []
    assert len(list_comments(SCOPE_CAR, 101, include_hidden=True)) == 1


def test_flagging_a_missing_comment_returns_none(seeded):
    assert flag_comment(SCOPE_CAR, 424242) is None


def test_pagination(seeded):
    for i in range(5):
        create_comment(SCOPE_CAR, 101, 1, f"comment number {i}")
    page = list_comments(SCOPE_CAR, 101, limit=2, offset=2)
    assert len(page) == 2
    assert count_comments(SCOPE_CAR, 101) == 5


def test_recent_post_count_spans_both_surfaces_and_counts_deleted(seeded):
    """Post-then-delete must not reset the per-user cap, or spam is unbounded."""
    create_comment(SCOPE_CAR, 101, 42, "one")
    c = create_comment(SCOPE_DEALER, 7, 42, "two")
    delete_own_comment(SCOPE_DEALER, c["id"], 42)

    assert count_recent_comments_by_user(42) == 2
    assert count_recent_comments_by_user(43) == 0

    # A comment from two hours ago is outside the default one-hour window.
    old = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    conn = seeded.get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO car_comments (car_id, user_id, body, is_hidden, flag_count, "
            "created_at, updated_at) VALUES (?, ?, ?, 0, 0, ?, ?)",
            (101, 42, "ancient history", old, old),
        )
        conn.commit()
    finally:
        conn.close()
    assert count_recent_comments_by_user(42) == 2


def test_subject_existence_gates_writes(seeded):
    assert subject_exists(SCOPE_CAR, 101) is True
    assert subject_exists(SCOPE_CAR, 999) is False
    assert subject_exists(SCOPE_DEALER, 7) is True
    assert subject_exists(SCOPE_DEALER, 4242) is False


# ---------------------------------------------------------------------------
# Dealer ratings
# ---------------------------------------------------------------------------

def test_rating_upsert_is_idempotent(seeded):
    upsert_dealer_rating(7, place_id="ChIJabc", rating=4.7, review_count=27557)
    upsert_dealer_rating(7, place_id="ChIJabc", rating=4.6, review_count=27600)

    row = get_dealer_rating(7)
    assert row["google_rating"] == 4.6
    assert row["google_review_count"] == 27600
    assert row["google_place_id"] == "ChIJabc"
    assert row["source"] == "google_places"
    assert row["fetched_at"]

    conn = seeded.get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM dealer_ratings")
        assert cur.fetchone()[0] == 1
    finally:
        conn.close()


def test_missing_rating_reads_as_none(seeded):
    assert get_dealer_rating(8) is None


def test_work_queue_skips_dealers_already_rated(seeded):
    queue = list_dealerships_needing_rating(limit=10)
    assert {d["id"] for d in queue} == {7, 8}

    upsert_dealer_rating(7, place_id="ChIJabc", rating=4.7, review_count=100)
    queue = list_dealerships_needing_rating(limit=10)
    assert {d["id"] for d in queue} == {8}


def test_work_queue_refresh_window_reclaims_stale_rows(seeded):
    stale = (datetime.now(timezone.utc) - timedelta(days=60)).isoformat()
    upsert_dealer_rating(
        7, place_id="ChIJabc", rating=4.7, review_count=100, fetched_at=stale
    )
    upsert_dealer_rating(8, place_id="ChIJdef", rating=3.1, review_count=12)

    assert list_dealerships_needing_rating(limit=10) == []
    stale_ids = {d["id"] for d in list_dealerships_needing_rating(limit=10, refresh_after_days=30)}
    assert stale_ids == {7}


def test_recording_a_miss_stops_the_importer_re_asking(seeded):
    """A NULL rating with a fetched_at is 'we asked and Google had nothing'."""
    upsert_dealer_rating(7, place_id=None, rating=None, review_count=None)
    row = get_dealer_rating(7)
    assert row["google_rating"] is None
    assert row["fetched_at"]
    assert {d["id"] for d in list_dealerships_needing_rating(limit=10)} == {8}


# ---------------------------------------------------------------------------
# Runtime DDL vs migrations/V002__community.sql
# ---------------------------------------------------------------------------

class _RecordingCursor:
    """Collects DDL instead of running it, so the Postgres branch is inspectable."""

    def __init__(self):
        self.statements: list[str] = []

    def execute(self, sql, params=None):
        self.statements.append(" ".join(str(sql).split()))


def _index_defs(statements):
    """``{name: (table, columns, predicate)}`` from CREATE INDEX statements."""
    import re as _re

    out = {}
    for stmt in statements:
        m = _re.match(
            r"CREATE INDEX IF NOT EXISTS (\S+) ON (?:public\.)?(\S+?)\s*"
            r"(?:USING btree\s*)?\((.+?)\)\s*(?:WHERE\s*(.+?))?;?$",
            stmt, _re.I,
        )
        if not m:
            continue
        name, table, cols, where = m.groups()
        norm = lambda s: _re.sub(r"[()\s]+", " ", (s or "")).strip().lower()  # noqa: E731
        out[name] = (table.lower(), norm(cols), norm(where))
    return out


def test_ddl_matches_v002_migration(monkeypatch):
    """The runtime tables must be the migration's tables, indexes included.

    Whichever runs first wins, so a runtime index without V002's partial
    predicate would silently produce a differently-shaped database.
    """
    from pathlib import Path

    from backend.db import inventory_pg

    monkeypatch.setattr(inventory_pg, "is_inventory_postgres", lambda: True)
    cur = _RecordingCursor()
    comments_db.ensure_comment_tables(cur)
    comments_db.ensure_dealer_ratings_table(cur)
    runtime = _index_defs(cur.statements)

    sql_path = Path(__file__).resolve().parents[2] / "migrations" / "V002__community.sql"
    migration_stmts = [
        " ".join(s.split())
        for s in sql_path.read_text().splitlines()
        if s.strip().upper().startswith("CREATE INDEX")
    ]
    migration = _index_defs(migration_stmts)

    assert set(runtime) == set(migration), "runtime and V002 create different indexes"
    for name, spec in migration.items():
        assert runtime[name] == spec, f"{name} differs from V002"

    # And the constraints the migration relies on are declared at runtime too.
    ddl = " ".join(cur.statements)
    for table in ("car_comments", "dealer_comments"):
        for suffix in ("body_not_blank", "body_length", "flag_count_nonneg"):
            assert f"{table}_{suffix}" in ddl


# ---------------------------------------------------------------------------
# Places error classification (the importer's stop condition)
# ---------------------------------------------------------------------------

def test_service_disabled_is_classified_separately_from_permission_denied():
    payload = {
        "error": {
            "code": 403,
            "status": "PERMISSION_DENIED",
            "message": "Places API (New) has not been used in project 326691549936 before or it is disabled.",
            "details": [{"reason": "SERVICE_DISABLED"}],
        }
    }
    code, detail = classify_places_error(403, payload)
    assert code == "service_disabled"
    assert "326691549936" in detail

    code, _ = classify_places_error(403, {"error": {"status": "PERMISSION_DENIED", "message": "no"}})
    assert code == "permission_denied"


@pytest.mark.parametrize(
    "status,payload,expected",
    [
        (429, {}, "quota_exhausted"),
        (400, {"error": {"message": "API key not valid"}}, "invalid_key"),
        (404, {}, "not_found"),
        (500, None, "http_error"),
    ],
)
def test_places_error_codes(status, payload, expected):
    code, _detail = classify_places_error(status, payload)
    assert code == expected


def test_fatal_errors_stop_the_run():
    from backend.enrichment.dealer_ratings import FetchResult

    assert FetchResult(error="service_disabled").fatal is True
    assert FetchResult(error="network_error").fatal is False
    assert FetchResult(error="no_result").fatal is False


# ---------------------------------------------------------------------------
# Match verification -- the guard against attaching a rating to the wrong rooftop
# ---------------------------------------------------------------------------

def test_hosts_match_allows_subdomains_but_not_unrelated_sites():
    from backend.enrichment.dealer_ratings import hosts_match

    assert hosts_match("https://www.LongoToyota.com/inventory", "longotoyota.com") is True
    assert hosts_match("used.longotoyota.com", "https://longotoyota.com") is True
    # The failure this guard exists for: a name-only search hitting another store.
    assert hosts_match("https://toyotaofdowntownla.com", "longotoyota.com") is False
    assert hosts_match("", "longotoyota.com") is False


def test_unmatched_candidates_are_rejected_rather_than_guessed(seeded):
    """Google's top hit is only accepted when its website confirms the dealer."""
    from backend.enrichment.dealer_ratings import _pick_verified_place

    places = [
        {"id": "wrong", "websiteUri": "https://someotherstore.com"},
        {"id": "right", "websiteUri": "https://www.bogusmotors.com"},
    ]
    place, verified, reason = _pick_verified_place(places, ["bogusmotors.com"])
    assert (place["id"], verified, reason) == ("right", True, "")

    # No candidate on the dealer's domain -> no match at all, not the top hit.
    assert _pick_verified_place(places, ["nomatch.com"]) == (None, False, "host_mismatch")

    # Nothing to verify against: top hit, explicitly flagged unverified so the
    # importer files it under a different source string.
    place, verified, reason = _pick_verified_place(places, [])
    assert (place["id"], verified) == ("wrong", False)


# --- geography: telling one rooftop from another on a shared domain ---------

# The real registry rows behind this: both carry echopark.com.
_ECHOPARK_HOUSTON = (29.8819928, -95.4133455)
_ECHOPARK_CHARLOTTE = (35.2004748, -80.7810586)
# The genuine Places hit for the Houston (North Freeway) store.
_HOUSTON_PLACE = {
    "id": "ChIJjR3nFGLJQIYRMNso6HT9ZnM",
    "websiteUri": "https://www.echopark.com/dealerships/tx/houston/echopark-houston-north-freeway",
    "location": {"latitude": 30.032368, "longitude": -95.427492},
}


def test_haversine_matches_known_distances():
    from backend.enrichment.dealer_ratings import haversine_miles

    here = _HOUSTON_PLACE["location"]
    assert haversine_miles(*_ECHOPARK_HOUSTON, here["latitude"], here["longitude"]) == pytest.approx(
        10.4, abs=0.5
    )
    assert haversine_miles(
        *_ECHOPARK_CHARLOTTE, here["latitude"], here["longitude"]
    ) == pytest.approx(922.9, abs=2.0)


def test_shared_domain_rooftops_are_separated_by_geography():
    """A website match confirms the group, not the store -- distance decides."""
    from backend.enrichment.dealer_ratings import _pick_verified_place

    # Houston's registry row: the Houston place verifies.
    place, verified, reason = _pick_verified_place(
        [_HOUSTON_PLACE], ["echopark.com"],
        latitude=_ECHOPARK_HOUSTON[0], longitude=_ECHOPARK_HOUSTON[1], require_geo=True,
    )
    assert (place["id"], verified, reason) == (_HOUSTON_PLACE["id"], True, "")

    # Charlotte's registry row: same domain, 922mi away -> refused, not accepted.
    assert _pick_verified_place(
        [_HOUSTON_PLACE], ["echopark.com"],
        latitude=_ECHOPARK_CHARLOTTE[0], longitude=_ECHOPARK_CHARLOTTE[1], require_geo=True,
    ) == (None, False, "geo_mismatch")


def test_nearest_in_radius_candidate_wins():
    from backend.enrichment.dealer_ratings import _pick_verified_place

    far = {"id": "far", "websiteUri": "https://echopark.com",
           "location": {"latitude": 30.032368, "longitude": -95.427492}}
    near = {"id": "near", "websiteUri": "https://echopark.com",
            "location": {"latitude": 29.89, "longitude": -95.41}}
    place, verified, _ = _pick_verified_place(
        [far, near], ["echopark.com"],
        latitude=_ECHOPARK_HOUSTON[0], longitude=_ECHOPARK_HOUSTON[1], require_geo=True,
    )
    assert place["id"] == "near" and verified is True


def test_group_domain_without_coordinates_is_refused_not_guessed():
    """No geography available on a shared domain -> no rating, by design."""
    from backend.enrichment.dealer_ratings import _pick_verified_place

    no_loc = {"id": "x", "websiteUri": "https://echopark.com"}
    assert _pick_verified_place(
        [no_loc], ["echopark.com"], latitude=None, longitude=None, require_geo=True
    ) == (None, False, "ambiguous_domain")
    # A domain only one rooftop uses still resolves on the host test alone.
    place, verified, _ = _pick_verified_place([no_loc], ["echopark.com"], require_geo=False)
    assert place["id"] == "x" and verified is True


# The two carmax.com registry rows: ~9mi apart, so a radius alone cannot tell
# them apart -- only "which rooftop is this hit closest to?" can.
_CARMAX_PINEVILLE = (35.098613, -80.8805793)
_CARMAX_CHARLOTTE = (35.1594369, -80.7340161)


def test_nearby_same_domain_rooftops_are_split_by_nearest_not_radius():
    from backend.enrichment.dealer_ratings import _pick_verified_place, haversine_miles

    apart = haversine_miles(*_CARMAX_PINEVILLE, *_CARMAX_CHARLOTTE)
    assert apart < 25.0, "these rooftops are inside the radius -- radius cannot separate them"

    hit = {"id": "carmax-charlotte", "websiteUri": "https://www.carmax.com/stores/7106",
           "location": {"latitude": _CARMAX_CHARLOTTE[0], "longitude": _CARMAX_CHARLOTTE[1]}}

    # The Charlotte row owns it.
    place, verified, _ = _pick_verified_place(
        [hit], ["carmax.com"],
        latitude=_CARMAX_CHARLOTTE[0], longitude=_CARMAX_CHARLOTTE[1],
        require_geo=True, sibling_coords=[_CARMAX_PINEVILLE],
    )
    assert place["id"] == "carmax-charlotte" and verified is True

    # The Pineville row is inside the radius but is NOT the nearest rooftop, so
    # it does not get to claim Charlotte's reviews.
    assert _pick_verified_place(
        [hit], ["carmax.com"],
        latitude=_CARMAX_PINEVILLE[0], longitude=_CARMAX_PINEVILLE[1],
        require_geo=True, sibling_coords=[_CARMAX_CHARLOTTE],
    ) == (None, False, "ambiguous_domain")


def test_group_domain_rooftops_finds_shared_registry_domains(seeded):
    """Two rooftops on echopark.com are grouped; a one-rooftop domain is not."""
    from backend.enrichment.dealer_ratings import _sibling_coords, group_domain_rooftops

    conn = seeded.get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO dealerships (id, name, city, state, website_url, latitude, longitude, "
            "is_active) VALUES (64, 'EchoPark Charlotte', 'Charlotte', 'NC', "
            "'https://echopark.com', ?, ?, 1)", _ECHOPARK_CHARLOTTE,
        )
        cur.execute(
            "INSERT INTO dealerships (id, name, city, state, website_url, latitude, longitude, "
            "is_active) VALUES (151, 'EchoPark Houston (North Freeway)', 'Houston', 'TX', "
            "'https://www.echopark.com/', ?, ?, 1)", _ECHOPARK_HOUSTON,
        )
        conn.commit()
    finally:
        conn.close()

    groups = group_domain_rooftops()
    assert "echopark.com" in groups and len(groups["echopark.com"]) == 2
    assert "bogusmotors.com" not in groups

    # Houston's siblings are Charlotte only -- a rooftop is never its own rival.
    houston = {"id": 151, "website_url": "https://www.echopark.com/",
               "latitude": _ECHOPARK_HOUSTON[0], "longitude": _ECHOPARK_HOUSTON[1]}
    assert _sibling_coords(houston, ["echopark.com"], groups) == [
        pytest.approx(_ECHOPARK_CHARLOTTE)
    ]


def test_group_domain_rooftop_without_coords_is_refused_before_any_request(seeded):
    """The refusal happens without spending a Places call."""
    import backend.enrichment.dealer_ratings as dr

    class _NoCallSession:
        def get(self, *a, **k):
            raise AssertionError("no request should be made")

        def post(self, *a, **k):
            raise AssertionError("no request should be made")

    groups = {"echopark.com": [{"id": 64, "website_url": "https://echopark.com",
                                "latitude": None, "longitude": None},
                               {"id": 151, "website_url": "https://echopark.com",
                                "latitude": None, "longitude": None}]}
    result = dr.fetch_rating_for_dealer(
        {"id": 151, "name": "EchoPark Houston", "website_url": "https://echopark.com",
         "latitude": None, "longitude": None},
        api_key="k", session=_NoCallSession(), group_rooftops=groups,
    )
    assert result.ok is False
    assert result.error == "ambiguous_domain"


def test_fetch_by_place_id_requests_website_and_enforces_it(monkeypatch):
    """A stored place id is re-verified; the field mask must ask for websiteUri."""
    import backend.enrichment.dealer_ratings as dr

    assert "websiteUri" in dr.DETAIL_FIELD_MASK
    assert "location" in dr.DETAIL_FIELD_MASK

    seen: dict = {}

    class _Resp:
        status_code = 200

        def json(self):
            return {"id": "ChIJx", "rating": 4.2, "userRatingCount": 10,
                    "websiteUri": "https://someotherstore.com"}

    class _Sess:
        def get(self, url, headers=None, timeout=None):
            seen["mask"] = headers["X-Goog-FieldMask"]
            return _Resp()

    # A place id pointing at another company's website is rejected, not returned.
    result = dr.fetch_rating_by_place_id(
        "ChIJx", api_key="k", session=_Sess(), dealer_hosts=["bogusmotors.com"]
    )
    assert "websiteUri" in seen["mask"]
    assert result.ok is False
    assert result.error == "host_mismatch"


def test_unverified_is_visible_in_stats_output():
    """The count that says 'these were guesses' must survive as_dict()."""
    from backend.enrichment.dealer_ratings import SyncStats

    stats = SyncStats(resolved=5, unverified=2)
    d = stats.as_dict()
    assert d["unverified"] == 2
    assert d["verified"] == 3


def test_report_prints_unverified(caplog):
    """--apply output can never say 'resolved: 5' without flagging the guesses."""
    import logging

    from backend.enrichment.dealer_ratings import SyncStats
    from backend.scripts.import_dealer_google_ratings import _report

    stats = SyncStats(
        examined=5, resolved=5, unverified=2, written=5, dry_run=False,
        samples=[{"dealership_id": 7, "name": "Bogus Motors", "rating": 4.1,
                  "review_count": 9, "place_id": "ChIJx", "verified": False}],
    )
    with caplog.at_level(logging.INFO, logger="import_dealer_google_ratings"):
        assert _report(stats, apply=True) == 0
    text = caplog.text
    assert "UNVERIFIED 2" in text
    assert "NOT domain-confirmed" in text
    assert "[??]" in text  # the sample line carries the verified flag


def test_miss_sources_distinguish_refusal_from_no_listing():
    from backend.enrichment.dealer_ratings import SOURCE, SOURCE_UNMATCHED, _MISS_SOURCES

    assert _MISS_SOURCES["no_result"] == SOURCE
    assert _MISS_SOURCES["host_mismatch"] == SOURCE_UNMATCHED
    assert _MISS_SOURCES["geo_mismatch"] == SOURCE_UNMATCHED
    assert _MISS_SOURCES["ambiguous_domain"] == SOURCE_UNMATCHED


def test_unverified_matches_get_their_own_source_string(seeded):
    from backend.enrichment.dealer_ratings import SOURCE, SOURCE_UNVERIFIED, FetchResult
    from backend.discovery.google_place_rating import GooglePlaceRating

    rating = GooglePlaceRating(place_id="ChIJx", rating=4.2, review_count=10)
    assert FetchResult(rating=rating, verified=True).source == SOURCE
    assert FetchResult(rating=rating, verified=False).source == SOURCE_UNVERIFIED


def test_dealer_hosts_dedupes_across_both_url_columns():
    from backend.enrichment.dealer_ratings import dealer_hosts

    assert dealer_hosts(
        {"website_url": "https://www.bogusmotors.com/", "dealer_website_url": "bogusmotors.com"}
    ) == ["bogusmotors.com"]
    assert dealer_hosts({"website_url": None, "dealer_website_url": ""}) == []
