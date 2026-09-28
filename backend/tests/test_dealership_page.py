"""Dealership inventory browser: dealer-scoped facets, JSON endpoints, page contract.

The page reuses main.js wholesale, so what these tests guard is the *contract* that
reuse depends on: facet values that come from the serialized card (anything else
selects zero cars in the browser), payloads scoped to one rooftop, and the boot
blobs / element ids main.js looks for.
"""

from __future__ import annotations

import json

import pytest

from backend.main import app
from backend.routes import dealership_page as dp


def _card(**over):
    base = {
        "id": 1,
        "title": "2022 RAM 1500 Limited",
        "make": "RAM",
        "model": "1500",
        "trim": "Limited",
        "price": 50000,
        "mileage": 20000,
        "fuel_type": "Gasoline",
        "cylinders": 8,
        "transmission": "8-Speed Automatic",
        "drivetrain": "4WD",
        "body_style": "Truck",
        "exterior_color_families": ["black"],
        "interior_color_families": ["black"],
        "package_names": ["Level 2 Equipment Group"],
        "condition": "Used",
        "dealer_id": "beamancdjr-com",
        "dealer_name": "Beaman CDJR",
        "dealer_url": "https://www.beamancdjr.com",
    }
    base.update(over)
    return base


FLEET = [
    _card(),
    _card(id=2, make="Jeep", model="Wrangler", trim="Rubicon", condition="New",
          price=60000, cylinders=6, body_style="SUV", fuel_type="Gasoline",
          exterior_color_families=["red"], package_names=[]),
    # Case variants of an existing make/trim must collapse to ONE option.
    _card(id=3, make="ram", model="1500", trim="LIMITED", price=48000),
    # Feed junk: not a real make, must not become a facet.
    _card(id=4, make="Audi A3 premium", model="A3", trim=None, price=30000),
    # The serializer writes its em-dash placeholder into missing spec fields; these
    # are values the CARDS carry, so a facet built from the cards sees them.
    _card(id=5, make="Jeep", model="Gladiator", trim="—", drivetrain="—",
          transmission="—", fuel_type="—", body_style="—", price=45000,
          package_names=["—"]),
    # A marque MAKE_TO_COUNTRY has never heard of. It is a real car on the lot, so
    # dropping it from the Make facet makes it unreachable by that filter.
    _card(id=6, make="Rivian", model="R1S", trim="Adventure", price=80000,
          fuel_type="Electric", cylinders=0),
    # A different rooftop — must never leak into this dealer's page.
    _card(id=9, dealer_id="othertoyota-com", make="Toyota", model="Camry",
          trim="XSE", body_style="Sedan"),
]


def _whole_fleet_forbidden():
    raise AssertionError("the dealership page must never build the whole-fleet grid")


@pytest.fixture
def fleet(monkeypatch):
    import backend.db.inventory_db as inv
    import backend.db.repositories.listings_repo as lr

    # The page reads ONE dealer's cards from the persisted card store (a dealer-
    # scoped query, grid_cards_repo.cards_for_dealer). Stand in for that query with
    # FLEET filtered the same way, and make the old whole-fleet grid fail loudly.
    def dealer_cards(dealer_id):
        key = (dealer_id or "").strip().lower()
        return [
            json.dumps(c) for c in FLEET
            if str(c.get("dealer_id") or "").strip().lower() == key
        ]

    monkeypatch.setattr(dp, "_dealer_grid_cards_json", dealer_cards)
    monkeypatch.setattr(inv, "listings_grid_serialized_cars", _whole_fleet_forbidden)
    monkeypatch.setattr(lr, "listings_grid_serialized_cars", _whole_fleet_forbidden)
    # The marque list comes from the epa_master catalogue at runtime; pin it so the
    # tests do not depend on a database.
    monkeypatch.setattr(dp, "_known_catalog_makes", lambda: frozenset({"rivian", "lucid", "mclaren"}))
    return FLEET


# ── facets ────────────────────────────────────────────────────────────


def test_grid_cars_are_scoped_to_one_dealer(fleet):
    cars = dp._dealer_grid_cars("beamancdjr-com")
    assert {c["id"] for c in cars} == {1, 2, 3, 4, 5, 6}


def test_facet_values_come_from_the_serialized_card(fleet):
    f = dp._dealer_facets(dp._dealer_grid_cars("beamancdjr-com"))
    # Every facet value must exist on at least one card, or the client-side
    # filter (which compares serialized fields) would return an empty grid.
    cars = dp._dealer_grid_cars("beamancdjr-com")
    for fuel in f["fuel_types"]:
        assert any(c["fuel_type"] == fuel for c in cars)
    for body in f["body_styles"]:
        assert any(c["body_style"] == body for c in cars)
    for drive in f["drivetrains"]:
        assert any(c["drivetrain"] == drive for c in cars)
    assert f["cylinders"] == [0, 6, 8]
    assert f["exterior_colors"] and set(f["exterior_colors"]) <= {"black", "red"}


def test_make_variants_collapse_and_junk_makes_are_dropped(fleet):
    f = dp._dealer_facets(dp._dealer_grid_cars("beamancdjr-com"))
    assert sorted(m.lower() for m in f["makes"]) == ["jeep", "ram", "rivian"]
    assert [r for r in f["trim_rows"] if r[1] == "1500"] == [("RAM", "1500", "Limited")]


def test_marques_missing_from_the_country_dict_still_get_a_make_option(fleet):
    """A dropped make is a filter that cannot reach 152 of today's active cars.

    ``_facet_make_valid`` gates on ``MAKE_TO_COUNTRY``, a hand-kept dict, so Lucid /
    McLaren / Rivian / Polestar / Scion / Saturn / Pontiac and friends never became
    Make options. The catalogue-backed check keeps rejecting feed junk.
    """
    from backend.db.repositories.listings_repo import _facet_make_valid

    assert not _facet_make_valid("Rivian")  # the gap being covered
    f = dp._dealer_facets(dp._dealer_grid_cars("beamancdjr-com"))
    assert "Rivian" in f["makes"]
    assert ("Rivian", "R1S") in f["model_rows"]
    # ...and the junk make is still out, on the same code path.
    assert not any("a3" in m.lower() for m in f["makes"])


def test_serializer_placeholders_never_become_facet_options(fleet):
    """The cards carry "—" for a missing spec; that is not a value to filter on."""
    f = dp._dealer_facets(dp._dealer_grid_cars("beamancdjr-com"))
    dashes = {"—", "-", "--", "n/a", "none", "null", "unknown"}
    for key in ("makes", "fuel_types", "transmissions", "drivetrains",
                "body_styles", "all_package_names", "exterior_colors", "interior_colors"):
        assert not [v for v in f[key] if str(v).strip().lower() in dashes], key
    assert not [r for r in f["model_rows"] if str(r[1]).strip().lower() in dashes]
    assert not [r for r in f["trim_rows"] if str(r[2]).strip().lower() in dashes]
    # The car whose specs are all placeholder still contributes its make/model.
    assert ("Jeep", "Gladiator") in f["model_rows"]


def test_package_rows_ship_alongside_the_package_names(fleet):
    """PACKAGE_ROWS drives main.js's cascadePackages; [] means it can never narrow."""
    f = dp._dealer_facets(dp._dealer_grid_cars("beamancdjr-com"))
    assert f["all_package_names"] == ["Level 2 Equipment Group"]
    assert {"make": "RAM", "model": "1500", "name": "Level 2 Equipment Group"} in f["package_rows"]
    assert {"make": "Rivian", "model": "R1S", "name": "Level 2 Equipment Group"} in f["package_rows"]
    # Rows carry the same labels the Make/Model checkboxes do, or the cascade hides
    # every package the moment a make is checked.
    for row in f["package_rows"]:
        if not row["make"]:
            # The junk-make card still gets a row so its package stays selectable
            # while no make/model is checked (and disappears once one is).
            continue
        assert row["make"] in f["makes"]
        assert (row["make"], row["model"]) in f["model_rows"]
    # Every offered name has at least one row — otherwise cascadePackages hides it
    # permanently the moment PACKAGE_ROWS is non-empty.
    assert {r["name"] for r in f["package_rows"]} == set(f["all_package_names"])
    # The placeholder package on the spec-less card is not an option.
    assert "—" not in f["all_package_names"]


def test_forced_induction_is_not_offered(fleet):
    # The grid serializer does not emit forced_induction, so such a facet could
    # only ever empty the grid.
    assert "forced_inductions" not in dp._dealer_facets(dp._dealer_grid_cars("beamancdjr-com"))


def test_country_facet_only_when_the_lot_spans_more_than_one(fleet):
    f = dp._dealer_facets(dp._dealer_grid_cars("beamancdjr-com"))
    assert f["countries"] == []  # RAM + Jeep are both USA
    f2 = dp._dealer_facets([_card(), _card(id=2, make="Toyota", model="Camry", trim="XSE")])
    assert len(f2["countries"]) > 1


def test_inventory_counts_match_the_cards_that_are_filterable(fleet):
    inv = dp._dealer_inventory("beamancdjr-com")
    assert inv["total"] == 6
    assert inv["new_count"] == 1
    assert inv["used_count"] == 5
    assert len(inv["cars"]) <= dp._GRID_BOOTSTRAP


# ── filter ZIP (main.js's "no ZIP → no filtering" gate) ────────────────


def test_filter_zip_prefers_the_registry_zip():
    assert dp._dealer_filter_zip({"zip_code": "37207"}, None, None) == "37207"


def test_filter_zip_falls_back_to_a_sentinel_for_unlocatable_dealers():
    assert dp._dealer_filter_zip(None, None, None) == dp._UNKNOWN_FILTER_ZIP
    assert dp._ZIP5_RE.match(dp._UNKNOWN_FILTER_ZIP)


# ── JSON endpoints ────────────────────────────────────────────────────


def test_api_dealership_cars_is_dealer_scoped_and_etagged(fleet):
    with app.test_client() as client:
        r = client.get("/api/dealership/beamancdjr-com/cars")
        assert r.status_code == 200
        body = r.get_json()
        assert body["ok"] is True
        assert {c["dealer_id"] for c in body["cars"]} == {"beamancdjr-com"}
        etag = r.headers["ETag"]
        assert "beamancdjr-com" in etag
        r2 = client.get("/api/dealership/beamancdjr-com/cars", headers={"If-None-Match": etag})
        assert r2.status_code == 304
        assert r2.get_data(as_text=True) == ""


def test_api_dealership_filter_options_shape(fleet):
    with app.test_client() as client:
        r = client.get("/api/dealership/beamancdjr-com/filter-options")
        assert r.status_code == 200
        data = r.get_json()
        assert data["ok"] is True
        # main.js's hydrateLazyFacet reads exactly these three keys.
        assert isinstance(data["model_rows"], list)
        assert isinstance(data["trim_rows"], list)
        assert isinstance(data["all_package_names"], list)
        assert data["model_rows"] and len(data["model_rows"][0]) == 2
        assert data["trim_rows"] and len(data["trim_rows"][0]) == 3
        # Same keys as /api/listings/filter-options, package cascade included.
        assert isinstance(data["package_rows"], list)
        assert set(data["package_rows"][0]) == {"make", "model", "name"}
        # car_rows is a page-boot blob, not part of this payload.
        assert "car_rows" not in data


def test_api_dealership_cars_unknown_key_is_404(fleet):
    with app.test_client() as client:
        assert client.get("/api/dealership/ /cars").status_code in (404, 308)


@pytest.mark.parametrize(
    "raw_key",
    [
        "a%0d%0aX-Injected:%201",   # the key reached resp.headers["ETag"] -> 500
        "a%0aX-Injected:%201",
        "%3Cscript%3E",
        "..%2F..%2Fetc%2Fpasswd",
        "99999999999999999999999999999999",
        "%00",
        "a%20b",
    ],
)
def test_unresolvable_keys_get_a_clean_status_not_a_traceback(fleet, raw_key):
    """A key that cannot name a dealer must 404, never reach a stack trace.

    ``a\\r\\nX-Injected: 1`` used to be interpolated straight into the ETag, and
    werkzeug refuses a header value containing a newline — so the endpoint answered
    500 with a traceback for input that should simply not resolve.
    """
    with app.test_client() as client:
        for path in (
            f"/api/dealership/{raw_key}/cars",
            f"/api/dealership/{raw_key}/filter-options",
            f"/dealership/{raw_key}",
        ):
            r = client.get(path)
            assert r.status_code in (400, 404), (path, r.status_code)


def test_etag_only_ever_contains_header_safe_characters(fleet):
    with app.test_client() as client:
        etag = client.get("/api/dealership/beamancdjr-com/cars").headers["ETag"]
    assert etag.startswith('W/"') and etag.endswith('"')
    assert not (set("\r\n\"") & set(etag[3:-1]))


# ── page contract ─────────────────────────────────────────────────────


@pytest.fixture
def page_html(fleet, monkeypatch):
    monkeypatch.setattr(dp, "_find_dealership_by_dealer_id", lambda _id: None)
    monkeypatch.setattr(dp, "_dealer_specials", lambda _id: {"total": 0, "groups": [], "scraped_at": None})
    monkeypatch.setattr(
        dp,
        "_dealer_reviews",
        lambda _id: {"summary": {"count": 0, "avg_rating": None, "addon_fee_count": 0, "addon_fees": []}, "reviews": []},
    )
    with app.test_client() as client:
        r = client.get("/dealership/beamancdjr-com")
        assert r.status_code == 200
        return r.get_data(as_text=True)


def test_page_ships_the_main_js_boot_contract(page_html):
    for element_id in (
        "ds-listings-car-rows",       # gate: main.js does nothing without this
        "ds-listings-bootstrap-grid",
        "ds-listings-saved-ids",
        "results-grid",
        "listings-sort",
        "listings-per-page",
        "listings-pagination",
        "listings-active-chips",
        "compare-tray",
        "listings-zip-input",
    ):
        assert f'id="{element_id}"' in page_html, element_id


def test_page_renders_only_bootstrap_cards_not_the_whole_lot(page_html):
    start = page_html.index('id="ds-listings-bootstrap-grid"')
    blob = page_html[page_html.index(">", start) + 1: page_html.index("</script>", start)]
    assert len(json.loads(blob)) == 6


def test_long_tail_facets_render_only_what_is_checked(fleet, monkeypatch):
    """Model/Trim/Package must stay lazy — that is the whole page-weight budget."""
    monkeypatch.setattr(dp, "_find_dealership_by_dealer_id", lambda _id: None)
    monkeypatch.setattr(dp, "_dealer_specials", lambda _id: {"total": 0, "groups": [], "scraped_at": None})
    monkeypatch.setattr(
        dp,
        "_dealer_reviews",
        lambda _id: {"summary": {"count": 0, "avg_rating": None, "addon_fee_count": 0, "addon_fees": []}, "reviews": []},
    )
    with app.test_client() as client:
        plain = client.get("/dealership/beamancdjr-com").get_data(as_text=True)
        assert 'name="model"' not in plain
        assert 'name="trim"' not in plain
        # ...but a checked option from the URL IS server-rendered, so the first
        # frame filters correctly on a shared link.
        picked = client.get("/dealership/beamancdjr-com?model=1500").get_data(as_text=True)
        assert 'name="model" value="1500"' in picked
        assert "checked" in picked


def test_page_points_lazy_facets_at_the_dealer_scoped_endpoint(page_html):
    assert 'data-facet-options-url="/api/dealership/beamancdjr-com/filter-options"' in page_html
    assert "/api/dealership/beamancdjr-com/cars" in page_html
    # The fleet-wide payload must never be FETCHED from this page.
    assert 'fetch("/api/listings/cars"' not in page_html
