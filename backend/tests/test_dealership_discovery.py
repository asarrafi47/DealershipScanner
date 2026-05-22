"""Dealership discovery pipeline (mocked HTTP)."""
from __future__ import annotations

import json
import warnings

warnings.filterwarnings("ignore", message=r".*doesn't match a supported version.*")
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

from backend.discovery.candidate import DealerCandidate
from backend.discovery.merge import merge_and_dedupe
from backend.discovery.normalize import (
    looks_like_dealer_website,
    normalize_url,
    normalize_us_state_to_code,
)
from backend.discovery.osm import _parse_osm_element
from backend.discovery.pipeline import run_discovery
from backend.discovery.web import ddg_find_dealer_url
from backend.discovery.zcta_gazetteer import (
    _load_gazetteer_rows,
    iter_zcta_zip_codes,
    lookup_zcta_row,
    resolve_zip_center,
    suggested_search_radius_miles,
)

FIXTURES = Path(__file__).resolve().parent / "fixtures"
OVERPASS_JSON = FIXTURES / "dealership_discovery_overpass_sample.json"


def test_zip_validation_rejects_bad_input():
    with pytest.raises(ValueError, match="zip"):
        run_discovery("abc", 10.0, fill_urls_via_ddg=False)


def test_radius_validation():
    with pytest.raises(ValueError, match="radius"):
        run_discovery("28210", 0.0, fill_urls_via_ddg=False)


def test_normalize_url_rejects_google():
    assert normalize_url("https://google.com/search?q=dealer") is None


def test_normalize_us_state_full_name():
    assert normalize_us_state_to_code("North Carolina") == "NC"
    assert normalize_us_state_to_code("nc") == "NC"
    assert normalize_us_state_to_code("") == ""


def test_looks_like_dealer_website():
    assert looks_like_dealer_website("https://someford.example.com") is True
    assert looks_like_dealer_website("https://duckduckgo.com/l/?uddg=x") is False


def test_parse_osm_element():
    raw = json.loads(OVERPASS_JSON.read_text(encoding="utf-8"))
    el = raw["elements"][0]
    c = _parse_osm_element(el)
    assert c is not None
    assert c.osm_id == "n/900000001"
    assert c.source_osm is True
    assert "28210" in (c.zip_code or "")


def test_merge_json_export_rows_inserts(tmp_path):
    import json

    from backend.discovery.manifest_merge import merge_json_export_rows

    manifest = tmp_path / "dealers.json"
    manifest.write_text("[]\n", encoding="utf-8")
    stats = merge_json_export_rows(
        [{"name": "Example Motors", "url": "https://example.com"}],
        manifest_path=manifest,
    )
    assert stats["inserted"] == 1
    assert stats["skipped"] == 0
    data = json.loads(manifest.read_text(encoding="utf-8"))
    assert len(data) == 1
    assert data[0]["name"] == "Example Motors"
    assert data[0]["url"] == "https://example.com"
    assert data[0]["dealer_id"] == "example-com"


def test_is_franchised_dealer_word_boundaries():
    pytest.importorskip("duckdb")
    from backend.discovery.overture_discovery import is_franchised_dealer

    assert is_franchised_dealer("Tindol Ford") is True
    assert is_franchised_dealer("Hendrick Honda") is True
    assert is_franchised_dealer("mercedes-benz of charlotte") is True
    assert is_franchised_dealer("Oxford Auto") is False
    assert is_franchised_dealer("Macchia Motors") is False
    assert is_franchised_dealer("Craig Auto") is False
    assert is_franchised_dealer("") is False
    assert is_franchised_dealer(None) is False


def test_merge_json_skips_no_url(tmp_path):
    from backend.discovery.manifest_merge import merge_json_export_rows

    manifest = tmp_path / "dealers.json"
    manifest.write_text("[]\n", encoding="utf-8")
    stats = merge_json_export_rows(
        [{"name": "No Site", "url": ""}],
        manifest_path=manifest,
    )
    assert stats["skipped"] == 1
    assert manifest.read_text(encoding="utf-8").strip() == "[]"


def test_coordinate_enrich_respects_tagged_zip_state():
    """Nearest centroid may fall in SC near the border; tagged 28273 is NC."""
    from backend.discovery.coordinate_enrich import enrich_candidate_location_fields

    c = DealerCandidate(
        name="Toyota",
        city="Charlotte",
        state="",
        street_address="9101 South Boulevard",
        zip_code="28273",
        latitude=34.9423509,
        longitude=-80.9708715,
        source_osm=True,
    )
    enrich_candidate_location_fields(c)
    assert c.state == "NC"


def test_merge_dedupe_fuzzy_same_dealer():
    a = DealerCandidate(
        name="Acme Ford",
        city="Charlotte",
        state="NC",
        street_address="1 Main St",
        zip_code="28210",
        latitude=35.05,
        longitude=-80.85,
        dealer_website_url="https://acmeford.example.com",
        source_dmv=True,
    )
    b = DealerCandidate(
        name="Acme Ford Inc",
        city="Charlotte",
        state="NC",
        street_address="",
        zip_code="28210",
        latitude=35.051,
        longitude=-80.851,
        osm_id="n/1",
        source_osm=True,
    )
    m = merge_and_dedupe([a, b])
    assert len(m) == 1
    assert m[0].source_dmv and m[0].source_osm
    assert m[0].osm_id == "n/1"


@patch("backend.discovery.osm.requests.Session.post")
@patch("backend.discovery.pipeline.ddg_find_dealer_url")
def test_run_discovery_overpass_timeout_returns_empty_osm(mock_ddg, mock_post):
    mock_post.side_effect = requests.Timeout("network")
    mock_ddg.return_value = None
    rows = run_discovery("28210", 25.0, fill_urls_via_ddg=False)
    assert isinstance(rows, list)


@patch("backend.discovery.pipeline.fetch_google_places_dealerships")
def test_run_discovery_google_places_fixture(mock_gp):
    """Pipeline uses Google Places tier (OSM Overpass removed from orchestration)."""
    mock_gp.return_value = [
        DealerCandidate(
            name="Fixture Auto Sales",
            city="Charlotte",
            state="NC",
            zip_code="28210",
            latitude=35.051,
            longitude=-80.849,
            dealer_website_url="https://fixture-auto-example.invalid",
        )
    ]
    rows = run_discovery("28210", 25.0, fill_urls_via_ddg=False)
    names = {r.name for r in rows}
    assert "Fixture Auto Sales" in names


def test_iter_zcta_zip_codes_sorted(tmp_path):
    _load_gazetteer_rows.cache_clear()
    p = tmp_path / "zcta.txt"
    line = "GEOID|GEOIDFQ|ALAND|AWATER|ALAND_SQMI|AWATER_SQMI|INTPTLAT|INTPTLONG\n"
    p.write_text(
        line + "28211|x|0|0|25.|0.|35.0|-80.0\n" + "28210|x|0|0|36.|0.|35.1|-80.1\n",
        encoding="utf-8",
    )
    assert iter_zcta_zip_codes(p) == ["28210", "28211"]


def test_suggested_search_radius_clamped():
    from backend.discovery.zcta_gazetteer import ZctaGazetteerRow

    r = suggested_search_radius_miles(ZctaGazetteerRow("28210", 0, 0, 100.0))
    assert 5.0 <= r <= 45.0


def test_zcta_gazetteer_pipe_parse(tmp_path):
    _load_gazetteer_rows.cache_clear()
    p = tmp_path / "zcta.txt"
    p.write_text(
        "GEOID|GEOIDFQ|ALAND|AWATER|ALAND_SQMI|AWATER_SQMI|INTPTLAT|INTPTLONG\n"
        "28210|x|0|0|50.0|0.|35.05|-80.85\n",
        encoding="utf-8",
    )
    row = lookup_zcta_row(p, "28210")
    assert row is not None
    assert abs(row.lat - 35.05) < 1e-6
    assert abs(row.lon - (-80.85)) < 1e-6
    assert row.aland_sqmi == 50.0


def test_resolve_zip_center_prefers_gazetteer(tmp_path):
    _load_gazetteer_rows.cache_clear()
    p = tmp_path / "zcta.txt"
    p.write_text(
        "GEOID|GEOIDFQ|ALAND|AWATER|ALAND_SQMI|AWATER_SQMI|INTPTLAT|INTPTLONG\n"
        "28210|x|0|0|10.|0.|12.34|-56.78\n",
        encoding="utf-8",
    )

    def boom(*_a, **_k):
        raise AssertionError("pgeocode should not run when gazetteer matches")

    with patch("backend.db.geo.zip_to_coords", side_effect=boom):
        lat, lon, src = resolve_zip_center(
            "28210",
            project_root=None,
            gazetteer_path=p,
        )
    assert src == "zcta_gazetteer"
    assert lat == 12.34 and lon == -56.78


@patch("backend.discovery.pipeline.fetch_google_places_dealerships")
def test_run_discovery_seed_zip_scope(mock_gp):
    mock_gp.return_value = [
        DealerCandidate(
            name="Inside ZIP",
            city="Charlotte",
            state="NC",
            zip_code="28210",
            latitude=35.051,
            longitude=-80.849,
        ),
        DealerCandidate(
            name="Adjacent ZIP",
            city="Charlotte",
            state="NC",
            zip_code="28211",
            latitude=35.06,
            longitude=-80.84,
        ),
    ]
    rows = run_discovery(
        "28210",
        25.0,
        fill_urls_via_ddg=False,
        within_seed_zip_only=True,
    )
    assert len(rows) == 1
    assert rows[0].name == "Inside ZIP"


@patch("backend.discovery.web._ddg_html_find_dealer_url", return_value=None)
def test_ddg_skips_aggregator_urls(mock_html):
    resp = MagicMock()
    resp.json.return_value = {
        "AbstractURL": "https://www.google.com/foo",
        "RelatedTopics": [],
    }
    resp.raise_for_status = MagicMock()
    sess = MagicMock()
    sess.get.return_value = resp
    assert ddg_find_dealer_url("x", "y", "NC", session=sess) is None
