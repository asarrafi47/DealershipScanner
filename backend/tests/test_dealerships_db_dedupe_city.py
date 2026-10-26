"""B-028: fuzzy upsert dedupe must not merge rooftops differing by city or osm_id."""

from __future__ import annotations

import pytest

from backend.db import dealerships_db as ddb

pytestmark = pytest.mark.pg


def _row(city, osm=None):
    return {
        "name": "Smith Auto Group",
        "dealer_website_url": "https://smithauto.example",
        "city": city,
        "state": "NC",
        "osm_id": osm,
    }


def test_different_city_same_site_is_not_merged(pg_db):
    a = ddb.upsert_discovery_row(_row("Charlotte"))
    b = ddb.upsert_discovery_row(_row("Raleigh"))
    assert a != b


def test_same_city_still_merges(pg_db):
    a = ddb.upsert_discovery_row(_row("Charlotte"))
    b = ddb.upsert_discovery_row(_row("charlotte"))
    assert a == b


def test_differing_osm_id_not_merged(pg_db):
    a = ddb.upsert_discovery_row(_row("Charlotte", "node/1"))
    b = ddb.upsert_discovery_row(_row("Charlotte", "node/2"))
    assert a != b
