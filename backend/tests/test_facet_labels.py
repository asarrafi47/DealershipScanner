"""Facet label helpers (backend/db/repositories/facets/labels.py)."""

from __future__ import annotations

import pytest

from backend.db.repositories.facets.labels import _facet_make_valid


@pytest.mark.parametrize("make", ["Rivian", "Lucid", "Polestar", "McLaren", "Aston Martin", "Rolls-Royce", "Scion", "Saturn", "Pontiac", "rivian"])
def test_real_marques_outside_the_original_country_list_are_valid_facet_makes(make):
    assert _facet_make_valid(make)


@pytest.mark.parametrize("make", ["", None, "2022", "Audi A3 premium", "Land", "Flat Trailer"])
def test_junk_makes_stay_out_of_the_facet(make):
    assert not _facet_make_valid(make)
