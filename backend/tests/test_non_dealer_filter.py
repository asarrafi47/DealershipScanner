"""Unit tests for the discovery non-dealer filter."""
from __future__ import annotations

import pytest

from backend.discovery.non_dealer_filter import is_probable_non_dealer

# Real examples pulled from the national scan that must be EXCLUDED.
NON_DEALERS = [
    # self-service salvage / u-pull yards
    ("Pick-n-Pull", "https://www.picknpull.com"),
    ("U-Pull-&-Pay", "https://www.u-pull-and-pay.com"),
    ("Wrench-A-Part", "https://www.wrenchapart.com"),
    ("Go Pull-It", "https://www.gopullit.com"),
    ("Pick N Pull Auto Dismantlers", None),
    ("LKQ U-Pull", None),
    ("Pull-It Salvage Yard", None),
    ("Wrench A Part", None),
    # car rental
    ("Dirt Cheap Car Rental", "https://dirtcheapcarrental.com"),
    ("Enterprise Rent-A-Car", None),
    # RV dealers
    ("Camping World", "https://www.campingworld.com"),
    ("Bob's RV Center", None),
    # restaurants
    ("North Italia", "https://www.northitalia.com"),
    # direct-to-consumer, no franchise dealers
    ("Rivian", "https://rivian.com"),
    ("Tesla", "https://www.tesla.com"),
    ("Tesla Fremont", None),
    # auction / wholesale
    ("Manheim", "https://www.manheim.com"),
    ("Copart", "https://www.copart.com"),
    ("Carvana", "https://www.carvana.com"),
    ("CarMax", "https://www.carmax.com"),
    ("ADESA", "https://www.adesa.com"),
    ("Southern Auto Auction", None),
]

# Real franchise dealers that must be KEPT.
REAL_DEALERS = [
    ("Gardena Honda", "https://www.gardenahonda.com"),
    ("Toyota Sunnyvale", "https://www.toyotasunnyvale.com"),
    ("Tindol Ford", "https://tindolford.com"),
    ("Hendrick Honda", "https://hendrickhonda.com"),
    ("Mercedes-Benz of Charlotte", "https://mbofcharlotte.com"),
    ("Kia of Irvine", "https://www.kiaofirvine.com"),  # 'rv' inside Irvine must not trip
    ("Harvey Nissan", None),  # 'rv' inside Harvey must not trip
    ("Marv's Chevrolet", None),
    ("Serra Toyota", None),  # 'rv' inside Serra must not trip
    ("Garvey Auto Sales", "https://garveyauto.com"),
]


@pytest.mark.parametrize("name,url", NON_DEALERS)
def test_excludes_non_dealers(name, url):
    assert is_probable_non_dealer(name, url) is True, f"{name!r} should be flagged non-dealer"


@pytest.mark.parametrize("name,url", REAL_DEALERS)
def test_keeps_real_dealers(name, url):
    assert is_probable_non_dealer(name, url) is False, f"{name!r} should be kept as a dealer"


def test_empty_input_is_not_flagged():
    assert is_probable_non_dealer("", "") is False
    assert is_probable_non_dealer(None, None) is False


def test_google_places_primary_type_excluded():
    assert is_probable_non_dealer("Some Place", None, place_primary_type="restaurant") is True
    assert is_probable_non_dealer("Some Place", None, place_primary_type="car_dealer") is False


def test_google_places_types_excluded_only_without_car_dealer():
    # non-dealer type present, no car_dealer -> drop
    assert is_probable_non_dealer("X", None, place_types=["car_rental"]) is True
    # legit dealer that also lists auto_parts_store keeps car_dealer -> keep
    assert (
        is_probable_non_dealer("X", None, place_types=["car_dealer", "auto_parts_store"]) is False
    )


def test_url_only_signal():
    # name alone looks generic, but the host is a known non-dealer platform
    assert is_probable_non_dealer("Auto Center", "https://www.carvana.com/atlanta") is True
