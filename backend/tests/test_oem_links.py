"""OEM link helpers (pure, no network)."""

from backend.utils.oem_links import (
    MOPAR_VIN_LOOKUP_URL,
    mopar_vin_lookup_eligible,
    mopar_vin_lookup_url,
)


def test_mopar_eligible_jeep_valid_vin() -> None:
    assert mopar_vin_lookup_eligible("Jeep", "1C4RJFAG0JC123456") is True
    assert mopar_vin_lookup_url("Jeep", "1C4RJFAG0JC123456") == MOPAR_VIN_LOOKUP_URL


def test_mopar_not_eligible_wrong_make() -> None:
    assert mopar_vin_lookup_eligible("BMW", "1C4RJFAG0JC123456") is False
    assert mopar_vin_lookup_url("BMW", "1C4RJFAG0JC123456") is None


def test_mopar_not_eligible_bad_vin() -> None:
    assert mopar_vin_lookup_eligible("RAM", "short") is False
    assert mopar_vin_lookup_url("Dodge", "I" * 17) is None  # I not allowed in VIN alphabet
