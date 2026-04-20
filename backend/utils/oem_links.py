"""OEM / manufacturer resource URLs derived from listing fields (no scraping)."""

from __future__ import annotations

import re

# North American Stellantis brands commonly supported on Mopar VIN tools (Chrysler, Dodge, Jeep, Ram).
_STELLANTIS_MOPAR_MAKES = frozenset(
    {
        "chrysler",
        "dodge",
        "jeep",
        "ram",
    }
)

_VIN_RE = re.compile(r"^[A-HJ-NPR-Z0-9]{17}$", re.IGNORECASE)

MOPAR_VIN_LOOKUP_URL = "https://www.mopar.com/en-us/my-vehicle/vin-lookup.html"


def mopar_vin_lookup_eligible(make: str | None, vin: str | None) -> bool:
    """True when this listing is a Stellantis US brand with a normal 17-char VIN."""
    if not make or not vin:
        return False
    m = str(make).strip().lower()
    if m not in _STELLANTIS_MOPAR_MAKES:
        return False
    return bool(_VIN_RE.match(str(vin).strip()))


def mopar_vin_lookup_url(make: str | None, vin: str | None) -> str | None:
    """
    URL to Mopar's official VIN lookup (specs / features for Stellantis vehicles).

    The page may require entering the VIN in a form; callers should label the link clearly.
    """
    if not mopar_vin_lookup_eligible(make, vin):
        return None
    return MOPAR_VIN_LOOKUP_URL
