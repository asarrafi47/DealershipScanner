"""OEM franchise brand detection for dealer manifest filtering."""

from __future__ import annotations

import re

OEM_BRANDS = [
    "Acura",
    "Alfa Romeo",
    "Aston Martin",
    "Audi",
    "Bentley",
    "BMW",
    "Buick",
    "Cadillac",
    "Chevrolet",
    "Chevy",
    "Chrysler",
    "Dodge",
    "Ferrari",
    "Fiat",
    "Ford",
    "Genesis",
    "GMC",
    "Honda",
    "Hyundai",
    "Infiniti",
    "Jaguar",
    "Jeep",
    "Kia",
    "Lamborghini",
    "Land Rover",
    "Lexus",
    "Lincoln",
    "Lotus",
    "Lucid",
    "Maserati",
    "Mazda",
    "McLaren",
    "Mercedes",
    "Mercedes-Benz",
    "Mini",
    "Mitsubishi",
    "Nissan",
    "Polestar",
    "Porsche",
    "Ram",
    "Rivian",
    "Rolls-Royce",
    "Subaru",
    "Tesla",
    "Toyota",
    "Volkswagen",
    "VW",
    "Volvo",
]


def _compile_oem_brand_pattern() -> re.Pattern[str]:
    escaped = [re.escape(b) for b in sorted(OEM_BRANDS, key=len, reverse=True)]
    return re.compile(r"\b(?:" + "|".join(escaped) + r")\b", re.IGNORECASE)


_OEM_BRAND_WORD_RE = _compile_oem_brand_pattern()


def is_franchised_dealer(dealer_name: str | None) -> bool:
    """True when dealer_name contains a known OEM/franchise brand as a whole word."""
    if dealer_name is None:
        return False
    s = str(dealer_name).strip()
    if not s:
        return False
    return _OEM_BRAND_WORD_RE.search(s) is not None
