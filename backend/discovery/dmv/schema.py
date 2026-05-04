"""Normalized row from a state DMV / licensing bulk file."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class DMVRecord:
    business_name: str
    city: str
    state: str
    street_address: str = ""
    zip_code: str = ""
    website: str | None = None
