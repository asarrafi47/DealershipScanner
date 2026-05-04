"""Unified dealership row produced by discovery tiers."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class DealerCandidate:
    name: str
    city: str
    state: str
    street_address: str = ""
    zip_code: str = ""
    latitude: float | None = None
    longitude: float | None = None
    dealer_website_url: str = ""
    website_url: str = ""
    osm_id: str | None = None
    source_dmv: bool = False
    source_osm: bool = False
    source_web: bool = False

    def to_db_dict(self) -> dict[str, Any]:
        url = (self.dealer_website_url or self.website_url or "").strip()
        return {
            "name": self.name.strip() or "Unknown Dealer",
            "city": self.city.strip(),
            "state": self.state.strip().upper()[:2] if self.state else "",
            "street_address": self.street_address.strip(),
            "zip_code": self.zip_code.strip(),
            "latitude": self.latitude,
            "longitude": self.longitude,
            "website_url": url,
            "dealer_website_url": url,
            "source_dmv": self.source_dmv,
            "source_osm": self.source_osm,
            "source_web": self.source_web,
            "osm_id": self.osm_id or "",
        }
