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
    google_place_id: str | None = None
    google_rating: float | None = None
    google_review_count: int | None = None
    source_dmv: bool = False
    source_osm: bool = False
    source_web: bool = False
    is_dealer: bool = True
    # Google Places metadata (Basic-tier fields — no extra billing beyond the
    # website/rating fields we already request).
    google_primary_type: str = ""          # e.g. "car_dealer", "restaurant"
    google_types: list[str] | None = None  # full category list
    business_status: str = ""              # OPERATIONAL / CLOSED_PERMANENTLY / CLOSED_TEMPORARILY
    oem_brand: str = ""                    # derived from name, e.g. "Toyota"
    phone: str = ""                        # nationalPhoneNumber

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
            "is_dealer": self.is_dealer,
            "osm_id": self.osm_id or "",
            "google_place_id": (self.google_place_id or "").strip() or None,
            "google_rating": self.google_rating,
            "google_review_count": self.google_review_count,
            # Newer Google Places / derived classification fields.
            "oem_brand": (self.oem_brand or "").strip(),
            "business_status": (self.business_status or "").strip(),
            "google_primary_type": (self.google_primary_type or "").strip(),
            "phone": (self.phone or "").strip(),
        }
