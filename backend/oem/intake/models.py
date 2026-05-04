from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class NormalizedDealer:
    dealer_name: str
    normalized_dealer_name: str
    brand: str
    street: str = ""
    city: str = ""
    state: str = ""
    zip: str = ""
    latitude: float | None = None
    longitude: float | None = None
    phone: str = ""
    root_website: str = ""
    normalized_root_domain: str = ""
    map_reference_url: str = ""
    new_inventory_url: str | None = None
    used_inventory_url: str | None = None
    dealer_group_canonical: str | None = None
    confidence_score: float | None = None
    row_quality: str = "insufficient"
    row_rejection_reasons: list[str] = field(default_factory=list)
    enrichment_ready: bool = False
    partial_group_key: str = ""
    source_oem: str = "bmw_usa"
    source_locator_url: str = ""
    last_verified_at: str = ""
    dedupe_key: str = ""
    merged_raw_intake_ids: list[int] = field(default_factory=list)
    extra: dict[str, Any] = field(default_factory=dict)
