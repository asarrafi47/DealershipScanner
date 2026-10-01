"""``SearchQuery``: the ``search_cars`` keyword arguments as one value."""
from __future__ import annotations

from dataclasses import dataclass, fields
from typing import Any


@dataclass(frozen=True)
class SearchQuery:
    """Every ``search_cars`` parameter, unchanged (validation happens in the steps).

    Field names match the ``search_cars`` keywords one for one, so
    :meth:`from_call` can take the function's ``locals()`` verbatim.
    """

    makes: Any = None
    models: Any = None
    trims: Any = None
    fuel_types: Any = None
    cylinders: Any = None
    transmissions: Any = None
    drivetrains: Any = None
    forced_inductions: Any = None
    body_styles: Any = None
    exterior_colors: Any = None
    interior_colors: Any = None
    interior_color_bucket_filters: Any = None
    engine_displacement_l_min: Any = None
    engine_displacement_l_max: Any = None
    countries: Any = None
    min_year: Any = None
    max_year: Any = None
    max_price: Any = None
    max_mileage: Any = None
    cpo_only: Any = None
    inventory_condition: Any = None
    zip_code: Any = None
    radius_miles: Any = None
    dealership_registry_id: Any = None
    dealer_registry_ids: Any = None
    candidate_ids: Any = None
    packages_json_contains: Any = None
    packages_json_contains_list: Any = None
    packages_json_contains_all: Any = None
    trim_contains: Any = None
    trim_contains_list: Any = None
    vehicle_or: Any = None
    vin: Any = None
    include_incomplete: bool | None = None
    include_flagged: bool = False
    exclude_dealer_ids: Any = None
    limit: Any = None

    @classmethod
    def from_call(cls, call_locals: dict[str, Any]) -> "SearchQuery":
        """Build from ``search_cars``'s ``locals()`` taken as its first statement."""
        return cls(**{f.name: call_locals[f.name] for f in fields(cls)})
