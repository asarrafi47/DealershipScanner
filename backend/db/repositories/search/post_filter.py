"""Python-side ``search_cars`` filters that run after the SQL on hydrated rows.

Paint families and interior buckets are inferred from free-text colors, and the
displacement range parses ``engine_description``; none of that is expressible in
the portable SQL, so the ranked id scan over-fetches and these trim the rows.
"""
from __future__ import annotations

from dataclasses import dataclass

from backend.db.repositories.search.query import SearchQuery
from backend.utils.car_serialize import car_matches_engine_displacement_l_range
from backend.utils.interior_color_buckets import (
    infer_paint_color_buckets,
    parse_stored_buckets,
    row_matches_interior_bucket_filter,
)


def _normalized_interior_bucket_filters(raw) -> set[str] | None:
    if not raw:
        return None
    from backend.utils.interior_color_buckets import ALLOWED_BUCKETS

    sel = {str(x).strip().lower() for x in raw if str(x).strip()}
    sel &= ALLOWED_BUCKETS
    return sel or None


def _row_exterior_families(car: dict) -> set[str]:
    return set(infer_paint_color_buckets(car.get("exterior_color"), car.get("make")))


def _row_interior_families(car: dict) -> set[str]:
    stored = set(parse_stored_buckets(car.get("interior_color_buckets")))
    if stored:
        return stored
    return set(infer_paint_color_buckets(car.get("interior_color"), car.get("make")))


def _float_or_none(value) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


@dataclass(frozen=True)
class PostSqlFilters:
    ext_families: set[str] | None
    int_families: set[str] | None
    interior_buckets: set[str] | None
    eng_lo: float | None
    eng_hi: float | None

    @classmethod
    def from_query(cls, q: SearchQuery) -> "PostSqlFilters":
        return cls(
            ext_families=_normalized_interior_bucket_filters(q.exterior_colors),
            int_families=_normalized_interior_bucket_filters(q.interior_colors),
            interior_buckets=_normalized_interior_bucket_filters(q.interior_color_bucket_filters),
            eng_lo=_float_or_none(q.engine_displacement_l_min),
            eng_hi=_float_or_none(q.engine_displacement_l_max),
        )

    def apply(self, cars: list[dict]) -> list[dict]:
        out = cars
        if self.ext_families:
            out = [c for c in out if _row_exterior_families(c) & self.ext_families]
        if self.int_families:
            out = [c for c in out if _row_interior_families(c) & self.int_families]
        if self.interior_buckets:
            out = [c for c in out if row_matches_interior_bucket_filter(c, self.interior_buckets)]
        if self.eng_lo is not None or self.eng_hi is not None:
            out = [
                c for c in out
                if car_matches_engine_displacement_l_range(c, self.eng_lo, self.eng_hi)
            ]
        return out
