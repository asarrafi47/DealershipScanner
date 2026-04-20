"""
Hybrid inventory search: semantic recall first (Postgres pgvector), then SQLite.

Flow when ``query_text`` is non-empty:
  1. ``query_cars(query_text, top_k)`` → candidate car ids (ranked by embedding similarity)
  2. ``search_cars(..., candidate_ids=candidates)`` → apply exact filters / geo / completeness
  3. Return rows ordered by semantic similarity among rows that pass SQL filters

``query_cars`` is implemented in ``backend.vector.pgvector_service`` (requires ``PGVECTOR_URL``
or ``DATABASE_URL``).

When ``query_text`` is empty: SQL-only (facets, sorting by quality/price as before).
"""
from __future__ import annotations

import logging
import os
from typing import Any

from backend.db.inventory_db import search_cars
from backend.utils.field_clean import compute_data_quality_score

logger = logging.getLogger(__name__)
_HYBRID_DEBUG = os.environ.get("HYBRID_SEARCH_DEBUG", "").strip().lower() in ("1", "true", "yes")


def filters_dict_to_search_cars_kwargs(filters: dict[str, Any]) -> dict[str, Any]:
    """Map parse_natural_query() output to search_cars() keyword arguments."""
    if not filters:
        return {}
    out: dict[str, Any] = {}
    if filters.get("make"):
        out["makes"] = [filters["make"]] if not isinstance(filters["make"], list) else filters["make"]
    if filters.get("model"):
        out["models"] = [filters["model"]] if not isinstance(filters["model"], list) else filters["model"]
    if filters.get("drivetrain"):
        d = filters["drivetrain"]
        out["drivetrains"] = d if isinstance(d, list) else [d]
    if filters.get("body_style"):
        b = filters["body_style"]
        out["body_styles"] = b if isinstance(b, list) else [b]
    if filters.get("exterior_color"):
        out["exterior_colors"] = filters["exterior_color"]
    if filters.get("interior_color"):
        out["interior_colors"] = filters["interior_color"]
    if filters.get("min_year") is not None:
        out["min_year"] = filters["min_year"]
    if filters.get("max_year") is not None:
        out["max_year"] = filters["max_year"]
    if filters.get("max_price") is not None:
        out["max_price"] = filters["max_price"]
    if filters.get("max_mileage") is not None:
        out["max_mileage"] = filters["max_mileage"]
    return out


def flask_request_to_search_cars_kwargs(request: Any) -> dict[str, Any]:
    """Build ``search_cars`` kwargs from a Flask ``request`` (GET /search)."""
    g = request.args.getlist

    def scalar(key: str) -> str:
        vals = [v.strip() for v in request.args.getlist(key) if v.strip()]
        return vals[-1] if vals else ""

    zip_code = scalar("zip_code")
    radius = scalar("radius")
    max_price = scalar("max_price")
    max_mileage = scalar("max_mileage")
    reg_id_raw = scalar("dealership_registry_id")
    dealership_registry_id = None
    if reg_id_raw:
        try:
            dealership_registry_id = int(reg_id_raw)
        except ValueError:
            dealership_registry_id = None

    def _safe_float(raw: str) -> float | None:
        if not raw:
            return None
        try:
            return float(raw)
        except ValueError:
            return None

    def _safe_int(raw: str) -> int | None:
        if not raw:
            return None
        try:
            return int(float(raw))
        except ValueError:
            return None

    return {
        "makes": g("make") or None,
        "models": g("model") or None,
        "trims": g("trim") or None,
        "fuel_types": g("fuel_type") or None,
        "cylinders": g("cylinders") or None,
        "transmissions": g("transmission") or None,
        "drivetrains": g("drivetrain") or None,
        "body_styles": g("body_style") or None,
        "exterior_colors": g("exterior_color") or None,
        "interior_colors": g("interior_color") or None,
        "countries": g("country") or None,
        "max_price": _safe_float(max_price),
        "max_mileage": _safe_int(max_mileage),
        "zip_code": zip_code or None,
        "radius_miles": _safe_float(radius),
        "dealership_registry_id": dealership_registry_id,
    }


def hybrid_search_with_kwargs(
    query_text: str | None,
    sql_kwargs: dict[str, Any],
    *,
    vector_top_k: int = 100,
) -> tuple[list[dict], dict[str, Any]]:
    """
    Vector recall first when ``query_text`` is set: restrict SQL to semantic candidates, then exact filters.
    """
    q = (query_text or "").strip()
    meta: dict[str, Any] = {
        "mode": "sql_only",
        "vector_candidate_count": 0,
        "sql_count": 0,
        "vector_candidate_ids_head": [],
        "vector_backend": "pgvector",
    }

    if not q:
        rows = search_cars(**sql_kwargs)
        meta["sql_count"] = len(rows)
        return _sort_sql_rows(rows), meta

    candidate_ids: list[int] = []
    try:
        from backend.vector.pgvector_service import query_cars

        candidate_ids = query_cars(q, n_results=vector_top_k)
    except Exception as e:
        logger.warning("Semantic vector query failed, falling back to SQL: %s", e)
        candidate_ids = []

    meta["vector_candidate_count"] = len(candidate_ids)
    meta["vector_candidate_ids_head"] = candidate_ids[:20]

    if candidate_ids:
        merged = {**sql_kwargs, "candidate_ids": candidate_ids}
        rows = search_cars(**merged)
        by_id = {int(c["id"]): c for c in rows}
        ordered = [by_id[cid] for cid in candidate_ids if cid in by_id]
        meta["mode"] = "semantic_then_sql"
        meta["sql_count"] = len(ordered)
        if _HYBRID_DEBUG:
            logger.info(
                "hybrid semantic_then_sql q=%r vector_top_k=%s sql_count=%s head=%s",
                q,
                len(candidate_ids),
                len(ordered),
                meta["vector_candidate_ids_head"],
            )
        return ordered, meta

    rows = search_cars(**sql_kwargs)
    meta["mode"] = "sql_fallback_no_semantic_index"
    meta["sql_count"] = len(rows)
    return _sort_sql_rows(rows), meta


def hybrid_smart_search(
    query_text: str,
    filters: dict[str, Any],
    *,
    vector_top_k: int = 100,
) -> tuple[list[dict], dict[str, Any]]:
    """API smart search: natural-language *filters* from ``parse_natural_query`` + vector recall."""
    sql_kwargs = filters_dict_to_search_cars_kwargs(filters or {})
    meta_extra = {"parsed_filters": dict(filters) if filters else {}}
    rows, meta = hybrid_search_with_kwargs(query_text, sql_kwargs, vector_top_k=vector_top_k)
    meta.update(meta_extra)
    return rows, meta


def _price_key(c: dict) -> float:
    try:
        p = float(c.get("price") or 0)
        return p if p > 0 else float("inf")
    except (TypeError, ValueError):
        return float("inf")


def _sort_sql_rows(rows: list[dict]) -> list[dict]:
    return sorted(
        rows,
        key=lambda c: (
            -(float(c.get("data_quality_score") or 0) or compute_data_quality_score(c)),
            _price_key(c),
        ),
    )
