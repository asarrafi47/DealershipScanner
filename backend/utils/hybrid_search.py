"""
Hybrid inventory search: semantic recall first (Postgres pgvector), then SQLite.

Flow when ``query_text`` is non-empty:
  1. ``query_cars(query_text, top_k)`` → candidate car ids (ranked by embedding similarity)
  2. ``search_cars(..., candidate_ids=candidates)`` → apply exact filters / geo / completeness
  3. Return rows ordered by semantic similarity among rows that pass SQL filters

``query_cars`` is implemented in ``backend.vector.pgvector_service`` (requires ``PGVECTOR_URL``
or ``DATABASE_URL``).

When ``query_text`` is empty: SQL-only (facets, sorting by quality/price as before). Rows that fail
public completeness are omitted unless :func:`backend.db.inventory_db.listings_include_incomplete_cars`
is true (default: on in non-production, off in production; override with ``LISTINGS_INCLUDE_INCOMPLETE_CARS``).

If ``query_text`` is a full 17-character VIN, or a listing id (``#42``, ``id: 42``, or digits only
up to 10 characters), **semantic search is skipped** and ``search_cars`` is used with ``vin=`` or
``candidate_ids=`` when a matching row exists; otherwise the query falls back to normal behavior.
"""
from __future__ import annotations

import json
import logging
import os
import re
from functools import lru_cache
from typing import Any

from backend.db.inventory_db import search_cars
from backend.utils.field_clean import compute_data_quality_score

logger = logging.getLogger(__name__)
_HYBRID_DEBUG = os.environ.get("HYBRID_SEARCH_DEBUG", "").strip().lower() in ("1", "true", "yes")


def _semantic_car_ids(query_text: str, n_results: int) -> list[int]:
    """pgvector car-id recall; empty when unconfigured or on failure (SQL fallback)."""
    from backend.vector.pgvector_service import pgvector_configured, query_cars

    if not pgvector_configured():
        return []
    try:
        return query_cars(query_text, n_results=n_results)
    except Exception as e:
        logger.warning("Semantic vector query failed, falling back to SQL: %s", e)
        return []

# Full VIN: no I, O, Q; spaces ignored (NHTSA 17 character standard).
_LISTING_VIN_FULL_RE = re.compile(r"^[A-HJ-NPR-Z0-9]{17}$", re.I)
_LISTING_ID_PREFIX_RE = re.compile(r"^(?:#|(?:id|car|carid)\s*:\s*)(\d{1,10})\s*$", re.I)


def _normalize_listings_vin_query(q: str) -> str | None:
    s = re.sub(r"\s+", "", (q or "").strip().upper())
    if _LISTING_VIN_FULL_RE.match(s):
        return s
    return None


def _parse_listings_car_id_query(q: str) -> list[int] | None:
    s = (q or "").strip()
    m = _LISTING_ID_PREFIX_RE.match(s)
    if m:
        n = int(m.group(1))
        return [n] if n > 0 else None
    if s.isdigit() and 1 <= len(s) <= 10:
        n = int(s)
        # A bare 4-digit value in the plausible model-year range (e.g. "2020") is
        # almost always a year filter, not a listing id. Require a '#'/'id:' prefix
        # for those; anything else digits-only is still treated as an id.
        if len(s) == 4 and 1900 <= n <= 2100:
            return None
        return [n] if n > 0 else None
    return None


@lru_cache(maxsize=512)
def _expand_inventory_models_cached(make_key: str, model_hint: str) -> tuple[str, ...]:
    """
    Map a user/model hint (e.g. ``X5``, ``Accord``) to distinct ``cars.model`` values in inventory.
    Cached — hot path for smart search.
    """
    hint = (model_hint or "").strip()
    if not hint:
        return ()
    from backend.db.inventory_db import get_conn

    hint_l = hint.lower()
    make = make_key.strip() or None
    conn = get_conn()
    cur = conn.cursor()
    if make:
        cur.execute(
            """
            SELECT DISTINCT model FROM cars
            WHERE make IS NOT NULL AND model IS NOT NULL
              AND LOWER(TRIM(make)) = LOWER(TRIM(?))
              AND (COALESCE(listing_active, 1) = 1)
            """,
            (make,),
        )
    else:
        cur.execute(
            """
            SELECT DISTINCT model FROM cars
            WHERE model IS NOT NULL AND TRIM(model) != ''
              AND (COALESCE(listing_active, 1) = 1)
            """
        )
    rows = [r[0] for r in cur.fetchall() if r and r[0]]
    conn.close()

    matched: list[str] = []
    seen: set[str] = set()
    for md in rows:
        md_l = str(md).lower().strip()
        if md_l == hint_l or md_l.startswith(hint_l + " ") or md_l.startswith(hint_l + "-"):
            if md not in seen:
                seen.add(md)
                matched.append(md)
            continue
        if re.search(rf"(?i)\b{re.escape(hint)}\b", md):
            if md not in seen:
                seen.add(md)
                matched.append(md)
    if matched:
        return tuple(sorted(matched, key=len))
    return (hint,)


def expand_inventory_models(make: str | None, model_hint: str) -> list[str]:
    """Public wrapper around cached model expansion."""
    return list(_expand_inventory_models_cached(str(make or "").strip(), str(model_hint or "").strip()))


def filters_dict_to_search_cars_kwargs(filters: dict[str, Any]) -> dict[str, Any]:
    """Map parse_natural_query() output to search_cars() keyword arguments.

    Optional ``package_contains`` / ``packages_json_contains`` (substring, case-insensitive)
    maps to equipment fields on ``search_cars`` (``packages``, ``description``, ``title``,
    ``trim``, ``engine_description``). ``parse_natural_query`` emits ``packages_json_contains``
    for one feature or ``packages_json_contains_all`` when the user names several (AND).
    GET ``package`` checkboxes map to ``packages_json_contains_list`` (OR across selections).
    """
    if not filters:
        return {}
    out: dict[str, Any] = {}

    vehicle_or_raw = filters.get("vehicle_or")
    if isinstance(vehicle_or_raw, list) and len(vehicle_or_raw) >= 2:
        expanded_branches: list[dict[str, Any]] = []
        for branch in vehicle_or_raw:
            if not isinstance(branch, dict):
                continue
            entry: dict[str, Any] = {}
            mk = branch.get("make")
            if mk and str(mk).strip():
                entry["make"] = str(mk).strip()
            if branch.get("model"):
                entry["models"] = expand_inventory_models(
                    entry.get("make") if isinstance(entry.get("make"), str) else None,
                    str(branch["model"]),
                )
            tc = branch.get("trim_contains")
            if isinstance(tc, str) and tc.strip():
                entry["trim_contains"] = tc.strip()
            if entry:
                expanded_branches.append(entry)
        if len(expanded_branches) >= 2:
            out["vehicle_or"] = expanded_branches

    if not out.get("vehicle_or"):
        if filters.get("make"):
            raw_make = filters["make"]
            out["makes"] = raw_make if isinstance(raw_make, list) else [raw_make]
        if filters.get("model"):
            raw_model = filters["model"]
            if isinstance(raw_model, list):
                expanded: list[str] = []
                mk = filters.get("make")
                mk_val = mk[0] if isinstance(mk, list) and mk else mk
                for m in raw_model:
                    expanded.extend(expand_inventory_models(mk_val if isinstance(mk_val, str) else None, str(m)))
                out["models"] = list(dict.fromkeys(expanded)) if expanded else raw_model
            else:
                mk = filters.get("make")
                mk_val = mk[0] if isinstance(mk, list) and mk else mk
                expanded = expand_inventory_models(mk_val if isinstance(mk_val, str) else None, str(raw_model))
                out["models"] = expanded
    if filters.get("drivetrain"):
        d = filters["drivetrain"]
        out["drivetrains"] = d if isinstance(d, list) else [d]
    if filters.get("body_style"):
        from backend.utils.field_clean import body_styles_for_filter

        b = filters["body_style"]
        raw_list = b if isinstance(b, list) else [b]
        expanded: list[str] = []
        for item in raw_list:
            for v in body_styles_for_filter(str(item)):
                if v not in expanded:
                    expanded.append(v)
        out["body_styles"] = expanded
    if filters.get("exterior_color"):
        out["exterior_colors"] = filters["exterior_color"]
    if filters.get("interior_color"):
        out["interior_colors"] = filters["interior_color"]
    bic = filters.get("interior_color_buckets") or filters.get("interior_color_bucket")
    if bic:
        out["interior_color_bucket_filters"] = bic if isinstance(bic, list) else [str(bic)]
    if filters.get("engine_displacement_l_min") is not None:
        try:
            out["engine_displacement_l_min"] = float(filters["engine_displacement_l_min"])
        except (TypeError, ValueError):
            pass
    if filters.get("engine_displacement_l_max") is not None:
        try:
            out["engine_displacement_l_max"] = float(filters["engine_displacement_l_max"])
        except (TypeError, ValueError):
            pass
    if filters.get("min_year") is not None:
        out["min_year"] = filters["min_year"]
    if filters.get("max_year") is not None:
        out["max_year"] = filters["max_year"]
    if filters.get("max_price") is not None:
        out["max_price"] = filters["max_price"]
    if filters.get("max_mileage") is not None:
        out["max_mileage"] = filters["max_mileage"]
    pkg_all = filters.get("packages_json_contains_all")
    if isinstance(pkg_all, list):
        needles = [str(p).strip() for p in pkg_all if str(p).strip()]
        if needles:
            out["packages_json_contains_all"] = needles
    else:
        pkg = filters.get("packages_json_contains") or filters.get("package_contains")
        if isinstance(pkg, list):
            needles = [str(p).strip() for p in pkg if str(p).strip()]
            if needles:
                out["packages_json_contains_list"] = needles
        elif isinstance(pkg, str) and pkg.strip():
            out["packages_json_contains"] = pkg.strip()
        pkg_list = filters.get("packages_json_contains_list") or filters.get("package")
        if isinstance(pkg_list, list):
            needles = [str(p).strip() for p in pkg_list if str(p).strip()]
            if needles:
                existing = out.get("packages_json_contains_list") or []
                if isinstance(existing, str):
                    existing = [existing]
                merged = list(
                    dict.fromkeys([*(existing if isinstance(existing, list) else []), *needles])
                )
                out["packages_json_contains_list"] = merged
                out.pop("packages_json_contains", None)
    ft = filters.get("fuel_type")
    if ft:
        from backend.utils.field_clean import fuel_types_for_filter

        out["fuel_types"] = fuel_types_for_filter(str(ft))
    cyl = filters.get("cylinders")
    if cyl is not None:
        try:
            out["cylinders"] = [int(cyl)]
        except (TypeError, ValueError):
            pass
    tc = filters.get("trim_contains")
    if isinstance(tc, list):
        needles = [str(t).strip() for t in tc if str(t).strip()]
        if needles:
            out["trim_contains_list"] = needles
    elif isinstance(tc, str) and tc.strip():
        out["trim_contains"] = tc.strip()
    return out


def flask_request_to_search_cars_kwargs(request: Any) -> dict[str, Any]:
    """Build ``search_cars`` kwargs from a Flask ``request`` (GET ``/listings`` query string)."""
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

    dealer_registry_ids_raw = g("dealer_registry_id")
    dealer_registry_ids = None
    if dealer_registry_ids_raw:
        _parsed = []
        for v in dealer_registry_ids_raw:
            try:
                i = int(v.strip())
                if i > 0:
                    _parsed.append(i)
            except (ValueError, AttributeError):
                pass
        if _parsed:
            dealer_registry_ids = _parsed

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

    pkg_needle = scalar("package_contains") or scalar("pkg")
    package_filters = [v.strip() for v in g("package") if v.strip()]
    eng_l_min = scalar("engine_l_min") or scalar("engine_displacement_l_min")
    eng_l_max = scalar("engine_l_max") or scalar("engine_displacement_l_max")
    interior_bucket_filters: list[str] = []
    seen_ib: set[str] = set()
    for key in ("interior_bucket", "interior_color_bucket"):
        for x in g(key):
            s = (x or "").strip().lower()
            if s and s not in seen_ib:
                seen_ib.add(s)
                interior_bucket_filters.append(s)
    out = {
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
        "interior_color_bucket_filters": interior_bucket_filters or None,
        "countries": g("country") or None,
        "max_price": _safe_float(max_price),
        "max_mileage": _safe_int(max_mileage),
        "zip_code": zip_code or None,
        "radius_miles": _safe_float(radius),
        "dealership_registry_id": dealership_registry_id,
        "dealer_registry_ids": dealer_registry_ids,
        "engine_displacement_l_min": _safe_float(eng_l_min),
        "engine_displacement_l_max": _safe_float(eng_l_max),
    }
    if pkg_needle:
        out["packages_json_contains"] = pkg_needle
    if package_filters:
        out["packages_json_contains_list"] = package_filters
        out.pop("packages_json_contains", None)
    return out


def hybrid_search_with_kwargs(
    query_text: str | None,
    sql_kwargs: dict[str, Any],
    *,
    vector_top_k: int = 100,
) -> tuple[list[dict], dict[str, Any]]:
    """
    When ``query_text`` is a 17-char VIN or a listing id (see module doc), resolve with SQL only.

    Otherwise, vector recall first: restrict SQL to semantic candidates, then exact filters.
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

    vnorm = _normalize_listings_vin_query(q)
    if vnorm is not None:
        merged = {**sql_kwargs, "vin": vnorm}
        rows = search_cars(**merged)
        if rows:
            meta["mode"] = "vin_exact"
            meta["sql_count"] = len(rows)
            return _sort_sql_rows(rows), meta

    id_list = _parse_listings_car_id_query(q)
    if id_list is not None:
        merged = {**sql_kwargs, "candidate_ids": id_list}
        rows = search_cars(**merged)
        if rows:
            meta["mode"] = "car_id"
            meta["sql_count"] = len(rows)
            return _sort_sql_rows(rows), meta

    from backend.utils.query_parser import parse_natural_query

    parsed_q = parse_natural_query(q)
    if not _has_structured_filters(parsed_q):
        if not sql_kwargs_has_facet_filters(sql_kwargs):
            meta["mode"] = "no_parse_match"
            meta["sql_count"] = 0
            return [], meta
        rows = search_cars(**sql_kwargs)
        meta["mode"] = "sql_only"
        meta["sql_count"] = len(rows)
        return _sort_sql_rows(rows), meta

    candidate_ids = _semantic_car_ids(q, vector_top_k)

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


_STRUCTURED_FILTER_KEYS = frozenset(
    {
        "make",
        "model",
        "min_year",
        "max_year",
        "max_price",
        "max_mileage",
        "drivetrain",
        "body_style",
        "fuel_type",
        "exterior_color",
        "interior_color",
        "interior_color_buckets",
        "cylinders",
        "engine_displacement_l_min",
        "engine_displacement_l_max",
        "packages_json_contains",
        "packages_json_contains_list",
        "packages_json_contains_all",
        "fully_loaded",
        "trim_contains",
        "vehicle_or",
    }
)


def _has_structured_filters(filters: dict[str, Any] | None) -> bool:
    if not filters:
        return False
    for k in _STRUCTURED_FILTER_KEYS:
        v = filters.get(k)
        if v is None or v == "" or v == []:
            continue
        return True
    return False


_FACET_SQL_KWARG_KEYS = frozenset(
    {
        "makes",
        "models",
        "trims",
        "fuel_types",
        "cylinders",
        "transmissions",
        "drivetrains",
        "body_styles",
        "exterior_colors",
        "interior_colors",
        "interior_color_bucket_filters",
        "countries",
        "max_price",
        "max_mileage",
        "dealership_registry_id",
        "dealer_registry_ids",
        "engine_displacement_l_min",
        "engine_displacement_l_max",
        "packages_json_contains",
        "packages_json_contains_list",
        "packages_json_contains_all",
        "vehicle_or",
    }
)

NO_PARSE_MATCH_MESSAGE = (
    "No vehicles matched that search. Try a make, model, VIN, or equipment term."
)


def sql_kwargs_has_facet_filters(sql_kwargs: dict[str, Any] | None) -> bool:
    """True when GET facet params (not geo alone) constrain the grid."""
    if not sql_kwargs:
        return False
    for k, v in sql_kwargs.items():
        if k not in _FACET_SQL_KWARG_KEYS:
            continue
        if v is None or v == "" or v == []:
            continue
        return True
    return False


def query_is_actionable(
    q: str,
    filters: dict[str, Any] | None = None,
    sql_kwargs: dict[str, Any] | None = None,
) -> bool:
    """Skip vector/SQL-wide fallback when free text did not parse to anything useful."""
    q = (q or "").strip()
    if not q:
        return sql_kwargs_has_facet_filters(sql_kwargs)
    if _normalize_listings_vin_query(q) or _parse_listings_car_id_query(q):
        return True
    if _has_structured_filters(filters):
        return True
    if sql_kwargs_has_facet_filters(sql_kwargs):
        return True
    return False


def _collect_package_needles(filters: dict[str, Any] | None) -> list[str]:
    if not filters:
        return []
    needles: list[str] = []
    seen: set[str] = set()
    pkg = filters.get("packages_json_contains")
    if isinstance(pkg, str) and pkg.strip():
        low = pkg.strip().lower()
        if low not in seen:
            seen.add(low)
            needles.append(low)
    raw_list = filters.get("packages_json_contains_list")
    if isinstance(raw_list, list):
        for item in raw_list:
            low = str(item or "").strip().lower()
            if low and low not in seen:
                seen.add(low)
                needles.append(low)
    raw_all = filters.get("packages_json_contains_all")
    if isinstance(raw_all, list):
        for item in raw_all:
            low = str(item or "").strip().lower()
            if low and low not in seen:
                seen.add(low)
                needles.append(low)
    return needles


def _package_haystack(car: dict) -> str:
    parts = [
        str(car.get("packages") or ""),
        str(car.get("description") or ""),
        str(car.get("title") or ""),
        str(car.get("trim") or ""),
        str(car.get("engine_description") or ""),
        str(car.get("exterior_color") or ""),
        str(car.get("interior_color") or ""),
    ]
    return " ".join(parts).lower()


def _package_match_count(car: dict, needles: list[str]) -> int:
    if not needles:
        return 0
    hay = _package_haystack(car)
    return sum(1 for needle in needles if needle in hay)


def _packages_json_richness(car: dict) -> int:
    raw = car.get("packages")
    if not raw:
        return 0
    try:
        parsed = json.loads(raw) if isinstance(raw, str) else raw
        if isinstance(parsed, dict):
            return len(json.dumps(parsed))
    except (json.JSONDecodeError, TypeError):
        pass
    return len(str(raw))


def _should_skip_vector_rerank(filters: dict[str, Any] | None, query_text: str) -> bool:
    """Skip pgvector embed+query when structured parse already narrows/scores results."""
    if os.environ.get("SMART_SEARCH_SKIP_VECTOR_RERANK", "").strip().lower() in ("1", "true", "yes"):
        return True
    if not filters:
        return False
    if _collect_package_needles(filters):
        return True
    if filters.get("vehicle_or"):
        return True
    if filters.get("make") and filters.get("model"):
        return True
    if filters.get("make") and _has_structured_filters(filters):
        return True
    return False


def _rank_smart_search_results(
    rows: list[dict],
    query_text: str,
    filters: dict[str, Any] | None,
    *,
    vector_top_k: int,
) -> list[dict]:
    """Order SQL results by how closely they match equipment / option intent."""
    if not rows:
        return rows
    needles = _collect_package_needles(filters)
    fully_loaded = bool((filters or {}).get("fully_loaded"))
    vector_rank: dict[int, int] = {}
    q = (query_text or "").strip()
    if q and not _should_skip_vector_rerank(filters, q):
        order = _semantic_car_ids(q, min(max(vector_top_k, len(rows)), 80))
        if order:
            vector_rank = {int(cid): i for i, cid in enumerate(order)}

    from backend.utils.listings_sort import listing_sort_depriority

    def sort_key(car: dict) -> tuple:
        cid = int(car.get("id") or 0)
        return (
            *listing_sort_depriority(car),
            -_package_match_count(car, needles),
            -_packages_json_richness(car) if fully_loaded else 0,
            vector_rank.get(cid, 10**9),
            -(float(car.get("data_quality_score") or 0) or compute_data_quality_score(car)),
            _price_key(car),
        )

    return sorted(rows, key=sort_key)


def _rerank_rows_by_vector(query_text: str, rows: list[dict], *, vector_top_k: int) -> list[dict]:
    """Preserve SQL filter membership; order by semantic similarity when pgvector is available."""
    if not rows or not (query_text or "").strip():
        return rows
    vector_order = _semantic_car_ids(query_text, max(vector_top_k, len(rows)))
    if not vector_order:
        return rows
    if not vector_order:
        return rows
    rank = {int(cid): i for i, cid in enumerate(vector_order)}
    from backend.utils.listings_sort import listing_sort_depriority

    return sorted(
        rows,
        key=lambda c: (
            *listing_sort_depriority(c),
            rank.get(int(c.get("id") or 0), 10**9),
            -(float(c.get("data_quality_score") or 0) or compute_data_quality_score(c)),
            _price_key(c),
        ),
    )


def hybrid_smart_search(
    query_text: str,
    filters: dict[str, Any],
    *,
    vector_top_k: int = 100,
    listing_geo_kwargs: dict[str, Any] | None = None,
) -> tuple[list[dict], dict[str, Any]]:
    """API smart search: natural-language *filters* from ``parse_natural_query`` + vector recall.

    When structured filters are present (make, model, price, etc.), **SQL runs first** so results
    are not limited to an arbitrary semantic candidate pool. Free-text vector recall only re-ranks
    within rows that pass SQL. Vague queries without structured filters still use semantic-first recall.

    ``listing_geo_kwargs``: optional ``zip_code`` + ``radius_miles`` from the listings form so
    smart search respects the same radius as the facet grid (must match ``search_cars`` geo filter).
    """
    q = (query_text or "").strip()
    sql_kwargs = filters_dict_to_search_cars_kwargs(filters or {})
    if listing_geo_kwargs:
        sql_kwargs = {**sql_kwargs, **listing_geo_kwargs}
    meta_extra: dict[str, Any] = {"parsed_filters": dict(filters) if filters else {}}
    package_needles = _collect_package_needles(filters)

    if q and not query_is_actionable(q, filters, sql_kwargs):
        return [], {
            "mode": "no_parse_match",
            "sql_count": 0,
            "vector_candidate_count": 0,
            "vector_candidate_ids_head": [],
            "vector_backend": "pgvector",
            **meta_extra,
        }

    if _normalize_listings_vin_query(q) or _parse_listings_car_id_query(q):
        rows, meta = hybrid_search_with_kwargs(q, sql_kwargs, vector_top_k=vector_top_k)
        meta.update(meta_extra)
        return rows, meta

    if _has_structured_filters(filters):
        rows = search_cars(**sql_kwargs)
        meta: dict[str, Any] = {
            "mode": "sql_first",
            "sql_count": len(rows),
            "vector_candidate_count": 0,
            "vector_candidate_ids_head": [],
            "vector_backend": "pgvector",
            **meta_extra,
        }
        if q or package_needles or (filters or {}).get("fully_loaded"):
            before = rows
            rows = _rank_smart_search_results(rows, q, filters, vector_top_k=vector_top_k)
            if rows is not before:
                meta["mode"] = "sql_first_relevance_rank"
        else:
            rows = _sort_sql_rows(rows)
        return rows, meta

    rows, meta = hybrid_search_with_kwargs(q, sql_kwargs, vector_top_k=vector_top_k)
    meta.update(meta_extra)
    return rows, meta


_PUBLIC_SEARCH_META_KEYS = frozenset({"mode", "sql_count", "vector_candidate_count"})


def public_search_meta(meta: dict[str, Any]) -> dict[str, Any]:
    """Trim hybrid search diagnostics for unauthenticated ``/api/search/smart`` responses (SEC-087)."""
    return {k: meta[k] for k in _PUBLIC_SEARCH_META_KEYS if k in meta}


def _price_key(c: dict) -> float:
    try:
        p = float(c.get("price") or 0)
        return p if p > 0 else float("inf")
    except (TypeError, ValueError):
        return float("inf")


def _sort_sql_rows(rows: list[dict]) -> list[dict]:
    from backend.utils.listings_sort import listing_sort_depriority

    return sorted(
        rows,
        key=lambda c: (
            *listing_sort_depriority(c),
            -(float(c.get("data_quality_score") or 0) or compute_data_quality_score(c)),
            _price_key(c),
        ),
    )
