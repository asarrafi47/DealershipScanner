"""The ``search_cars`` SQL ``WHERE`` clause, one builder per filter family.

Pure (no database) except :func:`add_dealer_registry_clause`, which reads the
registry host map from the open connection. Clause text and parameter order are
part of the contract (the search golden pins both), so every builder appends in
the order ``search_cars`` has always used.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from backend.db.repositories.base_repo import _placeholders
from backend.db.repositories.search.countries import _makes_for_countries
from backend.db.repositories.search.query import SearchQuery


@dataclass
class SqlWhere:
    """``WHERE`` text (without the keyword) and its positional ``?`` params."""

    sql: str
    params: list[Any] = field(default_factory=list)

    def add(self, clause: str, params=()) -> None:
        self.sql += clause
        self.params.extend(params)


# ---------------------------------------------------------------------------
# Shared clause shapes
# ---------------------------------------------------------------------------


def add_multi(w: SqlWhere, col: str, values) -> None:
    """``AND col IN (...)`` (exact match)."""
    if values:
        w.add(f" AND {col} IN ({_placeholders(values)})", values)


def add_multi_ci(w: SqlWhere, col: str, values) -> None:
    """Case-insensitive match for scraped text fields (e.g. DODGE vs Dodge)."""
    if values:
        lowered = [str(v).lower().strip() for v in values]
        w.add(f" AND LOWER(TRIM(IFNULL({col}, ''))) IN ({_placeholders(lowered)})", lowered)


# ---------------------------------------------------------------------------
# Equipment needles (packages JSON, listing text, trim/title)
# ---------------------------------------------------------------------------

_EQUIPMENT_SEARCH_COLUMNS = (
    "packages",
    "description",
    "title",
    "trim",
    "engine_description",
)


def _normalize_equipment_needles(
    single: str | None,
    needles_list: list | None,
    needles_all: list | None,
) -> tuple[str | None, list[str], list[str]]:
    """Dedupe and cap equipment needles; return (single, or_list, and_list)."""
    or_needles: list[str] = []
    and_needles: list[str] = []
    seen: set[str] = set()

    def add(target: list[str], raw: str) -> None:
        needle = (raw or "").strip().lower()
        if not needle or needle in seen:
            return
        if len(needle) > 200:
            needle = needle[:200]
        seen.add(needle)
        target.append(needle)

    if needles_all:
        for raw in needles_all:
            add(and_needles, str(raw or ""))
    if single and str(single).strip():
        add(or_needles, str(single))
    if needles_list:
        for raw in needles_list:
            add(or_needles, str(raw or ""))
    if len(or_needles) == 1 and not and_needles:
        return or_needles[0], [], []
    if len(or_needles) > 1 and not and_needles:
        return None, or_needles, []
    if and_needles:
        return None, [], and_needles
    return None, or_needles, []


def _equipment_needle_sql_clause() -> str:
    parts = [
        f"INSTR(LOWER(IFNULL({col}, '')), ?) > 0" for col in _EQUIPMENT_SEARCH_COLUMNS
    ]
    return "(" + " OR ".join(parts) + ")"


def _equipment_needle_params(needle: str) -> list[str]:
    """One binding per column in :func:`_equipment_needle_sql_clause`."""
    return [needle] * len(_EQUIPMENT_SEARCH_COLUMNS)


# ---------------------------------------------------------------------------
# Hidden dealerships
# ---------------------------------------------------------------------------


def _exclude_dealer_ids_clause(exclude_dealer_ids) -> tuple[str, list[str]]:
    """``AND dealer_id NOT IN (?, ...)`` for a user's hidden dealerships ('' when none).

    ``cars.dealer_id`` is the lower-case host slug (0 mixed-case ids across 466
    dealers, 2026-09-28) and the keys are lower-cased here, so the comparison is
    exact and ``idx_cars_dealer_listing`` stays usable; ``LOWER(dealer_id)``
    forced a sequential scan on every search for a user with hidden dealers.
    """
    if not exclude_dealer_ids:
        return "", []
    keys = sorted({str(d or "").strip().lower() for d in exclude_dealer_ids} - {""})
    if not keys:
        return "", []
    return (
        f" AND (dealer_id IS NULL OR dealer_id NOT IN ({_placeholders(keys)}))",
        keys,
    )


# ---------------------------------------------------------------------------
# Filter families, in ``build_where`` order
# ---------------------------------------------------------------------------


def base_where(include_flagged: bool) -> SqlWhere:
    """Active listings; admin-flagged rows hidden unless ``include_flagged``."""
    w = SqlWhere("(COALESCE(listing_active, 1) = 1)")
    if not include_flagged:
        # Admin "marked for review" flag hides the row from public search results.
        w.add(" AND COALESCE(marked_for_review, 0) = 0")
    return w


def add_candidate_ids(w: SqlWhere, candidate_ids) -> None:
    """``id IN (...)`` over the positive ints in ``candidate_ids`` (others dropped)."""
    if not candidate_ids:
        return
    ids = []
    for x in candidate_ids:
        try:
            i = int(x)
            if i > 0:
                ids.append(i)
        except (TypeError, ValueError):
            continue
    if ids:
        w.add(f" AND id IN ({_placeholders(ids)})", ids)


def registry_filter_ids(dealership_registry_id, dealer_registry_ids) -> list[int]:
    """Positive registry ids, single id first, deduped; invalid values ignored."""
    out: list[int] = []
    if dealership_registry_id is not None:
        try:
            dr = int(dealership_registry_id)
        except (TypeError, ValueError):
            dr = 0
        if dr > 0:
            out = [dr]
    if dealer_registry_ids:
        for x in dealer_registry_ids:
            try:
                i = int(x)
                if i > 0 and i not in out:
                    out.append(i)
            except (TypeError, ValueError):
                continue
    return out


def add_vin(w: SqlWhere, vin) -> None:
    """Exact 17-char VIN (spaces stripped, case-insensitive); anything else ignored."""
    if vin and str(vin).strip():
        vnorm = re.sub(r"\s+", "", str(vin).strip().upper())[:20]
        if len(vnorm) == 17:
            w.add(" AND REPLACE(UPPER(TRIM(IFNULL(vin, ''))), ' ', '') = ?", [vnorm])


def makes_after_country_filter(makes, countries):
    """Country of origin filter: resolve countries to makes, combine with explicit makes."""
    makes_for_countries = _makes_for_countries(countries)
    if makes_for_countries is None:
        return makes
    if makes:
        allow_lower = {k.lower(): k for k in makes_for_countries}
        normalized = []
        for m in makes:
            hit = allow_lower.get(str(m).lower().strip())
            if hit is not None:
                normalized.append(hit)
        return list(dict.fromkeys(normalized))
    return list(makes_for_countries)


def add_vehicle_or(w: SqlWhere, vehicle_or) -> None:
    """OR of up to eight (make AND models AND trim needle) branches."""
    vehicle_or_clauses: list[str] = []
    for branch in vehicle_or[:8]:
        if not isinstance(branch, dict):
            continue
        sub_parts: list[str] = []
        mk = branch.get("make")
        if mk and str(mk).strip():
            sub_parts.append("LOWER(TRIM(IFNULL(make, ''))) = ?")
            w.params.append(str(mk).lower().strip())
        branch_models = branch.get("models") or branch.get("model")
        if branch_models:
            if isinstance(branch_models, str):
                branch_models = [branch_models]
            lowered_models = [str(v).lower().strip() for v in branch_models if str(v).strip()]
            if lowered_models:
                sub_parts.append(
                    f"LOWER(TRIM(IFNULL(model, ''))) IN ({_placeholders(lowered_models)})"
                )
                w.params.extend(lowered_models)
        branch_trim = branch.get("trim_contains")
        if branch_trim and str(branch_trim).strip():
            sub_parts.append("INSTR(LOWER(IFNULL(trim, '')), ?) > 0")
            w.params.append(str(branch_trim).strip().lower()[:100])
        if sub_parts:
            vehicle_or_clauses.append("(" + " AND ".join(sub_parts) + ")")
    if vehicle_or_clauses:
        w.add(" AND (" + " OR ".join(vehicle_or_clauses) + ")")


def add_make_model(w: SqlWhere, q: SearchQuery) -> None:
    """``vehicle_or`` (two or more branches) replaces make/model; else make + model."""
    makes = makes_after_country_filter(q.makes, q.countries)
    vehicle_or = q.vehicle_or
    if vehicle_or and isinstance(vehicle_or, list) and len(vehicle_or) >= 2:
        add_vehicle_or(w, vehicle_or)
    else:
        add_multi_ci(w, "make", makes)
        add_multi_ci(w, "model", q.models)


def add_spec_columns(w: SqlWhere, q: SearchQuery) -> None:
    """Trim, fuel, cylinders, transmission, induction, drivetrain, body style."""
    add_multi_ci(w, "trim", q.trims)
    add_multi(w, "fuel_type", q.fuel_types)
    add_multi(w, "cylinders", [int(c) for c in q.cylinders] if q.cylinders else None)
    add_multi(w, "transmission", q.transmissions)
    add_multi(w, "forced_induction", q.forced_inductions)
    add_multi(w, "drivetrain", q.drivetrains)
    add_multi_ci(w, "body_style", q.body_styles)


def add_equipment(w: SqlWhere, q: SearchQuery) -> None:
    """Equipment needles: one single, an OR list, or an AND list (see search_cars)."""
    pkg_single, pkg_or, pkg_and = _normalize_equipment_needles(
        q.packages_json_contains,
        q.packages_json_contains_list,
        q.packages_json_contains_all,
    )
    clause = _equipment_needle_sql_clause()
    if pkg_single:
        w.add(f" AND {clause}", _equipment_needle_params(pkg_single))
    if pkg_or:
        w.sql += " AND (" + " OR ".join([clause] * len(pkg_or)) + ")"
        for needle in pkg_or:
            w.params.extend(_equipment_needle_params(needle))
    if pkg_and:
        for needle in pkg_and:
            w.add(f" AND {clause}", _equipment_needle_params(needle))


def add_trim_needles(w: SqlWhere, trim_contains, trim_contains_list) -> None:
    """Trim substring(s): the list (OR) wins over the single needle."""
    if trim_contains_list:
        needles = [str(t).strip().lower()[:100] for t in trim_contains_list if str(t).strip()]
        if needles:
            w.add(
                " AND (" + " OR ".join(["INSTR(LOWER(IFNULL(trim, '')), ?) > 0"] * len(needles)) + ")",
                needles,
            )
    elif trim_contains and str(trim_contains).strip():
        needle = str(trim_contains).strip().lower()
        if len(needle) > 100:
            needle = needle[:100]
        w.add(" AND INSTR(LOWER(IFNULL(trim, '')), ?) > 0", [needle])


def add_year_range(w: SqlWhere, min_year, max_year) -> None:
    if min_year is not None:
        w.add(" AND year >= ?", [int(min_year)])
    if max_year is not None:
        w.add(" AND year <= ?", [int(max_year)])


def add_price_mileage_caps(w: SqlWhere, max_price, max_mileage) -> None:
    """Caps keep unpriced / zero rows; ``0`` is a real cap, unparseable values are ignored."""
    if max_price is not None:
        try:
            mp = float(max_price)
        except (TypeError, ValueError):
            pass
        else:
            w.add(" AND (price IS NULL OR price <= ? OR price = 0)", [mp])
    if max_mileage is not None:
        try:
            mm = int(float(max_mileage))
        except (TypeError, ValueError):
            pass
        else:
            w.add(" AND (mileage IS NULL OR mileage <= ? OR mileage = 0)", [mm])


def add_condition(w: SqlWhere, cpo_only, inventory_condition) -> None:
    """``cpo_only`` plus ``inventory_condition`` (new / pre_owned / cpo).

    ``inventory_condition`` is the listings UI vocabulary, also emitted by
    parse_natural_query for "new" / "used" / "certified" in free text. Blank
    condition matches neither new nor pre_owned, same as the client filter.
    """
    if cpo_only:
        w.add(" AND is_cpo = 1")
    _cond = str(inventory_condition or "").strip().lower()
    if _cond == "new":
        w.add(" AND LOWER(TRIM(COALESCE(condition, ''))) = 'new' AND COALESCE(is_cpo, 0) <> 1")
    elif _cond == "pre_owned":
        w.add(" AND (is_cpo = 1 OR LOWER(TRIM(COALESCE(condition, ''))) NOT IN ('', 'new'))")
    elif _cond == "cpo":
        w.add(" AND is_cpo = 1")


def add_excluded_dealers(w: SqlWhere, exclude_dealer_ids) -> None:
    excl_clause, excl_params = _exclude_dealer_ids_clause(exclude_dealer_ids)
    w.add(excl_clause, excl_params)


def build_where(q: SearchQuery) -> SqlWhere:
    """Every SQL filter family except the registry clause (that one needs ``conn``)."""
    w = base_where(q.include_flagged)
    add_candidate_ids(w, q.candidate_ids)
    add_vin(w, q.vin)
    add_make_model(w, q)
    add_spec_columns(w, q)
    add_equipment(w, q)
    add_trim_needles(w, q.trim_contains, q.trim_contains_list)
    add_year_range(w, q.min_year, q.max_year)
    add_price_mileage_caps(w, q.max_price, q.max_mileage)
    add_condition(w, q.cpo_only, q.inventory_condition)
    add_excluded_dealers(w, q.exclude_dealer_ids)
    return w


def add_dealer_registry_clause(w: SqlWhere, conn: Any, registry_ids: list[int]) -> None:
    """Registry ids -> cars of those rooftops (stamped id or matching dealer host)."""
    if not registry_ids:
        return
    from backend.listings.dealer_registry_match import (
        dealer_registry_sql_filter,
        registry_id_by_dealer_host,
    )

    host_map = registry_id_by_dealer_host(conn)
    clause, extra = dealer_registry_sql_filter(
        registry_ids,
        host_map,
        placeholders_fn=_placeholders,
    )
    w.add(clause, extra)
