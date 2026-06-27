"""OEM ``catalog_*`` table lookups (replaces runtime Complete_Options CSV reads)."""

from __future__ import annotations

from functools import lru_cache
from typing import Any

from backend.enrichment.dictionary_catalog import canonical_make
from backend.enrichment.epa_master_store import _model_search_variants


def _year_int(year: Any) -> int | None:
    try:
        y = int(year)
        return y if 1900 <= y <= 2100 else None
    except (TypeError, ValueError):
        return None


def _fetch_rows(sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
    from backend.db.inventory_db import db_conn

    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description] if cur.description else []
        out: list[dict[str, Any]] = []
        for row in cur.fetchall():
            if isinstance(row, dict):
                out.append(row)
            else:
                out.append(dict(zip(cols, row)))
        return out


@lru_cache(maxsize=256)
def has_catalog_rows(year: int, make: str, model: str) -> bool:
    y = _year_int(year)
    if y is None or not (make or "").strip() or not (model or "").strip():
        return False
    mk = canonical_make(make)
    for md in _model_search_variants(mk, model):
        rows = _fetch_rows(
            """
            SELECT 1 FROM catalog_trims
            WHERE year = ? AND lower(trim(make)) = lower(?)
              AND lower(trim(model)) = lower(?)
            LIMIT 1
            """,
            (y, mk, md),
        )
        if rows:
            return True
    return False


def fetch_catalog_trims(year: Any, make: str, model: str) -> list[dict[str, Any]]:
    y = _year_int(year)
    if y is None:
        return []
    mk = canonical_make(make)
    seen: set[int] = set()
    out: list[dict[str, Any]] = []
    for md in _model_search_variants(mk, model):
        for row in _fetch_rows(
            """
            SELECT id, year, make, model, trim, trim_level,
                   horsepower, torque_lb_ft, base_msrp
            FROM catalog_trims
            WHERE year = ? AND lower(trim(make)) = lower(?)
              AND lower(trim(model)) = lower(?)
            ORDER BY trim
            """,
            (y, mk, md),
        ):
            vid = int(row.get("id") or 0)
            if vid and vid not in seen:
                seen.add(vid)
                out.append(row)
    return out


def _join_list(values: list[str], *, sep: str = ", ") -> str:
    clean = [v.strip() for v in values if (v or "").strip()]
    return sep.join(clean)


def _package_rows(vehicle_id: int) -> list[dict[str, Any]]:
    return _fetch_rows(
        """
        SELECT p.package_name, p.package_code, p.package_msrp,
               f.feature_name
        FROM catalog_packages p
        LEFT JOIN catalog_package_features f ON f.package_id = p.id
        WHERE p.vehicle_id = ?
        ORDER BY COALESCE(p.sort_order, 9999), p.id, COALESCE(f.sort_order, 9999), f.id
        """,
        (int(vehicle_id),),
    )


def _option_rows(vehicle_id: int) -> list[dict[str, Any]]:
    return _fetch_rows(
        """
        SELECT option_name, option_code, option_msrp, category, description
        FROM catalog_options
        WHERE vehicle_id = ?
        ORDER BY id
        """,
        (int(vehicle_id),),
    )


def _color_rows(vehicle_id: int) -> list[str]:
    ext = _fetch_rows(
        """
        SELECT color_name FROM catalog_exterior_colors
        WHERE vehicle_id = ? ORDER BY id
        """,
        (int(vehicle_id),),
    )
    return [str(r.get("color_name") or "").strip() for r in ext if (r.get("color_name") or "").strip()]


def fetch_catalog_option_rows(year: Any, make: str, model: str) -> list[dict[str, str]]:
    """
    Return Complete_Options-shaped dict rows for ``trim_ladder`` adapters.
    """
    trims = fetch_catalog_trims(year, make, model)
    if not trims:
        return []
    out: list[dict[str, str]] = []
    for trim_row in trims:
        vid = int(trim_row.get("id") or 0)
        if vid <= 0:
            continue
        pkg_names: list[str] = []
        pkg_details: list[str] = []
        seen_pkg: set[str] = set()
        for prow in _package_rows(vid):
            name = str(prow.get("package_name") or "").strip()
            feat = str(prow.get("feature_name") or "").strip()
            if name and name not in seen_pkg:
                seen_pkg.add(name)
                pkg_names.append(name)
            if feat:
                pkg_details.append(feat)
        opt_names: list[str] = []
        opt_details: list[str] = []
        for orow in _option_rows(vid):
            name = str(orow.get("option_name") or "").strip()
            desc = str(orow.get("description") or orow.get("category") or "").strip()
            if name:
                opt_names.append(name)
            if desc:
                opt_details.append(desc)
        colors = _color_rows(vid)
        out.append(
            {
                "Trim": str(trim_row.get("trim") or "").strip(),
                "Make": str(trim_row.get("make") or make).strip(),
                "Model": str(trim_row.get("model") or model).strip(),
                "Packages": _join_list(pkg_names),
                "packageDetails": _join_list(pkg_details, sep="; "),
                "Options": _join_list(opt_names),
                "optionDetails": _join_list(opt_details, sep="; "),
                "exteriorColors": _join_list(colors),
            }
        )
    return [r for r in out if (r.get("Trim") or "").strip()]


def lookup_catalog_trim_oem(
    year: Any,
    make: str,
    model: str,
    trim: str,
) -> dict[str, Any]:
    """Best-effort OEM hp/torque/msrp for a listing trim."""
    from backend.enrichment.trim_ladder import _trim_match_score

    needle = (trim or "").strip()
    if not needle:
        return {}
    best: dict[str, Any] | None = None
    best_score = 0
    for row in fetch_catalog_trims(year, make, model):
        name = str(row.get("trim") or "").strip()
        if not name:
            continue
        score = _trim_match_score(needle, [name], make, model)
        if score > best_score:
            best_score = score
            best = row
    if not best or best_score < 60:
        return {}
    return {
        "horsepower": best.get("horsepower"),
        "torque_lb_ft": best.get("torque_lb_ft"),
        "base_msrp": best.get("base_msrp"),
        "trim_level": best.get("trim_level"),
        "catalog_trim_id": best.get("id"),
        "match_score": best_score,
    }


def clear_catalog_caches() -> None:
    has_catalog_rows.cache_clear()
