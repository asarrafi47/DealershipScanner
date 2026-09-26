from __future__ import annotations

from typing import Any


_COVERAGE_FIELDS = (
    "vin", "year", "make", "model", "trim", "mileage", "condition",
    "price", "exterior_color", "interior_color",
    "engine_description", "transmission", "drivetrain", "body_style",
    "fuel_type", "mpg_city",
)


# Fields where 0 is a legitimate reading (e.g. a brand-new car's odometer)
# and must not be treated as "missing".
_ZERO_IS_VALID_FIELDS = frozenset({"mileage"})


def _has_value(v: dict[str, Any], key: str) -> bool:
    val = v.get(key)
    if val is None:
        return False
    if isinstance(val, str) and not val.strip():
        return False
    if isinstance(val, (int, float)) and val == 0 and key not in _ZERO_IS_VALID_FIELDS:
        return False
    return True


def compute_dealer_coverage(
    vehicles: list[dict[str, Any]],
    dealer_id: str = "",
) -> dict[str, Any]:
    """Return field coverage percentages for a batch of scraped vehicles."""
    n = len(vehicles)
    if not n:
        return {"dealer_id": dealer_id, "count": 0, "coverage": {}}

    def pct(count: int) -> float:
        return round(count / n * 100, 1)

    coverage: dict[str, Any] = {}
    for field in _COVERAGE_FIELDS:
        count = sum(1 for v in vehicles if _has_value(v, field))
        coverage[field] = {"count": count, "pct": pct(count)}

    # Gallery: at least one HTTPS image URL
    gallery_count = sum(
        1 for v in vehicles
        if (
            (isinstance(v.get("gallery"), list) and any(
                isinstance(u, str) and u.startswith("https://") for u in v["gallery"]
            ))
            or (isinstance(v.get("image_url"), str) and v["image_url"].startswith("https://"))
        )
    )
    coverage["gallery"] = {"count": gallery_count, "pct": pct(gallery_count)}

    return {"dealer_id": dealer_id, "count": n, "coverage": coverage}


def aggregate_coverage_reports(reports: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Roll up several ``compute_dealer_coverage`` results (one per dealer) into a
    single scan-wide coverage report, weighted by each dealer's vehicle count.
    """
    totals: dict[str, int] = {}
    n_total = 0
    for report in reports:
        if not isinstance(report, dict):
            continue
        cov = report.get("coverage") or {}
        n = int(report.get("count") or 0)
        if n <= 0:
            continue
        n_total += n
        for field, stat in cov.items():
            totals[field] = totals.get(field, 0) + int((stat or {}).get("count") or 0)

    if not n_total:
        return {"count": 0, "dealers": 0, "coverage": {}}

    coverage = {
        field: {"count": count, "pct": round(count / n_total * 100, 1)}
        for field, count in totals.items()
    }
    return {"count": n_total, "dealers": len(reports), "coverage": coverage}


def format_coverage_log(report: dict[str, Any]) -> str:
    """One-line summary suitable for a logger.info call."""
    cov = report.get("coverage", {})
    n = report.get("count", 0)
    dealer = report.get("dealer_id", "")
    parts = [f"{k}={v['pct']}%" for k, v in cov.items() if v["pct"] < 100.0]
    gaps = ", ".join(parts) if parts else "all fields 100%"
    return f"Coverage [{dealer}] n={n}: {gaps}"
