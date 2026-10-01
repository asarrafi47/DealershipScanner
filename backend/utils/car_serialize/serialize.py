"""
Top-level API/UI serialization entry points.

``serialize_car_for_api`` (car detail / embedded payloads) and
``serialize_car_for_listings_grid`` (compact grid cards), plus the price-history
and gallery helpers they use.

Split out of the former monolith ``backend/utils/car_serialize.py`` and
re-exported from the package facade so the public import surface is unchanged.
"""
from __future__ import annotations

import json
import os
from typing import Any

from backend.parsers.base import filter_spyne_gallery_variants
from backend.utils.field_clean import (
    clean_car_row_dict,
    coerce_drivetrain_stored,
    is_effectively_empty,
    normalize_optional_url,
)
from backend.utils.safe_listing_url import normalize_listing_image_url

from ._common import format_display_value
from .api_dealer import apply_listing_vdp_url, apply_location_and_running_costs
from .api_extended import (
    apply_catalog_options,
    apply_color_families,
    apply_extended_plausibility,
    apply_package_names,
)
from .api_identity import (
    apply_display_names,
    apply_first_seen,
    apply_mileage_not_listed,
    copy_row_fields,
    resolve_verified_specs,
)
from .api_media import apply_spin_assets, apply_url_fields
from .api_price import apply_deal_score, apply_msrp_and_payment, withhold_implausible_price
from .api_specs import (
    apply_body_style,
    apply_forced_induction,
    apply_fuel_economy,
    apply_fuel_type_override,
    apply_generation,
    fill_cylinders_from_verified,
    apply_vpic_transmission,
    resolve_drivetrain_display,
    resolve_transmission_display,
)
from .api_withheld import (
    apply_extended_figures,
    apply_tank_and_range,
    gate_ev_only_fields,
    resolve_tank_and_range,
    withhold_contaminated_figures,
)
from .bmw import apply_bmw_model_trim_display
from .condition import fill_derived_condition_for_display
from .engine import _effective_fuel_type_for_display, build_engine_display


def serialize_car_for_api(
    car: dict[str, Any],
    *,
    include_verified: bool = True,
    verified_specs: dict[str, Any] | None = None,
    include_extended_display: bool = True,
) -> dict[str, Any]:
    """
    Shallow copy safe for JSON: strings cleaned + display dashes, numbers preserved.
    Adds ``engine_display``; ``model`` / ``trim`` may be BMW-normalized for display only.

    ``include_verified=False`` skips EPA/trim merge (use for bulk listing payloads).
    Pass ``verified_specs`` when the caller already merged (e.g. car detail page).

    ``include_extended_display=False`` skips the two enrichment lookups that only feed
    ancillary display fields (``ai_engine_specs`` → hp/torque/tow/0-60/tank/curb; factory
    ``catalog_*`` → ``catalog_packages`` / ``catalog_options``). Those fields are set to
    ``None``/left as-is instead. Use it when the caller reads only the core spec-sheet
    fields (e.g. listing-completeness detection), to avoid two per-car reference queries.

    The steps live in ``api_identity`` / ``api_media`` / ``api_dealer`` / ``api_price``
    / ``api_specs`` / ``api_withheld`` / ``api_extended``; their ORDER here is part of
    the contract (key order of the payload, and later steps read earlier ones' keys).
    """
    if not car:
        return {}
    c = clean_car_row_dict(dict(car))
    vs = resolve_verified_specs(c, include_verified=include_verified, verified_specs=verified_specs)
    model_d, trim_d = apply_bmw_model_trim_display(c)
    engine_disp = build_engine_display(c, vs if vs else None)

    # identity, media, dealer link
    out = copy_row_fields(c)
    apply_url_fields(out, c)
    apply_spin_assets(out, c)
    apply_first_seen(out, c)
    apply_listing_vdp_url(out, c)
    apply_display_names(out, model_d, trim_d, engine_disp)
    withhold_implausible_price(out, c)

    # spec sheet (vPIC outranks the feed on transmission)
    apply_forced_induction(out, c)
    apply_fuel_type_override(out, c, engine_disp)
    fill_cylinders_from_verified(out, c, vs)
    td = resolve_transmission_display(c, vs)
    dd = resolve_drivetrain_display(c, vs)
    td = apply_vpic_transmission(out, c, td)
    out["transmission_display"] = td
    out["drivetrain_display"] = dd
    apply_fuel_economy(out, c, vs)

    # extended figures and the ones always withheld
    tank_gal, factory_range = resolve_tank_and_range(c)
    apply_extended_figures(out, c, vs, engine_disp)
    withhold_contaminated_figures(out)
    apply_tank_and_range(out, tank_gal, factory_range)
    apply_generation(out, vs)
    gate_ev_only_fields(out, c)
    if include_extended_display:
        apply_extended_plausibility(out, c)
    apply_catalog_options(out, c, include_extended_display=include_extended_display)
    apply_body_style(out, c, vs)

    # condition, flags, colors, packages
    fill_derived_condition_for_display(c, out)
    apply_mileage_not_listed(out, c)
    apply_color_families(out, c, car)
    apply_package_names(out)

    out["created_at"] = c.get("first_seen_at") or c.get("scraped_at")
    out["price_history"] = _price_history_for_vdp(c)
    apply_location_and_running_costs(
        out, c, engine_disp=engine_disp, vs=vs, tank_gal=tank_gal, factory_range=factory_range
    )
    apply_msrp_and_payment(out, c, include_extended_display=include_extended_display)
    apply_deal_score(out, c)
    return out


def _price_history_for_vdp(car: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Price history for the VDP: ``[{date, price}, ...]`` oldest first.

    Reads operator ``price_provenance_json`` when it stores sweep history; never
    exposes the raw provenance blob on the public car payload.

    Returns a Python list (the template applies ``| tojson`` exactly once).
    The scanner appends a point every time a scan sees a different price than
    the stored row, so two sources that disagree (a VDP price vs an SRP
    payment-shaped price, say) leave an alternating trail with several points
    on one day.  Those are collapsed here: one point per UTC day (the last
    observation wins), then consecutive equal prices are dropped, so what is
    left is only the days on which the listed price actually changed.
    """
    events: list[dict[str, Any]] = []
    raw = car.get("price_provenance_json")
    if raw and str(raw).strip():
        try:
            parsed = json.loads(raw) if isinstance(raw, str) else raw
        except (json.JSONDecodeError, TypeError, ValueError):
            parsed = None
        if isinstance(parsed, list):
            source = parsed
        elif isinstance(parsed, dict):
            source = parsed.get("history") or parsed.get("sweeps") or parsed.get("price_history") or []
        else:
            source = []
        if isinstance(source, list):
            for item in source:
                if not isinstance(item, dict):
                    continue
                when = item.get("date") or item.get("recorded_at") or item.get("scraped_at")
                amt = item.get("price")
                if when is None or amt is None:
                    continue
                try:
                    events.append({"date": str(when), "price": float(amt)})
                except (TypeError, ValueError):
                    continue
    return _dedupe_price_history(events)


def _dedupe_price_history(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep the last point per UTC day, then drop consecutive equal prices."""
    if not events:
        return []
    by_day: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for evt in events:  # scanner appends chronologically; the last write per day wins
        day = str(evt["date"])[:10]
        if day not in by_day:
            order.append(day)
        by_day[day] = evt
    out: list[dict[str, Any]] = []
    for day in order:
        evt = by_day[day]
        if out and out[-1]["price"] == evt["price"]:
            continue
        out.append(evt)
    return out


def _price_history_json_for_vdp(car: dict[str, Any]) -> str:
    """JSON string form of :func:`_price_history_for_vdp` (kept for older callers)."""
    return json.dumps(_price_history_for_vdp(car))


_PRICE_DROP_RECENT_DAYS = max(1, int(os.environ.get("PRICE_DROP_RECENT_DAYS", "14")))


def _latest_price_drop_for_grid(car: dict[str, Any]) -> tuple[float | None, int | None]:
    """
    ``(amount, days_ago)`` for the most recent price drop within
    :data:`_PRICE_DROP_RECENT_DAYS`, else ``(None, None)``.

    Cheap grid-card signal — compares only the last two ``price_provenance_json``
    snapshots (scanner appends chronologically, so ``[-1]`` is newest). Does not
    parse/expose the full history array; see ``_price_history_json_for_vdp`` for that.
    """
    raw = car.get("price_provenance_json")
    if not raw or not str(raw).strip():
        return None, None
    try:
        history = json.loads(raw) if isinstance(raw, str) else raw
    except (json.JSONDecodeError, TypeError, ValueError):
        return None, None
    if not isinstance(history, list) or len(history) < 2:
        return None, None
    prev, latest = history[-2], history[-1]
    if not isinstance(prev, dict) or not isinstance(latest, dict):
        return None, None
    try:
        prev_price = float(prev.get("price"))
        latest_price = float(latest.get("price"))
    except (TypeError, ValueError):
        return None, None
    if latest_price >= prev_price:
        return None, None
    when = latest.get("date")
    if not when:
        return None, None
    import re as _re
    from datetime import datetime, timezone

    try:
        when_dt = datetime.fromisoformat(_re.sub(r"Z$", "+00:00", str(when)))
        if when_dt.tzinfo is None:
            when_dt = when_dt.replace(tzinfo=timezone.utc)
        days_ago = (datetime.now(timezone.utc) - when_dt).days
    except (TypeError, ValueError):
        return None, None
    if days_ago < 0 or days_ago > _PRICE_DROP_RECENT_DAYS:
        return None, None
    return round(prev_price - latest_price, 2), days_ago


_LISTINGS_GRID_GALLERY_MAX = max(1, int(os.environ.get("LISTINGS_GRID_GALLERY_MAX", "4")))


def _package_names_from_raw(packages_raw: Any) -> list[str]:
    names: list[str] = []
    if is_effectively_empty(packages_raw) or str(packages_raw).strip() in ("{}", "[]"):
        return names
    try:
        pkg = json.loads(packages_raw) if isinstance(packages_raw, str) else packages_raw
    except Exception:
        return names
    if not isinstance(pkg, dict):
        return names
    for entry in pkg.get("packages_normalized") or []:
        if isinstance(entry, dict):
            n = (entry.get("canonical_name") or entry.get("name") or "").strip()
            if n:
                names.append(n)
    for n in pkg.get("possible_packages") or []:
        if isinstance(n, str) and n.strip():
            names.append(n.strip())
    return names


def _listings_grid_gallery(raw: Any) -> list[str]:
    if isinstance(raw, list):
        parsed = raw
    elif isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
        except Exception:
            return []
        if not isinstance(parsed, list):
            return []
    else:
        return []
    out = filter_spyne_gallery_variants(parsed)
    safe = [u for u in out if normalize_listing_image_url(u)]
    return safe[:_LISTINGS_GRID_GALLERY_MAX] if safe else []


def _parse_gallery_url_list(raw: Any) -> list[str]:
    if isinstance(raw, list):
        parsed = raw
    elif isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
        except Exception:
            return []
        if not isinstance(parsed, list):
            return []
    else:
        return []
    return [u.strip() for u in parsed if isinstance(u, str) and u.strip()]


# Photo counting is by far the most expensive part of a listings-grid rebuild:
# ~1.8M per-URL heuristic evaluations across the fleet, ~22s of the ~29s build.
# The gallery blob for a given car almost never changes between rebuilds (which
# fire on a 60s cache token), so the same inputs are re-scored over and over
# while holding the GIL — starving every concurrent request, which is what made
# a car page take 30s+.  Memoize on a digest of the exact inputs: the counting
# rule is a pure function of (gallery blob, image_url), so this cannot change a
# single displayed value.  Digest keys (not the raw JSON) keep the table small.
_PHOTO_COUNT_MEMO: dict[bytes, int] = {}
# One entry per active listing plus churn headroom; each entry is ~100 bytes.
_PHOTO_COUNT_MEMO_MAX = 200_000


def _photo_count_memo_key(raw: Any, image_url: Any) -> bytes | None:
    """16-byte digest of the counting inputs, or ``None`` when they aren't hashable text."""
    if raw is None or isinstance(raw, str):
        raw_part = raw or ""
    elif isinstance(raw, (list, tuple)):
        try:
            raw_part = json.dumps(list(raw), separators=(",", ":"))
        except (TypeError, ValueError):
            return None
    else:
        return None
    import hashlib

    h = hashlib.blake2b(digest_size=16)
    h.update(raw_part.encode("utf-8", "replace"))
    h.update(b"\x00")
    h.update(str(image_url or "").encode("utf-8", "replace"))
    return h.digest()


def _count_public_gallery_urls(urls: list[str]) -> int:
    """
    ``len(filter_public_gallery_urls(urls))`` without materialising or sorting.

    The filter keeps the first occurrence of each raw URL that survives the
    fluff heuristic, then collapses the survivors by
    :func:`prefer_full_gallery_url` (a thumbnail and its full-size twin count
    once).  Its ``kept.sort(...)`` only decides which twin is *returned* — the
    number of distinct upgraded URLs does not depend on the order — so a count
    can skip the sort and the two list builds entirely.  Measured over 20,000
    live active rows with the memo emptied: photo counting 6.30s -> 4.41s
    (fleet-scale 22.6s -> 15.8s), whole-grid serialization 7.84s -> 6.24s.
    """
    if not urls:
        return 0
    from backend.vision.url_heuristics import (
        heuristic_listing_gallery_fluff_url,
        prefer_full_gallery_url,
    )

    seen: set[str] = set()
    seen_full: set[str] = set()
    for u in urls:
        if not u or not isinstance(u, str):
            continue
        s = u.strip()
        if not s or s in seen:
            continue
        if heuristic_listing_gallery_fluff_url(s):
            continue
        seen.add(s)
        full = prefer_full_gallery_url(s)
        if full:
            seen_full.add(full)
    return len(seen_full)


def _public_gallery_photo_count(raw: Any, *, image_url: Any = None) -> int:
    """Count public listing photos (same rules as car detail gallery)."""
    key = _photo_count_memo_key(raw, image_url)
    if key is not None:
        hit = _PHOTO_COUNT_MEMO.get(key)
        if hit is not None:
            return hit

    from backend.vision.url_heuristics import heuristic_listing_gallery_fluff_url

    n = _count_public_gallery_urls(_parse_gallery_url_list(raw))
    if not n and image_url:
        iu = str(image_url).strip()
        if iu and not heuristic_listing_gallery_fluff_url(iu):
            n = 1

    if key is not None:
        # Fleet-sized bound: drop the whole table rather than track LRU order —
        # a rebuild refills it in one pass and correctness never depends on it.
        if len(_PHOTO_COUNT_MEMO) >= _PHOTO_COUNT_MEMO_MAX:
            _PHOTO_COUNT_MEMO.clear()
        _PHOTO_COUNT_MEMO[key] = n
    return n


def serialize_car_for_listings_grid(
    car: dict[str, Any],
    *,
    attribution: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Compact listings grid JSON: filter/cascade fields + card display only.

    Skips full ``serialize_car_for_api`` (EPA merge, transmission heuristics, etc.)
    to keep ``GET /api/listings/cars`` fast on SQLite (~2.5k rows).

    *attribution* is this car's entry from ``cars_repo.car_attribution_states`` — the
    caller batches that read for the whole fleet, so the card never looks it up itself.
    """
    if not car:
        return {}
    c = clean_car_row_dict(dict(car))
    from backend.utils.field_clean import normalize_body_style_for_car
    from backend.utils.interior_color_buckets import infer_paint_color_buckets, parse_stored_buckets

    model_d, trim_d = apply_bmw_model_trim_display(c)
    ext_fam = infer_paint_color_buckets(c.get("exterior_color"), c.get("make"))
    _ib = parse_stored_buckets(car.get("interior_color_buckets"))
    int_fam = _ib if _ib else infer_paint_color_buckets(c.get("interior_color"), c.get("make"))

    def num(key: str) -> Any:
        return c.get(key)

    dt_stored = coerce_drivetrain_stored(c.get("drivetrain"))
    if dt_stored == "FWD" and "pickup" in str(c.get("body_style") or "").lower():
        dt_stored = None
    fuel_display = _effective_fuel_type_for_display(c, c.get("engine_description") or "") or c.get("fuel_type")

    out: dict[str, Any] = {
        "id": num("id"),
        "title": format_display_value(c.get("title")),
        "year": num("year"),
        "make": format_display_value(c.get("make")),
        "model": model_d,
        "trim": trim_d,
        "price": num("price"),
        "mileage": num("mileage"),
        "fuel_type": format_display_value(fuel_display),
        "cylinders": num("cylinders"),
        "transmission": format_display_value(c.get("transmission")),
        "drivetrain": format_display_value(dt_stored),
        "body_style": format_display_value(
            normalize_body_style_for_car(
                c.get("body_style"),
                make=c.get("make"),
                model=c.get("model"),
                trim=c.get("trim"),
                title=c.get("title"),
            )
            or c.get("body_style")
        ),
        "exterior_color": format_display_value(c.get("exterior_color")),
        "interior_color": format_display_value(c.get("interior_color")),
        "exterior_color_families": ext_fam,
        "interior_color_families": int_fam,
        "engine_l": c.get("engine_l"),
        "engine_description": c.get("engine_description"),
        "image_url": (img_url := normalize_listing_image_url(c.get("image_url"))),
        # Grid cards only need a primary image; omit duplicate gallery URLs from JSON.
        "gallery": [] if img_url else _listings_grid_gallery(c.get("gallery")),
        "photo_count": _public_gallery_photo_count(c.get("gallery"), image_url=c.get("image_url")),
        "dealer_name": format_display_value(c.get("dealer_name")),
        "dealer_url": normalize_optional_url(c.get("dealer_url")),
        "dealership_registry_id": num("dealership_registry_id"),
        "dealer_id": c.get("dealer_id"),
        "package_names": _package_names_from_raw(c.get("packages")),
        "data_quality_score": num("data_quality_score"),
    }
    out["condition"] = format_display_value(c.get("condition"))
    fill_derived_condition_for_display(c, out)
    # Raw column, matching exactly what search_cars(cpo_only=True) filters on server-side —
    # NOT derived from out["condition"], which can read "Certified" (no "Pre-Owned") for some
    # rows even when is_cpo=1, which would disagree with the SQL-side filter.
    out["is_cpo"] = c.get("is_cpo") in (1, True, "1")
    from backend.utils.mileage_display import mileage_not_listed

    out["mileage_not_listed"] = mileage_not_listed(
        c.get("mileage"), condition=out.get("condition"), is_cpo=out["is_cpo"], year=c.get("year")
    )
    # Whether this listing has a vehicle-history (Carfax/AutoCheck) report linked.
    # We only know a report EXISTS (the URL) — not its contents (owners/accidents),
    # which aren't parsed — so a card chip should say "Carfax", not "1-owner".
    _carfax = c.get("carfax_url")
    out["has_carfax"] = bool(_carfax and str(_carfax).strip())
    price_drop_amount, price_drop_days_ago = _latest_price_drop_for_grid(c)
    out["price_drop_amount"] = price_drop_amount
    out["price_drop_days_ago"] = price_drop_days_ago
    # An advertised monthly payment scraped into the price field ($85, $122)
    # is not a sale price: the card says so, the sorter sinks it with
    # call-for-price, and the price cap filter ignores the figure
    # (visual review 2026-09-28, IH-01 / ES-2). The figure is kept so the
    # card can print "$122/mo advertised" instead of pretending it is unknown.
    from backend.utils.market_price import is_payment_shaped_price  # lazy: market_price imports the DB layer

    out["payment_listed"] = is_payment_shaped_price(out.get("price"), year=c.get("year"))
    # Same cohort-relative guard as serialize_car_for_api (see
    # backend/utils/price_plausibility.py) — in-memory cache, no per-card DB hit.
    if out.get("price") is not None and not out["payment_listed"]:
        try:
            from backend.utils.price_plausibility import implausible_price

            if implausible_price(c, float(out["price"])):
                out["price"] = None
        except (TypeError, ValueError):
            pass
    # Coarse deal score for grid cards (in-memory band cache; no per-card DB hit).
    try:
        from backend.intelligence.deal_score_cache import public_deal_score

        out["deal_score"] = public_deal_score(c)
    except Exception:
        out["deal_score"] = None
    # Softens the dealer line when this listing's own photos (or a proven group feed)
    # contradict the filed rooftop. Adds nothing to the card otherwise.
    from backend.utils.car_serialize.attribution import attribution_public_fields

    out.update(attribution_public_fields(attribution))
    return out


def _format_mileage_mi(mileage: Any) -> str:
    if mileage is None or str(mileage).strip() == "":
        return "—"
    try:
        return f"{int(mileage):,} mi"
    except (TypeError, ValueError):
        return "—"


def build_detail_display_snapshot(
    verified_specs: dict[str, Any],
    ser: dict[str, Any],
) -> dict[str, Any]:
    """
    Text the car detail template would show for key spec rows (for /dev/api/car-debug).
    *ser* must be from serialize_car_for_api(..., verified_specs=verified_specs).
    """
    # Raw cylinder count for dev/debug; user-facing copy is folded into ``engine_display``.
    cc = ser.get("cylinders")
    if cc is None or str(cc).strip() == "":
        cyl_render = "—"
    else:
        cyl_render = str(cc)
    return {
        "year": ser.get("year"),
        "make": ser.get("make"),
        "model": ser.get("model"),
        "trim": ser.get("trim"),
        "mileage_mi": _format_mileage_mi(ser.get("mileage")),
        "engine": ser.get("engine_display"),
        "transmission": ser.get("transmission_display"),
        "drivetrain": ser.get("drivetrain_display"),
        "body_style": ser.get("body_style"),
        "fuel_type": ser.get("fuel_type"),
        "condition": ser.get("condition"),
        "cylinders": cyl_render,
        "efficiency": ser.get("fuel_economy_display"),
        "exterior_color": ser.get("exterior_color"),
        "interior_color": ser.get("interior_color"),
        "vin": ser.get("vin"),
        "stock_number": ser.get("stock_number"),
    }
