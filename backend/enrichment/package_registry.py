"""Observed package/option value registry — the price book behind the build sheet.

Every time we see a package or option priced on a real OEM window sticker, or
named in a dealer listing, we record an *observation*. Observations aggregate
into a deduped canonical ``package_values`` row per (year, make, model, trim,
kind, code|name) holding the best price we've seen and how sure we are of it.

Authority order for adopting a price: OEM sticker > dealer listing > estimate.
A sticker price always overrides a dealer-listing price; a dealer price only
fills a slot that has no price yet. This is how a single parsed sticker teaches
the value of that option for every future car of the same configuration.

Reference data only — mirrors the reference-store rule: never written onto cars.
"""
from __future__ import annotations

import re
from functools import lru_cache
from typing import Any, Iterable

from backend.enrichment.knowledge_engine import _conn

# Source authority + baseline confidence. Higher authority wins price conflicts.
# Authority ranks how much a source may overrule another when they disagree.
#
# 'sticker_photo' is an option price read off a PHOTOGRAPH of a Monroney found in a
# dealer's gallery (see backend/scripts/feed_package_registry_from_vision.py). It sits at
# 2, level with dealer_listing and deliberately below the parsed PDF, for one specific
# reason: a sticker shot at an angle shifts the price column against the label column, so
# an option can inherit its neighbour's price. That is documented three times in this
# corpus (a $1,450 "First Aid Kit", centre caps priced at a destination charge, and a
# $1,550 "Decoding additional functions"). A parsed PDF cannot fail that way. Guards catch
# most of it; "mostly caught" is not "structurally impossible", and this ladder is where
# that difference belongs. Raise it if the photo-sourced values prove out against the PDFs.
_SOURCE_AUTHORITY = {"oem_sticker": 3, "dealer_listing": 2, "sticker_photo": 2, "estimate": 1}
# Keep every source present in BOTH maps. Registering one and not the other raises a
# bare KeyError deep inside record_package_observations, where a caller reads it as
# "unknown source" rather than "half-wired source".
_SOURCE_CONFIDENCE = {
    "oem_sticker": 0.95, "dealer_listing": 0.6, "sticker_photo": 0.75, "estimate": 0.3,
}

# Trailing nouns that are noise for matching ("Premium Package" == "Premium").
_NAME_SUFFIX_RE = re.compile(
    r"\b(package|pkg|packages|group|grp|option|options|equipment|edition)\b", re.I
)
_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")


def normalize_name(name: Any) -> str:
    """Lowercase, drop package/option nouns and punctuation for matching."""
    s = str(name or "").strip().lower()
    if not s:
        return ""
    s = _NAME_SUFFIX_RE.sub(" ", s)
    s = _NON_ALNUM_RE.sub(" ", s)
    return " ".join(s.split())


_PACKAGE_HINT_RE = re.compile(r"\b(package|pkg|group|preferred pkg|conv(?:enience)?)\b", re.I)


def classify_kind(name: Any) -> str:
    """'package' when the name reads like a package/group, else 'option'."""
    return "package" if _PACKAGE_HINT_RE.search(str(name or "")) else "option"


def _clean(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _price_or_none(v: Any) -> float | None:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()


def record_package_observations(
    car: dict[str, Any],
    source: str,
    items: Iterable[dict[str, Any]],
    *,
    conn: Any = None,
) -> int:
    """Record package/option sightings for one car and fold them into the registry.

    ``source`` is 'oem_sticker' | 'dealer_listing' | 'estimate'. ``items`` are
    dicts with keys: kind ('package'|'option'), name, and optional code, price,
    category. Returns the number of observations written. Safe to re-run — the
    observation UNIQUE key makes it idempotent per (vin, kind, name, source).
    """
    if source not in _SOURCE_AUTHORITY:
        raise ValueError(f"unknown source: {source!r}")
    year = car.get("year")
    try:
        year = int(year) if year not in (None, "") else None
    except (TypeError, ValueError):
        year = None
    make = _clean(car.get("make"))
    model = _clean(car.get("model"))
    if not (make and model):
        return 0
    trim = _clean(car.get("trim")) or ""
    vin = _clean(car.get("vin"))
    now = _now_iso()
    authority = _SOURCE_AUTHORITY[source]
    confidence = _SOURCE_CONFIDENCE[source]

    own_conn = conn is None
    if own_conn:
        conn = _conn()
    written = 0
    try:
        cur = conn.cursor()
        for item in items:
            kind = (item.get("kind") or "option").strip().lower()
            if kind not in ("package", "option"):
                kind = "option"
            raw_name = _clean(item.get("name"))
            name_norm = normalize_name(raw_name)
            if not name_norm:
                continue
            code = _clean(item.get("code"))
            price = _price_or_none(item.get("price"))
            category = _clean(item.get("category"))
            # Key on the normalized NAME, not the code: dealer listings never
            # carry OEM codes, so name-keying is what lets a coded sticker fold
            # into the codeless listing sightings of the same package. Two
            # genuinely different packages sharing a normalized name on one trim
            # is vanishingly rare; the code is kept as a display attribute.
            match_key = name_norm[:200]
            price_authority = authority if price is not None else 0

            cur.execute(
                """
                INSERT INTO package_observations
                    (vin, year, make, model, trim, kind, code, raw_name, name_norm,
                     price, category, source, seen_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (vin, kind, name_norm, source) DO UPDATE SET
                    price = COALESCE(EXCLUDED.price, package_observations.price),
                    code = COALESCE(EXCLUDED.code, package_observations.code),
                    category = COALESCE(EXCLUDED.category, package_observations.category),
                    seen_at = EXCLUDED.seen_at
                """,
                (vin, year, make, model, trim, kind, code, raw_name, name_norm,
                 price, category, source, now),
            )

            sticker_inc = 1 if source == "oem_sticker" else 0
            cur.execute(
                """
                INSERT INTO package_values
                    (year, make, model, trim, kind, match_key, name_display, name_norm,
                     code, msrp, msrp_source, msrp_authority, category,
                     observation_count, sticker_count, confidence, first_seen_at, last_seen_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?)
                ON CONFLICT (year, make, model, trim, kind, match_key) DO UPDATE SET
                    observation_count = package_values.observation_count + 1,
                    sticker_count = package_values.sticker_count + ?,
                    -- adopt the new price only when it beats the incumbent's authority
                    msrp = CASE
                        WHEN EXCLUDED.msrp IS NOT NULL
                             AND EXCLUDED.msrp_authority >= package_values.msrp_authority
                        THEN EXCLUDED.msrp ELSE package_values.msrp END,
                    msrp_source = CASE
                        WHEN EXCLUDED.msrp IS NOT NULL
                             AND EXCLUDED.msrp_authority >= package_values.msrp_authority
                        THEN EXCLUDED.msrp_source ELSE package_values.msrp_source END,
                    msrp_authority = CASE
                        WHEN EXCLUDED.msrp IS NOT NULL
                             AND EXCLUDED.msrp_authority >= package_values.msrp_authority
                        THEN EXCLUDED.msrp_authority ELSE package_values.msrp_authority END,
                    -- prefer a real code / sticker display name once we have one
                    code = COALESCE(package_values.code, EXCLUDED.code),
                    name_display = CASE
                        WHEN EXCLUDED.msrp_authority > package_values.msrp_authority
                        THEN EXCLUDED.name_display ELSE package_values.name_display END,
                    category = COALESCE(package_values.category, EXCLUDED.category),
                    confidence = GREATEST(package_values.confidence, EXCLUDED.confidence),
                    last_seen_at = EXCLUDED.last_seen_at
                """,
                (year, make, model, trim, kind, match_key, raw_name, name_norm,
                 code, price, source if price is not None else None, price_authority,
                 category, sticker_inc, confidence, now, now,
                 sticker_inc),
            )
            written += 1
        if own_conn:
            conn.commit()
    finally:
        if own_conn:
            try:
                conn.close()
            except Exception:
                pass
    return written


@lru_cache(maxsize=4096)
def _lookup_cached(
    year: int | None, make: str, model: str, trim: str
) -> tuple[tuple, tuple]:
    conn = None
    try:
        conn = _conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT kind, code, name_display, msrp, msrp_source, category,
                   confidence, sticker_count, observation_count
            FROM package_values
            WHERE year IS NOT DISTINCT FROM ?
              AND lower(make)=lower(?) AND lower(model)=lower(?)
              AND (lower(trim)=lower(?) OR trim='')
            ORDER BY kind, (msrp IS NULL), msrp DESC
            """,
            (year, make, model, trim),
        )
        packages: list[tuple] = []
        options: list[tuple] = []
        for row in cur.fetchall():
            (kind, code, name, msrp, msrp_source, category, conf, scount, ocount) = row
            entry = (name, code, msrp, msrp_source, category, conf, scount, ocount)
            (packages if kind == "package" else options).append(entry)
        return tuple(packages), tuple(options)
    except Exception:
        return (), ()
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def _entry_to_dict(e: tuple) -> dict[str, Any]:
    (name, code, msrp, msrp_source, category, conf, scount, ocount) = e
    return {
        "name": name,
        "code": code,
        "msrp": msrp,
        "msrp_source": msrp_source,
        "category": category,
        "confidence": conf,
        "sticker_count": scount,
        "observation_count": ocount,
        "from_sticker": bool(scount),
    }


def lookup_package_values(
    year: Any, make: Any, model: Any, trim: Any
) -> dict[str, list[dict[str, Any]]]:
    """Observed packages/options with prices for a configuration.

    Returns ``{"packages": [...], "options": [...]}``; empty lists when we have
    no observations yet. Includes trim-specific and model-wide (trim='') rows.
    """
    make_s = _clean(make)
    model_s = _clean(model)
    if not (make_s and model_s):
        return {"packages": [], "options": []}
    try:
        y = int(year) if year not in (None, "") else None
    except (TypeError, ValueError):
        y = None
    trim_s = _clean(trim) or ""
    packages, options = _lookup_cached(y, make_s, model_s, trim_s)
    return {
        "packages": [_entry_to_dict(e) for e in packages],
        "options": [_entry_to_dict(e) for e in options],
    }


# ---------------------------------------------------------------------------
# Where the numbers in this registry come from, measured 2026-08-02
#
# Every price in it is OBSERVED, not inferred. Of 25,773 ``package_values`` rows,
# 2,376 carry an ``msrp`` and all 2,376 have ``msrp_source='oem_sticker'`` /
# ``msrp_authority=3``; no row has ever adopted a dealer-listing or ``estimate``
# price, because dealer-listing observations carry no price (36,384 of the
# 47,830 observations are ``oem_sticker``, 11,446 ``dealer_listing``). Nothing
# here is averaged, interpolated or modelled.
#
# WHAT WAS STILL BEING PRESENTED AS OBSERVED WHEN IT WAS NOT. :func:`price_for_package`
# scored an exact (year, trim) match highest and then took the best remaining row
# for the make+model — any model year, any trim — and returned it with
# ``from_sticker=True`` and no indication that it had widened. 331 of the 1,547
# priced (make, model, name) groups span more than one model year and 272 span
# more than one trim; 163 hold more than one distinct price for the same name,
# and the spread inside a group reaches $114,500-$198,300. So the widened answer
# was frequently a real sticker price for a different car, shown as this car's
# option price with a "sticker" badge on it.
#
# The observation is still returned — it is real and useful — but it now says
# which (year, trim) it was seen on and whether that is this car's, and the
# caller decides. See ``generated_spec_sheet._catalog_equipment``, which prices
# only exact matches and keeps the rest as unpriced named packages.
#
# NOT FIXED HERE, and it is a writer-side bug, not a lookup one: 266 priced rows
# hold a base-vehicle MSRP parsed off a window sticker as though it were an
# option — ``name_display='Base'`` at $198,300, ``'i8Roadster'`` at $163,300.
# They are unreachable from the build sheet, whose only input is the package
# names a listing states, but they are wrong in the store. Fixing that means
# changing what ``backend.enrichment.window_sticker_service`` feeds
# :func:`record_package_observations`, which is not this file.
# ---------------------------------------------------------------------------


@lru_cache(maxsize=8192)
def _price_cached(
    make: str, model: str, year: int | None, trim: str, name_norm: str
) -> tuple:
    """Best observed price for a named package, and the config it was seen on.

    Returns ``(price, source, from_sticker, observed_year, observed_trim, exact)``
    or ``(None, None, False, None, None, False)`` when we have never seen a price
    for that name on this make+model.

    ``exact`` is True only when the observation's model year and trim are this
    car's. The query still widens — a price from another year is worth knowing —
    but the caller is told, instead of being handed a number that looks like a
    quote for the car in front of it.
    """
    conn = None
    try:
        conn = _conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT msrp, msrp_source, sticker_count, year, trim,
                   (CASE WHEN year IS NOT DISTINCT FROM ? THEN 2 ELSE 0 END
                    + CASE WHEN lower(trim)=lower(?) THEN 1 ELSE 0 END) AS spec
            FROM package_values
            WHERE lower(make)=lower(?) AND lower(model)=lower(?)
              AND name_norm=? AND msrp IS NOT NULL
            ORDER BY spec DESC, (sticker_count > 0) DESC, confidence DESC,
                     observation_count DESC
            LIMIT 1
            """,
            (year, trim, make, model, name_norm),
        )
        row = cur.fetchone()
        if not row:
            return (None, None, False, None, None, False)
        msrp, source, sticker_count, obs_year, obs_trim, spec = row
        return (msrp, source, bool(sticker_count), obs_year, obs_trim, spec == 3)
    except Exception:
        return (None, None, False, None, None, False)
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def price_for_package(
    make: Any, model: Any, year: Any, trim: Any, name: Any
) -> dict[str, Any]:
    """Look up the observed price of a package/option named on a listing.

    Returns ``{"price", "source", "from_sticker", "observed_year",
    "observed_trim", "exact_config"}``. ``price`` is ``None`` when we have no
    observed price for that name on this make+model.

    ``exact_config`` False means the price is real but was observed on a
    DIFFERENT year or trim of this nameplate. Callers must not print such a
    price as this car's without saying whose it is — see the block comment above
    for how far the widening reaches and how far apart the prices inside one
    group get.
    """
    make_s = _clean(make)
    model_s = _clean(model)
    name_norm = normalize_name(name)
    miss = {
        "price": None,
        "source": None,
        "from_sticker": False,
        "observed_year": None,
        "observed_trim": None,
        "exact_config": False,
    }
    if not (make_s and model_s and name_norm):
        return miss
    try:
        y = int(year) if year not in (None, "") else None
    except (TypeError, ValueError):
        y = None
    price, source, from_sticker, obs_year, obs_trim, exact = _price_cached(
        make_s, model_s, y, _clean(trim) or "", name_norm
    )
    if price is None:
        return miss
    return {
        "price": price,
        "source": source,
        "from_sticker": from_sticker,
        "observed_year": obs_year,
        "observed_trim": _clean(obs_trim),
        "exact_config": exact,
    }


def clear_lookup_cache() -> None:
    _lookup_cached.cache_clear()
    _price_cached.cache_clear()
