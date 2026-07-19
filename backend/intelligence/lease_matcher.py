"""Read a lease special's fine print, then tell the buyer which inventory cars
actually qualify for the advertised payment.

This is the differentiating feature of the dealership research surface. A lease
ad ("$399/mo") is only meaningful against a *specific* car — trim, MSRP, and
often a single stock/VIN buried in the disclaimer. We already have the car
(the dealer's own inventory rows) and the deal (parsed from the specials page),
so this is a matching problem, not a language problem — no LLM required. We:

  1. Read structured lease terms straight off the offer: the specials scraper
     (:mod:`backend.scanner.specials.extract`) already parses the vehicle,
     payment, term, due, MSRP and expiry from the card. The only extras the
     matcher needs — a stock#/VIN the payment is pinned to, and a mileage
     allowance stated only in prose — are recovered from the verbatim
     ``fine_print`` with plain regex (:func:`parse_lease_terms`).
  2. Match those terms against the dealer's active ``cars`` rows — first on
     year+make+model+trim, falling back to year+make+model, and narrowing to a
     single car when the offer names a stock#/VIN or a distinctive MSRP.
  3. Return the qualifying cars with a confidence and a plain-English note
     ("matches advertised trim — confirm stock # with dealer"), never
     over-promising a VIN-exact guarantee.

Results are cached per offer (:mod:`backend.scanner.specials.lease_matches_store`)
so the page never recomputes on every request.
"""
from __future__ import annotations

import logging
import re
import sqlite3
from typing import Any

logger = logging.getLogger("lease_matcher")

# Top-N qualifying cars surfaced per offer.
_MAX_MATCHES = 6

# A 17-char VIN (no I/O/Q), and a dealer stock number ("stock #NL497846").
_RE_VIN = re.compile(r"\b([A-HJ-NPR-Z0-9]{17})\b")
_RE_STOCK = re.compile(
    r"(?:stock|stk)\s*(?:#|number|no\.?|num)?\s*[:#\-]?\s*([A-Z0-9]{4,})", re.I
)
_RE_MILES_PER_YR = re.compile(
    r"([\d,]+)\s*miles?\s*(?:per\s*year|/\s*year|/\s*yr|a\s*year|annually)", re.I
)
# A total mileage cap over the lease term, e.g. "$0.25/mile over 15,000 miles".
_RE_TOTAL_MILES = re.compile(
    r"(?:over|excess of|based on)\s+([\d,]{4,})\s*(?:total\s+)?miles\b", re.I
)


def _extract_stock_or_vin(text: str) -> str | None:
    """Pull a specific stock#/VIN the offer restricts the advertised payment to,
    if the fine print names one. A full 17-char VIN wins over a stock token."""
    if not text:
        return None
    m = _RE_VIN.search(text)
    if m:
        return m.group(1)
    m = _RE_STOCK.search(text)
    if m:
        tok = m.group(1)
        # A bare 4-digit run after "stock" is usually a year/qty, not a stock#.
        if not tok.isdigit() or len(tok) >= 5:
            return tok
    return None


def _derive_mileage_per_year(offer: dict) -> int | None:
    """Annual mileage allowance: the scraped field first, else parse the fine
    print — a direct 'X miles per year', else a total cap ('over 15,000 miles')
    divided by the term in years ('over 15,000 miles' on a 24-mo lease → 7,500)."""
    direct = _intval(offer.get("mileage_per_year"))
    if direct:
        return direct
    fine = f"{offer.get('fine_print') or ''} {offer.get('title') or ''}"
    m = _RE_MILES_PER_YR.search(fine)
    if m:
        return _intval(m.group(1))
    m = _RE_TOTAL_MILES.search(fine)
    if m:
        total = _intval(m.group(1))
        term = _intval(offer.get("term_months"))
        if total and term and term >= 12:
            return int(round(total / (term / 12.0)))
        if total:
            return total
    return None


def parse_lease_terms(offer: dict) -> dict:
    """Deterministically derive structured lease terms from an offer.

    Sources the vehicle/payment/term/due/MSRP/expiry the specials scraper already
    parsed off the card, and recovers the stock#/VIN and mileage allowance from
    the verbatim fine print with plain regex. No model, no network, never raises.
    Returns the same schema :func:`match_cars` and :func:`summarize_deal` consume.
    """
    fine = f"{offer.get('fine_print') or ''} {offer.get('title') or ''}"
    return _coerce_terms({
        "year": offer.get("vehicle_year"),
        "make": offer.get("vehicle_make"),
        "model": offer.get("vehicle_model"),
        "trim": offer.get("vehicle_trim"),
        "model_code": None,
        "payment": offer.get("payment"),
        "term_months": offer.get("term_months"),
        "due_at_signing": offer.get("due_at_signing"),
        "mileage_per_year": _derive_mileage_per_year(offer),
        "msrp_or_price": offer.get("msrp"),
        "credit_tier": None,
        "expiration": offer.get("expires"),
        "stock_or_vin_specific": _extract_stock_or_vin(fine),
    })


def _num(v: Any) -> float | None:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = re.sub(r"[^0-9.]", "", str(v))
    try:
        return float(s) if s else None
    except ValueError:
        return None


def _intval(v: Any) -> int | None:
    n = _num(v)
    return int(n) if n is not None else None


def _coerce_terms(t: dict) -> dict:
    return {
        "year": _intval(t.get("year")),
        "make": _clean_str(t.get("make")),
        "model": _clean_str(t.get("model")),
        "trim": _clean_str(t.get("trim")),
        "model_code": _clean_str(t.get("model_code")),
        "payment": _num(t.get("payment")),
        "term_months": _intval(t.get("term_months")),
        "due_at_signing": _num(t.get("due_at_signing")),
        "mileage_per_year": _intval(t.get("mileage_per_year")),
        "msrp_or_price": _num(t.get("msrp_or_price")),
        "credit_tier": _clean_str(t.get("credit_tier")),
        "expiration": _clean_str(t.get("expiration")),
        "stock_or_vin_specific": _clean_str(t.get("stock_or_vin_specific")),
    }


def _clean_str(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    if not s or s.lower() in ("null", "none", "n/a", "na", ""):
        return None
    return s


# ── Normalization helpers for matching ───────────────────────────────────────

_MAKE_ALIASES = {
    "mercedes": "mercedes-benz",
    "mercedes benz": "mercedes-benz",
    "mb": "mercedes-benz",
    "vw": "volkswagen",
    "chevy": "chevrolet",
}


def _norm(s: Any) -> str:
    """Lowercase, drop ®/™ and punctuation, collapse whitespace."""
    if s is None:
        return ""
    s = str(s).lower().replace("®", "").replace("™", "")
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _norm_make(s: Any) -> str:
    n = _norm(s)
    return _norm(_MAKE_ALIASES.get(n, n))


def _trim_tokens(s: Any) -> set[str]:
    return set(_norm(s).split()) if s else set()


def _alnum(s: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(s or "").lower())


def _stock_vin_match(token: str | None, vin: str | None, stock: str | None) -> bool:
    """True when an advertised stock#/VIN token identifies this specific car.

    Dealers frequently print a short stock number whose tail is the VIN's last
    six ('NL497846' -> VIN ...497846), so we match on containment and on the
    six-digit tail, in addition to exact stock/VIN equality.
    """
    tok = _alnum(token)
    if not tok or len(tok) < 4:
        return False
    v = _alnum(vin)
    s = _alnum(stock)
    if s and (tok == s or tok in s or s in tok):
        return True
    if v:
        if tok == v or tok in v:
            return True
        tail = tok[-6:]
        if len(tail) >= 5 and v.endswith(tail):
            return True
    return False


# ── Matching ─────────────────────────────────────────────────────────────────


def _car_label(c: dict) -> str:
    """Human label 'YEAR MAKE MODEL TRIM', dropping MODEL when the trim already
    carries it (avoids 'GLE GLE 350 4MATIC SUV')."""
    model, trim = c.get("model"), c.get("trim")
    if trim and model and _norm(model) not in _norm(trim):
        core = f"{model} {trim}"
    else:
        core = trim or model
    return " ".join(str(x) for x in (c.get("year"), c.get("make"), core) if x).strip()


def _fetch_dealer_cars(conn: Any, dealer_id: str) -> list[dict]:
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, vin, stock_number, year, make, model, trim, price, msrp,
               condition, image_url
        FROM cars
        WHERE dealer_id = ? AND COALESCE(listing_active, 1) = 1
        """,
        (dealer_id,),
    )
    return [dict(r) for r in cur.fetchall()]


def _effective(terms: dict | None, offer: dict) -> dict:
    """Merge LLM terms with the offer's structured fields (offer wins when set
    for the headline vehicle fields; LLM fills the gaps like stock/mileage)."""
    t = terms or {}
    return {
        "year": offer.get("vehicle_year") or t.get("year"),
        "make": offer.get("vehicle_make") or t.get("make"),
        "model": offer.get("vehicle_model") or t.get("model"),
        "trim": offer.get("vehicle_trim") or t.get("trim"),
        "msrp": offer.get("msrp") or t.get("msrp_or_price"),
        "stock_or_vin": t.get("stock_or_vin_specific"),
        "model_code": t.get("model_code"),
    }


def match_cars(conn: Any, dealer_id: str, terms: dict | None, offer: dict) -> list[dict]:
    """Find the dealer's active cars that qualify for a lease offer.

    Returns a confidence-ranked list of match dicts:
        {car_id, vin, stock_number, year, make, model, trim, price, msrp,
         condition, image_url, confidence, confidence_label, note}
    """
    eff = _effective(terms, offer)
    o_year = _intval(eff["year"])
    o_make = _norm_make(eff["make"])
    o_model = _norm(eff["model"])
    o_trim_tokens = _trim_tokens(eff["trim"])
    o_msrp = _num(eff["msrp"])
    stock_tok = eff["stock_or_vin"]

    if not (o_make and o_model):
        return []

    cars = _fetch_dealer_cars(conn, dealer_id)
    candidates: list[dict] = []
    for c in cars:
        if _norm_make(c.get("make")) != o_make:
            continue
        c_model = _norm(c.get("model"))
        # model match: equal, or one contains the other (e.g. "GLE" vs "GLE 350")
        if not (c_model == o_model or o_model in c_model or c_model in o_model):
            continue
        if o_year is not None and _intval(c.get("year")) not in (None, o_year):
            continue
        candidates.append(c)

    if not candidates:
        return []

    scored: list[dict] = []
    for c in candidates:
        c_trim_tokens = _trim_tokens(c.get("trim"))
        c_msrp = _num(c.get("msrp"))
        c_price = _num(c.get("price"))

        trim_exact = bool(o_trim_tokens) and o_trim_tokens.issubset(c_trim_tokens)
        stock_hit = _stock_vin_match(stock_tok, c.get("vin"), c.get("stock_number"))
        msrp_hit = (
            o_msrp is not None
            and (
                (c_msrp is not None and abs(c_msrp - o_msrp) <= 100)
                or (c_price is not None and abs(c_price - o_msrp) <= 100)
            )
        )

        conf, label, note = _score(
            trim_exact=trim_exact,
            has_trim=bool(o_trim_tokens),
            stock_hit=stock_hit,
            has_stock=bool(stock_tok),
            msrp_hit=msrp_hit,
            has_msrp=o_msrp is not None,
        )
        scored.append(
            {
                "car_id": c.get("id"),
                "vin": c.get("vin"),
                "stock_number": c.get("stock_number"),
                "year": c.get("year"),
                "make": c.get("make"),
                "model": c.get("model"),
                "trim": c.get("trim"),
                "display": _car_label(c),
                "price": c_price,
                "msrp": c_msrp,
                "condition": c.get("condition"),
                "image_url": c.get("image_url"),
                "confidence": conf,
                "confidence_label": label,
                "note": note,
                "_stock_hit": stock_hit,
            }
        )

    # If the offer names specific stock and we found it, only surface that car —
    # the advertised payment is for that VIN, not every same-trim sibling.
    stock_hits = [m for m in scored if m["_stock_hit"]]
    if stock_hits:
        scored = stock_hits

    scored.sort(key=lambda m: m["confidence"], reverse=True)
    for m in scored:
        m.pop("_stock_hit", None)
    return scored[:_MAX_MATCHES]


def _score(
    *, trim_exact: bool, has_trim: bool, stock_hit: bool, has_stock: bool,
    msrp_hit: bool, has_msrp: bool,
) -> tuple[float, str, str]:
    """Confidence + human note for one car/offer pairing. Deliberately caveated:
    even a stock hit is 'confirm with dealer', never a guarantee."""
    if stock_hit:
        return (
            0.95,
            "high",
            "Matches the exact stock number named in the offer's fine print — "
            "confirm it's still available with the dealer.",
        )
    if trim_exact and msrp_hit:
        return (
            0.85,
            "high",
            "Matches the advertised trim and MSRP — confirm the stock # with the "
            "dealer, as the lease payment may apply to specific vehicles.",
        )
    if trim_exact:
        return (
            0.7,
            "medium",
            "Matches the advertised trim — confirm the stock # with the dealer.",
        )
    if has_trim:
        return (
            0.45,
            "low",
            "Same year/make/model as the offer, but a different trim than "
            "advertised — confirm with the dealer whether it qualifies.",
        )
    return (
        0.5,
        "medium",
        "Matches the advertised year/make/model — confirm the exact trim and "
        "stock # with the dealer.",
    )


# ── Summary + orchestration ──────────────────────────────────────────────────


def summarize_deal(offer: dict, terms: dict | None, matches: list[dict]) -> str:
    """One-line human summary: '$399/mo, 24 mo, $4,999 due, 7,500 mi/yr —
    qualifying 2026 GLE 350 4MATIC in stock'."""
    eff = _effective(terms, offer)
    t = terms or {}
    parts: list[str] = []
    pay = _num(offer.get("payment")) or _num(t.get("payment"))
    if pay:
        parts.append(f"${pay:,.0f}/mo")
    term = offer.get("term_months") or t.get("term_months")
    if term:
        parts.append(f"{int(term)} mo")
    due = offer.get("due_at_signing") if offer.get("due_at_signing") is not None else t.get("due_at_signing")
    due = _num(due)
    if due:
        parts.append(f"${due:,.0f} due")
    miles = offer.get("mileage_per_year") or t.get("mileage_per_year")
    if miles:
        parts.append(f"{int(miles):,} mi/yr")

    head = ", ".join(parts)
    model, trim = eff.get("model"), eff.get("trim")
    if trim and model and _norm(model) not in _norm(trim):
        veh_core = f"{model} {trim}"
    else:
        veh_core = trim or model
    veh = " ".join(
        str(x) for x in (eff.get("year"), eff.get("make"), veh_core) if x
    ).strip()
    n = len(matches)
    if n and veh:
        tail = f"qualifying {veh} in stock" if n == 1 else f"{n} qualifying {veh} in stock"
    elif n:
        tail = f"{n} qualifying car{'s' if n != 1 else ''} in stock"
    else:
        tail = "no matching inventory found"
    return f"{head} — {tail}" if head else tail


def compute_offer_matches(conn: Any, dealer_id: str, offer: dict) -> dict:
    """Parse terms + match inventory for a single lease offer (no caching).

    Returns {extracted, matches, summary, confidence}.
    """
    terms = parse_lease_terms(offer)
    matches = match_cars(conn, dealer_id, terms, offer)
    summary = summarize_deal(offer, terms, matches)
    confidence = matches[0]["confidence"] if matches else None
    return {
        "extracted": terms,
        "matches": matches,
        "summary": summary,
        "confidence": confidence,
    }


def refresh_dealer_lease_matches(
    conn: Any,
    dealer_id: str,
    *,
    force: bool = False,
    only_missing: bool = True,
) -> dict:
    """Compute + cache lease matches for all of a dealer's lease offers.

    ``only_missing`` (default) skips offers whose ``offer_hash`` already has a
    cached row, so re-runs are cheap; ``force`` recomputes everything. Returns a
    small stats dict. Safe to call from a script or a background refresh.
    """
    from backend.scanner.specials.lease_matches_store import (
        get_matches_for_dealer,
        upsert_offer_match,
    )
    from backend.scanner.specials.store import get_specials_for_dealer

    offers = get_specials_for_dealer(conn, dealer_id)
    lease_offers = [o for o in offers if (o.get("type") or "").lower() == "lease"]
    cached = {} if force else get_matches_for_dealer(conn, dealer_id)

    computed = skipped = matched = 0
    for off in lease_offers:
        h = off.get("offer_hash")
        if only_missing and not force and h and h in cached:
            skipped += 1
            continue
        result = compute_offer_matches(conn, dealer_id, off)
        if h:
            upsert_offer_match(
                conn,
                dealer_id,
                h,
                offer_id=off.get("id"),
                extracted=result["extracted"],
                summary=result["summary"],
                matches=result["matches"],
                confidence=result["confidence"],
            )
        computed += 1
        matched += len(result["matches"])
    return {
        "dealer_id": dealer_id,
        "lease_offers": len(lease_offers),
        "computed": computed,
        "skipped": skipped,
        "total_matches": matched,
    }
