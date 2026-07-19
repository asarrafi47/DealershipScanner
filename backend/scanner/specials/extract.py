"""Extract dealer *offer* cards from specials-page HTML (network-free).

Given the HTML of a dealer specials page, :func:`extract_offers_from_html`
returns a list of normalized :class:`Offer` dicts. It is defensive across the
several platforms SoCal dealers run:

* **dealer.com** — ``.special-offer`` cards with ``.offer-content`` titles,
  ``.offeritem/.offerrate/.offerlabel`` payment rows, and a ``.disclaimer``
  block carrying the verbatim fine print (tustintoyota, mbontario).
* **DealerEProcess / Kia-style** — ``.offer-box`` cards whose full text carries
  the title, "Lease for $X per month", MSRP breakdown, VIN and expiry (ggkia).
* a generic **disclaimer-anchored** fallback for other card markups.

The FULL fine-print text is captured verbatim (a later LLM phase consumes it),
plus a raw HTML snippet of the card. Monetary/term fields are parsed with
tolerant regexes and left ``None`` when absent — never guessed.
"""
from __future__ import annotations

import hashlib
import re
from typing import Any, Dict, List

from bs4 import BeautifulSoup, Tag

Offer = Dict[str, Any]

# Multi-word makes must be matched before single tokens so "Mercedes-Benz" and
# "Land Rover" parse as one make.
_MAKES = [
    "Mercedes-Benz", "Land Rover", "Alfa Romeo", "Aston Martin",
    "Rolls-Royce",
    "BMW", "Audi", "Porsche", "Volkswagen", "VW", "Toyota", "Honda", "Nissan",
    "Lexus", "Mazda", "Subaru", "Mitsubishi", "Acura", "Infiniti", "Ford",
    "Chevrolet", "Chevy", "Ram", "Tesla", "Jeep", "Dodge", "Cadillac", "Buick",
    "GMC", "Chrysler", "Lincoln", "Hyundai", "Kia", "Genesis", "Jaguar",
    "Bentley", "Mini", "Ferrari", "Lamborghini", "Fiat", "Maserati", "Volvo",
]
_MAKE_LOOKUP = {m.lower(): m for m in _MAKES}
_MAKE_ALIAS = {"chevy": "Chevrolet", "vw": "Volkswagen"}

# Body-style / filler words to strip when isolating the trim.
_BODY_WORDS = {
    "sedan", "suv", "coupe", "convertible", "hatchback", "wagon", "truck",
    "van", "minivan", "crossover", "sport", "utility", "pickup", "cab",
    "models", "model", "hybrid", "gas", "electric", "ev", "awd", "fwd",
    "rwd", "4wd", "4x4",
}

# --- Candidate offer-card selectors, most specific / reliable first. The first
#     selector that yields at least one card wins (avoids double-counting the
#     same offer via a broader selector). ----------------------------------
_CARD_SELECTORS = [
    ".special-offer",
    ".offer-box",
    ".vehicle-special",
    ".incentive-offer",
    "[data-offer]",
    ".specialOffer",
    ".offer-card",
    "li.offer",
    ".coupon-specials .couponspecials",
]

_MONEY = r"\$\s?([\d,]+(?:\.\d{1,2})?)"

_RE_PAYMENT = re.compile(
    _MONEY + r"\s*(?:/\s?mo\b|/\s?month|per\s+month|a\s+month|monthly|mo\.)",
    re.I,
)
# "$15.19 per month per $1,000 financed" is a rate, NOT the customer's payment.
_RE_PER_THOUSAND = re.compile(r"per\s*\$?\s?1[,.]?000\s*financed", re.I)
_RE_TERM = re.compile(r"(?:for|at)\s+(\d{1,2})\s*[- ]?\s*month", re.I)
_RE_APR = re.compile(r"([\d.]+)\s*%\s*APR", re.I)
_RE_DUE = re.compile(
    _MONEY + r"\s*(?:cash\s+)?due\s+at\s+signing", re.I,
)
_RE_MSRP = re.compile(r"MSRP(?:\s+of)?\s*[:=]?\s*" + _MONEY, re.I)
_RE_MILES_YR = re.compile(
    r"([\d,]+)\s*miles?\s*(?:per\s*year|/\s*year|/\s*yr|a\s*year|annually)", re.I
)
_RE_EXPIRES = re.compile(
    r"expires?\s*[:\-]?\s*(\d{1,2}/\d{1,2}/\d{2,4})", re.I
)
_RE_YEAR_VEH = re.compile(r"\b(19|20)(\d{2})\b")


def _txt(node: Tag | None, sep: str = " ") -> str:
    if node is None:
        return ""
    return re.sub(r"[ \t ]+", " ", node.get_text(sep, strip=True)).strip()


def _money_to_float(raw: str | None) -> float | None:
    if not raw:
        return None
    try:
        return float(raw.replace(",", "").strip())
    except ValueError:
        return None


def _first(pat: re.Pattern, text: str) -> str | None:
    m = pat.search(text)
    return m.group(1) if m else None


def _extract_payment(text: str) -> float | None:
    """First monthly payment that is NOT a "per $1,000 financed" rate."""
    for m in _RE_PAYMENT.finditer(text):
        tail = text[m.end():m.end() + 24]
        if _RE_PER_THOUSAND.search(tail):
            continue
        return _money_to_float(m.group(1))
    return None


_LEASE_SIGNALS = (
    "lease for", "lease payment", "cash due at signing", "due at signing",
    "capitalized cost", "acquisition fee", "miles per year", "/mo lease",
    "lease special", "total monthly payments",
)
_FINANCE_SIGNALS = (
    "apr", "financed", "% financing", "financing for", "per $1,000",
    "per $1000", "months at", "% for", "balloon",
)
_CASH_SIGNALS = (
    "bonus cash", "cash back", "rebate", "cash allowance", "customer cash",
    "cash offer", "conquest cash", "loyalty cash",
)


def _classify_type(text: str) -> str:
    """Score-based classifier. Substring ``"lease"`` alone is unreliable (a
    finance disclaimer often says "Excludes leases"), so weigh phrase signals."""
    low = text.lower()
    if "manager special" in low or "manager's special" in low:
        return "manager"
    lease = sum(1 for s in _LEASE_SIGNALS if s in low)
    finance = sum(1 for s in _FINANCE_SIGNALS if s in low)
    cash = sum(1 for s in _CASH_SIGNALS if s in low)
    # A real APR percentage is a strong finance tell.
    if _RE_APR.search(text):
        finance += 2
    best = max(lease, finance, cash)
    if best == 0:
        return "lease" if "lease" in low else "other"
    if finance == best:
        return "finance"
    if lease == best:
        return "lease"
    return "cash"


def _parse_vehicle(title: str, data_offer: str | None) -> dict:
    """Best-effort year/make/model/trim from the card title and data-offer attr."""
    out = {"vehicle_year": None, "vehicle_make": None, "vehicle_model": None, "vehicle_trim": None}

    # data-offer like "cpo.2026.Mercedes-Benz.GLA.250.Sport Utility"
    if data_offer and "." in data_offer:
        toks = [t for t in data_offer.split(".") if t]
        yr_i = next((i for i, t in enumerate(toks) if re.fullmatch(r"(19|20)\d{2}", t)), None)
        if yr_i is not None:
            out["vehicle_year"] = int(toks[yr_i])
            rest = toks[yr_i + 1:]
            if rest:
                out["vehicle_make"] = _MAKE_LOOKUP.get(rest[0].lower(), rest[0])
            if len(rest) > 1:
                out["vehicle_model"] = rest[1]
            if len(rest) > 2:
                trim = " ".join(w for w in rest[2:] if w.lower() not in _BODY_WORDS)
                out["vehicle_trim"] = trim or None

    # Title fallback / refinement, e.g. "New 2026 Kia K4 LXS Sedan".
    if out["vehicle_year"] is None:
        ym = _RE_YEAR_VEH.search(title or "")
        if ym:
            out["vehicle_year"] = int(ym.group(0))
    if title and (out["vehicle_make"] is None or out["vehicle_model"] is None):
        low = title.lower()
        make_pos = None
        make_val = None
        for ml, canon in _MAKE_LOOKUP.items():
            idx = low.find(ml)
            if idx != -1 and (make_pos is None or idx < make_pos):
                make_pos, make_val = idx, canon
        if make_val:
            out["vehicle_make"] = out["vehicle_make"] or _MAKE_ALIAS.get(make_val.lower(), make_val)
            tail = title[make_pos + len(make_val):].strip()
            words = [w for w in re.split(r"[\s,]+", tail) if w]
            if words and out["vehicle_model"] is None:
                out["vehicle_model"] = words[0].strip("().")
            if len(words) > 1 and not out["vehicle_trim"]:
                trim_words = [w for w in words[1:] if w.lower().strip("().") not in _BODY_WORDS]
                trim = " ".join(trim_words).strip(" ().")
                out["vehicle_trim"] = trim or None
    return out


def _title_from_card(card: Tag, full_text: str) -> str | None:
    """Pull the offer heading. Prefer explicit title-ish nodes; else first line."""
    for sel in (".offer-content", ".make", ".offer-title", "h2", "h3", "h4", "h5", ".offerheading"):
        node = card.select_one(sel)
        if node:
            t = _txt(node)
            # .offer-content includes the disclaimer text; keep only its lead line.
            t = re.split(r"\bView Disclaimer\b|\bLease for\b|\bFinance for\b|\bReceive\b", t)[0].strip()
            if 3 <= len(t) <= 140:
                return t
    # Fallback: first meaningful line of the card text.
    for line in re.split(r"\s{2,}|\n", full_text):
        line = line.strip()
        if len(line) >= 4 and not line.lower().startswith(("view", "lease for", "finance")):
            return line[:140]
    return None


def _fine_print(card: Tag, full_text: str) -> str:
    """The verbatim fine print: the dedicated disclaimer block if present,
    else the whole card text (which for some platforms carries the disclosure)."""
    best = ""
    for el in card.find_all(True):
        classes = " ".join(el.get("class") or []).lower()
        if "disclaimer" in classes or "disclosure" in classes:
            t = _txt(el)
            if len(t) > len(best):
                best = t
    if len(best) >= 40:
        return best
    return full_text


def _offer_from_card(card: Tag, source_url: str) -> Offer | None:
    full_text = _txt(card)
    if len(full_text) < 12:
        return None
    # Require some offer signal so we don't ingest random page furniture.
    low = full_text.lower()
    has_signal = bool(
        _RE_PAYMENT.search(full_text)
        or _RE_APR.search(full_text)
        or "lease" in low
        or "disclaimer" in low
        or "special" in low
        or "offer expires" in low
    )
    if not has_signal:
        return None

    fine_print = _fine_print(card, full_text)
    title = _title_from_card(card, full_text)
    data_offer = card.get("data-offer") if isinstance(card.get("data-offer"), str) else None
    veh = _parse_vehicle(title or "", data_offer)

    payment = _extract_payment(full_text)
    term = _first(_RE_TERM, full_text)
    due = _money_to_float(_first(_RE_DUE, fine_print) or _first(_RE_DUE, full_text))
    msrp = _money_to_float(_first(_RE_MSRP, fine_print) or _first(_RE_MSRP, full_text))
    miles = _first(_RE_MILES_YR, fine_print) or _first(_RE_MILES_YR, full_text)
    expires = _first(_RE_EXPIRES, full_text) or _first(_RE_EXPIRES, fine_print)

    offer_type = _classify_type(full_text)

    raw_html = str(card)
    if len(raw_html) > 20000:
        raw_html = raw_html[:20000]

    offer: Offer = {
        "title": title,
        "type": offer_type,
        "vehicle_year": veh["vehicle_year"],
        "vehicle_make": veh["vehicle_make"],
        "vehicle_model": veh["vehicle_model"],
        "vehicle_trim": veh["vehicle_trim"],
        "payment": payment,
        "term_months": int(term) if term else None,
        "due_at_signing": due,
        "mileage_per_year": int(miles.replace(",", "")) if miles else None,
        "msrp": msrp,
        "expires": expires,
        "fine_print": fine_print or None,
        "source_url": source_url,
        "raw_html_snippet": raw_html,
    }
    offer["offer_hash"] = _offer_hash(offer)
    return offer


def _offer_hash(offer: Offer) -> str:
    basis = "|".join(
        str(offer.get(k) or "")
        for k in ("title", "type", "payment", "term_months", "vehicle_model")
    )
    fp = (offer.get("fine_print") or "")[:400]
    return hashlib.sha1((basis + "|" + fp).encode("utf-8", "replace")).hexdigest()


def _select_cards(soup: BeautifulSoup) -> list[Tag]:
    for sel in _CARD_SELECTORS:
        cards = soup.select(sel)
        if cards:
            return cards
    # Disclaimer-anchored fallback: take the nearest sensible ancestor of each
    # disclaimer block as the card.
    anchors: list[Tag] = []
    seen: set[int] = set()
    for el in soup.find_all(True):
        classes = " ".join(el.get("class") or []).lower()
        if "disclaimer" in classes or "disclosure" in classes:
            anc = el
            for _ in range(3):
                if anc.parent and isinstance(anc.parent, Tag):
                    anc = anc.parent
                    if 60 < len(_txt(anc)) < 4000:
                        break
            if id(anc) not in seen:
                seen.add(id(anc))
                anchors.append(anc)
    return anchors


def extract_offers_from_html(html: str, source_url: str = "") -> List[Offer]:
    """Parse a specials page's HTML into a de-duplicated list of offer dicts."""
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    cards = _select_cards(soup)
    offers: List[Offer] = []
    seen_hashes: set[str] = set()
    for card in cards:
        if not isinstance(card, Tag):
            continue
        offer = _offer_from_card(card, source_url)
        if not offer:
            continue
        h = offer["offer_hash"]
        if h in seen_hashes:
            continue
        seen_hashes.add(h)
        offers.append(offer)
    return offers
