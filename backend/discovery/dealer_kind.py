"""Sort a dealer into franchise, independent, national_retailer, marketplace or unknown.

A documented, offline heuristic for reports only (docs/independent_dealers/ANALYSIS.md
section 3). It never decides what is scanned or shown. Standard library only, so that
``backend/scripts/dealer_mix_report.py`` can run it inside the web container even when
the deployed release predates this file (the report command sends both files).

Signals, first match wins:

1. ``marketplace``: the host is a listing marketplace or classifieds site.
2. ``national_retailer``: the name or host is a national used-car chain.
3. ``franchise``: a car brand appears in the name (whole word) or in the host
   (substring, with a short list of place names that only look like a brand).
4. ``franchise``: no brand word, but the dealer sells new cars (at least 10 new rows and
   at least 20% of its rows), which in the US only a franchise store can do.
5. ``independent``: a used-lot word in the name or host ("auto sales", "autos",
   "motorcars", "pre-owned", ...).
6. ``independent``: no brand word, used cars only (``rows_new == 0`` and ``rows_used > 0``)
   and, when the makes are known, at least ``MIN_MAKES_FOR_MIXED_LOT`` different makes.
7. ``unknown``: nothing above fired.

Precision on the 172 dealers of the 2026-11-03 fixtures was checked by hand
(ANALYSIS.md section 3.2). Known weak spots: a franchise group whose host names no brand
and whose new cars were not captured (one-condition failure) reads as independent; a
used-only lot with a brand word in its name ("Toyota Town Used Cars") reads as franchise.
"""
from __future__ import annotations

import re
from collections.abc import Iterable

KINDS = ("franchise", "independent", "national_retailer", "marketplace", "unknown")

# Listing marketplaces and classifieds: never scanned (ANALYSIS.md section 6).
MARKETPLACE_HOSTS = (
    "cars.com", "autotrader.com", "cargurus.com", "truecar.com", "carfax.com",
    "edmunds.com", "kbb.com", "autolist.com", "carsforsale.com", "craigslist.org",
    "facebook.com", "offerup.com", "ebay.com", "carsdirect.com", "iseecars.com",
    "autotempest.com", "copart.com", "manheim.com", "adesa.com", "iaai.com",
)
# National used-car chains (own sites, one company, many stores).
NATIONAL_RETAILER_TOKENS = (
    "carmax", "carvana", "vroom", "shift.com", "echopark", "drivetime",
    "car-mart", "carmart.com", "carlotz", "hertzcarsales", "enterprisecarsales",
    "autonationusa", "autonation usa",
)
# Brands whose name in a dealer's name or host marks a franchise store.
BRANDS = (
    "acura", "alfa romeo", "aston martin", "audi", "bentley", "bmw", "buick", "cadillac",
    "chevrolet", "chevy", "chrysler", "dodge", "ferrari", "fiat", "ford", "genesis", "gmc",
    "honda", "hyundai", "infiniti", "jaguar", "jeep", "kia", "lamborghini", "land rover",
    "lexus", "lincoln", "lotus", "maserati", "mazda", "mclaren", "mercedes", "mini",
    "mitsubishi", "nissan", "polestar", "porsche", "ram", "rolls royce", "subaru", "toyota",
    "volkswagen", "vw", "volvo",
)
# Host-only brand fragments (hosts have no spaces): dealer groups' short forms included.
_HOST_BRANDS = tuple(sorted({b.replace(" ", "") for b in BRANDS if len(b) >= 4} | {
    "bmw", "gmc", "kia", "vw", "cdjr", "cjdr", "cdj", "lambo", "mbof", "chev",
}, key=len, reverse=True))
# Hosts that contain a brand fragment only because of a place or family name.
_HOST_BRAND_FALSE_FRIENDS = (
    "oxford", "stafford", "bradford", "hartford", "medford", "crawford", "langford",
    "radford", "milford", "guilford", "bedford", "stratford", "ashford", "clifford",
    "sanford", "stanford", "rutherford", "binford", "wexford", "telford", "salford",
    "lincolnauto", "lincolncity", "lincolnpark", "lincolnwood", "genesisauto",
    "minivan", "dominion", "geminiauto", "skiauto", "kiawah",
)
# Words a used-car lot puts in its name or host.
INDEPENDENT_TOKENS = (
    "auto sales", "autosales", "autosale", "car sales", "carsales", "autos", "motorcars",
    "motor cars", "pre-owned", "preowned", "used cars", "usedcars", "used car", "usedcar",
    "wholesale", "imports", "autoboutique", "auto boutique", "autoplex", "automart",
    "auto mart", "carcenter", "car center", "autoexchange", "auto exchange", "buy here",
    "buyhere", "bhph", "cardeal", "car deal", "carsinc", "cars inc", "auto brokers",
)
MIN_MAKES_FOR_MIXED_LOT = 5
NEW_ROWS_MIN = 10
NEW_SHARE_MIN = 0.20

_WORD_BRAND_RE = re.compile(
    r"\b(?:" + "|".join(re.escape(b) for b in sorted(BRANDS, key=len, reverse=True)) + r")\b",
    re.IGNORECASE,
)
_TLD_RE = re.compile(r"-(com|net|org|us|cars|auto|biz|co)$")


def host_from_dealer_id(dealer_id: str | None) -> str:
    """``davekirk-com`` -> ``davekirk.com`` (the scanner's dealer_id is the host, dots as dashes)."""
    s = (dealer_id or "").strip().lower()
    return _TLD_RE.sub(lambda m: "." + m.group(1), s)


def _norm_host(host: str | None) -> str:
    h = (host or "").strip().lower()
    h = re.sub(r"^[a-z]+://", "", h).split("/", 1)[0]
    return h[4:] if h.startswith("www.") else h


def _host_has_brand(host: str) -> str:
    stem = host.split(".", 1)[0]
    for friend in _HOST_BRAND_FALSE_FRIENDS:
        stem = stem.replace(friend, " ")
    if stem.startswith("mb") and not stem.startswith("mbl"):
        return "mercedes"
    for frag in _HOST_BRANDS:
        if frag in stem:
            return frag
    return ""


def classify_dealer(
    *,
    dealer_id: str | None = None,
    name: str | None = None,
    host: str | None = None,
    rows_new: int | None = None,
    rows_used: int | None = None,
    makes: Iterable[str] | None = None,
) -> tuple[str, str]:
    """Return ``(kind, signal)``; ``kind`` is one of :data:`KINDS`, ``signal`` says which rule fired."""
    h = _norm_host(host) or host_from_dealer_id(dealer_id)
    nm = (name or "").strip().lower()
    blob = f"{nm} {h}"
    for mh in MARKETPLACE_HOSTS:
        if h == mh or h.endswith("." + mh):
            return "marketplace", f"host:{mh}"
    for tok in NATIONAL_RETAILER_TOKENS:
        if tok in blob:
            return "national_retailer", f"token:{tok}"
    m = _WORD_BRAND_RE.search(nm) if nm else None
    if m:
        return "franchise", f"name_brand:{m.group(0).lower()}"
    frag = _host_has_brand(h) if h else ""
    if frag:
        return "franchise", f"host_brand:{frag}"
    n_new = int(rows_new or 0)
    n_used = int(rows_used or 0)
    total = n_new + n_used
    if n_new >= NEW_ROWS_MIN and total and n_new / total >= NEW_SHARE_MIN:
        return "franchise", "sells_new"
    stem = h.split(".", 1)[0]
    for tok in INDEPENDENT_TOKENS:
        if tok in nm or tok.replace(" ", "").replace("-", "") in stem:
            return "independent", f"lot_word:{tok}"
    if n_new == 0 and n_used > 0:
        distinct = {str(x).strip().lower() for x in (makes or ()) if str(x or "").strip()}
        if makes is None or len(distinct) >= MIN_MAKES_FOR_MIXED_LOT:
            return "independent", "used_only" + (f"_{len(distinct)}_makes" if makes is not None else "")
    return "unknown", "no_signal"
