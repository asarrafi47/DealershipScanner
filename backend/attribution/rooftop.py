"""The rooftop gate: which rows of one feed payload belong to the store being scanned.

Moved verbatim from ``backend/parsers/__init__.py`` (2026-10-01, monolith audit
P1 #4 / scanner F-19). The decision logic is unchanged; ``backend.parsers``
re-exports every name here so its 149 importers keep working.

Group platforms serve EVERY rooftop in the group from the account the queried
store sits on, so the queried host is NOT evidence of who sells the car.
:func:`resolve_rooftop_attribution` reads the per-vehicle rooftop identity the
feed itself carries and refuses any row it cannot positively tie to the store
being scanned.

Callers should not use this module directly: :mod:`backend.attribution` is the
entry point (``gate_page`` for one payload, ``decide`` for the all-rows pass).
"""
from __future__ import annotations

import logging
import re
import sys
from typing import Any
from urllib.parse import urlparse

# The gate's log lines have always been emitted under ``backend.parsers`` (ops
# reports and the corpus test read them there); the move keeps the name.
_log = logging.getLogger("backend.parsers")


def roster_name_aliases(dealer_id: str) -> tuple[str, ...]:
    """Per-dealer recorded feed spellings (``backend/parsers/rooftop_aliases.py``).

    Resolved through ``backend.parsers.roster_name_aliases`` when that package is
    loaded, because the tests (and any caller that stubs the alias store) patch
    the name there; imported lazily so this module never imports ``backend.parsers``
    at load time (``backend.parsers`` imports this module).
    """
    mod = sys.modules.get("backend.parsers")
    fn = getattr(mod, "roster_name_aliases", None) if mod is not None else None
    if fn is None or fn is roster_name_aliases:
        from backend.parsers.rooftop_aliases import roster_name_aliases as fn
    return fn(dealer_id)


# ─────────────────────────── rooftop attribution ────────────────────────────
#
# Parsers do not decide attribution. A parser that can see a per-vehicle rooftop
# identifier attaches it verbatim as ``row["_rooftop"]``:
#
#     {"key": <feed's rooftop id>, "name": <store name as the feed spells it>,
#      "site": <store url>, "address": ..., "city": ..., "state": ..., "zip": ...}
#
# and this module decides. Carriers, from payloads inspected 2026-08-03:
#   carscommerce   ``dealer.location``  (store name, or an address block)
#   typesense      ``dealerName`` / ``dealer.name`` + ``dealer.address``/``city``
#   team_velocity  ``dealerName`` + ``dealerCity``/``dealerState``/``dealerZip``
#   dealer_dot_com ``accountId`` + the body's ``accounts`` map (name, url and a
#                  full postal address per rooftop)
#
# Rows from the remaining platforms carry no rooftop and pass through this gate
# untouched. Do not restate that as "no other platform names a rooftop" — that
# claim was in this comment on 2026-08-03 and was false.
#
# ``dealer.url`` (typesense) and ``dealerDomain`` (team velocity) are rewritten
# by the platform to whatever host asked, so a host comparison alone silently
# passes every sibling row — that is why site evidence is used only when the
# payload actually shows more than one distinct host. Dealer.com does NOT rewrite
# it (a body fetched from crownlexus.com on 2026-08-03 returned 12 rooftops
# carrying 12 different urls, ``www.bmwofmonrovia.net`` among them), but the gate
# does not special-case that: the ">1 distinct host" condition already holds
# wherever the field is trustworthy enough to matter.

_ROOFTOP_NAME_STOPWORDS = frozenset({"of", "the", "and", "at"})

# Tiers that identify the store only by WHERE IT IS, not by who it is. Two
# rooftops of one group can share a zip and certainly a city, so a "unique"
# winner here can be an artefact of which rows happened to land on this page.
# Siblings refused under these tiers are marked ``sibling_rooftop_weak_tier``,
# which backend/scanner/rooftop_disown.py deliberately leaves OUT of
# EVIDENCE_BACKED_REJECTS: refuse the write, never un-list the car.
_LOCALITY_ONLY_TIERS = frozenset({"zip_code", "city_state"})

# Name tiers that match on a SUBSET of the roster's words identify the store
# only loosely: "Ted Russell Ford" ⊂ both "Ted Russell Ford" and "Ted Russell
# Ford - Parkside", so a roster row named after the wrong lot picks the wrong
# rooftop (tedrussellford-net 2026-09-28: 600 real cars un-listed). Keeping on
# these tiers can only add inventory; un-listing on them needs the exact name,
# the host, a slug or a trusted street, so their siblings get the weak marker.
_WEAK_NAME_TIERS = frozenset({"name_token_subset", "name_brand_alias_subset"})

# A rooftop named "<store> Service" / "<store> Service Center" / "<store> Parts"
# is the same storefront's department label, not a sibling lot: dealer.com
# accounts file the whole lot under it (terrylabontechevy-com 2026-09-28:
# 400 cars under "Terry Labonte Chevrolet Service", 11 under "Terry Labonte
# Chevrolet"; the exact-name tier picked the 11 and un-listed 373). Such a
# rooftop is folded into the store it names before the target is picked.
_DEPARTMENT_SUFFIXES = (
    "service center", "service department", "service dept", "service",
    "parts center", "parts department", "parts dept", "parts",
    "collision center", "collision", "body shop", "quick lane",
)

# The tiers that matched on the roster's street_address. They prove IDENTITY —
# but only as far as the roster street itself can be trusted, which is what
# ``dealerships.street_address_source`` records (migrations/V003).
_STREET_TIERS = frozenset({
    "street_address", "street_address_partial",
    "street_address_suffix", "street_address_suffix_partial",
})

# street_address_source values whose street is a SCRAPE GUESS, not a structured
# claim: free-text pattern matching over rendered page text (the backfill's
# weakest tiers). A street from these sources still identifies the store well
# enough to KEEP its rows — keeping can only add inventory — but it must never
# be the sole evidence that UN-LISTS a sibling's rows, so a street-tier match
# under these sources is demoted to the same weight as the city tier (siblings
# get ``sibling_rooftop_weak_tier``: refuse the write, never un-list). NULL /
# empty source means "unknown origin" per V003 (rows that predate provenance,
# e.g. hand-entered) and keeps the historical strong-tier behaviour; the
# structured tiers (site_jsonld*, osm_website) are machine-readable claims by
# the dealer's own site and stay strong.
_WEAK_STREET_SOURCES = frozenset({"site_text", "site_text_browser"})

# ``dealer.location`` is a free-text field: some CarsCommerce accounts fill it
# with the store name, others with an HTML address block, others with inventory
# tags ("loaner", "none", "ford,pal"). Only the first two describe a storefront.
_ADDRESS_SHAPED = re.compile(
    r",\s*[A-Za-z]{2}\.?\s+\d{5}(?:-\d{4})?\b"  # "… Cerritos, CA 90703"
    r"|^\s*\d+[A-Za-z]?\s+[A-Za-z]"             # "18500 Studebaker Rd", "41B Auto Center Dr"
)


def _nrm(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def _tokens(value: Any) -> frozenset[str]:
    raw = re.split(r"[^a-z0-9]+", str(value or "").lower())
    return frozenset(t for t in raw if t and t not in _ROOFTOP_NAME_STOPWORDS)


# ── brand-abbreviation equivalence ──────────────────────────────────────────
#
# A Stellantis store is named four interchangeable ways by the same group's own
# systems. Observed 2026-08-03 in live feeds and rosters:
#   roster "Tutton CDJR of Jasper"  ↔ feed "Tutton Chrysler Jeep Dodge RAM of Jasper"
#   roster "Landers McLarty Chrysler Dodge Jeep Ram FIAT" ↔ feed "Landers McLarty DCJR"
# The initials are written in any order (CDJR / DCJR / CJDR), sometimes with the
# FIAT franchise appended as a fifth brand or a trailing "F". That is a naming
# convention, not a guess: every spelling names the same set of franchises. It is
# folded into ONE token so the name tiers compare the rest of the name normally.
#
# Deliberately narrow. Only the Stellantis initials are recognised, only when a
# token is made of nothing but them with no letter repeated, and only when the
# name really carries the cluster (three or more of Chrysler/Dodge/Jeep/Ram) —
# so "Landers McLarty Subaru" and "Encinitas Ford" are untouched, and the tier
# built on this never even runs for a dealer whose name has no cluster in it.
_STELLANTIS_INITIALS = {"c": "chrysler", "d": "dodge", "j": "jeep", "r": "ram", "f": "fiat"}
_STELLANTIS_CORE = frozenset({"chrysler", "dodge", "jeep", "ram"})
_STELLANTIS_CLUSTER_TOKEN = "@cdjr"


def _expand_brand_initials(token: str) -> frozenset[str]:
    """"cdjr" → {chrysler, dodge, jeep, ram}; anything else → {token}.

    Requires every character to be a Stellantis initial, no repeats, at least
    three of them, and "j" among them — no English word in a storefront name is
    spelled from ``cdjrf`` alone, and demanding the Jeep initial keeps the rule
    to abbreviations that are actually used ("CDJ", "CJDR", "CDJRF").
    """
    if 3 <= len(token) <= 5 and "j" in token and len(set(token)) == len(token):
        if all(ch in _STELLANTIS_INITIALS for ch in token):
            return frozenset(_STELLANTIS_INITIALS[ch] for ch in token)
    return frozenset({token})


def _brand_canonical_tokens(value: Any) -> frozenset[str]:
    """Token set with a spelled-out or abbreviated Stellantis cluster folded flat.

    Returns the plain token set unchanged when the name carries no cluster, so a
    caller can test ``!= _tokens(value)`` to see whether the rule applies at all.
    """
    expanded: set[str] = set()
    for token in _tokens(value):
        expanded |= _expand_brand_initials(token)
    if len(expanded & _STELLANTIS_CORE) < 3:
        return frozenset(expanded)
    return frozenset(
        {t for t in expanded if t not in _STELLANTIS_CORE and t != "fiat"}
        | {_STELLANTIS_CLUSTER_TOKEN}
    )


def _host(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if "//" not in raw:
        raw = "https://" + raw.lstrip("/")
    try:
        host = (urlparse(raw).netloc or "").lower()
    except ValueError:
        return ""
    return host[4:] if host.startswith("www.") else host


_HTML_TAG = re.compile(r"<(?!br)[^>]*>", re.I)
_LINE_BREAK = re.compile(r"<br\s*/?>|[\r\n]+", re.I)
_TRAILING_SPLIT = re.compile(r"\s+[-–—|]\s+")


def _label_lines(label: str) -> list[str]:
    """Split a rooftop label into its display lines.

    Accounts write these as one string with the store name first and the postal
    address (and often a phone number) after a ``<br>``:
    ``"Carriage Kia of Woodstock<br/>630 Olde Rope Mill Park Rd<br/>(770)…"``.
    """
    text = _HTML_TAG.sub(" ", str(label or ""))
    lines = [ln.strip(" \t,") for ln in _LINE_BREAK.split(text)]
    lines = [ln for ln in lines if ln]
    if lines:
        lines[0] = _TRAILING_SPLIT.split(lines[0])[0].strip()
    return [ln for ln in lines if ln]


def _looks_like_address(value: str) -> bool:
    return bool(_ADDRESS_SHAPED.search(value or ""))


def _looks_like_store_name(value: str) -> bool:
    """True for "Courtesy Chevrolet" / "Shottenkirk Honda Decatur"; false for
    "loaner", "none", "ford,pal", "28900.00" — inventory tags and prices that
    some accounts park in the same field. A storefront name has a space and at
    least two real words.

    A genuinely single-word rooftop name would be classed as a tag and its rows
    treated as unlabelled, i.e. this gate would not fire for it. No such name
    appears in any feed inspected on 2026-08-03.
    """
    text = str(value or "").strip()
    if " " not in text or _looks_like_address(text):
        return False
    words = [w for w in re.split(r"\s+", text) if w]
    return len(words) >= 2 and any(len(re.sub(r"[^a-z]", "", w.lower())) >= 3 for w in words)


# ``key`` is an IDENTIFIER slot, ``name`` a display slot. A platform that has
# only a free-text label puts the same string in both (CarsCommerce copies
# ``dealer.location`` into each), so a key that merely repeats the name carries
# no evidence the name did not already carry. That is what keeps the inventory
# tags some CarsCommerce accounts park in that field — "loaner", "none",
# "ford,pal", "28900.00" — out of the identifier path: not a list of known bad
# words, but the fact that they arrive as a label in both slots.
#
# Dealer.com fills the two slots from different fields: ``key`` is the
# per-vehicle ``accountId`` ("soniccrownlexus", "lexusofcarlsbad",
# "autonationhondacostamesa") and ``name`` is the ``accounts`` map's display
# name ("Crown Lexus"). Such a key is a rooftop slug: one lowercase token, so
# ``_looks_like_store_name`` (which needs two space-separated words) reads it as
# a tag and ignores it. The tiers below read it directly instead.
_SLUG_SHAPED = re.compile(r"^[a-z0-9][a-z0-9._-]*$")


def _rooftop_slug(rooftop: dict) -> str:
    """The feed's own identifier for this rooftop, normalised, or ``""``.

    A single lowercase token that is spelled differently from the record's
    display name. Requires six letters so a bare number, a price or a two-letter
    code cannot become a storefront identity.
    """
    key = str(rooftop.get("key") or "").strip().lower()
    if not key or not _SLUG_SHAPED.match(key):
        return ""
    if len(re.sub(r"[^a-z]", "", key)) < 6:
        return ""
    name = rooftop.get("name")
    if name and _nrm(name) == _nrm(key):
        return ""
    return _nrm(key)


def _rooftop_name(rooftop: dict) -> str:
    """The storefront NAME in this record, or "" when it names none."""
    for candidate in (rooftop.get("key"), rooftop.get("name")):
        lines = _label_lines(candidate)
        if lines and _looks_like_store_name(lines[0]):
            return lines[0]
    return ""


def _rooftop_identity(rooftop: dict) -> str:
    """Stable per-rooftop grouping key for one feed payload.

    The storefront label is the identity — the store name where the feed gives
    one, else its street line plus the city/state/zip line, which is what tells
    two rooftops of the same group apart when neither is named. The phone number
    on the third line is left out: one store can publish several.

    Never the url: group platforms rewrite it to whichever host asked, so it is
    identical on every row including the siblings'.
    """
    slug = _rooftop_slug(rooftop)
    if slug:
        # The feed's own rooftop id beats its display name: it is what the feed
        # actually keys the car on, and two rooftops of one group can share a
        # display name where they cannot share an id.
        return "s:" + slug
    for candidate in (rooftop.get("key"), rooftop.get("name")):
        lines = _label_lines(candidate)
        if not lines:
            continue
        if _looks_like_store_name(lines[0]):
            return "l:" + _nrm(lines[0])
        if _looks_like_address(lines[0]):
            return "l:" + _nrm(" ".join(lines[:2]))
    return ""


def _rooftop_of(row: dict) -> dict | None:
    rt = row.get("_rooftop")
    return rt if isinstance(rt, dict) else None


# USPS suffix and directional abbreviations, expanded to one spelling so that a
# roster address and a feed's address block can be compared as places rather
# than as strings. BMW of Murrieta publishes "41430 Auto Mall Parkway" on its own
# website while its feed's rooftop block reads "41430 Auto MALL PKWY": the same
# storefront, and under plain normalisation neither string contains the other.
# Expanding never merges two different streets — "pkwy"/"parkway" are the same
# word — so the unique-winner requirement below is unaffected.
_STREET_WORDS = {
    "st": "street", "str": "street", "ave": "avenue", "av": "avenue",
    "blvd": "boulevard", "blv": "boulevard", "rd": "road", "dr": "drive",
    "drv": "drive", "pkwy": "parkway", "pky": "parkway", "pkway": "parkway",
    "hwy": "highway", "hway": "highway", "ln": "lane", "ct": "court",
    "cir": "circle", "pl": "place", "sq": "square", "ter": "terrace",
    "trl": "trail", "tpke": "turnpike", "tpk": "turnpike", "expy": "expressway",
    "expwy": "expressway", "fwy": "freeway", "byp": "bypass", "xing": "crossing",
    "rte": "route", "n": "north", "s": "south", "e": "east", "w": "west",
    "ne": "northeast", "nw": "northwest", "se": "southeast", "sw": "southwest",
    "ste": "suite", "no": "north",
}


def _street_key(value: Any) -> str:
    """``_nrm`` with USPS abbreviations spelled out, for street comparison."""
    words = [w for w in re.split(r"[^a-z0-9]+", str(value or "").lower()) if w]
    return "".join(_STREET_WORDS.get(w, w) for w in words)


def _rooftop_place(rooftop: dict) -> tuple[str, str, str, str]:
    """``(street, street_key, city_state, zip)`` for this record, or four ``""``.

    Two sources, because the platforms split differently. Typesense and Team
    Velocity fill discrete ``address``/``city``/``state``/``zip`` keys. A
    CarsCommerce account that writes an address block instead of a store name
    puts the whole thing in the label: ``"41430 Auto Mall Pkwy<br/>Murrieta, CA
    92562<br/>(951)…"`` — street on the first line, city/state/zip on the
    second, phone on the third.

    ``street`` is plainly normalised and ``street_key`` additionally expands
    USPS abbreviations; the caller keeps both so the strict comparison can be
    tried before the forgiving one.
    """
    raw_street = str(rooftop.get("address") or "").strip()
    city = _nrm(rooftop.get("city"))
    state = _nrm(rooftop.get("state"))
    postal = re.sub(r"\D", "", str(rooftop.get("zip") or ""))[:5]

    if not raw_street or not city:
        for candidate in (rooftop.get("key"), rooftop.get("name")):
            lines = _label_lines(candidate)
            if not lines or not _looks_like_address(lines[0]):
                continue
            if not raw_street:
                raw_street = lines[0]
            locale = next((ln for ln in lines[1:] if _looks_like_address(ln)), "")
            hit = re.search(r"(.+?),\s*([A-Za-z]{2})\.?\s+(\d{5})", locale or lines[0])
            if hit:
                city = city or _nrm(hit.group(1))
                state = state or _nrm(hit.group(2))
                postal = postal or hit.group(3)
            break

    return (
        _nrm(raw_street),
        _street_key(raw_street),
        (city + state if city and state else ""),
        postal,
    )


class _Rooftop:
    """One storefront as a single feed payload describes it."""

    def __init__(self, identity: str):
        self.identity = identity
        self.names: set[str] = set()
        self.alt_names: set[str] = set()  # feed free-text store labels: match-only, never refuse on them
        self.slugs: set[str] = set()
        self.hosts: set[str] = set()
        self.streets: set[str] = set()
        self.street_keys: set[str] = set()
        self.locales: set[str] = set()
        self.zips: set[str] = set()
        self.sources: set[str] = set()  # feed accounts (carscommerce source_id / api_id) behind this stamp
        self.rows: list[dict] = []

    def add(self, rooftop: dict, row: dict) -> None:
        src = str(rooftop.get("source") or row.get("_feed_source") or "").strip()
        if src:
            self.sources.add(src)
        name = _rooftop_name(rooftop)
        if name:
            self.names.add(name)
        alt = str(rooftop.get("alt_name") or "").strip()
        if alt and _looks_like_store_name(alt):
            self.alt_names.add(alt)
        slug = _rooftop_slug(rooftop)
        if slug:
            self.slugs.add(slug)
        host = _host(rooftop.get("site"))
        if host:
            self.hosts.add(host)
        street, street_key, locale, postal = _rooftop_place(rooftop)
        if street:
            self.streets.add(street)
        if street_key:
            self.street_keys.add(street_key)
        if locale:
            self.locales.add(locale)
        if postal:
            self.zips.add(postal)
        self.rows.append(row)


def _unique(matches: list[_Rooftop]) -> _Rooftop | None:
    return matches[0] if len(matches) == 1 else None


def _pick_target(
    rooftops: list[_Rooftop], dealer_name: str, dealer_id: str, dealer_url: str,
    place: tuple[str, str, str, str] = ("", "", "", ""),
) -> tuple[_Rooftop | None, str]:
    """The one rooftop in this group payload that IS the store being scanned.

    Tiers run strongest-first and every tier demands a UNIQUE winner: two
    plausible rooftops mean we cannot tell them apart, which is a refusal, not a
    coin flip. Returns ``(rooftop, tier_name)``; ``(None, reason)`` when the
    store cannot be identified.
    """
    target_host = _host(dealer_url)
    host_evidence = {h for rt in rooftops for h in rt.hosts}
    if target_host and len(host_evidence) > 1:
        hit = _unique([rt for rt in rooftops if target_host in rt.hosts])
        if hit is not None:
            return hit, "site_host"

    target_nrm = _nrm(dealer_name)
    target_tokens = _tokens(dealer_name)
    if target_nrm:
        hit = _unique([rt for rt in rooftops if any(_nrm(n) == target_nrm for n in rt.names)])
        if hit is not None:
            return hit, "name_exact"

        if target_tokens:
            hit = _unique([rt for rt in rooftops if any(_tokens(n) == target_tokens for n in rt.names)])
            if hit is not None:
                return hit, "name_tokens"

            # Feeds spell a store out more fully than the roster does
            # ("Welborn Chevrolet of Rome" vs "Welborn Chevrolet GMC of Rome").
            # Every roster word must appear, and only one rooftop may qualify.
            hit = _unique([rt for rt in rooftops if any(target_tokens < _tokens(n) for n in rt.names)])
            if hit is not None:
                return hit, "name_token_subset"

            # Same store, same words, different way of writing the franchises:
            # "Tutton CDJR of Jasper" vs "Tutton Chrysler Jeep Dodge RAM of
            # Jasper". Only runs when the ROSTER name actually carries the
            # Stellantis cluster, and still demands a unique winner — "Voyles
            # CDJR of Birmingham" folds to the same cluster token and is
            # correctly told apart by the rest of the name.
            target_brand = _brand_canonical_tokens(dealer_name)
            if target_brand != target_tokens:
                hit = _unique([
                    rt for rt in rooftops
                    if any(_brand_canonical_tokens(n) == target_brand for n in rt.names)
                ])
                if hit is not None:
                    return hit, "name_brand_alias"
                # Feed spells the franchise cluster short AND adds the town:
                # "Shottenkirk CDJR Canton" for the roster's "Shottenkirk
                # Chrysler Dodge Jeep Ram" (2026-09-26, refused 297 rows as
                # "payload holds only … not this store"). Every canonical roster
                # token must appear, unique winner as always.
                hit = _unique([
                    rt for rt in rooftops
                    if any(target_brand < _brand_canonical_tokens(n) for n in rt.names)
                ])
                if hit is not None:
                    return hit, "name_brand_alias_subset"

        hit = _unique([
            rt for rt in rooftops
            if any(_nrm(n) and (_nrm(n) in target_nrm or target_nrm in _nrm(n)) for n in rt.names)
        ])
        if hit is not None:
            return hit, "name_contains"

    # A name this store's own feed publishes for itself that the roster does not
    # carry — a misspelling in one account, recorded per dealer as DATA (see
    # backend/parsers/rooftop_aliases.py) rather than absorbed by teaching this
    # gate to match approximately, which would make it guess. Exact match only,
    # because the alias IS the feed's exact spelling, and still unique-or-refuse.
    for alias in roster_name_aliases(dealer_id):
        alias_nrm = _nrm(alias)
        if not alias_nrm:
            continue
        hit = _unique([rt for rt in rooftops if any(_nrm(n) == alias_nrm for n in rt.names)])
        if hit is not None:
            return hit, "roster_name_alias"

    # A free-text store label the feed attaches per row (CarsCommerce
    # ``extra_fields.custom_location``): "Hendrick Buick GMC Cadillac Cary" for
    # the roster's "Hendrick Buick GMC Cary". Matched with the same name rules
    # and the same unique-winner demand, but ONLY ever used to find this store —
    # a junk or sibling label ("WRECKED CAR", "Voyles CDJR of Birmingham")
    # simply matches nothing and the address tiers below decide.
    if target_nrm and any(rt.alt_names for rt in rooftops):
        for probe_fn, tier_name in (
            (lambda n: _nrm(n) == target_nrm, "store_label_exact"),
            (lambda n: bool(target_tokens) and _tokens(n) == target_tokens, "store_label_tokens"),
            (lambda n: bool(target_tokens) and target_tokens < _tokens(n), "store_label_token_subset"),
        ):
            hit = _unique([rt for rt in rooftops if any(probe_fn(n) for n in rt.alt_names)])
            if hit is not None:
                return hit, tier_name
        for alias in roster_name_aliases(dealer_id):
            alias_nrm = _nrm(alias)
            if alias_nrm:
                hit = _unique([rt for rt in rooftops if any(_nrm(n) == alias_nrm for n in rt.alt_names)])
                if hit is not None:
                    return hit, "store_label_alias"

    # dealer_id is the storefront host with dots swapped for dashes
    # ("davekirk-com"); some rosters carry a longer trading name in dealer_name
    # while the feed uses the short one the host is built from.
    id_nrm = _nrm(re.sub(r"-(com|net|org)$", "", str(dealer_id or "")))
    if id_nrm:
        hit = _unique([
            rt for rt in rooftops
            if any(_nrm(n) and (id_nrm == _nrm(n) or id_nrm in _nrm(n)) for n in rt.names)
        ])
        if hit is not None:
            return hit, "dealer_id_host"

    # Slug tiers. Dealer.com identifies each rooftop by an ``accountId`` slug
    # that concatenates the group, the franchise and the town —
    # "soniccrownlexus", "lexusofcarlsbad", "autonationhondacostamesa". It is one
    # token, so the name tiers above (which compare whole words) never see it,
    # yet it is the only rooftop evidence a page carries when that page's
    # ``accounts`` map holds no display name.
    #
    # Containment, not equality: the slug carries a group prefix the roster does
    # not ("sonic" + "crownlexus"). Both probes still demand a UNIQUE winner, and
    # the host stem must be long enough that a franchise word alone ("bmw",
    # "kia") cannot match every sibling in the group.
    slugs_present = any(rt.slugs for rt in rooftops)
    if slugs_present:
        host_stem = _nrm(target_host.split(".")[0]) if target_host else ""
        for probe in (host_stem, id_nrm):
            if len(probe) < 8:
                continue
            hit = _unique([rt for rt in rooftops if any(probe in s for s in rt.slugs)])
            if hit is not None:
                return hit, "rooftop_slug_host"

        # Every word of the roster name must appear in the slug. Words shorter
        # than four letters are dropped: they are the franchise abbreviations
        # ("bmw", "vw", "gmc", "kia"), which appear in several of a group's
        # slugs at once and so identify a group, not a storefront.
        name_parts = [t for t in target_tokens if len(t) >= 4]
        if name_parts:
            hit = _unique([
                rt for rt in rooftops
                if any(all(part in s for part in name_parts) for s in rt.slugs)
            ])
            if hit is not None:
                return hit, "rooftop_slug_name_tokens"

    # Address tiers. A group feed whose rooftops are address blocks with no
    # store name (CarsCommerce account 5379783 / BMW of Murrieta: 84 rooftops,
    # every one an address) names its stores as precisely as any other feed —
    # just in a field the name tiers cannot read. Without these the whole
    # payload is "unidentified" and every row is refused, which retires a live
    # dealer's entire inventory on evidence that actually identifies it.
    #
    # Street is the strong tier: one storefront per street address. City/state
    # is deliberately last and still demands a unique winner, because a group
    # can run two rooftops in one city — that is a refusal, not a coin flip.
    # See _LOCALITY_ONLY_TIERS: the last two prove locality, not identity, so
    # what they refuse is never strong enough to un-list a car.
    street, street_key, locale, postal = place
    if street:
        hit = _unique([rt for rt in rooftops if street in rt.streets])
        if hit is not None:
            return hit, "street_address"
        hit = _unique([
            rt for rt in rooftops
            if any(s and (s in street or street in s) for s in rt.streets)
        ])
        if hit is not None:
            return hit, "street_address_partial"

    # Same address, different spelling of the suffix. The roster is filled from
    # the dealer's own website, which writes "41430 Auto Mall Parkway"; the feed
    # writes "41430 Auto MALL PKWY". Neither string contains the other, so the
    # two tiers above miss a storefront both sides name identically. Expanding
    # the abbreviations cannot merge two different streets, so this stays a
    # unique-winner test like every tier above it.
    if street_key:
        hit = _unique([rt for rt in rooftops if street_key in rt.street_keys])
        if hit is not None:
            return hit, "street_address_suffix"
        hit = _unique([
            rt for rt in rooftops
            if any(s and (s in street_key or street_key in s) for s in rt.street_keys)
        ])
        if hit is not None:
            return hit, "street_address_suffix_partial"

    if postal:
        hit = _unique([rt for rt in rooftops if postal in rt.zips])
        if hit is not None:
            return hit, "zip_code"

    if locale:
        hit = _unique([rt for rt in rooftops if locale in rt.locales])
        if hit is not None:
            return hit, "city_state"

    return None, "target_rooftop_unidentified"


def resolve_rooftop_attribution(
    rows: list[dict], *, dealer_id: str = "", dealer_name: str = "", dealer_url: str = "",
    dealer_address: str = "", dealer_city: str = "", dealer_state: str = "", dealer_zip: str = "",
    dealer_address_source: str = "",
) -> tuple[list[dict], list[dict]]:
    """
    Split rows by feed-declared rooftop, and record what was refused.

    Thin wrapper over :func:`_resolve_rooftop_attribution_inner`. The decision logic is
    unchanged; this only makes the outcome durable.

    The gate refuses roughly 1,106 payloads per sweep and, before this, said so only in a
    WARNING -- so the evidence of WHICH dealers are served another group's inventory was
    computed over a thousand times a scan and discarded every time (while drowning every
    other warning in the log). The refusals are the group-feed census; see
    migrations/V009__rooftop_refusals.sql.

    Recording is best-effort and buffered: a ledger must never cost a scan.
    """
    kept, rejected = _resolve_rooftop_attribution_inner(
        rows, dealer_id=dealer_id, dealer_name=dealer_name, dealer_url=dealer_url,
        dealer_address=dealer_address, dealer_city=dealer_city,
        dealer_state=dealer_state, dealer_zip=dealer_zip,
        dealer_address_source=dealer_address_source,
    )
    if rejected and dealer_id:
        try:
            from backend.scanner.rooftop_ledger import note_refusal

            by_reason: dict[str, int] = {}
            for row in rejected:
                reason = str(row.get("_rooftop_reject") or "unknown")
                by_reason[reason] = by_reason.get(reason, 0) + 1
            # The rooftop the FEED declared, which lives in the parsed `_rooftop` dict --
            # not `dealer_name`, which by this point is just the store we were scanning.
            # Getting this wrong empties the one column that says whose inventory arrived.
            seen: set[str] = set()
            for r in rejected:
                rt = r.get("_rooftop")
                if isinstance(rt, dict):
                    for key in ("name", "key"):
                        val = str(rt.get(key) or "").strip()
                        if val:
                            seen.add(val)
                            break
            for reason, count in by_reason.items():
                note_refusal(dealer_id, reason, count, seen)
            # Persist here rather than relying on a caller to flush at some later
            # boundary: an unflushed buffer is a silent no-op, which is the exact class
            # of bug this ledger exists to stop repeating.
            from backend.scanner.rooftop_ledger import flush as _flush_ledger

            _flush_ledger(dealer_id)
        except Exception:  # noqa: BLE001 - never break a scan for bookkeeping
            pass
    return kept, rejected


def _department_base(name: str) -> str:
    """``"terry labonte chevrolet"`` for ``"Terry Labonte Chevrolet Service"``; ``""`` otherwise."""
    words = [w for w in re.split(r"[^a-z0-9]+", str(name or "").lower()) if w]
    for suffix in _DEPARTMENT_SUFFIXES:
        sw = suffix.split()
        if len(words) > len(sw) and words[-len(sw):] == sw:
            return "".join(words[: -len(sw)])
    return ""


def _merge_department_rooftops(rooftops: list) -> list:
    """Fold "<store> Service"-style rooftops into the rooftop named ``<store>``.

    Only when the base name is present as another rooftop's own name; a lone
    "X Service" rooftop with no "X" beside it is left alone (it may be the
    only name the feed gives the store, and the name tiers handle that).
    """
    by_name: dict[str, Any] = {}
    for rt in rooftops:
        for nm in rt.names:
            by_name.setdefault(_nrm(nm), rt)
    merged: list = []
    for rt in rooftops:
        bases = {_department_base(nm) for nm in rt.names} - {""}
        host = None
        for b in bases:
            cand = by_name.get(b)
            if cand is not None and cand is not rt:
                host = cand
                break
        if host is None:
            merged.append(rt)
            continue
        host.rows.extend(rt.rows)
        host.sources |= rt.sources
        host.alt_names |= rt.names | rt.alt_names
        host.slugs |= rt.slugs
        host.hosts |= rt.hosts
        host.streets |= rt.streets
        host.street_keys |= rt.street_keys
        host.locales |= rt.locales
        host.zips |= rt.zips
        _log.info(
            "rooftop attribution: folded department rooftop %s (%d row(s)) into %s",
            sorted(rt.names)[0], len(rt.rows), sorted(host.names)[0],
        )
    return merged


def _resolve_rooftop_attribution_inner(
    rows: list[dict], *, dealer_id: str = "", dealer_name: str = "", dealer_url: str = "",
    dealer_address: str = "", dealer_city: str = "", dealer_state: str = "", dealer_zip: str = "",
    dealer_address_source: str = "",
) -> tuple[list[dict], list[dict]]:
    """Split parsed rows into ``(kept, rejected)`` by feed-declared rooftop.

    * Payload names no rooftop at all — no evidence, no decision: keep it whole.
      Most platforms and most CarsCommerce accounts land here, so ordinary
      single-store dealers are untouched by this gate.
    * Payload names one rooftop — kept, unless that rooftop names a store and
      the name is not ours (one page of a paginated group feed can hold nothing
      but one sibling's cars).
    * Payload names several rooftops and one of them is demonstrably the store
      being scanned — that rooftop's rows are kept, the siblings' are rejected.
    * Payload names several rooftops and none is demonstrably the store — every
      row is rejected. A car parked at the wrong storefront is worse than a car
      missing from the site, and there is no evidence here to attribute by.

    Rejected rows carry ``_rooftop_reject`` (a reason string); they are dropped
    again at the write boundary in ``backend.scanner.database.upsert_vehicles``.
    """
    if not rows:
        return [], []

    # 2026-09-28: the same decision procedure as a scored matcher
    # (backend/attribution/match.py), behind SCANNER_ROOFTOP_SCORER (default
    # on). The legacy ladder below stays for one release; SCANNER_ROOFTOP_SCORER=0
    # selects it. The regression corpus (backend/tests/test_rooftop_corpus.py)
    # holds the proof that both paths decide every known case the same way.
    if _scorer_enabled():
        return _resolve_via_scorer(
            rows, dealer_id=dealer_id, dealer_name=dealer_name, dealer_url=dealer_url,
            dealer_address=dealer_address, dealer_city=dealer_city, dealer_state=dealer_state,
            dealer_zip=dealer_zip, dealer_address_source=dealer_address_source,
        )

    order: list[str] = []
    groups: dict[str, _Rooftop] = {}
    unmarked: list[dict] = []
    for row in rows:
        rooftop = _rooftop_of(row)
        identity = _rooftop_identity(rooftop) if rooftop else ""
        if not identity:
            unmarked.append(row)
            continue
        if identity not in groups:
            groups[identity] = _Rooftop(identity)
            order.append(identity)
        groups[identity].add(rooftop, row)

    if not groups:
        return rows, []

    rooftops = _merge_department_rooftops([groups[i] for i in order])
    place = (
        _nrm(dealer_address),
        _street_key(dealer_address),
        (_nrm(dealer_city) + _nrm(dealer_state)) if dealer_city and dealer_state else "",
        re.sub(r"\D", "", str(dealer_zip or ""))[:5],
    )
    target, tier = _pick_target(rooftops, dealer_name, dealer_id, dealer_url, place)
    label = dealer_id or dealer_name or "?"

    # after the department fold: "Acme Chevrolet" + "Acme Chevrolet Service" is one rooftop
    if len(rooftops) == 1:
        # One rooftop in this payload. A paginated group feed hands out pages
        # that hold nothing but one sibling's cars, so "only one store here" is
        # not the same as "this store". Keep the rows unless the single rooftop
        # NAMES a store and that name is not ours — an unnamed one (address only)
        # is no evidence either way and must not cost a dealer its inventory.
        if target is not None or not rooftops[0].names:
            return rows, []
        _log.warning(
            "rooftop attribution [%s]: payload holds only %s, which is not this store — "
            "refusing all %d row(s)",
            label, sorted(rooftops[0].names)[0], len(rows),
        )
        for row in rows:
            row["_rooftop_reject"] = "single_rooftop_is_not_this_store"
        return [], list(rows)

    if target is None:
        _log.warning(
            "rooftop attribution [%s]: group feed names %d rooftops (%s) and none is this "
            "store — refusing all %d row(s) rather than mis-attributing them",
            label, len(rooftops),
            ", ".join(sorted({n for rt in rooftops for n in rt.names})[:6]) or "unnamed",
            len(rows),
        )
        for row in rows:
            row["_rooftop_reject"] = tier
        return [], list(rows)

    kept: list[dict] = list(target.rows)
    rejected: list[dict] = []
    # A sibling refusal normally un-lists the car (it is positive evidence the
    # feed sells it elsewhere). That is only sound when the tier that picked the
    # target proves IDENTITY. ``zip_code`` and ``city_state`` prove only
    # LOCALITY, and this gate also runs per PAGE, where a page carrying just one
    # of two same-city rooftops hands the weak tier a spurious "unique" winner —
    # and the next page hands it the other one. Measured on bmwofmurrieta-com
    # (two Murrieta rooftops, no roster street_address): pages disagreed on the
    # winner and 1,697 of a 1,941-car dealer were queued to be un-listed. So a
    # weak-tier match refuses to WRITE the siblings but never un-lists them; the
    # union pass, which sees every rooftop at once, is what settles the dealer.
    # A street-tier win is only as strong as the roster street it compared
    # against. When that street was backfilled by the free-text scrape tiers
    # (V003 provenance: site_text / site_text_browser), the match is demoted to
    # locality weight: this store's rows are still KEPT (keeping can only add
    # inventory, never remove it), but the siblings are refused with the weak
    # marker so weak-provenance street evidence is never on its own enough to
    # un-list a car. Empty/NULL source is "unknown origin" per V003, not "weak",
    # and keeps the historical behaviour.
    weak_street_provenance = (
        tier in _STREET_TIERS
        and str(dealer_address_source or "").strip().lower() in _WEAK_STREET_SOURCES
    )
    sibling_reject = (
        "sibling_rooftop"
        if tier not in _LOCALITY_ONLY_TIERS and tier not in _WEAK_NAME_TIERS and not weak_street_provenance
        else "sibling_rooftop_weak_tier"
    )
    # A stamp that names no store (a street block, or nothing) filed under the
    # matched store's own feed account is the same store, written differently:
    # Honda of Huntersville's feed names the store on some pages, prints
    # "12815 Statesville Rd<br/>Huntersville, NC 28078<br/>(704) 875-3232" on
    # others and leaves its 200 used cars unstamped, all under source MP23253;
    # the name tier kept 141 of 389 (2026-09-26). Siblings in a real group feed
    # file under their own ids (Hendrick), so a foreign id still refuses.
    same_source = 0
    # Only a feed account that no OTHER stamped rooftop uses can vouch for a
    # row: Hendrick's group-wide used feed "RHendrickUsed" sits behind nine
    # rooftops, and a city_state (locality) match must never pull unstamped
    # group rows in (hendrickhonda-com kept 73 foreign rows, 2026-09-26).
    own_street_key = place[1] if place else ""
    shared_sources = {
        src for rt in rooftops
        if rt is not target and (rt.names or (rt.street_keys and not (own_street_key and rt.street_keys <= {own_street_key})))
        for src in rt.sources
    }
    own_sources = (target.sources - shared_sources) if tier not in _LOCALITY_ONLY_TIERS else set()
    for rt in rooftops:
        if rt is target:
            continue
        if not rt.names and rt.sources and own_sources and rt.sources <= own_sources:
            kept.extend(rt.rows)
            same_source += len(rt.rows)
            continue
        if not rt.names and own_street_key and rt.street_keys and rt.street_keys <= {own_street_key}:
            # A street block at the store's OWN street (registry or the page's
            # JSON-LD / title, 12815 Statesville Rd for Honda of Huntersville's
            # 105 new cars under feed 209014, 2026-09-26) is the store; keeping can
            # only add inventory, never un-list.
            kept.extend(rt.rows)
            same_source += len(rt.rows)
            continue
        for row in rt.rows:
            row["_rooftop_reject"] = sibling_reject
            rejected.append(row)
    # Rows the parser could not stamp at all are unattributable inside a group
    # payload: nothing says they belong to this store — unless the feed account
    # they are filed under is the matched store's own.
    for row in unmarked:
        src = str(row.get("_feed_source") or "").strip()
        if src and src in own_sources:
            kept.append(row)
            same_source += 1
            continue
        row["_rooftop_reject"] = "unstamped_row_in_group_feed"
        rejected.append(row)
    if same_source:
        _log.info(
            "rooftop attribution [%s]: kept %d unnamed/unstamped row(s) filed under this store's feed source(s) %s or at its street %r",
            label, same_source, sorted(own_sources)[:3], own_street_key,
        )

    if rejected:
        _log.info(
            "rooftop attribution [%s]: group feed with %d rooftops; matched this store by %s, "
            "kept %d row(s), refused %d sibling row(s)",
            label, len(rooftops), tier, len(kept), len(rejected),
        )
    return kept, rejected


def _scorer_enabled() -> bool:
    try:
        from backend.attribution.match import scorer_enabled

        return scorer_enabled()
    except Exception:  # noqa: BLE001 - never let the flag reader cost a scan
        return False


def _resolve_via_scorer(
    rows: list[dict], *, dealer_id: str = "", dealer_name: str = "", dealer_url: str = "",
    dealer_address: str = "", dealer_city: str = "", dealer_state: str = "", dealer_zip: str = "",
    dealer_address_source: str = "",
) -> tuple[list[dict], list[dict]]:
    """The gate's decision through ``rooftop_match.score_rows`` (same verdicts, same log
    lines, plus ``_rooftop_tier`` / ``_rooftop_score`` on every row for triage)."""
    from backend.attribution.match import score_rows

    roster = {
        "dealer_id": dealer_id, "name": dealer_name, "url": dealer_url, "street": dealer_address,
        "city": dealer_city, "state": dealer_state, "zip": dealer_zip, "street_source": dealer_address_source,
    }
    results = score_rows(rows, roster)
    kept: list[dict] = []
    rejected: list[dict] = []
    tiers: dict[str, int] = {}
    corroborated = 0
    for row, res in zip(rows, results):
        row["_rooftop_tier"] = res["tier"]
        row["_rooftop_score"] = res["score"]
        tiers[res["tier"]] = tiers.get(res["tier"], 0) + 1
        if res["decision"] == "keep":
            row.pop("_rooftop_reject", None)
            kept.append(row)
            if res["tier"] in ("same_source_feed", "own_street_block", "unstamped_under_own_source"):
                corroborated += 1
        else:
            row["_rooftop_reject"] = res["tier"]
            rejected.append(row)

    label = dealer_id or dealer_name or "?"
    if not rejected:
        if corroborated:
            _log.info("rooftop attribution [%s]: kept %d unnamed/unstamped row(s) filed under this store's feed source(s) or at its street",
                      label, corroborated)
        return kept, rejected
    reasons = {t for t in tiers if t in {"target_rooftop_unidentified", "single_rooftop_is_not_this_store"}}
    if not kept and "single_rooftop_is_not_this_store" in reasons:
        names = sorted({n for r in rows for n in [(_rooftop_of(r) or {}).get("name") or ""] if n})
        _log.warning("rooftop attribution [%s]: payload holds only %s, which is not this store — refusing all %d row(s)",
                     label, names[0] if names else "an unnamed rooftop", len(rows))
    elif not kept:
        stamps = sorted({_rooftop_name(_rooftop_of(r) or {}) for r in rows if _rooftop_of(r)} - {""})
        _log.warning("rooftop attribution [%s]: group feed names rooftops (%s) and none is this store — refusing all %d row(s) rather than mis-attributing them",
                     label, ", ".join(stamps[:6]) or "unnamed", len(rows))
    else:
        winning = next((t for t in tiers if t not in {"same_source_feed", "own_street_block", "unstamped_under_own_source",
                                                       "sibling_rooftop", "sibling_rooftop_weak_tier", "unstamped_row_in_group_feed"}), "?")
        if corroborated:
            _log.info("rooftop attribution [%s]: kept %d unnamed/unstamped row(s) filed under this store's feed source(s) or at its street",
                      label, corroborated)
        _log.info("rooftop attribution [%s]: group feed; matched this store by %s, kept %d row(s), refused %d sibling row(s)",
                  label, winning, len(kept), len(rejected))
    return kept, rejected

