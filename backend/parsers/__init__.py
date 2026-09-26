"""Provider parser registry + the shared ROOFTOP ATTRIBUTION gate.

Every scan path (delta replay, browser scan, recipe validation, recipe heal)
reaches a parser through :func:`parse`, which is therefore the one place a
vehicle can be tied to a storefront. Dealer-group platforms serve EVERY rooftop
in the group from the account the queried store sits on, so the queried host is
NOT evidence of who sells the car. :func:`resolve_rooftop_attribution` reads the
per-vehicle rooftop identity the feed itself carries and refuses any row it
cannot positively tie to the store being scanned.
"""
import logging
import re
from functools import lru_cache
from typing import Any
from urllib.parse import urlparse

from backend.parsers.carscommerce import detect as detect_carscommerce
from backend.parsers.carscommerce import parse as parse_carscommerce
from backend.parsers.chapman import detect as detect_chapman
from backend.parsers.chapman import parse as parse_chapman
from backend.parsers.dealer_dot_com import parse as parse_dealer_dot_com
from backend.parsers.dealer_eprocess import parse as parse_dealer_eprocess
from backend.parsers.dealermasters import parse as parse_dealermasters
from backend.parsers.oneaudi import parse as parse_oneaudi
from backend.parsers.wp_vehicles_index import parse as parse_wp_vehicles_index
from backend.parsers.dealer_on import parse as parse_dealer_on
from backend.parsers.jazel import parse as parse_jazel
from backend.parsers.motive_ridemotive import parse as parse_motive_ridemotive
from backend.parsers.overfuel import parse as parse_overfuel
from backend.parsers.rooftop_aliases import roster_name_aliases
from backend.parsers.sister_tv import parse as parse_sister_tv
from backend.parsers.team_velocity import detect as detect_team_velocity
from backend.parsers.team_velocity import parse as parse_team_velocity
from backend.parsers.typesense import parse as parse_typesense


def _parse_autowall(raw_data, *, base_url="", dealer_id="", dealer_name="", dealer_url=""):
    """autoWALL vehicles come either pre-parsed (browser path: ``{"inventory": [...]}``)
    or as one raw SRP page of HTML (HTTP recipe replay of ``/gs-vehicle/list?page=N``,
    Long of Athens 2026-09-24: 309 cars, 25 per page, ``data-vin`` on every card)."""
    if isinstance(raw_data, str):
        from backend.scanner.scrapers.autowall import parse_autowall_inventory_html

        return parse_autowall_inventory_html(raw_data, base_url=base_url or dealer_url, dealer_id=dealer_id,
                                             dealer_name=dealer_name or dealer_id, dealer_url=dealer_url or base_url)
    if isinstance(raw_data, dict) and isinstance(raw_data.get("inventory"), list):
        return [v for v in raw_data["inventory"] if isinstance(v, dict) and v.get("vin")]
    return []


def _parse_cosmos_cards(raw_data, *, base_url="", dealer_id="", dealer_name="", dealer_url=""):
    """DealerOn cosmos SRP bodies ({"DisplayCards": [{"VehicleCard": ...}]}).

    Dealers on this platform are often provider-hinted "dealer_dot_com", so this
    shape must be reachable through auto-detect, not just the declared parser.
    """
    if not (isinstance(raw_data, dict) and isinstance(raw_data.get("DisplayCards"), list)):
        return []
    from backend.scanner.scrapers.dealer_on import _extract_vehicles_from_srp_body

    return _extract_vehicles_from_srp_body(
        raw_data, base_url, dealer_id, dealer_name or dealer_id, dealer_url or base_url
    )


PARSERS = {
    "carscommerce": parse_carscommerce,
    "dealer_dot_com": parse_dealer_dot_com,
    "dealer_on": parse_dealer_on,
    "dealer_on_cosmos": _parse_cosmos_cards,
    "typesense": parse_typesense,
    "sister_tv": parse_sister_tv,
    "team_velocity": parse_team_velocity,
    "dealer_eprocess": parse_dealer_eprocess,
    "autowall": _parse_autowall,
    "motive_ridemotive": parse_motive_ridemotive,
    "overfuel": parse_overfuel,
    "chapman": parse_chapman,
    "jazel": parse_jazel,
    "dealermasters": parse_dealermasters,
    "wp_vehicles_index": parse_wp_vehicles_index,
    "oneaudi": parse_oneaudi,
}

_log = logging.getLogger(__name__)
_warned_unknown_providers: set[str] = set()


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
        self.rows: list[dict] = []

    def add(self, rooftop: dict, row: dict) -> None:
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

    rooftops = [groups[i] for i in order]
    place = (
        _nrm(dealer_address),
        _street_key(dealer_address),
        (_nrm(dealer_city) + _nrm(dealer_state)) if dealer_city and dealer_state else "",
        re.sub(r"\D", "", str(dealer_zip or ""))[:5],
    )
    target, tier = _pick_target(rooftops, dealer_name, dealer_id, dealer_url, place)
    label = dealer_id or dealer_name or "?"

    if len(groups) == 1:
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
        if tier not in _LOCALITY_ONLY_TIERS and not weak_street_provenance
        else "sibling_rooftop_weak_tier"
    )
    for rt in rooftops:
        if rt is target:
            continue
        for row in rt.rows:
            row["_rooftop_reject"] = sibling_reject
            rejected.append(row)
    # Rows the parser could not stamp at all are unattributable inside a group
    # payload: nothing says they belong to this store.
    for row in unmarked:
        row["_rooftop_reject"] = "unstamped_row_in_group_feed"
        rejected.append(row)

    if rejected:
        _log.info(
            "rooftop attribution [%s]: group feed with %d rooftops; matched this store by %s, "
            "kept %d row(s), refused %d sibling row(s)",
            label, len(rooftops), tier, len(kept), len(rejected),
        )
    return kept, rejected


def _parse_rows(provider: str, raw_data, _kwargs: dict, dealer_id: str) -> list[dict]:
    # CarsCommerce search payloads are provider-hinted "dealer_dot_com" /
    # "dealer_inspire", but the generic parser drops colors, carfax, features,
    # packages and warranty. Route by shape BEFORE the declared provider so the
    # rich fields the feed returns are actually captured.
    if provider != "carscommerce" and detect_carscommerce(raw_data):
        # Once the CarsCommerce shape is detected, its parser OWNS the payload —
        # even an empty result must not fall through to generic parsers, or the
        # rooftop-attribution gate would be silently bypassed (the generic parser
        # does not read ``source_id``, so its rows carry no rooftop evidence).
        return list(parse_carscommerce(raw_data, **_kwargs))

    # Team Velocity feeds are provider-hinted "dealer_dot_com", but the generic
    # parser drops the feed's distinct field names (sellingPrice, driveTrain,
    # engineCylinders, city/highwayMpg) and never seeds the image-completion
    # placeholder. Route by shape BEFORE the declared provider so the dedicated
    # handler owns TV rows (its VDP image/carfax completion runs post-scan).
    if provider != "team_velocity" and detect_team_velocity(raw_data):
        tv_rows = list(parse_team_velocity(raw_data, **_kwargs))
        if tv_rows:
            return tv_rows

    # Chapman Auto Group dealers (chapmanbmwchandler-com, chapmanfordaz-com) have
    # been captured with the recipe mis-tagged "dealer_dot_com" in production —
    # the generic parser guesses via loose key matching and returns rows, so
    # nothing flags it as broken, but it doesn't know colorExt/drive/fuel/body
    # and silently drops color/drivetrain/fuel/body_style for every row. Route
    # by shape BEFORE the declared provider so a mis-tagged recipe can never
    # again silently degrade this dealer's data.
    if provider != "chapman" and detect_chapman(raw_data):
        chapman_rows = list(parse_chapman(raw_data, **_kwargs))
        if chapman_rows:
            return chapman_rows

    fn = PARSERS.get(provider)
    if fn:
        result = list(fn(raw_data, **_kwargs))
        if result:
            return result
        # Declared parser returned nothing — fall through to others
    elif provider not in _warned_unknown_providers:
        _warned_unknown_providers.add(provider)
        _log.debug("Unknown provider %r for dealer_id=%s — trying auto-detect", provider, dealer_id)

    # Try remaining parsers
    for p_name, p_fn in PARSERS.items():
        if p_name == provider:
            continue  # already tried
        try:
            result = list(p_fn(raw_data, **_kwargs))
            if result:
                _log.debug("Auto-detected provider %r for dealer_id=%s (declared: %r)", p_name, dealer_id, provider)
                return result
        except Exception:
            continue
    return []


def parse(
    provider: str,
    raw_data,
    base_url: str,
    dealer_id: str,
    dealer_name: str = "",
    dealer_url: str = "",
    rejected_out: list | None = None,
    dealer_address: str = "",
    dealer_city: str = "",
    dealer_state: str = "",
    dealer_zip: str = "",
    dealer_address_source: str = "",
    trust_feed_scope: bool = False,
):
    """Parse *raw_data* into vehicle rows, resolving which store each belongs to.

    ``trust_feed_scope=True`` skips the rooftop gate: the caller replayed a
    recipe whose request is already filtered to this store's own feed ids
    (CarsCommerce ``facetFilters.source_id``, verified at synthesis by replaying
    the filter and seeing one rooftop in the store's city). Mall of Georgia
    Mazda files under two feed ids that stamp the same store two ways — a bare
    "Buford, GA" and a "3546 Highway 20" address block — and the gate, which can
    only ever pick ONE rooftop, refused 309 of 424 verified rows (2026-09-25).

    Pass ``rejected_out`` and you get back ONLY this store's rows, with the
    sibling rooftops' rows in that list — what the delta scanner wants, since it
    also has to disown VINs it stamped onto this store on earlier runs.

    Without ``rejected_out`` the sibling rows come back too, each carrying
    ``_rooftop_reject``. They are deliberately not swallowed here: the recipe
    replay counts VINs from this return value to decide whether a page was the
    last one and whether the endpoint is worth using at all, so silently
    emptying it truncates the page walk and retires the recipe of exactly the
    group-feed dealers this gate exists for. Storage is where the refusal is
    enforced — ``backend.scanner.database.upsert_vehicles`` drops every marked
    row, so a marked row can never reach ``cars``.

    The ``dealer_address``/``city``/``state``/``zip`` arguments are this store's
    postal address from the registry. They are the ONLY way to identify the
    store in a group feed whose rooftops are address blocks carrying no store
    name; omit them on such a feed and every page refuses every row. Callers
    that have a roster should pass them — see
    ``backend.scanner.delta_scan._roster_place``. ``dealer_address_source`` is
    that street's provenance (``dealerships.street_address_source``, V003):
    weak scrape tiers demote a street match so it cannot un-list on its own.
    """
    _kwargs = dict(base_url=base_url, dealer_id=dealer_id, dealer_name=dealer_name, dealer_url=dealer_url)
    rows = _parse_rows(provider, raw_data, _kwargs, dealer_id)
    if not trust_feed_scope and isinstance(raw_data, dict) and raw_data.get("_feed_scoped") is True:
        trust_feed_scope = True  # marker set by the recipe replay (recipes.py) on a store-scoped payload
    if trust_feed_scope:
        for r in rows:
            r["_feed_scoped"] = True  # the union pass in dealer_run must not re-gate these
        stamps = {str((r.get("_rooftop") or {}).get("key") or "")[:60] for r in rows if isinstance(r.get("_rooftop"), dict)}
        if len(stamps) > 1:
            _log.info("rooftop attribution [%s]: gate skipped, recipe is scoped to this store's own feed ids; %d row(s) across %d stamp(s) %s",
                      dealer_id or dealer_name, len(rows), len(stamps), sorted(stamps)[:4])
        if rejected_out is None:
            return rows
        return rows
    kept, rejected = resolve_rooftop_attribution(
        rows, dealer_id=dealer_id, dealer_name=dealer_name, dealer_url=dealer_url,
        dealer_address=dealer_address, dealer_city=dealer_city,
        dealer_state=dealer_state, dealer_zip=dealer_zip,
        dealer_address_source=dealer_address_source,
    )
    if rejected_out is None:
        return kept + rejected
    rejected_out.extend(rejected)
    return kept


@lru_cache(maxsize=256)
def _cached_roster_place_items(dealer_url: str) -> tuple[tuple[str, str], ...]:
    # roster_place already swallows lookup errors and returns {} (the registry
    # is an optimisation, never a parse blocker); cached because recipe
    # validation calls parse_kept once per page of the same dealer.
    from backend.scanner.rooftop_disown import roster_place

    return tuple(sorted((roster_place(dealer_url) or {}).items()))


def parse_kept(
    provider: str,
    raw_data,
    *,
    base_url: str,
    dealer_id: str,
    dealer_name: str = "",
    dealer_url: str = "",
    **place_override,
):
    """``parse()`` returning ONLY this store's rows, with the store identified.

    ``dealer_city`` / ``dealer_state`` / ``dealer_zip`` / ``dealer_address`` passed
    explicitly win over the registry lookup (a place learned from the site's own
    <title> when the store is not in the registry — dealer_place.learn_place).

    Discarding the rooftop gate's refusals is only safe when the gate can tell
    which rooftop this store IS — on a group feed whose rooftops are address
    blocks with no store name, ``parse(..., rejected_out=[])`` without the
    ``dealer_address``/``city``/``state``/``zip`` kwargs refuses every row and
    silently returns nothing (the terrylabontechevy.com failure). This wrapper
    looks the roster place up itself, so a kept-rows caller cannot forget it.
    Best-effort: a dealer missing from the registry falls back to the name and
    host tiers, exactly as before.
    """
    place = dict(_cached_roster_place_items(dealer_url or base_url))
    place.update({k: v for k, v in place_override.items() if k in ("dealer_address", "dealer_city", "dealer_state", "dealer_zip") and v})
    if place_override.get("trust_feed_scope"):
        place["trust_feed_scope"] = True
    return parse(
        provider, raw_data,
        base_url=base_url, dealer_id=dealer_id,
        dealer_name=dealer_name, dealer_url=dealer_url or base_url,
        rejected_out=[],
        **place,
    )
