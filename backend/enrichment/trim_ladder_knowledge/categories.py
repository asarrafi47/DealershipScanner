"""Rank and categorise trim adds; drop universal (every-trim) content."""
from __future__ import annotations

import re
from typing import Callable, Iterable

from .bullets import (
    is_wellformed_trim_bullet,
)

# ---------------------------------------------------------------------------
# "What this trim adds" ranking
#
# Brochure/CSV feature lists arrive in publication order, which puts trivia
# ("Security alarm system", "Bright scuff plates in cargo area") ahead of the
# thing that actually changes the car (the engine). The table below scores each
# bullet by CATEGORY so the ordering improves for every model at once instead of
# needing a hand-written list per nameplate.
#
# Rank bands (higher = shown first):
#   100-80  hardware that changes how the vehicle drives or what it can do
#    79-60  the equipment a shopper cross-shops on (seating, screen, audio, roof)
#    59-30  cabin comfort and driver-assist content
#    29-1   cosmetic / minor convenience
# ---------------------------------------------------------------------------

# (category key, rank, pattern). First match in this order wins per bullet, so
# the list is ordered by rank descending and patterns must be specific enough
# not to swallow a lower category (e.g. "audio system", never a bare "audio").
TRIM_ADD_CATEGORY_RULES: tuple[tuple[str, int, str], ...] = (
    (
        "powertrain",
        100,
        # "Remote Engine Start System" is a convenience feature, not a
        # powertrain change — unqualified, the word "engine" put it at the top
        # of the 2026 Pathfinder SV rung, above the AWD system.
        r"(?<!remote )\bengines?\b(?!\s+start)|\bhemi\b|\bpentastar\b|\bhurricane\b"
        r"|\becoboost\b|\bpowerboost\b"
        r"|\bcoyote\b|\bduramax\b|\bpower\s?stroke\b|\bcummins\b|\bboxer\b|\bskyactiv\b"
        r"|\bv-?6\b|\bv-?8\b|\bv-?10\b|\bv-?12\b|\bi-?4\b|\bi-?6\b|\binline-\d\b|\bflat-\d\b"
        r"|\b\d\.\d\s?l\b|\bturbocharged?\b|\btwin-turbo\b|\bsupercharged?\b|\bhorsepower\b"
        r"|\b\d+\s?hp\b|\blb-?ft\b|\btorque\b|\betorque\b|\bmild hybrid\b|\bplug-in hybrid\b"
        r"|\bhybrid powertrain\b|\belectric motors?\b|\bdual-motor\b|\bkwh\b|\bbattery pack\b"
        # "2-speed transfer case" is a drivetrain part, not a gearbox count.
        r"|\btransmission\b|\b\d+-speed\b(?!\s+transfer)|\bmds\b",
    ),
    (
        "drivetrain",
        92,
        r"\bawd\b|\b4wd\b|\b4x4\b|\ball-wheel drive\b|\bfour-wheel drive\b|\brear-wheel drive\b"
        r"|\btransfer case\b|\bquadra-trac\b|\bquadra-drive\b|\bxdrive\b|\bquattro\b|\b4motion\b"
        r"|\b4matic\b|\blimited-slip\b|\blocking (?:rear )?differential\b|\btorque vectoring\b",
    ),
    (
        "suspension",
        88,
        r"\bsuspension\b|\bair springs?\b|\bquadra-lift\b|\badaptive damping\b|\badaptive dampers?\b"
        r"|\bmagne-?ride\b|\bmagnetic ride\b|\bbilstein\b|\bmonotube\b|\bshock absorbers?\b"
        r"|\bload-level\w*\b|\bsway bar\b|\bride height\b|\bcoil springs?\b",
    ),
    (
        "brakes",
        84,
        r"\bbrembo\b|\bbrakes?\b|\bbrake rotors?\b|\bcalipers?\b",
    ),
    (
        "capability",
        80,
        r"\btow(?:ing)? capacity\b|\btow(?:ing)? (?:package|group)\b|\btrailer (?:tow|hitch|brake)\b"
        r"|\btow hitch\b|\bhitch receiver\b"
        r"|\bpayload\b|\bskid plates?\b|\boff-road\b|\btrail rated\b|\bdesert-rated\b|\brock mode\b"
        r"|\bgvwr\b|\bwading depth\b|\bapproach angle\b",
    ),
    (
        "seating",
        74,
        # Capacity and configuration only — a heated 2nd row is seat comfort, not
        # a seating change, so bare row numbers deliberately do not appear here.
        r"\bseating for \d\b|\b\d-passenger\b|\b(?:three|two)-row seating\b"
        r"|\bthird[- ]row seat\w*\b|\b3rd[- ]row (?:seat|bench|50/50)\w*\b"
        r"|\bcaptain'?s chairs\b|\bbench seat\b|\bsplit-folding\b|\bfold-and-tumble\b",
    ),
    # Wheels and tyres are quoted in inches too, and because the infotainment
    # rule below keys on a bare "<n>-inch" they were ALL being scored as screen
    # content — "20-inch gray machined alloy wheels" outranked the audio system
    # and the seats. Matching them first sends them to the exterior band, where
    # a wheel diameter belongs, and leaves the inch measure meaning "screen".
    (
        "exterior_wheels",
        58,
        r"\b(?:\d{2}(?:\.\d)?[- ]?(?:inch|in\.?|\")|\d{2} by \d(?:\.\d)?-inch)\b[^.]{0,40}"
        r"\b(?:wheels?|rims?|tires?|tyres?)\b"
        r"|\balloy wheels?\b|\bforged wheels?\b|\bbeadlock\b|\ball-season tires?\b"
        r"|\b\d{3}/\d{2}[A-Z]?\d{2}\b",
    ),
    (
        "infotainment",
        70,
        r"\b\d{1,2}(?:\.\d)?-inch\b|\b\d{1,2}(?:\.\d)?\"\b|\btouchscreen\b|\bdisplay screen\b"
        r"|\bcenter (?:screen|display)\b|\buconnect\b|\bsync ?\d\b|\bmbux\b|\bidrive\b"
        r"|\bhead-up display\b|\bdigital (?:instrument )?cluster\b|\bnavigation\b|\bnav\b",
    ),
    (
        "audio",
        66,
        r"\baudio system\b|\bpremium audio\b|\bsound system\b|\b\d+ speakers?\b|\bamplifier\b"
        r"|\b\d+-watt\b|\bsubwoofer\b|\bharman\b|\bbose\b|\balpine\b|\bbang ?& ?olufsen\b|\bb&o\b"
        r"|\bburmester\b|\bmark levinson\b|\bjbl\b|\brevel\b|\bmeridian\b|\bnaim\b|\bfender\b",
    ),
    (
        "roof_body",
        62,
        r"\bsunroof\b|\bmoonroof\b|\bpanoramic\b|\bdual-pane\b|\bglass roof\b|\btarga\b"
        r"|\bconvertible top\b|\bpower liftgate\b|\bhands-free liftgate\b|\bpower tailgate\b"
        r"|\bmultifunction tailgate\b|\btonneau\b|\bbed liner\b",
    ),
    (
        "exterior",
        58,
        r"\b\d{2}-inch wheels?\b|\b\d{2}\" wheels?\b|\balloy wheels?\b|\bforged wheels?\b"
        r"|\bled headla\w+\b|\bmatrix led\b|\badaptive headl\w+\b|\bhid headl\w+\b"
        r"|\brunning boards?\b|\broof rails?\b|\bblacktop\b|\bappearance package\b",
    ),
    (
        "seat_comfort",
        54,
        r"\bnappa\b|\balcantara\b|\bsuede\b|\bleather-appointed\b|\bleather (?:seat|trim|upholstery)\w*\b"
        # "Heated and ventilated FRONT seats" is the common OEM wording and was
        # falling through to the unranked catch-all, below plain cosmetic trim.
        r"|\b(?:ventilated|cooled|climate-controlled)\b(?:[\w\- ]{0,20}?)\bseats?\b|\bmassag\w+\b|\bmemory seats?\b"
        r"|\b\d+-way power (?:driver|passenger|front)\b|\bpower (?:driver|passenger) seat\b"
        r"|\blumbar\b|\bheated (?:front |rear |2nd-row |1st-row )?seats?\b",
    ),
    (
        "climate",
        48,
        r"\b(?:two|three|four|dual|tri)-zone\b|\bautomatic temperature control\b|\bclimate control\b"
        r"|\bheated steering wheel\b|\brear air conditioning\b|\bremote (?:engine )?start\b",
    ),
    (
        "driver_assist",
        44,
        r"\badaptive cruise\b|\bblind[- ]spot\b|\blane (?:keep|depart|centering)\w*\b"
        r"|\bforward collision\b|\bautomatic emergency braking\b|\bsurround-?view\b"
        r"|\b360-degree camera\b|\bparksense\b|\bpark assist\b|\bnight vision\b"
        r"|\bdriver[- ]assist\w*\b|\bcross[- ]path\b|\bcross[- ]traffic\b",
    ),
    (
        "cosmetic",
        12,
        r"\bsteering wheel\b|\bmirrors?\b|\bcargo (?:cover|net|organizer)\b|\bfloor console\b"
        r"|\bsun visors?\b|\bambient light\w*\b|\bbadg\w+\b|\bdecals?\b|\bpedals?\b"
        r"|\bshift knob\b|\bheadliner\b|\bchrome\b|\bbright \w+\b",
    ),
)

_TRIM_ADD_CATEGORY_RULES_COMPILED: tuple[tuple[str, int, "re.Pattern[str]"], ...] = tuple(
    (key, rank, re.compile(pattern, re.I)) for key, rank, pattern in TRIM_ADD_CATEGORY_RULES
)

# Fallback for anything the table does not recognise — below the named comfort
# bands but above the cosmetic bucket, so unknown-but-specific content survives.
TRIM_ADD_DEFAULT_RANK = 30

# Content that is effectively standard on every car of that vintage, so calling
# it a trim "add" is noise. ``since_year`` is the model year from which the item
# became universal (US federal mandate where one exists) — before that year the
# same words really were a differentiator and we leave the bullet alone.
UNIVERSAL_TRIM_ADD_RULES: tuple[tuple[str, int | None], ...] = (
    (r"\bsecurity alarm\b|\bvehicle (?:anti-)?theft\b|\bimmobiliz\w+\b|\balarm system\b", 2000),
    (r"\bscuff plates?\b|\bsill plates?\b|\bfloor mats?\b|\bcargo mat\b", None),
    (r"\bcup ?holders?\b|\bbottle holders?\b|\bstorage bins?\b", None),
    (r"\b12-?volt\b|\bpower outlets?\b|\baccessory outlets?\b|\bcigar lighter\b", None),
    (r"\bpower windows?\b|\bpower (?:door )?locks?\b|\bpower steering\b", 2000),
    (r"\bremote keyless entry\b|\bkeyless entry\b", 2010),
    (r"\bspeed control\b|(?<!adaptive )\bcruise controls?\b", 2005),
    (r"\btrial subscriptions?\b|\btrial period\b|\b\d+-(?:year|month) trial\b", None),
    (r"\bsiriusxm\b|\bsatellite radio\b|\bhd radio\b|\btravel link\b", 2015),
    # Federal mandates: rear visibility FMVSS 111 (MY2018), ESC FMVSS 126
    # (MY2012), TPMS FMVSS 138 (MY2008), advanced air bags FMVSS 208 (MY2007).
    (r"\bback-?up camera\b|\brear ?view camera\b|\bparkview\b|\brear visibility\b", 2018),
    (r"\belectronic stability control\b|\btraction control\b|\banti-?lock brak\w+\b", 2012),
    (r"\btire pressure monitor\w*\b", 2008),
    (r"\bair ?bags?\b|\bchild seat anchors?\b|\blatch system\b|\bseat ?belt pretension\w*\b", 2007),
    (r"\bdaytime running (?:lamps?|lights?)\b", 2015),
    (r"\bbluetooth\b|\bhands-free (?:phone|calling)\b|\busb ports?\b|\bauxiliary input\b", 2016),
    (r"\brear window defroster\b|\brear (?:window )?wiper\b|\bintermittent wipers?\b", 2000),
    (r"\bvanity mirrors?\b|\bmap lights?\b|\bglove box lamp\b|\bdome lamp\b", None),
    (r"\btilt/?telescoping steering column\b(?!.*\bpower\b)", 2010),
)

_UNIVERSAL_TRIM_ADD_RULES_COMPILED: tuple[tuple["re.Pattern[str]", int | None], ...] = tuple(
    (re.compile(pattern, re.I), since) for pattern, since in UNIVERSAL_TRIM_ADD_RULES
)

# A bare spec-sheet label ("All-Wheel Drive", "8-Speed Automatic") is a field
# value that leaked out of a spec table, not something the trim adds — it scores
# high on category alone, so it has to be dropped before ranking rather than by
# the commodity rule below.
_BARE_SPEC_LABEL_RE = re.compile(
    r"^(?:"
    r"(?:all|four|rear|front)-wheel drive"
    r"|awd|rwd|fwd|4wd|4x4|4x2|2wd"
    r"|\d+-speed(?: (?:automatic|manual))?(?: transmission)?"
    r"|automatic(?: transmission)?|manual(?: transmission)?"
    r"|gasoline|regular unleaded|premium unleaded|diesel|hybrid|electric"
    r")\.?$",
    re.I,
)

# A bullet that also carries hardware content (engine, screen size, roof, ...)
# survives even if it mentions a commodity item in passing — a bullet whose only
# substance is comfort/cosmetic does not.
_UNIVERSAL_DROP_RANK_CEILING = 60

# Keeps one *named* category from filling the whole list (six audio bullets,
# say). It is a preference, not a filter: anything held back by the cap is
# appended afterwards if the max_items budget still has room, so the cap can
# reorder content but can never delete it. ``other`` is exempt — it is the
# catch-all for everything the table does not recognise, so its members are not
# alike and capping them dropped unrelated bullets while slots sat empty.
_MAX_PER_CATEGORY = 3
_UNCAPPED_CATEGORIES = frozenset({"other", ""})

# Ranks at or above this are "changes how it drives / what it can do"; below is
# cabin content. The split is what the slot reservation in rank_trim_adds uses.
_HARDWARE_RANK_FLOOR = 80


def trim_add_category(text: str) -> tuple[str, int]:
    """Classify one 'what this trim adds' bullet → (category key, rank)."""
    s = str(text or "").strip()
    if not s:
        return ("", 0)
    for key, rank, pattern in _TRIM_ADD_CATEGORY_RULES_COMPILED:
        if pattern.search(s):
            return (key, rank)
    return ("other", TRIM_ADD_DEFAULT_RANK)


def is_universal_trim_add(text: str, *, year: int | None = None) -> bool:
    """True when the bullet describes equipment every car of that year already has."""
    s = str(text or "").strip()
    if not s:
        return False
    try:
        y = int(year) if year else None
    except (TypeError, ValueError):
        y = None
    for pattern, since in _UNIVERSAL_TRIM_ADD_RULES_COMPILED:
        if not pattern.search(s):
            continue
        if since is not None and y is not None and y < since:
            continue
        return True
    return False


def strip_trial_subscription_clause(text: str) -> str:
    """Drop the '... with 5-year trial subscription' tail; keep the feature itself.

    The data allowance in front of the term is part of the same offer, so
    "4G LTE WiFi Hotspot with 1GB/3-month trial" has to lose "1GB/" too —
    stopping at the number left the dangling "…with 1GB/" on the page.
    """
    s = str(text or "").strip()
    s = re.sub(
        r"\s*(?:,|;|\band\b|\bwith\b|\bincluding\b)?\s*(?:a\s+)?"
        r"(?:[\w.]+\s*[GMT]B\s*/\s*)?\d+-(?:year|month|day)\s+trial"
        r"(?:\s+subscriptions?|\s+periods?)?\.?\s*$",
        "",
        s,
        flags=re.I,
    )
    s = re.sub(r"\s*(?:,|;|\band\b|\bwith\b)?\s*trial subscriptions?\.?\s*$", "", s, flags=re.I)
    # Whatever the strip left behind must still read as a finished phrase.
    s = re.sub(r"[\s,;:/\\\-–—]+$", "", s)
    s = re.sub(r"\s+\b(?:with|and|including|plus|or|for)\b$", "", s, flags=re.I)
    return s.strip() or str(text or "").strip()


def rank_trim_adds(
    adds: Iterable[str] | None,
    *,
    year: int | None = None,
    max_items: int = 8,
    hardware_hint: "Callable[[str], bool] | None" = None,
) -> list[str]:
    """Order 'what this trim adds' by what actually changes the car.

    Drops commodity equipment, then sorts by category rank (stable within a
    rank, so brochure order still breaks ties). If every bullet turns out to be
    commodity content we keep the best few rather than blanking the trim — an
    empty list sends the caller into generic marketing prose, which is worse.

    ``hardware_hint`` is an optional caller-side predicate for hardware wording
    the category table does not cover; a hit floors the bullet into the top band.
    """
    scored: list[tuple[int, int, str, str]] = []
    demoted: list[tuple[int, int, str, str]] = []
    for i, raw in enumerate(adds or []):
        line = strip_trial_subscription_clause(str(raw or "").strip())
        # Re-check our own edit: this is the last transform before display, so a
        # strip that left a half-phrase must not be shown.
        if not line or not is_wellformed_trim_bullet(line):
            continue
        key, rank = trim_add_category(line)
        if _BARE_SPEC_LABEL_RE.match(line):
            demoted.append((0, i, "spec_label", line))
            continue
        if hardware_hint is not None and rank < 84 and hardware_hint(line):
            key, rank = ("hardware", 84)
        if is_universal_trim_add(line, year=year) and rank < _UNIVERSAL_DROP_RANK_CEILING:
            demoted.append((rank, i, key, line))
            continue
        scored.append((rank, i, key, line))

    def _take(pool: list[tuple[int, int, str, str]], limit: int) -> list[str]:
        """Best-ranked first, spreading across categories — but never dropping.

        The per-category cap only *defers*: whatever it holds back is appended
        once every category has had its turn, so a bullet is lost only when the
        caller's own ``limit`` is genuinely full.
        """
        if limit <= 0:
            return []
        out: list[str] = []
        deferred: list[str] = []
        per_category: dict[str, int] = {}
        for _rank, _i, key, line in sorted(pool, key=lambda t: (-t[0], t[1])):
            if len(out) >= limit:
                break
            if key not in _UNCAPPED_CATEGORIES and per_category.get(key, 0) >= _MAX_PER_CATEGORY:
                deferred.append(line)
                continue
            per_category[key] = per_category.get(key, 0) + 1
            out.append(line)
        for line in deferred:
            if len(out) >= limit:
                break
            out.append(line)
        return out

    # Hardware leads, but it does not get to eat the whole list: a shopper still
    # wants to see the screen and the seats, so the cabin half keeps a few slots
    # whenever both kinds of content exist.
    hardware = [t for t in scored if t[0] >= _HARDWARE_RANK_FLOOR]
    cabin = [t for t in scored if t[0] < _HARDWARE_RANK_FLOOR]
    if hardware and cabin:
        hardware_slots = max(4, max_items - 3)
        out = _take(hardware, hardware_slots)
        out += _take(cabin, max(0, max_items - len(out)))
    else:
        out = _take(scored, max_items)
    if not out:
        out = _take(demoted, min(3, max_items))
    return out[:max_items]
