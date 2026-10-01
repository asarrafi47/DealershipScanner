"""
Move cars to the rooftop their photographs actually show.

    .venv/bin/python -m backend.scripts.apply_attribution_moves --dry-run
    .venv/bin/python -m backend.scripts.apply_attribution_moves
    .venv/bin/python -m backend.scripts.apply_attribution_moves --revert

This is the step that changes what a shopper sees. Everything before it only recorded
evidence: `car_attribution` says a car filed at BMW of Murrieta carries a plate frame
reading "Rick Hendrick Chevrolet Naples, FL", and 81% of the photo-judged cars at that one
dealer are somewhere else entirely.

Three conditions before a car moves, all of them hard:

1. ``status = 'conflicting'`` -- a photograph named a rooftop, and it is not the filed one.
   Never ``unverified`` (no evidence) and never ``owner_only`` (a parent-group watermark is
   ownership, not location, and BMW of Dallas legitimately carries AutoNation branding).
2. The named rooftop resolves to exactly ONE active dealership by strict token match,
   marque names included. The loose matcher used elsewhere reduces "Hendrick Porsche" and
   "Hendrick Chevrolet Hoover" both to {hendrick}; that is fine for asking "is this my own
   store" and catastrophic for choosing a destination.
3. That dealership has coordinates. Radius search is driven by the dealer geocode, so
   moving a car to an ungeocoded rooftop HIDES it -- the fix would look like data loss.

Every move is logged in ``car_move_log`` with the origin, so ``--revert`` restores the
previous state exactly. That is not a nicety: three separate agent passes this week
proposed 179 MSRP deletions and not one was correct, and the only reason that cost nothing
was that the prior value had been kept.
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from pathlib import Path
from urllib.parse import urlparse

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from backend.db.connect import connect as db_connect, inventory_dsn  # noqa: E402

_log = logging.getLogger("attribution_moves")

_DDL = """
CREATE TABLE IF NOT EXISTS car_move_log (
    id                  BIGSERIAL PRIMARY KEY,
    car_id              BIGINT NOT NULL,
    from_dealer_id      TEXT,
    from_dealer_name    TEXT,
    from_registry_id    BIGINT,
    to_dealer_id        TEXT,
    to_dealer_name      TEXT,
    to_registry_id      BIGINT,
    evidence            TEXT,
    moved_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    reverted_at         TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_car_move_log_car ON car_move_log (car_id, moved_at DESC);
"""


def _dealer_slug(website: str) -> str:
    """``https://www.bmwofbeverlyhills.com`` -> ``bmwofbeverlyhills-com``."""
    host = (urlparse(website or "").hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    return host.replace(".", "-")


# ---------------------------------------------------------------------------
# Choosing which registered store a photographed name refers to.
#
# This is the whole risk surface of the script, and every rule below is here because a
# looser version of it moved real cars to real wrong places. It is module-level rather
# than nested in main() so the failures have regression tests --
# backend/tests/test_attribution_destination.py replays each one.
# ---------------------------------------------------------------------------

# Group rows must be recognised BEFORE tokenisation.
#
# The first version tested the tokens against a set of group words and could never fire:
# `_STOP` already deletes "automotive", "group", "motors" and "cars", so "Hendrick
# Automotive Group" arrives as the single token {hendrick} and looks like an ordinary
# store. It then scored a perfect 1.0 against Hendrick Porsche, Rick Hendrick Chevrolet
# Duluth and every other Hendrick rooftop, turning each into a two-way tie and blocking
# 40+ correct moves as "ambiguous".
_GROUP_RE = re.compile(r"\b(auto(motive)?\s+group|group|holdings|enterprises)\b")

# Groups big enough to appear on a plate frame without being registered as their own row.
_KNOWN_GROUPS = {"autonation", "sonic", "penske", "lithia", "asbury", "echopark"}


def _toks(name: str) -> set[str]:
    """Identity tokens, with hyphens treated as spaces.

    `_tokens` strips punctuation rather than splitting on it, so the hyphenated registry
    spelling "Stevenson-Hendrick" became one token `stevensonhendrick` sharing nothing with
    the unhyphenated spelling a plate frame shows.
    """
    from backend.scripts.classify_attribution import _tokens

    return _tokens((name or "").replace("-", " ").replace("/", " "))


def _marques() -> set[str]:
    from backend.scripts.classify_attribution import _MARQUES

    return _MARQUES


def is_unusable_destination(name: str) -> bool:
    """Registry rows that can never be a destination.

    The first run reported 110 "ambiguous" cars and almost none were genuinely ambiguous:
    the registry holds discovery artifacts named just "Chevrolet" (three of them, 0 cars,
    no website) and "Mazda", which match every Chevrolet and Mazda rooftop on earth, plus
    "Hendrick Automotive Group", which matches every Hendrick store. A bare-marque row is
    also what moved car 494553 from California to Newport Beach: "Jaguar Land Rover" scored
    a perfect match against "Jaguar Land Rover Charlotte".
    """
    toks = _toks(name)
    if not toks:
        return True
    if _GROUP_RE.search((name or "").lower()):
        return True                       # group-level: "Hendrick Automotive Group"
    return not (toks - _marques())        # bare marque: "Chevrolet", "Jaguar Land Rover"


def group_vocabulary(names) -> set[str]:
    """Words that name a PARENT GROUP rather than a rooftop.

    Read off the registry's own group rows, so onboarding a new group teaches the rule
    without a code change.
    """
    vocab: set[str] = set()
    for name in names:
        if _GROUP_RE.search((name or "").lower()):
            vocab |= (_toks(name) - _marques())
    return vocab | _KNOWN_GROUPS


# Group branding that arrives looking like a store name.
#
# "HendrickCars.com" is a shopping site for 90+ rooftops; "1Price Pre-Owned Vehicles" is an
# AutoNation programme, not an address. `_tokens` cannot see through them -- it strips the
# dot in "Cars.com" and glues the rest into one token -- so they are listed.
_GROUP_BRAND_TOKENS = {
    "hendrickcars", "hendrickcarscom", "carscom", "com",
    # `_toks` splits on hyphens, so "Pre-Owned" arrives as two tokens, not one.
    "1price", "price", "preowned", "pre", "owned", "vehicles", "certified",
}


def names_a_specific_rooftop(name: str, group_tokens: set[str]) -> bool:
    """Could this photographed name, in principle, identify ONE store?

    A `conflicting` verdict claims a photograph named a DIFFERENT rooftop than the one the
    car is filed under. Group branding cannot support that claim: BMW of Dallas legitimately
    carries AutoNation branding, and a HendrickCars.com plate frame is on cars at every
    Hendrick store in the country. 25 of the conflicts recorded here named only a group, so
    the conflict count overstated what the photographs actually established.

    Whatever survives after removing the parent-group vocabulary, group web brands and bare
    numbers is the part that could point at an address.

    The marque is NOT removed here, unlike everywhere else in this module. Elsewhere "bmw"
    is noise because every BMW store shares it; here it is the whole signal. A group owns
    exactly one Porsche store, so "Hendrick Porsche" names a rooftop, while "HendrickCars.com"
    -- the same group's shopping site, whose plate frames are on cars in 90+ stores -- names
    nothing. Stripping marques judged both alike and would have discarded 9 correct conflicts.
    """
    # A name that SAYS it is a group is one, whatever the registry knows.
    #
    # `group_tokens` is learned from registry rows, and a group is usually not a dealership:
    # there is no "Darrell Waltrip Automotive Group" row, only its three rooftops, so
    # {darrell, waltrip} could never be learned and three cars kept asserting they were at a
    # holding company. The words are right there in the name.
    #
    # Parentheticals are stripped first, because they carry ownership notes rather than the
    # store's identity -- "BMW of South Austin (Hendrick Automotive Group)" names a rooftop
    # and would otherwise be rejected by its own footnote.
    from backend.scripts.classify_attribution import _PAREN

    if _GROUP_RE.search(_PAREN.sub(" ", (name or "").lower())):
        return False
    rest = _toks(name) - group_tokens - _GROUP_BRAND_TOKENS
    return bool({t for t in rest if not t.isdigit()})


def score(a: str, b: str) -> float:
    """Overlap of the shorter identity-token set. Marques KEPT."""
    ta, tb = _toks(a), _toks(b)
    if not ta or not tb:
        return 0.0
    small, big = (ta, tb) if len(ta) <= len(tb) else (tb, ta)
    return len(small & big) / len(small)


def specificity(observed: str, candidate: str) -> int:
    """How many IDENTITY tokens the two names genuinely share.

    `score` saturates at 1.0 whenever one name contains the other, so "Hendrick BMW" and
    "Hendrick BMW Northlake" both score perfectly against "Hendrick BMW Northlake". This
    breaks that tie by how much NON-MARQUE identity the candidate accounts for. Marques are
    excluded because counting them picks the store with the longest franchise list rather
    than the right family -- on "Hendrick Chrysler Dodge Jeep Ram Fiat", raw token overlap
    prefers "Hunter Dodge Chrysler Jeep RAM FIAT" (5 marques shared, wrong family) over
    "Hendrick Dodge Chrysler Jeep", which is the actual store.
    """
    marq = _marques()
    return len((_toks(observed) - marq) & (_toks(candidate) - marq))


def explains(observed: str, candidate: str, group_tokens: set[str]) -> bool:
    """Is every identity word in the photographed name accounted for by this store?

    `specificity` alone will happily pick the family-generic row when the specific one is
    not registered. "Hendrick BMW of McKinney" (Texas), "Hendrick Honda Hickory", "Hendrick
    Honda Bradenton" and "Hendrick BMW Southpoint" all resolved to plain "Hendrick
    BMW"/"Hendrick Honda" in Charlotte, NC -- cars sent 1,000+ miles from the store in their
    own photographs.

    A leftover word is safe only when it names the parent GROUP. "Hendrick BMW of South
    Austin" -> "BMW of South Austin" leaves {hendrick}, which is ownership, and the rooftop
    is still uniquely named. "Hendrick BMW Southpoint" -> "Hendrick BMW" leaves
    {southpoint}, which is a different rooftop, and dropping it invents a location.
    """
    marq = _marques()
    obs_t, cand_t = _toks(observed), _toks(candidate)
    if (obs_t - marq) - (cand_t - marq) - group_tokens:
        return False
    # A house name and a franchise name are not the same kind of name.
    #
    # "Hendrick Motors of Charlotte" is the group's Mercedes-Benz store. `_STOP` drops
    # "motors" as generic, leaving {hendrick, charlotte} -- identical to what "Hendrick
    # Lexus Charlotte" reduces to, so a Mercedes was about to be filed at the Lexus store
    # across town. Requiring the franchise word on both sides, or neither, keeps them apart.
    # Exact matches are unaffected: identical token sets necessarily agree here.
    return not (cand_t & marq) or bool(obs_t & marq)


def choose_destination(observed: str, dealers, group_tokens: set[str]):
    """Pick the one registered store a photographed name refers to.

    `dealers` are ``(id, name, website, latitude)`` rows. Returns ``(row, reason)`` with
    row None when nothing may be chosen; reason is ``"unmapped"`` or ``"ambiguous"``.
    """
    hits = [
        (score(observed, d[1] or ""), d)
        for d in dealers
        if not is_unusable_destination(d[1] or "")
    ]
    hits = [h for h in hits if h[0] >= 0.80 and explains(observed, h[1][1] or "", group_tokens)]
    if not hits:
        return None, "unmapped"

    hits.sort(key=lambda x: (-x[0], -specificity(observed, x[1][1] or "")))

    # An exact identity match settles it outright: "Hendrick Honda" IS the store called
    # Hendrick Honda, even though "Hendrick Honda Easley" also scores 1.0.
    exact = [h for h in hits if _toks(observed) == _toks(h[1][1] or "")]
    if len(exact) == 1:
        return exact[0][1], "exact"

    # Otherwise a tie between two real stores is genuine ambiguity -- "Stevenson-Hendrick
    # Honda" is a Wilmington store and a Jacksonville store, and guessing is how a car lands
    # at the wrong one. Require a strictly better winner on both keys.
    if len(hits) > 1 and (
        hits[0][0] == hits[1][0]
        and specificity(observed, hits[0][1][1] or "")
        == specificity(observed, hits[1][1][1] or "")
    ):
        return None, "ambiguous"
    return hits[0][1], "specificity"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--revert", action="store_true", help="undo every move not already reverted")
    ap.add_argument("--limit", type=int)
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    inventory_dsn(export=True)
    conn = db_connect(autocommit=True, timeout=20)
    cur = conn.cursor()
    cur.execute(_DDL)

    if args.revert:
        cur.execute(
            "SELECT id, car_id, from_dealer_id, from_dealer_name, from_registry_id "
            "FROM car_move_log WHERE reverted_at IS NULL ORDER BY id DESC"
        )
        rows = cur.fetchall()
        for log_id, car_id, did, dname, rid in rows:
            cur.execute(
                "UPDATE cars SET dealer_id = %s, dealer_name = %s, dealership_registry_id = %s "
                "WHERE id = %s",
                (did, dname, rid, car_id),
            )
            cur.execute("UPDATE car_move_log SET reverted_at = NOW() WHERE id = %s", (log_id,))
        _log.info("reverted %d move(s)", len(rows))
        conn.close()
        return 0

    cur.execute(
        "SELECT id, name, COALESCE(NULLIF(dealer_website_url,''), website_url, ''), latitude "
        "FROM dealerships WHERE is_active = 1"
    )
    dealers = cur.fetchall()
    group_tokens = group_vocabulary(d[1] or "" for d in dealers)

    cur.execute(
        """
        SELECT a.car_id, a.observed_rooftop, a.evidence,
               c.dealer_id, COALESCE(c.dealer_name,''), c.dealership_registry_id
        FROM car_attribution a
        JOIN cars c ON c.id = a.car_id
        WHERE a.status = 'conflicting'
          AND a.observed_rooftop IS NOT NULL
          AND c.listing_removed_at IS NULL
        ORDER BY a.car_id
        """
    )
    candidates = cur.fetchall()

    moves: list[tuple] = []
    ambiguous = ungeocoded = unmapped = 0
    for car_id, rooftop, evidence, from_did, from_name, from_rid in candidates:
        best, why = choose_destination(rooftop, dealers, group_tokens)
        if best is None:
            if why == "ambiguous":
                ambiguous += 1
            else:
                unmapped += 1
            continue
        rid, dname, site, lat = best
        if lat is None:
            ungeocoded += 1
            continue
        to_did = _dealer_slug(site)
        if not to_did or to_did == from_did:
            continue
        moves.append((car_id, from_did, from_name, from_rid, to_did, dname, rid, evidence))

    if args.limit:
        moves = moves[: args.limit]

    _log.info(
        "%s %d move(s); skipped %d unmapped, %d ambiguous, %d ungeocoded",
        "would apply" if args.dry_run else "applying", len(moves), unmapped, ambiguous, ungeocoded,
    )
    by_dest: dict[str, int] = {}
    for m in moves:
        by_dest[m[5]] = by_dest.get(m[5], 0) + 1
    for dest, n in sorted(by_dest.items(), key=lambda x: -x[1])[:15]:
        _log.info("    -> %-44s %d car(s)", dest[:44], n)

    if args.dry_run:
        conn.close()
        return 0

    for car_id, from_did, from_name, from_rid, to_did, to_name, to_rid, evidence in moves:
        cur.execute(
            "INSERT INTO car_move_log (car_id, from_dealer_id, from_dealer_name, "
            "from_registry_id, to_dealer_id, to_dealer_name, to_registry_id, evidence) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            (car_id, from_did, from_name, from_rid, to_did, to_name, to_rid, evidence),
        )
        cur.execute(
            "UPDATE cars SET dealer_id = %s, dealer_name = %s, dealership_registry_id = %s "
            "WHERE id = %s",
            (to_did, to_name, to_rid, car_id),
        )
    _log.info("moved %d car(s); undo with --revert", len(moves))
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
