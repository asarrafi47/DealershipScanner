"""
Decide, per dealer and per car, whether we actually know where the car is.

    .venv/bin/python -m backend.scripts.classify_attribution --dry-run
    .venv/bin/python -m backend.scripts.classify_attribution

Two passes.

**Dealer scope.** For every dealer with photographic evidence, compare the rooftop names its
photographs carry against the store it is filed as. A store whose own photos mostly name
other rooftops is being served a group feed. `bmwofmurrieta-com`: 168 photos name a dealer,
22 of them say "BMW of Murrieta", the rest name Hendrick rooftops in three other states.

**Per car.** Confirmed where a photograph names the filed dealer; conflicting where it names
someone else; unverified where there is no photograph AND the dealer is group-scoped.

Nothing is moved and nothing is deleted. This writes a judgement table that read-time code
consults, which is the same shape as every other correction in this project: the scanner owns
the write path, derived facts live beside it. Re-attributing a car needs its rooftop
registered and geocoded first, and doing it from a plate frame alone would repeat the Sold-To
regression where an invoice rooftop overwrote a correct plate-frame reading.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_log = logging.getLogger("classify_attribution")

VERSION = 100

# A store's photographs must name it this often for the feed to be its own. Below this the
# feed is serving the group. Set from the observed split: single-rooftop dealers in this
# corpus sit at 85-100% self-naming, the group-fed ones at 13-30%.
_SELF_SHARE_ROOFTOP = 0.60

# Below this many named photos we do not judge the dealer at all. Three plate frames is not
# a census, and calling a dealer group-fed on thin evidence would flag real inventory as
# unlocatable -- the expensive direction of this mistake.
_MIN_NAMED = 12


def _dsn() -> str:
    dsn = (os.environ.get("INVENTORY_DATABASE_URL") or os.environ.get("DATABASE_URL") or "").strip()
    if dsn:
        return dsn
    env = _REPO_ROOT / ".env"
    if env.exists():
        m = re.search(r"^INVENTORY_DATABASE_URL=(.+)$", env.read_text(), re.M)
        if m:
            return m.group(1).strip().strip("'\"")
    raise SystemExit("INVENTORY_DATABASE_URL is not set")


_PUNCT = re.compile(r"[^a-z0-9]+")
# Group-level branding that names an owner, not a rooftop. "AutoNation" on a BMW of Dallas
# car is ownership, not misattribution -- that store really is AutoNation-owned, and treating
# the watermark as a conflict would flag correct inventory fleet-wide.
_OWNER_WORDS = {
    "autonation", "hendrick", "hendrickcars", "hendrickautomotivegroup", "sonic",
    "sonicautomotive", "lithia", "penske", "group1", "asbury", "carmax", "echopark",
    "automotivegroup", "autogroup", "motorgroup",
}


def _norm(s: str) -> str:
    return _PUNCT.sub("", (s or "").lower())


def _is_owner_only(seen: str) -> bool:
    """True when the photo names a parent company rather than a specific store."""
    n = _norm(seen)
    return bool(n) and n in _OWNER_WORDS


# Words that carry no identity. "of" is in half these names; matching on it means nothing.
_STOP = {"of", "the", "inc", "llc", "auto", "automotive", "motors", "motor", "car", "cars",
         "dealer", "dealership", "group", "a", "and", "at"}

# Marque names are shared by every store of that franchise, so "bmw" matching "bmw" says
# nothing about WHICH BMW store. Identity lives in the non-marque tokens.
_MARQUES = {
    "bmw", "mini", "mercedesbenz", "mercedes", "benz", "audi", "volkswagen", "vw", "porsche",
    "toyota", "lexus", "scion", "honda", "acura", "nissan", "infiniti", "hyundai", "genesis",
    "kia", "mazda", "subaru", "volvo", "jaguar", "landrover", "chevrolet", "chevy", "gmc",
    "buick", "cadillac", "ford", "lincoln", "chrysler", "dodge", "jeep", "ram", "fiat",
    "tesla", "rivian", "lucid", "mitsubishi", "alfaromeo", "maserati", "dcjal", "cdjr",
}

_PAREN = re.compile(r"\([^)]*\)")


def _tokens(s: str) -> set[str]:
    """Identity tokens: parentheticals, stopwords and marque names removed."""
    cleaned = _PAREN.sub(" ", (s or "").lower())
    raw = {_PUNCT.sub("", t) for t in cleaned.split()}
    return {t for t in raw if t and t not in _STOP}


def _names_same_store(seen: str, dealer_name: str, dealer_id: str) -> bool:
    """
    Does the photographed name refer to the store the car is filed under?

    Token-set, not substring, and not word-order dependent. Three earlier versions of this
    idea failed on real data and each failure was the same shape -- a string test that looked
    reasonable and silently mis-scored a fleet:

      * bare ``in`` matched 'mb' inside "cumberland" and 'ford' inside "Ledford" (37 false hits)
      * ``regexp_replace(make,'[^a-z]','')`` ran before ``lower()``, so 'LEXUS' became '' and
        every uppercase-make dealer looked 100% off-brand
      * substring containment could not see that "Toyota Avondale" and "Avondale Toyota" are
        one store, nor that "Landers McLarty Dodge Chrysler Jeep Ram" and "Landers McLarty
        Chrysler Dodge Jeep Ram FIAT" are one store with the marques reordered

    So: compare identity tokens, ignoring order, parentheticals ("(A Sonic Automotive
    Dealer)"), stopwords, and marque names. Marques are excluded deliberately -- every BMW
    store shares "bmw", so counting it would make "BMW of Beverly Hills" look like a match for
    "BMW of Monrovia". What distinguishes rooftops is the place or family name.
    """
    a, b = _tokens(seen), _tokens(dealer_name)
    if not a or not b:
        return False
    ident_a, ident_b = a - _MARQUES, b - _MARQUES
    # Nothing but marque words on one side (a bare "BMW" watermark): no identity claim either
    # way, so treat it as a match rather than manufacture a conflict from a logo.
    if not ident_a or not ident_b:
        return True
    shared = ident_a & ident_b
    if shared:
        return len(shared) / min(len(ident_a), len(ident_b)) >= 0.5
    # Last resort: the host slug, which carries the rooftop ("bmwofmurrieta-com").
    slug = _PUNCT.sub("", (dealer_id or "").rsplit("-", 1)[0])
    return bool(slug) and any(t in slug for t in ident_a if len(t) >= 5)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    os.environ.setdefault("INVENTORY_DATABASE_URL", _dsn())
    import psycopg

    conn = psycopg.connect(_dsn(), connect_timeout=20)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(
        """
        SELECT c.dealer_id, COALESCE(MAX(c.dealer_name), ''), t.car_id,
               NULLIF(t.summary->>'dealer_name_seen', '')
        FROM car_image_text t
        JOIN cars c ON c.id = t.car_id
        WHERE t.version = %s AND c.listing_removed_at IS NULL
        GROUP BY c.dealer_id, t.car_id, t.summary->>'dealer_name_seen'
        """,
        (VERSION,),
    )
    rows = cur.fetchall()

    per_dealer: dict[str, dict] = defaultdict(
        lambda: {"name": "", "self": 0, "other": 0, "rooftops": set(), "cars": []}
    )
    for dealer_id, dealer_name, car_id, seen in rows:
        if not dealer_id:
            continue
        d = per_dealer[dealer_id]
        d["name"] = d["name"] or dealer_name
        if not seen:
            d["cars"].append((car_id, None, None))
            continue
        if _is_owner_only(seen):
            # Names the parent group. Says nothing about which rooftop -- neither evidence
            # for nor against, so it is not counted in the split.
            d["cars"].append((car_id, None, None))
            continue
        if _names_same_store(seen, dealer_name, dealer_id):
            d["self"] += 1
            d["cars"].append((car_id, "confirmed", seen))
        else:
            d["other"] += 1
            d["rooftops"].add(seen)
            d["cars"].append((car_id, "conflicting", seen))

    scopes: dict[str, str] = {}
    for dealer_id, d in per_dealer.items():
        named = d["self"] + d["other"]
        if named < _MIN_NAMED:
            continue
        scopes[dealer_id] = "rooftop" if d["self"] / named >= _SELF_SHARE_ROOFTOP else "group"

    group_ids = sorted(k for k, v in scopes.items() if v == "group")
    _log.info(
        "judged %d dealer(s) with >= %d named photos: %d group-fed, %d single-rooftop",
        len(scopes), _MIN_NAMED, len(group_ids), len(scopes) - len(group_ids),
    )
    for dealer_id in group_ids:
        d = per_dealer[dealer_id]
        named = d["self"] + d["other"]
        cur.execute(
            "SELECT count(*) FROM cars WHERE dealer_id = %s AND listing_removed_at IS NULL",
            (dealer_id,),
        )
        total = cur.fetchone()[0]
        _log.info(
            "  GROUP  %-30s %5d active cars | photos name self %d/%d (%.0f%%), "
            "%d other rooftop(s)",
            dealer_id, total, d["self"], named, 100 * d["self"] / named, len(d["rooftops"]),
        )

    # Per-car rows. Unverified applies ONLY inside group-fed dealers: elsewhere, absence of a
    # photograph is just absence of a photograph.
    car_rows: list[tuple] = []
    for dealer_id, d in per_dealer.items():
        scope = scopes.get(dealer_id)
        for car_id, status, seen in d["cars"]:
            if status is None:
                if scope != "group":
                    continue
                car_rows.append((car_id, "unverified", None, dealer_id,
                                 "no rooftop visible in gallery; dealer is served a group feed"))
            else:
                car_rows.append((
                    car_id, status, seen, dealer_id,
                    f"gallery names {seen!r}" + ("" if status == "confirmed"
                                                 else f"; filed as {d['name'] or dealer_id!r}"),
                ))

    counts: dict[str, int] = defaultdict(int)
    for r in car_rows:
        counts[r[1]] += 1
    _log.info(
        "%s %d car judgement(s): %d confirmed, %d conflicting, %d unverified",
        "would write" if args.dry_run else "writing", len(car_rows),
        counts["confirmed"], counts["conflicting"], counts["unverified"],
    )

    # How many cars sit at a group-fed dealer in total -- the real exposure, most of which has
    # no photograph yet.
    if group_ids:
        cur.execute(
            "SELECT count(*) FROM cars WHERE listing_removed_at IS NULL AND dealer_id = ANY(%s)",
            (group_ids,),
        )
        _log.info("cars filed under a group-fed dealer altogether: %d", cur.fetchone()[0])

    if args.dry_run:
        return 0

    for dealer_id, scope in scopes.items():
        d = per_dealer[dealer_id]
        cur.execute(
            """
            INSERT INTO dealer_feed_scope
                (dealer_id, scope, photos_naming_self, photos_naming_other,
                 distinct_rooftops, decided_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            ON CONFLICT (dealer_id) DO UPDATE SET
                scope = EXCLUDED.scope,
                photos_naming_self = EXCLUDED.photos_naming_self,
                photos_naming_other = EXCLUDED.photos_naming_other,
                distinct_rooftops = EXCLUDED.distinct_rooftops,
                decided_at = NOW()
            """,
            (dealer_id, scope, d["self"], d["other"], len(d["rooftops"])),
        )

    for car_id, status, seen, filed, evidence in car_rows:
        cur.execute(
            """
            INSERT INTO car_attribution
                (car_id, status, observed_rooftop, filed_dealer_id, evidence, decided_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            ON CONFLICT (car_id) DO UPDATE SET
                status = EXCLUDED.status,
                observed_rooftop = EXCLUDED.observed_rooftop,
                filed_dealer_id = EXCLUDED.filed_dealer_id,
                evidence = EXCLUDED.evidence,
                decided_at = NOW()
            """,
            (car_id, status, seen, filed, evidence),
        )

    _log.info("done")
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
