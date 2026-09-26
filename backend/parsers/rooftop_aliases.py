"""Per-dealer roster name aliases for the rooftop attribution gate — DATA ONLY.

Some feeds spell a store's name differently from the roster, and not every
difference is a rule. A brand abbreviation ("CDJR" for "Chrysler Dodge Jeep
Ram") is a rule and lives in the gate itself. A one-off misspelling in one
account's feed is not a rule: teaching the gate fuzzy/edit-distance matching to
absorb it would make it GUESS, and a gate that guesses attributes a car to the
wrong storefront the first time two rooftops are one edit apart. So a
misspelling is recorded here, per dealer, as the exact string the feed publishes.

Each entry is an EXTRA name the gate will accept for that dealer, tried after
the roster's own name and matched by the same unique-winner tiers. It can never
break a tie: if two rooftops match the alias the gate still refuses.

Keys are ``dealer_id`` slugs (the storefront host with dots as dashes, e.g.
``bmwofmurrieta-com``). Every entry MUST carry the date the spelling was
observed in the live feed, so a stale alias can be re-checked or dropped.
"""
from __future__ import annotations

ROSTER_NAME_ALIASES: dict[str, tuple[str, ...]] = {
    # Observed 2026-08-03 in this store's own CarsCommerce group payload: the
    # account writes ``dealer.location`` as "Ford Lincoln of Cookville" — the
    # city is Cook(e)ville, TN and the feed drops the "e". 324 of the 1,746
    # rows the replay returned carry that label. The same account misspells its
    # sibling rooftops too ("Nissan of Cookville:", "Hyundai of Cookville",
    # "Ford or Murfreesboro", "Ford Lincoln of Frankin"); those stores are not
    # scanned today, so no alias is recorded for them.
    "fordlincolnofcookeville-com": ("Ford Lincoln of Cookville",),
}


# Aliases LEARNED by recipe synthesis live in the dealer's scan hints (key
# ``rooftop_name_aliases``: [{"name", "observed", "evidence"}]) — still data, still
# per dealer, still exact strings. Recorded only when the store's own feed id
# (``source_id`` == the page's ``oem_code``) returned one rooftop whose name is
# not the roster's: that feed IS this store, so its spelling is this store's.
# Hendrick Buick GMC Cary → "Hendrick Buick GMC Cadillac Cary"; Rick Hendrick
# Chevrolet Naples → "Chevy Naples" (2026-09-24).
_HINT_KEY = "rooftop_name_aliases"
_hint_cache: dict[str, tuple[str, ...]] = {}


def _hinted_aliases(dealer_id: str) -> tuple[str, ...]:
    if dealer_id in _hint_cache:
        return _hint_cache[dealer_id]
    out: tuple[str, ...] = ()
    try:
        from backend.scanner.recipe_store import get_scan_hints

        raw = (get_scan_hints(dealer_id) or {}).get(_HINT_KEY) or []
        out = tuple(str(x.get("name") if isinstance(x, dict) else x).strip() for x in raw if x)
        out = tuple(a for a in out if a)
    except Exception:  # noqa: BLE001 - a hint-store outage must not change attribution
        out = ()
    _hint_cache[dealer_id] = out
    return out


def roster_name_aliases(dealer_id: str) -> tuple[str, ...]:
    """Extra roster names to try for *dealer_id*, or ``()``."""
    key = str(dealer_id or "").strip().lower()
    static = ROSTER_NAME_ALIASES.get(key, ())
    learned = _hinted_aliases(key) if key else ()
    return static + tuple(a for a in learned if a not in static)


def record_rooftop_alias(dealer_id: str, name: str, *, evidence: str, observed: str) -> bool:
    """Persist a feed-observed spelling for *dealer_id* (idempotent)."""
    key = str(dealer_id or "").strip().lower()
    name = str(name or "").strip()
    if not key or not name:
        return False
    try:
        from backend.scanner.recipe_store import get_scan_hints, set_scan_hints

        cur = list((get_scan_hints(key) or {}).get(_HINT_KEY) or [])
        if any((x.get("name") if isinstance(x, dict) else x) == name for x in cur):
            return True
        cur.append({"name": name, "observed": observed, "evidence": evidence[:300]})
        ok = set_scan_hints(key, {_HINT_KEY: cur}, merge=True)
    except Exception:  # noqa: BLE001
        return False
    _hint_cache.pop(key, None)
    return bool(ok)


__all__ = ["ROSTER_NAME_ALIASES", "roster_name_aliases", "record_rooftop_alias"]
