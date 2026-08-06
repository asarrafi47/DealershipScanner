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


def roster_name_aliases(dealer_id: str) -> tuple[str, ...]:
    """Extra roster names to try for *dealer_id*, or ``()``."""
    return ROSTER_NAME_ALIASES.get(str(dealer_id or "").strip().lower(), ())


__all__ = ["ROSTER_NAME_ALIASES", "roster_name_aliases"]
