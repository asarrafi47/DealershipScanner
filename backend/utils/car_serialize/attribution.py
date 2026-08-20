"""Public wording for the photo-attribution overlay.

``cars_repo.car_attribution_states`` decides WHETHER a listing's filed dealership can
still be presented as fact; this module decides what the shopper is told when it
cannot. One place, because the same sentence has to hold on a grid card, the car
detail page, the compare table and the dealership page — a card that says "location
unconfirmed" next to a detail page that states the address flatly is worse than
either alone.

Nothing here removes a car or blanks ``dealer_name``: the listing genuinely came off
that dealer's feed, and that is a true and useful thing to show. What we cannot
support is the leap from "this rooftop published it" to "this rooftop has it".
"""

from __future__ import annotations

from typing import Any

def attribution_public_fields(state: dict[str, Any] | None) -> dict[str, Any]:
    """Fields to merge into a serialized car, or ``{}`` when there is nothing to say.

    Absent for the ~99% of the fleet no photograph has judged, and for every verdict
    that leaves the filing standing, so this adds no weight to the listings payload
    for the cars it does not concern.
    """
    if not state or not state.get("location_unconfirmed"):
        return {}
    rooftop = (state.get("observed_rooftop") or "").strip() or None
    return {
        "location_confirmed": False,
        "attribution_status": state.get("status") or None,
        "observed_rooftop": rooftop,
        "location_note": attribution_note(state),
    }


def attribution_note(state: dict[str, Any] | None) -> str:
    """One sentence explaining why the filed dealership is not stated as fact."""
    if not state or not state.get("location_unconfirmed"):
        return ""
    rooftop = (state.get("observed_rooftop") or "").strip()
    if rooftop:
        # The strongest case: a plate frame, dealer decal or building sign in this
        # listing's own photos names a different store. Say what was seen rather
        # than only that we doubt the filing.
        return (
            f"This listing's photos show the vehicle at {rooftop}, "
            "not the dealership it is listed under. Confirm the location before you visit."
        )
    if state.get("group_feed"):
        # No photograph placed this one either way, but the feed it arrived on carries a
        # whole ownership group's inventory, so the publishing rooftop is not evidence of
        # where the car sits.
        return (
            "This dealership is served a feed covering its whole ownership group, so a car "
            "listed here may sit at another store. Confirm the location before you visit."
        )
    # Reachable when a photograph contradicts the filing but does not settle a replacement:
    # car 229310 is a Porsche listed by a BMW franchise, with one image watermarked "BMW of
    # Beverly Hills" and another showing a "World Famous BMW" plate frame. We know the filing
    # is wrong and cannot say what is right. Falling through to the group-feed sentence would
    # have asserted a feed arrangement we have no evidence for.
    return (
        "This listing's photos do not match the dealership it is listed under. "
        "Confirm the location before you visit."
    )
