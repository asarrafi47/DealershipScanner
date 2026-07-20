import logging

from backend.parsers.carscommerce import detect as detect_carscommerce
from backend.parsers.carscommerce import parse as parse_carscommerce
from backend.parsers.chapman import parse as parse_chapman
from backend.parsers.dealer_dot_com import parse as parse_dealer_dot_com
from backend.parsers.dealer_eprocess import parse as parse_dealer_eprocess
from backend.parsers.dealer_on import parse as parse_dealer_on
from backend.parsers.jazel import parse as parse_jazel
from backend.parsers.motive_ridemotive import parse as parse_motive_ridemotive
from backend.parsers.overfuel import parse as parse_overfuel
from backend.parsers.sister_tv import parse as parse_sister_tv
from backend.parsers.team_velocity import detect as detect_team_velocity
from backend.parsers.team_velocity import parse as parse_team_velocity
from backend.parsers.typesense import parse as parse_typesense


def _parse_autowall(raw_data, *, base_url="", dealer_id="", dealer_name="", dealer_url=""):
    """Pass-through parser: autoWALL vehicles are already parsed dicts; return them as-is."""
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
}

_log = logging.getLogger(__name__)
_warned_unknown_providers: set[str] = set()


def parse(provider: str, raw_data, base_url: str, dealer_id: str, dealer_name: str = "", dealer_url: str = ""):
    _kwargs = dict(base_url=base_url, dealer_id=dealer_id, dealer_name=dealer_name, dealer_url=dealer_url)

    # CarsCommerce search payloads are provider-hinted "dealer_dot_com" /
    # "dealer_inspire", but the generic parser drops colors, carfax, features,
    # packages and warranty. Route by shape BEFORE the declared provider so the
    # rich fields the feed returns are actually captured.
    if provider != "carscommerce" and detect_carscommerce(raw_data):
        # Once the CarsCommerce shape is detected, its parser OWNS the payload —
        # even an empty result must not fall through to generic parsers, or the
        # group-feed sibling-store guard would be silently bypassed (the exact
        # all-rows-dropped case it exists for).
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
