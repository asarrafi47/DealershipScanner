import logging

from backend.parsers.dealer_dot_com import parse as parse_dealer_dot_com
from backend.parsers.dealer_on import parse as parse_dealer_on

PARSERS = {
    "dealer_dot_com": parse_dealer_dot_com,
    "dealer_on": parse_dealer_on,
}

_log = logging.getLogger(__name__)
_warned_unknown_providers: set[str] = set()


def parse(provider: str, raw_data, base_url: str, dealer_id: str, dealer_name: str = "", dealer_url: str = ""):
    _kwargs = dict(base_url=base_url, dealer_id=dealer_id, dealer_name=dealer_name, dealer_url=dealer_url)
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
