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
    fn = PARSERS.get(provider)
    if not fn:
        if provider not in _warned_unknown_providers:
            _warned_unknown_providers.add(provider)
            _log.warning(
                "Unknown inventory provider %r — skipping parse (dealer_id=%s); "
                "use dealer_dot_com or dealer_on in dealers.json",
                provider,
                dealer_id,
            )
        return []
    return fn(raw_data, base_url=base_url, dealer_id=dealer_id, dealer_name=dealer_name, dealer_url=dealer_url)
