from backend.parsers.dealer_dot_com import parse as parse_dealer_dot_com
from backend.parsers.dealer_on import parse as parse_dealer_on

PARSERS = {
    "dealer_dot_com": parse_dealer_dot_com,
    "dealer_on": parse_dealer_on,
}


def parse(provider: str, raw_data, base_url: str, dealer_id: str, dealer_name: str = "", dealer_url: str = ""):
    fn = PARSERS.get(provider)
    if not fn:
        return []
    return fn(raw_data, base_url=base_url, dealer_id=dealer_id, dealer_name=dealer_name, dealer_url=dealer_url)
