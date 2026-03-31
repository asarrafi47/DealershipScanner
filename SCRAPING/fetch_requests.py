"""HTTP session + homepage fetch (requests)."""
from __future__ import annotations

from urllib.parse import urlparse

import requests

from SCRAPING.constants import USER_AGENT
from SCRAPING.text_utils import dns_check


def fetch_requests_session(
    timeout: int,
    verify_ssl: bool,
) -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT, "Accept-Language": "en-US,en;q=0.9"})
    s.verify = verify_ssl
    return s


def fetch_homepage_requests(
    session: requests.Session,
    url: str,
    timeout: int,
) -> tuple[str | None, str | None, list[str], str, list[str]]:
    """Returns html, error, redirect_chain, final_url, domain_flags."""
    flags: list[str] = []
    host = urlparse(url).netloc
    ok, dns_msg = dns_check(host)
    if not ok:
        flags.append("dns_failure")
        return None, dns_msg, [], url, flags
    try:
        r = session.get(url, timeout=timeout, allow_redirects=True)
        chain = [h.headers.get("Location", "") for h in r.history if h.is_redirect]
        final = r.url
        if r.history:
            flags.append("redirect")
        r.raise_for_status()
        return r.text, None, chain, final, flags
    except requests.exceptions.SSLError as e:
        flags.append("ssl_failure")
        return None, str(e), [], url, flags
    except requests.exceptions.RequestException as e:
        if "403" in str(e) or (
            getattr(e, "response", None)
            and e.response is not None
            and e.response.status_code == 403
        ):
            flags.append("http_403")
        return None, str(e), [], url, flags
