"""HTTP session + homepage fetch (requests)."""
from __future__ import annotations

from dataclasses import dataclass, field
from urllib.parse import urlparse

import requests

from SCRAPING.constants import USER_AGENT
from SCRAPING.text_utils import dns_check


@dataclass
class HomepageFetchResult:
    """Result of a single GET to a dealer root (or start URL)."""

    html: str | None
    error: str | None
    redirect_chain: list[str]
    final_url: str
    flags: list[str] = field(default_factory=list)
    response_headers: dict[str, str] = field(default_factory=dict)


def fetch_requests_session(
    timeout: int,
    verify_ssl: bool,
) -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT, "Accept-Language": "en-US,en;q=0.9"})
    s.verify = verify_ssl
    return s


def fetch_homepage_full(
    session: requests.Session,
    url: str,
    timeout: int,
) -> HomepageFetchResult:
    flags: list[str] = []
    host = urlparse(url).netloc
    ok, dns_msg = dns_check(host)
    if not ok:
        flags.append("dns_failure")
        return HomepageFetchResult(
            None, dns_msg, [], url, flags=flags, response_headers={}
        )
    try:
        r = session.get(url, timeout=timeout, allow_redirects=True)
        chain = [h.headers.get("Location", "") for h in r.history if h.is_redirect]
        final = r.url
        if r.history:
            flags.append("redirect")
        r.raise_for_status()
        hdrs = {str(k): str(v) for k, v in r.headers.items()}
        return HomepageFetchResult(
            r.text, None, chain, final, flags=flags, response_headers=hdrs
        )
    except requests.exceptions.SSLError as e:
        flags.append("ssl_failure")
        return HomepageFetchResult(
            None, str(e), [], url, flags=flags, response_headers={}
        )
    except requests.exceptions.RequestException as e:
        if "403" in str(e) or (
            getattr(e, "response", None)
            and e.response is not None
            and e.response.status_code == 403
        ):
            flags.append("http_403")
        hdrs = {}
        final_u = url
        resp = getattr(e, "response", None)
        if resp is not None:
            if resp.headers:
                hdrs = {str(k): str(v) for k, v in resp.headers.items()}
            if resp.url:
                final_u = resp.url
        return HomepageFetchResult(
            None, str(e), [], final_u, flags=flags, response_headers=hdrs
        )


def fetch_homepage_requests(
    session: requests.Session,
    url: str,
    timeout: int,
) -> tuple[str | None, str | None, list[str], str, list[str]]:
    """Returns html, error, redirect_chain, final_url, domain_flags."""
    fr = fetch_homepage_full(session, url, timeout)
    return fr.html, fr.error, fr.redirect_chain, fr.final_url, fr.flags
