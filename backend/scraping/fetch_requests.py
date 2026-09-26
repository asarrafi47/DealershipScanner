"""HTTP session + homepage fetch (requests)."""
from __future__ import annotations

import ipaddress
import socket
from dataclasses import dataclass, field
from urllib.parse import urljoin, urlparse

import requests

from backend.scraping.constants import USER_AGENT
from backend.scraping.text_utils import dns_check

# Max redirect hops we will follow while re-validating the destination of each
# hop (mirrors requests' own default cap).
_MAX_REDIRECTS = 10


def _resolve_ips(host: str) -> list[str]:
    """Resolve `host` to its IP addresses, or [] if resolution fails."""
    try:
        infos = socket.getaddrinfo(host, None)
    except OSError:
        return []
    return list({info[4][0] for info in infos})


def _is_unsafe_ip(ip_str: str) -> bool:
    try:
        ip = ipaddress.ip_address(ip_str)
    except ValueError:
        return True
    return (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_multicast
        or ip.is_reserved
        or ip.is_unspecified
    )


def _host_is_unsafe(host: str | None) -> bool:
    """SSRF guard: True unless every address `host` resolves to is a public,
    routable address. Blocks loopback/RFC1918/link-local (incl. the
    169.254.169.254 cloud-metadata address)/multicast/reserved destinations.
    """
    if not host:
        return True
    ips = _resolve_ips(host)
    if not ips:
        return True
    return any(_is_unsafe_ip(ip) for ip in ips)


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
    if _host_is_unsafe(urlparse(url).hostname):
        flags.append("ssrf_blocked")
        return HomepageFetchResult(
            None,
            "blocked: destination resolves to a private/loopback/link-local address",
            [],
            url,
            flags=flags,
            response_headers={},
        )
    current_url = url
    chain: list[str] = []
    try:
        # allow_redirects is disabled so each hop's destination can be
        # re-validated before it is connected to (closes the DNS-rebinding
        # gap between the pre-flight check above and the actual connection).
        for _ in range(_MAX_REDIRECTS + 1):
            r = session.get(current_url, timeout=timeout, allow_redirects=False)
            if r.is_redirect or r.is_permanent_redirect:
                location = r.headers.get("Location", "")
                if not location:
                    break
                next_url = urljoin(current_url, location)
                if _host_is_unsafe(urlparse(next_url).hostname):
                    flags.append("ssrf_blocked")
                    return HomepageFetchResult(
                        None,
                        "blocked: redirect destination resolves to a "
                        "private/loopback/link-local address",
                        chain,
                        next_url,
                        flags=flags,
                        response_headers={},
                    )
                chain.append(location)
                flags.append("redirect")
                current_url = next_url
                continue
            r.raise_for_status()
            hdrs = {str(k): str(v) for k, v in r.headers.items()}
            return HomepageFetchResult(
                r.text, None, chain, current_url, flags=flags, response_headers=hdrs
            )
        flags.append("too_many_redirects")
        return HomepageFetchResult(
            None, "too many redirects", chain, current_url, flags=flags, response_headers={}
        )
    except requests.exceptions.SSLError as e:
        flags.append("ssl_failure")
        return HomepageFetchResult(
            None, str(e), chain, current_url, flags=flags, response_headers={}
        )
    except requests.exceptions.RequestException as e:
        if "403" in str(e) or (
            getattr(e, "response", None)
            and e.response is not None
            and e.response.status_code == 403
        ):
            flags.append("http_403")
        hdrs = {}
        final_u = current_url
        resp = getattr(e, "response", None)
        if resp is not None:
            if resp.headers:
                hdrs = {str(k): str(v) for k, v in resp.headers.items()}
            if resp.url:
                final_u = resp.url
        return HomepageFetchResult(
            None, str(e), chain, final_u, flags=flags, response_headers=hdrs
        )


def fetch_homepage_requests(
    session: requests.Session,
    url: str,
    timeout: int,
) -> tuple[str | None, str | None, list[str], str, list[str]]:
    """Returns html, error, redirect_chain, final_url, domain_flags."""
    fr = fetch_homepage_full(session, url, timeout)
    return fr.html, fr.error, fr.redirect_chain, fr.final_url, fr.flags
