"""Client IP for rate limiting and logging — never trust X-Forwarded-For without a trusted proxy.

Three environment knobs, read on every call:

* ``TRUST_PROXY_HEADERS`` — master switch. Unset: X-Forwarded-For is ignored and the socket
  peer (``remote_addr``) is the client. Set: the socket peer is a trusted proxy that appends
  the address it saw to X-Forwarded-For (Railway's edge does).
* ``TRUSTED_PROXY_HOPS`` — how many trusted proxies sit in front of this app (default 1;
  prod keeps 2: Cloudflare -> Railway edge). Without ``TRUSTED_PROXY_CIDRS`` the client is
  exactly that many entries from the right. With 2 hops a request sent straight to the origin
  has only the edge's entry to its right, so its own forged entry is picked; the module logs
  that once (set ``TRUSTED_PROXY_CIDRS=cloudflare`` to close it).
* ``TRUSTED_PROXY_CIDRS`` — optional, comma-separated CIDRs or bare IPs, and/or the token
  ``cloudflare`` (Cloudflare's published ranges, :data:`CLOUDFLARE_PROXY_CIDRS`). When set,
  X-Forwarded-For is walked from the right and the client is the first entry outside those
  ranges, but never more than ``TRUSTED_PROXY_HOPS`` entries from the right: every entry the
  walk skips must lie inside the ranges, and the walk stops at the ``TRUSTED_PROXY_HOPS``-th
  entry wherever it lies. A request sent straight to the origin (bypassing Cloudflare) cannot
  choose its own IP: the edge's entry for it lies outside the ranges and wins.

Why the hop bound: Cloudflare's ranges also carry traffic that merely egresses from
Cloudflare. A Worker subrequest into another customer's zone arrives with the Worker client IP
2a06:98c0:3600::103 (inside 2a06:98c0::/29), and Cloudflare appends it after whatever
X-Forwarded-For the Worker sent. An unbounded walk would skip it and return the Worker's forged
entry; with 2 hops the walk stops at the Worker IP, so that cross-zone path is closed. No
X-Forwarded-For rule closes a Cloudflare-egress sender (a Worker, for example) that reaches the
origin directly, because its own Cloudflare address is the one skipped hop: that needs the
``*.up.railway.app`` domain removed (N3.13 step 6) plus an origin lock (D-SEC6 follow-up,
candidate for P16).

``TRUSTED_PROXY_CIDRS`` does not turn proxy trust on by itself: without
``TRUST_PROXY_HEADERS`` the header is never read.
"""

from __future__ import annotations

import ipaddress
import logging
import os
from functools import lru_cache

from flask import Request

_UNKNOWN = "unknown"

_log = logging.getLogger(__name__)
_HOPS_LOGGED = False
_NO_CIDRS_LOGGED = False

IPNetwork = ipaddress.IPv4Network | ipaddress.IPv6Network
IPAddress = ipaddress.IPv4Address | ipaddress.IPv6Address

# Cloudflare's published proxy ranges, the expansion of TRUSTED_PROXY_CIDRS=cloudflare.
# As of 2026-10-18, fetched from https://www.cloudflare.com/ips-v4 and /ips-v6 (identical to
# https://api.cloudflare.com/client/v4/ips, etag 38f79d050aa027e3be3865e495dcc9bc).
# REFRESH: re-fetch those URLs at least quarterly and whenever Cloudflare announces a range
# change; update this tuple and CLOUDFLARE_PROXY_CIDRS_AS_OF together. A missing range makes
# visitors routed through it share that Cloudflare node's rate-limit bucket (fails closed); a
# range Cloudflare has given up would let whoever holds it next forge client IPs.
# Trust boundary: these ranges also carry traffic that merely egresses from Cloudflare, such as
# Workers. TRUSTED_PROXY_HOPS bounds the CIDR walk, which closes the cross-zone Worker path
# (2a06:98c0:3600::103 arriving through our own zone; see the module docstring). A
# Cloudflare-egress sender that reaches the origin directly can still place a forged entry left
# of its own hop: that needs the *.up.railway.app domain removed (N3.13 step 6) plus an origin
# lock (for example a secret header Cloudflare adds and the app checks), not CIDRs.
CLOUDFLARE_PROXY_CIDRS_AS_OF = "2026-10-18"
CLOUDFLARE_PROXY_CIDRS: tuple[str, ...] = (
    # IPv4
    "173.245.48.0/20",
    "103.21.244.0/22",
    "103.22.200.0/22",
    "103.31.4.0/22",
    "141.101.64.0/18",
    "108.162.192.0/18",
    "190.93.240.0/20",
    "188.114.96.0/20",
    "197.234.240.0/22",
    "198.41.128.0/17",
    "162.158.0.0/15",
    "104.16.0.0/13",
    "104.24.0.0/14",
    "172.64.0.0/13",
    "131.0.72.0/22",
    # IPv6
    "2400:cb00::/32",
    "2606:4700::/32",
    "2803:f800::/32",
    "2405:b500::/32",
    "2405:8100::/32",
    "2a06:98c0::/29",
    "2c0f:f248::/32",
)
_CLOUDFLARE_TOKEN = "cloudflare"

# A configured range shorter than this prefix is broader than any one organisation holds
# (10.0.0.0/8 and Cloudflare's /13 and /29 pass), so it is almost certainly a mistake such as
# 0.0.0.0/0: every client-written entry inside it would count as a proxy hop.
_BROAD_PREFIXLEN = {4: 8, 6: 16}


def trust_proxy_headers() -> bool:
    """When True, X-Forwarded-For is used (set only behind a proxy that appends to it)."""
    return (os.environ.get("TRUST_PROXY_HEADERS") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def trusted_proxy_hops() -> int:
    """Number of trusted proxies between the client and this app. Railway's edge = 1."""
    raw = (os.environ.get("TRUSTED_PROXY_HOPS") or "").strip()
    if not raw:
        return 1
    try:
        return max(1, int(raw))
    except ValueError:
        return 1


@lru_cache(maxsize=16)
def _parse_trusted_cidrs(raw: str) -> tuple[IPNetwork, ...]:
    """Parse a TRUSTED_PROXY_CIDRS value. Invalid tokens are dropped with one warning."""
    networks: list[IPNetwork] = []
    invalid: list[str] = []
    broad: list[str] = []
    for piece in raw.split(","):
        token = piece.strip()
        if not token:
            continue
        if token.lower() == _CLOUDFLARE_TOKEN:
            networks.extend(ipaddress.ip_network(cidr) for cidr in CLOUDFLARE_PROXY_CIDRS)
            continue
        try:
            network = ipaddress.ip_network(token, strict=False)
        except ValueError:
            invalid.append(token)
            continue
        networks.append(network)
        if network.prefixlen < _BROAD_PREFIXLEN[network.version]:
            broad.append(str(network))
    if invalid:
        _log.warning(
            "client_ip: TRUSTED_PROXY_CIDRS ignores %d invalid entr%s: %s",
            len(invalid),
            "y" if len(invalid) == 1 else "ies",
            ", ".join(invalid),
        )
    if broad:
        # Kept, not dropped (the operator asked for it), but loudly: only TRUSTED_PROXY_HOPS
        # then stops a client-written entry from being taken as a proxy hop.
        _log.warning(
            "client_ip: TRUSTED_PROXY_CIDRS trusts very broad range%s %s: client-written "
            "X-Forwarded-For entries inside %s count as proxy hops, up to TRUSTED_PROXY_HOPS; "
            "list only your proxies' ranges",
            "" if len(broad) == 1 else "s",
            ", ".join(broad),
            "it" if len(broad) == 1 else "them",
        )
    return tuple(networks)


def trusted_proxy_cidrs() -> tuple[IPNetwork, ...] | None:
    """The configured trusted proxy networks, or None when TRUSTED_PROXY_CIDRS is unset.

    A value that is set but holds no valid entry returns an empty tuple, not None: the
    operator asked for CIDR mode, so nothing is trusted beyond the socket peer rather than
    silently falling back to the spoofable hop-count rule.
    """
    raw = (os.environ.get("TRUSTED_PROXY_CIDRS") or "").strip()
    if not raw:
        return None
    return _parse_trusted_cidrs(raw)


def _normalize_ip(token: str) -> str | None:
    """Validate one X-Forwarded-For entry, stripping any port. None when it is not an IP."""
    value = token.strip()
    if not value:
        return None
    if value.startswith("["):
        # [2001:db8::1]:443 — bracketed IPv6, optionally with a port.
        closing = value.find("]")
        if closing == -1:
            return None
        value = value[1:closing]
    elif value.count(":") == 1:
        # 203.0.113.7:44321 — IPv4 with a port. A bare IPv6 always has 2+ colons.
        value = value.split(":", 1)[0]
    try:
        return str(ipaddress.ip_address(value))
    except ValueError:
        return None


def _in_networks(ip: str, networks: tuple[IPNetwork, ...]) -> bool:
    """True when ``ip`` (already normalized) lies in one of ``networks``."""
    addr: IPAddress = ipaddress.ip_address(ip)
    if isinstance(addr, ipaddress.IPv6Address) and addr.ipv4_mapped is not None:
        # ::ffff:172.67.1.1 is the IPv4 peer 172.67.1.1 seen on a dual-stack socket.
        addr = addr.ipv4_mapped
    return any(addr in net for net in networks if net.version == addr.version)


def _client_from_cidrs(parts: list[str], networks: tuple[IPNetwork, ...]) -> str | None:
    """Walk X-Forwarded-For from the right over trusted proxies; None to fall back.

    The rightmost entry was written by the socket peer (trusted by TRUST_PROXY_HEADERS).
    Every entry left of a trusted-proxy entry was written by that proxy, so it is the
    address that proxy saw. The client is the first entry outside ``networks``, but never
    more than TRUSTED_PROXY_HOPS entries from the right: an address inside ``networks`` is
    not proof that it is one of our proxies (a Cloudflare Worker's cross-zone subrequest
    arrives from 2a06:98c0:3600::103, inside Cloudflare's ranges), so at most the expected
    hops are skipped, and each of them must lie inside ``networks``.
    When the chain is shorter than that and every entry is trusted, the leftmost one is
    the best-known client. A malformed entry on the walk means a hop misbehaved: fall back.
    """
    hops = trusted_proxy_hops()
    leftmost_trusted: str | None = None
    for position, raw in enumerate(reversed(parts)):
        candidate = _normalize_ip(raw)
        if candidate is None:
            return None
        if position >= hops - 1 or not _in_networks(candidate, networks):
            return candidate
        leftmost_trusted = candidate
    return leftmost_trusted


def _client_from_hops(parts: list[str]) -> str | None:
    """The entry TRUSTED_PROXY_HOPS from the right; None when absent or malformed."""
    index = len(parts) - trusted_proxy_hops()
    if 0 <= index < len(parts):
        return _normalize_ip(parts[index])
    return None


def client_ip(request: Request) -> str:
    """The client's IP from X-Forwarded-For, read only from the right (see module docstring).

    A proxy appends the address it saw to X-Forwarded-For, so with one trusted proxy the
    client is the *last* entry. Anything a client injects itself lands to the left of that
    and can never be selected. Reading the first entry instead would let any client forge
    its own rate-limit key by sending its own X-Forwarded-For.

    With TRUSTED_PROXY_CIDRS set, trusted-proxy entries are skipped from the right and the
    first other entry wins, but the walk never goes past TRUSTED_PROXY_HOPS entries from the
    right. Otherwise the entry TRUSTED_PROXY_HOPS from the right wins.

    Falls back to remote_addr — the trusted socket peer, never spoofable — whenever the
    header is missing, malformed, or shorter than the configured hop count.
    """
    if trust_proxy_headers():
        forwarded = request.headers.get("X-Forwarded-For") or ""
        parts = [p for p in (piece.strip() for piece in forwarded.split(",")) if p]
        networks = trusted_proxy_cidrs()
        global _HOPS_LOGGED, _NO_CIDRS_LOGGED
        if not _HOPS_LOGGED and forwarded:
            # Entry count only (no addresses): lets an operator check TRUSTED_PROXY_HOPS
            # against the real proxy chain, e.g. Cloudflare -> Railway edge = 2.
            _HOPS_LOGGED = True
            _log.warning(
                "client_ip: X-Forwarded-For has %d entr%s; TRUSTED_PROXY_HOPS=%d; TRUSTED_PROXY_CIDRS %s",
                len(parts),
                "y" if len(parts) == 1 else "ies",
                trusted_proxy_hops(),
                "unset" if networks is None else f"has {len(networks)} networks",
            )
        if networks is None and not _NO_CIDRS_LOGGED and trusted_proxy_hops() >= 2:
            _NO_CIDRS_LOGGED = True
            _log.warning(
                "client_ip: TRUSTED_PROXY_HOPS=%d without TRUSTED_PROXY_CIDRS: a request sent "
                "straight to the origin picks its own client IP; set "
                "TRUSTED_PROXY_CIDRS=cloudflare",
                trusted_proxy_hops(),
            )
        if networks is None:
            candidate = _client_from_hops(parts)
        else:
            candidate = _client_from_cidrs(parts, networks)
        if candidate:
            return candidate
    return request.remote_addr or _UNKNOWN
