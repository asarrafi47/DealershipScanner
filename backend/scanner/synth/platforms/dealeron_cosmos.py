"""
DealerOn cosmos (ws/vhcliaa SRP) platform template.
"""
from __future__ import annotations

import re

from backend.scanner.recipes import EndpointRecipe, PAGINATION_COSMOS_PT
from backend.scanner.synth.common import _origin
from backend.scanner.synth.http import fetch_dealer_html


# ── Platform: DealerOn cosmos (ws/vhcliaa SRP) ────────────────────────────────

# DealerOn cosmos SRP endpoint:
#   https://{domain}/api/vhcliaa/vehicle-pages/cosmos/srp/vehicles/{account}/{pagecfg}
# parameterized by two ids, both recoverable over plain HTTP:
#   account  — the dealer id, in the homepage HTML (site-provider="dealeron",
#              data-website-id="do-{account}", "dealerId":"{account}").
#   pagecfg  — the SRP page config id, NOT on the homepage but embedded in the
#              used-inventory SRP page as its own page config
#              ({"dealerId":...,"pageId":{pagecfg},"pageType":"itemlist"|"custom"...}).
#              Most DealerOn dealers tag this "itemlist"; some (Toyota Direct,
#              Weatherford BMW of Berkeley — both fingerprinted via the shared
#              banrsaa.dealeron.com script host) tag the same used-SRP page
#              config "custom" instead. Either pageType's pageId replays fine
#              as the cosmos pagecfg (verified live for both), so no browser
#              capture is needed — just accept both pageType spellings.
# The endpoint paginates session-free via ?pt=N&pn=96 (pt = page number, pn =
# page size, 96 is the server max). NOT ?pg=: that parameter is ignored and
# returns page 1 again, which is why the August heal walked page 1 of every
# cosmos store 24 times (verified live 2026-09-23 on Cherokee County Toyota).
_COSMOS_PATH = "/api/vhcliaa/vehicle-pages/cosmos/srp/vehicles"
_COSMOS_PAGE_SIZE = 96
# SRP pages that carry a Used-scoped itemlist page config, and the New-scoped ones.
# Each SRP has its own pageId (Cherokee County Toyota: used 769890 = 106 cars,
# new 769883 = 282 cars); one recipe per section covers the lot.
_COSMOS_SRP_PATHS = ("/used-inventory/", "/searchused.aspx", "/used-vehicles/", "/inventory/used")
_COSMOS_SRP_PATHS_NEW = ("/new-inventory/", "/searchnew.aspx", "/new-vehicles/", "/inventory/new")

_COSMOS_ACCOUNT_RES = (
    re.compile(r'data-website-id="do-(\d+)"'),
    re.compile(r'"dealerId"\s*:\s*"?(\d+)"?'),
    re.compile(r'/static/dealer-(\d+)/'),
)
# {"dealerId":"25003","pageId":2483381,"pageType":"itemlist"...} — or, on some
# dealers (Toyota Direct, Weatherford BMW), the same shape tagged "custom".
_COSMOS_ITEMLIST_RE = re.compile(
    r'"dealerId"\s*:\s*"?(\d+)"?\s*,\s*"pageId"\s*:\s*(\d+)\s*,\s*"pageType"\s*:\s*"(?:itemlist|custom)"'
)


def _detect_dealer_on_cosmos(html: str, dealer_url: str) -> bool:
    low = html.lower()
    markers = ('site-provider="dealeron"', 'data-website-id="do-', "vhcliaa", "cosmos/srp/vehicles")
    if not any(m in low for m in markers):
        return False
    return bool(_extract_cosmos_account(html))


def _extract_cosmos_account(html: str) -> str | None:
    for rx in _COSMOS_ACCOUNT_RES:
        m = rx.search(html)
        if m:
            return m.group(1)
    return None


def _extract_cosmos_pagecfg(html: str) -> tuple[str, str] | None:
    """(account, pagecfg) from a Used SRP itemlist page config, or ``None``."""
    m = _COSMOS_ITEMLIST_RE.search(html)
    if m:
        return m.group(1), m.group(2)
    return None


def _cosmos_pagecfg_from_paths(origin: str, paths: tuple[str, ...]) -> tuple[str, str] | None:
    for path in paths:
        srp = fetch_dealer_html(origin + path)
        if not srp:
            continue
        pair = _extract_cosmos_pagecfg(srp)
        if pair:
            return pair
    return None


def _synth_dealer_on_cosmos(dealer_id: str, dealer_url: str, html: str) -> list[EndpointRecipe]:
    """One recipe per SRP section (used, new): each has its own pageId."""
    account = _extract_cosmos_account(html)
    origin = _origin(dealer_url)
    pairs: list[tuple[str, str]] = []
    home_pair = _extract_cosmos_pagecfg(html)
    if home_pair:
        pairs.append(home_pair)
    for paths in (_COSMOS_SRP_PATHS, _COSMOS_SRP_PATHS_NEW):
        pair = _cosmos_pagecfg_from_paths(origin, paths)
        if pair and pair not in pairs:
            pairs.append(pair)
    out: list[EndpointRecipe] = []
    seen: set[str] = set()
    for srp_account, pagecfg in pairs:
        acct = account or srp_account
        if not acct or not pagecfg or pagecfg == "0" or pagecfg in seen:
            continue
        seen.add(pagecfg)
        out.append(EndpointRecipe(
            dealer_id=dealer_id,
            url=f"{origin}{_COSMOS_PATH}/{acct}/{pagecfg}",
            method="GET",
            content_type="application/json",
            post_template=None,
            auth_headers={},
            pagination=PAGINATION_COSMOS_PT,
            provider_hint="dealer_on_cosmos",
        ))
    return out
