"""Where is this store? City / state evidence for the rooftop attribution gate.

Group feeds (Hendrick's carscommerce index serves 11,000 cars from 15 rooftops)
name their rooftops only as "City, ST". Without the store's own city the gate
refuses every row rather than mis-attribute, and the dealer reads as
``validated_zero`` (hendrickbuickgmccary-com, 2026-09-24). Sources, in order:

  1. the dealership registry (``roster_place``: street, city, state, zip)
  2. the site's own <title>: "Hendrick Buick GMC Cary | Buick, GMC Dealer in Cary, NC"
  3. JSON-LD PostalAddress (addressLocality / addressRegion / postalCode)

A learned place is saved to the dealer's recipe scan hints so the scanner (which
looks the store up by URL) finds it on every later run without the page.
"""
from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger("scanner")

_TITLE_IN_RE = re.compile(r"\bin\s+([A-Z][A-Za-z.'\- ]{1,40}?),\s*([A-Z]{2})\b")
_LD_RE = re.compile(
    r'"addressLocality"\s*:\s*"([^"]{2,40})"[^}]{0,200}?"addressRegion"\s*:\s*"([A-Za-z]{2})"(?:[^}]{0,120}?"postalCode"\s*:\s*"(\d{5})")?'
)
_LD_STREET_RE = re.compile(r'"streetAddress"\s*:\s*"([^"]{4,80})"')
_US_STATES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "DC",
}


def place_from_html(html: str | None) -> dict[str, str]:
    """{dealer_city, dealer_state[, dealer_zip], place_source} from the page, or {}."""
    if not html:
        return {}
    m = re.search(r"<title[^>]*>(.*?)</title>", html, re.S | re.I)
    title = re.sub(r"\s+", " ", m.group(1)) if m else ""
    # The street (JSON-LD streetAddress) is what tells two same-town rooftops of
    # one group apart: Stevenson Hendrick Mazda's OEM-code feed stamps its cars
    # only "Wilmington, NC" while its DMS feed stamps "5911 Market St" — the
    # page says "5911 Market Street" (2026-09-24).
    st = _LD_STREET_RE.search(html)
    street = {"dealer_address": st.group(1).strip()} if st and re.match(r"\d", st.group(1).strip()) else {}
    hit = _TITLE_IN_RE.search(title)
    if hit and hit.group(2).upper() in _US_STATES:
        return {"dealer_city": hit.group(1).strip(), "dealer_state": hit.group(2).upper(), "place_source": "title", **street}
    ld = _LD_RE.search(html)
    if ld and ld.group(2).upper() in _US_STATES:
        out = {"dealer_city": ld.group(1).strip(), "dealer_state": ld.group(2).upper(), "place_source": "jsonld", **street}
        if ld.group(3):
            out["dealer_zip"] = ld.group(3)
        return out
    return {}


def place_from_hints(dealer_id: str) -> dict[str, str]:
    try:
        from backend.scanner.recipe_store import get_scan_hints

        h = get_scan_hints(dealer_id) or {}
    except Exception:  # noqa: BLE001
        return {}
    out = {k: str(h[k]) for k in ("dealer_city", "dealer_state", "dealer_zip", "dealer_address") if h.get(k)}
    if out:
        out["place_source"] = str(h.get("place_source") or "scan_hints")
    return out


def learn_place(dealer_id: str, dealer_url: str, html: str | None = None, *, save: bool = True) -> dict[str, Any]:
    """Registry first, then the page; persist a page-derived place to scan hints."""
    try:
        from backend.scanner.rooftop_disown import roster_place

        reg = roster_place(dealer_url)
    except Exception:  # noqa: BLE001
        reg = {}
    if reg.get("dealer_city") and reg.get("dealer_state"):
        reg = dict(reg)
        reg["place_source"] = "registry"
        if not reg.get("dealer_address") and html:
            st = _LD_STREET_RE.search(html)
            if st and re.match(r"\d", st.group(1).strip()):
                reg["dealer_address"] = st.group(1).strip()
                if save:
                    # The replay gate reads the roster (no street for 150/178
                    # dealers) and never sees this page: Honda of Huntersville's
                    # 105 street-block cars stayed refused with own street ""
                    # (2026-09-26). Keep the page street in scan hints so
                    # try_fetch_via_recipes can merge it.
                    try:
                        from backend.scanner.recipe_store import set_scan_hints

                        set_scan_hints(dealer_id, {"dealer_address": reg["dealer_address"], "dealer_address_source": "site_jsonld"}, merge=True)
                    except Exception as exc:  # noqa: BLE001
                        logger.debug("street hint save failed [%s]: %s", dealer_id, exc)
        return reg
    hinted = place_from_hints(dealer_id)
    if hinted.get("dealer_city"):
        if not hinted.get("dealer_address") and html:
            st = _LD_STREET_RE.search(html)
            if st and re.match(r"\d", st.group(1).strip()):
                hinted["dealer_address"] = st.group(1).strip()
        return hinted
    learned = place_from_html(html)
    if learned and save:
        try:
            from backend.scanner.recipe_store import set_scan_hints

            set_scan_hints(dealer_id, {k: v for k, v in learned.items()}, merge=True)
        except Exception as exc:  # noqa: BLE001
            logger.debug("place hint save failed [%s]: %s", dealer_id, exc)
    return learned


_DI_NAME_RE = re.compile(r'"dealername"\s*:\s*"([^"]{3,80})"')
_OG_NAME_RE = re.compile(r'<meta[^>]+property=["\']og:site_name["\'][^>]+content=["\']([^"\']{3,80})["\']', re.I)
_DI_OEM_CODE_RE = re.compile(r'"oem_code"\s*:\s*"([A-Za-z0-9_-]{2,20})"')


def name_from_html(html: str | None) -> str:
    """The store's own name as its page states it (Dealer Inspire ``dealername``,
    else og:site_name), or ""."""
    for rx in (_DI_NAME_RE, _OG_NAME_RE):
        m = rx.search(html or "")
        if m:
            return m.group(1).strip()
    return ""


def oem_code_from_html(html: str | None) -> str:
    """Dealer Inspire pages carry the OEM dealer code (GM BAC etc.) that the
    CarsCommerce feed uses as the store's ``source_id``: "178465" on Hendrick
    Buick GMC Cary, "292164" on Rick Hendrick Chevrolet Naples (2026-09-24)."""
    m = _DI_OEM_CODE_RE.search(html or "")
    return m.group(1) if m else ""


def place_kwargs(place: dict[str, Any] | None) -> dict[str, str]:
    """Only the keys the attribution gate accepts. The street (registry, or the
    page's JSON-LD streetAddress) is what matches an address-block rooftop:
    Tutton CDJR's own cars are stamped "1050 Highway 515 South" and nothing
    else (2026-09-26)."""
    return {k: str(v) for k, v in (place or {}).items() if k in ("dealer_address", "dealer_city", "dealer_state", "dealer_zip") and v}


def place_label(place: dict[str, Any] | None) -> str:
    """"Buford, GA" — the spelling group feeds use for their store tags."""
    if not place or not place.get("dealer_city") or not place.get("dealer_state"):
        return ""
    return f"{str(place['dealer_city']).strip()}, {str(place['dealer_state']).strip().upper()}"
