"""
Borrow a working recipe from another dealer when synthesis has nothing to offer.

``recipe_synth`` builds a recipe from a *template* it knows for a platform. That covers
any dealer on a platform someone has already hand-modelled, and after the TLS fix it
handled 134 of 170 silent rooftops. The remainder fail for reasons templates cannot
address: the platform is recognised but has no template yet, or is not recognised at all,
or its parameters are not visible in the HTML.

This module takes the other route. Rather than modelling the platform, it treats every
recipe that currently works for *some* dealer as a candidate shape, rewrites it for the
target dealer, and replays it. If one returns real VINs, that is a working recipe --
regardless of whether we can name the platform it belongs to.

    for shape in known working shapes, best first:
        adapt shape to this dealer
        replay it and count unique VINs
        first shape over the threshold wins and is saved

Why this works at all: on most dealer platforms the inventory endpoint lives on the
dealer's own hostname with a path that is identical across every tenant. The only
dealer-specific part is the host, sometimes echoed in a Referer/Origin header or in the
POST body. Substituting the donor's host for the target's therefore produces a valid
request far more often than it has any right to.

Where it does NOT work, and deliberately so: platforms keyed by an account id or API key
that is not derivable from the hostname (carscommerce ccid, a Typesense key). Those
tokens belong to the donor. Such an adaptation either 401s or -- the case actually worth
guarding -- returns the DONOR's inventory. Serving another dealer's cars is far worse
than finding none, so every candidate is checked against the target's own domain before
it is accepted; see :func:`_looks_like_donor_inventory`.

Shapes are deduplicated so a cascade tries ~40 distinct request shapes rather than the
several hundred recipes those shapes came from.
"""

from __future__ import annotations

import logging
import re
from dataclasses import replace
from typing import Any, Iterable
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# A shape must have produced this many rows for its donor to be worth borrowing.
_MIN_DONOR_ROWS = 15

# Accept an adapted recipe once it yields this many unique VINs.
DEFAULT_MIN_VINS = 5

# Cap the work per dealer: shapes are tried best-first and the tail is mostly
# near-duplicates, so an exhaustive sweep costs time without adding coverage.
_MAX_SHAPES_PER_DEALER = 40


def _host(url: str) -> str:
    return (urlparse(url).hostname or "").lower()


def _bare(host: str) -> str:
    """Hostname without www. -- the token that tends to appear inside POST bodies."""
    return host[4:] if host.startswith("www.") else host


def _label(host: str) -> str:
    """The registrable label, e.g. 'courtesychev' from 'www.courtesychev.com'."""
    parts = _bare(host).split(".")
    return parts[0] if parts else ""


# A path segment that identifies the tenant rather than the resource: a numeric account
# id, or a long hex/uuid. Seen as /api/v1/listings/196006/search -- where 196006 is the
# DONOR's account. Swapping the hostname leaves that id pointing at the donor, so the
# adapted request either 404s or, much worse, returns the donor's cars under the target's
# name. Those shapes are not rehostable and are refused rather than tried.
_TENANT_SEGMENT_RE = re.compile(r"^(?:\d{3,}|[0-9a-f]{16,}|[0-9a-f-]{32,})$", re.I)


def path_has_tenant_id(path: str) -> bool:
    return any(_TENANT_SEGMENT_RE.match(seg) for seg in (path or "").split("/") if seg)


def shape_key(recipe: Any) -> tuple[str, str, str]:
    """
    Identity of a request *shape*, independent of which dealer produced it.

    The host is dropped (that is the part being replaced) and numbers are collapsed in
    both the path and the POST body, so paging offsets and per-tenant ids do not split
    one shape into hundreds of near-identical entries -- 638 raw "shapes" turned out to
    be far fewer once account ids stopped counting as distinguishing.
    """
    p = urlparse(recipe.url)
    path = re.sub(r"\d+", "#", p.path)
    body = re.sub(r"\d+", "#", recipe.post_template or "")[:400]
    return (recipe.method.upper(), path, body)


def collect_shapes(dealer_ids: Iterable[str], *, min_rows: int = _MIN_DONOR_ROWS) -> list[Any]:
    """
    One exemplar recipe per distinct working shape, richest donor first.

    Preferring the donor with the most rows is not cosmetic: a shape proven against a
    2,000-car rooftop is likelier to be the real inventory endpoint than the same shape
    seen once on a lot with nine cars.
    """
    from backend.scanner.recipes import load_recipes

    best: dict[tuple[str, str, str], Any] = {}
    for dealer_id in dealer_ids:
        try:
            recipes = load_recipes(dealer_id)
        except Exception as exc:  # a single unreadable dealer must not stop the sweep
            logger.debug("cascade: cannot load recipes for %s: %s", dealer_id, exc)
            continue
        for r in recipes:
            if r.stale or (r.vehicle_rows or 0) < min_rows:
                continue
            key = shape_key(r)
            incumbent = best.get(key)
            if incumbent is None or (r.vehicle_rows or 0) > (incumbent.vehicle_rows or 0):
                best[key] = r
    ordered = sorted(best.values(), key=lambda r: -(r.vehicle_rows or 0))
    logger.info("cascade: %d distinct recipe shapes available", len(ordered))
    return ordered


def adapt_recipe(donor: Any, target_dealer_id: str, target_url: str) -> Any | None:
    """
    Rewrite *donor* to address the target dealer, or None if it cannot be rehosted.

    Substitution covers the URL, the POST body and any header that echoes the origin
    (Referer/Origin/Host), in three forms: full hostname, hostname without ``www.`` and
    the bare label. Those are the shapes a dealer identifier takes in practice.
    """
    donor_host = _host(donor.url)
    target_host = _host(target_url)
    if not donor_host or not target_host or donor_host == target_host:
        return None

    # The donor's tenant id is embedded in the path and cannot be derived for the target;
    # rehosting would leave the request selecting the donor's inventory. Recipes of this
    # kind can only come from synthesis, which reads the id out of the target's own HTML.
    if path_has_tenant_id(urlparse(donor.url).path):
        return None

    pairs = [
        (donor_host, target_host),
        (_bare(donor_host), _bare(target_host)),
        (_label(donor_host), _label(target_host)),
    ]

    def swap(text: str | None) -> str | None:
        if not text:
            return text
        out = text
        for old, new in pairs:
            if old and new:
                out = out.replace(old, new)
        return out

    new_url = swap(donor.url)
    if not new_url or _host(new_url) != target_host:
        # The endpoint is not on the dealer's own hostname (a shared multi-tenant API),
        # so rehosting is meaningless -- the donor's account id would still select the
        # donor's inventory.
        return None

    headers = {k: (swap(v) or v) for k, v in (donor.auth_headers or {}).items()}

    return replace(
        donor,
        dealer_id=target_dealer_id,
        url=new_url,
        post_template=swap(donor.post_template),
        auth_headers=headers,
        vehicle_rows=0,
        total_count=None,
        saved_at=0.0,
        last_ok_at=0.0,
        stale=False,
        stale_reason="",
        provider_hint=(donor.provider_hint or "") + "+cascade",
    )


def _looks_like_donor_inventory(recipe: Any, target_url: str) -> bool:
    """
    True when the adapted request would still be answered by the donor's tenant.

    The failure this prevents is the expensive one: a shared endpoint keyed by an account
    id returns a full, healthy-looking page of cars that belong to a different
    dealership. Those rows would be attributed to the target and be indistinguishable
    from real inventory afterwards.
    """
    target_host = _host(target_url)
    if _host(recipe.url) != target_host:
        return True
    blob = f"{recipe.post_template or ''} {' '.join((recipe.auth_headers or {}).values())}".lower()
    label = _label(target_host)
    # Any other dealer-looking hostname left in the body means a donor token survived.
    for other in re.findall(r"\b([a-z0-9][a-z0-9-]{4,})\.(?:com|net|org)\b", blob):
        if label and other != _bare(target_host).split(".")[0] and other != label:
            return True
    return False


def cascade_for_dealer(
    dealer_id: str,
    dealer_url: str,
    dealer_name: str,
    shapes: list[Any],
    *,
    min_vins: int = DEFAULT_MIN_VINS,
    max_shapes: int = _MAX_SHAPES_PER_DEALER,
) -> tuple[Any | None, int, int]:
    """
    Try borrowed shapes against one dealer.

    Returns ``(winning_recipe_or_None, vin_count, shapes_tried)``. The recipe is NOT
    saved here; the caller decides, so a dry run can report exactly what a real run
    would write.
    """
    from backend.scanner.recipe_synth import validate_recipe

    tried = 0
    for donor in shapes:
        if tried >= max_shapes:
            break
        adapted = adapt_recipe(donor, dealer_id, dealer_url)
        if adapted is None:
            continue
        if _looks_like_donor_inventory(adapted, dealer_url):
            logger.debug("cascade %s: shape %s still addresses the donor, skipping",
                         dealer_id, donor.url[:70])
            continue
        tried += 1
        try:
            vins = validate_recipe(adapted, dealer_url, dealer_id, dealer_name)
        except Exception as exc:
            logger.debug("cascade %s: %s failed: %s", dealer_id, adapted.url[:70], str(exc)[:90])
            continue
        if vins >= min_vins:
            adapted.vehicle_rows = vins
            logger.info(
                "cascade %s: borrowed shape from %s -> %d VINs (%d shape(s) tried)",
                dealer_id, donor.dealer_id, vins, tried,
            )
            return adapted, vins, tried
    return None, 0, tried
