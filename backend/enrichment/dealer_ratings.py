"""Import each dealership's Google star rating + review count into ``dealer_ratings``.

Why this exists next to ``backend/discovery/google_place_rating.py``: that module
is a fire-and-forget helper for the discovery pipeline — every failure collapses
into ``None`` and a log line, so a run against a project whose Places API is
switched off looks exactly like a run against dealers Google has never heard of.
This project has actually been in that state (403 ``SERVICE_DISABLED`` on GCP
project 326691549936), so the importer classifies the failure instead: a
key/project-level problem is *fatal* and stops the run on the first response
rather than spending the next two hundred requests rediscovering it.

Its HTTP shape (endpoints, field masks, response parsing) is reused from
``google_place_rating`` so there is still one definition of what a Places
response looks like.

Pacing and idempotence are not optional here: this project gets soft-blocked by
bursts, and the work queue is "dealers with no rating yet", so an interrupted
run resumes instead of restarting.
"""

from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

import requests

from backend.db import comments_db
from backend.discovery.google_place_rating import (
    SEARCH_TEXT_URL,
    GooglePlaceRating,
    google_maps_api_key,
    rating_from_place_dict,
)

logger = logging.getLogger(__name__)

SOURCE = "google_places"
# A rating we stored without confirming it against the dealer's own domain.
# Stored under a different source string so a later pass can find and re-verify.
SOURCE_UNVERIFIED = "google_places_unverified"
# A NULL-rating row recording "Google had hits but none of them is this rooftop"
# (or "we refused to guess"), as opposed to plain SOURCE, which records the
# honest "we asked Google and it had nothing".
SOURCE_UNMATCHED = "google_places_unmatched"

# ``websiteUri`` and ``location`` are the whole point of this module's own field
# masks: text search on a dealer name alone has already put this project's
# dealers in the wrong city (up to 2,497mi off in the geocode verification
# work), and a rating attached to the wrong rooftop is worse than no rating.
# ``backend/discovery/google_place_rating``'s masks deliberately are not reused
# — its detail mask is ``id,rating,userRatingCount``, which cannot verify
# anything.
DETAIL_FIELD_MASK = "id,rating,userRatingCount,displayName,websiteUri,formattedAddress,location"
SEARCH_FIELD_MASK = (
    "places.id,places.rating,places.userRatingCount,places.displayName,"
    "places.websiteUri,places.formattedAddress,places.location"
)
# Enough candidates that the right rooftop is usually in the list, few enough
# that a mistyped name does not turn into a scrape.
SEARCH_CANDIDATES = 5

# How far a Places hit may sit from the registry's own coordinates and still be
# believed to be the same rooftop. Sized off real data: the correct EchoPark
# Houston (North Freeway) place is 10.4mi from its registry row, while the
# EchoPark Charlotte row that shares echopark.com is 922.9mi away — so this
# separates same-domain rooftops with a wide margin either side.
MATCH_RADIUS_MILES = 25.0

# Google Places (New) bills per call and this project has been rate-limited by
# bursts before; sequential with a gap is the only mode.
DEFAULT_DELAY_SECONDS = 0.35
DEFAULT_LIMIT = 25

# Error codes that mean "the key or the project is the problem" — every
# subsequent request would fail identically, so the run stops.
FATAL_ERRORS = frozenset(
    {"no_api_key", "service_disabled", "permission_denied", "invalid_key", "quota_exhausted"}
)

# Misses worth remembering, and the ``source`` each is filed under. "Google had
# nothing" and "Google had hits but none of them is this rooftop" look identical
# in a NULL rating row unless the source says which happened.
_MISS_SOURCES = {
    "no_result": SOURCE,
    "no_query": SOURCE,
    "host_mismatch": SOURCE_UNMATCHED,
    "geo_mismatch": SOURCE_UNMATCHED,
    "ambiguous_domain": SOURCE_UNMATCHED,
}


@dataclass(frozen=True)
class FetchResult:
    """One Places call: a rating, or a classified reason there is none."""

    rating: GooglePlaceRating | None = None
    error: str | None = None
    detail: str | None = None
    # False when the match could not be confirmed against the dealer's domain.
    verified: bool = True

    @property
    def ok(self) -> bool:
        return self.rating is not None

    @property
    def fatal(self) -> bool:
        return self.error in FATAL_ERRORS

    @property
    def source(self) -> str:
        return SOURCE if self.verified else SOURCE_UNVERIFIED


@dataclass
class SyncStats:
    examined: int = 0
    resolved: int = 0
    unverified: int = 0
    no_result: int = 0
    failed: int = 0
    written: int = 0
    dry_run: bool = True
    stopped_early: str | None = None
    errors: dict[str, int] = field(default_factory=dict)
    samples: list[dict[str, Any]] = field(default_factory=list)

    def note_error(self, code: str) -> None:
        self.errors[code] = self.errors.get(code, 0) + 1

    def as_dict(self) -> dict[str, Any]:
        return {
            "examined": self.examined,
            "resolved": self.resolved,
            # Subset of ``resolved``: stored, but never confirmed against the
            # dealer's own domain. Omitting this is how a name-only guess gets
            # reported as a clean "resolved: 5".
            "unverified": self.unverified,
            "verified": max(0, self.resolved - self.unverified),
            "no_result": self.no_result,
            "failed": self.failed,
            "written": self.written,
            "dry_run": self.dry_run,
            "stopped_early": self.stopped_early,
            "errors": dict(self.errors),
            "samples": list(self.samples),
        }


def classify_places_error(status_code: int, payload: Any) -> tuple[str, str]:
    """Map a Places API error response to ``(code, human detail)``.

    Google reports a disabled API as HTTP 403 with ``status: PERMISSION_DENIED``
    and a ``reason: SERVICE_DISABLED`` buried in ``error.details`` — the two
    need different operator action ("enable the API in the console" vs "this key
    is not allowed to call it"), so they get different codes.
    """
    body = payload if isinstance(payload, dict) else {}
    err = body.get("error") if isinstance(body.get("error"), dict) else {}
    status = str(err.get("status") or "").upper()
    message = str(err.get("message") or "").strip()
    reasons = {
        str(d.get("reason") or "").upper()
        for d in (err.get("details") or [])
        if isinstance(d, dict)
    }
    blob = f"{status} {message} {' '.join(sorted(reasons))}".upper()

    if "SERVICE_DISABLED" in blob or "HAS NOT BEEN USED IN PROJECT" in blob:
        return "service_disabled", message or "Places API (New) is disabled for this GCP project."
    if "API_KEY_INVALID" in blob or "API KEY NOT VALID" in blob:
        return "invalid_key", message or "GOOGLE_MAPS_API_KEY is not valid."
    if status == "RESOURCE_EXHAUSTED" or status_code == 429:
        return "quota_exhausted", message or "Places API quota exhausted or rate limited."
    if status == "PERMISSION_DENIED" or status_code == 403:
        return "permission_denied", message or "Places API rejected this key."
    if status_code == 404:
        return "not_found", message or "Place not found."
    return "http_error", message or f"HTTP {status_code} from Places API."


def _call_places(
    session: requests.Session,
    *,
    method: str,
    url: str,
    headers: dict[str, str],
    json_body: dict[str, Any] | None,
    timeout_s: float,
) -> tuple[dict[str, Any] | None, FetchResult | None]:
    """Return ``(payload, None)`` on success or ``(None, FetchResult(error=...))``."""
    try:
        if method == "POST":
            resp = session.post(url, headers=headers, json=json_body, timeout=timeout_s)
        else:
            resp = session.get(url, headers=headers, timeout=timeout_s)
    except requests.RequestException as exc:
        return None, FetchResult(error="network_error", detail=str(exc))

    try:
        payload = resp.json()
    except ValueError:
        payload = None

    if resp.status_code >= 400:
        code, detail = classify_places_error(resp.status_code, payload)
        return None, FetchResult(error=code, detail=detail)
    if not isinstance(payload, dict):
        return None, FetchResult(error="bad_response", detail="Places API returned non-JSON.")
    return payload, None


def fetch_rating_by_place_id(
    place_id: str,
    *,
    api_key: str,
    session: requests.Session,
    dealer_hosts: list[str] | None = None,
    latitude: float | None = None,
    longitude: float | None = None,
    require_geo: bool = False,
    sibling_coords: list[tuple[float, float]] | None = None,
    timeout_s: float = 15.0,
) -> FetchResult:
    """Place Details for a known place id — the cheap refresh path.

    The stored place id is *not* trusted: many were resolved by an earlier
    name-only lookup, which is exactly the process that filed dealers in the
    wrong city. The answer goes through the same guard as a fresh search, which
    is why this asks for ``websiteUri``/``location`` rather than reusing
    discovery's rating-only field mask.
    """
    pid = (place_id or "").strip()
    if pid.startswith("places/"):
        pid = pid[len("places/"):]
    if not pid:
        return FetchResult(error="no_place_id", detail="Dealer has no google_place_id.")
    payload, failure = _call_places(
        session,
        method="GET",
        url=f"https://places.googleapis.com/v1/places/{pid}",
        headers={"X-Goog-Api-Key": api_key, "X-Goog-FieldMask": DETAIL_FIELD_MASK},
        json_body=None,
        timeout_s=timeout_s,
    )
    if failure is not None:
        return failure

    place, verified, reason = _pick_verified_place(
        [payload or {}],
        [h for h in (dealer_hosts or []) if h],
        latitude=latitude,
        longitude=longitude,
        require_geo=require_geo,
        sibling_coords=sibling_coords,
    )
    if place is None:
        return FetchResult(
            error=reason or "no_result",
            detail=(
                f"stored place id {pid} did not verify against "
                f"{', '.join(dealer_hosts or []) or 'no dealer website'} ({reason})"
            ),
        )
    parsed = rating_from_place_dict(place)
    if parsed is None:
        return FetchResult(error="no_result", detail="Place details had no id.")
    return FetchResult(rating=parsed, verified=verified)


def normalize_host(url: str | None) -> str:
    """Comparable hostname from a URL or bare host (``www.`` and case dropped)."""
    raw = (url or "").strip().lower()
    if not raw:
        return ""
    if "//" not in raw:
        raw = "https://" + raw
    host = (urlparse(raw).hostname or "").strip(".")
    return host[4:] if host.startswith("www.") else host


def hosts_match(a: str | None, b: str | None) -> bool:
    """True when two URLs point at the same site, allowing a subdomain either way.

    Dealer groups routinely list a rooftop under ``used.longotoyota.com`` while
    the registry holds ``longotoyota.com``; treating those as different would
    reject a correct match, and treating unrelated hosts as equal is exactly the
    failure this guard exists to prevent.

    This answers "same *site*", never "same *rooftop*" — 'EchoPark Charlotte'
    and 'EchoPark Automotive Houston (North Freeway)' both carry echopark.com,
    as do the two CarMax rows. Telling those apart is
    :func:`_pick_verified_place`'s geography check, not this function's job.
    """
    ha, hb = normalize_host(a), normalize_host(b)
    if not ha or not hb:
        return False
    return ha == hb or ha.endswith("." + hb) or hb.endswith("." + ha)


def haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in statute miles."""
    r_miles = 3958.7613
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = p2 - p1
    dlambda = math.radians(lon2 - lon1)
    h = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlambda / 2) ** 2
    return 2 * r_miles * math.asin(math.sqrt(min(1.0, h)))


def _place_coords(place: dict[str, Any]) -> tuple[float, float] | None:
    """``(lat, lon)`` of a Places hit, or ``None`` if it carries no location."""
    loc = place.get("location")
    if not isinstance(loc, dict):
        return None
    try:
        return float(loc["latitude"]), float(loc["longitude"])
    except (KeyError, TypeError, ValueError):
        return None


def _place_distance_miles(
    place: dict[str, Any], latitude: float | None, longitude: float | None
) -> float | None:
    """Miles between a Places hit and the registry's coordinates, or ``None``."""
    if latitude is None or longitude is None:
        return None
    coords = _place_coords(place)
    if coords is None:
        return None
    return haversine_miles(float(latitude), float(longitude), coords[0], coords[1])


def _pick_verified_place(
    places: list[dict[str, Any]],
    dealer_hosts: list[str],
    *,
    latitude: float | None = None,
    longitude: float | None = None,
    require_geo: bool = False,
    sibling_coords: list[tuple[float, float]] | None = None,
) -> tuple[dict[str, Any] | None, bool, str]:
    """``(place, verified, reject_reason)`` — the candidate that is this rooftop.

    Three tests, because each one alone has a known failure mode:

    * **Host.** The candidate's ``websiteUri`` must be on a domain the registry
      row claims. Without this a search for a name as generic as "Toyota"
      resolves to whatever rooftop Google ranks first.
    * **Nearest sibling.** When other registry rooftops share this domain,
      the candidate must be closer to *this* row than to any of them. A radius
      cannot do this job: the two carmax.com rows are only ~9mi apart, closer
      than the 10.4mi by which a correct match (EchoPark Houston) missed its own
      registry coordinates.
    * **Radius.** Whatever survives must still be within
      :data:`MATCH_RADIUS_MILES`, and the nearest candidate wins. This is the
      backstop against the 2,497mi kind of error.

    ``require_geo`` is set by the caller for group domains — a shared host is
    not evidence of anything on its own, so a candidate that cannot be placed
    geographically is refused rather than guessed at.

    With no dealer website on file there is nothing to host-verify against, so a
    geographically plausible top hit comes back ``verified=False`` and is stored
    under a distinct ``source`` rather than passed off as confirmed.
    """
    if not places:
        return None, False, "no_result"

    candidates = places
    host_confirmed = False
    if dealer_hosts:
        candidates = [
            p for p in places if any(hosts_match(p.get("websiteUri"), h) for h in dealer_hosts)
        ]
        if not candidates:
            return None, False, "host_mismatch"
        host_confirmed = True

    measured = [(p, _place_distance_miles(p, latitude, longitude)) for p in candidates]

    if sibling_coords:
        kept = []
        for place, dist in measured:
            coords = _place_coords(place)
            if dist is None or coords is None:
                # Cannot attribute it to anyone; on a shared domain that is a
                # refusal, handled by ``require_geo`` below.
                continue
            nearest_sibling = min(
                haversine_miles(slat, slon, coords[0], coords[1]) for slat, slon in sibling_coords
            )
            if nearest_sibling < dist:
                logger.info(
                    "Places hit on %s is %.1fmi from this rooftop but %.1fmi from another "
                    "registry rooftop on the same domain — not ours",
                    ", ".join(dealer_hosts) or "a shared domain", dist, nearest_sibling,
                )
                continue
            kept.append((place, dist))
        if not kept:
            return None, False, "ambiguous_domain"
        measured = kept

    in_radius = sorted(
        ((p, d) for p, d in measured if d is not None and d <= MATCH_RADIUS_MILES),
        key=lambda pair: pair[1],
    )
    if in_radius:
        return in_radius[0][0], host_confirmed, ""
    if any(d is not None for _p, d in measured):
        # Candidates could be placed, and every one of them is somewhere else.
        nearest = min(d for _p, d in measured if d is not None)
        logger.info(
            "rejecting Places hit(s) on %s: nearest is %.0fmi away (limit %.0fmi)",
            ", ".join(dealer_hosts) or "an unknown domain", nearest, MATCH_RADIUS_MILES,
        )
        return None, False, "geo_mismatch"
    # Nothing to cross-check against (registry row or Places hit has no
    # coordinates). Safe for a unique domain; not for a shared one.
    if require_geo:
        return None, False, "ambiguous_domain"
    return candidates[0], host_confirmed, ""


def fetch_rating_by_search(
    name: str,
    city: str,
    state: str,
    *,
    api_key: str,
    session: requests.Session,
    dealer_hosts: list[str] | None = None,
    latitude: float | None = None,
    longitude: float | None = None,
    require_geo: bool = False,
    sibling_coords: list[tuple[float, float]] | None = None,
    timeout_s: float = 15.0,
) -> FetchResult:
    """Text search — the first-time resolution path for a dealer with no place id.

    Candidates are filtered by website host *and* distance (see
    :func:`_pick_verified_place`); a dealer whose name is as generic as "Toyota"
    otherwise resolves to whatever rooftop Google ranks first.
    """
    name = (name or "").strip()
    if not name:
        return FetchResult(error="no_query", detail="Dealership row has no name.")
    text_query = " ".join(
        p for p in (name, (city or "").strip(), (state or "").strip().upper(), "car dealer") if p
    )
    body: dict[str, Any] = {
        "textQuery": text_query,
        "includedType": "car_dealer",
        "maxResultCount": SEARCH_CANDIDATES,
    }
    if latitude is not None and longitude is not None:
        body["locationBias"] = {
            "circle": {
                "center": {"latitude": float(latitude), "longitude": float(longitude)},
                "radius": 5000.0,
            }
        }
    payload, failure = _call_places(
        session,
        method="POST",
        url=SEARCH_TEXT_URL,
        headers={
            "X-Goog-Api-Key": api_key,
            "X-Goog-FieldMask": SEARCH_FIELD_MASK,
            "Content-Type": "application/json",
        },
        json_body=body,
        timeout_s=timeout_s,
    )
    if failure is not None:
        return failure
    places = (payload or {}).get("places") or []
    if not places:
        return FetchResult(error="no_result", detail=f"No Places match for {text_query!r}.")

    place, verified, reason = _pick_verified_place(
        places,
        [h for h in (dealer_hosts or []) if h],
        latitude=latitude,
        longitude=longitude,
        require_geo=require_geo,
        sibling_coords=sibling_coords,
    )
    if place is None:
        return FetchResult(
            error=reason or "no_result",
            detail=(
                f"{len(places)} Places hit(s) for {text_query!r} rejected ({reason}); "
                f"dealer domain(s): {', '.join(dealer_hosts or []) or 'none on file'}"
            ),
        )
    parsed = rating_from_place_dict(place)
    if parsed is None:
        return FetchResult(error="no_result", detail="Places match had no id.")
    return FetchResult(rating=parsed, verified=verified)


def _coord(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def fetch_rating_for_dealer(
    dealer: dict[str, Any],
    *,
    api_key: str,
    session: requests.Session,
    group_rooftops: dict[str, list[dict[str, Any]]] | None = None,
    timeout_s: float = 15.0,
) -> FetchResult:
    """Details when we know the place id, text search otherwise.

    A fatal failure from the details call is returned as-is: falling through to
    a search would double the request count against an API that has already said
    no at the project level. A details answer that fails verification *does*
    fall through to a search — one extra call to replace a place id that points
    at the wrong rooftop is worth paying.

    ``group_rooftops`` maps a shared domain to every registry row on it (see
    :func:`group_domain_rooftops`). For those domains the host test proves only
    that Google found the right *group*, so the sibling rooftops are handed to
    the matcher as competition and a row with no coordinates of its own is
    refused outright (``ambiguous_domain``) before a request is spent on it.
    """
    hosts = dealer_hosts(dealer)
    latitude = _coord(dealer.get("latitude"))
    longitude = _coord(dealer.get("longitude"))
    siblings = _sibling_coords(dealer, hosts, group_rooftops or {})
    require_geo = any(h in (group_rooftops or {}) for h in hosts)
    if require_geo and (latitude is None or longitude is None):
        return FetchResult(
            error="ambiguous_domain",
            detail=(
                f"{', '.join(hosts)} is shared by several registry rooftops and this row "
                "has no coordinates to tell them apart — refusing to guess."
            ),
        )

    place_id = (dealer.get("google_place_id") or "").strip()
    if place_id:
        result = fetch_rating_by_place_id(
            place_id,
            api_key=api_key,
            session=session,
            dealer_hosts=hosts,
            latitude=latitude,
            longitude=longitude,
            require_geo=require_geo,
            sibling_coords=siblings,
            timeout_s=timeout_s,
        )
        if result.ok or result.fatal:
            return result
        logger.info(
            "stored place id for %s did not verify (%s) — re-resolving by search",
            dealer.get("name"), result.error,
        )
    return fetch_rating_by_search(
        str(dealer.get("name") or ""),
        str(dealer.get("city") or ""),
        str(dealer.get("state") or ""),
        api_key=api_key,
        session=session,
        dealer_hosts=hosts,
        latitude=latitude,
        longitude=longitude,
        require_geo=require_geo,
        sibling_coords=siblings,
        timeout_s=timeout_s,
    )


def dealer_hosts(dealer: dict[str, Any]) -> list[str]:
    """Every host a registry row claims, deduped — the verification whitelist."""
    out: list[str] = []
    for key in ("website_url", "dealer_website_url"):
        host = normalize_host(dealer.get(key))
        if host and host not in out:
            out.append(host)
    return out


def group_domain_rooftops() -> dict[str, list[dict[str, Any]]]:
    """Shared domain -> every active registry rooftop on it.

    Dealer groups (echopark.com, carmax.com) put every store on one domain, so
    a website match there confirms the *group*, not the store. Domains with a
    single rooftop are left out entirely: they need no disambiguation, and
    keeping them would make every lookup pay for a sibling comparison.
    """
    by_host: dict[str, list[dict[str, Any]]] = {}
    for row in comments_db.list_active_dealership_websites():
        for host in dealer_hosts(row):
            by_host.setdefault(host, []).append(row)
    return {h: rows for h, rows in by_host.items() if len(rows) > 1}


def _sibling_coords(
    dealer: dict[str, Any], hosts: list[str], group_rooftops: dict[str, list[dict[str, Any]]]
) -> list[tuple[float, float]]:
    """Coordinates of the *other* rooftops sharing this dealer's domain(s)."""
    out: list[tuple[float, float]] = []
    seen: set[int] = set()
    try:
        own_id = int(dealer.get("id"))
    except (TypeError, ValueError):
        own_id = -1
    for host in hosts:
        for row in group_rooftops.get(host, ()):
            try:
                rid = int(row.get("id"))
            except (TypeError, ValueError):
                continue
            if rid == own_id or rid in seen:
                continue
            lat, lon = _coord(row.get("latitude")), _coord(row.get("longitude"))
            if lat is None or lon is None:
                continue
            seen.add(rid)
            out.append((lat, lon))
    return out


def _mirror_legacy_columns(dealership_id: int, rating: GooglePlaceRating) -> None:
    """Keep ``dealerships.google_*`` in step with ``dealer_ratings``.

    The dealership research page already reads the old columns; not writing them
    would give the same dealer two different star counts depending on which page
    you opened.
    """
    try:
        from backend.db.dealerships_db import save_dealer_google_rating

        save_dealer_google_rating(
            int(dealership_id),
            place_id=rating.place_id,
            rating=rating.rating,
            review_count=rating.review_count,
        )
    except Exception as exc:  # legacy mirror is best-effort; dealer_ratings is the record
        logger.warning("legacy google_* mirror failed for dealership %s: %s", dealership_id, exc)


def sync_dealer_ratings(
    *,
    limit: int = DEFAULT_LIMIT,
    refresh_after_days: int | None = None,
    dry_run: bool = True,
    delay_seconds: float = DEFAULT_DELAY_SECONDS,
    session: requests.Session | None = None,
    api_key: str | None = None,
) -> SyncStats:
    """Fetch and store ratings for up to ``limit`` dealerships, sequentially.

    Writing is opt-in (``dry_run=False``). Re-running is safe: the work queue is
    built from dealerships that have no ``dealer_ratings`` row (or one older
    than ``refresh_after_days``), and each write is an upsert keyed by
    dealership id.
    """
    stats = SyncStats(dry_run=dry_run)
    key = api_key or google_maps_api_key()
    if not key:
        stats.stopped_early = "no_api_key"
        stats.note_error("no_api_key")
        logger.error("No GOOGLE_MAPS_API_KEY — cannot import dealer ratings.")
        return stats

    dealers = comments_db.list_dealerships_needing_rating(
        limit=limit, refresh_after_days=refresh_after_days
    )
    if not dealers:
        return stats

    # One registry-wide pass, not a query per dealer: the set is small (2 hosts
    # across 581 active rows today) and it decides whether a website match is
    # allowed to stand on its own.
    group_rooftops = group_domain_rooftops()
    if group_rooftops:
        logger.info(
            "group domains needing geographic proof: %s",
            ", ".join(f"{h} ({len(rows)} rooftops)" for h, rows in sorted(group_rooftops.items())),
        )

    sess = session or requests.Session()
    pace = max(0.0, float(delay_seconds))
    for dealer in dealers:
        stats.examined += 1
        result = fetch_rating_for_dealer(
            dealer, api_key=key, session=sess, group_rooftops=group_rooftops
        )

        if result.ok:
            stats.resolved += 1
            rating = result.rating
            if not result.verified:
                stats.unverified += 1
                logger.warning(
                    "UNVERIFIED rating for %s (id %s): %s stars from place %s, not confirmed "
                    "against %s",
                    dealer.get("name"), dealer.get("id"), rating.rating, rating.place_id,
                    ", ".join(dealer_hosts(dealer)) or "any dealer domain (none on file)",
                )
            if len(stats.samples) < 8:
                stats.samples.append(
                    {
                        "dealership_id": int(dealer["id"]),
                        "name": dealer.get("name"),
                        "rating": rating.rating,
                        "review_count": rating.review_count,
                        "place_id": rating.place_id,
                        "verified": result.verified,
                    }
                )
            if not dry_run:
                comments_db.upsert_dealer_rating(
                    int(dealer["id"]),
                    place_id=rating.place_id,
                    rating=rating.rating,
                    review_count=rating.review_count,
                    source=result.source,
                )
                # Only a domain-confirmed match is allowed to overwrite the
                # legacy columns the dealership page already renders.
                if result.verified:
                    _mirror_legacy_columns(int(dealer["id"]), rating)
                stats.written += 1
        else:
            code = result.error or "unknown"
            stats.note_error(code)
            if result.fatal:
                stats.stopped_early = code
                logger.error(
                    "Places API is unusable (%s): %s — stopping after %d dealer(s).",
                    code, result.detail, stats.examined,
                )
                break
            if code in _MISS_SOURCES:
                stats.no_result += 1
                # Record the miss so the next run does not pay to re-ask about a
                # dealer Google has no (matching) listing for. Nothing about the
                # inputs changes on its own; --refresh-days reopens these.
                if not dry_run:
                    comments_db.upsert_dealer_rating(
                        int(dealer["id"]),
                        place_id=(dealer.get("google_place_id") or None),
                        rating=None,
                        review_count=None,
                        source=_MISS_SOURCES[code],
                    )
                    stats.written += 1
                if code != "no_result":
                    logger.info("no confirmed match for %s (%s): %s",
                                dealer.get("name"), code, result.detail)
            else:
                # Transient (network/HTTP): leave no row so it is retried later.
                stats.failed += 1
                logger.warning(
                    "rating lookup failed for %s (%s): %s",
                    dealer.get("name"), code, result.detail,
                )
        if pace:
            time.sleep(pace)
    return stats
