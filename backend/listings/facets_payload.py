"""The /listings facet payload: cascade table + long facet vocabularies, out of the HTML.

Web speed 2026-11 (PR 4): the /listings document used to carry the dictionary-encoded
cascade table (``car_rows``, ~12.7k rows in production) and every package row inline:
82% of its wire bytes, the same on every page and every visit, and never cacheable
because the HTML is per-session. Both now live in one JSON document served by
``GET /api/listings/facets?v=<version>``:

* ``version`` is a digest of the payload bytes, so a URL names exactly one body and the
  response is ``public, max-age=31536000, immutable`` (browser and CDN keep it until the
  facets change, when the page links a new version).
* The body is built once per facet rebuild (``get_filter_options`` caches by the grid
  token) and kept gzipped in memory: a request is a dict lookup plus a write.
* No per-user data: hidden dealers, saved cars and paid flags stay in the page.

The page fetches it when the shopper opens the filters or starts a search, never on a
bare visit (nav_perf.js rule: never speculatively pull big payloads).
"""

from __future__ import annotations

import gzip
import hashlib
import json
import threading
from typing import Any, NamedTuple

# Column order for the packed cascade table. Every column except ``cyl`` is
# dictionary-encoded: the values repeat across ~12,700 rows, so shipping integer
# codes plus one vocabulary per column is far smaller than a list of dicts.
_CAR_ROW_COLUMNS = ("make", "model", "trim", "fuel", "cyl", "drive", "body_style", "induction")
_CAR_ROW_RAW_COLUMNS = frozenset({"cyl"})

# Keys of ``get_filter_options()`` the client reads from this payload. The first group is
# the ``/api/listings/filter-options`` shape (``lazyFacetEntries`` reads model_rows,
# trim_rows and all_package_names), then the cascade inputs the page used to inline.
_LIST_KEYS = (
    "makes", "model_rows", "trim_rows", "fuel_types", "cylinders", "transmissions",
    "drivetrains", "forced_inductions", "body_styles", "exterior_colors",
    "interior_colors", "package_rows", "all_package_names",
)

FACETS_API_PATH = "/api/listings/facets"


def pack_car_rows(car_rows: list[dict]) -> dict:
    """Dictionary-encode the cascade table.

    ``listings_boot.js`` (``unpackCarRows``) unpacks this back into the same row
    objects, so the cascade sees exactly what it did before.
    """
    vocabs: list[dict | None] = []
    orders: list[list | None] = []
    for col in _CAR_ROW_COLUMNS:
        if col in _CAR_ROW_RAW_COLUMNS:
            vocabs.append(None)
            orders.append(None)
        else:
            vocabs.append({})
            orders.append([])

    packed_rows: list[list] = []
    for row in car_rows or []:
        out: list = []
        for idx, col in enumerate(_CAR_ROW_COLUMNS):
            val = row.get(col)
            vocab = vocabs[idx]
            if vocab is None:
                out.append(val)
                continue
            if val is None:
                out.append(-1)
                continue
            code = vocab.get(val)
            if code is None:
                order = orders[idx]
                assert order is not None
                code = len(order)
                order.append(val)
                vocab[val] = code
            out.append(code)
        packed_rows.append(out)

    return {"c": list(_CAR_ROW_COLUMNS), "v": orders, "r": packed_rows}


def facets_payload_dict(options: dict[str, Any]) -> dict[str, Any]:
    """The JSON object served at ``/api/listings/facets`` for one facet snapshot."""
    out: dict[str, Any] = {"ok": True}
    for key in _LIST_KEYS:
        out[key] = options.get(key) or []
    out["country_to_makes"] = options.get("country_to_makes") or {}
    out["car_rows"] = pack_car_rows(options.get("car_rows") or [])
    return out


class FacetsPayload(NamedTuple):
    source: Any        # the facet snapshot this body was built from (identity-compared)
    version: str       # digest of ``raw``; the ``?v=`` the page links
    gz_body: bytes
    raw_len: int

    @property
    def url(self) -> str:
        return f"{FACETS_API_PATH}?v={self.version}"

    @property
    def etag(self) -> str:
        return f'"lf-{self.version}"'


_payload: FacetsPayload | None = None
_payload_lock = threading.Lock()


def _build(options: dict[str, Any]) -> FacetsPayload:
    raw = json.dumps(facets_payload_dict(options), separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    version = hashlib.blake2b(raw, digest_size=8).hexdigest()
    return FacetsPayload(options, version, gzip.compress(raw, compresslevel=6), len(raw))


def get_facets_payload() -> FacetsPayload:
    """The current payload, rebuilt only when the facet snapshot changed."""
    global _payload
    from backend.db.repositories.listings_repo import filter_options_snapshot

    options = filter_options_snapshot()
    hit = _payload
    if hit is not None and hit.source is options:
        return hit
    with _payload_lock:
        hit = _payload
        if hit is not None and hit.source is options:
            return hit
        built = _build(options)
        _payload = built
        return built


def reset_facets_payload() -> None:
    """Tests: forget the cached body."""
    global _payload
    _payload = None
