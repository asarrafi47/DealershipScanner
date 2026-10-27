"""
Endpoint replay recipes: persist the inventory API endpoints a scan discovers so
future scans can fetch inventory over plain HTTP before launching Playwright.

A recipe is one captured endpoint (URL, method, POST template, auth headers,
pagination shape) proven to return vehicle rows during a browser scan. Recipes
are promoted from the ``NetworkObserver`` ledger at the end of each dealer run
and stored per dealer under ``workspace/recipes/<dealer_id>.json``.

The cache dir (``RECIPES_DIR``) is anchored at the repo root, never the
process cwd: ``<repo>/workspace/recipes`` (``/app/workspace/recipes`` in the
Railway image, where ``/app/workspace`` is a symlink onto the volume).
``RECIPES_CACHE_DIR`` overrides it for the whole process (read once, at
import); the VDP recipes follow the override as ``<override>/vdp``. Every
cache write is atomic (``_atomic_write_json``): a crash or a concurrent
reader never sees a half-written file.

The cache is tagged with the ``dealer_recipes`` store it mirrors:
``<cache dir>/_store.json`` holds a fingerprint (a sha256 of the store's
host, port and database name, never credentials). A process whose store has
another fingerprint warns once and treats the cache as read-only: it never
pushes this cache's files up into its store, and neither its saves nor its DB
adoptions write into the cache, which stays the mirror of the store it is
tagged for. It serves the store's own copy whenever the store has one, and a
save never overwrites a store copy with a set that came from the cache: when
the store could not be read, saves of that dealer are refused until a store
read shows it has no copy (see ``check_cache_store``, ``load_recipes`` and
``save_recipes``). Re-seed a cache for a new store with
``python -m backend.scanner.recipes --reseed``, or point ``RECIPES_CACHE_DIR``
at a cache of its own.

Replay contract (``recipe_fetch`` phase, see ``try_fetch_via_recipes``):
  - attempt each stored recipe over HTTP with the recorded headers;
  - accept only when the parsed result yields >= ``min_vehicles`` unique VINs;
  - on 401/403 mark the recipe stale (auth rotated) — the browser scan that
    follows re-captures fresh auth headers and re-promotes.

Treat recipe files as sensitive-ish (they can embed public search API keys);
they stay under workspace/ which is not committed.
"""
from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import logging
import os
import re
import threading
import time
from collections.abc import Mapping
from datetime import datetime, timezone
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, unquote, urlencode, urlparse, urlsplit, urlunparse

from backend.scanner import recipe_cache
from backend.scanner.net import client as net_client

logger = logging.getLogger("scanner")

RECIPES_CACHE_DIR_ENV = "RECIPES_CACHE_DIR"


def resolve_recipes_dir(
    module_file: str | os.PathLike[str] | None = None,
    environ: Mapping[str, str] | None = None,
) -> Path:
    """The recipe file cache dir: ``RECIPES_CACHE_DIR`` when set, else
    ``<repo root>/workspace/recipes``.

    The root comes from this module's own location
    (``<root>/backend/scanner/recipes.py``), so the result never depends on the
    process cwd. The real path of this file is used but the returned dir is not
    resolved, so the Railway layout (``/app/workspace`` symlinked onto the
    volume) keeps ``/app/workspace/recipes``. A relative override is pinned
    against the cwd once, here, so a later ``chdir`` cannot move the cache.
    """
    env = os.environ if environ is None else environ
    override = (env.get(RECIPES_CACHE_DIR_ENV) or "").strip()
    if override:
        return Path(override).expanduser().absolute()
    root = Path(__file__ if module_file is None else module_file).resolve().parents[2]
    return root / "workspace" / "recipes"


# Module-level name kept on purpose: tests monkeypatch ``RECIPES_DIR`` and
# scripts import it. Every reader below looks it up at call time.
RECIPES_DIR = resolve_recipes_dir()


def _atomic_write_json(path: Path, obj: Any) -> None:
    """Atomic JSON write of a cache file (``recipe_cache.atomic_write_json``); the
    name stays here because scripts and tests import and patch it."""
    recipe_cache.atomic_write_json(path, obj)


# Pagination shapes we know how to walk during replay.
PAGINATION_CARSCOMMERCE = "carscommerce_page"   # POST body {"page": N, "perPage": M}
PAGINATION_TYPESENSE = "typesense_page"          # POST body searches[].page / per_page
PAGINATION_DEALER_COM = "dealer_com_start"       # POST body inventoryParameters.start
PAGINATION_PAGE_QUERY = "page_query"             # GET URL ?page=N (Team Velocity JSON feed)
PAGINATION_DEP_SRP = "dep_srp_page"              # GET URL ?p=N (Dealer eProcess server-rendered SRP w/ JSON-LD)
PAGINATION_ALGOLIA = "algolia_page"              # POST body {"page": N (0-based), "hitsPerPage": M} (Motive/ridemotive Algolia)
PAGINATION_HTML_PAGE = "html_page_query"         # GET URL ?page=N returning HTML (Overfuel __NEXT_DATA__, nabthat JSON-LD SRP)
PAGINATION_JAZEL_SRP = "jazel_srp_page"          # GET path-walk .../srp-page-N/ returning HTML (Jazel SSR SRP)
PAGINATION_COSMOS_PT = "cosmos_pt"               # GET URL ?pt=N&pn=96 (DealerOn cosmos SRP JSON; Paging.PaginationDataModel)
PAGINATION_GRAPHQL_OFFSET = "graphql_offset"    # POST GraphQL body variables.sp.paging.{offset,limit} (OneAudi omnigraph stockCarSearch)
PAGINATION_NONE = "none"                         # single-shot GET/POST


def _recipe_section_discriminator(post_template: Any) -> str:
    """A stable tag distinguishing inventory *sections* served by the SAME
    endpoint URL.

    Dealer.com calls one ``ws-inv-data`` URL with different POST bodies for
    new / used / certified inventory (``pageAlias`` /
    ``preferences["listing.config.id"]``). Without this tag in the recipe key,
    those bodies collapse to a single recipe and every section but one is
    silently dropped at promotion — the HTTP replay then misses whole slices of
    the lot (e.g. all NEW cars), which shows up downstream as a permanently
    partial delta feed. Returns "" for platforms that do not split inventory by
    request body (CarsCommerce, Typesense, GET feeds), so their keys are
    unchanged and there is no regression.

    Only section-stable fields are used (never the per-page ``start``/``page``
    params), so every page of one section shares a key.
    """
    if not post_template:
        return ""
    body = post_template
    if isinstance(body, str):
        try:
            body = json.loads(body)
        except (TypeError, ValueError):
            return ""
    if not isinstance(body, dict):
        return ""
    alias = str(body.get("pageAlias") or "").strip()
    if not alias:
        prefs = body.get("preferences")
        if isinstance(prefs, dict):
            alias = str(prefs.get("listing.config.id") or "").strip()
    return f"#{alias}" if alias else ""


@dataclass
class EndpointRecipe:
    dealer_id: str
    url: str
    method: str
    content_type: str
    post_template: str | None
    auth_headers: dict[str, str] = field(default_factory=dict)
    pagination: str = PAGINATION_NONE
    vehicle_rows: int = 0
    total_count: int | None = None
    provider_hint: str = ""
    # Last write of this dealer's set (epoch seconds): save_recipes stamps every
    # row on every save. A value set here before the save is overwritten.
    saved_at: float = 0.0
    # Last mutation of THIS recipe (epoch seconds), stamped by every key-scoped op
    # (mark_stale, record_replay_ok, merge_recipes) beside saved_at (N13 P7.3).
    mutated_at: float = 0.0
    last_ok_at: float = 0.0
    stale: bool = False
    stale_reason: str = ""
    # price / trim / exterior_color fractions: from the network ledger at capture,
    # refreshed from parsed rows on every successful replay.
    field_coverage: dict[str, float] = field(default_factory=dict)

    def key(self) -> tuple[str, str]:
        p = urlparse(self.url)
        base = f"{(p.hostname or '').lower()}{p.path}"
        if self.provider_hint == "html_cards" and self.pagination == PAGINATION_HTML_PAGE and p.query:
            # one XHR path serves every condition, section in the query (PixelMotion)
            return (self.method, f"{base}?{html_page_section_key(self.url)}")
        # Dealer.com serves new/used/certified from ONE URL via different bodies;
        # discriminate them so all sections persist as distinct recipes.
        return (self.method, f"{base}{_recipe_section_discriminator(self.post_template)}")


def infer_pagination(url: str, post_template: str | None) -> str:
    host = (urlparse(url).hostname or "").lower()
    body = post_template or ""
    if "carscommerce" in host and '"page"' in body:
        return PAGINATION_CARSCOMMERCE
    if "typesense" in host:
        return PAGINATION_TYPESENSE
    if "ws-inv-data" in url and "inventoryParameters" in body:
        return PAGINATION_DEALER_COM
    if "/cosmos/srp/vehicles/" in url:
        return PAGINATION_COSMOS_PT
    return PAGINATION_NONE


def _recipe_slug(dealer_id: str) -> str:
    return re.sub(r"[^a-z0-9_-]+", "-", (dealer_id or "unknown").lower()).strip("-") or "unknown"


def _recipe_path(dealer_id: str) -> Path:
    return RECIPES_DIR / f"{_recipe_slug(dealer_id)}.json"


# Dealer identity is a pure function of the site URL (dev.dealers.slug_from_url),
# so a hostname change (aaronfordofescondido.com -> .org) mints a NEW dealer id
# and strands the recipes saved under the old one. ``_aliases.json`` bridges
# those rekeys: a flat ``{old_slug: current_dealer_id}`` map kept next to the
# recipe files. READS consult it (file loads here; DB loads in
# ``recipe_store``); WRITES never do — they always target the current slug, so
# a rekeyed dealer's next save migrates its content forward naturally. The
# underscore prefix keeps the file out of ``*.json`` dealer listings (see
# ``import_recipes_to_db`` / ``heal_from_recipes``).
ALIASES_FILENAME = "_aliases.json"


def _load_recipe_aliases() -> dict[str, str]:
    """``{old_slug: current_dealer_id}`` from ``_aliases.json``.

    Failure-tolerant by design: a missing, unreadable, or corrupt alias file —
    or one that is not a flat string->string object — simply means no aliasing.
    """
    try:
        raw = json.loads((RECIPES_DIR / ALIASES_FILENAME).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    if not isinstance(raw, dict):
        return {}
    return {
        _recipe_slug(k): v.strip()
        for k, v in raw.items()
        if isinstance(k, str) and isinstance(v, str) and k.strip() and v.strip()
    }


def resolve_alias_slug(dealer_id: str) -> str | None:
    """The retired slug that may still hold *dealer_id*'s recipes, or ``None``.

    Reverse lookup over the alias map: returns the old slug whose entry points
    at *dealer_id*'s current slug. Self-referential entries are ignored.
    """
    cur = _recipe_slug(dealer_id)
    for old_slug, new_id in _load_recipe_aliases().items():
        if old_slug != cur and _recipe_slug(new_id) == cur:
            return old_slug
    return None


def _recipe_read_path(dealer_id: str) -> Path:
    """Path to READ recipes from: the current slug's file when it exists,
    else an alias-mapped retired slug's file. Writes must keep using
    ``_recipe_path`` (always the current slug) so rekeyed content migrates
    forward on the next save instead of resurrecting the old identity."""
    path = _recipe_path(dealer_id)
    if path.exists():
        return path
    old_slug = resolve_alias_slug(dealer_id)
    if old_slug:
        aliased = RECIPES_DIR / f"{old_slug}.json"
        if aliased.exists():
            return aliased
    return path


# ── Store identity: which dealer_recipes store this cache mirrors ──────────────
#
# A recipe cache dir mirrors ONE ``dealer_recipes`` store. ``load_recipes`` pushes
# a cache file up into the store when the file is newer, which is right for the
# store the cache was filled from and wrong for any other: the MBP cache mirrors
# the local Postgres, and a home-IP run against prod with that cache would push
# every newer local set into prod. ``_store.json`` in the cache dir records the
# fingerprint of the store the cache mirrors. The underscore keeps it out of the
# ``*.json`` dealer listings, like ``_aliases.json``.
STORE_TAG_FILENAME = "_store.json"
STORE_TAG_VERSION = 1
RESEED_BACKUP_DIRNAME = "_reseed_backups"
_PG_DEFAULT_PORT = 5432
_LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})
_store_tag_warned: set[tuple[str, str, str]] = set()
_store_tag_lock = threading.Lock()


def _pg_store_identity(dsn: str) -> str:
    """``postgres:<host>:<port>/<dbname>`` for a Postgres URL.

    Never the user, the password or any other parameter. The port is part of the
    identity because hosts differ only by port in practice: on the mac mini,
    ``localhost:15432/cars`` is the SSH tunnel to the MBP's store and
    ``localhost:5432/cars`` is the mini's own Postgres. Loopback names and a
    Unix-socket host (empty, or a ``/`` directory) all read ``localhost``.

    A part the URL leaves out falls back to ``PGHOST`` / ``PGPORT`` /
    ``PGDATABASE``, as libpq does when ``psycopg.connect`` gets the URL, so
    ``postgresql:///cars`` with ``PGHOST`` naming a remote host is that host.
    """
    parts = urlsplit(dsn)
    query = dict(parse_qsl(parts.query))
    try:
        host = parts.hostname or ""
        port: int | None = parts.port
    except ValueError:  # multi-host lists ("h1:5432,h2:5432") carry no single port
        host, port = parts.netloc.rsplit("@", 1)[-1], None
    host = (host or query.get("host") or os.environ.get("PGHOST") or "").strip().lower()
    if not host or host.startswith("/") or host in _LOOPBACK_HOSTS:
        host = "localhost"
    if port is None:
        try:
            port = int(query.get("port") or (os.environ.get("PGPORT") or "").strip() or _PG_DEFAULT_PORT)
        except ValueError:
            port = _PG_DEFAULT_PORT
    dbname = (unquote(parts.path.lstrip("/")) or query.get("dbname")
              or (os.environ.get("PGDATABASE") or "").strip())
    return f"postgres:{host}:{port}/{dbname}"


def _resolve_store_identity() -> tuple[str | None, bool]:
    """``(identity, failed)``: ``store_identity()``, plus whether working it out
    raised while the store is on (``failed``; the identity is then ``None``)."""
    try:
        from backend.scanner.recipe_store import _enabled

        if not _enabled():
            return None, False
        from backend.db import inventory_pg

        dsn = inventory_pg.inventory_postgres_dsn()
        if dsn:
            return _pg_store_identity(dsn), False
        if inventory_pg.inventory_sqlite_tests_allowed():
            from backend.db.repositories.base_repo import _resolve_db_path

            return "sqlite:" + os.path.realpath(_resolve_db_path()), False
    except Exception as exc:  # noqa: BLE001 — reported as "failed", never a failed load
        logger.debug("Recipe store identity unavailable: %s", type(exc).__name__)
        return None, True
    return None, False


def store_identity() -> str | None:
    """The ``dealer_recipes`` store this process writes through to, as a
    credential-free string, or ``None`` when it uses no store.

    Mirrors how ``recipe_store`` connects (``backend.db.inventory_db.get_conn``):
    the Postgres URL from ``INVENTORY_DATABASE_URL`` / ``DATABASE_URL``, else the
    SQLite inventory file the test suite runs on. ``None`` when
    ``RECIPES_DB_DISABLED`` is on or no store is configured: then nothing is
    pushed up and there is nothing to tag. Also ``None`` when the store is on but
    its identity could not be worked out; ``check_cache_store`` then treats a
    tagged cache as foreign (it cannot show the cache mirrors this store).
    """
    return _resolve_store_identity()[0]


def store_fingerprint(identity: str | None = None) -> str | None:
    """sha256 hex of ``identity`` (default: this process's ``store_identity()``)."""
    ident = store_identity() if identity is None else identity
    if not ident:
        return None
    return hashlib.sha256(ident.encode("utf-8")).hexdigest()


def read_store_tag(cache_dir: Path | None = None) -> tuple[str, str | None]:
    """``(state, fingerprint)`` of ``<cache dir>/_store.json``.

    ``state`` is ``"missing"`` (no file), ``"tagged"`` (a fingerprint was read) or
    ``"unreadable"`` (unreadable, not JSON, or no fingerprint in it).
    """
    path = (RECIPES_DIR if cache_dir is None else cache_dir) / STORE_TAG_FILENAME
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return "missing", None
    except (OSError, ValueError):
        return "unreadable", None
    fp = raw.get("fingerprint") if isinstance(raw, dict) else None
    if not isinstance(fp, str) or not fp.strip():
        return "unreadable", None
    return "tagged", fp.strip()


def write_store_tag(identity: str, *, source: str, cache_dir: Path | None = None) -> Path:
    """Tag the cache dir as the mirror of ``identity``'s store (atomic write).

    The file holds the fingerprint and the store kind (``postgres`` / ``sqlite``),
    never the identity itself.
    """
    d = RECIPES_DIR if cache_dir is None else cache_dir
    d.mkdir(parents=True, exist_ok=True)
    path = d / STORE_TAG_FILENAME
    _atomic_write_json(path, {
        "version": STORE_TAG_VERSION,
        "fingerprint": store_fingerprint(identity),
        "kind": identity.split(":", 1)[0],
        "tagged_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "tagged_by": source,
    })
    return path


@dataclass(frozen=True)
class CacheStoreCheck:
    """This process's store against the store the cache dir is tagged with."""

    cache_dir: Path
    identity: str | None             # this process's store (None: no store in use, or unknown)
    fingerprint: str | None
    tag_state: str                   # "missing" | "tagged" | "unreadable"
    tag_fingerprint: str | None
    identity_unknown: bool = False   # the store is on, but its identity could not be worked out

    @property
    def foreign(self) -> bool:
        """The cache mirrors another store (or its tag cannot be read, or this
        process's store cannot be named while the cache carries a tag). It is
        read-only for this process: its files are never pushed up into this
        process's store, and neither a save nor a DB adoption writes into it,
        so it stays a faithful mirror of the store it is tagged for."""
        if self.fingerprint is None:
            return self.identity_unknown and self.tag_state != "missing"
        if self.tag_state == "unreadable":
            return True
        return self.tag_state == "tagged" and self.tag_fingerprint != self.fingerprint

    @property
    def untagged(self) -> bool:
        """No tag yet and a store in use: the first successful load tags it."""
        return self.fingerprint is not None and self.tag_state == "missing"

    @property
    def verdict(self) -> str:
        if self.foreign:
            return "mismatch"
        if self.fingerprint is None:
            return "store unknown" if self.identity_unknown else "no store"
        return "untagged" if self.untagged else "match"


def check_cache_store(cache_dir: Path | None = None) -> CacheStoreCheck:
    """Compare this process's store with the cache's ``_store.json`` tag.

    On a mismatch (or an unreadable tag, or a tagged cache while this process's
    store cannot be named) it logs one WARNING per process for that cache dir and
    pair of fingerprints; the cache is then read-only for this process
    (``load_recipes`` refuses push-ups, serves the store's own copy when the
    store has one and skips the adoption write; ``save_recipes`` writes to the
    store only, and refuses a dealer whose store copy could not be read).
    """
    d = RECIPES_DIR if cache_dir is None else cache_dir
    identity, unknown = _resolve_store_identity()
    fp = store_fingerprint(identity) if identity else None
    state, tag_fp = read_store_tag(d)
    check = CacheStoreCheck(d, identity, fp, state, tag_fp, identity_unknown=unknown)
    if check.foreign:
        key = (str(d), tag_fp or "<unreadable>", fp or "<unknown>")
        with _store_tag_lock:
            first = key not in _store_tag_warned
            _store_tag_warned.add(key)
        if first:
            tagged = f"tagged for store {tag_fp[:12]}" if tag_fp else "carries an unreadable store tag"
            this = (f"this process writes to {identity} ({(fp or '')[:12]})" if identity else
                    "this process's store could not be named from INVENTORY_DATABASE_URL / DATABASE_URL")
            logger.warning(
                "Recipe cache %s %s, but %s: it does not mirror this store, so this process treats "
                "it as read-only. No cache file is pushed up into the store and the cache is never "
                "written; the store's own copy is served whenever the store has one, and saves go to "
                "the store only. A save never replaces a store copy with a set from this cache: when "
                "the store's copy of a dealer cannot be read, that dealer's saves are refused until a "
                "store read shows the store has no copy. Point RECIPES_CACHE_DIR at a cache for this "
                "store, or re-seed this one with `python -m backend.scanner.recipes --reseed`.",
                d, tagged, this,
            )
    return check


def _tag_untagged_cache(check: CacheStoreCheck) -> None:
    """First successful load on an untagged cache: it now mirrors this store."""
    if not check.untagged or not check.identity:
        return
    if read_store_tag(check.cache_dir)[0] != "missing":
        return  # another loader tagged it meanwhile; its tag stands
    try:
        write_store_tag(check.identity, source="load_recipes", cache_dir=check.cache_dir)
    except OSError as exc:
        logger.debug("Recipe cache %s: store tag not written: %s", check.cache_dir, exc)
        return
    logger.info(
        "Recipe cache %s tagged as the mirror of store %s (%s)",
        check.cache_dir, (check.fingerprint or "")[:12], check.identity.split(":", 1)[0],
    )


_WS_INV_DATA_PATH = "/api/widget/ws-inv-data/"


def _rows_to_recipes(raw: Any) -> list[EndpointRecipe]:
    out: list[EndpointRecipe] = []
    for row in raw if isinstance(raw, list) else []:
        try:
            rec = EndpointRecipe(**{k: v for k, v in row.items()
                                    if k in EndpointRecipe.__dataclass_fields__})
        except TypeError:
            continue
        if rec.pagination == PAGINATION_NONE:
            # Files written before a pagination shape was known stay single-shot
            # forever otherwise (cosmos recipes replayed page 1 of 2+ for months).
            rec.pagination = infer_pagination(rec.url, rec.post_template)
        if (rec.provider_hint or "unknown") == "unknown" and _WS_INV_DATA_PATH in urlparse(rec.url).path:
            # captured before the URL shape named the platform: route it to the
            # dealer.com parser (a generic parse drops the per-car accountId)
            logger.info("Recipe %s: provider_hint %r retagged dealer_dot_com (ws-inv-data URL)",
                        rec.dealer_id, rec.provider_hint or "unknown")
            rec.provider_hint = "dealer_dot_com"
        out.append(rec)
    return out


def load_recipes(dealer_id: str) -> list[EndpointRecipe]:
    """
    The dealer's recipes: the ``dealer_recipes`` row is the authority
    (``backend.scanner.recipe_store``), the cache file a mirror of it (N13 P7.2).

    - Row holding the dealer's recipes (``StoreRow.has_recipes``: ``recipes_written_at``
      set, or a pre-V034 row that has recipes): its rows are returned and the
      cache file is rewritten, atomically and under its ``flock``, only when its
      content differs (a missing, corrupt or truncated file differs). Until
      ``<cache>/_reconciled`` names this store, a differing file that exists is first
      copied to ``workspace/backups/recipe_cache_differing``; when that copy fails the
      file is left alone. The file's age does not matter: a newer file never
      overwrites the store (the old ``saved_at`` push-up is gone).
    - No row, or a hints-only row (not authoritative), and a cache file with rows: the
      file seeds the store through ``db_mutate`` (only while the row still holds no
      recipes) and is returned as it is.
    - The store unreachable (or disabled): the file rows, as before.

    Before the read, a pending offline op-log of this dealer for this store is replayed
    through ``db_mutate`` (``_replay_pending``), so the rows returned include it.

    ``saved_at`` stays the per-write stamp (``save_recipes``) because hosts running
    older code still compare it; the store keeps its max as ``max_saved_at``.

    Store identity (``_store.json``, see ``check_cache_store``): when the cache
    dir is tagged for another store than this process's, the cache is
    read-only for this process (``_load_foreign``). Nothing is pushed up and
    nothing is written into the cache, whichever copy is newer, so the cache
    keeps mirroring the store it is tagged for. The store's own copy is
    returned whenever the store has one, so a caller that saves the set
    (``save_recipes`` then writes to the store only) cannot write the other
    store's file back through. The cache file is returned only when a strict
    store read showed the store has no copy, or when the store could not be
    read; in the second case ``save_recipes`` refuses that dealer until a store
    read shows there is no copy. An untagged cache is tagged for this process's
    store on the first load that reached the store (a row came back, or a seed
    landed).
    """
    store_check = check_cache_store()
    _replay_pending(dealer_id)
    # Read may resolve through _aliases.json (URL-rekeyed dealer); any write
    # below targets the CURRENT slug so the content migrates forward.
    read_path = _recipe_read_path(dealer_id)
    file_rows: list[dict] = []
    try:
        raw = json.loads(read_path.read_text(encoding="utf-8"))
        if isinstance(raw, list):
            file_rows = raw
    except (OSError, ValueError):
        pass
    if store_check.foreign:
        return _load_foreign(dealer_id, store_check, file_rows)

    slug = _recipe_slug(dealer_id)
    row = None
    try:
        from backend.scanner.recipe_store import db_read

        row = db_read(slug)
    except Exception:  # noqa: BLE001 — DB is optional here
        pass

    if row is not None and row.has_recipes:
        _refresh_cache(dealer_id, row.rows, store_check)
        _tag_untagged_cache(store_check)
        return _rows_to_recipes(row.rows)

    seeded = False
    if file_rows:
        seeded = _seed_store(dealer_id, file_rows)
    if row is not None or seeded:
        _tag_untagged_cache(store_check)
    return _rows_to_recipes(file_rows)


def _seed_store(dealer_id: str, file_rows: list[dict]) -> bool:
    """Seed the store with the cache file's rows, exactly as on file (no re-stamp:
    the seeded copy keeps the file's own ``saved_at``). Skipped when another host made
    the row authoritative since this process read it."""
    try:
        from backend.scanner.recipe_store import db_mutate

        landed = db_mutate(
            _recipe_slug(dealer_id),
            lambda cur: file_rows if cur is None or not cur.has_recipes else None,
        )
    except Exception:  # noqa: BLE001
        return False
    if landed is None or landed.rows != file_rows:
        return False
    logger.warning(
        "Recipes [%s]: dealer_recipes had no recipes for this dealer; seeded it with the "
        "%d recipe(s) of the local cache file", dealer_id, len(file_rows),
    )
    return True


def _refresh_cache(dealer_id: str, db_rows: list[dict], check: CacheStoreCheck) -> bool:
    """Make the cache file hold ``db_rows``; True when it was rewritten.

    Compare-then-write runs under the file's ``flock`` and touches nothing when the
    content is already equal. A differing file that exists is backed up first while
    the cache is not yet reconciled with this store; a failed backup or write leaves
    the file as it was (the caller still returns the store's rows).
    """
    path = _recipe_path(dealer_id)
    try:
        RECIPES_DIR.mkdir(parents=True, exist_ok=True)
        with recipe_cache.cache_lock(path):
            if recipe_cache.read_rows(path) == db_rows:
                return False
            if path.exists() and not recipe_cache.is_reconciled(check.cache_dir, check.fingerprint):
                saved = recipe_cache.backup_cache_file(path)
                logger.warning(
                    "Recipes [%s]: the cache file differs from the store and the cache is not "
                    "reconciled with it; backed it up to %s before the store's copy replaces it",
                    dealer_id, saved,
                )
            _atomic_write_json(path, db_rows)
            return True
    except OSError as exc:
        logger.debug("Recipes [%s]: cache refresh skipped: %s", dealer_id, exc)
        return False


# Dealers for which a load on a foreign cache (one tagged for another store) could
# not read this process's store and served the cache file instead. A save of such a
# set must not reach the store: it may hold a copy the read never saw, and the
# save would replace it with the other store's set under a fresh stamp that then
# wins on every host. Keyed by (cache dir, store fingerprint, slug); a key leaves
# only when a store read at save time shows the store has no copy.
_foreign_unread: set[tuple[str, str, str]] = set()
_foreign_warned: set[str] = set()  # which foreign-cache WARNINGs this process already logged


def _foreign_key(check: CacheStoreCheck, slug: str) -> tuple[str, str, str]:
    return (str(check.cache_dir), check.fingerprint or "<unknown>", slug)


def _foreign_store_read(slug: str) -> tuple[list[dict], float] | None:
    """The store's copy for ``slug``; ``None`` only when the store has none.

    Raises when the read cannot show which (``RecipeStoreReadError``, or any
    error reaching ``recipe_store`` at all)."""
    from backend.scanner.recipe_store import db_load_recipes

    return db_load_recipes(slug, strict=True)


def _read_failure(exc: BaseException) -> str:
    """Why a strict store read failed, without the error text (it can quote the URL)."""
    from backend.scanner.recipe_store import RecipeStoreReadError

    return str(exc) if isinstance(exc, RecipeStoreReadError) else type(exc).__name__


def _warn_first(kind: str, msg: str, *args: Any) -> None:
    """``msg`` at WARNING the first time ``kind`` is logged in this process, at
    DEBUG after that."""
    with _store_tag_lock:
        first = kind not in _foreign_warned
        _foreign_warned.add(kind)
    if first:
        logger.warning(msg + " Further such cases in this process log at DEBUG.", *args)
    else:
        logger.debug(msg, *args)


def _load_foreign(dealer_id: str, check: CacheStoreCheck, file_rows: list[dict]) -> list[EndpointRecipe]:
    """``load_recipes`` on a cache tagged for another store (read-only here).

    - The store has a copy: it is returned, whichever copy is newer, and nothing
      is written (no push-up, no adoption into the cache).
    - The store has no copy (a strict read showed it): the cache file is
      returned. A save of it seeds this store with that set (a replay that
      answered, or a stale flag); it replaces no store copy.
    - The store could not be read: the cache file is returned for replay, and
      the dealer is held in ``_foreign_unread`` so ``save_recipes`` refuses its
      saves until a store read shows the store has no copy.
    """
    slug = _recipe_slug(dealer_id)
    try:
        found = _foreign_store_read(slug)
    except Exception as exc:  # noqa: BLE001 — fail closed: saves refused below
        with _store_tag_lock:
            _foreign_unread.add(_foreign_key(check, slug))
        _warn_first(
            "unread",
            "Recipes [%s]: this process's store could not be read (%s), and the recipe cache is "
            "tagged for another store, so the cache file (%d recipe(s)) is served for replay only: "
            "saves of this dealer are refused in this process until a store read shows the store "
            "has no copy.",
            dealer_id, _read_failure(exc), len(file_rows),
        )
        return _rows_to_recipes(file_rows)
    if found is not None:
        db_rows = found[0]
        logger.debug(
            "Recipes [%s]: serving the store's copy (%d recipe(s)); the recipe cache is tagged for "
            "another store, so nothing is pushed up and the cache file is left as it is",
            dealer_id, len(db_rows),
        )
        return _rows_to_recipes(db_rows)
    logger.debug(
        "Recipes [%s]: the store has no copy; serving the cache file (%d recipe(s)) of a cache tagged "
        "for another store, and pushing nothing up", dealer_id, len(file_rows),
    )
    return _rows_to_recipes(file_rows)


def save_recipes(dealer_id: str, recipes: list[EndpointRecipe]) -> None:
    """Write the dealer's whole recipe set: the cache file, then the
    write-through to ``dealer_recipes`` through ``db_mutate``.

    This is the explicit REPLACE, for scripts and tests; scan code changes one recipe
    with the key-scoped ops below (``mark_stale``, ``record_replay_ok``,
    ``merge_recipes``) so concurrent hosts do not overwrite each other.

    Every row's ``saved_at`` and ``mutated_at`` are stamped with one ``time.time()``
    taken here, before either write, so the file and the DB row carry the same stamp
    and every write outranks older copies on other hosts (see ``load_recipes``). The
    caller's ``EndpointRecipe`` objects are not modified. A store write that did not
    reach the store (unreachable) is appended to the offline op-log
    (``_store_op``) and replayed by the next load that reaches the store.

    When the cache dir is tagged for another store (``check_cache_store``), the
    cache is read-only for this process: the file is left as it is and the set
    goes to this process's store only (``_save_to_store_only``). The store is
    then the only copy, so a failed store write is logged as a lost save
    (WARNING once per error class per process; repeats at DEBUG). A dealer for
    which a foreign load in this process could not read the store is refused
    unless a store read now shows the store has no copy.
    """
    store_check = check_cache_store()
    now = time.time()
    rows = [asdict(r) for r in recipes]
    for row in rows:
        row["saved_at"] = now
        row["mutated_at"] = now
    if store_check.foreign:
        _save_to_store_only(dealer_id, rows, store_check)
        return
    RECIPES_DIR.mkdir(parents=True, exist_ok=True)
    path = _recipe_path(dealer_id)
    with recipe_cache.cache_lock(path):
        _atomic_write_json(path, rows)
    try:
        _store_op(dealer_id, {"op": OP_REPLACE, "key": None, "observed_at": None,
                              "payload": {"rows": rows}, "at": now})
    except Exception:  # noqa: BLE001 — file write is the contract; DB is best-effort
        pass


def _save_to_store_only(dealer_id: str, rows: list[dict], check: CacheStoreCheck) -> None:
    """``save_recipes`` on a cache tagged for another store: write the stamped
    rows to this process's store and leave the cache file as it is.

    A dealer in ``_foreign_unread`` (a load of it in this process could not read
    the store and served the cache file) is written only when a store read made
    here shows the store has no copy; otherwise the save is refused, even when a
    later load read the store's copy, because nothing shows which load this set
    came from. The rows may be the other store's set, and writing them would
    replace a copy that the failed read never saw.
    """
    slug = _recipe_slug(dealer_id)
    key = _foreign_key(check, slug)
    with _store_tag_lock:
        unread = key in _foreign_unread
    if unread:
        try:
            absent = _foreign_store_read(slug) is None
            why = "the store holds a copy"
        except Exception as exc:  # noqa: BLE001 — still unread: refuse
            absent = False
            why = f"the store still cannot be read ({_read_failure(exc)})"
        if not absent:
            _warn_first(
                "refused",
                "Recipes [%s]: save of %d recipe(s) refused: the recipe cache is tagged for another "
                "store, an earlier load of this dealer in this process could not read the store and "
                "served that cache's file, so this set may be the other store's, and %s. The store "
                "and the cache file are left as they are.",
                dealer_id, len(rows), why,
            )
            return
        with _store_tag_lock:
            _foreign_unread.discard(key)  # the store has no copy: this save seeds it
    saved = False
    try:
        from backend.scanner.recipe_store import db_save_recipes

        saved = bool(db_save_recipes(slug, rows, file_written=False))
    except Exception as exc:  # noqa: BLE001 — db_save_recipes logs its own failures
        logger.warning("Recipes [%s]: %d recipe(s) were not saved: the store write raised %s",
                       dealer_id, len(rows), type(exc).__name__)
        return
    logger.debug(
        "Recipes [%s]: %s %d recipe(s) to this process's store only; the recipe cache is tagged for "
        "another store, so the cache file was left as it is",
        dealer_id, "saved" if saved else "could not save (logged by recipe_store)", len(rows),
    )


# ── Key-scoped mutation API (N13 P7.3) ───────────────────────────────────────
#
# A scan changes ONE recipe of a set other hosts also change, so it must not
# load the whole set and save it back (the later save wins and drops the other
# host's edit). Each op below is applied to the row's CURRENT content inside
# ``db_mutate``'s compare-and-set, touches only its key, and stamps that recipe's
# ``mutated_at`` and ``saved_at`` (``saved_at`` stays a per-write stamp because
# hosts on older code still compare it).
#
# When the store cannot take an op, it is appended to the offline op-log
# (``workspace/recipes/_pending/<store fingerprint>/<slug>``) and applied to the
# cache file under its flock; the next ``load_recipes`` that reaches the store
# replays the log op by op through ``db_mutate`` (``_replay_pending``): ops, never
# a snapshot of the cache, so another host's newer edits are not overwritten.

OP_MARK_STALE = "mark_stale"
OP_REPLAY_OK = "record_replay_ok"
OP_MERGE = "merge_recipes"
OP_REPLACE = "replace_recipes"


def _row_key(row: dict) -> tuple[str, str] | None:
    """The key a recipe row has once ``load_recipes`` normalizes it."""
    recs = _rows_to_recipes([row])
    return recs[0].key() if recs else None


def _merge_rank(
    existing: list[EndpointRecipe], candidates: list[EndpointRecipe], max_recipes: int,
) -> list[EndpointRecipe]:
    """The merge, rank and cap rules of promotion: a fresh capture wins the slot of
    the same key unless the recipe on file is live, has answered, and has more rows;
    rich feeds (price + trim/colour) replay first, VIN-only endpoints stay on file for
    delta VIN-set work but never outrank a real inventory feed. Mutates ``candidates``
    that win (their ``last_ok_at`` and missing ``field_coverage`` carry over)."""
    merged: dict[tuple[str, str], EndpointRecipe] = {r.key(): r for r in existing}
    for c in candidates:
        cur = merged.get(c.key())
        # A fresh capture always wins: newer auth headers, un-stales the entry.
        if cur is None or not cur.last_ok_at or cur.stale or c.vehicle_rows >= cur.vehicle_rows:
            c.last_ok_at = cur.last_ok_at if cur else 0.0
            if cur is not None and not c.field_coverage and cur.field_coverage:
                c.field_coverage = dict(cur.field_coverage)
            merged[c.key()] = c
    ranked = sorted(
        merged.values(),
        key=lambda r: (
            r.stale,
            0 if recipe_coverage_is_rich(r.field_coverage) else 1,
            -(r.total_count or 0),
            -r.vehicle_rows,
        ),
    )
    return ranked[:max_recipes]


def _apply_op(
    entry: dict, rows: list[dict], *, now: float, cur: Any = None, replaying: bool = False,
) -> list[dict] | None:
    """``entry``'s op applied to ``rows``: the new row list, or None when the op changes
    nothing (key gone, stale mark older than a success, no candidate lands).

    Pure on its inputs, so ``db_mutate`` can re-run it on a fresh row. ``cur`` is the
    ``StoreRow`` being mutated (None: no row, or the cache file). ``replaying`` is an
    op-log entry meeting the store later: its ``mutated_at`` is the time it was
    recorded, and the guards that keep an old op from undoing newer work apply."""
    op = entry["op"]
    payload = entry.get("payload") or {}
    stamp = float(entry.get("at") or now) if replaying else now
    key = tuple(entry["key"]) if entry.get("key") else None
    observed_at = entry.get("observed_at")

    if op == OP_MARK_STALE:
        out = [dict(r) for r in rows]
        hit = False
        for r in out:
            if _row_key(r) != key:
                continue
            if observed_at is not None and float(r.get("last_ok_at") or 0.0) > float(observed_at):
                continue  # another host's success is newer than the failure seen
            r.update(stale=True, stale_reason=str(payload.get("reason") or "")[:200],
                     mutated_at=stamp, saved_at=now)
            hit = True
        return out if hit else None

    if op == OP_REPLAY_OK:
        out = [dict(r) for r in rows]
        hit = False
        total = payload.get("total_count")
        for r in out:
            if _row_key(r) != key:
                continue
            r["last_ok_at"] = max(float(r.get("last_ok_at") or 0.0), float(payload.get("last_ok_at") or 0.0))
            r["field_coverage"] = dict(payload.get("field_coverage") or {})
            if payload.get("unstale"):
                r["stale"] = False
                r["stale_reason"] = ""
            if total:
                r["total_count"] = int(total)
            r.update(mutated_at=stamp, saved_at=now)
            hit = True
        return out if hit else None

    if op == OP_MERGE:
        cands = _rows_to_recipes(payload.get("candidates") or [])
        existing = _rows_to_recipes(rows)
        if replaying and cur is not None:
            present = {r.key() for r in existing}
            base_keys = entry.get("base_keys")
            base_rev = entry.get("base_rev")
            moved = base_rev is None or cur.rev > base_rev
            gone = {tuple(s.get("key") or ()): float(s.get("superseded_at") or 0.0)
                    for s in (getattr(cur, "superseded", None) or [])}
            kept = []
            for c in cands:
                k = c.key()
                if k not in present and moved and (base_keys is None or list(k) in base_keys):
                    continue  # the key was there when this op was recorded and has been removed since
                if gone.get(k, 0.0) >= stamp:
                    continue  # superseded after this op was recorded: never un-supersede
                kept.append(c)
            cands = kept
        if not cands:
            return None
        landed = {id(c) for c in cands}
        out = []
        for r in _merge_rank(existing, cands, int(payload.get("max_recipes") or 8)):
            row = asdict(r)
            if id(r) in landed:
                row.update(mutated_at=stamp, saved_at=now)
            out.append(row)
        return None if out == rows else out

    if op == OP_REPLACE:
        if replaying and cur is not None:
            base_rev = entry.get("base_rev")
            if base_rev is None or cur.rev > base_rev:
                return None  # the store moved on: a snapshot is never merged over it
        return list(payload.get("rows") or [])

    raise ValueError(f"unknown recipe op {op!r}")


def _store_op(dealer_id: str, entry: dict, *, foreign: bool = False) -> tuple[bool, Any, Any]:
    """Run ``entry`` through ``db_mutate`` on this process's store.

    Returns ``(took, written, prev)``: ``took`` is True when the store applied the op or
    judged it a no-op, False when it could not be reached (disabled, down, or the CAS
    gave up). ``written`` is the ``StoreRow`` as written (None for a no-op or a miss),
    ``prev`` the row the op was planned on. A miss on an enabled store is appended to
    the offline op-log with the rev it was planned against (None when never read)."""
    from backend.scanner import recipe_store

    slug = _recipe_slug(dealer_id)
    seen: dict[str, Any] = {}
    now = float(entry["at"])

    def fn(cur):
        seen["ran"] = True
        seen["cur"] = cur
        new = _apply_op(entry, cur.rows if cur else [], now=now, cur=cur)
        seen["noop"] = new is None
        return new

    if not recipe_store._enabled():
        return False, None, None
    try:
        written = recipe_store.db_mutate(slug, fn, file_written=not foreign)
    except Exception:  # noqa: BLE001 — a store problem never fails a scan
        written = None
    took = written is not None or bool(seen.get("ran") and seen.get("noop"))
    prev = seen.get("cur")
    if written is not None and written is prev:
        written = None  # the unchanged row of a no-op
    if not took:
        _oplog_append(slug, entry, base_rev=prev.rev if prev is not None else None)
    return took, written, prev


def _oplog_path(slug: str) -> Path | None:
    fp = store_fingerprint()
    return recipe_cache.oplog_path(RECIPES_DIR, fp, slug) if fp else None


def _oplog_append(slug: str, entry: dict, *, base_rev: int | None) -> None:
    path = _oplog_path(slug)
    if path is None:
        _warn_first("oplog-fp", "Recipes: the store's identity is unknown, so an op the store could "
                    "not take is not logged for replay (the cache file has it).")
        return
    row = dict(entry, base_rev=base_rev)
    if entry["op"] == OP_MERGE:
        cached = recipe_cache.read_rows(_recipe_path(slug)) or []
        row["base_keys"] = [list(k) for k in (_row_key(r) for r in cached) if k]
    try:
        with recipe_cache.cache_lock(path):
            recipe_cache.oplog_append(path, row)
    except OSError as exc:
        logger.warning("Recipes [%s]: an op the store could not take could not be logged for replay: %s",
                       slug, exc)


def _replay_pending(dealer_id: str) -> None:
    """Replay this dealer's offline op-log (for this process's store only) through
    ``db_mutate``, oldest first. Entries the store took (applied or judged a no-op)
    leave the log; at the first one the store cannot take, the rest stay. Cheap when
    there is no log: one ``exists`` check."""
    slug = _recipe_slug(dealer_id)
    if not (RECIPES_DIR / recipe_cache.PENDING_DIR).is_dir():
        return
    from backend.scanner import recipe_store

    if not recipe_store._enabled():
        return
    path = _oplog_path(slug)
    if path is None or not path.exists():
        return
    try:
        with recipe_cache.cache_lock(path):
            entries = recipe_cache.oplog_read(path)
            remaining = list(entries)
            while remaining:
                entry = remaining[0]
                seen: dict[str, Any] = {}
                now = time.time()

                def fn(cur, entry=entry, seen=seen, now=now):
                    seen["ran"] = True
                    try:
                        new = _apply_op(entry, cur.rows if cur else [], now=now, cur=cur, replaying=True)
                    except Exception as exc:  # noqa: BLE001 — an unappliable entry must not wedge the log
                        logger.warning("Recipes [%s]: dropped an unreplayable pending %s op: %s",
                                       dealer_id, entry.get("op"), exc)
                        new = None
                    seen["noop"] = new is None
                    return new

                written = recipe_store.db_mutate(slug, fn)
                if not (written is not None or (seen.get("ran") and seen.get("noop"))):
                    break
                remaining.pop(0)
            if remaining != entries or not entries:
                recipe_cache.oplog_rewrite(path, remaining)
    except Exception as exc:  # noqa: BLE001
        logger.debug("Recipes [%s]: pending-op replay skipped: %s", dealer_id, exc)


def _cache_apply(dealer_id: str, entry: dict) -> list[dict] | None:
    """Apply ``entry`` to the cache file under its flock; the resulting rows, or None
    when nothing changed (or the file system refused)."""
    path = _recipe_path(dealer_id)
    try:
        RECIPES_DIR.mkdir(parents=True, exist_ok=True)
        with recipe_cache.cache_lock(path):
            rows = recipe_cache.read_rows(_recipe_read_path(dealer_id)) or []
            new = _apply_op(entry, rows, now=float(entry["at"]))
            if new is None:
                return None
            _atomic_write_json(path, new)
            return new
    except OSError as exc:
        logger.debug("Recipes [%s]: cache op skipped: %s", dealer_id, exc)
        return None


def _cache_follow(dealer_id: str, new_rows: list[dict], prev: Any, check: CacheStoreCheck) -> None:
    """After the store took an op, make the cache file hold the store's rows. A file that
    held neither the rows the op was planned on nor the new ones is backed up first
    while the cache is not reconciled with this store."""
    path = _recipe_path(dealer_id)
    try:
        RECIPES_DIR.mkdir(parents=True, exist_ok=True)
        with recipe_cache.cache_lock(path):
            have = recipe_cache.read_rows(path)
            if have == new_rows:
                return
            if (path.exists() and have != (prev.rows if prev is not None else None)
                    and not recipe_cache.is_reconciled(check.cache_dir, check.fingerprint)):
                recipe_cache.backup_cache_file(path)
            _atomic_write_json(path, new_rows)
    except OSError as exc:
        logger.debug("Recipes [%s]: cache follow skipped: %s", dealer_id, exc)


def _mutate(dealer_id: str, entry: dict) -> list[dict]:
    """Apply one key-scoped op: the store first (compare-and-set), then the cache file.

    Returns the dealer's rows after the op as the store (or, when the store did not
    take it, the cache file) holds them; an op that changed nothing returns the rows
    as they were. Where the cache is tagged for another store the file is left alone
    and the op goes to this process's store only."""
    check = check_cache_store()
    entry = dict(entry, at=time.time())
    took, written, prev = _store_op(dealer_id, entry, foreign=check.foreign)
    if took:
        if written is not None:
            if not check.foreign:
                _cache_follow(dealer_id, written.rows, prev, check)
            else:
                logger.debug(
                    "Recipes [%s]: %s applied to this process's store only; the recipe cache is tagged "
                    "for another store, so the cache file was left as it is", dealer_id, entry["op"],
                )
            return written.rows
        if prev is None or not prev.has_recipes:
            # The store holds no recipes for the dealer (a cache file waiting to be seeded
            # by the next load): the op belongs to the file.
            new = _cache_apply(dealer_id, entry)
            if new is not None:
                return new
        return list(prev.rows) if prev is not None else []
    if check.foreign:
        return []
    new = _cache_apply(dealer_id, entry)
    return new if new is not None else (recipe_cache.read_rows(_recipe_path(dealer_id)) or [])


def mark_stale(dealer_id: str, recipe: EndpointRecipe, reason: str, *, observed_at: float | None = None) -> None:
    """Flag ``recipe``'s key stale. Does nothing when that key is no longer in the set,
    and, when ``observed_at`` (the time the failure was observed, taken before the
    request) is given, when that recipe's ``last_ok_at`` is newer: another host's success
    after the failure began outranks it."""
    _mutate(dealer_id, {"op": OP_MARK_STALE, "key": list(recipe.key()), "observed_at": observed_at,
                        "payload": {"reason": reason}})


def record_replay_ok(
    dealer_id: str, key: tuple[str, str], *, last_ok_at: float, field_coverage: dict[str, float],
    unstale: bool, total_count: int | None = None,
) -> None:
    """Record a replay that answered with rows on the recipe ``key``: its ``last_ok_at``
    (never moved back) and measured ``field_coverage``, un-stale when ``unstale``, and
    the learned ``total_count`` for that key only."""
    _mutate(dealer_id, {"op": OP_REPLAY_OK, "key": list(key), "observed_at": None,
                        "payload": {"last_ok_at": last_ok_at, "field_coverage": dict(field_coverage),
                                    "unstale": bool(unstale), "total_count": total_count}})


def merge_recipes(
    dealer_id: str, candidates: list[EndpointRecipe], *, max_recipes: int = 8,
) -> list[EndpointRecipe]:
    """Merge ``candidates`` into the stored set by promotion's rules (``_merge_rank``):
    inside the compare-and-set, against the row as it is when the write lands, so a
    recipe another host added meanwhile survives. Returns the set after the merge."""
    rows = _mutate(dealer_id, {"op": OP_MERGE, "key": None, "observed_at": None,
                               "payload": {"candidates": [asdict(c) for c in candidates],
                                           "max_recipes": max_recipes}})
    return _rows_to_recipes(rows)


def replace_recipes(dealer_id: str, recipes: list[EndpointRecipe], *, reason: str = "") -> None:
    """Make ``recipes`` the dealer's whole live set. A stub for ``save_recipes`` until
    P7.5 archives the replaced recipes into ``superseded_json`` (``reason`` is for that)."""
    save_recipes(dealer_id, recipes)


RECIPE_STATUS_STALE_PREFIX = "stale:"
RECIPE_STATUS_BLOCKED_PREFIX = "blocked:"


def egress_tag() -> str:
    """``SCANNER_EGRESS_TAG`` names a scan host whose IP some dealer edges refuse
    (a datacenter such as Railway; Cloudflare challenges it while a home IP gets
    through). Empty on a home scanner.

    On a tagged host a 401/403 says "this host is blocked", not "the recipe is
    dead": the recipe store and ``scan_hints`` are shared by every scanner, so a
    tagged host never marks a recipe stale or rejects it for auth. It writes
    ``recipe_status = blocked:<tag>:<status>:<iso>`` instead (2026-09-29: the
    first Railway runs staled 25+ dealers' recipes that replay fine from home,
    and every scanner then skipped them as recipe-less)."""
    return re.sub(r"[^a-z0-9_-]", "", (os.environ.get("SCANNER_EGRESS_TAG") or "").strip().lower())[:32]


def blocked_status_value(status: int | str) -> str:
    return (f"{RECIPE_STATUS_BLOCKED_PREFIX}{egress_tag()}:{status}:"
            f"{datetime.now(timezone.utc).replace(microsecond=0).isoformat()}")


def blocked_on_this_host(recipe_status: str | None) -> bool:
    """True when *recipe_status* is a ``blocked:`` written by a host with this
    host's egress tag. Other hosts ignore it."""
    tag = egress_tag()
    return bool(tag) and str(recipe_status or "").startswith(f"{RECIPE_STATUS_BLOCKED_PREFIX}{tag}:")


def _parses_json(text: str | None) -> bool:
    if not text:
        return False
    try:
        json.loads(text)
        return True
    except ValueError:
        return False


def record_stale_status(dealer_id: str, status: int) -> str | None:
    """``scan_hints.recipe_status = "stale:<status>:<iso>"`` after a replay
    whose auth is dead (401/403 before any VIN). Best-effort: the scan must
    never fail because the hint store did. Returns the value written."""
    if egress_tag():
        value = blocked_status_value(int(status))
    else:
        value = f"{RECIPE_STATUS_STALE_PREFIX}{int(status)}:{datetime.now(timezone.utc).replace(microsecond=0).isoformat()}"
    try:
        from backend.scanner.recipe_store import set_scan_hints

        set_scan_hints(dealer_id, {"recipe_status": value})
    except Exception as exc:  # noqa: BLE001
        logger.debug("recipe_status stale hint skipped [%s]: %s", dealer_id, exc)
    return value


def clear_stale_status(dealer_id: str) -> bool:
    """A successful replay clears a ``stale:`` recipe_status (to ``ok``), and a
    ``rejected:`` one too: that verdict was on a re-synthesis that lost to the
    recipe on file, which has just answered with rows, so the dealer is not
    recipe-less and must not be routed into the lifecycle every run.
    ``uncertain:`` is left alone: it describes the recipes now replaying.
    Best-effort."""
    try:
        from backend.scanner.recipe_store import get_scan_hints, set_scan_hints

        cur = str((get_scan_hints(dealer_id) or {}).get("recipe_status") or "")
        if not cur.startswith((RECIPE_STATUS_STALE_PREFIX, "rejected:", RECIPE_STATUS_BLOCKED_PREFIX)):
            return False
        return bool(set_scan_hints(dealer_id, {"recipe_status": "ok"}))
    except Exception as exc:  # noqa: BLE001
        logger.debug("recipe_status clear skipped [%s]: %s", dealer_id, exc)
        return False


def promote_from_ledger(
    dealer_id: str,
    provider: str,
    ledger_endpoints: list[Any],
    *,
    min_vehicle_rows: int = 3,
    max_recipes: int = 8,
    validate: bool = False,
    base_url: str = "",
    dealer_name: str = "",
    place: dict[str, str] | None = None,
    fetch: Any = None,
    log_root: Path | None = None,
    validation_out: dict[str, Any] | None = None,
) -> int:
    """
    Merge qualifying ``CapturedEndpoint``s into the dealer's recipe file.
    Fresh captures replace stale/older entries for the same (method, host+path).
    Returns the number of recipes written.

    ``validate=True`` (the discovery capture) runs the merged live set through
    :func:`backend.scanner.recipe_validation.gate_recipes` before the save: a
    ``reject`` (one condition, section-scoped, dead auth, no rows, short page
    under half the lot) leaves the recipe file untouched, logs the report to
    the dealer's discovery.md and returns 0; ``uncertain`` / ``ok`` save with
    ``scan_hints.recipe_status`` recorded. *fetch* / *log_root* are the test
    seams; *validation_out* receives the report's summary and status.
    """
    now = time.time()
    candidates: list[EndpointRecipe] = []
    for ep in ledger_endpoints or []:
        rows = int(getattr(ep, "vehicle_rows", 0) or 0)
        total = getattr(ep, "total_count", None)
        if rows < min_vehicle_rows and not total:
            continue
        url = str(getattr(ep, "url", "") or "")
        if not url.startswith("http"):
            continue
        post = getattr(ep, "post_data_sample", None)
        ctype = str(getattr(ep, "content_type", "") or "")
        hint = provider or ""
        pagination = infer_pagination(url, post)
        if str(getattr(ep, "reason", "") or "") == "html_fragment_cards":
            # NetworkObserver ledgered an HTML fragment (PixelMotion / server-rendered
            # card pagers) rather than JSON: replay hands the text to the html_cards
            # parser and walks ?page=N until a page adds no VIN (mcpeeks-com, 2026-09-26).
            hint = "html_cards"
            if pagination == PAGINATION_NONE and not post:
                pagination = PAGINATION_HTML_PAGE
        candidates.append(
            EndpointRecipe(
                dealer_id=dealer_id,
                url=url,
                method=str(getattr(ep, "method", "GET") or "GET").upper(),
                content_type=ctype,
                post_template=post,
                auth_headers=dict(getattr(ep, "auth_headers", None) or {}),
                pagination=pagination,
                vehicle_rows=rows,
                total_count=int(total) if total else None,
                provider_hint=hint,
                saved_at=now,
                field_coverage={
                    k: float(v) for k, v in (getattr(ep, "field_coverage", None) or {}).items()
                },
            )
        )
    if not candidates:
        return 0

    # Validate on a snapshot, then apply through the key-scoped merge: it re-runs the
    # same rules inside the compare-and-set against the row as it is when the write
    # lands, so a recipe another host adds during validation survives (N13 P7.4).
    keep = _merge_rank(load_recipes(dealer_id), candidates, max_recipes)
    validated: list[EndpointRecipe] = []
    if validate:
        from backend.scanner.recipe_validation import gate_recipes

        live = [r for r in keep if not r.stale]
        kept_live, report = gate_recipes(
            dealer_id, live, base_url=base_url, dealer_name=dealer_name or dealer_id,
            place=place, fetch=fetch, context="discovery capture", log_root=log_root,
        )
        if validation_out is not None:
            validation_out.update(report.summary())
            validation_out["status"] = report.status
            validation_out["recipes"] = [asdict(c) for c in report.recipes]
        if not kept_live:
            logger.warning("Recipes [%s]: capture refused by validation (%s); recipe file left as it was",
                           dealer_id, "; ".join(report.reasons)[:200])
            return 0
        for r, chk in zip(live, report.recipes):
            if chk.vins:
                r.vehicle_rows = max(int(r.vehicle_rows or 0), int(chk.vins))
                r.last_ok_at = now
                if chk.site_total and not r.total_count:
                    r.total_count = int(chk.site_total)
                validated.append(r)
    merged = merge_recipes(dealer_id, candidates, max_recipes=max_recipes)
    for r in validated:
        # The merge carries last_ok_at over from the stored recipe; the validation's
        # success is recorded on its own key afterwards.
        record_replay_ok(
            dealer_id, r.key(), last_ok_at=now, field_coverage=dict(r.field_coverage or {}),
            unstale=False, total_count=r.total_count,
        )
    thin = sum(1 for r in merged if r.field_coverage and not recipe_coverage_is_rich(r.field_coverage))
    logger.info(
        "Recipes [%s]: %d endpoint(s) promoted (%d candidate(s) this scan, %d thin: VINs without price/trim/colour)",
        dealer_id, len(merged), len(candidates), thin,
    )
    return len(merged)


# ── Replay ───────────────────────────────────────────────────────────────────


def recipe_fetch_enabled() -> bool:
    raw = (os.environ.get("SCANNER_RECIPE_FETCH") or "").strip().lower()
    return raw not in ("0", "false", "no", "off")


def recipe_min_vehicles() -> int:
    try:
        return max(1, int(os.environ.get("SCANNER_RECIPE_MIN_VEHICLES") or 10))
    except ValueError:
        return 10


# How far a replay walk goes (D-RC3): to the site's own total, capped. The cap is
# SCANNER_REPLAY_MAX_PAGES (default 150); a walk with no usable total keeps the old 40.
_REPLAY_PAGES_HARD_CAP = 150
_REPLAY_PAGES_NO_TOTAL = 40


def replay_max_pages() -> int:
    try:
        return max(1, int(os.environ.get("SCANNER_REPLAY_MAX_PAGES") or _REPLAY_PAGES_HARD_CAP))
    except ValueError:
        return _REPLAY_PAGES_HARD_CAP


def _is_html_walk(recipe: "EndpointRecipe") -> bool:
    """Server-rendered pagination: the total frozen into the recipe at synth time says
    nothing about today's lot, so only an on-page count may stop such a walk."""
    return recipe.pagination in (PAGINATION_DEP_SRP, PAGINATION_HTML_PAGE, PAGINATION_JAZEL_SRP) or (
        recipe.provider_hint == "html_cards"
    )


def _walk_pages(total: int | None, page_size: int, hard_cap: int) -> int:
    """Pages to walk once page 1 is read: ``min(hard_cap, ceil(total / page_size) + 1)``
    when both are known, else the no-total default (never above the hard cap)."""
    if total and page_size > 0:
        return min(hard_cap, -(-int(total) // page_size) + 1)
    return min(hard_cap, _REPLAY_PAGES_NO_TOTAL)
_REPLAY_TIMEOUT_S = 20.0


# facetFilters keys that select an SRP section (condition / make / model / body),
# never a rooftop; a whole-lot replay must not carry them
_CC_SECTION_FACETS = frozenset({"type_slug", "type", "make", "model_slug", "model", "body_type", "trim_slug", "year",
                                "fuel_type", "transmission", "exterior_color_generic", "features", "certified",
                                "special_use_category", "condition", "in_transit", "is_special", "drivetrain", "price_range"})


def _learn_dealer_com_page_size(template: Any, parsed: Any, dealer_name: str = "") -> int:
    """Copy the page size the dealer.com server actually used into the walk's
    template. The captured preferences say pageSize 500; the server answers 48
    and ignores the preference, so page 2 started at 500 — past the end — and 14
    dealers of the 2026-09-24 fleet run stopped at exactly 48 rows (Village VW:
    157 cars, Mtn View Ford: 436). Returns the size learned, or 0."""
    if not isinstance(template, dict) or not isinstance(parsed, dict):
        return 0
    info = parsed.get("pageInfo")
    try:
        real = int((info or {}).get("pageSize") or 0) if isinstance(info, dict) else 0
    except (TypeError, ValueError):
        real = 0
    if real <= 0:
        return 0
    prefs = template.setdefault("preferences", {})
    if isinstance(prefs, dict) and str(prefs.get("pageSize") or "") != str(real):
        logger.info("Recipe [%s] dealer.com pageSize %s -> %d (the server's)", dealer_name, prefs.get("pageSize"), real)
        prefs["pageSize"] = str(real)
    return real


def recipe_is_section_scoped(recipe: EndpointRecipe) -> bool:
    """True for a captured CarsCommerce recipe whose facetFilters pin one SRP
    section (used-only / new-only / one make): it can never yield the lot."""
    if recipe.pagination != PAGINATION_CARSCOMMERCE or not recipe.post_template:
        return False
    try:
        body = json.loads(recipe.post_template)
    except ValueError:
        return False
    ff = body.get("facetFilters") if isinstance(body, dict) else None
    return isinstance(ff, dict) and any(k in _CC_SECTION_FACETS for k in ff)


def recipe_is_store_scoped(recipe: EndpointRecipe, dealer_name: str = "") -> bool:
    """True when the request itself selects this store, so every row it returns
    is this store's by construction: CarsCommerce ``facetFilters.source_id``
    (the store's own feed ids), or a single facet value that IS the store's name
    (the site's Location facet, "Group 1 Toyota North Austin"). Both are set and
    replay-verified by ``recipe_synth._carscommerce_store_filter``; the API
    rejects unknown body keys, so the scope cannot be marked any other way."""
    if recipe.pagination != PAGINATION_CARSCOMMERCE or not recipe.post_template:
        return False
    try:
        body = json.loads(recipe.post_template)
    except ValueError:
        return False
    ff = body.get("facetFilters") if isinstance(body, dict) else None
    if not isinstance(ff, dict):
        return False
    ids = ff.get("source_id")
    if isinstance(ids, list) and ids:
        return True
    nrm = lambda s: re.sub(r"[^a-z0-9]", "", str(s or "").lower())  # noqa: E731
    own = {nrm(dealer_name), nrm(re.sub(r"-(com|net|org)$", "", recipe.dealer_id or ""))} - {""}
    for facet, vals in ff.items():
        if facet.startswith("custom_text_") and isinstance(vals, list) and len(vals) == 1 and nrm(vals[0]) in own:
            return True
    return False


def _mutate_for_page(recipe: EndpointRecipe, template: Any, page_index: int) -> Any:
    """Return the request body for 0-based *page_index* per the recipe's pagination shape."""
    body = copy.deepcopy(template)
    if recipe.pagination == PAGINATION_CARSCOMMERCE and isinstance(body, dict):
        body["page"] = page_index + 1
        # The browser often captures a widget's query, not the lot: Culver City
        # Toyota's recipe carried facetFilters {"custom_text_1": ["true"]} (a
        # "featured" flag) and replayed 30 of 529 cars. Boolean facet flags narrow
        # to a carousel; drop them. Section filters (type_slug etc.) live under
        # "filters" and are kept. perPage 100 is accepted and cuts the walk 5x.
        ff = body.get("facetFilters")
        if isinstance(ff, dict):
            # A browser capture is one SRP SECTION's query: 16 of the 44 no_rows
            # verdicts of the 2026-09-24 fleet run were carscommerce recipes with
            # facetFilters type_slug=["Certified Used"] / ["New"] + make + model_slug
            # (Lindsay Honda 109 used of 577, Germain Toyota 65 new of 401). Those
            # keys describe inventory sections, never the store; drop them. Store
            # identity keys (source_id, custom_text_N) are kept.
            kept = {
                k: v for k, v in ff.items()
                if k not in _CC_SECTION_FACETS
                and not (isinstance(v, list) and v and all(str(x).lower() in ("true", "false", "1", "0") for x in v))
            }
            if kept:
                body["facetFilters"] = kept
            else:
                body.pop("facetFilters", None)
        flt = body.get("filters")
        if isinstance(flt, dict) and any(k in _CC_SECTION_FACETS for k in flt):
            # same section keys under "filters" (a used-SRP capture writes
            # filters.type_slug=["used"]); "status" stays
            body["filters"] = {k: v for k, v in flt.items() if k not in _CC_SECTION_FACETS}
        try:
            if int(body.get("perPage") or 0) < 100:
                body["perPage"] = 100
        except (TypeError, ValueError):
            body["perPage"] = 100
    elif recipe.pagination == PAGINATION_TYPESENSE and isinstance(body, dict):
        for s in body.get("searches") or []:
            if isinstance(s, dict):
                s["page"] = page_index + 1
    elif recipe.pagination == PAGINATION_DEALER_COM and isinstance(body, dict):
        prefs = body.get("preferences") or {}
        # Do NOT strip preferences["listing.config.id"]: without it dealer.com
        # returns the lot but omits the VIN from every item (Lexus of Austin: 48
        # parsed as-is, 0 stripped; three no_rows verdicts on the 2026-09-26 mini
        # check). Camelback Toyota's certified-only config stays open (its widget
        # carries no VIN either way; workspace/pipeline/needs_work.txt).
        try:
            page_size = int(str(prefs.get("pageSize") or 20))
        except (TypeError, ValueError):
            page_size = 20
        params = body.setdefault("inventoryParameters", {})
        if isinstance(params, dict):
            params["start"] = [str(page_index * page_size)]
    elif recipe.pagination == PAGINATION_ALGOLIA and isinstance(body, dict):
        # Algolia pages are 0-based (page 0 is the first page of results).
        body["page"] = page_index
    elif recipe.pagination == PAGINATION_GRAPHQL_OFFSET and isinstance(body, dict):
        # OneAudi: {"query": ..., "variables": {"sp": {"paging": {"limit": 48, "offset": N}}}}
        sp = (body.get("variables") or {}).get("sp")
        if isinstance(sp, dict):
            paging = sp.setdefault("paging", {})
            if isinstance(paging, dict):
                try:
                    limit = int(paging.get("limit") or 48)
                except (TypeError, ValueError):
                    limit = 48
                paging["limit"] = limit
                paging["offset"] = page_index * limit
    return body


_HTML_PAGE_PARAMS = ("page", "inv_page", "paged", "pg", "p")


def _html_page_param(query: str) -> str:
    """Name of the page parameter an HTML pager URL already carries, else ``page``."""
    keys = [k for k, _ in parse_qsl(query or "", keep_blank_values=True)]
    for cand in _HTML_PAGE_PARAMS:
        if cand in keys:
            return cand
    return "page"


def html_page_section_key(url: str) -> str:
    """Query string of an HTML-page recipe minus its page parameter: the section
    (PixelMotion ``condition[]=new`` vs ``used``) that one endpoint path serves."""
    parts = urlparse(url)
    page_param = _html_page_param(parts.query)
    return urlencode(sorted((k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if k != page_param and k != "newUrl"))


def _url_for_page(recipe: EndpointRecipe, page_index: int) -> str:
    """Per-page request URL for 0-based *page_index*.

    ``PAGINATION_PAGE_QUERY`` / ``PAGINATION_HTML_PAGE`` set ``?page=N`` and
    ``PAGINATION_DEP_SRP`` sets ``?p=N`` (all 1-based); ``PAGINATION_JAZEL_SRP``
    walks a path segment ``.../srp-page-N/`` (page 1 is the bare SRP URL); every
    other pagination shape paginates via the request body, so the URL is returned
    unchanged.
    """
    if recipe.pagination == PAGINATION_JAZEL_SRP:
        # Page 1 is the bare all-vehicles SRP; page N>=2 is .../srp-page-N/.
        if page_index == 0:
            return recipe.url
        base = recipe.url.rstrip("/")
        return f"{base}/srp-page-{page_index + 1}/"
    if recipe.pagination not in (PAGINATION_PAGE_QUERY, PAGINATION_DEP_SRP, PAGINATION_HTML_PAGE, PAGINATION_COSMOS_PT):
        return recipe.url
    page_param = {PAGINATION_DEP_SRP: "p", PAGINATION_COSMOS_PT: "pt"}.get(recipe.pagination, "page")
    parts = urlparse(recipe.url)
    if recipe.pagination == PAGINATION_HTML_PAGE:
        # The captured URL names its own page parameter when the pager is an
        # XHR (PixelMotion: inv_page=2, newUrl=/inventory/new/?inv_page=2);
        # appending ?page=N to that URL returns page 1 forever (mcpeeks-com).
        page_param = _html_page_param(parts.query)
    drop = {page_param, "pn"} if recipe.pagination == PAGINATION_COSMOS_PT else {page_param}
    query = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if k not in drop]
    if recipe.pagination == PAGINATION_HTML_PAGE:
        query = [(k, re.sub(r"(?<=[?&]){}=\d+".format(re.escape(page_param)), f"{page_param}={page_index + 1}", v)) for k, v in query]
    query.append((page_param, str(page_index + 1)))
    if recipe.pagination == PAGINATION_COSMOS_PT:
        # pn = page size, 96 is the server maximum (verified 2026-09-23 on Cherokee
        # County Toyota: 282 cars = 3 pages of 96 instead of 24 of 12).
        query.append(("pn", "96"))
    return urlunparse(parts._replace(query=urlencode(query)))


# Statuses a WAF returns when it dislikes the TLS handshake rather than the request.
# Cloudflare and Akamai use 403; DataDome and some Akamai configs use 405/429.
_TLS_FINGERPRINT_STATUSES = net_client.TLS_FINGERPRINT_STATUSES


def _replay_proxy_kwargs() -> dict[str, Any]:
    """``{"proxies": ...}`` when SCANNER_HTTP_PROXY (or HTTPS_PROXY/HTTP_PROXY) is set, else
    nothing. Replay is the main feed path, so it routes the same way as every other scanner
    fetcher (it used to ignore the scanner proxy; audit F-18, owner decision 2026-10-05)."""
    proxies = net_client.scanner_proxies()
    return {"proxies": proxies} if proxies else {}


def _replay_impersonated(
    recipe: EndpointRecipe, req_url: str, headers: dict[str, str], payload: str | None
) -> tuple[int, Any | None]:
    """
    Replay one request with a real browser's TLS fingerprint.

    Returns ``(0, None)`` when curl_cffi is not installed, so this stays a pure
    enhancement: without it the caller keeps the status the plain client already got.

    Profiles are rotated because edges run different rule sets -- across a 30-dealer
    sample "chrome" alone cleared about half while chrome/chrome124/safari17_0 together
    cleared 29 of 30.
    """
    cffi_requests = net_client.curl_cffi_requests()
    if cffi_requests is None:
        return 0, None

    send_kwargs: dict[str, Any] = {"headers": headers, **_replay_proxy_kwargs()}
    if recipe.method != "GET":
        send_kwargs["data"] = payload

    def _failed(profile: str, exc: Exception) -> None:
        logger.debug("impersonated replay %s failed: %s", profile, str(exc)[:120])

    got = net_client.rotate_impersonation(
        cffi_requests, req_url, method=recipe.method, via_get=recipe.method == "GET",
        profiles=net_client.IMPERSONATE_PROFILES,
        accept=lambda r: r.status_code == 200, on_error=_failed,
        timeout=_REPLAY_TIMEOUT_S, **send_kwargs,
    )
    if got.response is not None:
        resp, profile = got.response, got.profile
        logger.info("recipe replay cleared via TLS impersonation (%s): %s", profile, req_url[:70])
        if recipe.pagination in (PAGINATION_DEP_SRP, PAGINATION_HTML_PAGE, PAGINATION_JAZEL_SRP) or recipe.provider_hint == "html_cards":
            return resp.status_code, resp.text or None
        try:
            parsed = resp.json()
        except Exception:
            return resp.status_code, None
        return resp.status_code, parsed if isinstance(parsed, (dict, list)) else None
    return 0, None


def _replay_request(
    recipe: EndpointRecipe, body: Any, base_url: str, url: str | None = None
) -> tuple[int, Any | None]:
    """One synchronous HTTP call. Returns (status, parsed_json_or_None).

    *url* overrides ``recipe.url`` for this call (used to walk ``?page=N``
    pagination); defaults to ``recipe.url``.
    """
    import requests

    req_url = url or recipe.url
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Origin": base_url.rstrip("/"),
        "Referer": base_url.rstrip("/") + "/",
        **recipe.auth_headers,
    }
    if recipe.method != "GET":
        headers["Content-Type"] = "application/json"
    payload = json.dumps(body) if body is not None else None

    try:
        if recipe.method == "GET":
            resp = net_client.send(
                requests, "GET", req_url, via_get=True, headers=headers, timeout=_REPLAY_TIMEOUT_S,
                **_replay_proxy_kwargs(),
            )
        else:
            resp = net_client.send(
                requests, recipe.method, req_url, via_get=False, headers=headers,
                data=payload, timeout=_REPLAY_TIMEOUT_S, **_replay_proxy_kwargs(),
            )
    except requests.RequestException as e:
        logger.debug("Recipe replay request failed (%s): %s", recipe.url[:80], str(e)[:150])
        return _replay_impersonated(recipe, req_url, headers, payload)

    if net_client.is_fingerprint_block(resp.status_code, _TLS_FINGERPRINT_STATUSES):
        # The edge rejected the handshake, not the request. Retry with a real browser's
        # TLS signature before treating this as a dead recipe.
        #
        # This is the seam that made previously-saved recipes useless: impersonation had
        # been added to the synthesis and validation helpers, so a recipe would be built,
        # validated against live inventory and stored -- and then 403 the first time a
        # real scan replayed it here, which also flips it stale. Recipe capture and recipe
        # replay have to clear the same edge.
        status, parsed = _replay_impersonated(recipe, req_url, headers, payload)
        if status == 200:
            return status, parsed
        return resp.status_code, None

    if resp.status_code != 200:
        return resp.status_code, None
    # Some platforms serve inventory as server-rendered HTML (no JSON API): Dealer
    # eProcess / nabthat embed per-vehicle JSON-LD, Overfuel embeds __NEXT_DATA__,
    # and Jazel embeds per-card jzlSetVehicleInfoContext() calls. Hand the raw HTML
    # text to the provider parser instead of attempting resp.json().
    if recipe.pagination in (PAGINATION_DEP_SRP, PAGINATION_HTML_PAGE, PAGINATION_JAZEL_SRP) or recipe.provider_hint == "html_cards":
        return resp.status_code, resp.text or None
    try:
        parsed = resp.json()
    except ValueError:
        return resp.status_code, None
    return resp.status_code, parsed if isinstance(parsed, (dict, list)) else None


def _unique_vins(vehicles: list[dict]) -> set[str]:
    return {v.get("vin", "").strip().upper() for v in vehicles if (v.get("vin") or "").strip()}


# Fields a recipe replay must actually carry before it is allowed to REPLACE the
# browser scrape. VIN count alone is the wrong yardstick: on 2026-08-04 a fleet
# capture-only run accepted endpoints like Team Velocity's GetKeyFeaturesByVins
# and Gatsby page-data blobs because they listed VINs, then the 70%-of-known-VINs
# rule suppressed the SRP scrape, and 67k rows landed with no trim, price or
# colour. Every one of those fields lives on the VDP for those platforms.
_COVERAGE_FIELDS = ("price", "trim", "exterior_color")


def _field_present(v: dict, field: str) -> bool:
    val = v.get(field)
    if val is None:
        return False
    if field == "price":
        try:
            return float(val) > 0
        except (TypeError, ValueError):
            return False
    return bool(str(val).strip())


def recipe_field_coverage(vehicles: list[dict]) -> dict[str, float]:
    """Fraction of parsed vehicles carrying each of ``_COVERAGE_FIELDS``.

    ``{"n": <vehicle count>, "price": 0.0-1.0, "trim": ..., "exterior_color": ...}``.
    An empty list yields ``n=0`` and zero coverage everywhere.
    """
    n = len(vehicles)
    out: dict[str, float] = {"n": float(n)}
    for f in _COVERAGE_FIELDS:
        out[f] = (sum(1 for v in vehicles if _field_present(v, f)) / n) if n else 0.0
    return out


def recipe_min_price_coverage() -> float:
    try:
        return min(1.0, max(0.0, float(os.environ.get("SCANNER_RECIPE_MIN_PRICE_COVERAGE") or 0.5)))
    except ValueError:
        return 0.5


def recipe_min_field_coverage() -> float:
    try:
        return min(1.0, max(0.0, float(os.environ.get("SCANNER_RECIPE_MIN_FIELD_COVERAGE") or 0.5)))
    except ValueError:
        return 0.5


def recipe_coverage_is_rich(coverage: dict[str, float] | None) -> bool:
    """True when a recipe's rows look like an inventory feed, not a VIN list."""
    cov = coverage or {}
    price = float(cov.get("price", 0.0))
    listing = max(float(cov.get("trim", 0.0)), float(cov.get("exterior_color", 0.0)))
    return price >= recipe_min_price_coverage() and listing >= recipe_min_field_coverage()


def recipe_yield_replaces_browser(
    recipe_vins: int, known_vins: int, coverage: dict[str, float] | None
) -> tuple[bool, str]:
    """Decide whether a recipe replay may stand in for the browser SRP scrape.

    Both bars must clear: VIN yield >= 70% of the last known lot (the historical
    rule), AND the parsed rows carry a price on at least
    ``SCANNER_RECIPE_MIN_PRICE_COVERAGE`` of vehicles and a trim OR exterior colour
    on at least ``SCANNER_RECIPE_MIN_FIELD_COVERAGE``. Returns ``(ok, reason)``;
    the reason is logged so a "partial" verdict says which bar failed.
    """
    if known_vins <= 0:
        return False, "no_known_lot"
    if recipe_vins < int(0.7 * known_vins):
        return False, f"vins {recipe_vins}/{known_vins} < 70%"
    cov = coverage or {}
    price = float(cov.get("price", 0.0))
    listing = max(float(cov.get("trim", 0.0)), float(cov.get("exterior_color", 0.0)))
    if price < recipe_min_price_coverage():
        return False, f"price coverage {price:.0%} < {recipe_min_price_coverage():.0%}"
    if listing < recipe_min_field_coverage():
        return False, f"trim/colour coverage {listing:.0%} < {recipe_min_field_coverage():.0%}"
    return True, "ok"


def last_known_vin_count(dealer_id: str) -> int:
    """Distinct VINs currently LISTED for *dealer_id* (0 on any failure).

    Active rows only: counting retired rows too made the 70% browser-skip gate
    unreachable right after a reconcile (Hiley VW 2026-09-22: 338 feed VINs vs
    "624 known" of which 286 had just been unlisted), so a rich feed re-ran the
    browser and every store's gate drifted with its sales history."""
    try:
        from backend.db.inventory_pg import is_inventory_postgres, pg_connect

        if not is_inventory_postgres():
            return 0
        conn = pg_connect()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT count(DISTINCT vin) FROM cars WHERE dealer_id = %s "
                "AND listing_removed_at IS NULL AND COALESCE(listing_active, 1) = 1",
                (dealer_id,),
            )
            return int(cur.fetchone()[0])
        finally:
            conn.close()
    except Exception:
        return 0


def _persist_replay_success(dealer_id: str, recipe: EndpointRecipe, retrying_stale: bool) -> None:
    """Write a successful replay back to the dealer's stored set through
    ``record_replay_ok``: the recipe's ``last_ok_at``, measured ``field_coverage`` and
    learned ``total_count`` (on its own key only), and, when a stale recipe answered
    again, its un-stale.

    Blocking store I/O (the compare-and-set write and the cache file), so
    ``try_fetch_via_recipes`` runs it with ``asyncio.to_thread``; it never runs on the
    event loop.
    """
    record_replay_ok(
        dealer_id, recipe.key(), last_ok_at=float(recipe.last_ok_at or time.time()),
        field_coverage=dict(recipe.field_coverage or {}), unstale=bool(retrying_stale),
        total_count=recipe.total_count,
    )


# Walk-report ends that mean the recipe saw the whole of what it asked for.
_COMPLETE_ENDS = frozenset({"single_page", "total_reached", "exhausted", "no_new_rows", "short_page"})
_COMPLETE_TOTAL_FRACTION = 0.98


def recipe_scope_hash(recipe: EndpointRecipe, dealer_name: str = "") -> str:
    """Short hash of what a recipe's request selects (endpoint, section, store scope,
    CarsCommerce facets), never the per-page cursor. Two scans whose walks carry the
    same hash asked the site for the same slice of the lot, so their VIN sets are
    comparable; a changed hash means the slice changed under the dealer."""
    facets = ""
    if recipe.post_template:
        try:
            body = json.loads(recipe.post_template)
        except ValueError:
            body = None
        ff = body.get("facetFilters") if isinstance(body, dict) else None
        if isinstance(ff, dict):
            facets = json.dumps(ff, sort_keys=True, default=str)
    method, path = recipe.key()
    raw = "|".join((
        method, path, recipe.pagination, facets,
        "store" if recipe_is_store_scoped(recipe, dealer_name) else "",
        "section" if recipe_is_section_scoped(recipe) else "",
    ))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12]


def _walk_complete(end: str, raw_vins: int, total_count: int | None) -> bool:
    """With a known site total, a walk is complete when it saw >= 98% of it,
    whatever stopped it (an 'exhausted' 48 of 500 is not complete, and a
    'page_cap' walk that reached the total is). Without one, only a walk that
    ended by running out of rows, or by being one page, is."""
    if total_count and total_count > 0:
        return raw_vins >= _COMPLETE_TOTAL_FRACTION * total_count
    return end in _COMPLETE_ENDS


def _walk_entry(
    recipe: EndpointRecipe, dealer_name: str, stale_retry: bool, end: str, pages: int, raw: int, kept: int,
    total: Any = ...,
) -> dict:
    method, path = recipe.key()
    if total is ...:
        total = recipe.total_count
    return {
        "key": f"{method} {path}",
        "url": recipe.url,
        "stale_retry": bool(stale_retry),
        "scope_hash": recipe_scope_hash(recipe, dealer_name),
        "pages": pages,
        "raw_vins": raw,
        "kept_vins": kept,
        "total_count": total,
        "end": end,
        "complete": _walk_complete(end, raw, total),
        "in_union": False,
    }


def _finalize_replay(replay_out: dict | None, *, union_recipes: int | None = None) -> None:
    """Fold the per-recipe walk entries into the summary the run result carries:
    ``truncated`` (a walk the scan was meant to rely on stopped before the site's
    total), ``capped_recipes`` (walks cut by the page cap) and ``source``."""
    if replay_out is None:
        return
    walks = replay_out.get("recipes") or []
    # A retried stale recipe that never answered is not part of the lot this scan
    # was expected to deliver, so its silence does not make the lot incomplete.
    replay_out["truncated"] = any(
        not w["complete"] and not (w["stale_retry"] and w["raw_vins"] == 0) for w in walks
    )
    replay_out["capped_recipes"] = sum(1 for w in walks if w["end"] == "page_cap")
    replay_out["source"] = "recipe"
    if union_recipes is not None:
        replay_out["union_recipes"] = union_recipes


async def try_fetch_via_recipes(
    dealer_id: str,
    provider: str,
    base_url: str,
    dealer_name: str,
    *,
    union: bool = False,
    coverage_out: dict[str, float] | None = None,
    registry_id: int | None = None,
    replay_out: dict | None = None,
) -> tuple[list[tuple[str, Any]], int] | None:
    """
    Replay this dealer's stored recipes over plain HTTP (pre-Playwright).

    ``registry_id`` is the manifest's roster row for this store; it picks the gate's
    own-store place for a store that shares its website host with sister stores.

    When ``coverage_out`` is a dict it is filled (in place) with
    :func:`recipe_field_coverage` over the parsed vehicles behind the returned
    records, so the caller can judge whether the replay is complete enough to
    REPLACE the browser scrape, not merely whether it listed enough VINs.

    Returns ``(records, unique_vin_count)`` — intercept-shaped ``[(url, body), ...]``
    plus the VIN yield — when a recipe returns at least ``recipe_min_vehicles()``
    unique parseable VINs; ``None`` otherwise. The caller decides whether the yield
    is complete enough to skip the browser scrape (a recipe captured on one listing
    config can be type-filtered and cover only part of the lot).
    401/403 responses mark the recipe stale so the browser pass re-captures auth.

    ``union=True`` replays EVERY non-stale recipe and returns the combined
    yield: a dealer's recipes are often per-listing-config (new / used / CPO
    pages), so the first passing recipe alone can cover only part of the lot.
    Delta scans want the union; the pre-browser scan fast path keeps the
    cheaper first-hit behavior.

    When ``replay_out`` is a dict it is filled (in place) with the walk report,
    one entry per tried recipe under ``recipes``: ``{key, url, stale_retry,
    scope_hash, pages, raw_vins, kept_vins, total_count, end, complete, in_union}``.
    ``raw_vins`` counts unique VINs the pages carried, kept plus refused by the
    rooftop gate. ``end`` is one of single_page, total_reached, no_new_rows,
    page_cap, http_<code>, fetch_failed, auth_dead, unparsed_mid, unreplayable;
    ``complete`` is :func:`_walk_complete`. ``in_union`` is separate from ``end``:
    a complete walk dropped for low yield is ``in_union=False`` and its kept VINs
    go to ``low_yield_vins``, so a caller can protect them. Beside the entries:
    ``truncated``, ``capped_recipes`` and ``source``. Reporting only; what the
    walk fetches and returns is the same with or without it.
    """
    # This store's postal address, for the rooftop gate.
    #
    # Without it, a group feed refuses EVERY row. That is not hypothetical: synthesising a
    # recipe for terrylabontechevy.com pulled 3,979 VINs -- HendrickCars.com's national
    # inventory -- and the gate logged "group feed names 48 rooftops (Buford GA, Cary NC,
    # Duluth GA, Durham NC, Greensboro NC, Merriam KS) and none is this store" on every
    # page. Terry Labonte Chevrolet IS the Greensboro NC store; the feed identifies its
    # rooftops by address block, not by name, so with no address to compare the gate could
    # not recognise the very dealer being scanned and threw the lot away.
    #
    # backend.parsers.parse says this explicitly -- "omit them on such a feed and every
    # page refuses every row" -- and delta_scan already passes them. This path did not, so
    # every dealer on a Hendrick / Sonic / Fletcher Jones / CarsCommerce group feed yielded
    # zero cars through recipe replay while looking like a healthy multi-thousand-VIN
    # recipe. Best-effort: a dealer missing from the registry falls back to the name and
    # host tiers, exactly as before.
    if not recipe_fetch_enabled():
        return None
    from backend.attribution import DealerCtx, mark_payload_feed_scoped, parse_page

    # The store place (backend.attribution.store_place: registry + page-learned
    # street) hits the DB — keep it off the event loop, and don't pay for it at
    # all when recipe fetch is disabled. Never raises (falls back to no place).
    attr_ctx = await asyncio.to_thread(DealerCtx.for_store, dealer_id, dealer_name, base_url, registry_id)
    # load_recipes may consult Postgres (recipe_store sync) — keep that
    # blocking I/O off the event loop this coroutine runs on.
    loaded = await asyncio.to_thread(load_recipes, dealer_id)
    live = [r for r in loaded if not r.stale]
    # Recipe lifecycle (HTTP_ONLY_SCANS_PLAN Phase 3): a recipe marked stale by
    # an earlier 401/403 is still tried once, AFTER the live ones — keys rotate
    # back, and a replay that answers again un-stales it. One page-1 request per
    # stale recipe per scan when the auth is still dead; nothing else.
    stale_retry = [r for r in loaded if r.stale and (r.method == "GET" or _parses_json(r.post_template))]
    if not live and not stale_retry:
        return None

    walks: list[dict] = []
    low_yield_vins: list[str] = []
    if replay_out is not None:
        replay_out["recipes"] = walks
        replay_out["low_yield_vins"] = low_yield_vins
    union_recipe_n = 0

    min_vehicles = recipe_min_vehicles()
    union_records: list[tuple[str, Any]] = []
    union_vins: set[str] = set()
    union_vehicles: list[dict] = []
    auth_dead_status: int | None = None
    succeeded_any = False

    def _finish_status() -> None:
        # One hint write per replay: a dealer with any working recipe is not
        # stale even when a sibling recipe's key died; a dealer whose every
        # tried recipe answered 401/403 is.
        if succeeded_any:
            clear_stale_status(dealer_id)
        elif auth_dead_status is not None:
            record_stale_status(dealer_id, auth_dead_status)

    for recipe in live + stale_retry:
        retrying_stale = recipe.stale
        # Taken before page 1: a stale mark from this walk yields to a success another
        # host records on the key after this instant (mark_stale's observed_at).
        observed_at = time.time()
        template: Any = None
        if recipe.post_template:
            try:
                template = json.loads(recipe.post_template)
            except ValueError:
                template = None
        if recipe.method != "GET" and template is None:
            # Truncated/unparseable POST sample — can't replay safely. Mark it
            # stale so the next browser scan re-captures a full body instead of
            # this recipe silently never working.
            await asyncio.to_thread(
                mark_stale, dealer_id, recipe, "unreplayable_post_template", observed_at=observed_at,
            )
            walks.append(_walk_entry(recipe, dealer_name, retrying_stale, "unreplayable", 0, 0, 0))
            logger.info(
                "Recipe stale [%s] %s — POST template unparseable (truncated capture?); "
                "browser scan will re-capture",
                dealer_name, recipe.url[:80],
            )
            continue
        records: list[tuple[str, Any]] = []
        vins: set[str] = set()
        vehicles: list[dict] = []
        raw_vins: set[str] = set()
        hard_cap = replay_max_pages()
        pages = 1 if recipe.pagination == PAGINATION_NONE else hard_cap
        # The total this walk stops on. A JSON walk falls back to the recipe's stored one;
        # an HTML walk does not (the file's value was frozen at synth time).
        walk_total: int | None = None if _is_html_walk(recipe) else recipe.total_count
        auth_dead = False
        end: str | None = None
        page_i = -1
        while page_i + 1 < pages:
            page_i += 1
            body = _mutate_for_page(recipe, template, page_i) if template is not None else None
            page_url = _url_for_page(recipe, page_i)
            status, parsed = await asyncio.to_thread(_replay_request, recipe, body, base_url, page_url)
            if status in (401, 403):
                if not vins:
                    # Failed before collecting anything — the recipe's auth is dead.
                    if not egress_tag():
                        # A tagged (datacenter) host records blocked:<tag> in
                        # _finish_status instead; the shared recipe stays live.
                        await asyncio.to_thread(
                            mark_stale, dealer_id, recipe, f"http_{status}", observed_at=observed_at,
                        )
                    auth_dead_status = int(status)
                    logger.info(
                        "Recipe stale [%s] %s — HTTP %d (%s); discovery capture will re-learn it",
                        dealer_name, recipe.url[:80], status,
                        "still dead on retry" if retrying_stale else "auth rotated?",
                    )
                    auth_dead = True
                    end = "auth_dead"
                else:
                    end = f"http_{int(status)}"
                    # Mid-pagination — likely a rate limit / page boundary, not dead
                    # auth. Keep the VINs already collected instead of discarding them.
                    logger.info(
                        "Recipe [%s] %s — HTTP %d after %d page(s); keeping %d VIN(s)",
                        dealer_name, recipe.url[:80], status, page_i, len(vins),
                    )
                break
            if parsed is None:
                if not isinstance(status, int) or status <= 0:
                    end = "fetch_failed"
                elif status == 200:
                    end = "unparsed_mid"
                else:
                    end = f"http_{status}"
                break
            store_scoped = recipe_is_store_scoped(recipe, dealer_name)
            if store_scoped:
                # dealer_run re-parses these payloads later (recovery / final
                # row set) without knowing the recipe; the marker travels with
                # the payload so no later parse() re-gates the store's own feed
                # (Tutton CDJR: 346 -> 5 rows through that second gate, 2026-09-26)
                mark_payload_feed_scoped(parsed)
            # rejected_out is REQUIRED, not optional decoration. parse() ends with:
            #
            #     if rejected_out is None:
            #         return kept + rejected
            #
            # so a caller that omits it gets back the very rows the rooftop gate just
            # refused. This path omitted it, and the effect was invisible because the
            # refusals still logged: replaying terrylabontechevy.com printed FORTY
            # "refusing all 100 row(s)" warnings and returned 3,931 VINs -- Hendrick's
            # national inventory, filed under one Greensboro store. That is precisely the
            # bug that gave bmwofmurrieta-com 1,921 cars across 37 makes.
            #
            # Passing a list makes parse() return only `kept`, which is what every other
            # caller already does. parse_page (backend.attribution) always passes one.
            _refused: list[dict] = []
            page_vehicles = parse_page(
                recipe.provider_hint or provider, parsed, attr_ctx, _refused,
                trust_feed_scope=store_scoped,
            )
            new = _unique_vins(page_vehicles) - vins
            # The stop condition reads RAW rows: a page of nothing but refused siblings
            # (a group feed's other rooftops) holds no kept VIN yet is not the end.
            new_raw = (_unique_vins(page_vehicles) | _unique_vins(_refused)) - raw_vins
            raw_vins |= new_raw
            records.append((recipe.url, parsed))
            if page_i == 0 and recipe.pagination == PAGINATION_DEALER_COM:
                _learn_dealer_com_page_size(template, parsed, dealer_name)
            if page_i == 0:
                # The payload's own total wins over the file's: a stale total_count
                # (95 saved in August, lot now 106) stopped the walk after page 1.
                try:
                    from backend.scanner.recipe_totals import replay_site_total

                    _t = replay_site_total(recipe, parsed)
                    if _t and _t > 0:
                        recipe.total_count = walk_total = int(_t)
                except Exception:  # noqa: BLE001
                    pass
                if recipe.pagination != PAGINATION_NONE:
                    pages = _walk_pages(walk_total, len(raw_vins), hard_cap)
            vehicles.extend(v for v in page_vehicles if (v.get("vin") or "").strip().upper() in new)
            vins |= new
            if not new_raw:
                end = "single_page" if recipe.pagination == PAGINATION_NONE else "no_new_rows"
                break
            if walk_total and len(raw_vins) >= walk_total:
                end = "total_reached"
                break
        if end is None:
            end = "single_page" if recipe.pagination == PAGINATION_NONE else "page_cap"
        walk = _walk_entry(
            recipe, dealer_name, retrying_stale, end, len(records), len(raw_vins), len(vins), walk_total
        )
        walks.append(walk)
        if end == "page_cap" or (end.startswith(("http_", "unparsed", "fetch_failed")) and vins):
            logger.warning(
                "Recipe walk [%s] %s stopped at %s after %d page(s): %d raw / %d kept VIN(s), site total %s "
                "-- the lot is %s",
                dealer_name, recipe.url[:80], end, len(records), len(raw_vins), len(vins),
                walk["total_count"] or "unknown", "complete" if walk["complete"] else "NOT fully collected",
            )
        if auth_dead:
            continue
        if len(vins) >= min_vehicles:
            walk["in_union"] = True
            succeeded_any = True
            recipe.last_ok_at = time.time()
            recipe.field_coverage = {
                k: v for k, v in recipe_field_coverage(vehicles).items() if k != "n"
            }
            # File read + write and the dealer_recipes sync: off the event loop.
            await asyncio.to_thread(_persist_replay_success, dealer_id, recipe, retrying_stale)
            logger.info(
                "Recipe fetch [%s]: %d unique VIN(s) from %d page(s) via %s%s",
                dealer_name, len(vins), len(records), recipe.url[:80],
                " (stale recipe answered again; un-staled)" if retrying_stale else "",
            )
            if not union:
                if coverage_out is not None:
                    coverage_out.update(recipe_field_coverage(vehicles))
                await asyncio.to_thread(_finish_status)
                _finalize_replay(replay_out)
                return records, len(vins)
            union_recipe_n += 1
            union_records.extend(records)
            union_vehicles.extend(v for v in vehicles if (v.get("vin") or "").strip().upper() not in union_vins)
            union_vins |= vins
        elif walk["complete"]:
            # A whole walk the yield floor drops: its VINs are real listings the
            # caller must not read as missing (RET-5).
            low_yield_vins.extend(sorted(vins))
    await asyncio.to_thread(_finish_status)
    _finalize_replay(replay_out, union_recipes=union_recipe_n)
    if union and len(union_vins) >= min_vehicles:
        if coverage_out is not None:
            coverage_out.update(recipe_field_coverage(union_vehicles))
        logger.info(
            "Recipe fetch [%s]: %d unique VIN(s) combined across %d recipe replay(s), %d page(s)",
            dealer_name, len(union_vins), union_recipe_n, len(union_records),
        )
        return union_records, len(union_vins)
    return None


# ── Re-seeding a cache for another store (``--reseed``) ───────────────────────


@dataclass
class ReseedResult:
    cache_dir: Path
    identity: str
    fingerprint: str
    previous_tag: tuple[str, str | None]
    moved: list[str]
    backup_dir: Path | None
    dry_run: bool


def reseed_cache(*, cache_dir: Path | None = None, dry_run: bool = False) -> ReseedResult:
    """Make the cache dir a mirror of this process's store.

    Every dealer recipe file (``*.json`` without a leading underscore) is moved
    into ``<cache dir>/_reseed_backups/<UTC stamp>/``, then ``_store.json`` is
    rewritten for this process's store. The files move first and the tag is
    written last, so an interrupted re-seed leaves the old tag (push-ups still
    refused), never the new tag over old files. The cache refills from the store
    as dealers load (a missing file adopts the DB copy). ``_aliases.json`` and the
    ``vdp/`` dir stay: the alias map is this host's, and VDP recipes have no store
    mirror. Raises ``RuntimeError`` when this process uses no store.
    """
    d = RECIPES_DIR if cache_dir is None else cache_dir
    identity, unknown = _resolve_store_identity()
    if unknown:
        raise RuntimeError(
            "the recipe store's identity could not be worked out from INVENTORY_DATABASE_URL / "
            "DATABASE_URL; nothing to re-seed against"
        )
    if not identity:
        raise RuntimeError(
            "no recipe store in use (RECIPES_DB_DISABLED is on, or neither INVENTORY_DATABASE_URL "
            "nor DATABASE_URL names a Postgres store); nothing to re-seed against"
        )
    previous = read_store_tag(d)
    files = sorted(p for p in d.glob("*.json") if not p.name.startswith("_")) if d.is_dir() else []
    backup: Path | None = None
    if files:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        backup = d / RESEED_BACKUP_DIRNAME / stamp
    if not dry_run:
        if backup is not None:
            base, n = backup, 1
            while True:  # never mix two re-seeds' files in one backup dir
                try:
                    backup.mkdir(parents=True, exist_ok=False)
                    break
                except FileExistsError:
                    n += 1
                    backup = base.with_name(f"{base.name}-{n}")
            for p in files:
                os.replace(p, backup / p.name)
        write_store_tag(identity, source="reseed", cache_dir=d)
    return ReseedResult(d, identity, store_fingerprint(identity) or "", previous,
                        [p.name for p in files], backup, dry_run)


def main(argv: list[str] | None = None) -> int:
    """``python -m backend.scanner.recipes [--status [--require-match] | --reseed [--dry-run]]``."""
    import argparse

    from backend.utils.project_env import load_project_dotenv

    ap = argparse.ArgumentParser(
        prog="python -m backend.scanner.recipes",
        description="Show or re-seed the store tag (_store.json) of the recipe cache dir "
                    "(RECIPES_CACHE_DIR, else <repo>/workspace/recipes).",
    )
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--status", action="store_true",
                      help="print the cache dir, its tag and this process's store (default)")
    mode.add_argument("--reseed", action="store_true",
                      help="move the dealer recipe files into _reseed_backups/<stamp>/ and tag the "
                           "cache for this process's store; stop scanners on this cache first")
    ap.add_argument("--dry-run", action="store_true", help="with --reseed: report, change nothing")
    ap.add_argument("--require-match", action="store_true",
                    help="with --status: exit 1 unless the verdict is 'match' (a gate for scripts "
                         "that push the cache into the store)")
    args = ap.parse_args(argv)
    if args.dry_run and not args.reseed:
        ap.error("--dry-run only goes with --reseed")
    if args.require_match and args.reseed:
        ap.error("--require-match only goes with --status")
    # The same .env the scanner loads (shell values win), so the store matches a scan's.
    load_project_dotenv()

    if args.reseed:
        try:
            res = reseed_cache(dry_run=args.dry_run)
        except RuntimeError as exc:
            print(f"reseed refused: {exc}")
            return 2
        verb = "would move" if res.dry_run else "moved"
        prev_state, prev_fp = res.previous_tag
        prev = f"{prev_state} {prev_fp[:12]}" if prev_fp else prev_state
        print(f"recipe cache: {res.cache_dir}")
        print(f"previous tag: {prev}")
        print(f"{verb} {len(res.moved)} dealer recipe file(s)"
              + (f" to {res.backup_dir}" if res.backup_dir else ""))
        print(f"{'would tag' if res.dry_run else 'tagged'} for store {res.identity} "
              f"(fingerprint {res.fingerprint[:12]}); the cache refills from the store as dealers load")
        return 0

    check = check_cache_store()
    files = sorted(p for p in check.cache_dir.glob("*.json") if not p.name.startswith("_")) \
        if check.cache_dir.is_dir() else []
    print(f"recipe cache: {check.cache_dir} ({len(files)} dealer recipe file(s))")
    print(f"cache tag:    {check.tag_state}"
          + (f" {check.tag_fingerprint[:12]}" if check.tag_fingerprint else ""))
    this = check.identity or ("store identity unknown" if check.identity_unknown else "no recipe store")
    print(f"this process: {this}"
          + (f" (fingerprint {check.fingerprint[:12]})" if check.fingerprint else ""))
    print(f"verdict:      {check.verdict}")
    if check.foreign:
        return 1
    return 1 if args.require_match and check.verdict != "match" else 0


if __name__ == "__main__":
    # Load .env first, as backend/scanner/cli.py does before it imports this
    # module: RECIPES_DIR is resolved at import, and main() loads .env only after
    # that, so a RECIPES_CACHE_DIR that only .env sets would otherwise be missed
    # and --status / --reseed would act on the default cache, not the scanner's.
    # The dir is resolved again after the import in case something imported the
    # module before .env was loaded. Then run the imported module's main, not
    # this __main__ copy, so module state (RECIPES_DIR, the warn-once set) is
    # the one every other importer sees.
    from backend.utils.project_env import load_project_dotenv as _load_project_dotenv

    _load_project_dotenv()
    import backend.scanner.recipes as _recipes

    _recipes.RECIPES_DIR = _recipes.resolve_recipes_dir()
    raise SystemExit(_recipes.main())
