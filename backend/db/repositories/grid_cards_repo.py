"""Radius- and dealer-scoped listings grid, served from a persisted card store.

Owner decision 2026-09-28: the listings page no longer downloads the whole fleet.
``GET /api/listings/cars?zip=&radius=`` returns only the cars near the shopper, and
it must be fast. SQL can find the cars in ~100 ms, but the card serializer
(``serialize_car_for_listings_grid``) costs ~0.9 ms per row -- 47k cars around
92694 would be ~40 s of Python per request. So the serialized card is kept in
``listings_grid_cards`` (one row per car, shared by every worker and surviving
restarts) and a request only concatenates stored JSON.

Freshness, per car:

* ``row_ver`` is Postgres ``xmin``: it moves on every write to the ``cars`` row, so
  an unchanged ``xmin`` plus an unchanged ``aux_key`` (attribution verdict,
  public-incomplete flag, serializer revision) is a hit with no further work.
* When ``xmin`` moved (or off Postgres, where there is no ``xmin``) the full row is
  read and digested; an unchanged ``content_key`` is still a hit -- a scan that
  rewrites a row with identical values costs a digest, not a serialization.
* A changed or missing card is serialized. When only a few are affected (or the
  store holds too little of this scope to serve), that happens inline; otherwise
  the stored (stale) cards are served and a background thread refreshes them --
  the same stale-while-revalidate contract the old whole-fleet cache had.
* Cards older than :data:`CARD_MAX_AGE_DAYS`, and cards whose output depends on
  today's date (``price_drop_days_ago``), are served and refreshed in background.

``backend/scripts/build_listings_grid_cards.py`` fills the whole store offline (its
own process, never the web worker); run it after deploys that change the serializer
and after scans.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import queue
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Iterable

from backend.db.inventory_pg import is_inventory_postgres
from backend.db.repositories.base_repo import db_conn
from backend.db.repositories.schema_repo import LISTINGS_GRID_CAR_COLUMNS

_log = logging.getLogger(__name__)

# Bump when the card shape changes in a way the source hash below cannot see.
GRID_CARD_REV = 1

# Serializer sources whose text is folded into the card revision, so editing the
# card serializer invalidates every stored card without anyone remembering to bump
# GRID_CARD_REV. Anything else the card depends on needs a manual bump.
# ``path::name`` folds in only that top-level function of a large module.
_REV_SOURCES = (
    "backend/utils/car_serialize",
    "backend/utils/fuel_type_normalize.py",
    "backend/utils/field_clean.py",
    "backend/utils/interior_color_buckets.py",
    "backend/utils/mileage_display.py",
    "backend/utils/listings_sort.py",
    # deal_score: public_deal_score -> band lookup + scoring + plausibility gates.
    "backend/intelligence/deal_score_cache.py",
    "backend/intelligence/market_pricing.py",
    "backend/utils/market_price.py",
    "backend/utils/price_plausibility.py",
    # public_incomplete: the listings_repo wrapper around the card serializer.
    "backend/db/repositories/listings_repo.py::serialize_car_for_listings_grid",
    "backend/db/repositories/data_quality_repo.py::_car_is_publicly_incomplete",
)

CARD_MAX_AGE_DAYS = 1
# Hard cap on cards serialized inline by one request (~0.9 ms each, so ~1.4 s). A
# request never runs a full scope build: past the cap it serves what is stored,
# queues the rest for the background refresher and marks the response partial.
_INLINE_REBUILD_MAX = 1500
# When the store can serve at least this share of the scope (fresh + stale cards) and
# more than the cap changed, nothing is serialized inline: stale cards are served.
# Below it (fresh deploy, empty store) the cap is spent on cards with NO stored copy,
# nearest first, so the first screen fills in.
_MIN_SERVABLE_SHARE = 0.5
_ID_BATCH = 800
_BG_YIELD_ROWS = 100
_BG_YIELD_S = 0.002
_ACTIVE = "(COALESCE(c.listing_active, 1) = 1) AND COALESCE(c.marked_for_review, 0) = 0"
_FAR_PRICE = 1e18  # listing_price_value() returns inf; stored as a large finite number


def _function_source(text: bytes, name: str) -> bytes:
    """The text of top-level ``def name`` in *text* (to the next top-level statement)."""
    lines = text.split(b"\n")
    start = None
    for i, ln in enumerate(lines):
        if start is None:
            if ln.startswith(f"def {name}(".encode()):
                start = i
        elif ln and not ln[:1].isspace() and not ln.startswith(b")"):
            return b"\n".join(lines[start:i])
    return b"\n".join(lines[start:]) if start is not None else b""


def _compute_rev() -> str:
    root = Path(__file__).resolve().parents[3]
    h = hashlib.blake2b(f"rev{GRID_CARD_REV}".encode(), digest_size=8)
    for spec in _REV_SOURCES:
        rel, _, func = spec.partition("::")
        p = root / rel
        files = sorted(p.glob("*.py")) if p.is_dir() else [p]
        for f in files:
            try:
                data = f.read_bytes()
            except OSError:
                h.update(spec.encode())
                continue
            if func:
                data = _function_source(data, func) or spec.encode()
            h.update(data)
    return h.hexdigest()


CARD_REV = _compute_rev()


# ---------------------------------------------------------------------------
# Table
# ---------------------------------------------------------------------------

_table_ready = False
_table_lock = threading.Lock()

_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS listings_grid_cards (
    car_id       BIGINT PRIMARY KEY,
    row_ver      TEXT,
    content_key  TEXT NOT NULL,
    aux_key      TEXT NOT NULL,
    built_day    INTEGER NOT NULL,
    day_dep      INTEGER NOT NULL DEFAULT 0,
    sort_bucket  INTEGER NOT NULL DEFAULT 0,
    sort_price   DOUBLE PRECISION,
    card_json    TEXT NOT NULL
)
"""


def ensure_grid_cards_table() -> bool:
    """Create ``listings_grid_cards`` once per process (lazily, never at import)."""
    global _table_ready
    if _table_ready:
        return True
    with _table_lock:
        if _table_ready:
            return True
        try:
            with db_conn() as conn:
                conn.execute(_CREATE_SQL)
                conn.commit()
            _table_ready = True
        except Exception:
            _log.exception("listings_grid_cards unavailable; serializing inline")
            return False
    return True


def reset_grid_cards_state() -> None:
    """Tests: forget the per-process table flag and in-memory helpers."""
    global _table_ready, _fallback_urls_cache, _attr_cache, _token_cache
    global _market_gen_cache, _market_gen_last, _attr_prev_count, _attr_last_good
    _table_ready = False
    _attr_prev_count = None
    _attr_last_good = None
    _fallback_urls_cache = None
    _attr_cache = None
    _token_cache = None
    _market_gen_cache = None
    _market_gen_last = None


# ---------------------------------------------------------------------------
# Cache token (what the scoped response ETag / LRU are keyed by)
# ---------------------------------------------------------------------------

_token_cache: tuple[float, Any] | None = None
# Sample the write fingerprint at most this often: a running scan writes ``cars``
# continuously and would otherwise change the token on every request.
TOKEN_MIN_RESAMPLE_S = 60.0


def grid_scope_token() -> Any:
    """Data-generation token for scoped grid responses (+ the day, for date fields)."""
    global _token_cache
    now = time.monotonic()
    hit = _token_cache
    if hit is not None and (now - hit[0]) < TOKEN_MIN_RESAMPLE_S:
        return hit[1]
    from backend.db.repositories import listings_repo as lr

    from backend.db.repositories.data_quality_repo import listings_include_incomplete_cars

    fp = lr._pg_grid_write_fingerprint()
    base = fp if fp is not None else lr._listings_cache_token()
    # Everything a card depends on beyond the ``cars`` row (see ``_aux_key``), so a
    # body or ETag keyed by this token moves when any of it does.
    token = (
        base,
        _today(),
        CARD_REV,
        market_bands_generation(),
        attribution_generation(),
        listings_include_incomplete_cars(),
    )
    if fp is not None:
        # Off Postgres the legacy token is an mtime pair that tests move by writing;
        # only the Postgres fingerprint is expensive enough to throttle.
        _token_cache = (now, token)
    return token


def _today() -> int:
    return int(time.time() // 86400)


# ---------------------------------------------------------------------------
# Per-car inputs that live outside the ``cars`` row
# ---------------------------------------------------------------------------

_attr_cache: tuple[float, dict[int, dict[str, Any]], str] | None = None
_ATTR_TTL_S = 60.0
# Last successful read (served while a read fails) and its size (drop detection).
_attr_last_good: tuple[dict[int, dict[str, Any]], str] | None = None
_attr_prev_count: int | None = None


def _attr_generation(states: dict[int, dict[str, Any]]) -> str:
    blob = repr(sorted((k, _attr_key(v)) for k, v in states.items())).encode("utf-8", "surrogatepass")
    return hashlib.blake2b(blob, digest_size=8).hexdigest()


def _attribution_read() -> tuple[dict[int, dict[str, Any]], str, bool]:
    """(verdicts, generation, ok). ``ok`` is False when the read failed: the last good
    verdicts are returned (``{}`` if there were none), the failure is NOT cached, and
    callers must not treat stored cards as changed on attribution grounds."""
    global _attr_cache, _attr_last_good, _attr_prev_count
    now = time.monotonic()
    hit = _attr_cache
    if hit is not None and (now - hit[0]) < _ATTR_TTL_S:
        return hit[1], hit[2], True
    from backend.db.repositories.cars_repo import car_attribution_states

    err: BaseException | None = None
    try:
        states = car_attribution_states(fail_open=False)
    except Exception as exc:
        err = exc
        msg = str(exc)
        if "car_attribution" in msg and ("no such table" in msg or "does not exist" in msg):
            states = {}  # a database that never had verdicts (fresh dev / test db)
        else:
            states = None
    if states is None:
        last = _attr_last_good
        _log.warning(
            "car_attribution read failed; keeping stored grid cards for this pass "
            "(last good read had %s verdicts)",
            len(last[0]) if last else "no",
            exc_info=err,
        )
        if last is not None:
            return last[0], last[1], False
        return {}, "unread", False
    if not states and _attr_prev_count:
        # A real (successful) read with zero verdicts where the last one had
        # thousands: every card rebuilt from here on ships with no location caveat.
        _log.warning(
            "attribution overlay returned no verdicts but the previous read had %d; "
            "grid cards rebuilt now carry no location caveats",
            _attr_prev_count,
        )
    _attr_prev_count = len(states)
    gen = _attr_generation(states)
    _attr_last_good = (states, gen)
    _attr_cache = (now, states, gen)
    return states, gen, True


def _attribution_all() -> dict[int, dict[str, Any]]:
    return _attribution_read()[0]


def attribution_generation() -> str:
    """Digest of the current attribution verdicts (the last good one while reads fail)."""
    return _attribution_read()[1]


def _attr_key(state: dict[str, Any] | None) -> str:
    if not state:
        return "-"
    return repr(
        (
            state.get("status"),
            state.get("observed_rooftop"),
            state.get("location_unconfirmed"),
            state.get("group_feed"),
        )
    )


# Market-band generation: ``deal_score`` on every card reads ``market_price_stats``
# (nightly step 6 recomputes it). Folded into every card's aux_key, so a recompute
# invalidates the cards before step 7 rebuilds them.
_market_gen_cache: tuple[float, str] | None = None
_market_gen_last: str | None = None
_MARKET_GEN_TTL_S = 60.0


def market_bands_generation() -> str:
    """``max(computed_at)|count`` of ``market_price_stats`` ("-" when there is none).

    A failed read keeps the last known generation (and is not cached), so a DB hiccup
    never looks like a recompute and invalidates the whole store. When the generation
    moves, the in-process deal-score band cache is reloaded first, so a card rebuilt
    for the new generation is scored against the new bands."""
    global _market_gen_cache, _market_gen_last
    now = time.monotonic()
    hit = _market_gen_cache
    if hit is not None and (now - hit[0]) < _MARKET_GEN_TTL_S:
        return hit[1]
    try:
        with db_conn() as conn:
            row = conn.execute(
                "SELECT MAX(computed_at), COUNT(*) FROM market_price_stats"
            ).fetchone()
        gen = f"{row[0]}|{row[1]}" if row is not None else "-"
    except Exception as exc:
        msg = str(exc)
        if "market_price_stats" in msg and ("no such table" in msg or "does not exist" in msg):
            gen = "-"
        else:
            _log.warning(
                "market_price_stats generation read failed; keeping %r", _market_gen_last, exc_info=True
            )
            return _market_gen_last if _market_gen_last is not None else "-"
    if _market_gen_last is not None and gen != _market_gen_last:
        try:
            from backend.intelligence.deal_score_cache import refresh_cache

            refresh_cache()
        except Exception:
            _log.warning("deal-score band cache reload failed", exc_info=True)
    _market_gen_last = gen
    _market_gen_cache = (now, gen)
    return gen


def _aux_key(ctx: "_Ctx", pub_incomplete: bool, state: dict[str, Any] | None) -> str:
    raw = (
        f"{CARD_REV}|{1 if pub_incomplete else 0}|{_attr_key(state)}"
        f"|inc{1 if ctx.include_incomplete else 0}|mkt{ctx.market_gen}"
    )
    return hashlib.blake2b(raw.encode("utf-8", "surrogatepass"), digest_size=8).hexdigest()


def _content_key(row: dict[str, Any]) -> str:
    blob = repr(tuple(row.get(c) for c in LISTINGS_GRID_CAR_COLUMNS)).encode(
        "utf-8", "surrogatepass"
    )
    return hashlib.blake2b(blob, digest_size=16).hexdigest()


class _Ctx:
    """Everything a scope build needs that is the same for every car in it."""

    def __init__(self) -> None:
        from backend.db.repositories.data_quality_repo import (
            _incomplete_index_snapshot_for_listings,
            listings_include_incomplete_cars,
        )

        self.include_incomplete = listings_include_incomplete_cars()
        self.snapshot = _incomplete_index_snapshot_for_listings()
        self.attribution, _gen, self.attribution_ok = _attribution_read()
        self.market_gen = market_bands_generation()
        self.today = _today()

    def pub_incomplete(self, car_id: int, row: dict[str, Any] | None = None) -> bool:
        snap = self.snapshot
        if getattr(snap, "per_row_fallback", False):
            if row is None:
                return False
            from backend.db.repositories.data_quality_repo import is_car_incomplete

            return bool(is_car_incomplete(row))
        return car_id in snap.ids


# ---------------------------------------------------------------------------
# Serialization + store writes
# ---------------------------------------------------------------------------


def _fetch_full_rows(ids: list[int]) -> dict[int, dict[str, Any]]:
    """Full grid rows by id, each with its ``_ver`` read in the same statement."""
    out: dict[int, dict[str, Any]] = {}
    if not ids:
        return out
    cols = ", ".join(f"c.{c}" for c in LISTINGS_GRID_CAR_COLUMNS)
    with db_conn(row_factory=sqlite3.Row) as conn:
        for start in range(0, len(ids), _ID_BATCH):
            batch = ids[start:start + _ID_BATCH]
            marks = ",".join("?" for _ in batch)
            cur = conn.cursor()
            cur.execute(
                f"SELECT {cols}, {_ver_expr()} AS _ver FROM cars c WHERE c.id IN ({marks})",
                tuple(batch),
            )
            for r in cur.fetchall():
                d = dict(r)
                out[int(d["id"])] = d
    return out


def _serialize_rows(
    rows: list[dict[str, Any]], ctx: _Ctx, *, yield_gil: bool = False
) -> list[tuple]:
    """Serialize raw rows -> store tuples (car_id, content_key, aux_key, day_dep,
    sort_bucket, sort_price, card_json). Rows the grid hides are skipped."""
    from backend.db.repositories.cars_repo import _parse_car_gallery
    from backend.db.repositories.listings_repo import serialize_car_for_listings_grid
    from backend.utils.listings_sort import listing_price_value, listing_sort_depriority

    try:
        from backend.enrichment.knowledge_engine import prime_vpic_cache

        prime_vpic_cache(r.get("vin") for r in rows)
    except Exception:
        pass

    out: list[tuple] = []
    for i, row in enumerate(rows):
        if yield_gil and i % _BG_YIELD_ROWS == 0:
            time.sleep(_BG_YIELD_S)
        row.pop("_ver", None)
        cid = int(row["id"])
        ckey = _content_key(row)
        pub_inc = ctx.pub_incomplete(cid, row)
        state = ctx.attribution.get(cid)
        _parse_car_gallery(row)
        card = serialize_car_for_listings_grid(
            row, incomplete_snapshot=ctx.snapshot, attribution=state
        )
        dep = listing_sort_depriority(card)
        bucket = dep[0] * 4 + dep[1] * 2 + dep[2]
        price = listing_price_value(card)
        if not math.isfinite(price):
            price = _FAR_PRICE
        out.append(
            (
                cid,
                ckey,
                _aux_key(ctx, pub_inc, state),
                1 if card.get("price_drop_days_ago") is not None else 0,
                bucket,
                float(price),
                json.dumps(card, separators=(",", ":"), default=str),
            )
        )
    return out


_UPSERT_SQL = (
    "INSERT INTO listings_grid_cards "
    "(car_id, row_ver, content_key, aux_key, built_day, day_dep, sort_bucket, sort_price, card_json) "
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
    "ON CONFLICT (car_id) DO UPDATE SET row_ver = excluded.row_ver, "
    "content_key = excluded.content_key, aux_key = excluded.aux_key, "
    "built_day = excluded.built_day, day_dep = excluded.day_dep, "
    "sort_bucket = excluded.sort_bucket, sort_price = excluded.sort_price, "
    "card_json = excluded.card_json"
)


def _store(entries: list[tuple], versions: dict[int, str], day: int) -> None:
    if not entries or not ensure_grid_cards_table():
        return
    params = [
        (e[0], versions.get(e[0]) or None, e[1], e[2], day, e[3], e[4], e[5], e[6])
        for e in entries
    ]
    try:
        with db_conn() as conn:
            cur = conn.cursor()
            for start in range(0, len(params), 500):
                cur.executemany(_UPSERT_SQL, params[start:start + 500])
            conn.commit()
    except Exception:
        _log.warning("listings_grid_cards upsert failed", exc_info=True)


def _touch_versions(pairs: list[tuple[str, int]]) -> None:
    """Record a new ``row_ver`` for cards whose content turned out unchanged."""
    if not pairs or not ensure_grid_cards_table():
        return
    try:
        with db_conn() as conn:
            cur = conn.cursor()
            for start in range(0, len(pairs), 500):
                cur.executemany(
                    "UPDATE listings_grid_cards SET row_ver = ? WHERE car_id = ?",
                    pairs[start:start + 500],
                )
            conn.commit()
    except Exception:
        _log.warning("listings_grid_cards row_ver update failed", exc_info=True)


# ---------------------------------------------------------------------------
# Background refresh (stale-while-revalidate for large invalidations)
# ---------------------------------------------------------------------------

_bg_queue: "queue.Queue[list[int]]" = queue.Queue(maxsize=64)
_bg_pending: set[int] = set()
_bg_pending_lock = threading.Lock()
_bg_thread: threading.Thread | None = None
_bg_thread_lock = threading.Lock()
_store_generation = 0
_store_generation_lock = threading.Lock()


def store_generation() -> int:
    """Process-local generation of the background refresher's work.

    A scoped body that contains stale or missing cards is cached against this value
    (``partial``). It moves when a refresh commits cards -- and ALSO when a refresh
    fails or ids could not be queued, so a partial body never looks current once the
    work it was waiting on is lost: the next request re-resolves and re-queues."""
    return _store_generation


def _bump_store_generation() -> None:
    global _store_generation
    with _store_generation_lock:
        _store_generation += 1


def _bg_worker() -> None:
    while True:
        ids = _bg_queue.get()
        try:
            refresh_cards(ids, yield_gil=True)
        except Exception:
            _log.warning(
                "background grid-card refresh failed for %d cars; partial scope bodies "
                "are invalidated so the next request retries",
                len(ids),
                exc_info=True,
            )
        finally:
            with _bg_pending_lock:
                _bg_pending.difference_update(ids)
            _bump_store_generation()


def _enqueue_refresh(ids: Iterable[int]) -> None:
    global _bg_thread
    with _bg_pending_lock:
        todo = [i for i in dict.fromkeys(ids) if i not in _bg_pending]
        _bg_pending.update(todo)
    if not todo:
        return
    with _bg_thread_lock:
        if _bg_thread is None or not _bg_thread.is_alive():
            _bg_thread = threading.Thread(target=_bg_worker, name="grid-cards-refresh", daemon=True)
            _bg_thread.start()
    for start in range(0, len(todo), 2000):
        chunk = todo[start:start + 2000]
        try:
            _bg_queue.put_nowait(chunk)
        except queue.Full:
            with _bg_pending_lock:
                _bg_pending.difference_update(todo[start:])
            _log.warning(
                "grid-card refresh queue full; dropped %d cars, partial scope bodies "
                "are invalidated so the next request re-queues them",
                len(todo) - start,
            )
            _bump_store_generation()
            return


def refresh_cards(ids: list[int], *, yield_gil: bool = False) -> int:
    """Re-serialize and store cards for *ids* (active rows only). Returns count stored."""
    if not ids:
        return 0
    ctx = _Ctx()
    rows = _fetch_full_rows(list(ids))
    versions = {cid: str(r.get("_ver") or "") for cid, r in rows.items()}
    active = [r for r in rows.values() if r.get("listing_active") in (None, 1, "1", True)]
    entries = _serialize_rows(active, ctx, yield_gil=yield_gil)
    _store(entries, versions, ctx.today)
    return len(entries)


def _ver_expr() -> str:
    return "CAST(c.xmin AS TEXT)" if is_inventory_postgres() else "''"


# ---------------------------------------------------------------------------
# Scope resolution
# ---------------------------------------------------------------------------


class ScopeResult:
    """Cards for one scope, ready to serialize: ``entries`` are
    ``(sort_key, card_json, dealer_id)`` in grid order."""

    def __init__(self) -> None:
        self.entries: list[tuple[tuple, str]] = []
        self.missing_coords = 0
        self.partial = False
        self.stats: dict[str, int] = {}

    def cards_json(self) -> list[str]:
        return [e[1] for e in self.entries]


_CANDIDATE_COLS = (
    "c.id, {ver} AS ver, c.dealer_id, g.row_ver, g.content_key, g.aux_key, "
    "g.built_day, g.day_dep, g.sort_bucket, g.sort_price, g.card_json"
)


def _hidden_clause(hidden: Iterable[str] | None) -> tuple[str, tuple]:
    vals = sorted({str(h or "").strip().lower() for h in (hidden or [])} - {""})
    if not vals:
        return "", ()
    marks = ",".join("?" for _ in vals)
    return f" AND LOWER(COALESCE(c.dealer_id, '')) NOT IN ({marks})", tuple(vals)


def _is_aged(cand: dict[str, Any], today: int) -> bool:
    try:
        bd = int(cand.get("built_day"))
    except (TypeError, ValueError):
        return True
    if bd < today - CARD_MAX_AGE_DAYS:
        return True
    return bool(int(cand.get("day_dep") or 0)) and bd != today


def _resolve(
    candidates: list[tuple[dict[str, Any], float | None]],
    ctx: _Ctx,
    *,
    allow_background: bool = True,
) -> ScopeResult:
    """Turn candidate rows (store columns + distance) into served cards.

    ``allow_background=False`` (the offline builder) rebuilds everything that is not
    current inline, aged cards included; the request path serves aged cards and
    refreshes them off-request."""
    res = ScopeResult()
    per_row = bool(getattr(ctx.snapshot, "per_row_fallback", False))
    fresh: list[tuple[dict[str, Any], float | None]] = []
    to_check: list[tuple[dict[str, Any], float | None]] = []
    for cand, dist in candidates:
        cid = int(cand["id"])
        if per_row:
            to_check.append((cand, dist))
            continue
        pub_inc = ctx.pub_incomplete(cid)
        if pub_inc and not ctx.include_incomplete:
            continue
        aux = _aux_key(ctx, pub_inc, ctx.attribution.get(cid))
        cand["_aux"] = aux
        ver = str(cand.get("ver") or "")
        if (
            cand.get("card_json")
            and ver
            and cand.get("row_ver") == ver
            and (cand.get("aux_key") == aux or not ctx.attribution_ok)
        ):
            fresh.append((cand, dist))
        else:
            to_check.append((cand, dist))

    changed: list[tuple[dict[str, Any], float | None, dict[str, Any] | None]] = []
    touch: list[tuple[str, int]] = []
    if to_check:
        full = _fetch_full_rows([int(c["id"]) for c, _ in to_check])
        for cand, dist in to_check:
            cid = int(cand["id"])
            row = full.get(cid)
            if row is None:
                continue
            if per_row:
                pub_inc = ctx.pub_incomplete(cid, row)
                if pub_inc and not ctx.include_incomplete:
                    continue
                cand["_aux"] = _aux_key(ctx, pub_inc, ctx.attribution.get(cid))
            if (
                cand.get("card_json")
                and cand.get("content_key") == _content_key(row)
                # Attribution unreadable this pass: an aux mismatch may be nothing
                # but the missing verdicts, so the stored card (with its caveat) wins.
                and (cand.get("aux_key") == cand["_aux"] or not ctx.attribution_ok)
            ):
                fresh.append((cand, dist))
                if row.get("_ver"):
                    touch.append((str(row["_ver"]), cid))
            else:
                changed.append((cand, dist, row))

    if not allow_background:
        keep: list[tuple[dict[str, Any], float | None]] = []
        for cand, dist in fresh:
            if _is_aged(cand, ctx.today):
                changed.append((cand, dist, None))
            else:
                keep.append((cand, dist))
        fresh = keep

    total = len(fresh) + len(changed)
    servable_stale = [x for x in changed if x[0].get("card_json")]
    if not allow_background or len(changed) <= _INLINE_REBUILD_MAX:
        build_now, deferred = changed, []
    elif (len(fresh) + len(servable_stale)) >= _MIN_SERVABLE_SHARE * max(total, 1):
        build_now, deferred = [], changed
    else:
        # Store too thin to serve this scope: spend the cap on cards with no stored
        # copy (nearest first) and defer everything else.
        missing = sorted(
            (x for x in changed if not x[0].get("card_json")),
            key=lambda x: (x[1] if x[1] is not None else float("inf"), int(x[0]["id"])),
        )
        build_now = missing[:_INLINE_REBUILD_MAX]
        now_ids = {int(x[0]["id"]) for x in build_now}
        deferred = [x for x in changed if int(x[0]["id"]) not in now_ids]
    res.stats = {
        "fresh": len(fresh),
        "changed": len(changed),
        "inline": len(build_now),
        "deferred": len(deferred),
        "omitted": sum(1 for x in deferred if not x[0].get("card_json")),
    }

    served: list[tuple[dict[str, Any], float | None]] = list(fresh)
    background: list[int] = []
    if deferred:
        served.extend((c, d) for c, d, _ in deferred if c.get("card_json"))
        background.extend(int(c["id"]) for c, _, _ in deferred)
    if build_now:
        need = [int(c["id"]) for c, _, r in build_now if r is None]
        extra = _fetch_full_rows(need) if need else {}
        rows = []
        versions: dict[int, str] = {}
        for c, _, r in build_now:
            row = r if r is not None else extra.get(int(c["id"]))
            if row is None:
                continue
            versions[int(row["id"])] = str(row.get("_ver") or "")
            rows.append(row)
        entries = _serialize_rows(rows, ctx)
        _store(entries, versions, ctx.today)
        dist_by_id = {int(c["id"]): d for c, d, _ in build_now}
        for e in entries:
            served.append(
                (
                    {
                        "id": e[0],
                        "sort_bucket": e[4],
                        "sort_price": e[5],
                        "card_json": e[6],
                        "built_day": ctx.today,
                        "day_dep": 0,
                    },
                    dist_by_id.get(e[0]),
                )
            )
    if touch:
        _touch_versions(touch)

    for cand, dist in served:
        if allow_background and cand.get("built_day") != ctx.today and _is_aged(cand, ctx.today):
            background.append(int(cand["id"]))
        card = cand["card_json"]
        if dist is not None:
            card = f'{card[:-1]},"distance_miles":{round(dist, 1)}}}'
        sp = cand.get("sort_price")
        key = (
            int(cand.get("sort_bucket") or 0),
            float(sp) if sp is not None else _FAR_PRICE,
            int(cand["id"]),
        )
        res.entries.append((key, card))
    res.entries.sort(key=lambda e: e[0])

    if background and allow_background:
        res.partial = True
        _enqueue_refresh(background)
    return res


# dealer_url -> (lat, lon) resolution for cars whose registry row has no coordinates
# (the "dealer_coords" fallback the old client-side radius filter used).
_fallback_urls_cache: tuple[float, list[tuple[str, str, int, tuple[float, float] | None]]] | None = None
_FALLBACK_TTL_S = 300.0


def _no_registry_coords_clause() -> str:
    return "(c.dealership_registry_id IS NULL OR d.id IS NULL OR d.latitude IS NULL OR d.longitude IS NULL)"


def _fallback_dealer_urls() -> list[tuple[str, str, int, tuple[float, float] | None]]:
    """(dealer_url, dealer_id, active car count, coords-or-None) for active cars that
    have no registry coordinates. Cached; the set moves on the order of scans."""
    global _fallback_urls_cache
    now = time.monotonic()
    hit = _fallback_urls_cache
    if hit is not None and (now - hit[0]) < _FALLBACK_TTL_S:
        return hit[1]
    from backend.db.dealer_geo import load_dealer_geo_index, lookup_dealer_coords

    out: list[tuple[str, str, int, tuple[float, float] | None]] = []
    with db_conn() as conn:
        rows = conn.execute(
            "SELECT c.dealer_url, c.dealer_id, COUNT(*) FROM cars c "
            "LEFT JOIN dealerships d ON d.id = c.dealership_registry_id "
            f"WHERE {_ACTIVE} AND {_no_registry_coords_clause()} "
            "GROUP BY c.dealer_url, c.dealer_id"
        ).fetchall()
        geo = load_dealer_geo_index(conn)
    for url, did, n in rows:
        coords = lookup_dealer_coords(str(url or ""), geo)
        out.append((str(url or ""), str(did or ""), int(n or 0), coords))
    _fallback_urls_cache = (now, out)
    return out


def missing_coords_count() -> int:
    """Active cars with neither registry nor dealer_url coordinates (never in any radius)."""
    try:
        return sum(n for _, _, n, coords in _fallback_dealer_urls() if coords is None)
    except Exception:
        return 0


def _bbox(lat: float, lon: float, radius_mi: float) -> tuple[float, float, float, float]:
    dlat = radius_mi / 69.0
    coslat = max(math.cos(math.radians(lat)), 0.01)
    dlon = radius_mi / (69.172 * coslat)
    return lat - dlat, lat + dlat, lon - dlon, lon + dlon


def cards_near(
    lat: float,
    lon: float,
    radius_mi: float,
    *,
    exclude_dealer_ids: Iterable[str] | None = None,
    allow_background: bool = True,
) -> ScopeResult:
    """Grid cards for every active car within *radius_mi* of (lat, lon).

    Registry coordinates first (``cars.dealership_registry_id`` -> ``dealerships``),
    bounding-box prefiltered in SQL, exact haversine per dealer location in Python.
    Cars with no registry coordinates are included only through the dealer_url
    coordinate fallback; the rest are counted in ``missing_coords``.
    """
    from backend.db.geo import haversine

    ensure_grid_cards_table()
    ctx = _Ctx()
    lat0, lat1, lon0, lon1 = _bbox(lat, lon, radius_mi)
    hid_sql, hid_params = _hidden_clause(exclude_dealer_ids)
    cols = _CANDIDATE_COLS.format(ver=_ver_expr())
    store_join = "LEFT JOIN listings_grid_cards g ON g.car_id = c.id" if _table_ready else (
        "LEFT JOIN (SELECT NULL AS car_id, NULL AS row_ver, NULL AS content_key, NULL AS aux_key, "
        "NULL AS built_day, NULL AS day_dep, NULL AS sort_bucket, NULL AS sort_price, "
        "NULL AS card_json) g ON 1 = 0"
    )
    candidates: list[tuple[dict[str, Any], float | None]] = []
    dist_memo: dict[tuple[float, float], float] = {}

    def _dist(la: float, lo: float) -> float:
        k = (la, lo)
        d = dist_memo.get(k)
        if d is None:
            d = haversine(lat, lon, la, lo)
            dist_memo[k] = d
        return d

    with db_conn(row_factory=sqlite3.Row) as conn:
        cur = conn.cursor()
        cur.execute(
            f"SELECT {cols}, d.latitude AS lat, d.longitude AS lon FROM cars c "
            "JOIN dealerships d ON d.id = c.dealership_registry_id "
            f"{store_join} "
            f"WHERE {_ACTIVE} AND c.dealership_registry_id IS NOT NULL "
            "AND d.latitude BETWEEN ? AND ? AND d.longitude BETWEEN ? AND ?"
            f"{hid_sql}",
            (lat0, lat1, lon0, lon1, *hid_params),
        )
        for r in cur.fetchall():
            d = _dist(float(r["lat"]), float(r["lon"]))
            if d <= radius_mi:
                candidates.append((dict(r), d))

        fb = [
            (url, did, coords)
            for url, did, _, coords in _fallback_dealer_urls()
            if coords is not None and _dist(coords[0], coords[1]) <= radius_mi
        ]
        if fb:
            url_dist = {url: _dist(coords[0], coords[1]) for url, _, coords in fb}
            ids = sorted({did for _, did, _ in fb if did})
            urls = sorted(url_dist)
            for start in range(0, len(urls), _ID_BATCH):
                ub = urls[start:start + _ID_BATCH]
                umarks = ",".join("?" for _ in ub)
                id_sql = ""
                id_params: tuple = ()
                if ids and len(ids) <= _ID_BATCH:
                    id_sql = f" AND c.dealer_id IN ({','.join('?' for _ in ids)})"
                    id_params = tuple(ids)
                cur = conn.cursor()
                cur.execute(
                    f"SELECT {cols}, c.dealer_url AS dealer_url FROM cars c "
                    "LEFT JOIN dealerships d ON d.id = c.dealership_registry_id "
                    f"{store_join} "
                    f"WHERE {_ACTIVE} AND {_no_registry_coords_clause()} "
                    f"AND c.dealer_url IN ({umarks}){id_sql}{hid_sql}",
                    (*ub, *id_params, *hid_params),
                )
                for r in cur.fetchall():
                    candidates.append((dict(r), url_dist.get(str(r["dealer_url"] or ""))))

    res = _resolve(candidates, ctx, allow_background=allow_background)
    res.missing_coords = missing_coords_count()
    return res


def cards_for_dealer(dealer_id: str, *, allow_background: bool = True) -> ScopeResult:
    """Grid cards for one dealer's active cars (the dealership research page)."""
    key = (dealer_id or "").strip()
    res = ScopeResult()
    if not key:
        return res
    ensure_grid_cards_table()
    ctx = _Ctx()
    cols = _CANDIDATE_COLS.format(ver=_ver_expr())
    variants = tuple(sorted({key, key.lower()}))
    marks = ",".join("?" for _ in variants)
    store_join = "LEFT JOIN listings_grid_cards g ON g.car_id = c.id" if _table_ready else (
        "LEFT JOIN (SELECT NULL AS car_id, NULL AS row_ver, NULL AS content_key, NULL AS aux_key, "
        "NULL AS built_day, NULL AS day_dep, NULL AS sort_bucket, NULL AS sort_price, "
        "NULL AS card_json) g ON 1 = 0"
    )
    with db_conn(row_factory=sqlite3.Row) as conn:
        cur = conn.cursor()
        cur.execute(
            f"SELECT {cols} FROM cars c {store_join} "
            f"WHERE {_ACTIVE} AND c.dealer_id IN ({marks})",
            variants,
        )
        candidates = [(dict(r), None) for r in cur.fetchall()]
    return _resolve(candidates, ctx, allow_background=allow_background)


def build_all_cards(*, batch: int = 2000, progress=None) -> dict[str, int]:
    """Offline: bring every active car's card up to date (the builder script)."""
    ensure_grid_cards_table()
    with db_conn() as conn:
        ids = [
            int(r[0])
            for r in conn.execute(f"SELECT c.id FROM cars c WHERE {_ACTIVE} ORDER BY c.id").fetchall()
        ]
    stats = {"active": len(ids), "fresh": 0, "rebuilt": 0}
    for start in range(0, len(ids), batch):
        chunk = ids[start:start + batch]
        ctx = _Ctx()
        cols = _CANDIDATE_COLS.format(ver=_ver_expr())
        marks = ",".join("?" for _ in chunk)
        with db_conn(row_factory=sqlite3.Row) as conn:
            cur = conn.cursor()
            cur.execute(
                f"SELECT {cols} FROM cars c LEFT JOIN listings_grid_cards g ON g.car_id = c.id "
                f"WHERE c.id IN ({marks})",
                tuple(chunk),
            )
            cands = [(dict(r), None) for r in cur.fetchall()]
        res = _resolve(cands, ctx, allow_background=False)
        stats["fresh"] += res.stats.get("fresh", 0)
        stats["rebuilt"] += res.stats.get("changed", 0)
        if progress:
            progress(start + len(chunk), len(ids), stats)
    # Cards of cars that left the active fleet are never served (every scoped query
    # starts from active cars); drop them so the table tracks the fleet's size.
    with db_conn() as conn:
        cur = conn.execute(
            "DELETE FROM listings_grid_cards WHERE NOT EXISTS ("
            f"SELECT 1 FROM cars c WHERE c.id = listings_grid_cards.car_id AND {_ACTIVE})"
        )
        stats["pruned"] = max(0, int(getattr(cur, "rowcount", 0) or 0))
        conn.commit()
    return stats
