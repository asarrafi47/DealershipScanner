"""Single-car reads/writes and car-row parse helpers."""
import json
import logging
import re
import sqlite3
from typing import Any, Iterable

from backend.db.repositories.base_repo import _placeholders, db_conn, get_conn
from backend.db.repositories.schema_repo import ensure_nhtsa_vpic_cache_table

_log = logging.getLogger(__name__)


def is_dummy_placeholder_vin(vin: str | None) -> bool:
    """
    True for legacy dev / seed rows such as ``VIN001``, ``VINXXX``, ``VINXXXX`` (not real VINs).

    Matches: ``VIN`` + digits only with total length ``< 17``; ``VIN`` + ``X`` only; or any
    value containing the literal substring ``VINXXX`` (case-insensitive).
    """
    v = str(vin or "").strip().upper()
    if not v:
        return False
    if "VINXXX" in v:
        return True
    if len(v) >= 17:
        return False
    if not v.startswith("VIN"):
        return False
    suf = v[3:]
    if suf.isdigit():
        return True
    if suf and re.fullmatch(r"X+", suf):
        return True
    return False


def delete_cars_with_dummy_placeholder_vins() -> dict[str, Any]:
    """
    Delete ``cars`` rows whose VIN matches :func:`is_dummy_placeholder_vin`, remove matching
    ``incomplete_listings`` and ``saved_cars`` rows, and drop ``nhtsa_vpic_cache`` entries for those VINs.
    """
    from backend.db.incomplete_listings_db import delete_incomplete_record

    conn = get_conn()
    try:
        ensure_nhtsa_vpic_cache_table(conn)
        cur = conn.cursor()
        cur.execute("SELECT id, vin FROM cars")
        rows: list[tuple[int, str]] = []
        for r in cur.fetchall():
            rid, rv = int(r[0]), str(r[1] or "")
            if is_dummy_placeholder_vin(rv):
                rows.append((rid, rv))
        if not rows:
            return {"deleted": 0, "vins": [], "nhtsa_cache_deleted": 0}
        ids = [r[0] for r in rows]
        vins = [r[1] for r in rows]
        n_cache = 0
        for cid in ids:
            try:
                delete_incomplete_record(cid)
            except Exception as exc:
                _log.debug("incomplete_listings delete car_id=%s: %s", cid, exc)
        for vin in vins:
            cur.execute("DELETE FROM nhtsa_vpic_cache WHERE UPPER(TRIM(vin)) = ?", (vin.upper().strip(),))
            n_cache += cur.rowcount
        ph = ",".join("?" * len(ids))
        try:
            cur.execute(f"DELETE FROM saved_cars WHERE car_id IN ({ph})", ids)
        except Exception:
            _log.debug("saved_cars delete for dummy VINs skipped (table missing?)")
        cur.execute(f"DELETE FROM cars WHERE id IN ({ph})", ids)
        conn.commit()
        return {"deleted": len(ids), "vins": vins, "nhtsa_cache_deleted": n_cache}
    finally:
        conn.close()


def _parse_car_gallery(car_dict):
    """Ensure car_dict['gallery'] is a list (parse from JSON string if needed)."""
    if not car_dict:
        return
    g = car_dict.get("gallery")
    if isinstance(g, list):
        return
    if g is None or g == "":
        car_dict["gallery"] = []
        return
    try:
        car_dict["gallery"] = json.loads(g) if isinstance(g, str) else []
    except (TypeError, ValueError):
        car_dict["gallery"] = []


def _parse_car_spin_frames(car_dict):
    """Ensure car_dict['spin_frames'] is a list (parse from JSON string if needed)."""
    if not car_dict:
        return
    s = car_dict.get("spin_frames")
    if isinstance(s, list):
        return
    if s is None or s == "":
        car_dict["spin_frames"] = []
        return
    try:
        parsed = json.loads(s) if isinstance(s, str) else []
        car_dict["spin_frames"] = parsed if isinstance(parsed, list) else []
    except (TypeError, ValueError):
        car_dict["spin_frames"] = []


def _parse_car_history_highlights(car_dict):
    """Ensure car_dict['history_highlights'] is a list (parse from JSON string if needed)."""
    if not car_dict:
        return
    h = car_dict.get("history_highlights")
    if isinstance(h, list):
        return
    if h is None or h == "":
        car_dict["history_highlights"] = []
        return
    try:
        car_dict["history_highlights"] = json.loads(h) if isinstance(h, str) else []
    except (TypeError, ValueError):
        car_dict["history_highlights"] = []


def get_car_by_id(car_id, *, include_inactive: bool = True):
    try:
        with db_conn(row_factory=sqlite3.Row) as conn:
            cursor = conn.cursor()
            if include_inactive:
                cursor.execute("SELECT * FROM cars WHERE id = ?", (car_id,))
            else:
                cursor.execute(
                    "SELECT * FROM cars WHERE id = ? AND (COALESCE(listing_active, 1) = 1)",
                    (car_id,),
                )
            row = cursor.fetchone()
    except sqlite3.OperationalError as ex:
        # Uninitialized SQLite file (e.g. tests pointing DB_PATH at an empty db):
        # no cars table means no car. Anything else is a real error.
        if "no such table" not in str(ex).lower():
            raise
        row = None
    car = dict(row) if row else None
    if car:
        _parse_car_gallery(car)
        _parse_car_spin_frames(car)
        _parse_car_history_highlights(car)
    return car


def get_cars_by_ids(car_ids: list[int]) -> list[dict]:
    """Fetch full car rows by primary key; order matches ``car_ids`` (skips missing)."""
    if not car_ids:
        return []
    ordered_unique: list[int] = []
    seen: set[int] = set()
    for raw in car_ids:
        try:
            i = int(raw)
        except (TypeError, ValueError):
            continue
        if i <= 0 or i in seen:
            continue
        seen.add(i)
        ordered_unique.append(i)
    if not ordered_unique:
        return []
    with db_conn(row_factory=sqlite3.Row) as conn:
        cursor = conn.cursor()
        ph = _placeholders(ordered_unique)
        cursor.execute(f"SELECT * FROM cars WHERE id IN ({ph})", ordered_unique)
        by_id = {dict(row)["id"]: dict(row) for row in cursor.fetchall()}
    out: list[dict] = []
    for cid in ordered_unique:
        row = by_id.get(cid)
        if not row:
            continue
        _parse_car_gallery(row)
        _parse_car_spin_frames(row)
        _parse_car_history_highlights(row)
        out.append(row)
    return out


def get_car_by_vin(vin):
    v = str(vin).strip() if vin is not None else ""
    with db_conn(row_factory=sqlite3.Row) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM cars WHERE vin = ?", (v,))
        row = cursor.fetchone()
        if row is None and v:
            # VINs are canonically uppercase, but the write path stores them as
            # received (scanner/database.py strips but does not upper-case). A
            # normalized feed VIN (always upper — normalize_scanner_vin) must
            # still match a raw lower/mixed-case DB row, else delta gap-fill
            # silently no-ops for such rows. Fall back to a case-insensitive
            # match only on miss, so the common exact-match path keeps using
            # the vin index.
            cursor.execute("SELECT * FROM cars WHERE UPPER(TRIM(vin)) = ?", (v.upper(),))
            row = cursor.fetchone()
    car = dict(row) if row else None
    if car:
        _parse_car_gallery(car)
        _parse_car_spin_frames(car)
        _parse_car_history_highlights(car)
    return car


# ---------------------------------------------------------------------------
# Photo attribution overlay (read-time)
# ---------------------------------------------------------------------------
#
# ``car_attribution`` holds ~2,700 verdicts about where a car ACTUALLY is, read off
# the listing's own photographs (plate frames, building signage, tailgate decals) by
# ``backend/scripts/attribution_batch.py``. ``dealer_feed_scope`` marks the dealers
# proven to be served their whole ownership group's feed -- ``bmwofmurrieta-com``
# lists 1,921 cars and 76 of the first 99 photo-judged ones name a rooftop in
# Georgia, North Carolina or Tennessee.
#
# ``apply_attribution_moves`` physically re-filed the 144 cars whose named rooftop
# resolves to exactly one registered, geocoded dealership. The rest CANNOT be moved:
# the real rooftop is unregistered, or the photo named a parent group ("Hendrick
# Automotive Group") rather than a store, or two registered stores match the name
# equally well. Until this overlay existed, nothing downstream read the table at
# all, so every one of those cars kept asserting a dealership we have photographic
# evidence against.
#
# This softens the CLAIM. It never filters a row: a previous fix moved cars to an
# ungeocoded dealer and silently dropped them out of radius search, and inventory
# vanishing is a worse failure than a location caveat.
# Two populations need the caveat, and only one of them has a row in car_attribution.
#
# The judged cars are the first branch. The second is the reason `dealer_feed_scope`
# exists at all: a car at a dealer we have PROVEN is served its whole group's inventory,
# which no photograph has looked at yet. `bmwofmurrieta-com` shows 76 photos naming other
# rooftops against 23 naming itself, across 53 distinct stores -- and 1,848 of its cars had
# never been judged, so every one of them stated its location as fact. That is exactly what
# V011 says must not happen: "we do not know where it is, and the site must not claim to."
#
# Driving this from `cars` would be an N+1 on the hottest read in the app, so the second
# branch is bounded by the group-fed dealers (2 dealers, ~2,200 unjudged cars) rather than
# by the fleet, and it stays a single query.
#
# Wrapped in a subquery so the id filter below applies to BOTH branches -- appending a
# WHERE to the raw union would silently bind to the last one only.
#
# ``moved_to`` is the destination ``apply_attribution_moves`` deliberately re-filed the
# car onto -- the latest non-reverted ``car_move_log`` row. The verdict itself never
# records a destination (``evidence`` is free text), so the move log is the only place
# "we resolved the photographed rooftop to THIS store" is written down. It is a
# correlated subquery rather than a join so multiple moves for one car cannot fan the
# row out, and the template exists because a database where the moves script has never
# run has no ``car_move_log`` at all -- see the fallback in ``car_attribution_states``.
_ATTRIBUTION_STATE_SQL_TEMPLATE = """
    SELECT car_id, status, observed_rooftop, filed_dealer_id, dealer_id, scope, moved_to FROM (
        SELECT a.car_id AS car_id, a.status AS status,
               a.observed_rooftop AS observed_rooftop, a.filed_dealer_id AS filed_dealer_id,
               c.dealer_id AS dealer_id, s.scope AS scope,
               {moved_to} AS moved_to
        FROM car_attribution a
        JOIN cars c ON c.id = a.car_id
        LEFT JOIN dealer_feed_scope s ON s.dealer_id = c.dealer_id
        UNION ALL
        SELECT c.id, 'unverified', NULL, NULL, c.dealer_id, s.scope, NULL
        FROM cars c
        JOIN dealer_feed_scope s ON s.dealer_id = c.dealer_id AND s.scope = 'group'
        LEFT JOIN car_attribution a2 ON a2.car_id = c.id
        WHERE a2.car_id IS NULL
          AND c.listing_removed_at IS NULL
    ) attribution_state
"""

_ATTRIBUTION_STATE_SQL = _ATTRIBUTION_STATE_SQL_TEMPLATE.format(
    moved_to=(
        "(SELECT m.to_dealer_id FROM car_move_log m"
        " WHERE m.car_id = a.car_id AND m.reverted_at IS NULL"
        " ORDER BY m.id DESC LIMIT 1)"
    )
)

# Same statement with no move-log read: every move looks unresolved, so a re-filed car
# keeps its caveat (over-caveating the 144 fixed cars) rather than dealer_id drift
# clearing one (asserting a location we have photographic evidence against).
_ATTRIBUTION_STATE_SQL_NO_MOVE_LOG = _ATTRIBUTION_STATE_SQL_TEMPLATE.format(moved_to="NULL")

# Above this many ids, read the whole (small) verdict table and filter in Python
# rather than build a monster IN list -- SQLite caps bound parameters at 999 on
# older builds, and the listings grid asks about the entire active fleet at once.
_ATTRIBUTION_ID_INLINE_MAX = 500


def _attribution_location_unconfirmed(status: str, *, group_fed: bool, refiled: bool) -> bool:
    """Whether the filed dealership may still be presented as fact.

    * ``conflicting`` -- a photograph named a rooftop and it is not the filed one.
      We cannot stand behind the filing, whether or not the named store is one we
      could have moved the car to.
    * ``unverified`` at a group-fed dealer -- no evidence either way, but the feed
      this listing arrived on serves the whole group, so "it is at the rooftop whose
      site published it" is exactly the assumption that is wrong here.
    * ``unverified`` anywhere else, and every ``confirmed`` row -- unchanged. Most of
      the fleet is fine and must not be caveated.
    """
    if refiled:
        # ``apply_attribution_moves`` already moved this car onto the rooftop its
        # photos named, and it writes ``cars`` without touching the verdict. The
        # stale ``conflicting`` row now describes where the car USED to be filed,
        # so reading it literally would caveat the 144 cars we already fixed.
        #
        # ``refiled`` means exactly that: the car's CURRENT dealer_id matches the
        # destination of a logged, non-reverted move. A bare ``dealer_id !=
        # filed_dealer_id`` is not enough -- V011 keeps ``filed_dealer_id`` precisely
        # so a re-scan that changes dealer_id "does not silently make this row look
        # consistent", and only the moves script performs deliberate moves.
        return False
    if status == "conflicting":
        return True
    if status == "unverified":
        return group_fed
    return False


def car_attribution_states(
    car_ids: Iterable[int] | None = None, *, fail_open: bool = True
) -> dict[int, dict[str, Any]]:
    """Photo-attribution state per car id, in ONE query (never one per car).

    ``fail_open=False`` re-raises a failed read instead of answering ``{}``: the
    persisted grid-card store must tell "no verdicts" from "could not read them",
    or one failed read rebuilds and persists every card without its caveat.

    Pass ``None`` for every judged car (the table is ~2,700 rows; the listings grid
    builder wants the lot). ``/api/listings/cars`` already ships megabytes over the
    whole active fleet, so a per-car lookup here would be an N+1 on the hottest read
    in the app.

    Cars with no verdict are absent from the mapping -- callers treat a miss as
    "nothing to say", which is the correct default for the ~99% of the fleet no
    photograph has judged. The one exception is a car at a dealer known to be served
    its whole group's inventory: no photograph has judged it either, but there we have
    positive evidence the filing is unreliable, so it comes back ``unverified``.
    """
    ids: list[int] | None = None
    if car_ids is not None:
        ids = []
        seen: set[int] = set()
        for raw in car_ids:
            try:
                i = int(raw)
            except (TypeError, ValueError):
                continue
            if i > 0 and i not in seen:
                seen.add(i)
                ids.append(i)
        if not ids:
            return {}

    sql = _ATTRIBUTION_STATE_SQL
    sql_no_move_log = _ATTRIBUTION_STATE_SQL_NO_MOVE_LOG
    params: list[int] = []
    if ids is not None and len(ids) <= _ATTRIBUTION_ID_INLINE_MAX:
        id_filter = f" WHERE car_id IN ({_placeholders(ids)})"
        sql = f"{sql}{id_filter}"
        sql_no_move_log = f"{sql_no_move_log}{id_filter}"
        params = ids

    def _fetch(statement: str) -> list:
        # A fresh connection per attempt: Postgres aborts the transaction on the
        # first failed statement, so the fallback cannot run on the same one.
        with db_conn() as conn:
            return conn.execute(statement, params).fetchall()

    try:
        rows = _fetch(sql)
    except Exception:
        try:
            # ``car_move_log`` only exists once apply_attribution_moves has run
            # against this database. Without it every move reads as unresolved,
            # which keeps caveats rather than clearing them -- the safe direction.
            rows = _fetch(sql_no_move_log)
        except Exception:
            if not fail_open:
                raise
            # The overlay is a caveat on top of what the page already shows. A missing
            # table (fresh SQLite dev/test db) or a failed read must degrade to "no
            # verdict", not 500 the listings grid -- but silently shipping the whole
            # fleet uncaveated is what WARNING exists for.
            _log.warning(
                "car_attribution overlay unavailable; listings will carry no location caveats",
                exc_info=True,
            )
            return {}

    wanted = set(ids) if ids is not None else None
    out: dict[int, dict[str, Any]] = {}
    for row in rows:
        try:
            car_id = int(row[0])
        except (TypeError, ValueError):
            continue
        if wanted is not None and car_id not in wanted:
            continue
        status = str(row[1] or "").strip().lower()
        rooftop = (str(row[2]).strip() or None) if row[2] is not None else None
        current_dealer_id = str(row[4] or "").strip().lower()
        group_fed = str(row[5] or "").strip().lower() == "group"
        moved_to = str(row[6] or "").strip().lower()
        # Refiled means a deliberate, logged, non-reverted apply_attribution_moves
        # run landed the car on the dealer it still sits at. Comparing dealer_id to
        # filed_dealer_id instead would treat ANY post-verdict re-scan drift as a
        # move and clear the caveat for a filing we have evidence against.
        refiled = bool(moved_to and current_dealer_id and current_dealer_id == moved_to)
        out[car_id] = {
            "status": status,
            "observed_rooftop": rooftop,
            "group_feed": group_fed,
            "location_unconfirmed": _attribution_location_unconfirmed(
                status, group_fed=group_fed, refiled=refiled
            ),
        }
    return out


# Only rows whose provenance is PROVABLE, matching backend/scripts/build_trim_msrp_bands.py
# and feed_package_registry_from_vision.py: the total must have been READ off the document
# (a reconstructed total is where a missed Destination Charge disappears without trace),
# and rows recorded before the provenance guards existed are skipped — not because they
# are presumed wrong, but because we cannot show they are right. The summary flags are
# fetched as columns and judged in Python: SQLite's ``->>`` yields ``1`` for JSON true
# where Postgres jsonb yields ``'true'``, and a literal comparison in SQL would silently
# agree with only one backend.
_STICKER_MSRP_SQL = """
    SELECT car_id, sticker_msrp,
           summary ->> 'msrp_read_directly',
           summary ->> 'msrp_provenance',
           summary ->> 'msrp_recheck',
           summary ->> 'sticker_is_original',
           summary ->> 'msrp_document',
           summary ->> 'exterior_color_seen',
           summary ->> 'interior_color_seen',
           summary ->> 'color_source'
    FROM car_image_text
    WHERE version >= 100
      AND (sticker_msrp IS NOT NULL OR summary ->> 'color_source' = 'monroney')
"""


def car_sticker_msrp_values(
    car_ids: Iterable[int] | None = None,
) -> dict[int, dict[str, Any]]:
    """Photographed-sticker MSRP per car id, in ONE query (never one per car).

    ``car_image_text.sticker_msrp`` at the agent-vision version (>= 100) already
    passed the record-time gates in ``image_batch.cmd_record`` — VIN confirmed,
    USD, total read directly, arithmetic reconciliation — and this read keeps
    only the rows whose summary proves it (see ``_STICKER_MSRP_SQL``). Since the
    2026-08-18 policy change the document need not be an original Monroney: a
    dealer build sheet's total is stored too, so each entry carries its
    provenance — ``{"value": int, "sticker_is_original": bool,
    "msrp_document": "monroney" | "dealer_build_sheet"}`` — for the overlay to
    label honestly. Rows recorded before ``msrp_document`` existed could only
    have passed the then-mandatory originality gate, so both fields default to
    the Monroney reading.

    The same query also carries the sticker's printed colors —
    ``exterior_color_seen`` / ``interior_color_seen`` / ``color_source`` from
    the summary, passed through verbatim. The MSRP gates above never suppress
    them (a refused TOTAL says nothing about the color lines), and a row whose
    sticker printed colors but yielded no servable total still gets an entry,
    with ``value: None``. Whether a color may supersede the feed's is decided by
    ``backend/utils/car_serialize/color_overlay.py`` (only ``color_source ==
    "monroney"`` — printed on the document — ever does; a photo-observed color
    never overrides the feed).

    Same contract as :func:`car_attribution_states`: pass ``None`` for every
    recorded car, absent ids mean "nothing to say", and a missing table degrades
    to ``{}`` rather than failing a page render. How the value is worded next to
    a car is ``backend/utils/car_serialize/msrp_overlay.py``'s job, not this
    one's.
    """
    ids: list[int] | None = None
    if car_ids is not None:
        ids = []
        seen: set[int] = set()
        for raw in car_ids:
            try:
                i = int(raw)
            except (TypeError, ValueError):
                continue
            if i > 0 and i not in seen:
                seen.add(i)
                ids.append(i)
        if not ids:
            return {}

    sql = _STICKER_MSRP_SQL
    params: list[int] = []
    if ids is not None and len(ids) <= _ATTRIBUTION_ID_INLINE_MAX:
        sql = f"{sql} AND car_id IN ({_placeholders(ids)})"
        params = ids
    try:
        with db_conn() as conn:
            rows = conn.execute(sql, params).fetchall()
    except Exception:
        _log.debug("sticker msrp overlay unavailable", exc_info=True)
        return {}

    wanted = set(ids) if ids is not None else None
    out: dict[int, dict[str, Any]] = {}
    for row in rows:
        try:
            car_id = int(row[0])
        except (TypeError, ValueError):
            continue
        if wanted is not None and car_id not in wanted:
            continue

        # The provenance gates below refuse the TOTAL, never the row: a color
        # printed on the sticker is legible whether or not the arithmetic on
        # the price column held, so a refused value degrades to None and the
        # color facts still ride along.
        value: int | None
        try:
            value = int(round(float(row[1])))
        except (TypeError, ValueError):
            value = None
        if value is not None:
            if str(row[2] or "").strip().lower() not in ("true", "1"):
                value = None
            elif str(row[3] or "").strip().lower() == "unverified_pre_guard":
                value = None
            elif str(row[4] or "").strip().lower() == "withdraw_proposed":
                # recheck_msrp parked this value pending human review: an agent
                # proposed withdrawing it and agents do not get to delete prices,
                # but a number under active dispute must not be served as a
                # photographed fact either.
                value = None
        # Judged in Python like msrp_read_directly above (SQLite ``->>`` yields
        # ``0``/``1`` where Postgres yields ``'true'``/``'false'``). Absent means
        # original: before 2026-08-18 non-original documents were refused at
        # record time, so a stored value without the flag can only be a Monroney.
        # ``row[5] is not None``, NOT ``row[5] or``: SQLite's integer 0 is falsy
        # and ``or`` would launder JSON false back into "original".
        is_original = (
            str(row[5] if row[5] is not None else "").strip().lower()
            not in ("false", "0")
        )
        document = str(row[6] or "").strip().lower()
        if document not in ("monroney", "dealer_build_sheet"):
            document = "monroney" if is_original else "dealer_build_sheet"
        exterior_color = str(row[7] or "").strip() or None
        interior_color = str(row[8] or "").strip() or None
        color_source = str(row[9] or "").strip().lower() or None
        if value is None and not (
            color_source and (exterior_color or interior_color)
        ):
            # Nothing servable survived: no total and no sourced color. Absent
            # means "nothing to say", exactly as before the color columns.
            continue
        out[car_id] = {
            "value": value,
            "sticker_is_original": is_original,
            "msrp_document": document,
            "exterior_color_seen": exterior_color,
            "interior_color_seen": interior_color,
            "color_source": color_source,
        }
    return out


_UPDATABLE_CAR_COLUMNS = frozenset(
    {
        "title",
        "year",
        "make",
        "model",
        "trim",
        "price",
        "mileage",
        "fuel_type",
        "cylinders",
        "transmission",
        "transmission_type",
        "drivetrain",
        "exterior_color",
        "interior_color",
        "image_url",
        "dealer_name",
        "dealer_url",
        "dealer_id",
        "stock_number",
        "gallery",
        "carfax_url",
        "window_sticker_url",
        "history_highlights",
        "msrp",
        "dealership_registry_id",
        "source_url",
        "body_style",
        "engine_description",
        "engine_l",
        "condition",
        "description",
        "data_quality_score",
        "mpg_city",
        "mpg_highway",
        "is_cpo",
        "model_full_raw",
        "recovery_status",
        "recovery_attempted_at",
        "recovery_source",
        "recovery_notes",
        "missing_field_count",
        "recoverability_score",
        "spec_source_json",
        "packages",
        "listing_active",
        "listing_removed_at",
        "interior_color_buckets",
        "first_seen_at",
        "last_price_change_at",
        "internal_notes",
        "marked_for_review",
        "price_provenance_json",
    }
)


def _guard_mild_hybrid_fuel_type(car_id: int, fields: dict) -> dict:
    """
    Return *fields* with a mild-hybrid ``fuel_type`` corrected to gasoline.

    The post-scan writers (``gap_fill``, the window-sticker enricher) propose a
    ``fuel_type`` from engine text, so a 48V BSG drivetrain reaches this function
    labelled "Hybrid" even though the upsert already corrected the fed value. The
    fuel FILTERS read ``cars.fuel_type`` directly, so the label has to be right in
    the column, not just on the card.

    The family match needs the nameplate/year, which a partial patch may not
    carry; the stored row is read only when the incoming value is a correctable
    non-plug-in hybrid label OR a bare "Electric" claim (which the same
    normalizer routes to its evidence-backed label when the row's own engine
    text / nameplate contradicts it — gas GX 550s fed as "Electric") — a small
    minority of patches either way.
    """
    from backend.utils.fuel_label_plausibility import is_bare_electric_label
    from backend.utils.fuel_type_normalize import (
        is_correctable_hybrid_label,
        normalize_fuel_type_for_storage,
    )

    _ft = fields.get("fuel_type")
    if not (is_correctable_hybrid_label(_ft) or is_bare_electric_label(_ft)):
        return fields
    try:
        stored = get_car_by_id(car_id) or {}
    except Exception:
        _log.exception("mild-hybrid fuel guard could not read car %s", car_id)
        return fields
    fixed = normalize_fuel_type_for_storage({**stored, **fields})
    if not fixed or fixed == fields.get("fuel_type"):
        return fields
    return {**fields, "fuel_type": fixed}


# Columns whose stored value is a closed vocabulary (FWD/RWD/AWD/4WD, the fuel
# presets, the body-style presets). The scanner upsert runs every incoming value
# through these coercers via ``clean_car_row_dict``; partial writers did not, so
# EPA/vPIC display strings ("Four-Wheel Drive", "Regular Gasoline", "Sport Utility
# Vehicle [SUV]/Multipurpose Vehicle [MPV]") landed in the columns verbatim and
# split the facet buckets they are supposed to fill.
_STORAGE_VOCABULARY_COERCERS = {
    "drivetrain": "coerce_drivetrain_stored",
    "fuel_type": "coerce_fuel_type_stored",
    "body_style": "coerce_body_style_stored",
}


def _coerce_vocabulary_fields(fields: dict) -> dict:
    """Canonicalize closed-vocabulary columns to the same values the upsert stores."""
    import backend.utils.field_clean as _fc

    patched: dict | None = None
    for col, fn_name in _STORAGE_VOCABULARY_COERCERS.items():
        if col not in fields:
            continue
        raw = fields[col]
        if raw is None:
            continue
        coerced = getattr(_fc, fn_name)(raw)
        if coerced != raw:
            if patched is None:
                patched = dict(fields)
            patched[col] = coerced
    return patched if patched is not None else fields


def update_car_row_partial(car_id: int, fields: dict) -> None:
    """Persist only provided keys (used by incomplete listing recovery)."""
    if not fields:
        return
    # Canonicalize BEFORE the mild-hybrid guard: that guard only recognises
    # canonical hybrid labels, so "Gasoline / Electric" has to become "Hybrid"
    # first for it to get a look at the value.
    fields = _coerce_vocabulary_fields(fields)
    if "fuel_type" in fields:
        fields = _guard_mild_hybrid_fuel_type(car_id, fields)
    if fields.get("cylinders") is not None:
        # Feed sentinels (GM sends 99 for EVs) and junk counts must not reach
        # the column through the enrichment/partial path either.
        from backend.utils.fuel_label_plausibility import sanitize_cylinder_count

        fields = {**fields, "cylinders": sanitize_cylinder_count(fields["cylinders"])}
    sets: list[str] = []
    vals: list = []
    for k, raw in fields.items():
        if k not in _UPDATABLE_CAR_COLUMNS:
            continue
        if k == "gallery" and isinstance(raw, list):
            raw = json.dumps(raw)
        sets.append(f"{k} = ?")
        vals.append(raw)
    if not sets:
        return
    vals.append(car_id)
    with db_conn() as conn:
        conn.execute(f"UPDATE cars SET {', '.join(sets)} WHERE id = ?", vals)
        conn.commit()
    try:
        from backend.db import incomplete_listings_db as ild

        ild.sync_incomplete_listing_for_car_id(car_id)
    except Exception:
        _log.exception("incomplete_listings sync after partial update failed")
