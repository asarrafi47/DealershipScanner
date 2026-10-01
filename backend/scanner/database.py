"""
Database layer for scanner: connection to inventory.db and vehicle upsert.
"""
import logging
import os
import re
from datetime import datetime, timezone
from typing import Any

from backend.db.inventory_db import ensure_cars_table_columns
from backend.utils.fuel_label_plausibility import is_known_bev_nameplate
from backend.utils.fuel_type_normalize import normalize_fuel_type_for_storage

# ``upsert_vehicles`` is an orchestrator over the named steps in
# ``backend.scanner.upsert``; the price-history helper is re-exported here
# under its historical names.
from backend.scanner.upsert import (
    dedupe_sorted_by_vin,
    guard_window,
    prefetch_existing,
    report_conflicts,
    reset_stats,
    run_post_write_enrichment,
    write_rows,
)
from backend.scanner.upsert.serialize import PRICE_HISTORY_MAX_ENTRIES as _PRICE_HISTORY_MAX_ENTRIES  # noqa: F401
from backend.scanner.upsert.serialize import price_history_json as _build_price_history_json  # noqa: F401

logger = logging.getLogger(__name__)


# Rows to write per transaction in :func:`upsert_vehicles`. One transaction for a
# whole dealer batch held row locks on ``cars`` (and an open snapshot) for the
# entire Python-side normalization of every vehicle -- minutes on a large feed.
# Upserts are idempotent per VIN, so committing in chunks costs nothing on a
# retry and bounds how long the scanner can block a concurrent DDL or reader.
_UPSERT_COMMIT_BATCH = 200

# ---------------------------------------------------------------------------
# VIN ownership guard (2026-09-29 Railway fleet incident)
# ---------------------------------------------------------------------------
# ``cars`` is one row per VIN and the upsert is ON CONFLICT(vin), so the last
# store to write a VIN owns it. The first full Railway fleet run reassigned
# 6,961 VINs (3.6%) to the wrong dealer that way (mbontario-com ->
# mbbeverlyhills-com 1,229, mtnviewnissan-com (CA) -> cleveland-nissan-com (TN)
# 876, ...). A write may therefore NOT move a VIN whose stored row is active and
# was scraped within the guard window under a different dealer_id: that
# vehicle's write is skipped (the stored row is not touched), counted, and
# recorded in ``vin_owner_conflicts``. A genuine transfer (sold between stores)
# still lands once the owner's next scan retires the row or the window lapses.
# No same-store exception: two entity ids for one store (hughwhitehonda-com /
# -net) are not detectable here; the conflict table shows those pairs.
_VIN_OWNER_GUARD_DEFAULT_HOURS = 48.0
_vin_owner_table_ready = False


def vin_owner_guard_hours() -> float:
    """SCANNER_VIN_OWNER_GUARD_HOURS (default 48); 0 (or negative) disables."""
    raw = (os.environ.get("SCANNER_VIN_OWNER_GUARD_HOURS") or "").strip()
    if not raw:
        return _VIN_OWNER_GUARD_DEFAULT_HOURS
    try:
        return max(0.0, float(raw))
    except ValueError:
        return _VIN_OWNER_GUARD_DEFAULT_HOURS


def _parse_scraped_at(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00").replace(" ", "T", 1))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _vin_owned_elsewhere(
    owner_dealer_id: Any,
    listing_active: Any,
    scraped_at: Any,
    claimant_dealer_id: str,
    cutoff: datetime,
) -> bool:
    """True when the stored row is active, fresh (scraped at/after ``cutoff``) and
    owned by a different, non-empty dealer_id than the claimant."""
    owner = str(owner_dealer_id or "").strip()
    if not owner or owner == (claimant_dealer_id or "").strip():
        return False
    try:
        if listing_active is not None and int(listing_active) != 1:
            return False
    except (TypeError, ValueError):
        pass
    ts = _parse_scraped_at(scraped_at)
    return ts is not None and ts >= cutoff


_VIN_OWNER_CONFLICTS_DDL = """
    CREATE TABLE IF NOT EXISTS vin_owner_conflicts (
        vin                 TEXT NOT NULL,
        owner_dealer_id     TEXT NOT NULL,
        claimant_dealer_id  TEXT NOT NULL,
        seen_at             TEXT NOT NULL,
        PRIMARY KEY (vin, owner_dealer_id, claimant_dealer_id)
    )
"""


def record_vin_owner_conflicts(conflicts: list[tuple[str, str, str]], seen_at: str) -> None:
    """Upsert (vin, owner, claimant) rows; ``seen_at`` is the latest sighting.

    One row per pair keeps the table bounded across nightly runs. Runs on its own
    connection AFTER the car writes committed, so a failure here (e.g. a missing
    table on a read-only replica) can never abort or roll back the inventory write.
    """
    global _vin_owner_table_ready
    if not conflicts:
        return
    conn = get_conn()
    try:
        cur = conn.cursor()
        if not _vin_owner_table_ready:
            cur.execute(_VIN_OWNER_CONFLICTS_DDL)
            conn.commit()
            _vin_owner_table_ready = True
        for vin, owner, claimant in conflicts:
            cur.execute(
                """
                INSERT INTO vin_owner_conflicts (vin, owner_dealer_id, claimant_dealer_id, seen_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (vin, owner_dealer_id, claimant_dealer_id)
                DO UPDATE SET seen_at = excluded.seen_at
                """,
                (vin, owner, claimant, seen_at),
            )
        conn.commit()
    except Exception:
        logger.warning("vin_owner_conflicts: could not record %d conflict(s)", len(conflicts), exc_info=True)
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        conn.close()


def _scanner_idle_in_txn_timeout_ms() -> int:
    """
    Backstop for PostgreSQL scanner connections: how long a session may sit
    ``idle in transaction`` before the server kills it. ``0`` disables it.

    This is a guard, not the fix -- the fix is that every code path below closes
    its transaction before doing per-car work (network calls, dictionary
    lookups). It exists because an idle-in-transaction scanner session is what
    parks a queued ``CREATE INDEX`` on ``cars``, and in PostgreSQL a waiting
    strong lock puts every later reader behind it. If the guard ever fires, the
    scan raises loudly instead of silently freezing every car page.
    """
    raw = (os.environ.get("SCANNER_IDLE_IN_TXN_TIMEOUT_MS") or "60000").strip()
    try:
        return max(0, int(float(raw)))
    except (TypeError, ValueError):
        return 60000


_idle_timeout_warned = False


def _apply_idle_in_txn_guard(conn) -> None:
    """Apply :func:`_scanner_idle_in_txn_timeout_ms` to a scanner-owned PG session."""
    global _idle_timeout_warned
    ms = _scanner_idle_in_txn_timeout_ms()
    if ms <= 0:
        return
    # Never touch a borrowed shared read connection: the SET would outlive this
    # call and change the web app's session behaviour.
    if getattr(conn, "_shared", False):
        return
    if getattr(conn, "_backend", None) != "postgres":
        return
    raw = getattr(conn, "_raw", conn)
    try:
        raw.execute(f"SET SESSION idle_in_transaction_session_timeout = {int(ms)}")
        raw.commit()
    except Exception:
        if not _idle_timeout_warned:
            _idle_timeout_warned = True
            logger.warning("could not set idle_in_transaction_session_timeout", exc_info=True)
        try:
            raw.rollback()
        except Exception:
            pass


def get_conn():
    """Use inventory connection settings (WAL + lock wait) so scanner and app agree on ``inventory.db``."""
    from backend.db.inventory_db import get_conn as inventory_get_conn

    conn = inventory_get_conn()
    _apply_idle_in_txn_guard(conn)
    return conn


def _ensure_schema(conn):
    from backend.db.inventory_pg import init_postgres_inventory, is_inventory_postgres

    if is_inventory_postgres():
        raw = getattr(conn, "_raw", conn)
        try:
            init_postgres_inventory(raw)
        except Exception:
            # init_postgres_inventory re-asserts the schema with a short lock_timeout so a
            # queued CREATE INDEX cannot park every other reader behind it (see the docstring
            # there). That makes it *expected* to fail when another session holds a long
            # transaction on `cars` -- which is precisely the situation a second scanner
            # starting mid-run hits. The authoritative schema is the migration chain, so an
            # aborted re-assert must not take the whole scan down with it. The per-process
            # flag stays unset, so a later connection retries.
            logger.warning(
                "inventory schema re-assert skipped (table busy); continuing with the "
                "schema owned by migrations/",
                exc_info=True,
            )
            try:
                raw.rollback()
            except Exception:
                pass
        return

    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS cars (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            vin              TEXT UNIQUE NOT NULL,
            title            TEXT,
            year             INTEGER,
            make             TEXT,
            model            TEXT,
            trim             TEXT,
            price            REAL,
            mileage          INTEGER,
            fuel_type        TEXT,
            cylinders        INTEGER,
            transmission     TEXT,
            drivetrain       TEXT,
            exterior_color   TEXT,
            interior_color   TEXT,
            image_url        TEXT,
            dealer_name      TEXT,
            dealer_url       TEXT,
            dealer_id        TEXT,
            scraped_at       TEXT
        )
    """)
    conn.commit()
    cursor.execute("PRAGMA table_info(cars)")
    cols = [row[1] for row in cursor.fetchall()]
    for col, ctype in [
        ("dealer_id", "TEXT"),
        ("stock_number", "TEXT"),
        ("gallery", "TEXT"),
        ("carfax_url", "TEXT"),
        ("history_highlights", "TEXT"),
        ("msrp", "REAL"),
        ("dealership_registry_id", "INTEGER"),
        ("is_cpo", "INTEGER"),
        ("model_full_raw", "TEXT"),
        ("mpg_city", "INTEGER"),
        ("mpg_highway", "INTEGER"),
    ]:
        if col not in cols:
            logger.info("Adding %s column to cars table", col)
            cursor.execute(f"ALTER TABLE cars ADD COLUMN {col} {ctype}")
            conn.commit()
    ensure_cars_table_columns(cursor)
    conn.commit()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS model_specs (
            make TEXT NOT NULL,
            model TEXT NOT NULL,
            cylinders INTEGER,
            gears INTEGER,
            transmission TEXT,
            drivetrain TEXT,
            body_style TEXT,
            fuel_type TEXT,
            PRIMARY KEY (make, model)
        )
    """)
    # Migrate older model_specs schemas that pre-date drivetrain / body_style / fuel_type columns
    _existing_model_specs_cols = {
        r[1] for r in cursor.execute("PRAGMA table_info(model_specs)").fetchall()
    }
    for _col, _typ in [("drivetrain", "TEXT"), ("body_style", "TEXT"), ("fuel_type", "TEXT")]:
        if _col not in _existing_model_specs_cols:
            cursor.execute(f"ALTER TABLE model_specs ADD COLUMN {_col} {_typ}")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS epa_master (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            epa_vehicle_id INTEGER,
            year INTEGER,
            make TEXT,
            model TEXT,
            cylinders INTEGER,
            displacement REAL,
            trany TEXT,
            drive TEXT,
            fuel_type TEXT
        )
    """)
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_epa_master_lookup ON epa_master(year, make, model)"
    )
    cursor.execute("PRAGMA table_info(epa_master)")
    epa_cols = [row[1] for row in cursor.fetchall()]
    for col, ctype in [
        ("city08", "REAL"),
        ("highway08", "REAL"),
        ("city_e", "REAL"),
        ("highway_e", "REAL"),
        ("atv_type", "TEXT"),
    ]:
        if col not in epa_cols:
            cursor.execute(f"ALTER TABLE epa_master ADD COLUMN {col} {ctype}")
            conn.commit()
    conn.commit()


def drop_unattributable_vehicles(vehicles: list[dict]) -> tuple[list[dict], int]:
    """Remove rows the rooftop-attribution gate refused.

    Every row written here is stamped with the dealer whose site was queried,
    and on a dealer-group platform that is not the dealer who sells the car.
    ``backend.parsers.resolve_rooftop_attribution`` marks such rows with
    ``_rooftop_reject``; this is the write-side enforcement of that decision, so
    a caller that forwards the rejects anyway still cannot store a wrong
    storefront.
    """
    kept = [v for v in vehicles if not v.get("_rooftop_reject")]
    refused = len(vehicles) - len(kept)
    if refused:
        reasons: dict[str, int] = {}
        for v in vehicles:
            reason = v.get("_rooftop_reject")
            if reason:
                reasons[str(reason)] = reasons.get(str(reason), 0) + 1
        logger.warning(
            "upsert_vehicles: refused %d row(s) not attributable to the scanned rooftop (%s)",
            refused,
            ", ".join(f"{k}={n}" for k, n in sorted(reasons.items())),
        )
    return kept, refused


def upsert_vehicles(vehicles: list[dict], stats: dict | None = None) -> int:
    """
    Insert or replace vehicles by vin. Strict de-duplication: one row per VIN
    (same car in 'New' and 'Used' counts once). Uses ON CONFLICT(vin) DO UPDATE.

    VIN ownership guard (see ``vin_owner_guard_hours``): a VIN whose stored row
    is active, scraped within the window, under a different dealer_id is skipped.
    ``stats`` (optional, filled in place and reset on every call so a retried
    write does not double count) receives ``vin_owner_conflicts`` (int),
    ``vin_owner_conflict_vins`` (sorted list) and ``vin_owner_conflict_owners``
    ({owner_dealer_id: n}).
    """
    reset_stats(stats)
    if not vehicles:
        return 0
    vehicles, _refused = drop_unattributable_vehicles(vehicles)
    if not vehicles:
        return 0
    # Sorted by VIN so concurrent writers (fleet shards) lock rows in one order.
    by_vin, vehicles = dedupe_sorted_by_vin(vehicles)
    conn = get_conn()
    count = 0
    try:
        _ensure_schema(conn)
        cursor = conn.cursor()
        now = datetime.utcnow().isoformat() + "Z"
        window = guard_window(vin_owner_guard_hours())
        existing_spec_src, conflicts = prefetch_existing(cursor, by_vin, window, _vin_owned_elsewhere)
        if conflicts:
            vehicles = [v for v in vehicles if (v.get("vin") or "").strip() not in conflicts]
        # The prefetch opened a read transaction; it is fully materialized in
        # ``existing_spec_src`` now, so end it before the per-vehicle work starts.
        conn.commit()
        count = write_rows(
            conn,
            cursor,
            vehicles,
            now=now,
            existing_spec_src=existing_spec_src,
            window=window,
            conflicts=conflicts,
            commit_every=_UPSERT_COMMIT_BATCH,
        )
        conn.commit()
    finally:
        conn.close()
    logger.info("Upserted %d vehicles", count)
    if conflicts:
        report_conflicts(conflicts, by_vin, window.hours, stats, record_vin_owner_conflicts)
    if count > 0:
        run_post_write_enrichment(
            list(by_vin.keys()),
            get_conn=get_conn,
            apply_model_specs_corrections=apply_model_specs_corrections,
        )
    return count


# ---------------------------------------------------------------------------
# Canonical make resolution for model_specs lookups
# Handles scraper-introduced make/model swaps (e.g. make='Wrangler' model='Wrangler')
# ---------------------------------------------------------------------------
_MAKE_FIX_MAP: dict[tuple[str, str], str] = {
    ("wrangler",        "wrangler"):          "Jeep",
    ("gladiator",       "gladiator"):         "Jeep",
    ("grand",           "grand cherokee"):    "Jeep",
    ("grand",           "grand cherokee l"):  "Jeep",
    ("grand",           "grand wagoneer"):    "Jeep",
    ("grand",           "grand wagoneer l"):  "Jeep",
    ("cherokee",        "cherokee"):          "Jeep",
    ("compass",         "compass"):           "Jeep",
    ("1500",            "1500"):              "RAM",
    ("2500",            "2500"):              "RAM",
    ("3500",            "3500"):              "RAM",
    ("classic",         "1500 classic"):      "RAM",
    ("promaster",       "promaster 1500"):    "RAM",
    ("promaster",       "promaster 2500"):    "RAM",
    ("5500hd",          "5500hd"):            "RAM",
    ("durango",         "durango"):           "Dodge",
    ("charger",         "charger"):           "Dodge",
    ("challenger",      "challenger"):        "Dodge",
    ("journey",         "journey"):           "Dodge",
    ("pacifica",        "pacifica"):          "Dodge",
    ("sierra",          "sierra 1500"):       "GMC",
    ("silverado",       "silverado 1500"):    "Chevrolet",
    ("silverado",       "silverado 1500 ltd"):"Chevrolet",
    ("tacoma",          "tacoma"):            "Toyota",
    ("telluride",       "telluride"):         "Kia",
    ("santa",           "santa fe"):          "Hyundai",
    ("santa",           "santa fe sport"):    "Hyundai",
    ("ioniq",           "ioniq 5"):           "Hyundai",
    ("hyundai",         "ioniq 5"):           "Hyundai",
    ("hyundai",         "ioniq 6"):           "Hyundai",
    ("hyundai",         "kona"):              "Hyundai",
    ("f-150",           "f-150"):             "Ford",
    ("explorer",        "explorer"):          "Ford",
    ("mustang",         "mustang"):           "Ford",
    ("escape",          "escape"):            "Ford",
    ("camaro",          "camaro"):            "Chevrolet",
    ("escalade",        "escalade esv"):      "Cadillac",
    ("tahoe",           "tahoe"):             "Chevrolet",
    ("yukon",           "yukon"):             "GMC",
    ("mdx",             "mdx"):               "Acura",
    ("rdx",             "rdx"):               "Acura",
    ("gx",              "gx"):                "Lexus",
    ("cx-50",           "cx-50"):             "Mazda",
    ("rav4",            "rav4"):              "Toyota",
    ("lacrosse",        "lacrosse"):          "Buick",
    ("tiguan",          "tiguan"):            "VW",
    ("forte",           "forte"):             "Kia",
    ("cooper",          "cooper s countryman"):"Mini",
    ("golf",            "golf gti"):          "VW",
    ("accord",          "accord sedan"):      "Honda",
    ("lincoln",         "aviator"):           "Lincoln",
    ("x5",              "x5"):                "BMW",
    ("4",               "4 series"):          "BMW",
}


def _resolve_canonical_make(make: str, model: str) -> str:
    """Return the canonical make for a (make, model) pair, handling scraper swaps."""
    key = (make.strip().lower(), model.strip().lower())
    return _MAKE_FIX_MAP.get(key, make)


def _infer_drivetrain_from_trim(trim: str | None, title: str | None) -> str | None:
    """
    Return AWD/RWD/FWD/4WD based on known drivetrain keywords in trim or title.
    Returns None when nothing conclusive is found (fall back to model_specs default).

    ``vehicle_facts.normalize_drivetrain(..., "text")``. Differences from the old
    inline copy: fused BMW badges read ("xDrive40i" AWD, "sDrive28i" RWD),
    "Four Wheel Drive" reads as 4WD, "4x2"/"2WD" with no end stays None, and
    "Dual Rear Wheel" / a bare "Rear Wheel" is not RWD.
    """
    from backend.vehicle_facts import normalize_drivetrain

    return normalize_drivetrain(f"{trim or ''} {title or ''}", "text")


def apply_model_specs_corrections(
    vins: list[str] | None = None,
    conn: Any | None = None,
    *,
    dry_run: bool = False,
) -> int:
    """
    Fill NULL/empty cylinders, transmission, drivetrain, body_style, and fuel_type
    from the model_specs dictionary.

    For drivetrain: also applies trim-level overrides (xDrive → AWD, sDrive → RWD,
    4MATIC → AWD, Quattro → AWD) which take precedence over the model default.

    Called after every upsert (for the just-inserted VINs) and by the backfill
    script (vins=None to scan all rows). Never overwrites a value the dealer
    already provided.

    When *dry_run* is True, no UPDATE is executed; returns how many rows would be updated.

    Returns the number of rows updated (or that would be updated if *dry_run*).
    """
    owns_conn = conn is None
    if owns_conn:
        conn = get_conn()
    cur = conn.cursor()

    if vins is not None:
        placeholders = ",".join("?" * len(vins))
        cur.execute(
            f"SELECT vin, make, model, trim, title, cylinders, transmission, drivetrain, body_style, fuel_type, year, engine_description "
            f"FROM cars WHERE vin IN ({placeholders})",
            vins,
        )
    else:
        cur.execute(
            "SELECT vin, make, model, trim, title, cylinders, transmission, drivetrain, body_style, fuel_type, year, engine_description "
            "FROM cars"
        )

    rows = cur.fetchall()
    # The SELECT above (``vins=None`` reads the whole fleet) opened a snapshot that
    # the dictionary-lookup loop below would otherwise hold open to the end. The
    # rows are materialized here, so close the read transaction now.
    conn.commit()
    updated = 0

    for (
        vin, raw_make, raw_model, trim, title, cylinders, transmission,
        drivetrain, body_style, fuel_type, year, engine_description,
    ) in rows:
        needs_cyl = cylinders is None or (
            isinstance(cylinders, (int, float)) and int(cylinders) == 0
            and not _is_electric_make_model(raw_make, raw_model, year=year)
        )
        needs_trans = not transmission or str(transmission).strip() == ""
        needs_drive = not drivetrain or str(drivetrain).strip() == ""
        needs_body = not body_style or str(body_style).strip() == ""
        needs_fuel = not fuel_type or str(fuel_type).strip() == ""

        if not any([needs_cyl, needs_trans, needs_drive, needs_body, needs_fuel]):
            continue

        canonical_make = _resolve_canonical_make(raw_make or "", raw_model or "")
        model = (raw_model or "").strip()

        from backend.enrichment.knowledge_engine import _transmission_has_gear_detail
        from backend.enrichment.model_specs_dictionary import lookup_model_specs_dictionary
        from backend.utils.field_clean import coerce_drivetrain_stored

        spec_dict = lookup_model_specs_dictionary(canonical_make, model)
        if not spec_dict:
            continue

        patch: dict[str, object] = {}

        spec_cyl = spec_dict.get("cylinders")
        spec_trans = spec_dict.get("transmission")
        spec_drive = spec_dict.get("drivetrain")
        spec_body = spec_dict.get("body_style")
        spec_fuel = spec_dict.get("fuel_type")

        if needs_cyl and spec_cyl is not None:
            patch["cylinders"] = int(spec_cyl)
        if needs_trans and spec_trans and str(spec_trans).strip():
            # model_specs is one row per make+model with no year column, so a specific
            # gear count (e.g. "10-Speed Automatic") only reflects whichever generation
            # was scraped/seeded and is wrong for the rest of a multi-gen nameplate's
            # run (confirmed: 2000-2016 F-150s got the 2017+ 10-speed spec this way).
            # Only persist the generic form ("Automatic", "CVT"); never guess a gear count.
            _spec_trans_s = str(spec_trans).strip()
            if not _transmission_has_gear_detail(_spec_trans_s):
                patch["transmission"] = _spec_trans_s
        if needs_drive and spec_drive:
            # Trim/title overrides take precedence over model default
            drive_raw = _infer_drivetrain_from_trim(trim, title) or spec_drive
            patch["drivetrain"] = coerce_drivetrain_stored(drive_raw) or str(drive_raw).strip()
        if needs_body and spec_body:
            patch["body_style"] = str(spec_body).strip()
        if needs_fuel and spec_fuel:
            _fuel = str(spec_fuel).strip()
            # Same mild-hybrid guard as the upsert: a nameplate-level dictionary
            # entry must not park a 48V BSG truck on the "Hybrid" fuel facet.
            _fuel = (
                normalize_fuel_type_for_storage(
                    {
                        "make": raw_make,
                        "model": raw_model,
                        "trim": trim,
                        "title": title,
                        "year": year,
                        "fuel_type": _fuel,
                        "engine_description": engine_description,
                    }
                )
                or _fuel
            )
            patch["fuel_type"] = _fuel

        if patch:
            updated += 1
            if not dry_run:
                sets = ", ".join(f"{k}=?" for k in patch)
                cur.execute(
                    f"UPDATE cars SET {sets} WHERE vin=?",
                    (*patch.values(), vin),
                )
                if updated % _UPSERT_COMMIT_BATCH == 0:
                    conn.commit()

    if not dry_run:
        conn.commit()
    if owns_conn:
        conn.close()

    if updated:
        if dry_run:
            logger.info("model_specs corrections (dry-run): %d rows would be updated", updated)
        else:
            logger.info("model_specs corrections: %d rows updated", updated)
    return updated


def _is_electric_make_model(make: str, model: str, year: Any = None) -> bool:
    """
    True for known BEV makes/models where 0 cylinders is correct (so the
    model_specs backfill must not re-fill a gas sibling's count onto them).

    Delegates to the shared nameplate table in ``fuel_label_plausibility`` —
    the old inline list missed the Bolt/Blazer EV/C-HR BEV families, which let
    this backfill undo the EV cylinder heal on the next nightly.
    """
    if is_known_bev_nameplate({"make": make, "model": model, "year": year}):
        return True
    make_u = (make or "").strip().upper()
    model_u = (model or "").strip().upper()
    if make_u == "BMW" and re.search(r"\bI[0-9X]\b", model_u):
        return True
    return make_u == "NIO"
