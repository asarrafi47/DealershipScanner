"""
Database layer for scanner: connection to inventory.db and vehicle upsert.
"""
import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from backend.db.inventory_db import ensure_cars_table_columns
from backend.utils.analytics_ep import apply_ep_from_scanner_dict
from backend.utils.car_serialize import infer_engine_l_for_db
from backend.utils.field_clean import clean_car_row_dict, compute_data_quality_score, is_effectively_empty
from backend.utils.forced_induction import classify_forced_induction_from_car_row
from backend.utils.fuel_label_plausibility import (
    cylinders_override_for_electric_claim,
    is_known_bev_nameplate,
)
from backend.utils.fuel_type_normalize import normalize_fuel_type_for_storage
from backend.utils.interior_color_buckets import interior_color_buckets_json
from backend.utils.in_transit import availability_spec_source_patch
from backend.utils.spec_provenance import merge_spec_source_json

logger = logging.getLogger(__name__)

_PRICE_HISTORY_MAX_ENTRIES = 24

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


def _build_price_history_json(
    existing_raw: Any, prev_price: Any, new_price: Any, scraped_at: str
) -> str | None:
    """Append a ``{date, price}`` snapshot when price changes (or seed on first sight).

    Read-modify-write against the row's own previous JSON; safe for the common case of
    one scan process per VIN. A rare concurrent-write race could drop a snapshot, which
    is acceptable since this is a nice-to-have history, not authoritative price data.
    """
    try:
        history = json.loads(existing_raw) if existing_raw else []
        if not isinstance(history, list):
            history = []
    except (TypeError, ValueError):
        history = []
    try:
        np = float(new_price) if new_price is not None else None
    except (TypeError, ValueError):
        np = None
    if np is None:
        return json.dumps(history) if history else None
    try:
        pp = float(prev_price) if prev_price is not None else None
    except (TypeError, ValueError):
        pp = None
    if not history or (pp is not None and np != pp):
        history.append({"date": scraped_at, "price": np})
    if len(history) > _PRICE_HISTORY_MAX_ENTRIES:
        history = history[-_PRICE_HISTORY_MAX_ENTRIES:]
    return json.dumps(history) if history else None


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
    if stats is not None:
        stats["vin_owner_conflicts"] = 0
        stats["vin_owner_conflict_vins"] = []
        stats["vin_owner_conflict_owners"] = {}
    if not vehicles:
        return 0
    vehicles, _refused = drop_unattributable_vehicles(vehicles)
    if not vehicles:
        return 0
    by_vin = {}
    for v in vehicles:
        vin = (v.get("vin") or "").strip()
        if vin:
            by_vin[vin] = v
    # Sorted by VIN so every concurrent writer (fleet shards) takes row and index
    # locks in the same order; Chapman Ford's upsert died with DeadlockDetected on
    # 2026-09-28 when two shards inserted overlapping rows in feed order.
    vehicles = [by_vin[k] for k in sorted(by_vin)]
    conn = get_conn()
    count = 0
    try:
        _ensure_schema(conn)
        cursor = conn.cursor()
        now = datetime.utcnow().isoformat() + "Z"
        # Existing enrichment provenance per VIN: the ON CONFLICT clause keeps
        # column values via COALESCE but would replace spec_source_json wholesale,
        # orphaning the provenance of every surviving enriched value. Merge the
        # incoming provenance into the stored one instead.
        existing_spec_src: dict[str, str] = {}
        guard_hours = vin_owner_guard_hours()
        guard_on = guard_hours > 0
        guard_cutoff = datetime.now(timezone.utc) - timedelta(hours=guard_hours if guard_on else 0)
        # Same text shape as ``scraped_at`` (isoformat + "Z") so the SQL backstop
        # below compares like with like on both SQLite and Postgres (TEXT column).
        guard_cutoff_iso = guard_cutoff.replace(tzinfo=None).isoformat() + "Z"
        conflicts: dict[str, tuple[str, str]] = {}  # vin -> (owner, claimant)
        vin_keys = list(by_vin.keys())
        for i in range(0, len(vin_keys), 500):
            chunk = vin_keys[i : i + 500]
            placeholders = ",".join("?" * len(chunk))
            cursor.execute(
                "SELECT vin, spec_source_json, dealer_id, listing_active, scraped_at "
                f"FROM cars WHERE vin IN ({placeholders})",
                chunk,
            )
            for row in cursor.fetchall():
                if row[1] is not None and str(row[1]).strip():
                    existing_spec_src[str(row[0])] = str(row[1])
                if guard_on and len(row) >= 5:
                    _vin = str(row[0])
                    _claimant = str((by_vin.get(_vin) or {}).get("dealer_id") or "").strip()
                    if _vin_owned_elsewhere(row[2], row[3], row[4], _claimant, guard_cutoff):
                        conflicts[_vin] = (str(row[2]).strip(), _claimant)
        if conflicts:
            vehicles = [v for v in vehicles if (v.get("vin") or "").strip() not in conflicts]
        # The prefetch above opened a read transaction; it is fully materialized in
        # ``existing_spec_src`` now, so end it before the per-vehicle work starts.
        conn.commit()
        for raw in vehicles:
            merged = apply_ep_from_scanner_dict(dict(raw))
            from backend.parsers.vdp_urls import apply_vehicle_source_url

            apply_vehicle_source_url(merged)
            v = clean_car_row_dict(merged)
            # 48V mild hybrids (Ram 1500 eTorque) arrive labelled "Hybrid" from
            # the feed. Correct the label BEFORE it is stored: the fuel FILTERS
            # (search_cars, facet cascade, nearby counts) read cars.fuel_type
            # directly, so a read-time-only correction leaves the card and the
            # filter disagreeing, and any one-off backfill is overwritten by the
            # next scan of the same dealer.
            _ft_fixed = normalize_fuel_type_for_storage(v)
            if _ft_fixed:
                v["fuel_type"] = _ft_fixed
            # Battery-electric rows arrive with the gas sibling's cylinder count
            # (Toyota C-HR BEV "4") or a feed sentinel (GM "99", nulled by
            # clean_car_row_dict above). Zero the count ONLY when the electric
            # label is plausible; when combustion evidence contradicts it (a gas
            # GX 550 fed as "Electric"), the cylinders ARE the evidence — they
            # are kept, and the label correction above / the read-time display
            # handles the fuel type. Runs AFTER the label normalization so a row
            # it just relabelled to gas/hybrid is no longer an electric claim.
            _cyl_fixed = cylinders_override_for_electric_claim(v)
            if _cyl_fixed is not None:
                v["cylinders"] = _cyl_fixed
            if not v.get("transmission_type") and v.get("transmission"):
                from backend.utils.transmission_normalize import normalize_transmission_standard
                _y = v.get("year")
                _tt, _ = normalize_transmission_standard(
                    v["transmission"],
                    make=v.get("make"),
                    model=v.get("model"),
                    trim=v.get("trim"),
                    title=v.get("title"),
                    year=_y if isinstance(_y, int) else None,
                    vin=v.get("vin"),
                    log_weak=False,
                )
                if _tt:
                    v["transmission_type"] = _tt
            if is_effectively_empty(v.get("engine_l")):
                _eng = infer_engine_l_for_db(v)
                if _eng is not None:
                    v["engine_l"] = _eng
            vin = (v.get("vin") or "").strip()
            if not vin:
                continue
            title = (
                v.get("title")
                or f"{v.get('year') or ''} {v.get('make') or ''} {v.get('model') or ''} {v.get('trim') or ''}".strip()
                or "Unknown vehicle"
            )
            # Price: ensure number (strip $ and , already done in parser); store as int/float
            try:
                price = v.get("price")
                price = int(round(float(price))) if price is not None and str(price).strip() != "" else None
            except (TypeError, ValueError):
                price = None
            if price is not None and price <= 0:
                price = None
            # Mileage: ensure integer; NULL when the feed had no odometer (a
            # stored 0 on a used row hides the gap from the completeness tally
            # and the VDP gap fill, F12 2026-09-28).
            try:
                mileage = v.get("mileage")
                mileage = int(float(str(mileage).replace(",", ""))) if mileage is not None and str(mileage).strip() != "" else None
            except (TypeError, ValueError):
                mileage = None
            try:
                msrp_val = v.get("msrp")
                msrp = int(round(float(msrp_val))) if msrp_val is not None and str(msrp_val).strip() != "" else None
                if msrp is not None and msrp <= 0:
                    msrp = None
            except (TypeError, ValueError):
                msrp = None
            # Gallery: stored as JSON string; always use json.dumps(list)
            gallery = v.get("gallery")
            if isinstance(gallery, list):
                gallery_json = json.dumps(gallery)
            elif gallery is not None and isinstance(gallery, str):
                try:
                    json.loads(gallery)
                    gallery_json = gallery
                except (TypeError, ValueError):
                    gallery_json = "[]"
            else:
                gallery_json = "[]"
            # 360 spin frames: stored as JSON string like gallery (None when absent
            # so the ON CONFLICT keep-if-nonempty clause preserves prior captures).
            spin = v.get("spin_frames")
            if isinstance(spin, list):
                _spin_urls = [str(u).strip() for u in spin if u and str(u).strip().startswith("http")]
                spin_frames_json = json.dumps(_spin_urls) if _spin_urls else None
            elif isinstance(spin, str) and spin.strip():
                try:
                    _parsed_spin = json.loads(spin)
                    spin_frames_json = spin if isinstance(_parsed_spin, list) and _parsed_spin else None
                except (TypeError, ValueError):
                    spin_frames_json = None
            else:
                spin_frames_json = None
            # Interior panorama: single URL string or None.
            interior_pano = v.get("interior_pano")
            interior_pano = interior_pano.strip() if isinstance(interior_pano, str) else None
            if not interior_pano or not interior_pano.startswith("http"):
                interior_pano = None
            highlights = v.get("history_highlights")
            highlights_json = json.dumps(highlights) if isinstance(highlights, list) else (highlights if isinstance(highlights, str) else "[]")
            img = v.get("image_url")
            if not img or not str(img).strip().startswith("http"):
                img = "/static/placeholder.svg"
            preview = {
                **v,
                "vin": vin,
                "title": title,
                "price": price,
                "mileage": mileage,
                "image_url": img,
            }
            dq = compute_data_quality_score(preview)
            interior_buckets_json = interior_color_buckets_json(v.get("interior_color"), v.get("make"))
            spec_src = v.get("spec_source_json")
            avail_patch = availability_spec_source_patch(v)
            if avail_patch:
                spec_src = merge_spec_source_json(
                    spec_src if isinstance(spec_src, str) else (json.dumps(spec_src) if isinstance(spec_src, dict) else None),
                    avail_patch,
                )
            lot_loc = str(v.get("_lot_location") or v.get("_inventory_location") or "").strip()
            if lot_loc:
                spec_src = merge_spec_source_json(
                    spec_src if isinstance(spec_src, str) else (json.dumps(spec_src) if isinstance(spec_src, dict) else None),
                    {
                        "inventory_lot_location": {
                            "source": str(v.get("_lot_location_source") or "inventory")[:40],
                            "value": lot_loc[:200],
                        },
                    },
                )
            elif isinstance(spec_src, dict):
                spec_src = json.dumps(spec_src, ensure_ascii=False)
            elif spec_src is not None and not isinstance(spec_src, str):
                spec_src = str(spec_src)
            prior_spec_src = existing_spec_src.get(vin)
            if prior_spec_src and spec_src and str(spec_src).strip():
                try:
                    _new_prov = json.loads(spec_src)
                except (json.JSONDecodeError, TypeError):
                    _new_prov = None
                if isinstance(_new_prov, dict):
                    spec_src = merge_spec_source_json(prior_spec_src, _new_prov)
            pkg_raw = v.get("packages")
            if isinstance(pkg_raw, dict):
                packages_json = json.dumps(pkg_raw, ensure_ascii=False)
            elif isinstance(pkg_raw, str) and pkg_raw.strip() not in ("", "{}", "[]", "null"):
                packages_json = pkg_raw.strip()
            else:
                packages_json = None
            fi = v.get("forced_induction") or classify_forced_induction_from_car_row(v)
            try:
                cursor.execute("SELECT price, price_provenance_json FROM cars WHERE vin = ?", (vin,))
                _prev_row = cursor.fetchone()
            except Exception:
                _prev_row = None
            price_history_json = _build_price_history_json(
                _prev_row[1] if _prev_row else None,
                _prev_row[0] if _prev_row else None,
                price,
                now,
            )
            cursor.execute(
                """
                INSERT INTO cars (
                    vin, title, year, make, model, trim, price, mileage,
                    image_url, dealer_name, dealer_url, dealer_id, scraped_at,
                    fuel_type, cylinders, transmission, transmission_type, drivetrain,
                    exterior_color, interior_color, interior_color_buckets, stock_number, gallery, carfax_url, history_highlights, msrp,
                    dealership_registry_id,
                    source_url, body_style, engine_description, engine_l, condition, description, data_quality_score,
                    mpg_city, mpg_highway, is_cpo, model_full_raw,
                    packages,
                    listing_active, listing_removed_at, spec_source_json,
                    first_seen_at, last_price_change_at, forced_induction, price_provenance_json,
                    spin_frames, interior_pano
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(vin) DO UPDATE SET
                    title=CASE
                        WHEN NULLIF(TRIM(excluded.title),'') IS NOT NULL AND excluded.title != 'Unknown vehicle'
                        THEN excluded.title
                        ELSE COALESCE(NULLIF(TRIM(cars.title),''), excluded.title)
                    END,
                    year=CASE WHEN IFNULL(excluded.year,0)!=0 THEN excluded.year ELSE COALESCE(cars.year,excluded.year) END,
                    make=COALESCE(NULLIF(TRIM(excluded.make),''), cars.make),
                    model=COALESCE(NULLIF(TRIM(excluded.model),''), cars.model),
                    trim=COALESCE(excluded.trim, cars.trim),
                    price=CASE WHEN COALESCE(excluded.price, 0) > 0 THEN excluded.price ELSE cars.price END,
                    -- feed odometer > 0 wins; an explicit 0 keeps a prior real reading
                    -- (else 0: new cars are 0 mi); NULL (no odometer) keeps a prior
                    -- real reading but replaces a stale default 0 with NULL (F12)
                    mileage=CASE WHEN IFNULL(excluded.mileage,0) > 0 THEN excluded.mileage
                                 WHEN excluded.mileage IS NOT NULL THEN COALESCE(NULLIF(cars.mileage,0), 0)
                                 ELSE NULLIF(cars.mileage,0) END,
                    image_url=CASE
                        WHEN excluded.image_url LIKE 'http%' THEN excluded.image_url
                        WHEN cars.image_url LIKE 'http%' THEN cars.image_url
                        ELSE excluded.image_url
                    END,
                    dealer_name=excluded.dealer_name, dealer_url=excluded.dealer_url,
                    dealer_id=excluded.dealer_id, scraped_at=excluded.scraped_at,
                    fuel_type=COALESCE(excluded.fuel_type, cars.fuel_type),
                    cylinders=COALESCE(excluded.cylinders, cars.cylinders),
                    transmission=COALESCE(excluded.transmission, cars.transmission),
                    transmission_type=COALESCE(excluded.transmission_type, cars.transmission_type),
                    drivetrain=COALESCE(excluded.drivetrain, cars.drivetrain),
                    exterior_color=COALESCE(NULLIF(TRIM(excluded.exterior_color), ''), cars.exterior_color),
                    interior_color=COALESCE(NULLIF(TRIM(excluded.interior_color), ''), cars.interior_color),
                    interior_color_buckets=CASE
                        WHEN NULLIF(TRIM(excluded.interior_color), '') IS NOT NULL THEN excluded.interior_color_buckets
                        ELSE cars.interior_color_buckets
                    END,
                    stock_number=COALESCE(NULLIF(excluded.stock_number, ''), cars.stock_number),
                    gallery=COALESCE(
                        NULLIF(NULLIF(TRIM(excluded.gallery), ''), '[]'),
                        cars.gallery
                    ),
                    -- Carfax link, history highlights and MSRP come from the VDP (or a
                    -- window sticker), never from the SRP card. An SRP-only refresh of
                    -- the same VIN therefore arrives with carfax_url=NULL,
                    -- history_highlights='[]' and msrp=NULL. Assigning excluded.* here
                    -- wiped all three on every such rescan, against this statement's
                    -- stated contract that an empty incoming value never overwrites a
                    -- stored one.
                    carfax_url=COALESCE(NULLIF(TRIM(excluded.carfax_url), ''), cars.carfax_url),
                    history_highlights=COALESCE(
                        NULLIF(NULLIF(TRIM(excluded.history_highlights), ''), '[]'),
                        cars.history_highlights
                    ),
                    msrp=COALESCE(excluded.msrp, cars.msrp),
                    dealership_registry_id=COALESCE(excluded.dealership_registry_id, cars.dealership_registry_id),
                    source_url=COALESCE(excluded.source_url, cars.source_url),
                    body_style=COALESCE(excluded.body_style, cars.body_style),
                    engine_description=COALESCE(excluded.engine_description, cars.engine_description),
                    engine_l=COALESCE(NULLIF(TRIM(excluded.engine_l), ''), cars.engine_l),
                    condition=COALESCE(NULLIF(TRIM(excluded.condition), ''), cars.condition),
                    description=COALESCE(excluded.description, cars.description),
                    -- Scored off the INCOMING payload, but every column this score
                    -- reads (title/year/make/model/trim/price/mileage/transmission/
                    -- drivetrain/fuel_type/colors/image/engine/mpg -- see
                    -- ``compute_data_quality_score``) is keep-if-nonempty above, so the
                    -- stored row's field set never shrinks through this statement.
                    -- Assigning excluded.* therefore let an SRP-only rescan drop the
                    -- score of a row that still holds every field it was scored on
                    -- (measured 95.37 -> 54.63), and ``hybrid_search`` ranks on this
                    -- column. Take the better of the two; a path that genuinely CLEARS
                    -- fields re-derives the score via refresh_car_data_quality_score.
                    data_quality_score=CASE
                        WHEN COALESCE(excluded.data_quality_score, 0) > COALESCE(cars.data_quality_score, 0)
                        THEN excluded.data_quality_score
                        ELSE cars.data_quality_score
                    END,
                    mpg_city=COALESCE(excluded.mpg_city, cars.mpg_city),
                    mpg_highway=COALESCE(excluded.mpg_highway, cars.mpg_highway),
                    is_cpo=COALESCE(excluded.is_cpo, cars.is_cpo),
                    model_full_raw=COALESCE(excluded.model_full_raw, cars.model_full_raw),
                    packages=COALESCE(NULLIF(TRIM(excluded.packages), ''), cars.packages),
                    listing_active=1,
                    listing_removed_at=NULL,
                    spec_source_json=CASE
                        WHEN excluded.spec_source_json IS NOT NULL AND length(trim(excluded.spec_source_json)) > 0
                        THEN excluded.spec_source_json
                        ELSE cars.spec_source_json
                    END,
                    first_seen_at=COALESCE(cars.first_seen_at, excluded.scraped_at),
                    -- Stamp only when the stored price ACTUALLY moves. The price
                    -- column above keeps its stored value when the incoming price is
                    -- missing (hidden price / "call for price" / a feed that dropped
                    -- the field), but the old condition compared
                    -- COALESCE(excluded.price, 0) and so read a missing price as a
                    -- change to 0 -- stamping "repriced today" on a row whose price
                    -- it had just decided not to touch. ``merchandising.py`` anchors
                    -- price aging on this column, so those rows read as permanently
                    -- just-repriced.
                    last_price_change_at=CASE
                        WHEN COALESCE(excluded.price, 0) > 0
                             AND COALESCE(cars.price, 0) != excluded.price
                        THEN excluded.scraped_at
                        ELSE COALESCE(cars.last_price_change_at, cars.first_seen_at, excluded.scraped_at)
                    END,
                    internal_notes=cars.internal_notes,
                    marked_for_review=cars.marked_for_review,
                    forced_induction=COALESCE(excluded.forced_induction, cars.forced_induction),
                    price_provenance_json=COALESCE(excluded.price_provenance_json, cars.price_provenance_json),
                    spin_frames=COALESCE(NULLIF(NULLIF(TRIM(excluded.spin_frames), ''), '[]'), cars.spin_frames),
                    interior_pano=COALESCE(NULLIF(TRIM(excluded.interior_pano), ''), cars.interior_pano)
                -- VIN ownership guard, atomic backstop for the prefetch check above
                -- (a concurrent shard may have claimed the VIN since): never move an
                -- active, fresh row to a different dealer_id.
                WHERE ? = 0
                   OR COALESCE(cars.dealer_id, '') = ''
                   OR cars.dealer_id = excluded.dealer_id
                   OR COALESCE(cars.listing_active, 1) != 1
                   OR cars.scraped_at IS NULL
                   OR cars.scraped_at < ?
                """,
                (
                    vin,
                    title,
                    v.get("year"),
                    v.get("make") or "",
                    v.get("model") or "",
                    v.get("trim"),
                    price,
                    mileage,
                    img,
                    v.get("dealer_name") or "",
                    v.get("dealer_url"),
                    v.get("dealer_id") or "",
                    now,
                    v.get("fuel_type"),
                    v.get("cylinders"),
                    v.get("transmission"),
                    v.get("transmission_type"),
                    v.get("drivetrain"),
                    v.get("exterior_color"),
                    v.get("interior_color"),
                    interior_buckets_json,
                    v.get("stock_number") or "",
                    gallery_json,
                    v.get("carfax_url"),
                    highlights_json,
                    msrp,
                    v.get("dealership_registry_id"),
                    v.get("source_url"),
                    v.get("body_style"),
                    v.get("engine_description"),
                    v.get("engine_l"),
                    v.get("condition"),
                    v.get("description"),
                    dq,
                    v.get("mpg_city"),
                    v.get("mpg_highway"),
                    v.get("is_cpo"),
                    v.get("model_full_raw"),
                    packages_json,
                    1,
                    None,
                    spec_src,
                    now,
                    now,
                    fi,
                    price_history_json,
                    spin_frames_json,
                    interior_pano,
                    1 if guard_on else 0,
                    guard_cutoff_iso,
                ),
            )
            if guard_on and getattr(cursor, "rowcount", 1) == 0:
                # Backstop fired: another writer owns this VIN (fresh, active).
                _owner = ""
                try:
                    cursor.execute("SELECT dealer_id FROM cars WHERE vin = ?", (vin,))
                    _r = cursor.fetchone()
                    _owner = str(_r[0] or "").strip() if _r else ""
                except Exception:
                    pass
                conflicts[vin] = (_owner, str(v.get("dealer_id") or "").strip())
                continue
            count += 1
            if count % _UPSERT_COMMIT_BATCH == 0:
                conn.commit()
            trace_vin = (os.environ.get("SCANNER_TRACE_VIN") or "").strip().upper()
            if trace_vin and vin.upper() == trace_vin[:17]:
                cursor.execute(
                    "SELECT transmission, drivetrain, interior_color, exterior_color, fuel_type, "
                    "body_style, engine_description, cylinders, mpg_city, mpg_highway, trim "
                    "FROM cars WHERE vin = ?",
                    (vin,),
                )
                rb = cursor.fetchone()
                logger.info(
                    "UPSERT VERIFY VIN %s mem: tr=%r drv=%r int=%r ext=%r fuel=%r | DB: %s",
                    vin[:17],
                    v.get("transmission"),
                    v.get("drivetrain"),
                    v.get("interior_color"),
                    v.get("exterior_color"),
                    v.get("fuel_type"),
                    rb,
                )
        conn.commit()
    finally:
        conn.close()
    logger.info("Upserted %d vehicles", count)
    if conflicts:
        for _vin in conflicts:
            by_vin.pop(_vin, None)  # post-write steps below must not touch the owner's row
        by_owner: dict[str, int] = {}
        by_claimant: dict[str, int] = {}
        for owner, claimant in conflicts.values():
            by_owner[owner] = by_owner.get(owner, 0) + 1
            by_claimant[claimant] = by_claimant.get(claimant, 0) + 1
        logger.warning(
            "vin_owner_guard %s",
            json.dumps(
                {
                    "skipped": len(conflicts),
                    "claimants": by_claimant,
                    "owners": by_owner,
                    "window_hours": guard_hours,
                },
                separators=(",", ":"),
                ensure_ascii=False,
            ),
        )
        record_vin_owner_conflicts(
            [(vin, owner, claimant) for vin, (owner, claimant) in sorted(conflicts.items())],
            datetime.utcnow().isoformat() + "Z",
        )
        if stats is not None:
            stats["vin_owner_conflicts"] = len(conflicts)
            stats["vin_owner_conflict_vins"] = sorted(conflicts)
            stats["vin_owner_conflict_owners"] = by_owner
    if count > 0:
        try:
            from backend.db import incomplete_listings_db as ild
            from backend.db.inventory_db import get_car_by_id
            from backend.enrichment.spec_structured_backfill import (
                apply_structured_spec_backfill_for_car,
                car_needs_transmission_or_cylinders_backfill,
            )

            # Resolve every VIN -> id up front and RELEASE the connection before the
            # per-car work below. That work does network I/O (NHTSA vPIC) and opens
            # its own connections; the previous shape held this one transaction open
            # for the whole loop, which is the `idle in transaction` session that
            # parked the web app behind a queued CREATE INDEX on `cars`.
            car_ids: list[int] = []
            conn2 = get_conn()
            try:
                cur2 = conn2.cursor()
                vin_keys2 = list(by_vin.keys())
                for i in range(0, len(vin_keys2), 500):
                    chunk = vin_keys2[i : i + 500]
                    placeholders = ",".join("?" * len(chunk))
                    cur2.execute(
                        f"SELECT id FROM cars WHERE vin IN ({placeholders})", chunk
                    )
                    for row_id in cur2.fetchall():
                        try:
                            car_ids.append(int(row_id[0]))
                        except (TypeError, ValueError):
                            continue
                conn2.commit()
            finally:
                conn2.close()
            for cid in car_ids:
                ild.sync_incomplete_listing_for_car_id(cid)
                car = get_car_by_id(cid, include_inactive=True)
                if not car or not car_needs_transmission_or_cylinders_backfill(car):
                    continue
                # EPA/trim merge (tier 1) + NHTSA vPIC (tier 2) for transmission/cylinders
                # (and other vPIC fillable fields still open on the row).
                apply_structured_spec_backfill_for_car(cid, use_vpic_cache=True)
        except Exception:
            logger.exception("incomplete_listings / spec backfill after upsert failed")

        # --- model_specs dictionary correction (cylinders + transmission) ---
        try:
            apply_model_specs_corrections(vins=list(by_vin.keys()))
        except Exception:
            logger.exception("model_specs correction after upsert failed")

        # --- catalog link (cars.epa_master_id) so new scans join the catalog
        #     immediately instead of waiting for the batch linker ---
        try:
            from backend.catalog.linker import link_cars_by_vins

            link_cars_by_vins(list(by_vin.keys()))
        except Exception:
            logger.exception("catalog linking after upsert failed")

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
    """
    blob = f"{trim or ''} {title or ''}".upper()
    # AWD signals
    if re.search(r"\b(XDRIVE|4MATIC|QUATTRO|SH-AWD|AWD|ALL[\s-]WHEEL)\b", blob):
        return "AWD"
    # 4WD truck signals
    if re.search(r"\b(4X4|4WD)\b", blob):
        return "4WD"
    # RWD signals
    if re.search(r"\b(SDRIVE|RWD|REAR[\s-]WHEEL)\b", blob):
        return "RWD"
    # FWD signals
    if re.search(r"\b(FWD|FRONT[\s-]WHEEL)\b", blob):
        return "FWD"
    return None


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
