"""NHTSA vPIC facts outrank the dealer feed for drivetrain and electrification.

Policy (2026-09-23): when the VIN decode STATES a drivetrain or an electrification
level, that value is stored in ``cars.drivetrain`` / ``cars.fuel_type`` and shown on
the page. A blank decode changes nothing. The lab measured why: on 1,611 fresh rows
the dealer feed contradicted the VIN on drivetrain 44 times (4x4 Tacomas listed
"RWD", Ridgelines listed "FWD") and labelled 17 hybrid-only cars "Gasoline"; the
VIN was right every time we checked.

Three call sites share this module:
  * scan write path  (``dealer_run``): override in memory before the upsert for
    VINs already in ``nhtsa_vpic_cache``;
  * post-scan        (``post_scan_vpic``): decode the run's new VINs, then heal
    their rows;
  * fleet heal       (``backend/scripts/heal_from_vpic.py``).

Provenance goes into ``spec_source_json`` under source ``nhtsa_vpic``, one entry
per field with the written ``value`` (the stale-provenance rule in
``backend/vehicle_facts/precedence.py`` needs it). Every accept / refuse decision
below is made by that module (P8A.4): this file gathers the decode's candidates
and persists the winners, it holds no rank rules of its own.

Deliberately NOT overridden:
  * AWD vs 4WD: vPIC says "4WD" for Toyota's AWD crossovers; both mean all four
    wheels, so they are treated as equivalent and the dealer's word stands.
  * 48V mild hybrids: vPIC reports "Mild HEV" or nothing; dealers say "Hybrid",
    EPA says gasoline. No override either way.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
from typing import Any, Iterable

logger = logging.getLogger("scanner")

VPIC_FUEL_LABEL = {"ev": "Electric", "phev": "Plug-In Hybrid", "hybrid": "Hybrid"}
_VPIC_BATCH_URL = "https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVINValuesBatch/"
_VIN_RE = re.compile(r"^[A-HJ-NPR-Z0-9]{17}$")
_BATCH = 50
_PAUSE_S = 0.6
_INLINE_REQUEST_CAP_S = 15.0


def vin_decode_inline_enabled() -> bool:
    """SCANNER_VIN_DECODE_INLINE (default on, D-PR5 a): decode uncached VINs on the
    scan write path. tests/conftest.py sets it to 0."""
    return (os.environ.get("SCANNER_VIN_DECODE_INLINE") or "1").strip().lower() not in ("0", "false", "no", "off")


def vin_decode_inline_budget_s() -> float:
    try:
        return max(0.0, float(os.environ.get("SCANNER_VIN_DECODE_INLINE_BUDGET_S") or 20))
    except ValueError:
        return 20.0


def vin_facts_enabled() -> bool:
    return (os.environ.get("SCANNER_VIN_FACTS") or "1").strip().lower() not in ("0", "false", "no", "off")


def _drive_bucket(s: Any) -> str:
    from backend.vehicle_facts.drivetrain import normalize_drivetrain

    return normalize_drivetrain(s, "dealer") or ""


def _fuel_bucket(s: Any) -> str:
    from backend.catalog.resolver import _fuel_bucket as _b

    return _b(s)


def _drive_same(a: str, b: str) -> bool:
    from backend.vehicle_facts.drivetrain import same_drive_wheels

    return same_drive_wheels(a, b)


def _vpic_plain_gasoline(fuel: Any) -> bool:
    """True when the decode's FuelTypePrimary is gasoline with no electric secondary."""
    from backend.utils.field_clean import coerce_fuel_type_stored

    text = str(fuel or "").strip()
    return bool(text) and "electric" not in text.lower() and coerce_fuel_type_stored(text) == "Gasoline"


def _vin_decisions(row: dict[str, Any], vpic: dict[str, Any] | None) -> dict[str, Any]:
    """{field: precedence.Decision} for every decode value that wins on *row*.

    The candidates are the decode's stated drivetrain / electrification (or
    diesel) / cylinders; whether each may replace the stored value is decided by
    ``precedence.decide`` against the row's provenance (D-PR1/4/6/7)."""
    if not vpic:
        return {}
    from backend.vehicle_facts import precedence as prec
    from backend.vehicle_facts.drivetrain import is_two_wheel_unknown

    prov = row.get("spec_source_json")
    ctx: dict[str, Any] = {"vpic": vpic, "engine_description": row.get("engine_description"),
                           "fuel_type": row.get("fuel_type")}
    out: dict[str, Any] = {}

    def _take(field: str, candidate: Any) -> None:
        d = prec.decide(field, row.get(field), prov, candidate, prec.NHTSA_VPIC, ctx)
        if d.write:
            out[field] = d

    vd = vpic.get("drivetrain")
    raw = vpic.get("drive_type_raw") or vpic.get("drivetrain_raw")
    if raw:
        ctx["vpic_drive"] = raw
    elif vd:
        ctx["vpic_drive"] = vd
    if vd:
        cur_d = _drive_bucket(row.get("drivetrain"))
        if not cur_d or not _drive_same(cur_d, _drive_bucket(vd)):
            _take("drivetrain", vd)  # fill a blank, or fix a contradiction
    elif raw and is_two_wheel_unknown(raw):
        _take("drivetrain", raw)  # "4x2": a constraint, never an end (D-PR1)
    el = vpic.get("electrification")
    if el in VPIC_FUEL_LABEL:
        want = VPIC_FUEL_LABEL[el]
        cur = str(row.get("fuel_type") or "").strip()
        if _fuel_bucket(cur) != _fuel_bucket(want):
            _take("fuel_type", want)
    elif el is None and str(vpic.get("fuel_type") or "") == "Diesel":
        cur = str(row.get("fuel_type") or "").strip()
        if _fuel_bucket(cur) in ("gas", ""):
            _take("fuel_type", "Diesel")
    elif el is None and _vpic_plain_gasoline(vpic.get("fuel_type")):
        # N15.5: fill a blank or non-canonical fuel ("F", "Direct Injection");
        # precedence refuses to replace any stated fuel with it.
        _take("fuel_type", "Gasoline")
    # EngineCylinders: the stored column is the weakest fact on the row (a 2027
    # Buick Enclave 2.5T carried 6, a 2026 Sierra 2.7L I4 carried 8 — both from a
    # model-level backfill; verified 2026-09-25 on hendrickbuickgmccary-com /
    # longofathens-com). The decode is per VIN. EVs have no cylinders: skip when
    # vPIC says it is an EV (electrification 'ev'). The decode counts only while
    # it does not contradict the engine text (D-PR7; precedence enforces it).
    try:
        vc = int(str(vpic.get("cylinders") or "").strip() or 0)
    except ValueError:
        vc = 0
    if 2 <= vc <= 16 and el != "ev":
        try:
            cur_c = int(row.get("cylinders") or 0)
        except (TypeError, ValueError):
            cur_c = 0
        if cur_c != vc:
            if "fuel_type" in out:
                ctx["fuel_type"] = out["fuel_type"].value
            _take("cylinders", vc)
    return out


def vin_overrides(row: dict[str, Any], vpic: dict[str, Any] | None) -> dict[str, tuple[Any, str]]:
    """{field: (old, new)} the decode would change on *row*. Empty when vPIC is
    silent, agrees, or loses to a higher-ranked stored fact. Pure: does not mutate."""
    out: dict[str, tuple[Any, Any]] = {}
    for field, d in _vin_decisions(row, vpic).items():
        old = row.get(field)
        if field == "fuel_type":
            old = str(old or "").strip() or None
        out[field] = (old, d.value)
    return out


def apply_vin_facts_to_row(row: dict[str, Any], vpic: dict[str, Any] | None) -> dict[str, tuple[Any, str]]:
    """Mutate *row* in place; returns what changed."""
    ch = vin_overrides(row, vpic)
    for field, (_old, new) in ch.items():
        row[field] = new
        row[f"_{field}_source"] = "nhtsa_vpic"
    return ch


def override_vehicles(vehicles: list[dict[str, Any]]) -> dict[str, int]:
    """Scan write path: apply the decode to every vehicle whose VIN is cached."""
    # Every key vin_overrides() can return must exist here: when the cylinder
    # override landed (2026-09-25) this dict lacked "cylinders", the first such
    # row raised KeyError, dealer_run logged "VIN facts failed … 'cylinders'" and
    # skipped the whole pass, so rescans wrote the feed's drivetrain/fuel/cylinders
    # back over healed rows (449 log lines by 2026-09-28).
    stats = {"vehicles": len(vehicles), "cached": 0, "drivetrain": 0, "fuel_type": 0, "cylinders": 0}
    if not vin_facts_enabled() or not vehicles:
        return stats
    from backend.enrichment.knowledge_engine import lookup_vpic_from_cache, prime_vpic_cache

    vins = [str(v.get("vin") or "").strip().upper() for v in vehicles]
    if vin_decode_inline_enabled():
        dec = decode_inline(v for v in vins if _VIN_RE.match(v))
        stats["decoded_inline"] = dec.get("stored", 0)
        stats["decode_failed"] = dec.get("failed", 0)
        stats["decode_skipped_budget"] = dec.get("skipped_budget", 0)
    prime_vpic_cache([v for v in vins if _VIN_RE.match(v)])
    for v, vin in zip(vehicles, vins):
        if not _VIN_RE.match(vin):
            continue
        vp = lookup_vpic_from_cache(vin)
        if not vp or not any(vp.get(k) for k in ("drivetrain", "electrification", "fuel_type", "cylinders")):
            continue
        stats["cached"] += 1
        decisions = _vin_decisions(v, vp)
        changed = apply_vin_facts_to_row(v, vp)
        for field in changed:
            stats[field] = stats.get(field, 0) + 1
        if changed:
            _stamp_provenance(v, changed, decisions)
    return stats


def _stamp_provenance(v: dict[str, Any], changed: dict[str, tuple[Any, Any]], decisions: dict[str, Any]) -> None:
    """Merge one ``nhtsa_vpic`` provenance entry per overridden field into
    ``v['spec_source_json']`` so the upsert stores it, ``value`` included (the
    stale-provenance rule reads it back)."""
    from backend.utils.spec_provenance import merge_spec_source_json

    existing = v.get("spec_source_json")
    if isinstance(existing, dict):
        existing = json.dumps(existing)
    patch: dict[str, dict[str, Any]] = {}
    for field, (old, new) in changed.items():
        entry: dict[str, Any] = {"source": "nhtsa_vpic", "detail": "scan-time cached decode",
                                 "previous": old, "value": new,
                                 "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
        d = decisions.get(field)
        if d is not None and d.cannot_assess:
            entry["cannot_assess"] = True
        patch[field] = entry
    v["spec_source_json"] = merge_spec_source_json(existing if isinstance(existing, str) else None, patch)


# --------------------------------------------------------------------------
# decode + heal (post-scan and fleet)
# --------------------------------------------------------------------------

def fetch_vpic_batch(vins: list[str], timeout: float = 90) -> dict[str, dict]:
    """``{vin: {"Results": [row]}}`` shaped like a single-VIN decode so the cache
    stays byte-compatible with ``vpic_specs``."""
    import requests

    resp = requests.post(_VPIC_BATCH_URL, data={"format": "json", "data": ";".join(vins)}, timeout=timeout)
    resp.raise_for_status()
    out: dict[str, dict] = {}
    for row in (resp.json() or {}).get("Results") or []:
        if not isinstance(row, dict):
            continue
        vin = str(row.get("VIN") or "").strip().upper()
        if vin:
            out[vin] = {"Count": 1, "Message": "Batch decode", "Results": [row]}
    return out


def decode_missing_vins(conn, vins: Iterable[str], *, dry_run: bool = False,
                        budget_s: float | None = None) -> dict[str, int]:
    """Decode VINs absent from ``nhtsa_vpic_cache``. Idempotent. A dry run only
    counts: no network call and no cache write (``would_store`` = the VINs a real
    run would decode).

    With *budget_s* (the inline scan path, P8B.4) each request times out at
    ``min(remaining budget, 15 s)``, no batch starts once the budget is spent
    (``skipped_budget`` counts those VINs) and the pause between batches is skipped
    when over budget. Decoded VINs are popped from the lookup memo so the next
    ``lookup_vpic_from_cache`` sees them (never ``clear_vpic_lookup_cache()``)."""
    todo = sorted({str(v or "").strip().upper() for v in vins if _VIN_RE.match(str(v or "").strip().upper())})
    stats = {"requested": len(todo), "already_cached": 0, "stored": 0, "failed": 0}
    if not todo:
        return stats
    cur = conn.cursor()
    have: set[str] = set()
    for i in range(0, len(todo), 900):
        chunk = todo[i:i + 900]
        cur.execute(
            "SELECT vin FROM nhtsa_vpic_cache WHERE vin IN (" + ",".join("?" * len(chunk)) + ")", tuple(chunk)
        )
        have.update(r[0] for r in cur.fetchall())
    stats["already_cached"] = len(have)
    missing = [v for v in todo if v not in have]
    if dry_run:
        stats["would_store"] = len(missing)
        return stats
    deadline = None if budget_s is None else time.monotonic() + budget_s
    for i in range(0, len(missing), _BATCH):
        chunk = missing[i:i + _BATCH]
        remaining = None if deadline is None else deadline - time.monotonic()
        if remaining is not None and remaining <= 0:
            stats["skipped_budget"] = stats.get("skipped_budget", 0) + len(missing) - i
            break
        try:
            if remaining is None:
                decoded = fetch_vpic_batch(chunk)
            else:
                decoded = fetch_vpic_batch(chunk, timeout=min(remaining, _INLINE_REQUEST_CAP_S))
        except Exception as exc:  # noqa: BLE001 - one bad batch must not stop the rest
            stats["failed"] += len(chunk)
            logger.warning("vPIC batch %d failed: %s", i // _BATCH, str(exc)[:120])
            if deadline is None or time.monotonic() < deadline:
                time.sleep(_PAUSE_S)
            continue
        for vin, payload in decoded.items():
            try:
                cur.execute(
                    "INSERT INTO nhtsa_vpic_cache (vin, response_json, fetched_at) VALUES (?, ?, ?) "
                    "ON CONFLICT (vin) DO NOTHING",
                    (vin, json.dumps(payload), time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())),
                )
                if cur.rowcount != 0:  # DO NOTHING on a concurrent insert reports 0
                    stats["stored"] += 1
            except Exception as exc:  # noqa: BLE001
                logger.debug("vPIC store %s failed: %s", vin, exc)
        conn.commit()
        if decoded:
            from backend.enrichment.knowledge_engine import _VPIC_MEMO

            for vin in decoded:
                _VPIC_MEMO.pop(vin, None)
        if deadline is None or time.monotonic() < deadline:
            time.sleep(_PAUSE_S)
    return stats


def decode_inline(vins: Iterable[str]) -> dict[str, int]:
    """Scan write path (P8B.4): decode this run's uncached VINs within
    ``SCANNER_VIN_DECODE_INLINE_BUDGET_S``. Blocking: callers off the event loop.
    Fails open: any error returns the counts so far and the scan carries on."""
    zero = {"requested": 0, "already_cached": 0, "stored": 0, "failed": 0}
    if not vin_decode_inline_enabled():
        return zero
    conn = None
    try:
        from backend.enrichment.knowledge_engine import _conn

        conn = _conn()
        return decode_missing_vins(conn, vins, budget_s=vin_decode_inline_budget_s())
    except Exception as exc:  # noqa: BLE001
        logger.warning("inline vPIC decode failed (continuing): %s", str(exc)[:120])
        return {**zero, "error": 1}
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass


# ErrorCode values the decode may carry and still be a full decode: 0 clean,
# 1 check digit off, 400 invalid character in the serial. Anything else (5, 14,
# ...) is a partial decode, fetched early for a pre-release VIN (INCOMPLETE 2.12).
_FULL_DECODE_CODES = frozenset({"0", "1", "400"})


def _cache_text_time(raw: Any) -> float | None:
    """Epoch seconds of a ``fetched_at`` value; the column mixes ``...Z`` and
    ``YYYY-MM-DD HH:MM:SS+00`` text (and psycopg may hand back a datetime)."""
    import calendar
    from datetime import datetime

    if isinstance(raw, datetime):
        return raw.timestamp() if raw.tzinfo else calendar.timegm(raw.timetuple())
    t = str(raw or "").strip().replace("Z", "+00:00").replace(" ", "T")
    if not t:
        return None
    try:
        dt = datetime.fromisoformat(t)
    except ValueError:
        return None
    return dt.timestamp() if dt.tzinfo else calendar.timegm(dt.timetuple())


def _is_partial_decode(row0: dict[str, Any]) -> bool:
    codes = {c.strip() for c in str(row0.get("ErrorCode") or "").split(",") if c.strip()}
    return bool(codes - _FULL_DECODE_CODES)


def stale_decode_vins(conn, vins: Iterable[str], *, max_age_days: float | None = None, partial: bool = True,
                      min_year: int | None = None, now: float | None = None) -> list[str]:
    """Cached VINs worth decoding again: a partial decode (ErrorCode outside
    0 / 1 / 400) and/or one fetched more than *max_age_days* ago, limited to model
    years ``>= min_year`` when given (the decode of an old model year does not
    change). Read-only."""
    todo = sorted({str(v or "").strip().upper() for v in vins if _VIN_RE.match(str(v or "").strip().upper())})
    now = time.time() if now is None else now
    cur = conn.cursor()
    out: list[str] = []
    for i in range(0, len(todo), 900):
        chunk = todo[i:i + 900]
        cur.execute("SELECT vin, response_json, fetched_at FROM nhtsa_vpic_cache WHERE vin IN ("
                    + ",".join("?" * len(chunk)) + ")", tuple(chunk))
        for vin, raw, fetched in cur.fetchall():
            try:
                payload = json.loads(raw) if isinstance(raw, str) else (raw or {})
                row0 = (payload.get("Results") or [{}])[0]
            except (ValueError, AttributeError, IndexError, TypeError):
                continue
            if not isinstance(row0, dict):
                continue
            if min_year is not None:
                try:
                    if int(str(row0.get("ModelYear") or "0")) < min_year:
                        continue
                except ValueError:
                    continue
            if partial and _is_partial_decode(row0):
                out.append(str(vin).upper())
                continue
            ts = _cache_text_time(fetched)
            if max_age_days is not None and (ts is None or now - ts > max_age_days * 86400):
                out.append(str(vin).upper())
    return sorted(set(out))


def refresh_decodes(conn, vins: Iterable[str], *, max_age_days: float | None = None, partial: bool = True,
                    min_year: int | None = None, dry_run: bool = False) -> dict[str, int]:
    """Decode again the cached VINs ``stale_decode_vins`` selects and replace their
    cache row. A full decode is never replaced by a partial one. A dry run selects
    and counts only: no network call, no cache write."""
    from backend.enrichment.knowledge_engine import clear_vpic_lookup_cache

    todo = stale_decode_vins(conn, vins, max_age_days=max_age_days, partial=partial, min_year=min_year)
    stats = {"selected": len(todo), "refreshed": 0, "kept_full": 0, "failed": 0}
    if dry_run or not todo:
        return stats
    cur = conn.cursor()
    for i in range(0, len(todo), _BATCH):
        chunk = todo[i:i + _BATCH]
        try:
            decoded = fetch_vpic_batch(chunk)
        except Exception as exc:  # noqa: BLE001 - one bad batch must not stop the rest
            stats["failed"] += len(chunk)
            logger.warning("vPIC refresh batch %d failed: %s", i // _BATCH, str(exc)[:120])
            time.sleep(_PAUSE_S)
            continue
        old = _vpic_flat_rows(conn, chunk)
        for vin, payload in decoded.items():
            new0 = (payload.get("Results") or [{}])[0]
            if _is_partial_decode(new0) and vin in old and not _is_partial_decode(old[vin]):
                stats["kept_full"] += 1
                continue
            cur.execute("UPDATE nhtsa_vpic_cache SET response_json = ?, fetched_at = ? WHERE vin = ?",
                        (json.dumps(payload), time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), vin))
            stats["refreshed"] += 1
        conn.commit()
        time.sleep(_PAUSE_S)
    clear_vpic_lookup_cache()  # the lookup memoises an empty decode for the process (INCOMPLETE 2.17)
    return stats


def _vpic_flat_rows(conn, vins: list[str]) -> dict[str, dict[str, str]]:
    """``{vin: Results[0]}`` straight from ``nhtsa_vpic_cache`` for the VINs given
    (the normalised lookup drops the engine / EV fields the derivation needs)."""
    out: dict[str, dict[str, str]] = {}
    if not vins:
        return out
    cur = conn.cursor()
    for i in range(0, len(vins), 900):
        chunk = vins[i:i + 900]
        cur.execute(
            "SELECT vin, response_json FROM nhtsa_vpic_cache WHERE vin IN (" + ",".join("?" * len(chunk)) + ")",
            tuple(chunk),
        )
        for vin, raw in cur.fetchall():
            try:
                payload = json.loads(raw) if isinstance(raw, str) else (raw or {})
                row0 = (payload.get("Results") or [{}])[0]
            except (ValueError, AttributeError, IndexError, TypeError):
                continue
            if isinstance(row0, dict):
                out[str(vin).upper()] = {str(k): ("" if v is None else str(v)) for k, v in row0.items()}
    return out


def _ev_engine_description(flat: dict[str, str]) -> str | None:
    """"Electric motor, Dual Motor, 300 hp / 224 kW" from the EV fields vPIC
    carries (EVDriveUnit / EngineHP / EngineKW); None when it carries none."""
    unit = (flat.get("EVDriveUnit") or "").strip()
    hp = (flat.get("EngineHP") or "").strip()
    kw = (flat.get("EngineKW") or "").strip()
    parts = ["Electric motor"]
    if unit and unit.lower() not in ("not applicable", "n/a"):
        parts.append(unit)
    power = []
    for val, suffix in ((hp, "hp"), (kw, "kW")):
        try:
            f = float(val)
        except ValueError:
            continue
        if f > 0:
            power.append(f"{int(round(f))} {suffix}")
    if power:
        parts.append(" / ".join(power))
    if len(parts) == 1:
        return None
    return ", ".join(parts)[:240]


def derived_fills(row: dict[str, Any], vp: dict[str, Any] | None, flat: dict[str, str] | None) -> dict[str, tuple[Any, str]]:
    """Fill-only derivations from the decode for a row whose engine_description /
    body_style is empty: never overwrites the dealer's own engine text (the
    catalog and the decode's synthesis never outrank it), never touches a
    non-empty body_style. EVs get a synthesised motor line when vPIC has the
    EV fields (F06, 2026-09-28: 6,338 engine-null non-EV rows with
    DisplacementL + EngineCylinders cached; 1,219 body-null rows with BodyClass)."""
    out: dict[str, tuple[Any, str]] = {}
    vp = vp or {}
    flat = flat or {}
    cur_engine = str(row.get("engine_description") or "").strip()
    if not cur_engine and flat:
        from backend.enrichment.nhtsa_vpic import _build_engine_description

        is_ev = vp.get("electrification") == "ev" or _fuel_bucket(row.get("fuel_type")) == "ev"
        text = _ev_engine_description(flat) if is_ev else _build_engine_description(flat)
        if text:
            out["engine_description"] = (row.get("engine_description"), text)
    cur_body = str(row.get("body_style") or "").strip()
    if not cur_body:
        body = vp.get("body_style")
        if not body:
            bc = (flat.get("BodyClass") or "").strip()
            if bc and bc.lower() not in ("", "not applicable"):
                body = bc[:120]
        if body:
            out["body_style"] = (row.get("body_style"), body)
    return out


_HEAL_FLAT_CHUNK = 500  # rows whose raw vPIC decodes are held at once in heal_rows


HEAL_FIELDS = ("drivetrain", "fuel_type", "cylinders", "engine_description", "body_style")


def heal_rows(conn, *, vins: Iterable[str] | None = None, dealers: Iterable[str] | None = None,
              dry_run: bool = False, fields: Iterable[str] | None = None) -> dict[str, Any]:
    """Store the decode's drivetrain / fuel / cylinders on active rows where it
    differs, and fill empty engine_description / body_style from the decode.

    Commits after every ``_HEAL_FLAT_CHUNK`` rows (a long heal keeps its progress
    and never holds one giant transaction). *fields* limits the heal to those
    columns (a subset of ``HEAL_FIELDS``); every write is decided by
    ``backend/vehicle_facts/precedence.py``."""
    from backend.enrichment.knowledge_engine import clear_vpic_lookup_cache, lookup_vpic_from_cache, prime_vpic_cache
    from backend.utils.spec_provenance import merge_spec_source_json

    only = frozenset(fields) if fields is not None else None
    if only is not None and not only <= set(HEAL_FIELDS):
        raise ValueError(f"unknown heal fields: {sorted(only - set(HEAL_FIELDS))}")
    cur = conn.cursor()
    where = "COALESCE(listing_active,1)=1 AND vin IS NOT NULL AND length(vin)=17"
    params: list[Any] = []
    vin_list = sorted({str(v or "").strip().upper() for v in (vins or []) if v})
    dealer_list = [d for d in (dealers or []) if d]
    if vin_list:
        where += " AND vin IN (" + ",".join("?" * len(vin_list)) + ")"
        params += vin_list
    if dealer_list:
        where += " AND dealer_id IN (" + ",".join("?" * len(dealer_list)) + ")"
        params += dealer_list
    cur.execute(f"SELECT c.id, c.vin, c.drivetrain, c.fuel_type, c.cylinders, c.spec_source_json, e.drive, "
                f"c.engine_description, c.body_style FROM cars c "
                f"LEFT JOIN epa_master e ON e.id = c.epa_master_id WHERE {where.replace('listing_active', 'c.listing_active').replace(' vin', ' c.vin').replace('dealer_id', 'c.dealer_id')}", tuple(params))
    rows = [dict(zip(("id", "vin", "drivetrain", "fuel_type", "cylinders", "spec_source_json", "catalog_drive",
                      "engine_description", "body_style"), r)) for r in cur.fetchall()]
    stats: dict[str, Any] = {"rows": len(rows), "decoded": 0, "drivetrain": 0, "fuel_type": 0, "cylinders": 0,
                             "engine_description": 0, "body_style": 0, "examples": []}
    clear_vpic_lookup_cache()
    prime_vpic_cache([r["vin"] for r in rows])
    for start in range(0, len(rows), _HEAL_FLAT_CHUNK):
        chunk = rows[start:start + _HEAL_FLAT_CHUNK]
        # Raw decode rows (Results[0], ~140 keys each) only for the chunk's rows
        # that have something to derive, fetched per chunk so a whole-fleet heal
        # never holds every decode at once.
        need_flat = sorted({r["vin"] for r in chunk
                            if not str(r.get("engine_description") or "").strip() or not str(r.get("body_style") or "").strip()})
        flats = _vpic_flat_rows(conn, need_flat)
        _heal_chunk(cur, chunk, flats, stats, dry_run, lookup_vpic_from_cache, merge_spec_source_json, only)
        if not dry_run:
            conn.commit()
            stats["commits"] = stats.get("commits", 0) + 1
    touched = stats.pop("_touched", [])
    if not dry_run:
        conn.commit()
        stats["resynced"] = _resync_incomplete_index(touched)
    return stats


def _resync_incomplete_index(car_ids: list[int]) -> int:
    """Refresh the incomplete-listings index row of each healed car (N6.2): the
    heal changes fuel / drive after the per-car sync ran, so without this the
    flags stay stale until the next fleet rebuild. Never fails the heal."""
    done = 0
    try:
        from backend.db import incomplete_listings_db as ild

        for cid in car_ids:
            ild.sync_incomplete_listing_for_car_id(int(cid))
            done += 1
    except Exception:
        import logging
        logging.getLogger(__name__).exception("incomplete index re-sync after vPIC heal failed")
    return done


def _heal_chunk(cur, rows: list[dict[str, Any]], flats: dict[str, dict[str, str]], stats: dict[str, Any],
                dry_run: bool, lookup_vpic_from_cache, merge_spec_source_json,
                only: frozenset[str] | None = None) -> None:
    """heal_rows' per-row work for one chunk; ``flats`` holds only this chunk's raw decodes."""
    from backend.vehicle_facts import precedence as prec

    for r in rows:
        vp = lookup_vpic_from_cache(r["vin"]) or {}
        flat = flats.get(r["vin"])
        if not any(vp.get(k) for k in ("drivetrain", "electrification", "fuel_type")) and not flat:
            continue
        stats["decoded"] += 1
        ch = dict(vin_overrides(r, vp))
        sources = {f: "nhtsa_vpic" for f in ch}
        cannot = {f for f, d in _vin_decisions(r, vp).items() if d.cannot_assess and f in ch}
        for field, (old, text) in derived_fills(r, vp, flat).items():
            if field == "engine_description":
                d = prec.decide(field, r.get(field), r.get("spec_source_json"), text, prec.NHTSA_VPIC)
                if not d.write:
                    continue
            ch[field] = (old, text)
            sources[field] = "nhtsa_vpic"
        # A "4x2" decode says two driven wheels and nothing about the end; the
        # feed's "4x2"/"2WD" used to be written as FWD (468 Tacomas / Grand
        # Cherokees on RWD catalog rows, 2026-09-26). When the decode is silent on
        # the end, the linked catalog row's drive is the best evidence there is.
        # Precedence (D-PR4) lets the tiebreak fill a 2WD / blank end only: it never
        # touches an explicit dealer end or an all-wheel claim.
        drive_raw = str(vp.get("drive_type_raw") or vp.get("drivetrain_raw") or "").lower()
        if "drivetrain" not in ch and (
            drive_raw in ("4x2", "2wd")
            or (not vp.get("drivetrain") and str(r.get("drivetrain") or "").upper() in ("2WD", "FWD", "RWD", ""))
        ):
            cat = _drive_bucket(r.get("catalog_drive"))
            cur_d = _drive_bucket(r.get("drivetrain"))
            if cat in ("FWD", "RWD") and cur_d != cat and cur_d not in ("AWD", "4WD"):
                d = prec.decide("drivetrain", r.get("drivetrain"), r.get("spec_source_json"), cat,
                                prec.EPA_TIEBREAK, {"vpic_drive": drive_raw or vp.get("drivetrain")})
                if d.write:
                    ch["drivetrain"] = (r.get("drivetrain"), d.value)
                    sources["drivetrain"] = prec.EPA_TIEBREAK
                    stats["drivetrain_catalog_tiebreak"] = stats.get("drivetrain_catalog_tiebreak", 0) + 1
        if only is not None:
            ch = {k: v for k, v in ch.items() if k in only}
        if not ch:
            continue
        for field in ch:
            stats[field] += 1
        if len(stats["examples"]) < 12:
            stats["examples"].append({"vin": r["vin"], **{k: f"{o} -> {n}" for k, (o, n) in ch.items()}})
        if dry_run:
            continue
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        prov = {k: {"source": sources[k], "detail": f"DecodeVINValuesBatch; feed said {o!r}",
                    "previous": o, "value": n, "fetched_at": now,
                    **({"cannot_assess": True} if k in cannot else {})}
                for k, (o, n) in ch.items()}
        new_src = merge_spec_source_json(r["spec_source_json"] if isinstance(r["spec_source_json"], str) else None, prov)
        sets = ", ".join(f"{k} = ?" for k in ch) + ", spec_source_json = ?"
        cur.execute(f"UPDATE cars SET {sets} WHERE id = ?", tuple(n for (_o, n) in ch.values()) + (new_src, r["id"]))
        stats.setdefault("_touched", []).append(r["id"])


def post_scan_decode(vins: list[str]) -> dict[str, Any]:
    """Post-scan step 1 (P8B.3): decode the run's new VINs, before the dictionary and model_specs stages read them."""
    from backend.db.inventory_db import get_conn

    conn = get_conn()
    try:
        return decode_missing_vins(conn, vins)
    finally:
        conn.close()


def post_scan_heal(vins: list[str]) -> dict[str, Any]:
    """Post-scan last step (P8B.3): heal the run's rows from the decode, after every other fact writer, so vPIC writes last."""
    from backend.db.inventory_db import get_conn

    conn = get_conn()
    try:
        out = heal_rows(conn, vins=vins)
        out.pop("examples", None)
        return out
    finally:
        conn.close()


def post_scan_vpic(vins: list[str]) -> dict[str, Any]:
    """Decode then heal in one call (callers that do not interleave other stages)."""
    return {"decode": post_scan_decode(vins), "heal": post_scan_heal(vins)}
