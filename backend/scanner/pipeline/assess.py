"""Step 4: verdict per dealer from scan_runs + row counts, row-by-row verification, timing. Moved verbatim from backend/scripts/dealer_pipeline.py (audit F11)."""
from __future__ import annotations

import json
from typing import Any

from backend.scanner.pipeline.constants import (
    DISCREPANCY_FLOOR,
    FIELD_FLOOR,
    INCOMPLETE_FLOOR,
    KEY_FIELDS,
    MILEAGE_COVERAGE_KEYS,
    MIN_ROWS_UNKNOWN,
    ROW_FLOOR,
    SECONDARY_FIELDS,
)
from backend.scanner.pipeline.db import _rows


def _one_condition_ok() -> set[str]:
    """Dealer ids whose lot legitimately carries a single condition (used-only
    independents, new-only fleet/RV sellers). Thin wrapper over the one loader,
    ``recipe_validation.one_condition_ok_ids`` (tracked list plus the workspace file)."""
    from backend.scanner.recipe_validation import one_condition_ok_ids
    return one_condition_ok_ids()


# --------------------------------------------------------------------------
# 4. assess
# --------------------------------------------------------------------------

def record_timing(dealer_id: str, run: dict[str, Any]) -> dict[str, Any]:
    """Fold this run's timing into the dealer's fingerprint (``scan_hints.timing``,
    backend/scanner/scan_timing.py) and return the triage view of it:
    {minutes, flags, vdp_http_first_max_sec, pages_needed}. Never raises — the
    assessment must not fail because the recipe store was unreachable."""
    from backend.scanner.scan_timing import minutes as _minutes, timing_entry

    view: dict[str, Any] = {"minutes": round(float(run.get("duration_seconds") or 0) / 60, 1), "flags": []}
    try:
        entry = timing_entry(run)
        view["minutes"], view["flags"] = _minutes(entry), list(entry["flags"])
        from backend.scanner.recipe_store import get_scan_hints, set_scan_hints
        from backend.scanner.scan_timing import merge_timing

        merged = merge_timing(get_scan_hints(dealer_id), entry)
        view["vdp_http_first_max_sec"] = merged.get("vdp_http_first_max_sec")
        view["pages_needed"] = merged.get("pages_needed")
        view["stored"] = bool(set_scan_hints(dealer_id, {"timing": merged}, merge=True))
    except Exception as exc:  # noqa: BLE001
        view["error"] = str(exc)[:120]
    return view


# VIN ownership guard (2026-09-29): a scan that tried to take more than this many
# VINs another dealer owns (active, fresh) is named in the triage reason — it is
# either replaying a group feed or a second entity id for the owner's store.
VIN_OWNER_CONFLICT_REASON_MIN = 10

# N9.12: "ok" needs the run to hold its ground against the previous scan. kearnymesasubaru
# fell 773 -> 90 rows (a host-exhausted VDP phase) and was still called ok.
COVERAGE_DROP_FLOOR = 0.30


def _safe_rows(conn, sql: str, params: tuple = ()) -> list[dict[str, Any]] | None:
    """``_rows`` that answers None on a missing table or column (a report must not break the
    assessment) and leaves a Postgres connection usable."""
    try:
        return _rows(conn, sql, params)
    except Exception:  # noqa: BLE001
        try:
            conn.rollback()
        except Exception:  # noqa: BLE001
            pass
        return None


def account_row(r: dict[str, Any]) -> dict[str, int]:
    """The honest row accounting of one assessed dealer, from the new keys or, for a triage
    written before N9.12, from ``rows`` (the feed before the guard) and the refusal counts:
    ``captured - refused_vin_owner - refused_rooftop = written``. An error dealer without a
    scan_run counts as zeros, not null; its ``active_after`` still counts."""
    captured = int(r.get("rows_captured") if r.get("rows_captured") is not None else (r.get("rows") or 0))
    vin_owner = int(r.get("rows_refused_vin_owner") if r.get("rows_refused_vin_owner") is not None else (r.get("vin_owner_conflicts") or 0))
    rooftop = int(r.get("rows_refused_rooftop") if r.get("rows_refused_rooftop") is not None else (r.get("filtered_count") or 0))
    written = int(r["rows_written"]) if r.get("rows_written") is not None else max(0, captured - vin_owner - rooftop)
    return {
        "rows_captured": captured, "rows_refused_vin_owner": vin_owner, "rows_refused_rooftop": rooftop,
        "rows_written": written, "active": int(r.get("active_after") or 0), "inactive": int(r.get("inactive_after") or 0),
    }


def fleet_accounting(results: list[dict[str, Any]]) -> dict[str, int]:
    """Sum of :func:`account_row` over a fleet: the numbers the FLEET SUMMARY and triage print."""
    tot = {"rows_captured": 0, "rows_refused_vin_owner": 0, "rows_refused_rooftop": 0, "rows_written": 0, "active": 0, "inactive": 0}
    for r in results:
        for k, v in account_row(r).items():
            tot[k] += v
    return tot


GROUP_FEED_MIN_CONFLICTS = 25
GROUP_FEED_MIN_SHARE = 0.20


def group_feed_flag(r: dict[str, Any]) -> dict[str, Any] | None:
    """``{sister, conflicts, share, captured}`` when >= 25 of the run's VIN-owner conflicts and
    >= 20% of its feed are against one sister store (a group feed the rows are not stamped for)."""
    owners = r.get("vin_owner_conflict_owners") or {}
    captured = int(r.get("rows_captured") if r.get("rows_captured") is not None else (r.get("rows") or 0))
    if not owners or captured <= 0:
        return None
    sister, n = max(owners.items(), key=lambda kv: int(kv[1]))
    n = int(n)
    if n >= GROUP_FEED_MIN_CONFLICTS and n / captured >= GROUP_FEED_MIN_SHARE:
        return {"sister": sister, "conflicts": n, "share": round(n / captured, 3), "captured": captured}
    return None


def handover_pending(conn, dealer_id: str, since_iso: str) -> dict[str, Any] | None:
    """B4: VINs this dealer retired (inactive) that another store claimed fresh (a
    ``vin_owner_conflicts`` row naming this dealer as owner, seen since the run started). The
    sale or transfer is half done: the old store has let go and the new one is refused."""
    rows = _safe_rows(
        conn,
        "SELECT DISTINCT c.vin AS vin, v.claimant_dealer_id AS claimant FROM cars c "
        "JOIN vin_owner_conflicts v ON v.vin = c.vin AND v.owner_dealer_id = c.dealer_id "
        "WHERE c.dealer_id = ? AND COALESCE(c.listing_active,1) = 0 AND v.seen_at >= ?",
        (dealer_id, since_iso),
    )
    if not rows:
        return None
    claimants: dict[str, int] = {}
    for x in rows:
        claimants[str(x["claimant"])] = claimants.get(str(x["claimant"]), 0) + 1
    return {"vins": len(rows), "examples": sorted(str(x["vin"]) for x in rows)[:5], "claimants": claimants}


def _new_arrival_rate(conn, dealer_id: str, since_iso: str) -> dict[str, Any] | None:
    """Of the cars first seen in this run, how many sit in the incomplete-listings index (IL-13).
    The index is rebuilt after the fleet, so this is the figure as of the assessment."""
    rows = _safe_rows(
        conn,
        "SELECT COUNT(*) AS n, COUNT(il.car_id) AS incomplete FROM cars c "
        "LEFT JOIN incomplete_listings il ON il.car_id = c.id "
        "WHERE c.dealer_id = ? AND COALESCE(c.listing_active,1) = 1 AND c.first_seen_at >= ?",
        (dealer_id, since_iso),
    )
    if not rows or not rows[0]["n"]:
        return None
    n, inc = int(rows[0]["n"]), int(rows[0]["incomplete"] or 0)
    return {"rows": n, "incomplete": inc, "rate": round(inc / n, 4)}


def _store_new_arrival(conn, run_id: Any, summary: dict[str, Any], rate: dict[str, Any]) -> None:
    """Fold the new-arrival rate into the run's ``summary_json`` (best effort)."""
    try:
        summary = dict(summary, new_arrival_incomplete=rate)
        conn.execute("UPDATE scan_runs SET summary_json = ? WHERE id = ?", (json.dumps(summary, default=str), run_id))
        conn.commit()
    except Exception:  # noqa: BLE001
        try:
            conn.rollback()
        except Exception:  # noqa: BLE001
            pass


def assess(conn, dealer_id: str, since_iso: str, known_before: int, recipe_info: dict[str, Any]) -> dict[str, Any]:
    out = _assess(conn, dealer_id, since_iso, known_before, recipe_info)
    n = int(out.get("vin_owner_conflicts") or 0)
    if n > VIN_OWNER_CONFLICT_REASON_MIN and "VIN-owner guard" not in str(out.get("reason") or "") \
            and "VIN owner guard" not in str(out.get("reason") or ""):
        owners = out.get("vin_owner_conflict_owners") or {}
        top = ", ".join(f"{o} {c}" for o, c in sorted(owners.items(), key=lambda kv: -kv[1])[:3])
        note = f"VIN owner guard: {n} VINs owned by other dealers not written" + (f" ({top})" if top else "")
        out["reason"] = f"{out['reason']}; {note}" if out.get("reason") else note
    gap = out.get("replay_persist_gap")
    if gap:
        by = ", ".join(f"{k} {v}" for k, v in (gap.get("dropped_by") or {}).items())
        note = f"replay->persist gap: {gap.get('persisted')} written of {gap.get('kept')} VINs the replay kept" + (f" ({by})" if by else "")
        out["reason"] = f"{out['reason']}; {note}" if out.get("reason") else note
    return out


def _truncation_text(replay: dict[str, Any]) -> str:
    """Why the walk report calls the replay truncated: the first live walk that did not finish
    (its stop reason and what it had read of the site total), else the error that killed it."""
    for w in replay.get("recipes") or replay.get("walks") or []:
        if w.get("complete") or (w.get("stale_retry") and not w.get("raw_vins")):
            continue
        total = w.get("total_count")
        return (f"{w.get('end') or 'unknown'} after {w.get('pages', '?')} page(s), "
                f"{w.get('raw_vins', '?')}" + (f" of {total}" if total else "") + " VINs")
    return str(replay.get("error") or "walk did not finish")[:120]


def _owner_text(owners: dict[str, Any]) -> str:
    return ", ".join(f"{o} {c}" for o, c in sorted(owners.items(), key=lambda kv: -kv[1])[:3])


def _placeholder_share(conn, dealer_id: str, since_iso: str) -> tuple[int, int] | None:
    """(rows with no real photo, rows) among this run's active rows, or None when unreadable."""
    rows = _safe_rows(
        conn,
        "SELECT COUNT(*) FILTER (WHERE image_url LIKE '%placeholder%') AS ph, COUNT(*) AS n "
        "FROM cars WHERE dealer_id = ? AND COALESCE(listing_active,1)=1 AND scraped_at >= ?",
        (dealer_id, since_iso),
    )
    return (int(rows[0]["ph"] or 0), int(rows[0]["n"] or 0)) if rows else None


def _previous_rows(conn, dealer_id: str, run_id: Any) -> int:
    """Rows the dealer's previous scan_run wrote (0 when there is none)."""
    prev = _safe_rows(conn, "SELECT upserted, summary_json FROM scan_runs WHERE dealer_id = ? AND id < ? ORDER BY id DESC LIMIT 1",
                      (dealer_id, run_id))
    if not prev:
        return 0
    try:
        ps = json.loads(prev[0].get("summary_json") or "{}")
    except ValueError:
        ps = {}
    if ps.get("rows_written") is not None:
        return int(ps["rows_written"])
    return int(prev[0].get("upserted") or 0)


def _thin(conn, out: dict[str, Any], dealer_id: str, since_iso: str, reason: str) -> dict[str, Any]:
    """A thin verdict still gets the row-by-row NHTSA verification (N9.12): the log and the
    triage show what is incomplete or discrepant even when the run cannot be called ok."""
    out["verdict"], out["reason"] = "thin", reason
    out["accuracy"] = verify_accuracy(conn, dealer_id, since_iso)
    return out


def _assess(conn, dealer_id: str, since_iso: str, known_before: int, recipe_info: dict[str, Any]) -> dict[str, Any]:
    runs = _rows(
        conn,
        "SELECT id, provider, finished_at, duration_seconds, inventory_rows, upserted, error, summary_json "
        "FROM scan_runs WHERE dealer_id = ? AND finished_at >= ? ORDER BY id DESC LIMIT 1",
        (dealer_id, since_iso),
    )
    active = _rows(conn, "SELECT COUNT(*) AS n FROM cars WHERE dealer_id = ? AND COALESCE(listing_active,1)=1", (dealer_id,))[0]["n"]
    inactive = _rows(conn, "SELECT COUNT(*) AS n FROM cars WHERE dealer_id = ? AND COALESCE(listing_active,1)=0", (dealer_id,))[0]["n"]
    out: dict[str, Any] = {
        "dealer_id": dealer_id, "recipe": recipe_info, "known_before": known_before, "active_after": int(active),
        "inactive_after": int(inactive),
        # no scan_runs row: counted as zeros, not null (an error dealer still has active rows)
        "rows": 0, "rows_captured": 0, "rows_refused_vin_owner": 0, "rows_refused_rooftop": 0, "rows_written": 0,
    }
    mix = _rows(
        conn,
        "SELECT COUNT(*) FILTER (WHERE lower(COALESCE(condition,'')) LIKE 'new%') AS n_new, "
        "COUNT(*) FILTER (WHERE lower(COALESCE(condition,'')) NOT LIKE 'new%') AS n_used, "
        "COUNT(DISTINCT dealer_name) AS names, COUNT(DISTINCT zip_code) AS zips "
        "FROM cars WHERE dealer_id = ? AND COALESCE(listing_active,1)=1 AND scraped_at >= ?",
        (dealer_id, since_iso),
    )[0]
    out.update({"rows_new": int(mix["n_new"] or 0), "rows_used": int(mix["n_used"] or 0),
                "dealer_names": int(mix["names"] or 0), "zip_codes": int(mix["zips"] or 0)})
    if not runs:
        out["verdict"] = "no_recipe" if recipe_info.get("synth") and not str(recipe_info.get("synth")).startswith("saved") and not recipe_info.get("had_recipes") else "error"
        out["reason"] = "no scan_runs row" if out["verdict"] == "error" else recipe_info.get("synth")
        return out
    r = runs[0]
    try:
        s = json.loads(r.get("summary_json") or "{}")
    except ValueError:
        s = {}
    cc = s.get("capture_coverage") or {}
    n = int(cc.get("n") or r.get("upserted") or 0)
    vin_refused = int(s.get("vin_owner_conflicts") or 0)
    roof_refused = int(s.get("filtered_count") or 0)
    written = int(s["rows_written"]) if s.get("rows_written") is not None else max(0, n - vin_refused - roof_refused)
    out.update({
        "run_id": r["id"], "provider": r.get("provider"), "minutes": round(float(r.get("duration_seconds") or 0) / 60, 1),
        "rows": n, "recipe_fetch": s.get("recipe_fetch"), "recipe_coverage": s.get("recipe_coverage"),
        "http_first": (s.get("vdp_prefetch") or {}).get("http_first"), "vdp_recipes": (s.get("vdp_prefetch") or {}).get("vdp_recipes"),
        "error": (r.get("error") or None),
        "coverage": {k: cc.get(k) for k in KEY_FIELDS + SECONDARY_FIELDS + MILEAGE_COVERAGE_KEYS if k in cc},
        "filtered_count": s.get("filtered_count"), "vin_facts": s.get("vin_facts"),
        "vin_owner_conflicts": vin_refused,
        "vin_owner_conflict_owners": s.get("vin_owner_conflict_owners") or {},
        "run_finished_at": str(r.get("finished_at") or ""),
        "rows_captured": n, "rows_refused_vin_owner": vin_refused, "rows_refused_rooftop": roof_refused, "rows_written": written,
    })
    hp = handover_pending(conn, dealer_id, since_iso)
    if hp:
        out["handover_pending"] = hp
    rate = _new_arrival_rate(conn, dealer_id, since_iso)
    if rate:
        out["new_arrival_incomplete"] = rate
        _store_new_arrival(conn, r["id"], s, rate)
    replay = s.get("recipe_replay") or {}
    out["replay_truncated"] = bool(replay.get("truncated"))
    if s.get("replay_persist_gap"):
        out["replay_persist_gap"] = s["replay_persist_gap"]
    out["timing"] = record_timing(dealer_id, r)
    if r.get("error"):
        out["verdict"], out["reason"] = "error", str(r["error"])[:200]
        return out
    if n == 0:
        out["verdict"], out["reason"] = "no_rows", "recipe replay yielded 0 rows"
        return out
    floor = known_before * ROW_FLOOR if known_before else MIN_ROWS_UNKNOWN
    # ``n`` counts the feed BEFORE the rooftop gate. A group feed answers with
    # the whole group (MB Beverly Hills 2026-09-28: 1,521 Fletcher Jones rows,
    # 2 kept), so the rows actually stamped to this store are the run's yield.
    stamped = int(out.get("rows_new") or 0) + int(out.get("rows_used") or 0)
    out["rows_stamped"] = stamped
    if stamped < floor and stamped < n * 0.5:
        out["verdict"] = "no_rows"
        # name the cause: the VIN-owner guard (another store owns the VINs) or the rooftop gate
        if vin_refused and stamped + vin_refused + roof_refused >= n * 0.9:
            out["guard_only"] = True
            out["reason"] = (f"{stamped} rows stamped to this store of {n} in the feed: the VIN-owner guard refused {vin_refused}"
                             + (f" ({_owner_text(out['vin_owner_conflict_owners'])})" if out["vin_owner_conflict_owners"] else "")
                             + f" (floor {floor:.0f})")
        else:
            out["reason"] = (f"{stamped} rows stamped to this store of {n} in the feed (VIN-owner guard refused {vin_refused}, "
                             f"rooftop gate refused {roof_refused}, the rest unaccounted; floor {floor:.0f})")
        return out
    if n < floor:
        out["verdict"], out["reason"] = "no_rows", f"{n} rows vs {known_before} listed before (floor {floor:.0f})"
        return out
    # P5A.7 (D-SR4 a): a replay that stopped before the site's own total (downeyhyundai: a
    # mid-walk 403 left about 130 new cars out) is thin, whatever the row floor says: the
    # run cannot vouch for the lot it did not finish reading.
    if replay.get("truncated"):
        return _thin(conn, out, dealer_id, since_iso, "replay_truncated: " + _truncation_text(replay))
    # N9.12: "ok" also needs the lot to hold against the previous scan, a site-total check
    # that ran, and real photos.
    prior = _previous_rows(conn, dealer_id, r["id"])
    if prior and stamped < prior * (1 - COVERAGE_DROP_FLOOR):
        out["previous_rows"] = prior
        return _thin(conn, out, dealer_id, since_iso,
                     f"coverage_drop: {stamped} rows vs {prior} in the previous scan ({1 - stamped / prior:.0%} fewer; limit {COVERAGE_DROP_FLOOR:.0%})")
    if not (replay.get("recipes") or replay.get("walks")):
        return _thin(conn, out, dealer_id, since_iso, "no_site_total_check: the run has no replay walk report to compare with the site's own total")
    ph = _placeholder_share(conn, dealer_id, since_iso)
    if ph and ph[1] and ph[0] == ph[1]:
        return _thin(conn, out, dealer_id, since_iso, f"placeholder_photos: all {ph[1]} rows carry a placeholder image")
    weak = [k for k in KEY_FIELDS if float(cc.get(k) or 0) < FIELD_FLOOR]
    weak2 = [k for k in SECONDARY_FIELDS if k in cc and float(cc.get(k) or 0) < FIELD_FLOOR]
    out["weak_key_fields"], out["weak_fields"] = weak, weak2
    # N6.9: mileage joins the gate for used rows (F4 MY2026+ at 0 mi is a data question, not a gap).
    if cc.get("used_mileage") is not None and float(cc["used_mileage"]) < FIELD_FLOOR:
        weak.append("used_mileage")
    if weak:
        return _thin(conn, out, dealer_id, since_iso, "key fields below 90%: " + ", ".join(f"{k}={float(cc.get(k) or 0):.0%}" for k in weak))
    # Verification (the process doc): incomplete fields after the NHTSA fill and
    # dictionary-vs-dealer discrepancies, row by row, with examples.
    acc = verify_accuracy(conn, dealer_id, since_iso)
    out["accuracy"] = acc
    problems = []
    if acc.get("rows"):
        if acc["incomplete_rows"] / acc["rows"] > INCOMPLETE_FLOOR:
            problems.append(f"incomplete {acc['incomplete_rows']}/{acc['rows']}: {', '.join(list(acc['missing'])[:4])}")
        if acc["hard_rows"] / acc["rows"] > DISCREPANCY_FLOOR:
            problems.append(f"discrepancies {acc['hard_rows']}/{acc['rows']}: {', '.join(list(acc['hard'])[:4])}")
    if out["rows_new"] == 0 or out["rows_used"] == 0:
        if dealer_id in _one_condition_ok():
            # independent used-only (or new-only) lots: one condition IS the lot
            # (quantumautosales-com 209/0 used, 2026-09-26); listed in
            # backend/config/one_condition_ok.txt after a human check of the site
            weak2 = list(weak2) + [f"one condition by design (new {out['rows_new']}, used {out['rows_used']})"]
        else:
            problems.append(f"only one condition captured (new {out['rows_new']}, used {out['rows_used']})")
    if problems:
        out["verdict"], out["reason"] = "inaccurate", "; ".join(problems)
    else:
        out["verdict"], out["reason"] = "ok", ("secondary gaps: " + ", ".join(weak2)) if weak2 else "complete and verified"
    return out


def verify_accuracy(conn, dealer_id: str, since_iso: str) -> dict[str, Any]:
    """Row-by-row verification from scan_lab_report: missing spec-sheet fields and
    dictionary-vs-dealer discrepancies (VIN decode, catalog link, engine text)."""
    try:
        from backend.scripts.scan_lab_report import INFORMATIONAL, tally_dealer

        t = tally_dealer(conn, dealer_id, since_iso)
    except Exception as exc:  # noqa: BLE001 - verification must not crash the triage
        return {"error": str(exc)[:200]}
    hard = {c: n for c, n in (t.get("discrepancies") or {}).items() if c not in INFORMATIONAL}
    hard_rows = 0
    for c in t.get("car_issues") or []:
        if any(k not in INFORMATIONAL for k in (c.get("disc") or {})):
            hard_rows += 1
    return {
        "rows": t.get("rows", 0), "vpic_cached": t.get("vpic_cached", 0),
        # fixable gaps only (section 2 exemptions applied in scan_lab_report.fixable_missing);
        # the unexempted counts stay under *_raw so nothing is hidden
        "incomplete_rows": t.get("incomplete_rows", 0), "missing": t.get("missing") or {},
        "incomplete_rows_raw": t.get("incomplete_rows_raw", t.get("incomplete_rows", 0)),
        "missing_raw": t.get("missing_raw") or (t.get("missing") or {}),
        "missing_exempt": t.get("missing_exempt") or {},
        "msrp_expected_rows": t.get("msrp_expected_rows", 0), "msrp_missing_rows": t.get("msrp_missing_rows", 0),
        "hard": hard, "hard_rows": hard_rows,
        "examples": {c: (t.get("discrepancy_examples") or {}).get(c, [])[:3] for c in list(hard)[:5]},
    }
