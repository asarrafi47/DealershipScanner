"""Step 4: verdict per dealer from scan_runs + row counts, row-by-row verification, timing. Moved verbatim from backend/scripts/dealer_pipeline.py (audit F11)."""
from __future__ import annotations

import json
from typing import Any

from backend.scanner.pipeline.constants import (
    DISCREPANCY_FLOOR,
    FIELD_FLOOR,
    INCOMPLETE_FLOOR,
    KEY_FIELDS,
    MIN_ROWS_UNKNOWN,
    ROOT,
    ROW_FLOOR,
    SECONDARY_FIELDS,
)
from backend.scanner.pipeline.db import _rows


def _one_condition_ok() -> set[str]:
    """Dealer ids whose lot legitimately carries a single condition (used-only
    independents, new-only fleet/RV sellers). One id per line, `#` comments."""
    try:
        text = (ROOT / "workspace" / "pipeline" / "one_condition_ok.txt").read_text()
    except OSError:
        return set()
    return {ln.split("#", 1)[0].strip() for ln in text.splitlines() if ln.split("#", 1)[0].strip()}


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


def assess(conn, dealer_id: str, since_iso: str, known_before: int, recipe_info: dict[str, Any]) -> dict[str, Any]:
    out = _assess(conn, dealer_id, since_iso, known_before, recipe_info)
    n = int(out.get("vin_owner_conflicts") or 0)
    if n > VIN_OWNER_CONFLICT_REASON_MIN:
        owners = out.get("vin_owner_conflict_owners") or {}
        top = ", ".join(f"{o} {c}" for o, c in sorted(owners.items(), key=lambda kv: -kv[1])[:3])
        note = f"VIN owner guard: {n} VINs owned by other dealers not written" + (f" ({top})" if top else "")
        out["reason"] = f"{out['reason']}; {note}" if out.get("reason") else note
    return out


def _assess(conn, dealer_id: str, since_iso: str, known_before: int, recipe_info: dict[str, Any]) -> dict[str, Any]:
    runs = _rows(
        conn,
        "SELECT id, provider, finished_at, duration_seconds, inventory_rows, upserted, error, summary_json "
        "FROM scan_runs WHERE dealer_id = ? AND finished_at >= ? ORDER BY id DESC LIMIT 1",
        (dealer_id, since_iso),
    )
    active = _rows(conn, "SELECT COUNT(*) AS n FROM cars WHERE dealer_id = ? AND COALESCE(listing_active,1)=1", (dealer_id,))[0]["n"]
    out: dict[str, Any] = {
        "dealer_id": dealer_id, "recipe": recipe_info, "known_before": known_before, "active_after": int(active),
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
    out.update({
        "run_id": r["id"], "provider": r.get("provider"), "minutes": round(float(r.get("duration_seconds") or 0) / 60, 1),
        "rows": n, "recipe_fetch": s.get("recipe_fetch"), "recipe_coverage": s.get("recipe_coverage"),
        "http_first": (s.get("vdp_prefetch") or {}).get("http_first"), "vdp_recipes": (s.get("vdp_prefetch") or {}).get("vdp_recipes"),
        "error": (r.get("error") or None),
        "coverage": {k: cc.get(k) for k in KEY_FIELDS + SECONDARY_FIELDS if k in cc},
        "filtered_count": s.get("filtered_count"), "vin_facts": s.get("vin_facts"),
        "vin_owner_conflicts": int(s.get("vin_owner_conflicts") or 0),
        "vin_owner_conflict_owners": s.get("vin_owner_conflict_owners") or {},
    })
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
        out["reason"] = f"{stamped} rows stamped to this store of {n} in the feed (rooftop gate refused the rest; floor {floor:.0f})"
        return out
    if n < floor:
        out["verdict"], out["reason"] = "no_rows", f"{n} rows vs {known_before} listed before (floor {floor:.0f})"
        return out
    weak = [k for k in KEY_FIELDS if float(cc.get(k) or 0) < FIELD_FLOOR]
    weak2 = [k for k in SECONDARY_FIELDS if k in cc and float(cc.get(k) or 0) < FIELD_FLOOR]
    out["weak_key_fields"], out["weak_fields"] = weak, weak2
    if weak:
        out["verdict"], out["reason"] = "thin", "key fields below 90%: " + ", ".join(f"{k}={float(cc.get(k) or 0):.0%}" for k in weak)
        return out
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
            # workspace/pipeline/one_condition_ok.txt after a human check of the site
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
