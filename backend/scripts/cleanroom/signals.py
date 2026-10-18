"""Per-dealer fleet signals (``--signals``) and the N11.10 flag list.

One row per rostered dealer. A dealer is flagged for N11.10 (D-N9 option C, step
2) when it carries at least one signal below, is not in a VIN-owner conflict
group with >= 20 rows (those go to the N1.2 group dossiers), has no analysis
record (when an analysis dir is given), and is not a pause-killed error
(``error`` with "no scan_runs row": rescanned, not analysed). At most
``cap`` dealers are flagged (plan: <= 46); the rest of the eligible dealers are
listed as ``overflow`` in priority order, never dropped silently.
"""

from __future__ import annotations

from typing import Any

UNSAMPLED_PLATFORMS = ("motive_ridemotive", "chapman", "dealer_on", "autowall", "oneaudi")
SIGNAL_ORDER = ("one_condition", "drop_over_30pct", "claimant", "page_cap", "gain_over_30pct",
                "unsampled_platform", "no_rows", "http_403_429")
DEFINITIONS = {
    "one_condition": "triage reason says 'only one condition captured' (assess.py:159; one_condition_ok.txt honoured)",
    "drop_over_30pct": "db active < 0.7 x reference active (dealer had reference rows; error verdicts excluded)",
    "gain_over_30pct": "db active > 1.3 x reference active",
    "claimant": "triage vin_owner_conflicts > 0 (VINs refused to this dealer by the VIN-owner guard)",
    "page_cap": "a recipe walk stopped at exactly _MAX_REPLAY_PAGES pages",
    "unsampled_platform": "triage provider in " + ", ".join(UNSAMPLED_PLATFORMS),
    "no_rows": "triage verdict no_rows",
    "http_403_429": "feed-level HTTP 403/429 lines (fleet_scan.tally_http rule) attributed to the dealer",
}
PAUSE_ERROR_REASON = "no scan_runs row"


def build_signals(
    *,
    triage: list[dict[str, Any]],
    db_active: dict[str, int],
    db_inactive: dict[str, int],
    ref_active: dict[str, int] | None,
    pairs: list[tuple[str, str, int]],
    groups: list[dict[str, Any]],
    page_cap_dealers: dict[str, int],
    http_by_dealer: dict[str, dict[str, int]],
    column_per_dealer: dict[str, dict[str, int]],
    awd_by_dealer: dict[str, int],
    incomplete_by_dealer: dict[str, dict[str, int]] | None,
    analyzed: set[str] | None,
    cap: int = 46,
) -> dict[str, Any]:
    group_of: dict[str, int] = {}
    for i, g in enumerate(groups, 1):
        for d in g["dealers"]:
            group_of[d] = i
    as_owner: dict[str, int] = {}
    as_claimant: dict[str, int] = {}
    for o, c, n in pairs:
        as_owner[o] = as_owner.get(o, 0) + n
        as_claimant[c] = as_claimant.get(c, 0) + n

    rows: list[dict[str, Any]] = []
    for d in sorted(triage, key=lambda x: x["dealer_id"]):
        did = d["dealer_id"]
        h = d.get("http_first") or {}
        acc = d.get("accuracy") or {}
        col = column_per_dealer.get(did, {})
        inc = (incomplete_by_dealer or {}).get(did, {})
        ref = (ref_active or {}).get(did) if ref_active is not None else None
        act = db_active.get(did, 0)
        reason = str(d.get("reason") or "")
        verdict = str(d.get("verdict"))
        sig: list[str] = []
        if "only one condition captured" in reason:
            sig.append("one_condition")
        if ref and act < 0.7 * ref and verdict != "error":
            sig.append("drop_over_30pct")
        if int(d.get("vin_owner_conflicts") or 0) > 0:
            sig.append("claimant")
        if page_cap_dealers.get(did):
            sig.append("page_cap")
        if ref and act > 1.3 * ref:
            sig.append("gain_over_30pct")
        if d.get("provider") in UNSAMPLED_PLATFORMS:
            sig.append("unsampled_platform")
        if verdict == "no_rows":
            sig.append("no_rows")
        hb = http_by_dealer.get(did, {})
        if hb:
            sig.append("http_403_429")
        pause_error = verdict == "error" and PAUSE_ERROR_REASON in reason
        cand = int(h.get("candidates") or 0)
        fetched = int(h.get("fetched") or 0)
        rows.append({
            "dealer_id": did,
            "shard": d.get("_shard"),
            "verdict": verdict,
            "reason": reason[:300],
            "provider": d.get("provider"),
            "captured": d.get("rows"),
            "written": d.get("rows_stamped"),
            "refused": d.get("vin_owner_conflicts"),
            "active_db": act,
            "inactive_db": db_inactive.get(did, 0),
            "ref_active": ref,
            "change_pct": round(100 * (act - ref) / ref, 1) if ref else None,
            "rows_new": d.get("rows_new"),
            "rows_used": d.get("rows_used"),
            "conflict_rows_as_owner": as_owner.get(did, 0),
            "conflict_rows_as_claimant": as_claimant.get(did, 0),
            "conflict_group": group_of.get(did),
            "page_cap_walks": page_cap_dealers.get(did, 0),
            "http_403": hb.get("403", 0),
            "http_429": hb.get("429", 0),
            "vdp_wanted": cand,
            "vdp_fetched": fetched,
            "vdp_blocked": bool(cand and fetched < 0.1 * cand),
            "vpic_fuel_rows": int((acc.get("hard") or {}).get("vpic_fuel") or 0),
            "hard_rows": int(acc.get("hard_rows") or 0),
            "dict_trim": col.get("dict_trim", 0),
            "token_color": col.get("token_color", 0),
            "junk_price": col.get("junk_price", 0),
            "vin_not_17": col.get("vin_not_17", 0),
            "no_photo": col.get("no_photo", 0),
            "http_placeholder": col.get("http_placeholder", 0),
            "used_zero_mi": col.get("used_zero_mi", 0),
            "awd_vs_vpic_2wd": awd_by_dealer.get(did, 0),
            "incomplete": inc.get("incomplete"),
            "incomplete_excl_new_null_mileage": inc.get("incomplete_excl_new_null_mileage"),
            "analyzed": (did in analyzed) if analyzed is not None else None,
            "pause_error": pause_error,
            "signals": sig,
        })

    def eligible(r: dict[str, Any]) -> bool:
        return (bool(r["signals"]) and r["conflict_group"] is None and not r["pause_error"]
                and not r["analyzed"])

    def priority(r: dict[str, Any]) -> tuple:
        first = min(SIGNAL_ORDER.index(s) for s in r["signals"])
        return (-len(r["signals"]), first, r["dealer_id"])

    elig = sorted((r for r in rows if eligible(r)), key=priority)
    flagged = {r["dealer_id"] for r in elig[:cap]}
    for rank, r in enumerate(elig, 1):
        r["n11_10_rank"] = rank
    for r in rows:
        r["flag_n11_10"] = r["dealer_id"] in flagged
    by_signal = {s: sum(1 for r in rows if s in r["signals"]) for s in SIGNAL_ORDER}
    by_signal_elig = {s: sum(1 for r in elig if s in r["signals"]) for s in SIGNAL_ORDER}
    return {
        "definitions": DEFINITIONS,
        "flag_rule": ("signal present, not in a >=20-row conflict group, no analysis record, not a pause-killed "
                      f"error; at most {cap} flagged in priority order (more signals first, then SIGNAL_ORDER)"),
        "summary": {
            "dealers": len(rows),
            "with_signal": sum(1 for r in rows if r["signals"]),
            "in_conflict_groups": sum(1 for r in rows if r["conflict_group"] is not None),
            "analyzed": None if analyzed is None else sum(1 for r in rows if r["analyzed"]),
            "pause_errors": sum(1 for r in rows if r["pause_error"]),
            "eligible": len(elig),
            "flagged": len(flagged),
            "cap": cap,
            "overflow": [r["dealer_id"] for r in elig[cap:]],
            "by_signal": by_signal,
            "by_signal_eligible": by_signal_elig,
        },
        "dealers": rows,
    }
