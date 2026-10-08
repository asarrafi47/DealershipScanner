"""Summarize results/*.json + vdp_results.json + offline.json into tables (analysis.json + stdout)."""
from __future__ import annotations

import collections
import glob
import json
import os

RUN = "/private/tmp/claude-501/phase0/p0b1_run"
HTML = {"dep_srp_page", "html_page_query", "jazel_srp_page"}


def stratum(rec: dict) -> str:
    pg = rec["pagination"]
    if pg == "dealer_com_start":
        return "dealer_com"
    if pg == "carscommerce_page":
        return "carscommerce"
    if pg == "cosmos_pt":
        return "cosmos_pt"
    if pg == "page_query":
        return "page_query (Team Velocity)"
    if pg == "dep_srp_page":
        return "dep_srp_page"
    if pg in ("html_page_query", "jazel_srp_page") or rec.get("provider_hint") == "html_cards":
        return "html_page_query / jazel"
    return f"other ({pg})"


def load():
    out = {}
    for p in sorted(glob.glob(os.path.join(RUN, "results", "*.json"))):
        if os.path.basename(p).startswith("_"):
            continue
        out[os.path.basename(p)[:-5]] = json.load(open(p))
    return out


def per_recipe(r: dict) -> list[dict]:
    """Join stored recipe, validate_recipe count, and the set2 / set40 checks per recipe."""
    # Align by position: report.recipes[i] and the i-th main _check_recipe call are live[i]
    # (several recipes can share one URL: carscommerce / dealer.com bodies).
    rows = []
    counts = {c["i"]: c for c in (r["steps"].get("count") or {}).get("counts") or []}
    checks = {}
    for k in ("set2", "set40"):
        rep = (r["steps"].get(k) or {}).get("report") or {}
        checks[k] = list(rep.get("recipes") or [])
    main40 = [c for c in r.get("checks") or [] if c["phase"] == "set40" and c["pages"] and c["pages"] > 2]
    for rec in r.get("recipes") or []:
        u = rec["url"]
        i = rec["i"]
        c2 = checks["set2"][i] if i < len(checks["set2"]) else {}
        c40 = checks["set40"][i] if i < len(checks["set40"]) else {}
        cnt = counts.get(i) or {}
        vinsets = {u: set(main40[i]["vins"])} if i < len(main40) and main40[i]["url"] == u else {}
        rows.append({**rec, "dealer": r["dealer"], "stratum": stratum(rec), "count": cnt.get("n"), "count_err": cnt.get("error"),
                     "count_wire": cnt.get("wire"),
                     "set2_vins": c2.get("vins"), "set40_vins": c40.get("vins"), "set40_pages": len(c40.get("pages") or []),
                     "set40_statuses": [p.get("status") for p in c40.get("pages") or []],
                     "set2_statuses": [p.get("status") for p in c2.get("pages") or []],
                     "site_total": c40.get("site_total"), "set40_exhausted": c40.get("exhausted"),
                     "set40_flags": {k: c40.get(k) for k in ("auth_needed", "auth_mid_walk", "short_page", "section_scoped") if c40.get(k)},
                     "set40_error": c40.get("error"), "gate_rejected": c40.get("gate_rejected"),
                     "vinset": vinsets.get(u, set())})
    return rows


def main():
    res = load()
    recs = []
    fatal = []
    for did, r in res.items():
        if r.get("fatal") or r.get("fatal_write_attempt"):
            fatal.append((did, r.get("fatal") or r.get("fatal_write_attempt")))
            continue
        recs += per_recipe(r)
    # 1. per-shape agreement
    shape = collections.defaultdict(lambda: collections.Counter())
    for x in recs:
        s = shape[x["stratum"]]
        s["recipes"] += 1
        a, b = x["count"], x["set40_vins"]
        if a is None or b is None:
            s["missing"] += 1
            continue
        if a == b:
            s["equal"] += 1
        elif b and abs(a - b) <= max(1, 0.02 * b):
            s["within_2pct"] += 1
        elif a > b:
            s["count_higher"] += 1
        else:
            s["count_lower"] += 1
        if (a >= 5) != (b >= 5):
            s["floor5_disagree"] += 1
        if (a == 0) != (b == 0):
            s["zero_disagree"] += 1
    # 2. verdicts
    verdicts = []
    for did, r in res.items():
        if r.get("fatal"):
            continue
        r2 = (r["steps"].get("set2") or {}).get("report") or {}
        r40 = (r["steps"].get("set40") or {}).get("report") or {}
        verdicts.append({"dealer": did, "set2": r["steps"].get("set2", {}).get("status"), "set40": r["steps"].get("set40", {}).get("status"),
                         "set2_reasons": r2.get("reasons"), "set40_reasons": r40.get("reasons"),
                         "set2_vins": r2.get("vins_total"), "set40_vins": r40.get("vins_total"), "site_total": r40.get("site_total"),
                         "cov2": r2.get("coverage"), "cov40": r40.get("coverage"), "flip": r["steps"].get("set2", {}).get("status") != r["steps"].get("set40", {}).get("status")})
    # 3. wire
    wire = collections.Counter()
    wire_phase = collections.defaultdict(collections.Counter)
    for did, r in res.items():
        for w in r.get("wire") or []:
            wire[str(w.get("status"))] += 1
            wire_phase[(w.get("phase"), w.get("lib"), w.get("profile"))][str(w.get("status")) + ("/challenge" if w.get("challenge") else "")] += 1
    # transient: a replay URL that answered non-200 in one phase and 200 in another
    transient = []
    for did, r in res.items():
        st = collections.defaultdict(list)
        for lg in r.get("logical") or []:
            st[lg["url"]].append((lg["phase"], lg["status"], lg.get("cached")))
        for u, seq in st.items():
            codes = {s for _, s, _ in seq}
            if any(c not in (200,) for c in codes):
                transient.append({"dealer": did, "url": u[:120], "seq": seq})
    # 4. D-OD1: scan union per option, from the measured pages=40 VIN sets (set2 verdict = today's gate)
    import math
    by_dealer = collections.defaultdict(list)
    for x in recs:
        by_dealer[x["dealer"]].append(x)
    od1 = []
    firsthit = []
    for did, xs in by_dealer.items():
        st2 = (res[did]["steps"].get("set2") or {}).get("status") or ""
        gate_ok = st2 == "ok" or st2.startswith("uncertain")

        def union(sel):
            u = set()
            for x in sel:
                u |= x["vinset"]
            return u
        today = [x for x in xs if len(x["vinset"]) >= 10]
        any0 = [x for x in xs if len(x["vinset"]) > 0]
        rec = today + [x for x in xs if 0 < len(x["vinset"]) < 10 and x["pins"] and x["pagination"] != "none" and gate_ok]
        ut, ua, ur = union(today), union(any0), union(rec)
        row = {"dealer": did, "set2_status": st2,
               "today": {"recipes": len(today), "vins": len(ut), "scan_ok": len(ut) >= 10},
               "admit_gt0": {"recipes": len(any0), "vins": len(ua), "added_vins": len(ua - ut), "scan_ok": len(ua) >= 5},
               "recommended": {"recipes": len(rec), "vins": len(ur), "added_vins": len(ur - ut), "scan_ok": len(ur) >= 5},
               "sub_floor": [{"url": x["url"][:100], "vins": len(x["vinset"]), "pins": x["pins"], "pagination": x["pagination"],
                              "new_to_union": len(x["vinset"] - ut)} for x in xs if 0 < len(x["vinset"]) < 10],
               "zero": [x["url"][:100] for x in xs if len(x["vinset"]) == 0]}
        if len(today) != len(any0) or len(today) != len(rec) or row["sub_floor"]:
            od1.append(row)
        for x in xs:
            n = len(x["vinset"])
            thr = min(10, max(3, math.ceil(0.5 * int(x["vehicle_rows"] or 0))))
            if (n >= 10) != (n >= thr):
                firsthit.append({"dealer": did, "url": x["url"][:100], "vins": n, "stored_rows": x["vehicle_rows"], "new_threshold": thr,
                                 "today_accepts": n >= 10, "new_accepts": n >= thr})
    out = {"n_dealers": len(res), "fatal": fatal, "od1": od1, "firsthit": firsthit, "shape": {k: dict(v) for k, v in shape.items()}, "verdicts": verdicts,
           "wire": dict(wire), "wire_phase": {str(k): dict(v) for k, v in wire_phase.items()}, "replay_non200": transient,
           "recipes": [{k: v for k, v in x.items() if k != "vinset"} for x in recs]}
    json.dump(out, open(os.path.join(RUN, "analysis.json"), "w"), indent=1, default=str)
    print("dealers", len(res), "recipes", len(recs), "fatal", fatal)
    for k, v in sorted(out["shape"].items()):
        print(k.ljust(28), dict(v))
    print("flips", sum(1 for v in verdicts if v["flip"]), "of", len(verdicts))
    for v in verdicts:
        if v["flip"]:
            print("  FLIP", v["dealer"], v["set2"], "->", v["set40"], v["set2_vins"], v["set40_vins"], v["site_total"])
    print("wire", dict(wire))
    print("replay URLs with a non-200:", len(transient))


if __name__ == "__main__":
    main()
