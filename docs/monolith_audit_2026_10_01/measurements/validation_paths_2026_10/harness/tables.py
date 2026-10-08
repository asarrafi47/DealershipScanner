"""Markdown tables for the P0B.1 doc from analysis.json, offline.json, results/, vdp_results.json."""
from __future__ import annotations

import collections
import json
import os


from urllib.parse import parse_qsl, urlencode, urlparse

_KEEP = {"tp", "ct", "mk", "p", "page", "pt", "pn", "condition", "cpo", "inventorytype", "filter", "type", "host"}


def red(url: str) -> str:
    """Path plus whitelisted query keys only: recipe URLs can embed public search API keys."""
    u = urlparse(url or "")
    q = parse_qsl(u.query, keep_blank_values=True)
    kept = [(k, v) for k, v in q if k.lower() in _KEEP]
    tail = ("?" + urlencode(kept)) if kept else ""
    if len(kept) < len(q):
        tail += ("&" if kept else "?") + "…"
    return f"{u.netloc}{u.path}{tail}"

RUN = "/private/tmp/claude-501/phase0/p0b1_run"
A = json.load(open(os.path.join(RUN, "analysis.json")))
O = json.load(open(os.path.join(RUN, "offline.json")))
V = json.load(open(os.path.join(RUN, "vdp_results.json"))) if os.path.exists(os.path.join(RUN, "vdp_results.json")) else {}
RES = {}
for f in os.listdir(os.path.join(RUN, "results")):
    if f.endswith(".json") and not f.startswith("_"):
        RES[f[:-5]] = json.load(open(os.path.join(RUN, "results", f)))
out = []
P = out.append


def md(rows, head):
    P("| " + " | ".join(head) + " |")
    P("|" + "---|" * len(head))
    for r in rows:
        P("| " + " | ".join(str(x) for x in r) + " |")
    P("")


recs = A["recipes"]
measured = {(r["dealer"], r["i"]): r for r in recs}

# T1 per-shape agreement
P("### T1 per-shape agreement")
by = collections.defaultdict(list)
for r in recs:
    by[r["stratum"]].append(r)
rows = []
for s, xs in sorted(by.items()):
    n = len(xs)
    ok = [x for x in xs if x["count"] is not None and x["set40_vins"] is not None]
    eq = sum(1 for x in ok if x["count"] == x["set40_vins"])
    hi = sum(1 for x in ok if x["count"] > x["set40_vins"])
    lo = sum(1 for x in ok if x["count"] < x["set40_vins"])
    f5 = sum(1 for x in ok if (x["count"] >= 5) != (x["set40_vins"] >= 5))
    p2 = sum(1 for x in ok if x["set2_vins"] is not None and x["set2_vins"] == x["set40_vins"])
    dealers = len({x["dealer"] for x in xs})
    capped = sum(1 for x in xs if x["set40_pages"] >= 40 and not x["set40_exhausted"])
    rows.append([s, dealers, n, eq, hi, lo, f5, p2, capped])
md(rows, ["shape", "dealers", "recipes", "count = p40", "count > p40", "count < p40", "disagree on >=5", "p2 = p40 VINs", "p40 hit 40-page cap"])

# T1b disagreements detail
P("### T1b recipes where validate_recipe and the pages=40 set check disagree")
rows = []
for x in recs:
    if x["count"] is None or x["set40_vins"] is None or x["count"] != x["set40_vins"]:
        rows.append([x["dealer"], x["stratum"], f"`{red(x['url'])[:70]}`", x["vehicle_rows"], x["total_count"], x["count"], x["set2_vins"], x["set40_vins"],
                     x["site_total"], "/".join(sorted({str(s) for s in x["set40_statuses"]})) or "-", x["set40_error"] or x["count_err"] or ""])
md(rows, ["dealer", "shape", "recipe", "stored rows", "stored total", "validate_recipe", "p2 VINs", "p40 VINs", "site total", "p40 statuses", "error"])

# T2 verdicts
P("### T2 set-gate verdict, pages=2 vs pages=40")
rows = []
for v in sorted(A["verdicts"], key=lambda v: (not v["flip"], v["dealer"])):
    rows.append([v["dealer"], v["set2"], v["set40"], "FLIP" if v["flip"] else "", v["set2_vins"], v["set40_vins"], v["site_total"],
                 v["cov2"], v["cov40"], "; ".join(v["set40_reasons"] or [])[:160]])
md(rows, ["dealer", "p2 status", "p40 status", "flip", "p2 VINs", "p40 VINs", "site total", "p2 cov", "p40 cov", "p40 reasons"])

# T3 HTML walks
P("### T3 HTML walks: stored total vs VINs walked")
rows = []
for x in recs:
    if x["pagination"] in ("dep_srp_page", "html_page_query", "jazel_srp_page") or x.get("provider_hint") == "html_cards":
        past = (x["set40_vins"] or 0) - int(x["total_count"] or 0) if x["total_count"] else None
        rows.append([x["dealer"], x["pagination"], f"`{red(x['url'])[:60]}`", x["vehicle_rows"], x["total_count"], x["count"], x["set40_vins"],
                     x["site_total"], x["set40_pages"], "yes" if (x["set40_pages"] >= 40 and not x["set40_exhausted"]) else "",
                     past if past is not None else "-"])
md(rows, ["dealer", "pagination", "recipe", "stored rows", "stored total", "validate_recipe", "p40 VINs", "site total", "pages", "cap hit", "p40 - stored total"])

# T4 D-OD1 measured
P("### T4 D-OD1 scan-union impact on the sample (measured pages=40 VIN sets)")
rows = []
for r in sorted(A["od1"], key=lambda r: r["dealer"]):
    sf = "; ".join(f"{s['vins']} VINs {s['pagination']} pins={','.join(s['pins']) or '-'} new={s['new_to_union']} `{red(s['url'])[:50]}`" for s in r["sub_floor"])
    rows.append([r["dealer"], r["set2_status"], f"{r['today']['vins']} ({r['today']['recipes']})", f"{r['admit_gt0']['vins']} (+{r['admit_gt0']['added_vins']})",
                 f"{r['recommended']['vins']} (+{r['recommended']['added_vins']})", sf or "-", len(r["zero"])])
md(rows, ["dealer", "p2 verdict", "today union (recipes)", "admit >0 (+new VINs)", "recommended (+new VINs)", "sub-floor recipes", "zero-VIN recipes"])
P("First-hit threshold flips (min(10, max(3, ceil(0.5*vehicle_rows)))): " + json.dumps(A["firsthit"])[:1500])
P("")

# T5 wire
P("### T5 wire statuses by phase / client / profile")
rows = []
for k, v in sorted(A["wire_phase"].items()):
    rows.append([k, json.dumps(v)])
md(rows, ["(phase, client, profile)", "statuses"])
P("Overall: " + json.dumps(A["wire"]))
P("")
P("Replay URLs that answered non-200 at least once: " + str(len(A["replay_non200"])))
for t in A["replay_non200"][:40]:
    P(f"- {t['dealer']} `{red(t['url'])[:90]}` {t['seq']}")
P("")

# T6 offline sub-10
P("### T6 offline: live recipes under 10 VINs (stored vehicle_rows)")
rows = []
for r in sorted(O["sub10"], key=lambda r: (r["shape"], r["dealer"])):
    m = measured.get((r["dealer"], r["i"]))
    rows.append([r["dealer"], r["vehicle_rows"], r["pagination"], r["shape"], ",".join(r["pins"]) or "-", r["active"],
                 "P0B.2" if r["p0b2"] else "", (m["set40_vins"] if m else "-"), f"`{red(r['url'])[:60]}`"])
md(rows, ["dealer", "stored VINs", "pagination", "shape", "pins", "local active", "excluded", "live p40 VINs (if sampled)", "recipe"])

# T7 VDP
if V.get("vdp"):
    P("### T7 VDP probes")
    vd = collections.defaultdict(list)
    for v in V["vdp"]:
        vd[v["dealer"]].append(v)
    rows = []
    tot = collections.Counter()
    for d, vs in sorted(vd.items()):
        gone = [v for v in vs if (v["today"].get("status") in (404, 410)) or all((v.get(k) or {}).get("status") in (404, 410) for k in ("c124_ua", "saf_ua", "saf_own", "chrome_own"))]
        live = [v for v in vs if v not in gone]
        c = lambda k: sum(1 for v in live if (v.get(k) or {}).get("cleared"))  # noqa: E731
        chrome_only = sum(1 for v in live if v["today"].get("chrome_attempt_cleared"))
        rot = sum(1 for v in live if v["today"].get("chrome_attempt_cleared") or v["c124_ua"].get("cleared") or v["saf_ua"].get("cleared"))
        st = collections.Counter()
        for v in vs:
            for k in ("c124_ua", "saf_ua", "saf_own", "chrome_own"):
                st[str((v.get(k) or {}).get("status"))] += 1
            for a in v["today"].get("attempts") or []:
                st[str(a.get("status")) + ("+ch" if a.get("challenge") else "")] += 1
        for k, n in (("live", len(live)), ("today", c("today")), ("chrome_only", chrome_only), ("rotation", rot), ("c124_ua", c("c124_ua")),
                     ("saf_ua", c("saf_ua")), ("saf_own", c("saf_own")), ("chrome_own", c("chrome_own"))):
            tot[k] += n
        rows.append([d, len(vs), len(live), c("today"), chrome_only, rot, c("c124_ua"), c("saf_ua"), c("saf_own"), c("chrome_own"), json.dumps(dict(st))])
    rows.append(["**total**", sum(len(v) for v in vd.values()), tot["live"], tot["today"], tot["chrome_only"], tot["rotation"], tot["c124_ua"], tot["saf_ua"],
                 tot["saf_own"], tot["chrome_own"], ""])
    md(rows, ["dealer", "VDPs", "live (not 404/410)", "today's prefetch", "chrome only (curl_cffi)", "rotation c->c124->saf", "chrome124+UA", "safari+Chrome UA",
              "safari own UA", "chrome own UA", "statuses"])
if V.get("feed_ua"):
    P("### T8 feed replay page 1: profile x UA")
    rows = []
    for f in V["feed_ua"]:
        rows.append([f["dealer"], f["pagination"], f"`{red(f['url'])[:60]}`"] + [
            next((str(p.get("status")) + ("+ch" if p.get("challenge") else "") for p in f["probes"] if p["profile"] == prof and p["explicit_ua"] == ua), "-")
            for prof in ("chrome", "chrome124", "safari17_0") for ua in (True, False)] + [
            next((str(p.get("status")) + ("+ch" if p.get("challenge") else "") for p in f["probes"] if p["profile"] == lab), "-")
            for lab in ("requests_replay_headers", "requests_nav_headers")])
    md(rows, ["dealer", "pagination", "url", "chrome+UA", "chrome own", "chrome124+UA", "chrome124 own", "safari+UA", "safari own",
              "requests, replay headers", "requests, synth nav headers + same-site Referer"])

open(os.path.join(RUN, "tables.md"), "w").write("\n".join(out))
print("\n".join(out)[:200])
