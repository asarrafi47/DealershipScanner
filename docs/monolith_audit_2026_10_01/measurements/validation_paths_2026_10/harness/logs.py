"""Append dated P0B.1 measurement blocks to workspace/dealer_logs/<id>/{discovery,scan_runs}.md.

Append-only; one block per file per dealer; idempotent (skips a dealer whose file already
carries this run's marker). Only dealers fetched by measure.py / vdp.py.
"""
from __future__ import annotations

import collections
import json
import os
import sys

RUN = "/private/tmp/claude-501/phase0/p0b1_run"
LOGS = "/Users/asarrafi/Projects/DealershipScanner/workspace/dealer_logs"
MARK = "validation-path measurement (P0B.1, no writes)"
DOC = "docs/monolith_audit_2026_10_01/measurements/validation_paths_2026_10.md"


def wire_line(wire: list[dict]) -> str:
    c = collections.Counter()
    for w in wire:
        c[(w.get("lib"), w.get("profile") or "-", str(w.get("status")) + ("+challenge" if w.get("challenge") else ""))] += 1
    return "; ".join(f"{lib}/{prof} {st} x{n}" for (lib, prof, st), n in sorted(c.items(), key=lambda kv: -kv[1]))


def block_for(did: str, r: dict, vdp: list[dict], stamp: str, feed: dict | None = None) -> tuple[str, str]:
    L = [f"## {stamp} {MARK}", "",
         f"- what: read-only measurement for remediation unit P0B.1 ({DOC}). Recipes read with json.load; every recipe/DB "
         f"writer patched to raise; DB session read-only; requests paced >= 1.5 s per host; worktree at dbdbf5cba.",
         "- nothing written: no recipe file, no dealer_recipes row, no scan_hints, no car row (sha256 manifest of "
         "workspace/recipes and max(updated_at) of dealer_recipes identical before and after).",
         f"- base url: {r.get('base_url')}; live recipes {r.get('live_recipes')}, stale {r.get('stale_recipes')}; "
         f"store place keys from registry/hints: {', '.join(r.get('place_keys') or []) or 'none'}"]
    s2 = (r.get("steps") or {}).get("set2") or {}
    s40 = (r.get("steps") or {}).get("set40") or {}
    rep2, rep40 = s2.get("report") or {}, s40.get("report") or {}
    L.append(f"- set gate (validate_recipe_set) pages=2: {s2.get('status')} ({'; '.join(rep2.get('reasons') or []) or 'no reasons'}), "
             f"{rep2.get('vins_total')} VINs, site total {rep2.get('site_total')}, coverage {rep2.get('coverage')}")
    L.append(f"- set gate pages=40: {s40.get('status')} ({'; '.join(rep40.get('reasons') or []) or 'no reasons'}), "
             f"{rep40.get('vins_total')} VINs, site total {rep40.get('site_total')}, coverage {rep40.get('coverage')}")
    counts = {c["i"]: c for c in (r.get("steps") or {}).get("count", {}).get("counts") or []}
    c2 = list(rep2.get("recipes") or [])
    c40 = list(rep40.get("recipes") or [])
    for rec in r.get("recipes") or []:
        u, i = rec["url"], rec["i"]
        a = counts.get(i, {})
        b = c2[i] if i < len(c2) else {}
        c = c40[i] if i < len(c40) else {}
        extra = []
        if rec.get("total_count") and c.get("vins") is not None and rec["pagination"] in ("dep_srp_page", "html_page_query", "jazel_srp_page"):
            extra.append(f"stored total_count {rec['total_count']} vs {c.get('vins')} VINs walked")
        if c.get("error"):
            extra.append(f"error {c.get('error')}")
        flags = [k for k in ("auth_needed", "auth_mid_walk", "short_page", "section_scoped") if c.get(k)]
        if flags:
            extra.append("flags " + ",".join(flags))
        L.append(f"  - {rec['method']} {u[:110]} [{rec['pagination']}, {rec.get('platform')}]: stored rows {rec['vehicle_rows']} / total {rec.get('total_count')}; "
                 f"validate_recipe {a.get('n')}; set check p2 {b.get('vins')} VINs (statuses {'/'.join(str(p.get('status')) for p in b.get('pages') or []) or '-'}), "
                 f"p40 {c.get('vins')} VINs over {len(c.get('pages') or [])} page(s), site total {c.get('site_total')}"
                 + (f"; {'; '.join(extra)}" if extra else ""))
    wire = r.get("wire") or []
    L.append(f"- wire ({len(wire)} requests): {wire_line(wire) or 'none'}")
    if r.get("errors"):
        L.append(f"- harness errors: {json.dumps([e.get('error') or e.get('write_attempt') for e in r['errors']])[:400]}")
    if vdp:
        cl = lambda k: sum(1 for v in vdp if (v.get(k) or {}).get("cleared"))  # noqa: E731
        L.append(f"- VDP probes ({len(vdp)} VDPs, 5 variants each): today's prefetch fetch cleared {cl('today')}, chrome124+UA {cl('c124_ua')}, "
                 f"safari17_0+Chrome UA {cl('saf_ua')}, safari17_0 own UA {cl('saf_own')}, chrome own UA {cl('chrome_own')}")
    if feed:
        pr = "; ".join(f"{x['profile']}{'' if x['profile'].startswith('requests') else ('+ChromeUA' if x['explicit_ua'] else ' own UA')} {x.get('status')}"
                       for x in feed.get("probes") or [])
        L.append(f"- feed page-1 probe ({feed.get('method')} {str(feed.get('url'))[:90]}): {pr}")
    L.append("- requests made 2026-10-08 12:21-13:42 UTC from the MBP (home IP); Railway egress may differ.")
    discovery = "\n".join(L) + "\n"
    S = [f"## {stamp} measurement P0B.1, no writes", "",
         "- not a scan: nothing upserted, no reconcile, no recipe or hint write; measurement of validation paths only "
         f"(details in discovery.md, same stamp; {DOC}).",
         f"- live VINs seen: set gate pages=40 {rep40.get('vins_total')} (site total {rep40.get('site_total')}); "
         f"validate_recipe per recipe {[counts.get(x['i'], {}).get('n') for x in r.get('recipes') or []]}",
         f"- requests {len(wire)}: {wire_line(wire) or 'none'}"]
    return discovery, "\n".join(S) + "\n"


def append(path: str, text: str, head: str) -> bool:
    if os.path.exists(path):
        cur = open(path, encoding="utf-8").read()
        if text.splitlines()[0] in cur:
            return False
        sep = "" if cur.endswith("\n\n") else ("\n" if cur.endswith("\n") else "\n\n")
    else:
        cur, sep = "", head
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(sep + text)
    return True


def main():
    stamp = sys.argv[1]  # e.g. "2026-10-08 12:21 UTC"
    vdp = {}
    p = os.path.join(RUN, "vdp_results.json")
    if os.path.exists(p):
        for v in json.load(open(p)).get("vdp") or []:
            vdp.setdefault(v["dealer"], []).append(v)
    feeds = {}
    if os.path.exists(p):
        for f in json.load(open(p)).get("feed_ua") or []:
            feeds[f["dealer"]] = f
    done = []
    for f in sorted(os.listdir(os.path.join(RUN, "results"))):
        if f.startswith("_") or not f.endswith(".json"):
            continue
        did = f[:-5]
        r = json.load(open(os.path.join(RUN, "results", f)))
        if not r.get("wire") and not r.get("steps"):
            continue
        disc, runs = block_for(did, r, vdp.get(did) or [], stamp, feeds.get(did))
        d = os.path.join(LOGS, did)
        os.makedirs(d, exist_ok=True)
        a = append(os.path.join(d, "discovery.md"), disc, f"# {did} — discovery\n\nProcess: docs/NETWORK_SCAN_PROCESS.md\n\n")
        b = append(os.path.join(d, "scan_runs.md"), runs, f"# {did} — scan runs\n\n")
        done.append((did, a, b))
    for x in done:
        print(*x)


if __name__ == "__main__":
    main()
