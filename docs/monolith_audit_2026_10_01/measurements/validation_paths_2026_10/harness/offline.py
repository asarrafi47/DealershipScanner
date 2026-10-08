"""P0B.1 offline part: sub-floor live recipes and HTML-walk totals (read-only).

Reads workspace/recipes/*.json with plain json.load and the dealer_recipes /
cars exports taken through a read-only psql session (db_hints.psv,
active_counts.psv). Imports only pure helpers from the worktree.
"""
import collections
import glob
import json
import os
import re
import sys
from urllib.parse import parse_qsl, urlparse

sys.path.insert(0, "/private/tmp/claude-501/phase0/p0b1")
from backend.scanner.recipes import EndpointRecipe, recipe_is_section_scoped  # noqa: E402
from backend.scanner.recipe_validation import platform_of, recipe_condition_filter  # noqa: E402

RECIPES = "/Users/asarrafi/Projects/DealershipScanner/workspace/recipes"
HERE = os.path.dirname(os.path.abspath(__file__))
VIN_RE = re.compile(r"\b[A-HJ-NPR-Z0-9]{17}\b")
HTML_WALK = {"dep_srp_page", "html_page_query", "jazel_srp_page"}
P0B2 = {"autosavvy-com", "terrylabontechevy-com", "toyotacarlsbad-com", "avondaletoyota-com",
        "mountainstatestoyota-com", "cavendertoyota-com", "duvalford-com", "mbofstevenscreek-com"}


def load_hints():
    out = {}
    for line in open(os.path.join(HERE, "db_hints.psv")):
        parts = line.rstrip("\n").split("|")
        if len(parts) < 5:
            continue
        did = parts[0]
        hints_raw = "|".join(parts[1:-3])
        try:
            hints = json.loads(hints_raw)
        except ValueError:
            hints = {}
        out[did] = {"hints": hints, "recipe_count": parts[-3], "max_saved_at": parts[-2], "md5": parts[-1]}
    return out


def load_active():
    out = {}
    for line in open(os.path.join(HERE, "active_counts.psv")):
        p = line.rstrip("\n").split("|")
        if len(p) == 5 and p[0]:
            out[p[0]] = {"active": int(p[1]), "new": int(p[2]), "used": int(p[3]), "last": p[4]}
    return out


def shape(r: dict, rec: EndpointRecipe) -> str:
    url = r.get("url") or ""
    low = url.lower()
    body = r.get("post_template") or ""
    q = {k.lower() for k, _ in parse_qsl(urlparse(url).query, keep_blank_values=True)}
    if "recommend" in low or "ws-rec" in low or "recommend" in body.lower():
        return "widget:vehicles-recommendations/ws-rec"
    if "page-data" in low:
        return "gatsby:page-data"
    if "vin" in q:
        return "query:?vin="
    if "getinventoryandfacets" in low or "getinventoryandfacets" in body.lower():
        return "fragment:getInventoryAndFacets"
    if VIN_RE.search(url) or VIN_RE.search(body if body != "None" else ""):
        return "vin-list:VIN in URL/body"
    pins = sorted(recipe_condition_filter(rec))
    if pins and r.get("pagination") != "none":
        return "side:condition-pinned paginated (" + ",".join(pins) + ")"
    if r.get("pagination") == "none":
        return "pagination:none (other single-shot)"
    if recipe_is_section_scoped(rec):
        return "section-scoped carscommerce"
    return "paginated, unpinned (other)"


def to_rec(r: dict) -> EndpointRecipe:
    pt = r.get("post_template")
    return EndpointRecipe(**{k: v for k, v in r.items() if k in EndpointRecipe.__dataclass_fields__} | {"post_template": pt})


def main():
    hints = load_hints()
    active = load_active()
    sub10, html_walk = [], []
    union = {}
    for p in sorted(glob.glob(os.path.join(RECIPES, "*.json"))):
        name = os.path.basename(p)
        if name.startswith("_"):
            continue
        did = name[:-5]
        rows = json.load(open(p))
        live = [r for r in rows if not r.get("stale")]
        status = str((hints.get(did, {}).get("hints") or {}).get("recipe_status") or "")
        dealer_rows = []
        for li, r in enumerate(live):
            rec = to_rec(r)
            vr = int(r.get("vehicle_rows") or 0)
            info = {"dealer": did, "i": li, "url": r.get("url"), "method": r.get("method"), "pagination": r.get("pagination"),
                    "provider_hint": r.get("provider_hint"), "platform": platform_of(rec), "vehicle_rows": vr,
                    "total_count": r.get("total_count"), "pins": sorted(recipe_condition_filter(rec)),
                    "recipe_status": status, "p0b2": did in P0B2,
                    "active": (active.get(did) or {}).get("active", 0)}
            info["shape"] = shape(r, rec)
            dealer_rows.append(info)
            if vr < 10:
                sub10.append(info)
            if (r.get("pagination") in HTML_WALK or r.get("provider_hint") == "html_cards") and r.get("total_count"):
                html_walk.append(info)
        union[did] = dealer_rows
    # D-OD1 union impact (stored vehicle_rows as the proxy; the online part measures it live)
    impact = []
    for did, rs in union.items():
        if not rs:
            continue
        today = [r for r in rs if r["vehicle_rows"] >= 10]
        any0 = [r for r in rs if r["vehicle_rows"] > 0]
        st = rs[0]["recipe_status"]
        verdict_ok = st == "ok" or st.startswith("uncertain") or st == ""
        rec_rule = today + [r for r in rs if 0 < r["vehicle_rows"] < 10 and r["pins"] and r["pagination"] != "none" and verdict_ok]
        if len(today) != len(any0) or len(today) != len(rec_rule):
            impact.append({"dealer": did, "recipes": len(rs), "today_n": len(today), "today_rows": sum(r["vehicle_rows"] for r in today),
                           "any0_n": len(any0), "any0_rows": sum(r["vehicle_rows"] for r in any0),
                           "rec_n": len(rec_rule), "rec_rows": sum(r["vehicle_rows"] for r in rec_rule),
                           "recipe_status": st, "active": rs[0]["active"],
                           "sub_floor": [(r["shape"], r["vehicle_rows"], r["url"][:90]) for r in rs if r["vehicle_rows"] < 10]})
    json.dump({"sub10": sub10, "html_walk": html_walk, "od1_impact": impact}, open(os.path.join(HERE, "offline.json"), "w"), indent=1)
    s10 = [r for r in sub10]
    s5 = [r for r in sub10 if r["vehicle_rows"] < 5]
    print("live recipes <10:", len(s10), "dealers", len({r["dealer"] for r in s10}))
    print("live recipes <5:", len(s5), "dealers", len({r["dealer"] for r in s5}))
    print(collections.Counter(r["shape"] for r in s10).most_common())
    eq = [r for r in html_walk if int(r["total_count"]) == r["vehicle_rows"]]
    gt = [r for r in html_walk if int(r["total_count"]) > r["vehicle_rows"]]
    lt = [r for r in html_walk if int(r["total_count"]) < r["vehicle_rows"]]
    print("html walks with total:", len(html_walk), "eq", len(eq), "total>rows", len(gt), "total<rows", len(lt))
    print("od1 impact dealers:", len(impact))


if __name__ == "__main__":
    main()
