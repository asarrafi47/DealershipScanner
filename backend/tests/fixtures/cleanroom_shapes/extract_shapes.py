"""Rebuild the N1.4 failure-shape fixtures (this directory) from the frozen clean room.

Not a test and not collected by pytest. Read-only: every query runs through
psql with ``default_transaction_read_only=on`` against ``cars_cleanroom`` on
the local socket (-h /tmp -p 5432); the triage rows come from the fleet run
dir's ``s*/triage.json``. ``workspace/`` exists only in the main checkout.

    python3 -I backend/tests/fixtures/cleanroom_shapes/extract_shapes.py \
        backend/tests/fixtures/cleanroom_shapes [identity retirements fivestar chapman]

``CLEANROOM_RUN_DIR`` overrides the run dir (default
``<main checkout>/workspace/cleanroom_2026_10/runs/fleet_20261013T140256Z``).
"""
from __future__ import annotations

import glob
import json
import os
import subprocess
import sys
from collections import defaultdict

RUN = os.environ.get("CLEANROOM_RUN_DIR") or subprocess.run(
    ["git", "-C", os.path.dirname(os.path.abspath(__file__)), "worktree", "list", "--porcelain"],
    capture_output=True, text=True, check=False,
).stdout.split("\n", 1)[0].removeprefix("worktree ") + "/workspace/cleanroom_2026_10/runs/fleet_20261013T140256Z"
DB = "cars_cleanroom"
ENV = dict(os.environ, PGOPTIONS="-c default_transaction_read_only=on -c statement_timeout=120000")


def q(sql: str) -> list[list[str]]:
    out = subprocess.run(["psql", "-X", "-h", "/tmp", "-p", "5432", "-d", DB, "-At", "-F", "\x1f", "-c", sql],
                         env=ENV, check=True, capture_output=True, text=True, timeout=130).stdout
    return [ln.split("\x1f") for ln in out.splitlines() if ln]


def identity(out_dir: str) -> None:
    triage = []
    for f in sorted(glob.glob(f"{RUN}/s*/triage.json")):
        triage += json.load(open(f)).get("dealers") or []
    runs = {}
    for r in q("select id, dealer_id, finished_at, duration_seconds, upserted, inventory_rows, deduped_rows, "
               "coalesce(error,''), coalesce(provider,''), summary_json from scan_runs"):
        runs[int(r[0])] = r
    cars = defaultdict(lambda: {"active": {}, "inactive": {}, "active_scraped_at": None, "inactive_scraped_at": None})
    for did, cond, act, n, mx in q("select dealer_id, coalesce(condition,''), coalesce(listing_active,1), count(*), max(scraped_at) "
                                    "from cars group by 1,2,3"):
        c = cars[did]
        key = "active" if act == "1" else "inactive"
        c[key][cond] = int(n)
        c[key + "_scraped_at"] = max(filter(None, [c[key + "_scraped_at"], mx]))
    cols = ["dealer_id", "verdict", "known_before", "triage_rows", "triage_rows_stamped", "triage_vin_owner_conflicts",
            "triage_active_after", "scan_run", "cars"]
    rows = []
    for t in sorted(triage, key=lambda r: r["dealer_id"]):
        did = t["dealer_id"]
        sr = None
        if t.get("run_id") is not None:
            r = runs[int(t["run_id"])]
            assert r[1] == did, (did, r[1])
            s = json.loads(r[9] or "{}")
            cc = s.get("capture_coverage") or {}
            sr = {
                "id": int(r[0]), "finished_at": r[2], "duration_seconds": float(r[3] or 0),
                "upserted": int(r[4] or 0), "inventory_rows": int(r[5] or 0), "deduped_rows": int(r[6] or 0),
                "error": r[7] or None, "provider": r[8] or None,
                "summary": {
                    "capture_coverage": {k: cc.get(k) for k in ("n", "price", "trim", "exterior_color") if k in cc},
                    "vin_owner_conflicts": s.get("vin_owner_conflicts"),
                    "vin_owner_conflict_owners": s.get("vin_owner_conflict_owners") or {},
                    "reconcile_marked_inactive": (s.get("reconcile") or {}).get("marked_inactive"),
                },
            }
        rows.append([did, t.get("verdict"), t.get("known_before"), t.get("rows"), t.get("rows_stamped"),
                     t.get("vin_owner_conflicts"), t.get("active_after"), sr, cars.get(did)])
    tot = q("select count(*), count(*) filter (where coalesce(listing_active,1)=1), "
            "count(*) filter (where coalesce(listing_active,1)=0) from cars")[0]
    doc = {
        "source": {
            "db": "cars_cleanroom (frozen, read-only)",
            "run_dir": "workspace/cleanroom_2026_10/runs/fleet_20261013T140256Z",
            "triage": "s0..s7/triage.json (one row per dealer: the latest run, lifecycle retries replace first passes)",
            "scan_runs": "the triage row's run_id; summary_json trimmed to capture_coverage{n,key fields}, vin_owner_conflicts, owners, reconcile.marked_inactive",
            "cars": "per dealer: count(*) by condition and listing_active, max(scraped_at)",
            "extracted": "2026-10-16 by N1.4 (extract_shapes.py in this directory, read-only psql)",
        },
        "fleet_started": "2026-10-13T14:02:56Z",
        "expected": {
            "rows_captured": 208233, "rows_refused_vin_owner": 9610, "rows_written_with_scan_run": 198623,
            "rows_written_without_scan_run": 834, "active": 199457, "inactive": 21, "cars": 199478,
        },
        "sql_totals": {"cars": int(tot[0]), "active": int(tot[1]), "inactive": int(tot[2])},
        "columns": cols,
        "dealers": rows,
    }
    path = os.path.join(out_dir, "triage_identity_20261013.json")
    with open(path, "w") as fh:
        json.dump(doc, fh, separators=(",", ":"), sort_keys=False)
        fh.write("\n")
    print("identity", path, len(rows), doc["sql_totals"])


RETIRE_DEALERS = {
    "terrylabontechevy-com": ("Terry Labonte Chevrolet", "https://www.terrylabontechevy.com", "carscommerce"),
    "coastlinecdjr-com": ("Coastline Chrysler Dodge Jeep Ram", "https://www.coastlinecdjr.com", "carscommerce"),
    "napletonhondamortongrove-com": ("Napleton Honda of Morton Grove", "https://www.napletonhondamortongrove.com", "carscommerce"),
    "southcountylexus-com": ("Lexus", "https://southcountylexus.com", "dealer_dot_com"),
}


def retirements(out_dir: str) -> None:
    ids = ",".join(f"'{d}'" for d in RETIRE_DEALERS)
    data = {}
    for did in RETIRE_DEALERS:
        data[did] = {"first_pass": [], "retry_new_rows": {}, "first_pass_at": None, "retry_at": None}
    for did, vin, cond, act, first_seen, scraped, removed in q(
            f"select dealer_id, vin, coalesce(condition,''), coalesce(listing_active,1), first_seen_at, scraped_at, "
            f"coalesce(listing_removed_at,'') from cars where dealer_id in ({ids}) and first_seen_at < '2026-10-14' order by dealer_id, vin"):
        d = data[did]
        d["first_pass"].append({"vin": vin, "condition": cond, "re_seen_by_retry": act == "1",
                                "retired_in_run_at": removed or None})
        d["first_pass_at"] = first_seen
        if act == "1":
            d["retry_at"] = scraped
    for did, cond, n, scraped in q(
            f"select dealer_id, coalesce(condition,''), count(*), max(scraped_at) from cars where dealer_id in ({ids}) "
            f"and first_seen_at >= '2026-10-14' group by 1,2 order by 1,2"):
        data[did]["retry_new_rows"][cond] = int(n)
        data[did]["retry_at"] = scraped
    doc = {
        "source": {
            "db": "cars_cleanroom (frozen, read-only)",
            "first_pass": "rows with first_seen_at < 2026-10-14 (written by the 10-13 first pass)",
            "re_seen_by_retry": "listing_active = 1 (the retry re-stamped scraped_at); 0 = retired by inventory_reconcile",
            "retry_new_rows": "rows first seen by the 10-14 lifecycle retry, by condition (VINs synthesized in the test)",
            "logs": "s5/scanner.log:1973 terry marked_inactive=16; s2:1892 coastline 3; s0:1764 napleton 1; s7:1729 southcountylexus 1",
            "extracted": "2026-10-16 by N1.4 (extract_shapes.py in this directory, read-only psql)",
        },
        "dealers": {did: {"name": RETIRE_DEALERS[did][0], "url": RETIRE_DEALERS[did][1],
                          "provider": RETIRE_DEALERS[did][2], **data[did]} for did in RETIRE_DEALERS},
    }
    path = os.path.join(out_dir, "inrun_retirements_20261014.json")
    with open(path, "w") as fh:
        json.dump(doc, fh, indent=1)
        fh.write("\n")
    for did, d in data.items():
        print(did, len(d["first_pass"]), sum(x["re_seen_by_retry"] for x in d["first_pass"]), d["retry_new_rows"],
              d["first_pass_at"], d["retry_at"])


def fivestar(out_dir: str) -> None:
    """Every row filed under 5starford-com, with the per-card Jazel account
    (accountId / accountName / accountAddress) where it is known: from N1.2's
    saved SRP pages (2026-10-16), the 2026-10-14 live VDP check, or the VDP
    city when the site's account pool has exactly one store in that city."""
    evidence = os.path.join(os.path.dirname(os.path.dirname(RUN.rstrip("/"))), "analysis", "platform_evidence", "jazel")
    fmap = json.load(open(os.path.join(evidence, "field_map.json")))
    accounts = {k.split(":", 1)[1]: v for k, v in fmap["store_id_map"].items()}
    seen: dict[str, str] = {}
    pool: set[str] = set()
    for name in ("payload_5starford-com.json", "payload_fivestarforddallas-com.json"):
        for pg in json.load(open(os.path.join(evidence, name)))["pages"]:
            pool |= set((pg.get("site") or {}).get("account_ids") or [])
            for car in pg.get("cars") or []:
                j = car["jzlSetVehicleInfoContext"]
                seen[j["vin"]] = str(j["accountId"])
    city_of_account = {a: accounts[a]["accountAddress"] for a in pool}
    by_city: dict[str, list[str]] = {}
    for city in ("North Richland Hills", "Dallas", "Carrollton", "Lewisville", "Grapevine"):
        by_city[city.replace(" ", "_")] = sorted(a for a, addr in city_of_account.items() if f" {city} TX " in addr)
    live = {"WAUABAF46MA015324": "108"}
    conflict = {r[0] for r in q("select vin from vin_owner_conflicts where owner_dealer_id='5starford-com' "
                                "and claimant_dealer_id='fivestarforddallas-com'")}
    rows = []
    for vin, cond, year, make, model, trim, price, mileage, stock, url in q(
            "select vin, coalesce(condition,''), coalesce(year::text,''), coalesce(make,''), coalesce(model,''), "
            "coalesce(trim,''), coalesce(price::text,''), coalesce(mileage::text,''), coalesce(stock_number,''), "
            "coalesce(source_url,'') from cars where dealer_id='5starford-com' order by vin"):
        city = url.split("----")[-1].split("--TX")[0] if "----" in url else ""
        acct, how = None, None
        if vin in seen:
            acct, how = seen[vin], "payload_2026-10-16"
        elif vin in live:
            acct, how = live[vin], "live_vdp_2026-10-14"
        elif len(by_city.get(city) or []) == 1:
            acct, how = by_city[city][0], "only_account_in_vdp_city"
        if acct is not None:
            assert f" {city.replace('_', ' ')} TX " in accounts[acct]["accountAddress"], (vin, city, acct)
        rows.append({"vin": vin, "condition": cond, "year": int(year) if year else None, "make": make, "model": model,
                     "trim": trim, "price": float(price) if price else None, "mileage": int(float(mileage)) if mileage else None,
                     "stock_number": stock, "vdp": url, "vdp_city": city, "in_dallas_feed": vin in conflict,
                     "accountId": acct, "account_source": how})
    doc = {
        "source": {
            "db": "cars_cleanroom (frozen, read-only): every row filed under 5starford-com",
            "vdp_city": "city slug of source_url (the Jazel vdpLink)",
            "in_dallas_feed": "VIN in vin_owner_conflicts (owner 5starford-com, claimant fivestarforddallas-com)",
            "accountId": ("the card's Jazel account (jzlSetVehicleInfoContext accountId; name and address in `accounts`). "
                          "account_source: payload_2026-10-16 = the VIN is in N1.2's saved SRP pages "
                          "(analysis/platform_evidence/jazel/payload_*.json); live_vdp_2026-10-14 = WAUABAF46MA015324, "
                          "Vehicle Location Five Star Chevrolet (analysis/dealers/5starford-com.json); "
                          "only_account_in_vdp_city = the site's account pool (page account_ids) holds exactly one store "
                          "in the card's VDP city (174/174 saved cards agree account and city). Carrollton has two "
                          "accounts (6 Ford, 108 Chevrolet): a Carrollton card not seen in either source has no account "
                          "(cannot_assess) and keeps only its VDP city"),
            "accounts": "analysis/platform_evidence/jazel/field_map.json store_id_map (N1.2, 2026-10-16)",
            "extracted": "2026-10-16 by N1.4 (extract_shapes.py in this directory, read-only psql)",
        },
        "account_pool": sorted(pool, key=int),
        "accounts": {a: {k: accounts[a].get(k) for k in ("accountName", "accountAddress", "dealer_id")} for a in sorted(pool, key=int)},
        "roster": {
            "5starford-com": {"registry_id": 276, "name": "Five Star Ford North Richland Hills",
                              "url": "https://www.5starford.com", "city": "Fort Worth", "state": "TX", "zip": "76180",
                              "manifest_name_in_cleanroom": "Five Star Ford Parts (registry name error, N8.9)"},
            "fivestarforddallas-com": {"registry_id": 233, "name": "Five Star Ford Dallas",
                                       "url": "https://www.fivestarforddallas.com", "city": "Dallas", "state": "TX",
                                       "zip": "75252"},
        },
        "cards": rows,
    }
    path = os.path.join(out_dir, "fivestar_jazel_group_srp_20261013.json")
    with open(path, "w") as fh:
        json.dump(doc, fh, separators=(",", ":"))
        fh.write("\n")
    from collections import Counter
    print("fivestar", len(rows), Counter(r["vdp_city"] for r in rows), sum(r["in_dallas_feed"] for r in rows),
          Counter(r["vdp_city"] for r in rows if r["in_dallas_feed"]),
          Counter((r["vdp_city"], r["accountId"], r["account_source"]) for r in rows))


def chapman(out_dir: str) -> None:
    rows = []
    for vin, cond, year, make, model, stock, price, img, url in q(
            "select c.vin, coalesce(c.condition,''), coalesce(c.year::text,''), coalesce(c.make,''), coalesce(c.model,''), "
            "coalesce(c.stock_number,''), coalesce(c.price::text,''), c.image_url, coalesce(c.source_url,'') "
            "from vin_owner_conflicts v join cars c on c.vin=v.vin where v.owner_dealer_id='chapmanfordaz-com' "
            "and v.claimant_dealer_id='chapmandodge-com' and c.image_url like '%/vehicles/CAE/%' order by c.vin limit 6"):
        rows.append({"vin": vin, "condition": cond, "year": int(year) if year else None, "make": make, "model": model,
                     "stock_number": stock, "price": float(price) if price else None, "image": img, "owner_vdp": url})
    counts = q("select substring(c.image_url from '/vehicles/(CA[A-Z])/') code, count(*) from vin_owner_conflicts v "
               "join cars c on c.vin=v.vin where v.owner_dealer_id='chapmanfordaz-com' and v.claimant_dealer_id='chapmandodge-com' "
               "group by 1 order by 1")
    doc = {
        "source": {
            "db": "cars_cleanroom (frozen, read-only)",
            "pair": "vin_owner_conflicts owner chapmanfordaz-com (first writer) <- claimant chapmandodge-com",
            "lot_code_counts": {c: int(n) for c, n in counts},
            "lot_code_meaning": "photo path photos.chapmanchoice.com/vehicles/<lot>/: CAE = Chapman Dodge (claimant's own code), "
                                "CAU = Chapman Ford, CAY/CAV = other Chapman stores (conflicts_rca.md section 3)",
            "claimant_stamp": ("the rows carry the store stamp dealer_dot_com.py:_rooftop_from_account builds from accountId; "
                               "the account key is a placeholder: whether chapmandodge's live payload stamps these cars is "
                               "cannot_assess until the N1.2 dossier"),
            "extracted": "2026-10-16 by N1.4 (extract_shapes.py in this directory, read-only psql)",
        },
        "roster": {
            "chapmanfordaz-com": {"registry_id": 167, "name": "Chapman Ford", "url": "https://www.chapmanfordaz.com",
                                  "street": "3950 N 89th St.", "city": "Scottsdale", "state": "AZ", "zip": "85251",
                                  "provider": "chapman"},
            "chapmandodge-com": {"registry_id": 417, "name": "Chapman Dodge Chrysler Jeep Ram Scottsdale",
                                 "url": "https://www.chapmandodge.com", "street": "3800 N 89th St.", "city": "Scottsdale",
                                 "state": "AZ", "zip": "85251", "provider": "dealer_dot_com"},
        },
        "cars": rows,
    }
    path = os.path.join(out_dir, "chapman_scan_order_20261013.json")
    with open(path, "w") as fh:
        json.dump(doc, fh, indent=1)
        fh.write("\n")
    print("chapman", len(rows), doc["source"]["lot_code_counts"])


if __name__ == "__main__":
    out = sys.argv[1]
    os.makedirs(out, exist_ok=True)
    which = sys.argv[2:] or ["identity", "retirements", "fivestar", "chapman"]
    for w in which:
        globals()[w](out)
