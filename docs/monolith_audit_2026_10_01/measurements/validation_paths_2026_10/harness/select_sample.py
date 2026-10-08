import glob, json, os, random
RECIPES = "/Users/asarrafi/Projects/DealershipScanner/workspace/recipes"
P0B2 = {"autosavvy-com","terrylabontechevy-com","toyotacarlsbad-com","avondaletoyota-com","mountainstatestoyota-com","cavendertoyota-com","duvalford-com","mbofstevenscreek-com"}
active = {}
for l in open("active_counts.psv"):
    p = l.rstrip("\n").split("|")
    if len(p) == 5 and p[0]: active[p[0]] = int(p[1])
by_pg = {}
for p in sorted(glob.glob(os.path.join(RECIPES, "*.json"))):
    did = os.path.basename(p)[:-5]
    if did.startswith("_") or did in P0B2: continue
    live = [r for r in json.load(open(p)) if not r.get("stale")]
    pgs = {r.get("pagination") for r in live}
    if len(pgs) == 1 and active.get(did, 0) >= 50:
        by_pg.setdefault(next(iter(pgs)), []).append(did)
fixed = ["hondaofelcajon-com","toyotaofhb-com","darcarshondatenafly-com","covertbuickgmc-com",
         "downeyhyundai-com","hyundaiofcookeville-com","mymetrohonda-com","nissanofcookeville-com","autoboutiqueohio-com",
         "5starford-com","encinitasford-com","fivestarforddallas-com","hemborgford-com","mcgrathcityhonda-com",
         "highcountrytoyota-com","robinsford-com",
         "bmwoffremont-com","howardorloffvolvocars-com","crownlexus-com","jordanford-net","gardenahonda-com",
         "mikecalverttoyota-com","scottclarkstoyota-com","audibellevue-com"]
rng = random.Random(20261008)
want = {"cosmos_pt": 4, "page_query": 4, "carscommerce_page": 4, "dealer_com_start": 4, "typesense_page": 1, "algolia_page": 1}
picked = list(fixed)
for pg, k in want.items():
    pool = [d for d in by_pg.get(pg, []) if d not in picked]
    rng.shuffle(pool)
    picked += pool[:k]
    print(pg, pool[:k])
json.dump(picked, open("sample.json", "w"), indent=0)
print(len(picked))
