# Scanning on Railway

Execution plan step 2 (owner decision 2026-09-28): the fleet scan runs on Railway, not on
home machines. This page is the runbook and the record of the first test (2026-09-28).
The per-dealership process is unchanged: `docs/NETWORK_SCAN_PROCESS.md`.

**Status 2026-09-28:** service `scanner-nightly` is deployed and idle. A 20-dealer test ran
twice from Railway against production. **No cron schedule is set.** The recommendation at
the end says when to set one.

## Architecture

```
Railway project dealership-scanner / environment production

  scanner-nightly (Dockerfile.scanner, 1.09 GB image, no browser)
    CMD scripts/railway_scan_fleet.sh
      /app/workspace -> /data/workspace          (volume scanner-nightly-volume at /data)
      python -m backend.scripts.fleet_scan
        roster   SCAN_DEALERS (never filtered), or SCAN_FLEET=1 -> dealers WITH active inventory
                   minus do_not_scan, plus deploy/railway/revived_dealers.txt (458 locally on 09-29;
                   was active ∪ dealer_recipes = 594 until the 09-29 incident, see below)
        shards   SCAN_SHARDS x  python -m backend.scripts.dealer_pipeline --dealers <slice> --batch SCAN_BATCH
                   each with its own SCANNER_LOCK_PATH; rosters balanced by active row count
                   (inside: recipe -> scanner.py HTTP-only -> vPIC heal -> assess -> reconcile
                    -> lifecycle -> dealer_logs)
        post     compute_market_stats; build_listings_grid_cards with FLASK_ENV=production
        summary  "FLEET SUMMARY {json}" + "fleet   done: ..." on stdout,
                 <run>/fleet_summary.json on the volume; exit 0 only when every shard
                 and both post steps exited 0 (Railway shows a non-zero exit as CRASHED)
          |
          v   private network: postgres.railway.internal:5432
  Postgres (production inventory, shared with web)
```

Nothing is started by a deploy: with neither `SCAN_FLEET=1` nor `SCAN_DEALERS` set the job
prints `idle` and exits 0 (after `SCAN_IDLE_HOLD_SECONDS`, see logs below).

### Image

| image | size | what is in it |
|---|---:|---|
| `dealership-scanner:http-only-20260928` (old `Dockerfile.scanner`, `requirements.txt`) | 11.8 GB | torch, sentence-transformers, playwright, crawl4ai, flask, ... (6.9 GB pip layer) |
| `Dockerfile.scanner` + `requirements-scanner.txt` (this change) | **1.09 GB** | python-dotenv, requests, curl_cffi, psycopg[binary], pydantic, beautifulsoup4, lxml, aiohttp, pgeocode (pandas/numpy), thefuzz, fake-useragent; 268 MB pip layer + 477 MB repo copy (398 MB of it is `backend/dictionary`) |

`requirements-scanner.txt` came from an import trace of a real 2-dealer pipeline run
(`PYTHONPROFILEIMPORTTIME=1`, every subprocess) and is checked by
`python scripts/scanner_import_sweep.py` inside the image: it imports the entry modules
(pipeline, scanner CLI/orchestrator, vPIC, market stats, grid-card builder, discovery probe,
recipe synth/validation) and every module under `backend.scanner/db/enrichment/utils`, and
fails on any missing package outside the browser/ML/web allow-list. Run it after touching
either requirements file. The pipeline's browser discovery capture is skipped in this image
(`run_discovery_capture` checks for Playwright) — captures belong to `Dockerfile.discovery`.

### Service configuration

Set on the service (the dashboard / API settings are what Railway uses; the
`deploy/railway/railway.scanner-nightly.json` config file was **not** picked up by
`railway up` — the first deploy built with Railpack and failed — so the same values were set
with the API):

| setting | value | how |
|---|---|---|
| Dockerfile | `Dockerfile.scanner` | `serviceInstanceUpdate(dockerfilePath)` + variable `RAILWAY_DOCKERFILE_PATH` |
| restart policy | `NEVER` (a batch job) | `serviceInstanceUpdate(restartPolicyType: NEVER)` |
| resource limits | 16 vCPU / 16 GB (a cap; billing is on use) | `serviceInstanceLimitsUpdate(vCPUs: 16, memoryGB: 16)` |
| volume | `scanner-nightly-volume` at `/data` | `railway volume -s <service id> add --mount-path /data` |
| cron | **none** | set `cronSchedule` only per the recommendation below |
| replicas | 1 | |

API calls used (project `90c01e1e-50dc-462f-b6e8-59f34c755c2c`, environment
`abe0fc9d-2dd5-4217-b303-12c6c4ac498a`, service `5fe67db1-b87e-46dc-a7db-42d4e1834f39`):

```bash
railway api 'mutation { serviceInstanceUpdate(serviceId: "<svc>", environmentId: "<env>", input: { dockerfilePath: "Dockerfile.scanner" }) }'
railway api 'mutation { serviceInstanceUpdate(serviceId: "<svc>", environmentId: "<env>", input: { restartPolicyType: NEVER }) }'
railway api 'mutation { serviceInstanceLimitsUpdate(input: { serviceId: "<svc>", environmentId: "<env>", vCPUs: 16, memoryGB: 16 }) }'
# to enable the nightly (NOT done):
railway api 'mutation { serviceInstanceUpdate(serviceId: "<svc>", environmentId: "<env>", input: { cronSchedule: "0 9 * * *" }) }'
```

### Environment variables (service `scanner-nightly`)

| variable | value | meaning |
|---|---|---|
| `INVENTORY_DATABASE_URL` | `${{Postgres.DATABASE_URL}}` | private-network URL (`postgres.railway.internal`); no public proxy, no egress |
| `SCAN_SHARDS` | `8` | parallel `dealer_pipeline` processes |
| `SCAN_BATCH` | `6` | dealers per scanner process inside a shard |
| `SCAN_DATA_DIR` | `/data` | volume root; `/app/workspace` is a symlink to `/data/workspace` |
| `SCAN_IDLE_HOLD_SECONDS` | `1500` | an idle start stays up 25 min so the volume can be read (see logs) |
| `PYTHONPATH` | `/app` | |
| `RAILWAY_DOCKERFILE_PATH` | `Dockerfile.scanner` | |
| `SCAN_FLEET` | *unset* | `1` = fleet roster (active-inventory dealers, see "Roster rule"). Set together with the cron. |
| `SCAN_DEALERS` | *unset* | comma list; wins over `SCAN_FLEET` (used for the test) |
| `SCAN_POST_STEPS` | *unset* (=1) | `0` skips market stats + grid cards |
| `SCAN_PIPELINE_ARGS` | *unset* | extra `dealer_pipeline` flags, e.g. `--no-lifecycle` |
| `SCAN_KEEP_DAYS` | *unset* (=14) | run dirs and scan logs older than this are pruned at start |
| `SCANNER_VIN_OWNER_GUARD_HOURS` | *unset* (=48) | VIN ownership guard window; `0` disables (see "VIN ownership guard") |

No secrets are needed beyond the database reference: the scan uses no API keys.

### Where the logs live (decision)

**On the Railway volume**, not in the database. `workspace/` (dealer_logs, the recipe file
cache, pipeline run dirs, scan logs) is written by dozens of code paths through plain file
APIs (pipeline, recipe validator, discovery probe, platform clustering); moving them into
Postgres would be a rewrite and would put megabytes of markdown into the production
inventory database. The volume keeps the tree exactly as on a laptop:
`/data/workspace/dealer_logs/<dealer_id>/…`, `/data/workspace/pipeline/fleet_<UTC stamp>/`
(`s<i>.txt` roster, `s<i>.out` pipeline output, `s<i>/scanner.log`, `s<i>/triage.json`,
`fleet_summary.json`). Recipes themselves are in `dealer_recipes` (Postgres) and
materialize into the volume on first use. The stdout summary lines stay in Railway's log
retention.

A stopped service exposes no volume (`railway volume files download` fails with an SFTP
timeout). To read the logs, start an idle container and copy them out while it holds:

```bash
railway service redeploy --service scanner-nightly -y   # idle start, holds SCAN_IDLE_HOLD_SECONDS
railway ssh --service scanner-nightly -- sh -c 'cd /data && tar czf - workspace' > railway_workspace.tgz
```

After a run, append the per-dealer blocks to the local tree (`workspace/dealer_logs/`) so
the learning logs stay in one place — done for the 2026-09-28 test runs (blocks marked
`appended from the Railway scanner-nightly volume`).

## How to run it

Deploy (tracked files of `HEAD` only — never `.env`, never `workspace/`):

```bash
deploy/railway/deploy_scanner_nightly.sh          # railway up --detach from a clean git archive
```

Run a scan manually:

```bash
# a handful of dealers
railway variable set 'SCAN_DEALERS=harehonda-com,toyotaplace-com' --service scanner-nightly
#   (setting a variable redeploys; the container scans and exits)
railway logs --service scanner-nightly                   # follow
railway variable delete SCAN_DEALERS --service scanner-nightly   # back to idle

# the whole fleet once
railway variable set SCAN_FLEET=1 --service scanner-nightly
```

Locally, the same entrypoint (proven 2026-09-28 against local Postgres):

```bash
docker build -f Dockerfile.scanner -t dealership-scanner:slim .
docker run --rm -v "$PWD/tmpdata:/data" \
  -e INVENTORY_DATABASE_URL=postgresql://asarrafi@host.docker.internal:5432/cars \
  -e SCAN_DEALERS=alfaromeoofanaheimhills-com,lambonb-com -e SCAN_SHARDS=2 -e SCAN_POST_STEPS=0 \
  dealership-scanner:slim
# -> fleet done: 2 dealers, verdicts {'ok': 2}, 1.5 min, peak 339 MB, exit 0
```

or without Docker: `SCAN_DEALERS=a,b SCAN_SHARDS=2 python -m backend.scripts.fleet_scan`.

## Test from Railway, 2026-09-28 (20 dealers, 8 shards, batch 6)

Dealers chosen across platforms, each `ok` on the local 09-28 fleet run
(`workspace/pipeline/fleet_2026_09_28_*/triage.json`). "carscommerce" = Dealer Inspire
sites whose recipe replays `websites-search.api.carscommerce.inc` (the triage and
`dealer_recipes.provider_hint` label them `dealer_dot_com`).

Run 1 (`fleet_20260928T230514Z`) scanned all 20 fine, then 7 of 8 shards crashed in assess
with `InFailedSqlTransaction`: a query added in commit 5d1a85be2
(`scan_lab_report._provider_hint`) selected `dealer_recipes.stale` / `ORDER BY id`, neither
of which exists; on Postgres the failed statement poisoned the pipeline's transaction.
Fixed in 13edb393e (and it had been failing silently on the laptop too — single-dealer
shards only lose their accuracy block). Run 2 (`fleet_20260928T232028Z`) is the table below.

| platform | dealer | 09-28 local: verdict, rows (new/used), min | Railway run 2: verdict, rows (new/used), min | note |
|---|---|---|---|---|
| carscommerce | alfaromeoofanaheimhills-com | ok, 71 (6/65), 1.1 | ok, 71 (6/65), 5.2 | VDP cap hit |
| carscommerce | santamargaritatoyota-com | ok, 432 (399/33), 3.9 | inaccurate, 414 (382/32), 5.8 | vpic_fuel 36/414; same verdict locally with current assess code (35/432) |
| carscommerce | jimelliscdjrwoodstock-com | ok, 269 (203/66), 4.4 | ok, 267 (201/66), 5.4 | |
| carscommerce | normreevesvw-com | ok, 209 (177/32), 4.7 | ok, 208 (177/31), 5.3 | |
| carscommerce | lagunahyundai-com | ok, 222 (171/51), 5.1 | ok, 221 (170/51), 5.4 | homepage re-synth `homepage_unreachable_exc` — same locally (5 times in its discovery.md) |
| dealer.com | harehonda-com | ok, 232 (171/61), 1.4 | ok, 229 (171/58), 3.4 | |
| dealer.com | peachstateford-net | ok, 86 (52/34), 1.5 | ok, 88 (52/36), 3.0 | |
| dealer.com | northcountrycjdr-cars | ok, 174 (154/20), 1.7 | ok, 174 (154/20), 1.0 | |
| dealer.com | villagevw-com | ok, 153 (115/38), 2.0 | ok, 152 (115/37), 1.4 | |
| dealer_on_cosmos | paradiseautos-com | ok, 341 (259/82), 1.9 | ok, 342 (258/84), 3.7 | |
| dealer_on_cosmos | sutherlinsubaru-com | ok, 279 (167/112), 2.4 | ok, 279 (167/112), 3.4 | |
| dealer_on_cosmos | eddrogersvalleyford-com | ok, 154 (31/123), 2.5 | ok, 154 (31/123), 1.8 | |
| team_velocity | hileymazdahuntsville-com | ok, 283, 1.3 | ok, 283 (250/33), 3.4 | |
| team_velocity | ourismanhonda-com | ok, 395 (309/86), 2.8 | inaccurate, 397 (311/86), 1.9 | fuel_type missing 23/397; same verdict locally with current assess code (23/395) |
| team_velocity | murgadofordchicago-com | ok, 316 (262/54), 3.2 | ok, 316 (262/54), 2.0 | |
| dealer_eprocess | highcountrytoyota-com | ok, 232 (121/111), 2.2 | ok, 230 (119/111), 2.5 | |
| dealer_eprocess | lambonb-com | ok, 33 (4/29), 1.5 | **no_recipe**, 0 | **403 from Railway's IP** (below) |
| typesense | hondaofcartersville-com | ok, 390 (116/274), 1.1 | ok, 392 (116/276), 1.8 | |
| typesense | toyotaplace-com | ok, 291 (234/57), 2.0 | ok, 288 (233/55), 1.7 | |
| chapman | chapmanbmwchandler-com | ok, 1343 (752/587), 4.7 | inaccurate, 1362 (768/594), 2.1 | 68 `vpic_fuel`: dealer "Plug-In Hybrid" vs vPIC "Electric"/"PHEV" — a comparator gap for PHEVs (dictionary), not a scan problem; 4 hard rows locally |

**Result:** 19 of 20 dealers scanned with the same row counts as the laptop (within 5%,
new/used split intact). 16 ok, 3 inaccurate — all three from the assess/verification code
(two reproduce locally with today's code; Chapman's is the PHEV fuel comparator), not
from Railway. 1 lost to an IP block. Totals: 5,867 rows; VDP HTTP-first fetches 2,523, all
200. vPIC: 5,848 VINs requested, all but one already cached. Reconcile retired 0 rows.

Per-dealer minutes are not comparable one-to-one (different batch mates and host
pacing); the five carscommerce dealers ran into the VDP wall-clock cap (`cap_hit
host_exhausted`, 5.2-5.8 min) on Railway where they took 1.1-5.1 min locally.

### 403 / 429 findings

| host | Railway | laptop (same day) |
|---|---|---|
| `websites-search.api.carscommerce.inc` (Dealer Inspire API, 5 dealers) | **0** 403, 0 429 | 0 |
| Dealer Inspire / dealer.com / cosmos / team_velocity / typesense / chapman sites (VDP HTTP-first, 2,523 fetches) | 0 403, 0 429 (all 200) | 0 |
| `www.lambonb.com` (dealer_eprocess, Lamborghini Newport Beach) | **403 on the SRP recipe even after curl_cffi impersonation**; homepage 403; recipe marked stale (run 1) then re-synth rejected `auth_needed` (run 2) | first request 403, then "recipe replay cleared via TLS impersonation (chrome124)", 33 rows |
| `…/wp-json/v1/vehicles` (ShopperExpress probe, lambonb) | 403 | 403 (every run; harmless probe) |

No Cloudflare challenge markers in any line. Reading: the Cloudflare/TLS-fingerprint 403s
that curl_cffi clears are cleared from Railway just as from home, but at least one WAF
(eProcess' edge for LamboNB) blocks Railway's datacenter IP outright. On the laptop fleet
the impersonation-exhausted 403 class covered ~5 dealers (hyundaiofcookeville, capitaltoyota,
cartersubaruballard, ...); expect that class, plus some IP-reputation blocks, on the first
full Railway run. Fix path per dealer: `scan_hints.needs_http_proxy` + `SCANNER_HTTP_PROXY`
(a static/residential egress) — logged in `workspace/dealer_logs/lambonb-com/summary.md`.
lambonb's 30 rows stay active (a no_recipe verdict never retires), and its
`dealer_recipes` row now says stale; the next scan from a home IP re-synthesizes it.

### Measured resources

From the job's own cgroup sampler (`fleet_summary.json`) and `railway metrics`:

| | run 1 | run 2 |
|---|---:|---:|
| scan phase wall (8 shards, 2-3 dealers each) | 357 s | 350 s of scanning (+ a 5-min DB stall, below) |
| scanner batch wall per shard | 144-349 s | 131-350 s |
| post: compute_market_stats / build_listings_grid_cards | 1.3 s / 149.5 s | 1.2 s / 151.6 s |
| CPU, scan phase (cgroup) | 0.257 core-h, avg 2.65 cores | 0.243 core-h |
| CPU, whole job (cgroup / Railway metrics) | 0.287 / 0.142 core-h | 0.275 / 0.136 core-h |
| peak cores (cgroup / Railway) | 6.3 / 5.6 | 6.3 / 6.4 |
| peak memory (cgroup / Railway) | 2.15 GB / 2.14 GB | 2.13 GB / 2.11 GB |
| memory-hours (Railway) | 0.087 GB-h | 0.102 GB-h |
| Postgres service CPU added | 0.060 core-h, peak 2.6 | 0.060 core-h, peak 1.9 |
| network | ingress ~120 MB, egress ~6 MB per run | |

Per process: scanner.py 225-340 MB RSS at 2-3 dealers, pipeline 65 MB. The grid-card step
rebuilds every card (202,924 of 214,668) every night because `compute_market_stats` moves
the market-band generation that is part of the card key — expected, 150 s, ~0.03 core-h.

Run 2's shards sat idle for ~5 minutes at start: a debugging session of mine (a local
psql/python connection over the public proxy) was left "idle in transaction" on the
production database and the pipelines' startup statements queued behind it. Killed at
23:26 UTC; web health stayed 200 and web logs show no errors in the window. Lesson: never
leave a transaction open on production while a scan runs; `idle_in_transaction_session_timeout`
on the Postgres service would make this impossible.

## Cost

Railway: $20 per vCPU-month, $10 per GB-month, billed per second on use (not on the
limit): $0.0278 per core-hour, $0.0139 per GB-hour.

Extrapolation, from rows (scan work tracks rows: VDP prefetch and upsert are per car):
the test scanned 5,867 rows, the fleet is 214,668 active rows on 458 dealers (594 on the
scannable roster), a factor of ~37.

| item per nightly cycle | basis | estimate |
|---|---|---:|
| scanner CPU | 0.25 core-h x 37 (cgroup figure; Railway's own metric is ~half) | 9.2 core-h |
| margin: non-ok dealers, lifecycle re-synth, bigger lots | the test picked fast, healthy dealers; the laptop fleet averaged 4.5 min/dealer vs ~3 here | up to 2x -> 9-18 core-h |
| Postgres CPU (upserts, reconcile) | 0.06 core-h x 37 | 2.2 core-h |
| grid cards + market stats | measured | 0.03 core-h |
| scanner memory | 8 shards x (~0.5 GB scanner at batch 6 + 65 MB pipeline) ≈ 3-4.5 GB x wall | 5-8 GB-h |

**Per night ≈ 11-20 core-h and 5-8 GB-h ≈ $0.40 (low) to $0.70 (high); ≈ $12-21 per month.**
Volume (~1 GB of logs after 14-day pruning) is cents. Egress is ~0.

Shard count mostly changes wall time, not cost (the same work, the same CPU-hours; memory
scales with shards but for proportionally less time):

| shards | est. wall (594 dealers, batch 6) | peak scanner cores | peak Postgres cores | est. cost / night | / month |
|---:|---|---:|---:|---:|---:|
| 4 | ~3.5 h | ~4-7 | ~1.5 | $0.40-0.70 | $12-21 |
| **8** | **~1.7-2 h** | ~8-14 | ~3 | $0.40-0.70 | $12-21 |
| 16 | ~1-1.2 h (not linear) | ~16 (at the cap) | ~6 | $0.45-0.75 | $13-23 |

Wall-time basis: batches of 6 run for about the slowest dealer's time (~5-7 min, the VDP
wall-clock cap), 594 / (shards x 6) batches per shard, plus ~10-15 min of vPIC, assess and
lifecycle per shard; the laptop's 8-shard fleet took ~1.5 h for 458 dealers. 16 shards
stops being linear: shared platform hosts (carscommerce API, typesense cluster) see twice
the concurrent requests (429 risk) and production Postgres, which also serves the web,
takes ~6 cores of upsert load at the peak. The old idle `scanner-worker` / `scanner-scheduler`
services cost ~30 MB each (~$0.60/month together); they were not touched.

## Incident 2026-09-29: first full fleet run reassigned 6,961 VINs

The first `SCAN_FLEET=1` run on Railway reassigned **6,961 VINs (3.6%)** to the wrong dealer in
production. `cars` holds one row per VIN and the scanner upsert is `ON CONFLICT(vin) DO UPDATE`
(`backend/scanner/database.py` `upsert_vehicles`), so the last store to write a VIN owned it.

| from (owner) | to (claimant) | VINs |
|---|---|---|
| mbontario-com | mbbeverlyhills-com | 1,229 |
| mblaguna-com | mbfoothill-com | 1,011 |
| mtnviewnissan-com (CA) | cleveland-nissan-com (TN) | 876 |
| crownlexus-com | bmwofmonrovia-net | 541 |
| bentleygmc-com | bentleycadillac-com | 471 |
| chapmandodge-com | chapmanfordaz-com | 436 |
| hughwhitehonda-com | hughwhitehonda-net (same store, two entity ids) | 393 |
| lagunahyundai-com | lagunaniguelhyundai-com | 220 |

**Cause.** The fleet roster (`load_roster` -> `manifest._load_scannable_dealers`) was active
inventory ∪ stored recipes. That added 136 dealers holding a recipe but **no active cars** —
most had been emptied on purpose because their recipe replays a whole group feed — and the
rooftop gate let their scans claim siblings' cars. The laptop run that had been verified used
only dealers with active inventory. Separately, southcoasttoyota-com lost 609 rows to
`canceling statement due to lock timeout` during an upsert while 8 shards wrote at once.

**Fixes** (branch `feature/http-only-scans`):

1. **Roster rule** (below): fleet = active-inventory dealers only.
2. **VIN ownership guard** (below): an upsert no longer moves a fresh, active VIN to another dealer.
3. **Lock-timeout retry**: `inventory_write._is_retryable_db_error` retries SQLSTATE `55P03`
   (lock_not_available) and `57014` only when the message says lock timeout, like a deadlock.

**Repair the 6,961 rows before the next fleet run.** The guard protects whoever owns a VIN
*now*, so while the wrong owner's rows are fresh (scraped within 48 h) the right owner's
scan is refused too. Restore `dealer_id` (and `dealer_name` / `dealer_url`) on those VINs
from the pre-run state first — or, if restoring is not possible, let the real owners rescan
with `SCANNER_VIN_OWNER_GUARD_HOURS=0` **only for an explicit `SCAN_DEALERS` list of the
owners** and only after the claimants' recipes are fixed, since an active claimant
(mbbeverlyhills-com, cleveland-nissan-com, ...) would otherwise take the VINs back.

### Roster rule

A whole-roster scan — `fleet_scan` with `SCAN_FLEET=1`, or `scanner.py` with
`DEALERS_FROM_SCANNABLE=1` and no `--dealer-id` — takes:

- dealers with active inventory (`COALESCE(listing_active,1)=1` rows),
- minus `workspace/pipeline/do_not_scan.txt` ∪ `deploy/railway/do_not_scan.txt` (tracked twin:
  `workspace/` is not in the Railway image; both are read),
- plus recipe-only dealers listed in `deploy/railway/revived_dealers.txt` (tracked, starts
  empty). List a dealer there only after a single-store scan showed its recipe is scoped to
  its own rooftop, and note the evidence in `workspace/dealer_logs/<id>/summary.md`.

An explicit list is **never** filtered: `SCAN_DEALERS`, `dealer_pipeline --dealers` (which
passes `--dealer-id` to scanner.py), `scanner.py --dealer-id`. `fleet_scan` prints
`fleet   roster: N dealers kept; excluded X do-not-scan, Y recipe-only ...`, writes
`<run>/roster_rule.json` (the excluded ids) and a `roster_rule` block in `fleet_summary.json`.
`SCANNABLE_ROSTER_RULE=0` turns the rule off for `load_manifest` (DEALERS_FROM_SCANNABLE) only;
the fleet roster always applies it. Local read-only check on 09-29: 594 scannable -> 458 kept,
132 recipe-only excluded (mbfoothill-com, hughwhitehonda-net and lagunaniguelhyundai-com among
them), 4 do-not-scan.

### VIN ownership guard

`upsert_vehicles` skips a vehicle whose stored row is **active**, was **scraped within
`SCANNER_VIN_OWNER_GUARD_HOURS`** (default 48, `0` disables) and belongs to a **different
dealer_id**. The stored row is not touched (no price, no scraped_at, no post-write
enrichment), and the claimant's scan drops the VIN from coverage, auto-heal and reconcile.
The check runs on the prefetch the upsert already does, with an `ON CONFLICT ... DO UPDATE
... WHERE` backstop so a concurrent shard's claim between prefetch and write is refused
atomically too. A real transfer between stores still lands once the old owner's row goes
inactive or stale.

There is no same-store exception: two entity ids for one store (hughwhitehonda-com / -net)
are not detectable at write time. Every refusal is recorded instead:

- table `vin_owner_conflicts(vin, owner_dealer_id, claimant_dealer_id, seen_at)`, one row per
  triple, `seen_at` = latest sighting (created lazily and by `migrations/V024__vin_owner_conflicts.sql`).
  A table rather than only a log line because Railway logs are ephemeral and the recurring
  pairs across nights are the diagnostic:
  `SELECT owner_dealer_id, claimant_dealer_id, COUNT(*), MAX(seen_at) FROM vin_owner_conflicts GROUP BY 1,2 ORDER BY 3 DESC;`
- one `vin_owner_guard {"skipped":..,"claimants":{..},"owners":{..}}` WARNING line per write;
- `scan_runs.summary_json.vin_owner_conflicts` / `vin_owner_conflict_owners` per dealer run,
  `vin_owner_conflicts` in the `dealer_run_summary` line and in `fleet_summary.json` (total);
- the pipeline triage `reason` names it when a dealer's count is > 10
  (`VIN owner guard: 876 VINs owned by other dealers not written (mtnviewnissan-com 876)`).

## Guarded full run, 2026-09-29 (deployment 90a3d2a0, commit 89c13ad52)

First full run with the active-inventory roster and the VIN ownership guard.

| | Railway 09-29 | Laptop 09-28 |
|---|---|---|
| Dealers | 561 (29 no recipe) | 367 assessed |
| ok / inaccurate / thin / no_rows | 315 / 146 / 54 / 17 | 228 / 71 / 39 / 28 |
| Rows written | 262,499 | 168,109 |
| Wall clock | 159 min, 8 shards all rc=0 | ~90 min |
| Peak memory | 12.7 GB (cap 16) | n/a |
| CPU | 18.1 core-hours, avg 7 cores | n/a |
| 403 / 429 | 357 / 4 | n/a |

- 403s are concentrated: autocollectionofmurfreesboro.com 267, praterford.com 20,
  terrylabontechevy.com 11; everything else is 1-2 per host. Not a Railway-IP ban.
- The guard refused 12,903 cross-dealer moves (`vin_owner_conflicts`). Early rows were
  chapmanbmwchandler-com claiming other BMW stores' cars (a known claimant recipe).
  The per-claimant breakdown is still to be read: the laptop's network on 09-29
  blocked the Postgres and SSH protocols (TCP opens, no reply), so the DB and the
  /data triage could not be read that day.
- Cost of this run at Railway list prices: about $0.50 CPU + $0.25 memory, so about
  $0.75 per night, roughly $22 per month.
- Grid cards rebuilt in the post step (171 s, 268,868 active cars).
- Nightly cron still off until the conflict breakdown and a per-dealer comparison
  with the laptop run are done.

## Recommendation

**Enable the nightly, at 09:00 UTC (`0 9 * * *`: 02:00 PT / 05:00 ET, the traffic trough
for a US site), with 8 shards, batch 6 — but only after one supervised full run.** Cost is
$12-21 a month, the image and the pipeline work from Railway, rows match the laptop, and the
Dealer Inspire API and the Cloudflare-fronted sites answered 200 from Railway's IPs.

Why not flip the cron today:

1. The test covered 20 healthy dealers (3% of the roster). One of them (lambonb) is blocked by
   IP from Railway; the full roster will show how many more are. Run once with
   `SCAN_FLEET=1` (≈ $0.50, ~2 h), then compare its triage with the laptop's 09-28 fleet:
   dealers that are `ok` locally and `no_recipe`/`error` with 403 on Railway need an egress
   proxy (`needs_http_proxy`) before the nightly owns them — otherwise every night marks
   their recipes stale in production.
2. Confirm the full-run wall time (target < 3 h) and peak memory (< 8 GB) so the cron cannot
   overlap itself or the morning traffic.
3. Set `idle_in_transaction_session_timeout` (e.g. 5 min) on the production Postgres so a
   stray session cannot stall the nightly the way it stalled run 2.

Then: `cronSchedule: "0 9 * * *"` via the API call above, `SCAN_FLEET=1` on the service, and
watch the first night's `fleet   done:` line (exit 0 = green in Railway; a non-zero exit shows
the deployment as CRASHED). Discovery for `needs_discovery` dealers still needs a browser
host (`Dockerfile.discovery`): the Railway image skips captures by design.

## Files

- `Dockerfile.scanner`, `requirements-scanner.txt`, `scripts/scanner_import_sweep.py`
- `scripts/railway_scan_fleet.sh` (entrypoint), `backend/scripts/fleet_scan.py` (shards, post steps, summary)
- `deploy/railway/deploy_scanner_nightly.sh`, `deploy/railway/railway.scanner-nightly.json`
- `deploy/railway/do_not_scan.txt`, `deploy/railway/revived_dealers.txt` (roster rule),
  `backend/scanner/manifest.py` `apply_scannable_roster_rule`
- `backend/scanner/database.py` VIN ownership guard, `migrations/V024__vin_owner_conflicts.sql`
- Test runs: volume `/data/workspace/pipeline/fleet_20260928T230514Z`, `fleet_20260928T232028Z`;
  per-dealer blocks appended to `workspace/dealer_logs/<dealer_id>/{discovery,scan_runs}.md`.
