# Retired nightly and k8s steps: capability table (N14.1, D-IF6 desk review)

Replaces P0B.5's scratch restore (D-N28). Read from code and git history only: no scan, no DB write,
no restore. The numbers come from the 2026-08-03 laptop log as quoted in
`docs/INCOMPLETE_LISTINGS_FINDINGS_2026_10.md` section 2.15 (`workspace/scanlogs/nightly_20260803.log`,
not present in a fresh clone), so they are history, not a fresh measurement. A step marked
**needs restore** is one this review cannot settle.

## Where the nightly stands today

- Laptop job: `deploy/nightly_http_refresh.sh` (7 steps, 03:30). Its plist is
  `~/Library/LaunchAgents/com.sarraficars.nightly-http-refresh.plist.disabled`; the last completed
  run was 2026-08-03.
- Replacement: `backend/scripts/fleet_scan.py` (Railway scanner-nightly, cron off until the owner
  turns it on). Shards run `dealer_pipeline` (scan, vPIC heal, assess, reconcile, lifecycle,
  per-dealer logs), then a post block: index rebuild (N6.1), `compute_market_stats`,
  `build_listings_grid_cards` with `FLASK_ENV=production`.
- `dealer_pipeline` runs `scanner.py --scan-only`, which switches off post-repair, window sticker,
  gap fill, dictionary enrich, enrich and vPIC inside the scanner process
  (`backend/scanner/scan_efficiency.py`, `cli.py`); the pipeline then runs its own vPIC heal. So no
  scheduled job runs the three recovery harvesters or `post_scan.py` any more (finding 2.15).

## Laptop nightly, 7 steps

| # | Step (command) | What it did | What consumes its output today | D-IF6 recommendation | Evidence |
|---|---|---|---|---|---|
| 1 | HTTP delta refresh (`scanner.py --delta`, `DEALERS_FROM_SCANNABLE=1`, 900 s per dealer, concurrency 8) | Replayed stored recipes over HTTP, upserted prices and inventory, marked sold cars inactive | Same data now written by the `fleet_scan` shards; reconcile and lifecycle sit in `dealer_pipeline` | **Replaced** by fleet shards. The `--delta` code path stays (`backend/scanner/delta_scan.py`) | Script `:150-156`; `fleet_scan.py` module docstring; on 08-04 the delta step took 25.2 h and blocked the next two starts (finding 2.15), the reason not to port it as is |
| 2 | `harvest_carscommerce.py` | Replayed CarsCommerce recipes at full page size; filled empty columns by VIN, never price | Nothing scheduled. Rows it filled are now written by the scan itself for CarsCommerce dealers | **Drop from the schedule.** Keep the script for an on-demand run | Patched 24 rows on 08-03 (log `:1496`); docstring "fills ONLY empty columns" |
| 3 | `heal_from_recipes.py` | Replayed every non-stale recipe, routed payloads by shape, filled NULL columns by VIN | Nothing scheduled | **Port as an optional recover step** (P15A.4): only dealers with price or trim gaps, with a yield gate. Not a default step | Replayed 75,101 VINs at 209 dealers, patched 51 rows in 15,009 s (log `:3659-3660`): not worth a fleet replay; 11,529 of 11,592 indexed cars still miss a field it could fill (finding 2.15), so the capability is real but must be targeted |
| 4 | `harvest_html_jsonld.py` | Fetched each incomplete car's VDP over HTTP, filled from JSON-LD, images included | Nothing scheduled. `synthesize_recipes.py` still names it as the strategy for server-rendered dealers | **Home lane only.** Offer it as an optional post step on the home lane (P15A.4); on Railway the target hosts are Cloudflare-walled | 22 of 22 candidates blocked on 08-03 (log `:3670`); honors `SCANNER_HTTP_PROXY` |
| 5 | `rebuild_listings_index.py` | Recomputed `incomplete_listings` | The web and the grid cards read the index | **Replaced by N6.1**: `rebuild_index_chunked(force=True)` once per fleet, after every vPIC heal and reconcile, in the `fleet_scan` post block | `fleet_scan.py` post block (`"index"` step); finding X2 (rules-version staleness) |
| 6 | `compute_market_stats` | Recomputed per-model price bands | Car page market read and deal badges | **Kept**, already in `fleet_scan` | Market table sat at 584 rows from 07-18 to 09-28 until this step was added |
| 7 | `build_listings_grid_cards` | Refreshed stored listings cards | `/api/listings/cars` | **Kept**, already in `fleet_scan`, now with `FLASK_ENV=production` (the laptop script did not set it, so its cards used a different card key) | `fleet_scan.py` `"cards"` step comment |

The wrapper also took a browser-process count around step 1 and a before/after `incomplete_listings`
count. The fleet summary carries the equivalent per-step rc and seconds under `post`; a browser guard
is `browser_gate` (`SCANNER_ALLOW_BROWSER` unset), so nothing needs porting.

## k8s CronJobs (`deploy/k8s/`, `deploy/car-scanner/`)

| Job | Schedule | Command | D-IF6 recommendation | Evidence |
|---|---|---|---|---|
| `dealership-scanner` | `0 */4 * * *` | `python scanner.py --scan-only` | **Drop.** Replaced by Railway fleet scan and the home lane | `cronjob-scanner.yaml:8,30`; SQLite-on-PVC single-pod design; no cluster in use (CLOUD_RESTRUCTURE_PLAN D1) |
| `dealership-scanner-sharded` | `0 */4 * * *` | `python scanner.py --scan-only` (sharded) | **Drop.** Same | `cronjob-scanner-sharded.yaml:11,33` |
| `dealership-post-scan` | `30 */4 * * *` | `python post_scan.py --hours 5` | **Port in part.** The gap-fill and window-sticker half becomes the recover phase (P15A.4, aimed at open index codes). The repair, vision and enrich flags are not ported | `cronjob-post-scan.yaml:8,27`; `post_scan/job.py` options; `scan_efficiency.py` shows scan-only drops these |
| `dealership-enrichment` | `0 2 * * *` | `repair_inventory_fields` then `enrich_from_dictionary` | **Drop the schedule.** Write-time backfill (`sync_incomplete_and_backfill_specs`) and the pipeline's vPIC heal cover it, and vPIC outranks catalog values. `repair_inventory_fields` wrote about 1,200 wrong cylinders once (memory: cylinders data integrity), so a nightly run is a risk. **Needs restore** only if the owner wants a measured yield before dropping `enrich_from_dictionary` | `cronjob-enrichment.yaml:8,32-33`; `upsert/post_write.py` |
| `dealership-discovery` | `0 1 * * 0` | `run_nationwide_discovery.py --manifest ... --verbose` | **Drop as a cron.** Discovery runs through the dealer-discovery workflow and the owner (Places API (New) is also disabled in the GCP project) | `cronjob-discovery.yaml:8,36`; D-IF3 (a) hands onboarding to dealer-discovery |
| `car-scanner` (`deploy/car-scanner/`) | `0 7 * * *` | image default `python scanner.py` | **Drop.** Same as the scanner jobs; image tag points at a retired LAN registry | `cronjob.yaml:43,71-74` |

## Reporting jobs (not post steps, listed so the retirement of the nightly loses nothing)

| Job | Schedule | Recommendation |
|---|---|---|
| `nightly_data_quality_invariants.sh` (plist loaded) | 06:00 | **Keep.** Read-only grading of what the scan wrote; P18A.6 makes it take `REPORTS_DATABASE_URL`. |
| `nightly_rooftop_refusals.sh` (plist in repo, not loaded) | 06:20 | **Keep**, same. Reads the refusal ledger; the 9,610 guard refusals in the clean-room finding were unread. |

## Answer to D-IF6

Per step, as recorded in the table: replace steps 1 and 5 (fleet shards, N6.1), keep 6 and 7 (already
ported), drop 2 from the schedule, port 3 and 4 only as optional configurable post steps
(`SCAN_POST_STEPS`, P15A.4; 4 on the home lane), port the gap-fill half of the k8s post-scan into the
recover phase, and drop the other k8s jobs. Only `enrich_from_dictionary`'s yield is unmeasured; it is
the one candidate for a scratch restore, and only if the owner objects to dropping it unmeasured.

## Inputs for later units

- P15A.4: step names `harvest_carscommerce` (on demand only), `heal_from_recipes` (gated),
  `harvest_html_jsonld` (home lane), plus `market` and `cards` (defaults). Cards stay last.
- P18A.5 writes `docs/NIGHTLY_OPERATIONS.md` from this table; P18A.7 deletes the k8s manifests
  after this table's schedules and commands (copied above) are the only record.
