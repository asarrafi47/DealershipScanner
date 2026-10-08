# Reference-Store Data Architecture

*Adopted 2026-07-19, following the BA303069 cylinders incident (see
`workspace/` memory: repair-script trim decoding wrote wrong model-level specs
into listing rows at a 7% error rate).*

## Principle

One store per **kind of fact**, keyed by **what the fact is true of** —
and model-level facts are **joined at read time, never written into `cars`**.
Listing rows store only what was observed at the dealer (price, miles, color,
VIN, dealer's own engine text). Everything model-level (hp, tq, cylinders,
forced induction, MPG, safety ratings, known issues) lives in reference stores
and reaches the UI through a single resolved link.

Precedence when sources disagree: **vPIC (VIN-decoded) > dealer-observed text >
EPA by exact year > heuristics**, and heuristic values must never contradict
dealer-observed engine text (`backend/utils/engine_consistency.py`).

## The seven stores

| # | Store | Key | Status |
|---|-------|-----|--------|
| 1 | **Vehicle catalog** — `epa_master` (+ `epa_extended_specs` hp/tq/0-60) | (year, make, model, trim) → engine variant row | exists; formalized as THE catalog by this plan |
| 2 | **Model generations** — `model_generations` | (make, model) → generation code + year range | **built in phase 1** |
| 3 | Ownership intelligence (known issues, maintenance) | generation ↔ engine | future — assistant grounding |
| 4 | Price book (price events + market aggregates) | VIN → events; (yr/make/model/trim × region) → aggregates | future — normalize `price_provenance_json` |
| 5 | Safety & recalls cache | (yr/make/model) ratings; VIN campaigns | future — cache the live NHTSA calls |
| 6 | Options/packages catalog | trim → option codes / MSRP | future — hardest to keep clean, last |
| 7 | Dealer registry + quality metrics | dealership | exists (`dealership_registry`, reviews) |
| 8 | **Scan recipes + per-dealer scan hints** — `dealer_recipes` (HTTP replay shortcuts, stale/health state, `scan_hints` instructions) | dealer_id | **built 2026-07-19**; `workspace/recipes/` files stay the local hot cache, and `load_recipes` / `save_recipes` (`backend/scanner/recipes.py`) keep each dealer's set in step with its row by a last-write stamp, so recipes follow the dealer to any machine (see "Recipe cache and store sync" below) |

### Per-dealer scan hints (`dealer_recipes.scan_hints`)

The containment mechanism for platform quirks: **platform handlers keep their
proven default behavior** for every dealer that works; a dealer that needs
different navigation carries its own JSON override
(`get_scan_hints`/`set_scan_hints` in `backend/scanner/recipe_store.py`), so
fixing a Tennessee dealer can never regress a California one. Motivating case:
`dealer_on_cosmos` serves 4 existing CA dealers + 14 new TN dealers correctly
but 11 TN dealers ship priceless SRP feeds — those 11 carry
`price_source: "vdp"` instead of anyone touching the shared cosmos handler.

Recognized keys (see `SCAN_HINT_KEYS`): `needs_http_proxy`, `price_source`
("list"|"vdp"|"second_endpoint"), `requires_browser`, `skip_reason`, `notes`,
`hint_source`. Scanner integration points (for the scan-side session):
consult hints at dealer-run start — `needs_http_proxy` → set proxy before
synth; `price_source=vdp` → route the price-heal/VDP path after capture;
`requires_browser` → skip HTTP fingerprinting; `skip_reason` → skip dealer.

All stores are **tables in the one Postgres**, not separate databases — the
value is in the joins.

### Recipe cache and store sync (`dealer_recipes` and `workspace/recipes/`)

*Documented 2026-10-08 (P1C.6). "Phase 0" here is the recipe-store fix in
docs/REMEDIATION_PLAN_2026_10.md Phase 1B (units P1B.2, P1B.3 and P1B.6), not a
phase of the catalog plan below.*

Each dealer's recipe set lives twice: as its `dealer_recipes` row (the store)
and as a file in each scanning host's cache. `load_recipes` and `save_recipes`
in `backend/scanner/recipes.py` keep the two in step.
`backend/scanner/recipe_store.py` reads and writes rows exactly as given and
never stamps them.

- **Cache dir.** `<repo>/workspace/recipes/<dealer_id>.json`, anchored at the
  repo root, never the process cwd (`/app/workspace/recipes` on Railway, where
  `/app/workspace` is a symlink onto the volume). `RECIPES_CACHE_DIR`
  overrides it for the whole process. It is read once, at import, and a
  relative value is pinned against the cwd at that moment. VDP recipes follow
  the override as `<dir>/vdp`. Every cache write is atomic: a temp file in the
  same dir, fsync, then `os.replace`.
- **`saved_at` is the last-write stamp.** `save_recipes` stamps every row of
  the set with one `time.time()` per call, before it writes the file and then
  the row, so the file and the row carry the same stamp and the row's
  `max_saved_at` is that stamp. Every write gets a stamp: a stale flag, a
  replay's last_ok / coverage / un-stale update, promote, `ensure_recipe`,
  cascade and synthesize. A copy's freshness is its max `saved_at`. The stamp
  stays a per-write stamp in later phases, because hosts that run older code
  still compare it.
- **The sync rule** (`load_recipes`) compares the file's max `saved_at` with
  the row's `max_saved_at`:
  - the row is newer: another host wrote the set since. The row wins and is
    written into the cache as it is;
  - the file is newer: a write-through failed, or the file predates the store.
    The file is pushed up into the store as it is (the push-up, below);
  - the stamps are equal: the file is returned and neither copy is written.

  Neither direction re-stamps, so every copy keeps the stamp of the write that
  made it.
- **`updated_at` is not a freshness stamp.** Every `set_scan_hints` call bumps
  it, and nearly every row carries hints (683 of 687 local rows on
  2026-10-08). Never compare it with `saved_at`.
- **Before Phase 0, "newer" meant "created later".** In VERSION 1.5.2
  and older (which includes the scanner-nightly image on Railway), `saved_at`
  was set only when a set was created: by promote, and by the
  `synthesize_recipes` CLI. Stale flags, un-stales and coverage updates never
  moved it, and sets synthesized by the pipeline (`ensure_recipe` in
  `dealer_pipeline.py`) or cascaded were saved with `saved_at=0`. The same
  comparison therefore ranked creation times, not writes. A stale flag or an
  un-stale written on one host left the stamps equal, so every other host kept
  its own file. Any older cache file with a nonzero stamp outranked a set saved
  with `saved_at=0` and was pushed back up over it.
- **Rows with `max_saved_at=0`.** Sets saved that way still carry
  `max_saved_at=0`: 201 local and 235 prod rows in the 2026-10-08 census
  (docs/db_layer/SCHEMA_LEDGER_2026_10.md). Under Phase 0 code too, such a row
  loses to any file with a nonzero stamp, however old, in a cache that mirrors
  its store, until the row is backfilled from its `last_ok_at` (remediation
  plan P1C.3 locally, P6B.2 on prod). Run that backfill against a store only
  once every host that writes the store runs Phase 0 code.
- **The push-up warning.** A push-up logs one WARNING:
  `Recipes [<dealer>]: local cache is newer than dealer_recipes; pushed N file
  recipe(s) (max saved_at S) up and overwrote the DB copy of M recipe(s) (max
  saved_at D)`, or `... up and no DB copy was read`. Under Phase 0 code it
  should be rare. The expected causes are an earlier failed write-through
  (that failure logs its own WARNING, once per error class per process), a
  file that predates the store, and a `max_saved_at=0` row (not yet
  backfilled, or written since by a host that still runs pre-Phase 0 code).
  Any other push-up means the cache is out of step with the store: check its
  store tag (next item) before anything else runs on that cache.
- **Store fingerprint (`_store.json`).** A cache mirrors one store.
  `<cache dir>/_store.json` holds the sha256 of that store's host, port and
  database name, taken from `INVENTORY_DATABASE_URL` / `DATABASE_URL` (a part
  the URL leaves out comes from `PGHOST` / `PGPORT` / `PGDATABASE`), never the
  user or password. An untagged cache is tagged for the process's store on the
  first load that reaches the store. A process whose store has another
  fingerprint warns once and treats the cache as read-only: no push-ups, no
  writes into the cache, the store's own copy is served, saves go to the store
  only, and a failed store read holds that dealer's saves.
  `python -m backend.scanner.recipes --status` prints the tag and the verdict
  (`--require-match` gates a script on `match`), and `--reseed` re-tags a
  cache for a new store. Cross-store work, such as the home-IP prod repair,
  runs on `RECIPES_CACHE_DIR=<empty scratch dir>`. Bulk imports
  (`backend/scripts/import_recipes_to_db.py`, a wrapper around
  `reconcile_recipe_store.py --cache-dir` since P1C.2) merge per recipe and
  refuse to apply unless the verdict is `match` or `untagged`. The full
  rules are in docs/RAILWAY_SCANNING.md, "The recipe cache is tagged with its
  store".

## Phase 1 (this change)

1. **Persistent catalog link**: `cars.epa_master_id` / `epa_match_confidence`
   / `epa_match_method`, written by ONE resolver
   (`backend/catalog/resolver.py`) that scores candidates by trim match plus
   engine agreement (engine_l, cylinders, drivetrain, fuel) instead of the old
   five independent `LIMIT 1` fuzzy lookups. Low-confidence cars stay
   unlinked (visible as a queue) rather than mislinked.
2. **`model_generations`** table + curated seed for the volume model lines
   (rows flagged `estimated` until human-verified). Gives every store and the
   assistant a generation key ("W212", 2010–2016) — the year-collision antidote.
3. **Read-time join**: `merge_verified_specs` uses the resolved
   `epa_master_id` for the per-trim EPA row and hp/tq extended specs when the
   link exists, falling back to the old fuzzy path when it doesn't. VDP output
   gains `generation_code` / `generation_years`.
4. **Backfill**: `backend/scripts/link_cars_to_catalog.py` links the active
   fleet and reports match-rate + confidence distribution.

## Phase 2+ (queued)

- Price book: `listing_price_events` table fed by the upsert (replacing
  per-row `price_provenance_json` growth), nightly aggregates per
  (year/make/model/trim × region) powering deal chips and payment estimates.
- Recalls cache: `nhtsa_recalls_cache` (model-level, 7-day TTL) + VIN campaign
  check on VDP ("open recall" banner).
- Ownership intelligence: `generation_notes` (AI-researched, status-flagged)
  keyed to `model_generations`, surfaced by the listing assistant.
- Retire the remaining write-into-cars enrichment paths once the read-time
  join covers their fields.
