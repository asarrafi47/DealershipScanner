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
| 8 | **Scan recipes + per-dealer scan hints** — `dealer_recipes` (HTTP replay shortcuts, stale/health state, `scan_hints` instructions) | dealer_id | **built 2026-07-19**; `workspace/recipes/` files stay the local hot cache, and `load_recipes` / `save_recipes` (`backend/scanner/recipes.py`) keep the cache in step with the row, which is the authority since N13 (P7), so recipes follow the dealer to any machine (see "Recipe cache and store sync" below) |

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
`skip_reason` → skip dealer. `requires_browser` is informational only (no code routes on it).

All stores are **tables in the one Postgres**, not separate databases — the
value is in the joins.

### Recipe cache and store sync (`dealer_recipes` and `workspace/recipes/`)

*Documented 2026-10-08 (P1C.6). "Phase 0" here is the recipe-store fix in
docs/REMEDIATION_PLAN_2026_10.md Phase 1B (units P1B.2, P1B.3 and P1B.6), not a
phase of the catalog plan below.*

Each dealer's recipe set lives twice: as its `dealer_recipes` row (the store, the
authority since N13 Phase 7) and as a file in each scanning host's cache. `load_recipes`
and the write API in `backend/scanner/recipes.py` keep the file a mirror of the row.
`backend/scanner/recipe_store.py` reads and writes rows exactly as given and never
stamps them. The sections below are written in the order the code changed: the cache
mechanics and the Phase 0 stamp history first, then the authority model that replaced the
Phase 0 sync rule, then the rollback runbook.

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
- **The Phase 0 sync rule (replaced by the authority model below; kept for hosts still
  running that code)** (`load_recipes`) compared the file's max `saved_at` with
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
- **The push-up warning (Phase 0 code only; the authority model has no push-up).** A push-up logged one WARNING:
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

#### Authority model (N13 Phase 7, P7.1 to P7.8)

The `dealer_recipes` row is the authority for a dealer's recipes; the cache file is a
mirror. Migration V034 (the plan said V031; V034 was the next free number) adds three
columns; the rest is code in `recipe_store.py`, `recipe_cache.py` and `recipes.py`.

- **`rev` and compare-and-set.** Every write (recipes or `scan_hints`) goes through
  `db_mutate` / `mutate_scan_hints`: read the row, compute the change, then
  `UPDATE ... SET ..., rev = rev + 1 WHERE dealer_id = ? AND rev = <rev read>`. A write that
  changed no row lost a race; it re-reads and recomputes (bounded retries, counted by
  `store_stats()`). Two hosts can no longer overwrite each other's recipes silently, and a
  first-insert race has exactly one winner.
- **`recipes_written_at`.** Stamped by recipe writes only (a hint write bumps `rev` and
  leaves it). 0 means the row holds no recipes of its own: a hints-only row holds `[]` and
  is not authoritative, so it no longer hides an alias (renamed-slug) dealer's recipes.
  Migration V034 stamped existing rows that held recipes from their own `max_saved_at`.
- **Cache semantics.** `load_recipes` returns the row's recipes when the row is
  authoritative and rewrites the cache file only if its content differs (atomic write, per-file
  `flock` on `<slug>.json.lock`). A newer file never overwrites the row. With no
  authoritative row, a cache file that has recipes seeds the store (only while the row still
  holds none) and is returned. With the store unreachable the file is served as before.
  Until `<cache>/_reconciled` names the current store (written by
  `reconcile_recipe_store`), a differing cache file is copied to
  `workspace/backups/recipe_cache_differing` before the row's copy replaces it. A cache
  tagged for another store stays read-only, as described above.
- **Key-scoped mutations.** `mark_stale` (with `observed_at`, taken before the request, so
  another host's later success outranks the failure), `record_replay_ok`, `merge_recipes`
  (promotion's merge/rank/cap rules, applied to the row as it is when the write lands) and
  `replace_recipes` each touch one recipe key inside the compare-and-set, and stamp that
  recipe's `mutated_at` and the per-write `saved_at`. No code loads a set and saves it back.
- **Offline op-log.** An op the store cannot take is appended to
  `workspace/recipes/_pending/<store fingerprint>/<slug>` (one JSON object per line) and
  applied to the cache file; the next load that reaches the same store replays the ops, not a
  snapshot, so another host's newer edits survive. The fingerprint directory keeps an entry
  from being replayed against a different store.
- **Archive.** `replace_recipes(reason)` (`synth:force`, `synth:cc_section_scoped`,
  `synth:cosmos_single_section`) moves each recipe it replaces into `superseded_json` as
  `{key, recipe, superseded_at, reason}` in the same compare-and-set. Entries are pruned
  after 30 days. `python -m backend.scripts.restore_superseded_recipes <dealer_id>` lists
  them (dry run) and `--index N --apply` restores one. The archive is never in `recipes_json`;
  consumers read `recipes_json` only. `cascade_recipes` and `synthesize_recipes` merge through
  `merge_recipes` instead of overwriting.
- **Per-host blocked status (D-RS4).** A tagged host (`SCANNER_EGRESS_TAG`, e.g. Railway)
  writes `scan_hints.recipe_blocked[<tag>]` and leaves `recipe_status` alone;
  `clear_stale_status` clears only its own tag's entry; `route_verdict` reads
  `recipe_blocked[egress_tag()]` and still honours a legacy `blocked:<tag>:` status;
  `lifecycle_last_attempt@<tag>` keys the daily attempt per host class. A Railway 403 never
  stales the home lane's recipe.
- **Reconcile tool.** `backend/scripts/reconcile_recipe_store.py` settles drift per recipe
  rather than per set: `--cache-dir` (file to DB), `--source-dsn S --target-dsn T` (DB to
  DB), `--compare-cache DIR --target-dsn T` (read-only sample of 10 dealers), dry run by
  default, `--apply` writes through `db_mutate` after backups, `--restore RUN_DIR` undoes an
  apply. It writes `_reconciled` for the store once a cache has been reconciled.

Still true from Phase 0: `saved_at` stays a per-write stamp and `max_saved_at` its max
(hosts on older code compare them; the backcompat test
`backend/tests/test_recipe_store_backcompat.py` pins this), the `recipes_json` row schema only
gains the optional `mutated_at` key, and `updated_at` is never a freshness stamp.

#### Rollback (recipe store authority)

Use this if the N13 code must come out after a release. V034 is additive and stays.

1. **Stop the fleets.** Pause every writer of the store (home lane, Railway scanner-nightly,
   the mini) so no host writes mid-rollback. No scan or pipeline may run until step 4.
2. **Revert to the previous tag** on each host (`deploy/railway/deploy_scanner_nightly.sh`
   from that tag for Railway). The older code reads `recipes_json` and `max_saved_at` only,
   which this phase keeps current on every write, and ignores `rev`, `recipes_written_at`,
   `superseded_json` and the extra `mutated_at` key. Its writes do not bump `rev`; that is
   harmless, because the next N13 write reads the row fresh.
3. **Keep V034.** Do not drop the columns: they are additive, defaulted, and rolling forward
   again needs them. Archived entries stay in `superseded_json`, invisible to older code.
4. **Replay or archive every `_pending` dir before deleting it.** Each
   `workspace/recipes/_pending/<store fingerprint>/<slug>` holds ops the store never took. With
   the N13 code still importable (a scratch checkout of the N13 tag), run one `load_recipes`
   per dealer with the pending file, against the store whose fingerprint names the directory,
   to replay it; otherwise move the directory to `workspace/backups/` with its date. Never
   delete one unread, and never replay it against a different store.
5. **Check, then resume.** Run `python -m backend.scanner.recipes --status` on each cache
   (expect `match`), compare the volume cache with prod via
   `reconcile_recipe_store --compare-cache <dir> --target-dsn <prod>` (read only), and restart
   one host before the rest.

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
