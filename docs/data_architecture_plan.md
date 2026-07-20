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
| 8 | **Scan recipes + per-dealer scan hints** — `dealer_recipes` (HTTP replay shortcuts, stale/health state, `scan_hints` instructions) | dealer_id | **built 2026-07-19**; `workspace/recipes/` files stay the local hot cache, `backend/scanner/recipe_store.py` syncs both ways so recipes follow the dealer to any machine |

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
