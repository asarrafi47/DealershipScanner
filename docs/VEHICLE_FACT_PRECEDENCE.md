# Vehicle fact precedence

Who may overwrite whom when a vehicle fact column is written. The rules live in
`backend/vehicle_facts/precedence.py` (pure: no database, no network) and are
applied by `backend/vehicle_facts/apply.py` (`apply_fact_proposals`,
`apply_labeled_patch`). Writers gather `(field, value, source)` proposals and
persist what `decide` / `plan_updates` returns; they never re-implement a rank.
`backend/tests/test_vehicle_facts_precedence.py` checks the table below against
`RANKS`, `FILL_ONLY` and `SOURCES`, so a code change that skips this file fails.

## Owner rules

- Ranking is per (field, source), never one global rank.
- NHTSA vPIC wins on driven wheels, electrification / diesel and cylinders. vPIC
  engine text, transmission and body are fill-only and lose to dealer text.
- The EPA catalog never outranks the dealer's own engine text or the VIN decode.
- A less specific value (2WD, 4x2, Other, blank) never replaces a stored specific end.
- LLM output never writes a fact column.

## Rank table

Higher wins. `-` means the source may never write that field. An `f` suffix means
fill-only: the source writes only into a blank or junk column, whatever its rank.

| source | drivetrain | fuel_type | cylinders | engine_description | engine_l | transmission | body_type | exterior_color | interior_color | forced_induction |
|---|---|---|---|---|---|---|---|---|---|---|
| `nhtsa_vpic` | 100 | 100 | 100 | 20f | 20f | 20f | 20f | - | - | 70 |
| `oem_window_sticker` | 80 | 80 | 80 | 90 | 90 | 90 | 90f | 90 | 90 | 90 |
| `oem_window_sticker_unverified` | 5f | 5f | 5f | 5f | 5f | 5f | 5f | 5f | 5f | 5f |
| `dealer_feed` | 60 | 60 | 60 | 80 | 80 | 80 | 80f | 80 | 80 | 80 |
| `dealer_vdp` | 60 | 60 | 60 | 80 | 80 | 80 | 80f | 80 | 80 | 80 |
| `dealer_normalized` | 55 | 55 | 55 | 70 | 70 | 70 | 70f | 70 | 70 | - |
| `dealer_text_heuristic` | 40 | 40 | 40 | 50 | 50 | 50 | - | - | - | 50 |
| `html_jsonld` | 55 | 55 | 55 | 70 | 70 | 70 | 70f | 70 | 70 | - |
| `recipe_heal` | 45 | 45 | 45 | 60 | 60 | 60 | 60f | 60 | 60 | - |
| `epa_catalog` | 30 | 30 | 30 | 15f | 15f | 15f | 15f | - | - | 60 |
| `epa_catalog_tiebreak` | 35 | - | - | - | - | - | - | - | - | - |
| `web_search` | 5 | 5 | 5 | 5f | 5f | 5f | - | - | - | - |
| `model_specs` | 10 | 10 | 10 | 10f | 10f | 10f | 10f | - | - | - |
| `trim_heuristic` | 20 | 5f | 5f | 5f | 5f | 5f | - | - | - | 10f |
| `llm` | - | - | - | - | - | - | - | - | - | - |
| `unknown` | 1 | 1 | 1 | 1 | 1 | 1 | 1f | 1 | 1 | 1 |

Legacy provenance labels map to a source: `nhtsa_vpic_heal` and `ev_cylinders_heal`
to `nhtsa_vpic`; `epa_master` and `epa_dictionary` to `epa_catalog`;
`inventory_repair` and `listing_gap_fill` to `unknown` (mixed-source tags).
`body_type` is fill-only for every source. `forced_induction` follows
`forced_induction.resolve_car_forced_induction` (P10C.2): dealer engine text and the
sticker, then the VIN decode (vPIC Turbo, typed by the catalog family), then
family-unanimous catalog flags, then description markers; the make/model heuristic
(`trim_heuristic`) only fills a blank column (D-ET4).

## Decision order (`decide`)

1. Unknown field, or source `llm`: refuse. A source absent from the field's table: refuse.
2. Blank candidate: refuse, nothing recorded.
3. The per-field guard runs (drivetrain, fuel_type, cylinders; below). It may decide alone.
4. Blank stored value: the candidate fills it ("fills blank").
5. Same value: refuse ("unchanged"). For drivetrain, AWD and 4WD compare equal.
6. Catalog engine correction (D-PR3): `epa_catalog` may replace `engine_description`,
   `engine_l` or a cylinder count only with `ctx["corroborated"]` (vPIC or dealer
   title agrees); a stored vPIC or sticker cylinder count is never replaced.
7. Fill-only source on a set column: refuse.
8. Otherwise the candidate wins only when its rank is strictly higher than the rank
   of the stored value's source ("`X` outranks `Y`").

## Per-field guards

**drivetrain.** vPIC "4x2" is a constraint {FWD, RWD, 2WD}, never RWD and never AWD
(D-PR1). With vPIC as candidate: a specific end replaces the stored value; 4x2 keeps a
stored FWD/RWD, turns a stored AWD/4WD into `2WD` flagged `cannot_assess`, and fills a
blank with `2WD`. For other sources: a less specific candidate never replaces a stored
end; a candidate that contradicts the vPIC drive is refused (and flagged
`cannot_assess` when two or more signals, the stored value and the candidate, disagree
with vPIC, D-PR6); vPIC 4x2 forbids an all-wheel candidate. `epa_catalog_tiebreak`
(D-PR4) is skipped when vPIC decided, is vetoed by the trim decoder (4MATIC, xDrive,
AWD badge), fills blank, and replaces a dealer FWD/RWD only when the trim decoder
agrees with the catalog.

**fuel_type.** vPIC wins only when decisive: BEV, PHEV, FCEV, HEV or diesel. Mild
hybrid and silent decodes change nothing. Plain "Gasoline" from vPIC fills a blank,
non-fuel or abbreviation ("F", "G"), never a stated hybrid, diesel or electric label
(N15.5). From other sources a Hybrid label never downgrades PHEV or BEV, and a label
that contradicts the vPIC electrification decode is refused.

**cylinders.** Non-numeric is refused. A count that conflicts with the engine text or
an EV fuel (`cylinders_conflicts_with_engine_text`) is refused, so vPIC cylinders count
only when they do not contradict the dealer's engine text (D-PR7).

## Stale provenance

Provenance lives in `spec_source_json[<field>]` as
`{source, value, fetched_at[, discrepancy, cannot_assess]}`. A stored entry's rank
counts only while its `value` still normalizes to the column value. With no entry, a
bare string label, an entry without `value`, or a value that no longer matches the
column, the stored source is `unknown` (rank 1) and any ranked writer may replace it.
Invariant `provenance_stale_drivetrain` counts such rows.

## Discrepancy sink

A refused candidate that is non-blank and differs from the stored value is not lost:
`Decision.discrepancy` is `{source, value, seen_at}` and `provenance_patch` merges it
into `spec_source_json[<field>]["discrepancy"]` beside the kept entry, with
`cannot_assess: true` when several signals contradicted vPIC (D-PR6). Refusals that
are "unchanged", blank, junk, or "not decisive" record nothing.
`plan_updates` is ordered (later proposals see earlier winners) and idempotent:
running it on its own output proposes nothing.

## Invariants

Read-only checks in `backend/scripts/data_quality_invariants.py`:
`vpic_electrification_fuel_conflict`, `vpic_drive_wheels_conflict`,
`vpic_cylinders_conflict`, `provenance_stale_drivetrain`, `sticker_vs_vpic_conflict`,
`engine_text_vs_vpic_displacement`, `catalog_tiebreak_over_explicit_end` (informational).
