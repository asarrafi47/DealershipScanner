# Accuracy and function gates for the scan loop

This document specifies the checks that run after every dealership scan: whether each dealer's inventory was captured completely and correctly, what a failing dealer needs next, which production writes a run may perform, and which fix is worth doing first. It is written for the engineer who implements `backend/scanner/loop/gates.py` and `backend/scanner/loop/queue.py` (with the wrapper `backend/scripts/ops_gates.py` and the tests `backend/tests/test_loop_gates.py` and `backend/tests/test_loop_queue.py`) and should be enough to work from on its own. Analysis sessions and reviewers who read `gate_report.json` and `queue.json` should use it to learn exactly what each gate means. It is one document of the scan-loop specification in `docs/scan_loop/`: `README.md` gives the rules and the data flow, `BUNDLE.md` the run bundle and the request files, `TASKS.md` the work cards. The numbers come from the 2026-11-03 fleet scan (458 dealers in eight shards, release 1.5.8, a scratch database, a residential connection) and from the code of release 1.5.17. The distributions in section 2, the dealer sets named in section 12 and the backtest were recomputed from that scan's triage files and scanner logs while this document was written and agree with the analysis that preceded it (called "the investigation" below) unless a note says otherwise; other counts (for example the stale rows in F1) are quoted from the investigation. A claim marked **[U]** could not be verified and is an estimate.

## 1. How the code is used

- `gates.py` and `queue.py` are pure functions of data: no network, no database, no clock (the caller passes `now`). The same code runs in two places. On Railway it runs when the fleet run ends: the dealer and fleet gates give each dealer a state and the run a verdict (task L15 acts on a HOLD), and the output is published as `gate_report.json` and `queue.json` in the run bundle. The write predicates of section 6 are the target rule for the scanner's write path; wiring them in is outside the current task queue, and production scans start with reconcile off until then. In a cloud session the code runs offline over a published bundle (golden tests, backtests, re-deriving a queue); the cloud copy never decides a production write. Cloud sessions cannot reach dealer sites, so they work only from persisted data.
- A gate returns `pass`, `fail` or `cannot_assess`, with the measured value and the threshold used. A **hard** failure counts at once. A **soft** failure counts after two consecutive failing scans (section 3, rule 3). A failure can switch off **retire**, **moves** or **promotion** for the dealer (section 6).
- Thresholds are module constants collected in one frozen `GateConfig`, not environment variables. Changing one is a pull request that also changes this document. Loop budgets (dealers per cycle, attempts per dealer) live in `config.py`, not here.
- Both modules use only the standard library, because they run in the scanner image. Suggested entry points (the task cards use the same names): in `gates.py`, `evaluate_dealer(entry, extras, history, registry, cfg, now)` returns gate results, state, reason codes, write permissions and the next history record (`now` dates the history and decides which registry entries have expired); `evaluate_run` does it for every dealer of a run; `evaluate_fleet(run, prior_runs, summary, cfg)` returns FG1 to FG8 and the outcome; `can_retire`, `can_move` and `can_promote` answer the write gates; `verify_fix` answers section 9; `INPUT_FIELDS` lists every field a gate reads; `legacy_inputs(entry, side)` adapts the 2026-11-03 files (section 12.2). In `queue.py`, `score_dealers` and `fix_units` rank the work and `select_cycle(ranked, attempts, pending, loop_cfg)` applies the per-cycle budget of the `LoopConfig` (section 8).
- `gate_report.json` carries, per dealer, the state, the `watch` flag, `since`, `streak`, the result and measured value of every gate, the reason codes and the three write permissions, plus a fleet block with FG1 to FG8 and the outcome. `queue.json` carries the ranked fix units (Q, E, class, dealers) and the owner list. Neither holds secrets; `BUNDLE.md` section 4.6 lists them as optional artifacts of the bundle, and their exact layout is fixed by the golden files of the tests (tasks L8 to L10).
- The pipeline's verdict (`ok`, `thin`, `inaccurate`, `no_rows`, `error`, `no_recipe`) is unchanged; the gates add a state per dealer. A dealer with verdict `error` (a crashed or killed scan) has no data: every gate returns `cannot_assess` and the dealer is simply rescanned. A dealer with `no_recipe` is `needs_recipe_work`.

## 2. Inputs: the metrics available per dealer

### 2.1 Fields of `triage.json`, `dealers[]` (458 entries on 2026-11-03)

| Group | Exact field names | Remarks |
|---|---|---|
| Identity, run | `dealer_id`, `run_id`, `provider`, `minutes`, `error` | `error` is null for all 458. |
| Volume | `known_before`, `rows`, `rows_stamped`, `rows_new`, `rows_used`, `active_after`, `dealer_names`, `zip_codes`, `filtered_count` | `rows` is the feed before the rooftop gate. `rows_stamped` is `rows_new + rows_used`, the dealer's active rows written in this run (absent for the 4 dealers with no feed rows). `known_before`, `zip_codes` and `filtered_count` are 0 for all 458 and `dealer_names` is at most 1: dead metrics. |
| Recipe | `recipe{had_recipes, providers, synth, platform, place, recipe_status, validation, synth_vins, lifecycle, resynth}`, `recipe_fetch`, `recipe_coverage{price, trim, exterior_color}` | `recipe_fetch` is `full` (404 dealers), `partial` (50) or `none` (4), each followed by `+http_only`. Partial means the replay returned under 70% of the scanner's known lot, or price coverage under 50%. |
| Capture coverage | `coverage{price, trim, exterior_color, interior_color, engine_description, transmission, drivetrain, fuel_type, body_style, description, stock_number, msrp, gallery_8plus}` | Measured before the upsert, after the VDP fill. Newer files add `used_mileage`, `used_n`, `used_zero_mi`, `used_zero_mi_my2026`; today's verdict already gates `used_mileage` at 0.90. |
| VDP | `http_first{candidates, fetched, fields_filled, skipped_cap, statuses, wall_clock_sec, wall_clock_hit, slowed_host, host_exhausted, slow_host_skipped, retried}`, `vdp_recipes{}` | |
| Attribution | `vin_owner_conflicts`, `vin_owner_conflict_owners{dealer: n}`, `vin_facts{vehicles, cached, drivetrain, fuel_type, cylinders}` | |
| Timing | `timing{minutes, flags, vdp_http_first_max_sec, pages_needed}` | The flags describe the VDP pass, not the VIN walk. |
| Verdict inputs | `weak_key_fields`, `weak_fields`, `accuracy{rows, vpic_cached, incomplete_rows, incomplete_rows_raw, missing, missing_raw, missing_exempt, msrp_expected_rows, msrp_missing_rows, hard, hard_rows, examples}`, `verdict`, `reason` | `accuracy` exists for 400 dealers (every ok and inaccurate one). Thin and no_rows dealers have none: the NHTSA step is skipped for them. |
| Write side | `reconcile{eligible, stale, retired, reason, baseline_fallback, oldest, kept_missing_condition, kept_low_bucket, retirable, buckets{new, used, unknown: {baseline, rows, stale, kept}}}` | |
| Lifecycle | `lifecycle`, `lifecycle_route`, `lifecycle_detail`, `retried`, `previous_verdict` | |

### 2.2 Run-level inputs

`triage.json` top level: `started`, `chromium_leaks`, `recipe_coverage`, `counts`, `needs_discovery`, `thin`, `inaccurate`, `lifecycle{routed, resynth_ok, capture_ok, failed, retried, by_dealer}`, `platform_clusters`. `fleet_summary.json`: `dealers`, `assessed`, `verdicts`, `rows`, `vin_owner_conflicts`, `http_403_429{by_status, top_hosts}`, `http_first_statuses`, scan and total seconds, `shard_rc`, `post`, `resources`, `exit`. Four scanner-log lines carry facts that triage lacks: `Inventory reconcile <id>: scraped_candidates= marked_inactive= skipped_reason=`; `Recipe fetch [<name>]: N unique VIN(s) from P page(s)`; `Recipe fetch [<name>]: X/Y known VIN(s)` or `X VIN(s) vs Y known - <why>`; and `rooftop attribution [<id>]: ... refusing all N row(s)`. Section 11 makes these first-class fields.

### 2.3 Observed distributions, 2026-11-03

Verdicts: ok 258 (56.3%), inaccurate 142 (31.0%), thin 49 (10.7%), no_rows 9 (2.0%), error 0, no_recipe 0. Share of ok by `provider`: `dealer_dot_com` 55% (n=321), `dealer_on_cosmos` 52% (33), `dealer_eprocess` 58% (26), `team_velocity` 79% (24), `typesense` 65% (17), `motive_ridemotive` 57% (7), `jazel` 100% (6), `overfuel` 20% (5). No single template explains the failures.

| Metric | n | median | p10 | p90 | Tail |
|---|---|---|---|---|---|
| `rows` (feed) | 458 | 373 | 127 | 928 | max 2,516 |
| `rows_stamped` | 454 | 369 | 105 | 903 | stamped/feed under 0.9 on 28 dealers, under 0.5 on 13 |
| `active_after / rows_stamped` | 454 | 1.00 | 1.00 | 1.50 | over 1.25 on 61, over 2 on 33; 21,039 stale rows in total |
| `minutes` | 458 | 5.6 | 1.5 | 9.3 | max 19.1 |
| capture price, trim, exterior_color | 454 | 0.998, 0.996, 1.000 | 0.944, 0.955, 0.973 | 1.0 | under 0.9 on 23, 26, 10 dealers |
| drivetrain, description, msrp, gallery_8plus (secondary, never gating) | 454 | 1.0, 1.0, 0.879, 0.837 | 0.34, 0.14, 0, 0 | 1, 1, 1, 0.97 | under 0.9 on 112, 148, 243, 306 |
| incomplete share after the NHTSA fill | 400 | 0.95% | 0 | 6.6% | over 5% on 57; max 36.7% |
| hard-discrepancy share | 400 | 1.74% | 0 | 7.0% | over 5% on 76; max 54.8% |
| vPIC decodes cached / rows | 400 | 1.00 | 0.987 | 1.0 | p5 0.979; 2 dealers under 0.95 |
| VDP fetched / candidates (at least 20 candidates) | 436 | 0.72 | 0.004 | 1.0 | 174 dealers hit the 300 s wall clock; fleet 111,435 of 193,850 (57.5%) |
| `vin_owner_conflicts` | 63 above 0 | 31 | 1 | 493 | 34 dealers above 10; sum 8,888 = 4.09% of feed rows; max 1,352 |

- Hard rows by code (rows / dealers): `vpic_fuel` 3,958 / 273; `vpic_model` 951 / 119; `vpic_displacement_vs_text` 530 / 69; `catalog_engine` 291 / 27; `column_cylinders_vs_text` 161 / 84; `vpic_cylinders_vs_text` 134 / 76; `electric_label_combustion_text` 33 / 9; `vpic_make` 24 / 19; `vpic_year` 11 / 8.
- Fixable missing rows (dealers): price 1,730 (189), exterior_color 1,224 (150), mileage 574 (59), trim 400 (135). Exempted: trim filled by vPIC 794, allocation VINs 131.
- The 142 inaccurate dealers (reasons overlap): discrepancies 76, incomplete 57, one condition 31, VIN guard note 13. Dominant hard code among the 76: `vpic_fuel` 50, `vpic_model` 12, `vpic_displacement_vs_text` 7, `catalog_engine` 6. Dominant missing field among the 57: price 25, exterior_color 17, mileage 4.
- The weak field of the 49 thin dealers: trim only 20, price only 18, exterior_color only 4, combinations 7.
- Stability, same dealers on 2026-10-13 and 2026-11-03: ok to ok 214 of 240; inaccurate to ok 31 of 139; thin to thin 37 of 51; no_rows persisted 6 of 7; ratio of stamped rows median 1.003, p5 0.816, p95 1.215 (n=410). From the 09-28/29 scan, 71 of 278 ok dealers got worse and 51 of 180 non-ok became ok. The fleets ran different code, so cross-fleet differences are an upper bound on noise.

### 2.4 Blind spots of today's files

1. **No baseline.** `known_before` is 0 for 458 of 458 dealers (456 of 458 on 10-13): on a scratch copy the 30-day window is empty, so the row floor fell back to 20 rows. The scanner log knew a lot size for every dealer; it never reached triage.
2. **No walk report.** Triage has no pages, cap, site total or stop reason, and the timing flag `cap_hit` describes the VDP wall clock, not the VIN walk.
3. **The VIN-owner guard does not change the verdict;** `assess()` only appends a note.
4. **Thin dealers skip the NHTSA step,** so 49 dealers have no post-fill numbers.
5. **History is local.** Verdict history sits in per-dealer logs that are not in the repository, and the one-condition allowlist has 2 entries.

## 3. Notation and general rules

| Symbol | Meaning |
|---|---|
| F | feed rows (`rows`) plus rows the parsers refused for the rooftop |
| S | rows stamped to this dealer in the run (`rows_stamped`; 0 when absent) |
| A | active rows after the run (`active_after`) |
| B, K | baseline B (rule 5) and the scanner's known lot K |
| g | days since B was measured |
| X | feed rows refused for attribution: `vin_owner_conflicts` plus rows the parsers refused for the rooftop |
| R, P, Y | R = X / F, P = S / F, Y = S / B |
| T, W | the site's own vehicle total (when the payload exposes one) and the raw VINs the recipe walked, before any rooftop gate |

1. **cannot_assess.** A gate without an input returns `cannot_assess` and never guesses. Such a result opens no code ticket and does not change the state by itself, but when the gate is one of GF1 to GF4 it keeps retire and moves off for that dealer in that run. A fleet metric with no history to compare with (the first run) is skipped; its absolute limits still apply.
2. **Hysteresis** on share gates (the GA1 trim gap, GA2, GA3): enter at 5.0%, leave at 3.5%. A passing gate fails above 5.0%; a failing gate passes again at or below 3.5%; in between it keeps its previous result, and a pass inside the band marks the dealer `watch`. The band 3.5 to 6.5% (taking the larger of the incomplete and hard shares) holds 101 of 400 dealers, whose verdict flips between ok and inaccurate 28% of the time against 10% outside the band.
3. **Persistence.** A soft gate raises only a flag the first time it fails and counts toward the state when it fails on two consecutive scans. A hard gate counts at once.
4. **Judge on stamped rows.** Volume and yield use S, never the feed count F or the database count A.
5. **Baseline source.** B is the stamped rows of the dealer's last accepted run, with no time window; failing that, the scanner's known lot K; failing that, there is no baseline. An accepted run is one with fleet outcome PASS in which the dealer's state was ok (with or without the `watch` flag).

## 4. Dealer gates

### 4.1 Functional gates: does the scan see the whole lot?

| Gate | Rule | Evidence for the thresholds | Effect |
|---|---|---|---|
| GF1 rows | Fail if S < max(20, 0.5 B). With no baseline: fail below 20 rows, otherwise `cannot_assess` (volume). 0.5 is today's `ROW_FLOOR`, 20 is `MIN_ROWS_UNKNOWN`. | `known_before` was 0 everywhere, so only the 20-row floor ran: robinsford-com passed as ok with 24 rows against 287 active cars. | Hard. The dealer counts as `no_rows`; retire off. |
| GF2 yield | Y = S / B. Warn below max(0.70, 0.95 - 0.004 g). Below 0.70 is a recipe-work condition. Below 0.50 fails (the same event as GF1). | Between consecutive scans Y has p5 0.943, 0.913, 0.829 at 1 to 3, 3 to 8, 20 to 60 days (749 pairs) and is under 0.70 in 1.7% of the pairs; `full` recipes have p5 0.845 against the known lot. | Warn is soft (ticket after two warns). Under 0.70 acts at once and switches retire off. |
| GF3 walk | Complete if the walk stopped before its page cap and (T is unknown or W >= 0.95 T). The cap is the one the walk report states: min(150, ceil(T / page size) + 2) when T is known and 40, today's value, when it is not (SF8.2, task L16), so with T unknown a walk that reaches its cap is not complete. A recipe without pagination is one page by design: only the W test applies. No walk report: `cannot_assess`. | 11 of 933 recipe walks stopped at today's 40-page cap (`_MAX_REPLAY_PAGES`); 6 of those dealers were ok and 4,858 rows were retired in the same run. Site totals are readable for providers covering at least 94.8% of dealers (not for jazel, overfuel, html_cards, nabthat). | Hard. Retire off. The scanner's verdict becomes `thin` with a reason starting `replay_truncated:` (owner decisions: walk to the total, cap 150). |
| GF4 attribution | Fail if R >= 0.10, or if F >= 50 and P < 0.9. | 28 dealers have R >= 0.10 (12 of them ok) and 13 have R >= 0.5; the same 28 dealers keep under 0.9 of their feed stamped (13 under 0.5), so on this data the second clause adds nothing; it covers rows lost to refusals that `vin_owner_conflicts` does not count. | Hard. Moves off and retire off; fixed by code. A pass with R from 0.07 flags `watch` (reading 17 of section 12.6). |
| GF5 condition | C = min(`rows_new`, `rows_used`). Fail if S >= 20, C = 0 and the dealer has no accepted gap. Informational note if S >= 100 and C / S < 0.02. | 45 dealers have 20 or more feed rows and one empty side (43 on S); the note applies to 27 of the 413 dealers with S >= 100. | Hard. Recipe ticket; an empty bucket is never retired. |
| GF6 freshness | Fail if the dealer has no live recipe, or its newest successful recipe run is older than 30 days and Y < 0.9. Count dealers, not stale recipes. | 190 of 1,113 recipes were older than 30 days; 24 of 27 stale-flagged recipe rows were harmless duplicates. | Hard. Recipe ticket. |
| GF7 egress | Fail if `recipe_status` starts with `blocked:<tag>:`, or at least 3 feed requests returned 403, 429 or a challenge page and no VIN came back, on a host with an egress tag. | About 25 of 561 dealers on the 2026-09-29 Railway run **[U]**. The scanner already records `blocked:<tag>` instead of staling the recipe. | State `blocked`; rows carry forward. |

The investigation gave GF5 to GF7 no hard/soft label; they act at once (the backtest in section 12 confirms this for GF5).

### 4.2 Accuracy gates: are the rows right?

| Gate | Rule | Evidence for the thresholds | Effect |
|---|---|---|---|
| GA1 key fields | Capture coverage of price >= 0.90 and of exterior_color >= 0.90 (after the VDP fill); `used_mileage` >= 0.90 when present. Trim is judged after the NHTSA fill: `accuracy.missing.trim / accuracy.rows` <= 5% (a share gate). Without an `accuracy` block use capture trim >= 0.73. A failing result on a dealer whose VDP pass hit its wall clock (`http_first.wall_clock_hit` true) is `cannot_assess` instead: the gap may be an artifact of the window. | 80% of price gaps and 91% of colour gaps survive the fill, but only 18.5% of trim gaps **[U]**; 11 of the 20 trim-only thin dealers would clear **[U]**. 174 of 458 dealers hit the wall clock. | Soft. |
| GA2 incomplete | `incomplete_rows / accuracy.rows`: enter above 5.0%, leave at 3.5%. The same wall-clock rule as GA1 applies to a failing result. | Median 0.95%, p90 6.6%. | Soft. |
| GA3 NHTSA | `hard_rows / assessable`: enter above 5%, leave at 3.5%. assessable = `vpic_cached` minus rows the vPIC error-code gate left unjudged (`vpic_gated`); if assessable is under 75% of `accuracy.rows` the gate is `cannot_assess`. Invariants: stored drivetrain equals the vPIC drive where vPIC states one (a vPIC 4x2 is excepted), and the stored fuel bucket equals the vPIC electrification; violations <= 0.2% of rows. | Fleet hard rows are 1.03% on the staged code and 0.61% with the next verifier fix **[U]**; 45 dealers have 25% or more of their rows gated **[U]**. | Soft. An invariant breach is hard. |
| GA4 identity | Fail if n(`vpic_make`) + n(`vpic_year`) >= max(5, 1% of `accuracy.rows`). | 35 rows of 186,026, at most 3 per dealer. | Hard. Promotion off. |

## 5. Dealer state

The state is the first rule below that applies. An unexpired accepted gap removes its gate from the evaluation.

1. `blocked`: GF7 fails. Rows carry forward, nothing else is judged, and the dealer goes to the owner list.
2. `needs_code_work` (hard code): GF3, GF4 or GA4 fails; a GA3 invariant is breached; or the dealer is a single-page widget (the recipe has no pagination and S equals the page size, as with the Algolia widgets that return 36 rows).
3. `needs_recipe_work`: GF1, GF5 or GF6 fails, or Y is under 0.70 with GF3 and GF4 clean.
4. `needs_code_work` (soft code): GA1, GA2 or GA3 counts as failing under the persistence rule (section 3, rule 3). (The investigation requires "VDP pass complete" here without defining it; this document reads it as GA1 and GA2 returning `cannot_assess` after a wall-clock cut, see section 4.2.)
5. `ok`. An `ok` dealer carries the flag `watch` when a soft gate failed for the first time, a share sits inside the hysteresis band, or GF2 warns. The flag changes neither the state nor the counts of section 12; it matters only to the HOLD rule of section 7.

The next history record keeps `since` (the date the current state began), `streak` (consecutive scans in this state) and, per soft gate, its last result and its consecutive failing scans; hysteresis and persistence read it. A run with fleet outcome FAIL leaves every history record unchanged. Tickets follow the state: `needs_recipe_work` opens a recipe ticket, `needs_code_work` a code ticket (for soft gates once the persistence rule is met), and the `watch` flag, `blocked` and accepted gaps open none; blocked dealers and accepted gaps go to the owner list.

**Accepted-gap registry.** If a soft gap, or a one-condition lot under GF5, persists for 3 scans, the VDP pass was at least 90% complete, no code touching the template changed meanwhile and the gap is at most 30% of rows, the gap is proposed as an `accepted_gap` (class d: nothing to fix on our side). The registry is the tracked file `deploy/railway/accepted_gaps.json` in the public repository, a JSON list of `{dealer_id, gate, evidence, approved, expires}` with the expiry 90 days after the approval date; `ops_gates.py --registry PATH` passes it to `evaluate_dealer`. For the gates it replaces the untracked `one_condition_ok.txt` allowlist (the scanner's own verdict keeps reading the tracked `deploy/railway/one_condition_ok.txt` that task L14 adds), and it is seeded with that allowlist's two entries, quantumautosales-com and honestcardeal-com, for gate GF5. An analysis session proposes an entry as a finding in the data repository with the evidence; the owner approves it, and it reaches the file through a code-session pull request. An expired entry exempts nothing.

## 6. Write gates

These block production writes; everything else only opens tickets. Production scans start with retirement (reconcile) off, so WR stays dormant until the walk report and the page-cap change (tasks L16 and L17) have shipped and the scanner's write path calls these predicates. That wiring is not in the current task queue (see its last section); task L9 only specifies and tests the predicates.

- **WR, retire.** Judged per condition bucket b (new, used). Let A_b be the bucket's active rows before the run, S_b its rows in this run and seen_b the part of A_b seen again (`per_condition.<b>.active_before`, `stamped`, `seen_again`). Retire only if all of these hold:
  - GF3 passes; S_b >= 10 and S_b / A_b >= 0.8; seen_b / A_b >= 0.70.
  - The dealer is not blocked, GF1 has not failed and none of GF1 to GF4 is `cannot_assess`; R < 0.10.
  - Each VIN to retire was already missing in an earlier eligible run at least 20 hours before (two strikes: the first miss only records a strike, a sighting clears it).
  - The retired share stays at or below min(0.60, 0.10 + 0.015 g) of the dealer's prior active rows, where g is the days between the run start and the median `scraped_at` of the rows being retired.
  - Why WR is stricter than the scanner's own reconcile: the scanner's floor of 0.5 is too loose (encinitasford-com New was retired at 52 to 59% coverage), and at coverage 0.5, 0.6, 0.7, 0.8 the merge tool retires 31,610, 19,197, 6,195, 986 rows. No gate may depend on re-fetching listings from a datacenter address: a live check of 150 retired listings was inconclusive for 102.
- **WM, VIN move** (an active VIN changes `dealer_id`). Refuse unless the claimant has per-car rooftop evidence, the current owner's run did not list the VIN, the owner's capture would itself pass WR, the claimant's R is under 0.10, and moves per run stay at or below max(5, 1% of the claimant's S). The 48-hour window of `_VIN_OWNER_GUARD_DEFAULT_HOURS` means nothing with weekly scans: 3,378 VINs changed owner on 11-03 and 6,961 on the first Railway run.
- **WP, promote** the dealer's fresh rows only for pipeline verdicts ok, thin and inaccurate, and for inaccurate only without a GA4 or GF4 failure. Verdicts no_rows and error, and state blocked, carry the dealer's previous rows forward unchanged.

## 7. Fleet gate

| Metric | Threshold | Observed (10-13 / 11-03 / Railway 09-29) |
|---|---|---|
| FG1 integrity: assessed / roster, shard return codes, post steps, error share | FAIL if assessed < 98%, any rc != 0, or error >= 2% | 21 pause-killed errors (4.6%) on 10-13, so FAIL; 0 on 11-03 |
| FG2 mix: ok share; (no_rows + error + no_recipe) share | HOLD if ok < 0.45 or below trailing - 0.08; HOLD if the bad share > max(3%, trailing + 1.5 points) | ok 52.4 / 56.3 / 56.1%; no_rows 1.5 / 2.0 / 3.0% |
| FG3 volume: sum of S / previous sum | HOLD outside [0.90, 1.12] | 1.05 (11-03 against 10-13) |
| FG4 shrink breadth: dealers below the GF2 warn level | HOLD if more than 8% | 5.3% below 0.8 and 2.8% below 0.7, of the 432 dealers stamped in both runs (see below) |
| FG5 attribution: sum X / sum F; new owner-claimant pairs | HOLD if > 6%, or if a pair absent from the previous run holds more than 100 VINs | 4.6% / 4.1%; 12,903 refused on 09-29 |
| FG6 retirement load: sum retired / prior active | HOLD above min(0.60, 0.10 + 0.015 g) | 49.8% at g = 36, so pass |
| FG7 egress: blocked share | HOLD above 6%, FAIL above 15% | 4.5% on Railway **[U]** |
| FG8 resources | FAIL if killed; HOLD if duration > 2 x trailing | 159 min, 12.7 GB peak of 16 |

"Trailing" is the mean of the previous three PASS runs (fewer if fewer exist). The FG4 figures were measured at fixed cut-offs. At the GF2 warn level for the 21 days between those two fleets (0.866) the share of dealers below it was 8.8% (38 of 432, re-checked), which would have been a HOLD; with daily scans the warn level is about 0.946 and the pair statistics suggest about 5%.

Outcome, in order of severity: **FAIL** (nothing promoted, rerun the failed shards; two FAILs in a row notify the owner), **HOLD-PROD-WRITES** (no retirements, no moves, additive updates only for dealers in state ok without watch; the staging database keeps the run), **PASS** (promote per WR, WM, WP). Any FAIL-level breach gives FAIL, else any HOLD-level breach gives HOLD-PROD-WRITES. FG5 concerns VIN moves and FG6 concerns retirements, but a HOLD from either stops both.

## 8. Improvement queue

For each reason r of dealer d, A_r is the cars affected, s_r the severity and t_r the tractability. The score is **Q_d = max_r(A_r s_r t_r) + 0.25 x the sum of the other terms.** A fix unit is the set of dealers one fix covers; its score is the sum of Q_d over them divided by E, in hours: 1 per dealer for a recipe regeneration, 8 + 0.05 x (dealers in the blast radius) for a template change, 6 for a guard or policy change. Class d items and blocked dealers are excluded and go to the owner list, as does a dealer whose attempt ledger shows 3 failed attempts. The per-cycle budget (6 dealers by default) comes from `LoopConfig` in `config.py` and is applied by `select_cycle` to the ranked list, skipping escalated dealers and dealers with a pending request; `score_dealers` and `fix_units` do not depend on it. The 4 cycles a day are counted by Railway's ledger (`BUNDLE.md` section 6, rule 5).

| Reason | A_r (cars) | s | t prior (observed) | Class |
|---|---|---|---|---|
| rot / no_rows | A (A - S for silent under-yield) | 1.0 | 0.60 (9 of 15 non-group rescans ok) | a |
| attribution | max(X, A - S), with X capped at A | 1.0 | 0.25 (0 of 4; the per-car rooftop stamps are unproven) | b, a, owner gates |
| identity | rows flagged | 1.0 | 0.5 | per case |
| one condition | max(A - S, 0.36 S) | 0.8 | 0.50 | a, c, d |
| truncated walk | T - W, else max(0.25 S, A - S) | 0.8 | 0.80 | c, b |
| single-page widget | A - S | 0.8 | 0.90 | b, a |
| price gap | (1 - coverage) S | 0.8 | 0.15 | c, b, d |
| trim, colour, incomplete, real discrepancy | gap rows or hard rows | 0.6 | 0.3 to 0.55 (pick one value per reason) | b, c, d |
| secondary gaps | rows | 0.3 | 0.2 | per case |
| comparator-only | hard rows | 0.1 | 0.95 | c |

- **Reasons come from the gates:** rot / no_rows from GF1 with S under 20 when GF4 does not explain it, and from GF6; attribution from GF4 and from parser-refused rows; identity from GA4; one condition from GF5; truncated walk from GF3; single-page widget from the widget test (section 5, rule 2); price, trim, colour and incomplete from GA1 and GA2; real discrepancy from GA3 after the comparator discount; comparator-only for dealers that GA3 clears only through that discount; secondary gaps from `weak_fields` outside the key fields. The reason codes are `rot`, `attribution`, `identity`, `one_condition`, `truncated_walk`, `single_page_widget`, `price_gap`, `trim_gap`, `colour_gap`, `incomplete`, `real_discrepancy`, `secondary_gaps` and `comparator_only`. Silent under-yield (GF1 or GF2 failing with S >= 20 and no other cause) uses the rot / no_rows row with A - S, so every dealer in `needs_recipe_work` has a queue item.
- **Constants.** 0.36 S comes from the median minority-condition share of 0.267 (0.267 / 0.733). The priors for rot and attribution come from the 11-03 lifecycle: 25 dealers had no rows on the first pass, re-synthesis saved a recipe for 18 (72%), 20 were rescanned (9 ok, 6 inaccurate, 1 thin, 4 no_rows, all four group feeds), and browser capture worked for 2 of 7. The investigation wrote the attribution row as max(X, A - S); capping X at A (refused VINs cannot exceed the cars at stake) is what reproduces the backtest below, otherwise mbontario-com ranks fifth instead of carvision-com.
- **Learning t.** Update t per failure signature (the reason, narrowed by platform family once data exists) as **t = (4 p + successes) / (4 + tries)**, p being the prior; a success is a dealer that meets "fixed" in section 9. Rot after one success in one try: (2.4 + 1) / 5 = 0.68.
- **Backtest on 11-03** (constants **[U]**): 220 dealers carry a reason and sum Q = 13,428; the top 10 hold 41%, the top 25 61%, the top 50 82%. The top five are autosavvy-com, toyotasb-com, lexusofsilverspring-com, hillsidetoyota-nyc, carvision-com. By dominant reason: rot 3,484 (5 dealers), one condition 3,156 (36), attribution 1,839 (24), truncated walk 1,544 (10), trim 1,033 (16), Algolia widgets 960, colour 402, price 339, real discrepancy 315, incomplete 204, comparator 151 (44).
- **Reconstruction.** That backtest has no reason for silent under-yield. A reconstruction with the table above (t = 0.45 for the four reasons with a range, the cap on X, no under-yield reason) reproduces the rot group (3,484.4), the truncated-walk group (1,546), the order of the top five and the cumulative shares (41%, 62%, 83%), with a total of 13,175; the trim and colour groups differ widely, so treat those two rows as provisional.
- **Hand-checkable examples.** autosavvy-com has A = 3,958 and S = 0: rot 3,958 x 1.0 x 0.60 = 2,374.8, which is the whole of Q while no parser-refused rows are known. With its 100 parser-refused rows an attribution term max(min(100, 3,958), 3,958) x 1.0 x 0.25 = 989.5 is added, so Q = 2,374.8 + 0.25 x 989.5 = 2,622.2. toyotasb-com: 803 x 0.60 = 481.8. hillsidetoyota-nyc has A = 1,449 and S = 576: max(873, 207.4) x 0.8 x 0.5 = 349.2. Work the queue by fix unit: one platform fix covers many dealers.

## 9. When a dealer is fixed, and the regression guard

A dealer is **fixed** only when all of these hold on a targeted rescan on Railway into the dev database:

1. The failing gate clears with margin: key coverage >= 0.93, incomplete and hard share <= 3.5%, Y >= 0.90 against B (or W >= 0.95 T), R < 0.03, and GF3 and GF5 pass.
2. NHTSA cross-check: decodes cached for >= 98% of rows; assessable share >= 75% (else the status is `fixed_unverifiable` and a sample check is needed); the drivetrain and electrification invariants show 0 violations (target) and at most 0.2% (limit); identity mismatches <= max(2, 0.3% of rows); hard share <= 3.5%; a row vPIC cannot decode is `cannot_assess`. vPIC outranks the dealer feed for drivetrain and electrification. The vPIC cache never refreshes (`ON CONFLICT DO NOTHING`), so stale decodes need a known-bad decode list keyed by VIN prefix and model year (a pending owner decision).
3. Both conditions are present where the site sells both (site census or the other side's URL).
4. A second scan at least 20 hours later passes the same gates. Until then the state is `fixed_pending` and writes follow WP and WR as before. Without this, the 10% verdict flap rate would mark about 1 fix in 10 falsely.
5. Logs and artifacts exist as `docs/NETWORK_SCAN_PROCESS.md` requires: the five dealer-log files, the recipe saved, `scan_instructions.md` written.
6. Write-path audit: for 10 VINs the stored price, mileage and condition equal the feed record (`cannot_assess` when the feed is unavailable).

**Regression guard for a change to a platform or shared module:**

1. *Blast radius.* Map each changed file to its family and dealers. Parsers and `synth/platforms/*` map to one family: ws-inv-data about 180 dealers, carscommerce about 178, cosmos about 80, team_velocity 36, DEP 29, typesense 21, algolia/motive 17, overfuel 5, jazel 6. `recipes.py`, attribution, `upsert/*`, `vpic_facts.py`, `scan_lab_report.py` and `model_aliases.py` map to all dealers.
2. *Offline replay* (works inside a cloud session): run the old and the new parser over stored page-1 and page-2 payloads of at least 5 dealers per family and diff the rows. Only the target dealers may differ, and only as expected. The repository has fixtures for about 20 dealers (`backend/tests/fixtures/rooftop_corpus`, `cosmos_rooftops`, `dep_rooftops`); Railway saves snapshots for the rest into the private data repository. The existing goldens stay green (`test_dealer_run_golden_20261001`, `test_attribution_golden_20261001`, `test_delta_scan_full_scan_parity`).
3. *Paired canary on Railway into the dev database:* control (current tag) and candidate scan the blast-radius set, or a stratified 30 (largest 10, random 10, all currently failing) when it exceeds 60, in the same window. Pass: for 98% of non-target dealers |S_candidate / S_control - 1| <= 0.05 and none below 0.90; key coverage drop <= 0.02; incomplete and hard share rise <= 1 point; X rise <= max(3, 1% of S); fleet ok share drop <= 1 point. Same-day pairs sit at p1 0.956 and p5 0.972, so 5% is wide; weekly noise (verdict flips on 10% of ok dealers) is why the comparison must be paired, never against last week. Cost about 0.3 to 0.75 USD and an hour per family.
4. *Release.* The first nightly after deploy runs in shadow (retire and moves off) for the family; an FG2, FG4 or FG5 breach with the change's signature means redeploying the previous tag.
5. The `dealer-discovery` workflow must verify with this guard, not with "the verdict of the failing group is ok" (`.claude/workflows/dealer-discovery.js`).

## 10. Failure categories of the 2026-11-03 scan

Class **a** regenerate the recipe (data); **b** change a template, parser or heal (code, release); **c** change a guard, policy or verifier (code); **d** not fixable from our side.

| # | Category and size | Root cause as established | Class |
|---|---|---|---|
| F1 | Group-feed attribution: no_rows 5 (mbontario-com 7 of 1,359 stamped, mtnviewnissan-com 0 of 922, parkscharlotte-com 4 of 312, overdrive-usa-com 14 of 156, autosavvy-com 0 rows); 13 inaccurate carry the guard note; 12 ok dealers have 10% or more of the feed refused; 28 dealers 10% or more; 8,888 VINs; 16,926 stale rows on 46 dealers | One feed serves many rooftops; parsers drop per-car rooftop fields (cosmos 3,968, ws-inv-data 1,082, Motive, Chapman, OneAudi, Jazel); the first-writer guard has no evidence tie-break; a recipe copied byte for byte (mbbeverlyhills-com = mbontario-com). The lifecycle fixed 0 of 4. | b, a, owner gates |
| F2 | Recipe rot or partial recipe: toyotasb-com (803 stale), hendricksubaruhoover-com (182), mckennasubaru-com (48), oldorchardnissan-com (12 new rows against 404 active), autosavvy-com (3,958) | Typesense collection 404; ws-inv-data 403; the recipe is the specials widget; a new-only partial recipe since 09-26. Browser capture failed 3 of 3. | a (browser capture needs the discovery image) |
| F3 | Algolia single page: 6 thin dealers (36, 36, 36, 36, 22, 46 rows, trim 0%) | `infer_pagination` has no Algolia branch; the widget filters cpo/used; 2,302 live own cars against 212 captured | b, a |
| F4 | Truncated walk: 11 dealers at 40 pages, 6 of them ok | `_MAX_REPLAY_PAGES = 40`; Jazel pages hold 12; dealer.com sections exceed 960; 4,858 rows retired in the same run | c, b |
| F5 | One condition: 45 dealers (feed rows >= 20, one side empty): inaccurate 32 (31 by reason text; honestcardeal-com is exempt), thin 10, no_rows 2, ok 1 (quantumautosales-com, exempt) | Section- or store-scoped recipes, group pool leaks, a side hidden behind a challenge page, a few by design | a, c, d |
| F6 | Key-field gaps (49 thin): price < 0.9 on 22, trim 26, exterior_color 10; 37 chronic | (i) Algolia 6; (ii) trim cured by the NHTSA fill, 11 of 20 trim-only dealers **[U]**; (iii) feeds without trim or colour (dealermasters 2, overfuel, dealer_on); (iv) price absent in feed and VDP at 100% VDP fetch (billholtblueridge-com, centralhoustonnissan-com, conicellitoyotaofconshohocken-com, economyhonda-com, warechev-com) **[U]**; (v) the 300 s VDP window | c, b, d |
| F7 | Incomplete rows (inaccurate 57) | 80% of capture-time price gaps survive the fill; junk dict-repr trims, 1,412 rows **[U]**; allocation VINs exempt 131 rows | b, c, d |
| F8 | Comparator noise: `vpic_fuel` 3,958 rows (68.5% of hard rows), `vpic_model` 951 | PHEV bucketed as hybrid; MINI "2 Door"; commercial and EQ model families; the error-code gate silences 14,610 rows (6.5%) **[U]** | c (verifier) |
| F9 | Real discrepancies, about 15 dealers: `catalog_engine` 291 rows (27 dealers), displacement against text 530 (69), cylinders text against column 295, BEV with liters 33 | The catalog link was made before the vPIC heal; text conventions; heal order | b, data repair |
| F10 | Silent under-capture on ok dealers: 29 dealers with stamped < 0.5 x active (9 ok, 3 of them attribution cases); 18 ok dealers with partial recipes | No baseline and no yield or refused-share gate | c (the new gates) |
| F11 | Infrastructure false alarms: 17 BROWSER LEAK events; `cap_hit` is the VDP wall clock | A machine-wide process count (`pipeline/db.py`) | c |

**Class d, no loop can fix:** (1) datacenter IP blocks, 25 of 561 dealers on the Railway run of 09-29 **[U]** (the scanner records `blocked:<tag>`); (2) managed-challenge pages on AMSI DEP and Dealer Inspire sites (a Referer or TLS profile sometimes helps); (3) dealer-side data: no price shown, allocation or in-transit placeholder VINs (84 of 220 New rows at northgeorgiatoyota-com, 51 of 1,287 at toyotacarlsbad-com), trailers and discontinued brands in `vpic_make` rows, vPIC wrong or stale (56 rows: 52 Audi Q5 Sportback, 16 BMW 550e from a stale cache) **[U]**; (4) rebranded or parked domains and junk entities (`do_not_scan`: 4).

## 11. Fields to persist before the gates can run in the loop

Cloud sessions cannot reach dealers or Railway, so the gates that decide production writes run on Railway and only their artifacts travel. Recipes and dealer logs can carry third-party API keys and never go in the public repository; payload snapshots and gate artifacts go only to the private data repository. Per run and dealer:

- **Baseline:** source (accepted run, known lot, none), B, and the run id and date it came from.
- **Walk report:** pages, page cap, `cap_hit`, `site_total`, page size, pagination type, raw and kept VINs, stop reason.
- **Yield:** recipe yield and known lot with the reason the scanner logged.
- **Counts per condition bucket:** active before the run, stamped, seen again (for GF5 and WR).
- **`vpic_gated` rows.** **Feed health:** requests answered 403, 429 or with a challenge page, VINs returned, egress tag.
- **Provenance:** code SHA, recipe-set hash and egress tag, to attribute flips between scans. **Allocation and in-transit VIN counts.**
- **State history** `dealer_scan_state`: state, since, streak, exemptions (the previous `gate_report.json` can carry it; a table is needed only if it cannot).
- **Outputs:** `gate_report.json` and `queue.json` (small, no secrets).
- **Evidence:** page-1 and page-2 payload snapshots for every non-ok dealer plus a fixed sample per platform family.

Also needed by the gates above though not in the investigation's list: rows the parsers refused for the rooftop (the log line `rooftop attribution [<id>]: ... refusing all N row(s)`; `filtered_count` is 0 for all 458 dealers today although autosavvy-com lost 100 rows that way), the newest successful recipe run and the number of live recipes (GF6), the drivetrain and electrification invariant counters (GA3, "fixed"), the last result of each soft gate and a strike record per VIN (hysteresis, persistence, WR).

Suggested layout of the per-dealer `extras` that `evaluate_dealer` reads (names may change, meanings may not). The natural home is each dealer's triage row and `scan_runs.summary_json`, so the fields travel in `triage.json.gz` and need no migration:

```
baseline          {source: accepted_run | known_lot | stand_in | none, B, run_id, date}
walk              {pages, cap, cap_hit, site_total, page_size, pagination, raw_vins, kept_vins, stop_reason}
yield             {recipe_yield, known_lot, reason}   # from the scanner's "X/Y known VIN(s)" line
per_condition     {new: {active_before, stamped, seen_again}, used: {...}, unknown: {...}}
refused           {parse_time, owner_guard}           # X = parse_time + owner_guard
vpic_gated        int
nhtsa_invariants  {checked, drivetrain, electrification}
feed_errors       {403, 429, challenge, vins_returned}
placeholder_vins  {allocation, in_transit}
recipe_age        {last_ok_at, live_recipes}
code              {sha, recipe_set_hash, egress_tag}
```

## 12. Acceptance targets

### 12.1 Backtest of the whole 2026-11-03 scan

Over 458 dealers the state function must give **ok 285** (139,409 active cars), **needs_recipe_work 42** (20,394), **needs_code_work 131** (74,767), **blocked 0** (not testable from a residential connection; expect about 4 to 5% on Railway **[U]**). Of the 258 dealers the old verdict called ok, 24 leave ok (12 attribution, 6 truncated walks, 6 under-yield); 51 of the 200 non-ok dealers become ok (42 comparator-only, 9 whose only fault was trim). About 40 of the needs_code_work dealers are price or colour cases that may turn out to be class d. This agrees with the staged-code replay (ok about 280 to 287) **[U]**. The run needs the full 458-dealer triage, so it happens where those files exist (the private data repository after the 11-03 run is converted into a bundle), not in the public repository's CI.

### 12.2 Reproducing it from the old triage files

The 11-03 files predate section 11, so a legacy adapter in `gates.py` (for example `legacy_inputs(entry, side)`, used by `ops_gates.py`) supplies what is missing. With these stand-ins the rules above reproduce all four targets exactly, including the active-car totals (checked while writing):

1. B = `active_after`, source `stand_in`. Only the fail level (S < 0.5 B) is evaluated; the 0.70 and warn levels return `cannot_assess`, because A includes stale rows and overstates B. (Evaluating 0.70 would move eleven dealers from ok to needs_recipe_work, seven of them with triage verdict ok, move three more from needs_code_work, and leave ok 274.)
2. Subtract `accuracy.hard.vpic_fuel` from `hard_rows` before GA3 (comparator noise that the verifier fix of release 1.5.11 removes). No other code is discounted.
3. Soft gates count at once (a single scan) and `wall_clock_hit` is ignored. (Applying the wall-clock rule of GA1 and GA2 would turn the soft failures of 37 dealers into `cannot_assess` and cut needs_code_work from 131 to 94.)
4. Side inputs, in the small JSON file `backend/tests/fixtures/ops_publish/side_inputs_2026_11_03.json` (keys `walk_capped` and `single_page_widget`, lists of dealer ids, and `refused_parse_time`, a map from dealer id to a count), filling `walk` (pages 40 of cap 40, `cap_hit`), the widget test (no pagination, page size equal to S) and `refused.parse_time`. Walk capped at 40 pages (11): 5starford-com, autonationtoyotacerritos-com, mountainstatestoyota-com, premiumautosinc-com, capitolchevy-com, mercedesbenzgreenway-com, duvalford-com, northparktoyota-com, mbofstevenscreek-com, toyotacarlsbad-com, gillmanhondahouston-com. Single-page widget (6): dreyerreinboldbmwnorth-com, carvision-com, milehighhonda-com, northparklexus-com, darcarshondatenafly-com, lexusofsilverspring-com. Parser-refused rows: autosavvy-com, 100 (it then counts R = 1.0 and lands in needs_code_work; without this input it falls to needs_recipe_work and the totals become 130 and 43).
5. GF5 exemptions are the two registry dealers. GF6 is not evaluated, GF7 never fires, GA4 never fires (at most 3 rows per dealer).

### 12.3 Golden file for the three fixture shards

`backend/tests/fixtures/ops_publish/` holds shards s0, s3 and s6 of the scan (`triage.json.gz`, `needs_discovery.txt`, `slow_dealers.txt`): 172 dealers, 101 ok, 13 thin, 55 inaccurate, 3 no_rows. `test_loop_gates.py` must classify all 172 through the legacy adapter, with the side inputs that fall inside them (6 walk-capped dealers, 1 widget, autosavvy-com), and compare the result with a stored golden file (for example `expected_dealer_states.json`: per dealer the state, the failing gates and the `cannot_assess` gates, plus the state counts, reviewed by hand against each triage `reason`). Regenerating the golden needs an explicit flag (`UPDATE_GOLDEN=1`), and a reviewer reads the diff. The reconstruction used for this document, replayed independently, gives 106 ok, 18 needs_recipe_work, 48 needs_code_work and 0 blocked for these 172, and the golden's counts must equal them unless the pull request explains a difference.

### 12.4 Cases asserted individually (those that fall inside the fixtures)

| Case | In the fixtures | Expected |
|---|---|---|
| robinsford-com: verdict ok with 24 rows (10 new, 14 used) against 287 active | s0 | GF1 fails (24 < 143.5); state needs_recipe_work; retire off |
| The 12 ok dealers with R >= 0.10 | 5 of 12: parksidekia-com (R 0.687), davekirk-com (0.596), hileyvwhuntsville-com (0.566), landersmclartysubaru-net (0.205), mbbeverlyhills-com (0.149). Outside: landersmclartytoyota-com, nissanofcookeville-com, mbofhenderson-com, landersmclartychevrolet-com, fivestarforddallas-com, hileymazdahuntsville-com, cumberlandchryslercenter-com | GF4 fails; state needs_code_work; moves and retire off |
| The 29 under-yield dealers (S < 0.5 A, verdict not no_rows; 9 of them ok, 3 of those are attribution cases) | 15. Ok: butlertoyotajacksonville-com, robinsford-com, doggetthondamedcenter-com, hudsontoyota-com, normreevestoyotadallas-com, davekirk-com, hileyvwhuntsville-com. Inaccurate: autonationfordtustin-com, mbhsv-com, napletonhondamortongrove-com, bmwofmurrieta-com, group1hondagreenbelt-com, kearnymesasubaru-com, crownlexus-com, lexusescondido-com | GF1 fails for all 15. needs_code_work where R >= 0.10 (davekirk-com, hileyvwhuntsville-com, mbhsv-com, crownlexus-com, lexusescondido-com), else needs_recipe_work |
| Stays ok under the stand-in baseline (0.5 <= S/A < 0.7) | freemantoyota-com, keithpiersontoyota-com (s6), capitaltoyota-com (s0) | ok |
| The three no_rows dealers | mckennasubaru-com, autosavvy-com, overdrive-usa-com | needs_recipe_work; needs_code_work with the parser-refused side input (needs_recipe_work without it); needs_code_work (GF4, 142 of 156 refused) |
| Truncated walks (side input) | ok: 5starford-com, autonationtoyotacerritos-com, mountainstatestoyota-com; thin: premiumautosinc-com, toyotacarlsbad-com; inaccurate: mbofstevenscreek-com | GF3 fails; needs_code_work; retire off |
| Single-page widget (side input) | dreyerreinboldbmwnorth-com | needs_code_work |
| Thin dealers whose only fault was trim | sterlingbmw-com, tustinlexus-com | ok |
| Comparator-only inaccurate dealers | 16 (for example bmwofsouthaustin-com, capitaltoyota-com, lexusofbellevue-com) | ok |

### 12.5 Other tests

`test_loop_gates.py` also covers: boundary tests just below and above every threshold; hysteresis sequences (4.9%, 5.1%, 4.0%, 3.4% gives pass, fail, fail, pass); two-scan persistence; `cannot_assess` propagation to retire and moves; the precedence matrix of section 5; fleet outcomes for FAIL, HOLD and PASS combinations; and a check that no threshold is read from the environment. `test_loop_queue.py` covers the examples of section 8 and the fix-unit ordering.

### 12.6 Readings adopted where the investigation is silent

Each is open to change by a pull request that updates this list. (1) An accepted run is a PASS run in which the dealer's state was ok (with or without `watch`); "trailing" is the mean of the previous three PASS runs. (2) GF5, GF6 and GF7 act at once. (3) F and X include rows the parsers refused for the rooftop. (4) GF3 reads the cap from the walk report (40 when T is unknown, SF8.2) and treats a recipe without pagination as one page. (5) A VDP pass counts as incomplete when `http_first.wall_clock_hit` is true, and it then turns a failing GA1 or GA2 into `cannot_assess`. (6) The FG5 pair clause is a second trigger, and FG5 and FG6 holds stop all retirements and moves. (7) In WM, R and S belong to the claimant and "the owner's capture is strong enough" means it would pass WR. (8) GA1 also gates `used_mileage`, because today's verdict does. (9) Silent under-yield uses the rot / no_rows row with A - S, and X is capped at A in the attribution row. (10) The signature for t is the reason, narrowed by platform family once data exists. (11) Handling of `error` and `no_recipe` verdicts (section 1). (12) A FAIL run leaves the history records unchanged (section 5). (13) Only a `cannot_assess` from GF1 to GF4 keeps retire and moves off (section 3, rule 1; section 6). (14) The accepted-gap registry is `deploy/railway/accepted_gaps.json` and `queue.select_cycle` applies the per-cycle budget (sections 5 and 8). (15) `watch` is a flag on `ok`, not a fifth state (section 5). (16) A dealer with verdict `error` gets the state `unassessed` (every gate `cannot_assess`, history carried over unchanged, all write permissions off); it is not one of the four tickets states and opens none. (17) A passing GF4 with R from 0.07 up to (not including) 0.10 flags `watch` on an `ok` dealer (`GateConfig.refused_watch`); it changes no state.
