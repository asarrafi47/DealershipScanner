# Owner decision brief, 2026-10

Written 2026-10-18 for unit N1.8 of `docs/NEXT_PHASES_PLAN_2026_10.md`. It lists every decision in that plan's Section 5 that is still open, grouped by the phase it blocks, soonest first. Answer in one line (format at the end). Each answer is then logged row by row in `docs/remediation/OWNER_DECISIONS_LOG.md`.

## Read this first

- **Answered 2026-10-16, not asked again:** D-N25, D-N9 (C+D), D-N1, D-N2 (b), D-N3, D-N4 (your change: build a profile for each missing rooftop and scan it as its own dealer), D-RC1 (A), D-RC2 (two strikes, at least 20 h apart). Earlier answers (D-REL1-10, D-IF7, D-RS5, D-SR7, D-TC5, D-SEC1, D-SEC4 and the others) stay as logged.
- **About "go ahead on the rest" (10-16).** The log records it only against the gating minimum. No other decision was logged row by row, so every other decision is listed here. If you meant every recommendation in the plan, reply `accept all`.
- **Settled by D-N2 (b); no answer needed unless you object.** D-DB5 = (b): the mini's own Postgres is retired. D-IF10 = the MBP holds the canonical `dealer_logs`. D-IF1's host = the MBP.
- **No answer needed.** D-RC4, D-DC10, D-OD12, D-HY8. D-SR1 matters only if you pick (b) for D-IF3. D-HY2 is an owner action (plan Section 6 item 9).
- **Dates** are estimates from the plan's Section 3.2 chart, with day 0 = 2026-10-16. If N1 slips, every date moves with it. Phases after N12 have no date in the plan. A phase never starts a unit whose decision is unanswered: that unit is marked BLOCKED and the rest of the phase goes on.
- **One recommendation differs from the plan: D-N6.** The N1.2 dossier for the Fletcher Jones group shows that fletcherjones-com has no rooftop of its own.
- **Evidence sources.** Numbers come from these sources and nowhere else:
  - `workspace/cleanroom_2026_10/analysis/baseline_r1.json`: the N1.3 acceptance tool run on `cars_cleanroom` at `078a0c211` on 2026-10-18. It matches the hand-built fixture on 67 of 67 values.
  - The N1.2 dossiers in `analysis/groups/*.json` and the field maps in `analysis/platform_evidence/*/field_map.json`. Each carries a checker verdict: 17 dossiers confirmed, 3 revise (group_1b, group_2, group_19); 6 maps confirmed, 2 revise (jazel, motive).
  - The values the N1.6 pack expects, read from the 10-04 prod dump (`workspace/phase_runs/N1/N1.6.json`). Live prod values stay cannot_assess until you run that pack.
  - The 10-15 gather reports in `workspace/phase_runs/N1/gather/`.
  - Read-only SQL run today on the local DBs.

## Summary

| Blocks | Decisions | Needed by (estimate) |
|---|---|---|
| N2 | D-N12 | today, 10-18 |
| N3 | D-N19, D-DEP1, D-SEC6, D-SEC5 | 10-19 (D-SEC6/D-SEC5 at the N3 env step, about 10-24) |
| N5 | D-DB6, D-DB7, D-PR8, D-N30 | 10-19 |
| N6 | D-N13, D-N14, D-N15, D-N16, D-N17, D-PR1, D-PR4, D-PR6, D-PR7 | 10-20 |
| N4 | D-N24 | 10-19 (hard deadline 12-01) |
| N7 | D-PLAN1, D-TC3, D-TC6, D-TC9 | 10-23 |
| N8 | D-N5, D-N6, D-N7, D-N34 | 10-23 |
| N9 | D-N8, D-RC3, D-RC7, D-SR4, D-N10, D-N11, D-N21 | 10-23 |
| N10 | D-IF5, D-SR2 | 10-23 |
| N11 | D-RC5, D-RC6 | 11-07 |
| N12 | D-IF4, D-N22, D-N23, D-IF1 | 11-12 |
| N13-N36 | 64 decisions, listed below | after N12 (no dates in the plan) |

## What the dossiers add to the answered decisions (no answer needed)

- **D-N3 (group pools).** The 19 conflict groups hold 9,615 refused rows. Summed from the dossiers, after the checkers' one-row fix in group_1b:
  - stored owner right: 2,179;
  - claimant right: 3,417;
  - a third store: 3,547;
  - pool with no evidence: 312;
  - unknown: 160.

  All 312 no-evidence rows are in group 2 (the Cookeville and AMSI Tennessee sites). So the `group_pools.json` primary is needed for about one group, not for the 10-15 estimate of about 1,995 rows. In most groups the strong stamped tier decides every car once the parser reads the store field (group_1a and group_13: "no designated primary needed").
- **D-N4 (missing rooftops).** After de-duplicating store names across the dossier files, the dossiers name 80 off-roster rooftops: 60 confirmed (53 of them hold clean-room rows, 4,446 in total, filed under sister stores today), 18 likely and 2 cannot_assess. The 3 Fletcher Jones California stores seen by two dossiers are counted once, with group_1a's live-payload counts (`docs/remediation/CLEANROOM_RCA_2026_10.md` Section 7.2).

  The checkers removed three entries from the profile queue:
  - "AMSI Kentucky Market pool" and "Louisville KY (unnamed)": no payload field names a store, so their rows stay held;
  - "CDJR Of Fort Payne": it is roster dealer basswoodcdjr-com and needs a recipe repair (N8.9), not a new profile.

  That leaves 15 likely rooftops (260 rows) for N1.11 to verify first. N1.11 batch 1 is the confirmed list (consolidated in `workspace/phase_runs/N1/N1.11_missing_rooftops.json`). autosavvy's 22 rooftops and infinitiofcharlotte's other makes are not in any conflict group of 20 or more rows, so the dossiers do not cover them; N1.11 starts them from the D-N4 list.

---

## N2: prod visibility hotfix (starts about 10-18)

**D-N12: a New car with no odometer value (IL-01).** Needed by N2.1, today.
- Question: a New, non-CPO car whose feed gives no mileage. Is the listing complete, and what does the page show?
- Options: complete / missing; page wording.
- Recommendation: complete. Show no mileage line on New cars. Keep "Mileage not listed" on used cars.
- Evidence:
  - In `cars_cleanroom`, 21,152 active New or blank-condition cars are flagged `mileage` only because the odometer is NULL. For 20,613 of them it is the only gap. Without that flag the clean room is 7.19% incomplete instead of 17.06% (`baseline_r1.json` incomplete).
  - The 10-04 prod dump has 28,983 such rows. 27,423 of them have no other gap, which is 10.2% of prod's 268,868 active cars (`N1.6.json`).
  - Whether prod hides them today is cannot_assess until you run the N1.6 pack.

## N3: security wave 1 and web stopgaps (about 10-19 to 10-24)

**D-N19: show "Inventory last checked <date>" while prod is stale?** Needed by N3.11, about 10-19.
- Options: yes / no.
- Recommendation: yes. One plain text line per dealer page and car page, with no card or box. It hides itself once the data is under 48 h old.
- Evidence: prod's newest scrape is the 09-29 fleet (plan Section 2.5). That is 19 days old today and will be about 44 days old at fleet #1 (about 11-12).

**D-DEP1: remove orphan dependencies and lift lxml to 6.1 or later.** Needed by P15B.1 (inside N3), about 10-19.
- Options: (a) now, with CI and Docker smokes / (b) with the next web deploy / (c) with the security cluster.
- Recommendation: (a).
- Evidence: lxml below 6.1 carries advisory PYSEC-2026-87. The orphans are crawl4ai, pyotp, segno, ollama, langchain-core/openai/text-splitters and langsmith (remediation plan D-DEP1).

**D-SEC6: X-Forwarded-For spoofing sent straight to the Railway origin.** Needed by N3.13 (the owner env step after P3.2 ships), about 10-24.
- Options: (a) `TRUSTED_PROXY_CIDRS=cloudflare`, so forwarded IPs are trusted only from Cloudflare / (b) remove the `*.up.railway.app` domain / (c) both.
- Recommendation: (c).
- Evidence: the railway.app origin also serves the site (Section 2.5). (b) alone does not close the hole, because the Railway edge routes by Host header (remediation plan D-SEC6). The variable change waits for `web-source-clear` (N1.5).

**D-SEC5: count the admin pass-through fix as applied?** Needed by N3.13, about 10-24.
- Options: applied / open.
- Recommendation: applied in effect. `DEV_IP_ALLOWLIST` stays unset until P3.2 ships and the Cloudflare CIDRs are live.
- Evidence: the prod pass-through variable was deleted on 10-08 (plan Section 2.1, owner prod block).

## N5: migration foundation (about 10-19 to 10-23)

**D-DB6: how migrations reach prod.** Needed by P4.8 and every later migration, about 10-19.
- Options: (a) by hand before the deploy (PG18 dump, dry run, apply), so the boot step finds nothing to do / (b) at web boot.
- Recommendation: (a).
- Evidence: web boot applies pending migrations by default (`scripts/docker-entrypoint-web.sh:21`, `MIGRATE_ON_BOOT` defaults to 1). The newest prod dump is from 10-04, and the local `pg_dump` 17 cannot dump PG18, so the PG18 client (Section 6 item 5) comes first.

**D-DB7: objects the code relies on that the migration chain does not create.** Needed by P4.3, about 10-19.
- Options: (a) adopt them into the chain / (b) drop them. Sub-question: use `NULLS NOT DISTINCT` on the option_rejections index?
- Recommendation: (a). `NULLS NOT DISTINCT` is fine: prod runs 18.6, local runs 17.10.
- Evidence: P0A.2 found 0 duplicate groups on prod and local, and no `*_embeddings` tables (gather report remediation_remaining), so adopting needs no data repair.

**D-PR8: the InventoryEnricher's Haiku fallback and `haiku_spec_cache`.** Needed by P4.3, about 10-19.
- Options: (a) never writes fact columns, opt-in, keep the table / (b) fills at the lowest rank / (c) today's behaviour / (d) delete the fallback and the table.
- Recommendation: (a).
- Evidence: nothing new since the plan. It keeps model guesses out of fact columns, in line with the CLAUDE.md data rules.

**D-N30: the 4 untracked log tables** (`image_summary_restore_log`, `make_canon_log`, `model_canon_log`, `package_value_purge_log`). Needed by P4.3, about 10-19.
- Options: adopt in V026 / drop now.
- Recommendation: adopt in V026, then export and drop them in P13B.4 with the other backup tables (D-DB1).
- Evidence: they exist outside the chain on prod and local (gather report remediation_remaining). Adopting them keeps strict mode from failing on boot.

## N6: listing write path and completeness (starts about 10-20)

**D-N13: the new-arrival window.** Needed by N6.4, about 10-20.
- Question: how long may a new car lack price or photos before it counts as incomplete, and what does the page show in the meantime?
- Options: N days; show or hide.
- Recommendation: 3 days, for price and photo gaps only. Show a plain "details coming" line, and never hide a car inside the window. The window is measured against the car's latest scrape, not the wall clock.
- Evidence:
  - Cars up to 3 days old were 11.5% incomplete, against about 4.8% overall.
  - In a paired test, 77% of price gaps and 65% of photo gaps filled on later scans, but only 36% of trim gaps and 32% of color gaps (plan Section 2.4). That is why the window covers price and photos only.
  - The N1.3 tool's own age split is flagged unreliable, because of the 14.6-day gap to the reference DB (`baseline_r1.json` incomplete.window).

**D-N14: price floor.** Needed by N6.7, about 10-20.
- Options: a dollar value; a rule for repeated values.
- Recommendation: $1,000. A value repeated across 5 or more models at one dealer is withheld (stored as NULL with a provenance note).
- Evidence: in the clean room (`baseline_r1.json` junk_prices):
  - 234 active cars at $10-$999, at 24 dealers;
  - 59 at $1,000-$3,000;
  - 5 over $1M, one an Accord at $6,001,323;
  - the repeated-value rule finds 7 dealer/price pairs covering 184 cars (the 10-15 report used a wider rule and found 9 pairs / 205 cars).

**D-N15: do placeholder-only images count as photos?** Needed by N6.8, about 10-20.
- Options: yes / no.
- Recommendation: no. A placeholder-only car counts as "no photo", and "no photo" counts as incomplete only outside the D-N13 window.
- Evidence: 9,270 active cars store only `/static/placeholder.svg`. Another 2,406 carry dealer placeholder URLs that count as photos today: unavailable_stockphoto 984, photo_unavailable 931, /default.jpg 416, photo-not-available 75 (`baseline_r1.json` placeholder_images).

**D-N16: MY2026-27 cars filed as used at 0 miles (F4).** Needed by N6.9, about 10-20.
- Options: exempt / relabel / flag.
- Recommendation: report it as a data question and do not count these cars as incomplete.
- Evidence: 497 active cars (464 Used, 33 Certified) at 18 dealers have model year 2026 or later and mileage 0 (read-only SQL on `cars_cleanroom` today).

**D-N17: identifier gate at write time.** Needed by N6.9, about 10-20.
- Options: refuse identifiers that are not 17 characters; decide what to do with 17-character IDs that contain I, O or Q.
- Recommendation: refuse anything that is not 17 characters now (order numbers, Koons' 8-character codes). The I/O/Q rows wait for D-N17b in N21.
- Evidence: 187 active rows at 11 dealers are not 17 characters long. 1,240 rows at 65 dealers are 17 characters with I, O or Q (`baseline_r1.json` vin_quality; same counts by SQL today). The plan's "1,251 at 69 dealers" is an older count.

**D-PR1: window sticker vs vPIC.** Needed by P8A.2 (inside N6), about 10-20.
- Options: (a) vPIC wins drive wheels, electrification and cylinders; the sticker wins engine text, transmission, colors, and which end drives when vPIC is silent or says 4x2 / (b) the sticker always wins / (c) vPIC wins everywhere.
- Recommendation: (a).
- Evidence: CLAUDE.md data rule. 5,306 active rows at 142 dealers store AWD or 4WD where vPIC decodes 2WD (AWD vs 4x2: 3,673; 4WD vs 4x2: 1,633) (`baseline_r1.json` drivetrain_vs_vpic). Live checks agreed with vPIC 15 to 0 (plan Section 2.3).

**D-PR4: scope of the drivetrain catalog tiebreak.** Needed by P8A.2, about 10-20.
- Options: (a) today's scope plus a veto for 4MATIC/xDrive names / (b) replace only 2WD, 4x2 or blank / (c) only when vPIC says 4x2. Sub-question: may a dealer's explicit FWD or RWD lose to the tiebreak?
- Recommendation: (a). On the sub-question: yes, when vPIC says 4x2 or nothing and the trim decoder agrees. A less specific feed value never replaces a stored drive end.
- Evidence: the same 5,306 rows. vPIC's "4x2" does not say which end drives, so the sticker or the trim has to name it.

**D-PR6: several listing signals contradict vPIC on drive.** Needed by P8A.2, about 10-20.
- Options: vPIC wins and the row is marked cannot_assess / vPIC wins silently / the dealer wins.
- Recommendation: vPIC wins, and the row is marked cannot_assess.
- Evidence: CLAUDE.md data rule; the 15-to-0 live checks.

**D-PR7: vPIC cylinders at write time.** Needed by P8A.2, about 10-20.
- Options: only when they do not contradict the engine text / always / keep both rules.
- Recommendation: only when they do not contradict the engine text.
- Evidence: the verifier finds 107 rows (62 dealers) where vPIC cylinders contradict the engine text, and 116 rows (67 dealers) where the stored column does (`baseline_r1.json` verifier_hard_codes).

## N4: Railway config-as-code (starts about 10-19; hard deadline 12-01)

**D-N24: replacement for Railway config-as-code.** Needed by N4.1, about 10-19.
- Options: Railway's supported mechanism after 12-01 / service settings through GraphQL.
- Recommendation: whichever one Railway documents as supported after 12-01. N4.1 verifies that before building anything.
- Evidence: the root `railway.toml` and `railway.scanner-nightly.json` stop working on 2026-12-01 (`SCANNING_OPS_LOG.md:136`). The plan wants the replacement proven by 11-20.

## N7: Postgres test harness core (starts about 10-23)

**D-PLAN1: run the Postgres harness right after N5?** Needed by N7, about 10-23.
- Options: yes / keep the old order (Phase 14A, late).
- Recommendation: yes. The plan already schedules N7 this way; a "no" moves N7 to later.
- Evidence: every retirement and guard test runs on SQLite only (`backend/tests/conftest.py:82-92`). N8 and N9 rewrite Postgres-specific SQL; the guard backstop compares `scraped_at` as text (`upsert/sql.py:150-155`).

**D-TC3: Postgres image for CI.** Needed by P14A.3, about 10-23.
- Options: match prod / pg17 / postgres:16.
- Recommendation: match prod: pg18 with pgvector 0.8.x.
- Evidence: prod runs PG 18.6 with pgvector 0.8.6 (Section 2.5). postgres:16 has no pgvector, so V001 cannot apply.

**D-TC6: the `integration` marker and CI job.** Needed by P14A.3, about 10-23.
- Options: replace the job with `pytest-pg` / keep both / fold them together.
- Recommendation: replace the job and keep the marker (it means "needs live data, dev machine only").
- Evidence: `pytest-integration` is one of the 4 required checks in the N1.7 ruleset (`workspace/phase_runs/N1/ruleset.json`), so you update the ruleset contexts when the job is renamed.

**D-TC9: may tests create `pytest_*` databases on the local server that holds `cars`?** Needed by P14A.1, about 10-23.
- Options: yes, guarded / a separate instance / CI only.
- Recommendation: yes, guarded. The guard refuses `cars`, `cars_cleanroom`, non-local hosts and port 15432.
- Evidence: nothing new since the plan.

## N8: attribution fix wave (starts about 10-23)

**D-N5: Hendrick used cars at Terry Labonte's street address (RET-9).** Needed by N8.8, about 10-23.
- Question: Hendrick's shared used pool (`source_id RHendrickUsed`) lists cars whose location is Terry Labonte Chevrolet's own street address. Are those Terry's cars?
- Options: yes / no.
- Recommendation: yes.
- Evidence:
  - The 11 cars wrongly retired at Terry on 10-14 all carry `RHendrickUsed` and dealer.location "1401 Bridford Pkwy, Greensboro, NC 27407", which is Terry's roster address. Two independent rounds confirmed this (gather report retirements_rca).
  - Re-checked live on 10-16, 12 of Terry's 16 retired cars are still for sale at Terry; 3 are gone and 1 moved to Darrell Waltrip Buick GMC (`analysis/groups/retired21_recheck.json`).
  - Terry's listing 9542 serves the whole Hendrick pool (11,943 cars, 11 source_ids), so the street address decides, not the pool (`platform_evidence/carscommerce/field_map.json`).
  - Today's store filter pins source_id 114069 and drops these cars (`synth/platforms/carscommerce_scope.py:182`).

**D-N6: scope of mbbeverlyhills-com and fletcherjones-com.** Needed by N8.9, about 10-23. **Recommendation changed from the plan.**
- Options: keep / drop, for each dealer.
- Plan's recommendation: keep both; re-discover the MB Beverly Hills endpoint; scope fletcherjones to its own rooftop.
- Recommendation now:
  - Keep mbbeverlyhills-com. Point its recipe at CarsCommerce `listings/6074385`, shipped together with the parser fix that reads the store label.
  - Drop fletcherjones-com as a store. N8.9 first moves its rows to the stores they name: Henderson now, the Las Vegas stores once N1.11 has profiled them. Then it leaves the scan roster.
- Evidence (mbbeverlyhills-com):
  - Its recipe queries mbontario-com's listing 2309549. 1,284 of its rows carry host www.mbontario.com, and 1,278 of those conflict with mbontario-com (`baseline_r1.json` cf7_class_b).
  - Listing 6074385 serves host www.mbbeverlyhills.com on 200 of 200 rows, with own source MBofBeverlyHills on 561 of 1,233 cars (10-16). Confidence: likely (strong). The site itself returns a Cloudflare 403.
  - With today's parser, the repointed recipe keeps 0 rows. With the label fix it keeps its own 80 of 200 sampled rows and refuses 120 sibling rows (`groups/group_1a.json`, real_mbbeverlyhills_endpoint).
- Evidence (fletcherjones-com):
  - All 131 of its clean-room rows are Mercedes-Benz of Henderson cars: 106 conflict rows plus 25 hidden ones.
  - It is a 13-rooftop portal on which every car names another store, so "its own rooftop" leaves 0 cars after the fix.
  - The dossier itself raises this as an N8.9 question (`groups/group_13.json`).

**D-N7: may `vdp_host_other_store` also un-list cars?** Needed by N8.5, about 10-23.
- Question: when a stored car's detail-page link points at another roster store's website, do we only refuse new writes, or also take the car off the site?
- Options: refuse the write only / also un-list.
- Recommendation: refuse the write only, until N11 proves the rule.
- Evidence:
  - The detail-page host and the photo account belong to the site that was queried, not to the store (`groups/group_1a.json` true_store_field).
  - Of the 1,278 mbbeverlyhills rows on host www.mbontario.com, 45 are MB Beverly Hills' own cars and 26 are unknown. Un-listing by host would take those down.
  - Separately, the stored host is the owner's own host on all 2,706 conflicted VINs held by 4 group dealers (plan Section 2.3).

**D-N34: clean up the 7 overlapping-recipe dealers in N8.9 instead of N13?** Needed by N8.9, about 10-23.
- Options: yes / no.
- Recommendation: yes. Keep one recipe set per dealer and archive the other in the repair file. If you decline, the clean-up stays in N13 as answered on 10-08.
- Evidence: andymohrford, dchparamushonda, stevenscreektoyota, huntersvilleford, bmwofmtlaurel, kunilexusofgreenwoodvillage and tuttleclickmazda each hold 3 recipes and 0 car rows in local `cars`. None of them was in the clean-room roster (SQL today), so the clean room says nothing about them. Whether they are in prod's roster is cannot_assess until N1.6.

## N9: retirement single writer (starts about 10-23)

**D-N8: role of `dealer_feed_scope` (CF-15).** Needed by N9.12, about 10-23.
- Question: a table marks some dealers' feeds as "group" or "rooftop". Should the scanner use it as evidence, or should it only drive the site's caveat line?
- Options: scanner evidence / display only.
- Recommendation: display only, refreshed from the scanner's `group_feed_unstamped` flag with your OK.
- Evidence: the table has 27 rows in both `cars` and `cars_cleanroom`: 2 group, 25 rooftop (SQL today). It covers 7 of 109 conflict dealers and was decided on 08-08. mbontario-com is marked rooftop although its listing holds several stores, and the scanner never reads the table (gather report conflicts_rca).

**D-RC3: how far a replay walk goes.** Needed by P5B.2, about 10-23.
- Options: keep 40 pages and only flag / walk to the payload total, capped at 150 / no cap.
- Recommendation: walk to the total, capped at 150 (`SCANNER_REPLAY_MAX_PAGES`).
- Evidence: 13 walks stopped at exactly 40 pages with nothing logged, at 13 dealers including terrylabontechevy, autosavvy, 5starford and mbofstevenscreek (`baseline_r1.json` page_cap). Cars a capped walk never reaches look sold to retirement.

**D-RC7: add the `listing_retirements` table?** Needed by P5B.3, about 10-23.
- Options: yes / `summary_json` only.
- Recommendation: yes.
- Evidence: the 21 clean-room retirements could be traced only by reading scanner logs line by line (for example `s5/scanner.log:1973`). The plan requires V027 on prod before any live retirement (Section 3.4).

**D-SR4: verdict for a truncated replay.** Needed by P5A.7, about 10-23.
- Options: (a) "ok" becomes "thin" with the reason token `replay_truncated:` / (b) a new verdict / (c) a note only.
- Recommendation: (a), and reconcile also reads the flag.
- Evidence: of the 13 dealers with a capped walk, 9 still got verdict ok (3 inaccurate, 1 error) (`analysis/fleet_signals.json`).

**D-N10: does a first pass with verdict `no_rows` write its rows?** Needed by P5A.1, about 10-23.
- Options: write / don't write.
- Recommendation: write. The prior-window rule and the two-strike rule protect against wrong retirements.
- Evidence: 7 clean-room dealers got no_rows (`fleet_signals.json`):
  - 3 lost almost every car to the VIN guard: mbontario-com wrote 11 of 1,507, mtnviewnissan-com 14 of 862, overdrive-usa-com 6 of 162;
  - 3 replayed 0 rows;
  - oldorchardnissan-com wrote 12, below its floor of 20.

  Writing nothing would hide the few real cars these dealers do have.

**D-N11: accept that failing closed keeps sold cars listed for one or two more runs?** Needed by P5A.1, about 10-23.
- Options: yes / no.
- Recommendation: yes.
- Evidence: of the 21 clean-room retirements, 12 cars were still for sale on 10-16 and 9 were not (`retired21_recheck.json`). The N1.4 tests pin the fixed behaviour: none of the 21 is retired in that run (`backend/tests/test_cleanroom_shapes_2026_10.py:246`). So the 9 would have waited one or two runs, and the 11 wrong retirements (plus P9559, retired correctly and relisted later) would not have happened.

**D-N21: alarm threshold for the first live prod retirement.** Needed by P5A.7, N11.7 and N12.8, about 10-23.
- Options: fixed 10% / gap-aware.
- Recommendation: gap-aware. Alarm when a dealer retires more than min(60%, 10% + 1.5% × days since its rows were last scraped) of its prior active rows. Nightly runs keep the fixed 10%.
- Evidence: prod's last scrape was 09-29, so at fleet #1 (about 11-12) the gap is about 44 days, which puts the alarm at the 60% cap. Turnover over the 16 days from 09-28 to 10-14 was up to 32%. That is an upper bound that includes scan misses (Section 2.5). A fixed 10% would trip on ordinary turnover after a six-week gap.

## N10: scan controls (starts about 10-23)

**D-IF5: how the VIN guard survives gaps between scans.** Needed by P6A.3', about 10-23.
- Options: (a) auto-extend / (b) set by env / (c) 72 h / (d) refuse known pairs.
- Recommendation (as revised in the plan): keep the 48 h default. Gap cover applies only to owners with a persisted strong tier. No blanket 72 h. Known pairs go through D-N3's `group_pools.json`.
- Evidence: a longer window locks in whoever wrote first (plan Section 8). The dossiers count 3,417 rows in the 19 groups where scan order put the car under the wrong store.

**D-SR2: stopping two hosts from scanning the same dealer at once.** Needed by P11A.3, about 10-23.
- Options: (a) delta scans take the file lock / (b) nothing / (c) a Postgres advisory lock per dealer, plus (a) as hygiene.
- Recommendation: (c).
- Evidence: file locks are per shard and per host, so they never exclude a dealer across hosts. The N12 home-IP bridge adds a second host writing to prod.

## N11: release-candidate rehearsal (starts about 11-07)

**D-RC5: wrong-retirement candidates that cannot be verified live.** Needed by P5B.8, about 11-07.
- Options: leave them retired, log and rescan / reactivate all / reactivate if recent and the bucket is empty.
- Recommendation: rescan first. Restore only what is verified live; send cannot_assess dealers to dealer-discovery.
- Evidence: the 10-15 RCA expected 11 of the 21 retired cars to be still listed. The 10-16 live check found 12 (`retired21_recheck.json`). Live state moves daily, so check at repair time.

**D-RC6: repair order.** Needed by P5B.8, about 11-07.
- Options: local then prod / prod only / local only.
- Recommendation: local, then prod.
- Evidence: under D-N1, `cars` is the prod-shaped rehearsal DB.

## N12: prod refresh (starts about 11-12)

**D-IF4: nightly run settings.** Needed by N12.7 and N12.11, about 11-12.
- Options: 8 shards × batch 6 at `0 9 * * *` UTC with a 14 GB memory alarm / batch 4 / investigate first.
- Recommendation: 8 × 6 at `0 9 * * *`, alarm at 14 GB (under the 16 GB cap).
- Evidence: the scanner is CPU-bound in one interpreter, so shards are the throughput lever (CLAUDE.md, 2026-09-28). Fleets take about 159 min on Railway (plan Section 8).

**D-N22: prod ownership before scanning resumes (CF-14).** Needed by N12.7, about 11-12.
- Options: manually reset the classified pairs / let the evidence rules re-file the cars.
- Recommendation: let the rules re-file during fleet #1, with snapshots and `car_move_log`. Reset by hand only pairs that DB checks verify, and only if any remain after fleet #2.
- Evidence: prod holds 12,903 conflict rows in 212 pairs (10,838 VINs), all from 09-29 (10-04 dump, `N1.6.json`). In the clean room the right store differs from row to row inside one pair: 2,179 owner right, 3,417 claimant right, 3,547 third store. A reset by pair would be wrong for many rows.

**D-N23: guard hours for fleet #1.** Needed by N12.7, about 11-12.
- Options: default 48 h / extended.
- Recommendation: the default 48 h.
- Evidence: prod's newest scrape is 09-29, so the guard has already lapsed. Fleet #1 then behaves like the N11 convergence pass, which is the pass that gets proven.

**D-IF1: the dealers that answer only from a home IP.** Needed by N12.10 (interim) and N14 (final), about 11-12.
- Options: (a) home lane / (b) paid residential proxy / (c) accept the loss.
- Recommendation: interim, you run a bridge from the MBP every 3 days or less; final (N14), (a) on the MBP.
- Evidence: in the 10-04 prod dump, 31 dealers have every recipe auth-staled and 235 recipe rows have `max_saved_at=0` (`N1.6.json`). A home IP is not a free pass either: the clean room saw HTTP 403/429 at 43 dealers (63 and 9 lines) (`baseline_r1.json` http_403_429).

---

## Later phases (after N12; the plan gives no dates)

These come from the plan with no new evidence unless a cell says otherwise. Each is needed before the named unit starts.

### N13 recipe store authority

| ID | Question | Options | Recommendation | Evidence | Needed by |
|---|---|---|---|---|---|
| D-RS1 | Which recipe store is canonical | (a) prod for all / (b) two stores plus a reconcile / (c) independent | (b) now; (a) once Railway does all scanning | 10-04 dump: 235 rows with `max_saved_at=0`, 31 dealers all auth-staled (`N1.6.json`) | P7.9 |
| D-RS2 | What re-synthesis does to old recipes | replace / replace-with-archive / merge | Replace-with-archive | old entries go to `superseded_json`, which old code never reads | P7.5 |
| D-RS3 | How far versioning goes | stop at phase 0 / DB-authoritative with CAS / DB only | DB-authoritative with CAS | home lane and Railway both write recipes from N12 on | P7.1 |
| D-RS4 | Blocked status per host? | shared / per host | Per host | a 403 on Railway says nothing about the home IP | P7.6 |

### N14 home-IP lane

| ID | Question | Options | Recommendation | Evidence | Needed by |
|---|---|---|---|---|---|
| D-IF3 | Who runs admin "onboard dealer" jobs | (a) retire queue onboarding, hand off to dealer-discovery / (b) onboard-only worker / (c) Railway service | (a); P11A.5 is then dropped and D-SR1 needs no answer | no worker service since 10-08; the mini has no role (D-N2) | P15A.3 |
| D-IF6 | Fate of the retired laptop nightly and k8s crons | port per step / home lane / drop | Per step, from the N14.1 desk review | P0B.5 never ran | P15A.4 |
| D-N28 | Replace the multi-GB scratch restore with a desk review of code and git history | yes / no | Yes; restore only for a step the review cannot settle | | N14.1 |

### N16 every fact writer through precedence

| ID | Question | Options | Recommendation | Evidence | Needed by |
|---|---|---|---|---|---|
| D-PR2 | May a rescan overwrite a higher-ranked value? | (a) no, record `feed_said` / (b) yes / (c) after N days | (a), only while the stored provenance still matches the column | | P8B.1 |
| D-PR3 | Dictionary engine correction | (a) fill blanks / (b) correct when vPIC agrees / (c) today / (d) correct when the dealer's own title or trim agrees | (b) + (d) | | P8B.5 |
| D-PR5 | How every scan path gets a vPIC decode | (a) inline, budgeted / (b) post-scan / (c) status quo | (a) | | P8B.4 |
| D-N32 | Does a noncanonical body style count as missing? | yes / normalize only | Normalize only | 24,817 active rows noncanonical (category 14,834, cab/van 2,478, trim copy 1,536, other 5,969); only 54 blank (`baseline_r1.json` body_style) | P8B.6 |

### N17 precedence close-out

| ID | Question | Options | Recommendation | Evidence | Needed by |
|---|---|---|---|---|---|
| heal window | When the prod fact heal runs | a date you pick | a weekday with no fleet running, after the N16 deploy is verified | | P8C.7 |

### N18 fetch policy and VDP prefetch

| ID | Question | Options | Recommendation | Evidence | Needed by |
|---|---|---|---|---|---|
| D-OD3 | Validation fetches through the scan's own replay? | (A) yes / (B) scan uses synth.http / (C) two stacks | (A) | VDP pages: 188,597 wanted, 92,282 fetched (48.9%); 140 dealers blocked (59,523 wanted, 365 fetched) (`baseline_r1.json` vdp) | P12A.2 |
| D-OD7 | One scanner fetch policy | (A) one block set {403,405,429,503} plus rotation from the per-host winner / (B) block set and rotation only / (C) status quo | (A), confirmed by N18.1; 429 gets a backoff, never rotation | same | P12A.2 |
| D-OD8 | Retry transient errors in replay | one retry / none / synth-style | One retry after 2 s, only if N18.1 or P6B.4 sees transient 429/5xx | | P12A.2 |
| D-OD13 | Does the DuckDuckGo spec search go through the proxy? | (a) no / (b) yes | (a) | | P12A.3 |

### N19 recipe validation rule

| ID | Question | Options | Recommendation | Evidence | Needed by |
|---|---|---|---|---|---|
| D-OD1 | Minimum-VIN rule | revised / status quo | Revised: one constant `RECIPE_MIN_VINS=5`, with the condition-pinned exception | "admit anything above 0" would let in 38 widget/fragment recipes on 33 dealers (remediation plan) | P12B.5 |
| D-OD2 | Which count decides pass | (A) one full walk at save, 2 pages on re-checks / (B) 2 pages / (C) both | (A) | numbers in `validation_paths_2026_10.md` §10 | P12B.2 |
| D-OD4 | `validate_recipe` | thin wrapper / delete / separate | Thin wrapper | | P12B.3 |
| D-OD5 | Cascade recipes go through the set gate | yes / no / retire cascade | Yes | | P12B.6 |
| D-OD6 | GET recipe with a non-JSON `post_template` | mirror the scan / error / repair | Mirror the scan | | P12B.3 |

### N20 dictionary lookups in prod

| ID | Question | Options | Recommendation | Evidence | Needed by |
|---|---|---|---|---|---|
| D-HY1 | Where the vehicle dictionary lives | keep tracked / LFS / object storage / other | Keep tracked | | P9.7 |
| D-HY9 | Serve trim-ladder content in prod | (a) yes, after you sign a 20-car before/after diff / (b) no | (a) | prod resolves Complete_Options for 54 of the top 300 groups; about 91k more active cars would gain content | P9.8 |

### N21 incomplete-listings remainder

| ID | Question | Options | Recommendation | Evidence | Needed by |
|---|---|---|---|---|---|
| D-N18 | Which price is "the price" | advertised final / internet price / fee-inclusive | The advertised price before taxes and government fees; MSRP only in `msrp`; add-on fees only if the site includes them | 6,281 active cars have no price (`baseline_r1.json` headline) | N21.1 |
| D-N17b | 17-character IDs with I, O or Q | refuse / keep flagged | Keep the rows, skip the vPIC decode, mark VIN-derived facts cannot_assess, leave them out of the VIN guard | 1,240 rows at 65 dealers (`baseline_r1.json`) | N21.2 |
| D-N31 | May a single vPIC trim fill feeds with no trim (Gardena)? | yes / no | Yes, provenance `vpic`, only when vPIC returns exactly one trim | about 620 rows (plan) | N21.4 |
| D-N33 | Dealer-relative interior color rule | adopt / not now | Not now; revisit after N21 | 4,573 blanks at 258 dealers that publish it on 75% or more of their cars (gather incomplete_listings) | N21.7 |

### N22-N25 EPA catalog and forced induction

| ID | Question | Options | Recommendation | Evidence | Needed by |
|---|---|---|---|---|---|
| D-EPA1 | How the 12,126 dictionary EPA CSVs are corrected | (a) in-place patch from a pinned `vehicles.csv` / (b) full regeneration / (c) forced-induction patch only | (a); you approve the pinned file (2026-09-29, 50,407 rows) | | P10A.5 |
| D-ET2 | How "naturally aspirated" is shown | (a) explicit value, a facet / (b) explicit, hidden / (c) blank / (d) provenance column | (a); revisit after the P10C.9 dry run | 10-07 estimate: about 77k cars become NA, about 19.8k boost labels filled, about 1.8k flip, 1,640 conflicts | P10A.1 |
| D-ET3 | Rows flagged both turbo and supercharged | Turbocharged / combined value | Turbocharged; raw flags kept | 229 rows | P10A.1 |
| D-ET4 | Role of the make/model heuristic | last resort / display only / remove | Last resort | | P10C.2 |
| D-ET5 | Twin-turbo labels | keep via one helper / collapse | Keep, one shared helper | | P10A.1 |
| D-ER1 | The one way EPA data enters `epa_master` | (a) vehicles.csv → dictionary CSVs → build / (b) both paths / (c) vehicles.csv directly | (a) | follows D-EPA1 (a) | P10B.3 |
| D-ER2 | Permanent catalog key | content key + ordinal / EPA id / (year, make, model, trim) | `epaId` when present, else a sticky content key | | P10A.2 |
| D-ER3 | The 20,006 legacy catalog rows | label only / re-resolve | Re-resolve, then archive unreferenced rows | 229,060 links, 136,001 active | P10B.6 |
| D-ER4 | Foreign-key policy | RESTRICT / SET NULL / none | RESTRICT, after a 0-dangling check on prod | | P10D.7 |
| D-ER5 | Root `DICTIONARY/` folder | git rm after audit / move / keep | git rm after the prod provenance backfill | 10,200 files | P10D.8 |
| D-ER6 | How EPA work reaches prod | agent with per-step approval / you run it / guards only | The agent prepares each step; you approve and run it | | P10D.6 |

### N26-N31 runtime, DB layer, dependencies, security wave 2, PG tests

| ID | Question | Options | Recommendation | Evidence | Needed by |
|---|---|---|---|---|---|
| D-N29 | Accept the conditional tail units with revival triggers | accept / keep unconditional | Accept (plan Section 8 lists each trigger) | | N26 |
| D-SR6 | Where the upsert measurement runs | MBP / mini / skip | MBP | the mini has no role (D-N2) | P11B.1 |
| D-DB3 | Once every DB is strict | (a) strict only, delete legacy DDL / (b) warn / (c) keep the fallback | (a); `INVENTORY_AUTO_MIGRATE=1` kept for dev | | P13B.1 |
| D-DB4 | Where boot-time DB init runs | (a) explicit step in every launcher / (b) at import / (c) gunicorn hook | (a) | | P13A.5 |
| D-DB8 | `cars.zip_code` | (a) drop / (b) keep | (b) keep; no migration | avoids an exclusive lock on `cars` | P13A.8 |
| D-DB1 | Backup tables | (a) export and drop / (b) backups schema / (c) keep | (a), after N25; list adds prod `cars_owner_backup_20260929` (6,961 rows), the 4 log tables and the N12 backups | | P13B.4 |
| D-SEC7 | Lock files per image | (a) `uv pip compile` with hashes / (b) floors plus pip-audit | (a), run only if confirmed | | P15B.8 |
| D-SEC2 | Dead TOTP/phone storage | (a) drop the columns / (b) scrub / (c) encrypt | (a) | 2FA removed in SEC-014 | P16.2 |
| D-SEC3 | Users data isolation on Postgres | (a) web-only schema and role / (b) disk encryption / (c) separate DB | (a), sized by P16.10 | | P16.10 |
| D-TC4 | Breadth of the Postgres test tier | targeted / whole suite / write path only | Targeted | | P14B.6 |
| D-TC8 | pytest-xdist | yes if over 12 min and safe / no | Decide from P14C.5 | | P14C.5 |

### N33-N36 browser retirement, splits, hygiene, docs

| ID | Question | Options | Recommendation | Evidence | Needed by |
|---|---|---|---|---|---|
| D-DC1 | Confirm the HTTP-only plan decisions | confirm / reopen | Confirm; D2 now reads "MBP primary" | follows D-N2 | P17B.11 |
| D-DC5 | Retire `--allow-browser` and the Playwright fallbacks | (a) now / (b) per platform / (c) keep | (a) | `SCANNER_ALLOW_BROWSER` unset on web and scanner-nightly | P17B.3 |
| D-DC6 | Dealer-group LLM adjudication (cannot run today) | (a) add an adapter / (b) delete the LLM branch / (c) leave | (b) | no `LLMClient` implementation exists | P17B.9 |
| D-DC7 | `backend/oem/vehicle_reference` | (a) keep / (b) keep the EPA client, delete the rest / (c) delete all | (b) | 1,669 lines, no importers | P17B.10 |
| D-DC9 | The `requires_browser` scan hint | (a) relabel / (b) retire the key / (c) drop the skip branch, keep the key | (c) | 19 local rows | P17B.7 |
| D-OD9 | Brochure CLI | recommended set / status quo | Recommended set | | P17C.2 |
| D-OD10 | Move the vPIC getter to module level | yes / no | Yes | | P17C.1 |
| D-OD11 | Scanner env knobs | registry + lint first / registry only / all at once / leave | Registry + lint first | 131 names, 101 never set in tracked deploy files | P17C.6 |
| D-HY3 | `dealer_logs` in git | (a) curated `_learning` only / (b) all / (c) none | (a) | | P18A.4 |
| D-HY4 | The 47 force-added `workspace/` files | (a) untrack one-offs, keep `dealers_backup.json` for now / (b) untrack all / (c) leave | (a) | | P18A.3 |
| D-IF9 | Deploy assets to delete | per plan | Delete the k8s, car-scanner and postgres manifests and the k8s block of `build.sh`; keep launchd plists until replaced | | P18A.7 |
| D-TC7 | How far ruff goes | first option / gate only / more | First option; later waves conditional (D-N29) | | P18A.8 |
| D-HY6 | Single entry document | (a) short RESUME_HERE pointer / (b) refresh both / (c) delete both | (a) | | P18B.1 |
| D-HY7 | Location-logging contract | (a) align docs and CLAUDE.md to what the pipeline writes / (b) add a `location.md` writer | (a); you approve the CLAUDE.md wording | | P18B.7 |

---

**Answer format:** `accept all except <ID> <your answer>, <ID> <your answer>` (for example: `accept all except D-N6 keep fletcherjones-com, D-N14 $500`).
