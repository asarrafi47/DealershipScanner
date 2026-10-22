# Next phases plan, 2026-10

## 1. Header

- **Written:** 2026-10-15, against v1.5.4 (`486f9012d` = tag `v1.5.4` = `origin/main` = `origin/feature/http-only-scans` = prod web deployment `848895c1`).
- **Asked for by the owner:** "create a phased plan of what is next to come, and then we will execute".
- **Inputs used:**
  - `docs/REMEDIATION_PLAN_2026_10.md`: the 39-phase plan from 2026-10-07, never edited since `dbdbf5cba`.
  - `docs/remediation/OWNER_DECISIONS_LOG.md`: 27 answered rows.
  - `docs/RELEASING.md`, `docs/SCANNING_OPS_LOG.md`, `docs/db_layer/SCHEMA_LEDGER_2026_10.md`.
  - `docs/INCOMPLETE_LISTINGS_FINDINGS_2026_10.md` (uncommitted).
  - The clean-room artifacts under `workspace/cleanroom_2026_10/`.
  - Seven gather reports written on 2026-10-15: remediation-remaining, conflicts RCA, retirements RCA, dealer records, incomplete listings, prod ops, retirement/attribution code map.
  - Three draft plans: value-first, risk-first, lean-throughput.
  - Three judge reviews. This plan is built on the value-first draft, adds the judges' grafts, and removes every fatal flaw they named (Section 3.4 lists each one and how it is handled).
- **Relation to the remediation plan.** `docs/REMEDIATION_PLAN_2026_10.md` keeps the detailed specs (Change, Tests, Accept) for every **P-unit**. This document does three things:
  - sets the order of all remaining work;
  - specifies the new **N-units** in full;
  - re-scopes P-units where the clean room showed that their premise was wrong.
  Where the two documents disagree, this one wins. N1.9 records the differences inside the remediation plan. On release and deploy mechanics, `docs/RELEASING.md` wins over both (Section 7.0).
- **Phase IDs.** N1-N36. Unit IDs are `N<phase>.<n>` for new units; P-units keep their IDs (P5A.1 and so on).

## 2. Where we are

### 2.1 Done phases

| Phase | State | Evidence |
|---|---|---|
| 0A gate facts, operational stops | DONE, except the mini parts and the web source | `SCANNING_OPS_LOG.md` blocks P0A.1-P0A.5 (lines 7, 308, 911, 1111, 1250); owner prod block at :1264 (worker and scheduler deleted, `CAR_CHAT_WEB_RESEARCH=0`, pass-through env deleted) |
| 0B decision-input measurements | DONE, except P0B.5 (never run) and the P0B.1 Dealer Inspire burst | `docs/monolith_audit_2026_10_01/measurements/*_2026_10.md` |
| 1A, 1B retirement stopgaps, recipe store phase 0 | DONE (P1B.5 dropped by D-RS5) | CHANGELOG `[1.5.3]` |
| 1C local repairs | DONE on the MBP; mini BLOCKED | `cars`: 0 of 687 recipe rows with `max_saved_at=0 AND recipe_count>0` |
| 2A CI on every branch | DONE | CI runs 37996383815 (main), 37995341168 (branch), both green on `486f9012d` |
| 2B release flow | PARTIAL | main = v1.5.4; retro tags v1.3.2..v1.5.3 on origin; P2B.5 ruleset missing; P2B.8 prune partial; no release-log row for 1.5.4 |

`git log v1.5.2..v1.5.4` holds 90 commits. Nothing has landed after v1.5.4.

### 2.2 Loose ends from 0A-2B

| ID | Loose end | Where this plan handles it |
|---|---|---|
| LE-1 | scanner-nightly still holds `SCAN_FLEET=1` (plus `SCAN_SHARDS=8`, `SCAN_BATCH=6`) on the 09-29 image `89c13ad52` (VERSION 1.4.1, built before the egress tag). Any redeploy, restart, deploying variable change, upload or rollback starts a 561-dealer fleet against prod (a restart relaunches the same deployment with its old variables, `RELEASING.md:579-580`). The prod VIN guard has also lapsed: the newest prod scrape is 09-29 and the window is 48 h | Section 6 item 1, N1.5 |
| LE-2 | Web's GitHub source was still set on 10-09. That blocks every **web variable change** (`RELEASING.md:597-604`). It does **not** block pushes or `deploy_web.sh` uploads (`RELEASING.md:634`; autodeploy is off everywhere, `RELEASING.md:652`) | Section 6 item 3, N1.5 |
| LE-3 | No GitHub ruleset on main (`gh api .../rulesets` returns `[]`), although D-REL6 (a) was answered | N1.7 |
| LE-4 | 6 stale remote branches, 9 stale local branches, no `archive/*` tags, no ref bundle | N1.7 |
| LE-5 | Branch naming: the phase-runner used `remediation/<id>`; D-REL2 says `phase/<id>` | N4.3, Section 10 |
| LE-6 | The release log has no 1.5.4 row; whether the P2B.6 Docker smoke ran is unverified | N1.9 |
| LE-7 | The mini is blocked (no SSH approval since 10-07). The plan's mini assumptions are stale (D-DB5, D-IF1, D-IF10, P6A.7, P15A.6) | D-N2, N4.4 |
| LE-8 | P0B.5 was never run, so D-IF6 cannot be answered | N14.1 (desk review) |
| LE-9 | The P0B.1 Dealer Inspire burst and the D-OD8 Railway recheck are still owed | N12 (P6B.4 collects D-OD8 data), N18.1 |
| LE-10 | The remediation plan is unedited: all statuses TODO, no answers or numbers pasted in | N1.9 |
| LE-11 | Owner-only items: D-HY2, www DNS (`dig +short www.sarraficars.com` is empty), admin password rotation, Actions failure email, car chat check on 1.5.4 | Section 6 |
| LE-12 | `deploy/railway/sync-vault-to-railway.sh:98` resets `TRUSTED_PROXY_HOPS` to 1 (prod uses 2) | Never run it before P3.7 (N3) |
| LE-13 | Stale memory notes | N1.1, N1.9 |
| LE-14 | The 7 overlapping-recipe dealers (decision log 10-08, "clean up in Phase 7") have no unit | N8.9 if the owner accepts D-N34 (ahead of the first prod fleet); otherwise N13 (Phase 7), as answered on 10-08 |

### 2.3 Clean-room scan facts (2026-10-13/14, DB `cars_cleanroom`)

The DB started empty: recipes, the VIN-decode cache and the EPA/catalog reference tables were copied from `cars`. 458 dealers were scanned from the MBP home IP, with paid steps and auto-heal off. A 21-hour SIGSTOP pause ran from 2026-10-13 15:34Z to 2026-10-14 13:16Z.

| Fact | Number | Source |
|---|---|---|
| Car rows / active / inactive | 199,478 / 199,457 / 21 | read-only SQL on `cars_cleanroom` |
| FLEET SUMMARY "rows" | 208,233 | sum of triage `rows`, which `assess.py:104-108` takes from `capture_coverage.n` at `persist.py:18`, **before** the VIN-owner guard drops refused VINs (`persist.py:25-35`) |
| Accounting identity | 208,233 captured − 9,610 guard refusals = 198,623 stamped; + 834 rows of the two pause-killed error dealers (righthonda 553, selmanchevy 281) = 199,457 active; + 21 inactive = 199,478 | `s*/triage.json`; holds for all 437 dealers with rows (the other 21 have `rows=null`) |
| **The "~8,800 in-run retirements" premise** | **False.** The 8,776 gap is 9,610 refusals − 834. Only 21 rows were retired | three independent re-checks |
| The 21 real retirements | all by the scanner's `inventory_reconcile` (UPDATE at `inventory_reconcile.py:323-330`) during lifecycle retries, 10-14 13:29-13:45Z: terrylabontechevy 16, coastlinecdjr 3, napletonhondamortongrove 1, southcountylexus 1. 10 correct, 11 wrong (all at Terry: cars still in Hendrick's pool at Terry's street address) | `s5/scanner.log:1973`, `s2:1892`, `s0:1764`, `s7:1729` |
| Retirement mechanism | a lifecycle retry with a re-synthesized recipe of a different scope runs against the failed first pass's rows. Coverage is computed after the upsert (`inventory_reconcile.py:242`), so a small stale tail always passes the guards (Terry used bucket 134/150 = 89%) | retirements RCA |
| `vin_owner_conflicts` | 9,638 rows, 7,393 VINs, 92 (owner, claimant) pairs, 60 owners, 65 claimants. Triage's 9,610 counts each dealer's latest run only | SQL; triage |
| Same car on both sides | same stock number on 9,577 of 9,638 rows; different prices on 3,170 | scan JSONL join |
| Conflict classes (rows) | (a) correct refusal 1,874; (b) wrong winner 3,334; (c) same store under two IDs 0; (d) third rooftop 1,422; pool with no evidence 1,995; unknown 1,013 | conflicts RCA, rule-based (likely) |
| Per VIN | about 4,060 of 7,393 (55%) stored under the wrong dealer; about 1,579 right | conflicts RCA (likely) |
| Ranked root causes | (1) parsers drop per-car rooftop data: cosmos 3,968 owner rows, dealer.com payloads routed to `generic_json` 1,082, Motive 624, Chapman 446, OneAudi 231, Jazel 130; (2) first-writer guard with no evidence tie-break (`database.py:83-101`, 48 h at `:53`); (3) cross-wired recipe: `mbbeverlyhills-com` is a byte copy of `mbontario-com`'s CarsCommerce listing 2309549: 1,284 rows on host `www.mbontario.com`, 1,278 of them conflict rows with claimant `mbontario-com`; (4) evidence-less pools, about 1,995 rows; (5) weak-tier keep at fletcherjones, 106 rows; (6) rooftops missing from the roster land under siblings with no conflict | conflicts RCA |
| Only 4 parsers stamp `_rooftop` | team_velocity, dealer_dot_com, typesense, carscommerce. The gate keeps every row when no row names a store (`match.py:338-342`, `rooftop.py:804-805`) | code map |
| Stored `source_url` is **not** owner evidence | all 2,706 conflicted VINs (4,201 conflict rows) owned by cleveland-nissan, bmwofmonrovia, fordofmurfreesboro and bentleycadillac carry the owner's own host, because group sites serve sister cars on their own domain | correctness judge, SQL on `cars_cleanroom` |
| Pause artifacts (labelled, not defects) | all 8 in-flight batches killed with rc=-9 after 78,209-78,778 s, because the 3600 s wall-clock timeout (`runner.py:119-122`) counted the SIGSTOP. That left 21 `error / no scan_runs row` dealers and 0 retirements. No conflict is a pause artifact: post-resume claims hit owner rows about 23 h old, inside the 48 h window | retirements RCA |
| Verdicts | ok 240, inaccurate 139, thin 51, error 21, no_rows 7; HTTP 403: 63, 429: 9 | `fleet.log:1215` |
| Verifier false positive | `vpic_fuel` fires on 262 dealers / 3,585 rows; in 261 of them every example is a correct PHEV. 18 inaccurate verdicts rest on it alone | `scan_lab_report.py:184-199` vs `catalog/resolver.py:57-72` |
| dealer.com parser junk | dict repr stored as trim: 1,228 rows at 52 dealers; literal `[[exteriorColor]]`: 882 rows at 60 dealers | `vdp_extras_parse.py:94-96` |
| VDP prefetch | 188,597 pages wanted, 92,282 fetched (48.9%); 140 dealers blocked (59,523 wanted, 365 fetched); 65 report `statuses={}` | triage `http_first` |
| Page cap | 13 replays stopped at `_MAX_REPLAY_PAGES = 40` (`recipes.py:1026`) with nothing logged | `grep 'from 40 page(s)'` |
| Drivetrain vs vPIC | 5,306 active rows at 142 dealers store AWD/4WD where vPIC decodes 2WD; live checks agreed with vPIC 15 to 0 | dealer records |
| Per-dealer analysis | 195 of 458 records exist (s0 0/57, s2 0/58, s7 7/57) | `analysis/dealers/*.json` |
| RCA workflow `wf_ef900653-f48` | wrote no findings (all agents hit the session limit); the tool results were salvaged and re-verified by the 10-15 gather reports | conflicts RCA §1 |
| Code version | likely v1.5.4 (no commits after 10-09); not stamped in the run artifacts | |

### 2.4 Incomplete-listings facts (full recompute under today's code)

| Fact | Number | Source |
|---|---|---|
| Local `cars` (09-28) | 11,719 incomplete of 214,675 active (5.46%) | `merge_verified_specs` + `listing_missing_field_codes` over every active car |
| `cars_cleanroom` | 34,023 of 199,457 (17.06%); 14,345 (7.19%) without the new-car NULL-mileage code | same |
| **F12 regression** | 21,152 active new or blank-condition cars with NULL mileage are flagged `mileage` (10.6% of active); for 20,613 it is the only gap. `_mileage_blank` returns True for None on any condition (`listing_completeness.py:130-132`), and a test pins it (`test_mileage_zero_f12.py:38`) | incomplete-listings report |
| Prod exposure | F12 is an ancestor of the prod scanner image `89c13ad52`. Production hides indexed incomplete cars unless `LISTINGS_INCLUDE_INCOMPLETE_CARS=1` (`data_quality_repo.py:105-122`; `runtime_env.py:16-20` treats Railway as production). Prod may already hide new cars rescanned on 09-29. Unverified (needs the read-only N1.6 census) | |
| Stale index flags from one scan | 4,581 complete cars flagged (body 3,501, fuel 1,584, engine 765, trans 592, drive 202, cyl 47): the per-car sync runs before backfill and the vPIC heal (`post_write.py:62-63`), and `fleet_scan` never rebuilds | |
| New-arrival gray area | cars ≤3 days old: 11.5% incomplete vs about 4.8% baseline; about 12% of incomplete cars and about 25% of price gaps are timing. In a paired test, 77% of the 09-28 cohort's price gaps and 65% of its photo gaps filled later, but only 36% of trim and 32% of color gaps | |
| Junk prices | 234 active at $10-$999 in the fresh scan; 205 cars in 9 repeated-value pairs; 1 at $6,001,323 | |
| Nothing in 1.5.3 or 1.5.4 closes any finding | only P1A.1 and `1344cbd09` touched the related paths after 09-28 | `git log --since=2026-09-27` |
| Temporary scripts | the recompute scripts live only in this session's scratchpad (`.../6ecbc4dc-.../scratchpad/il/`), which is cleared on reboot. Before this session ends they are copied to `workspace/cleanroom_2026_10/analysis/il_scripts/` (gitignored, outside the scratchpad, without `arr.pkl`); N1.3 ports them into the repo from there | |

### 2.5 Prod state

- Web: 1.5.4, deployment `848895c1`. The `*.up.railway.app` origin also serves it.
- Postgres: PG 18.6, pgvector 0.8.6, schema V025. 268,868 active cars. The last scrape was the 09-29 Railway fleet, **16 days ago**.
- `vin_owner_conflicts` on prod: 12,903 rows / 212 pairs, all from 09-29.
- Recipes: 235 rows with `max_saved_at=0`; 31 dealers whose every recipe is auth-staled.
- Backups: the newest prod dump is `workspace/backups/prod_20261004_pre150.dump`. The local `pg_dump` is 17.10 and cannot dump PG18.
- Turnover: about 32% of 09-28 VINs are no longer active in the 10-14 clean room, and 31% of clean-room VINs are new. This is an **upper bound**, because it includes clean-room scan misses. It is never to be used as an expected retirement count.
- Web boot applies pending migrations by default: `scripts/docker-entrypoint-web.sh:21` runs them unless `MIGRATE_ON_BOOT=0` (verified 10-15; one judge said the knob did not exist, but it does). So the prod dump and `migrate --apply` always come before a web deploy that carries a migration. Every web deploy ships all of main at its tag, including code and migrations merged by earlier no-deploy releases (N5, N6, N8, N9), and `RELEASING.md` step 1 compares only against `origin/main`, not against prod. Section 7.0 "Migration check before every web deploy" closes that gap.
- Railway config-as-code (root `railway.toml`, the staged `railway.scanner-nightly.json`) stops working on **2026-12-01**.
- Open security items live on 1.5.4:
  - X-Forwarded-For spoofing through the railway.app origin;
  - CSS injection under `style-src 'unsafe-inline'`;
  - esm.sh in the CSP;
  - no CSRF on operator DELETE routes;
  - CGNAT gap in the SSRF guard;
  - the latent OAuth takeover (OAuth is not configured; `/auth/google` and `/auth/apple` 302 to `/login`);
  - Chromium in a root web image (stopgap `CAR_CHAT_WEB_RESEARCH=0`).

## 3. Ordering rationale

### 3.1 Why this order

1. **The clean room merges the owner's two problems into one.** In-run retirements are 21 rows from one mechanism; the "8.8k" is the 9,610 VIN-owner refusals, i.e. group-feed attribution. So the attribution fix (N8) and the retirement single writer (N9) are equal first priorities. Each is proven by targeted clean-room re-runs, then by a full release-candidate rehearsal (N11), as the owner asked ("prove each fix by re-running the affected dealers in the clean room and showing the counts drop").
2. **N1 finishes the interrupted root-cause work cheaply.** The gather reports already agree, so N1 writes them up instead of redoing them. It analyses per conflict group and per platform, because those are the units the fixes act on. One read-only tool measures all 458 dealers instead of about 120 more per-dealer agents. N1 also removes the two live prod hazards (SCAN_FLEET=1 with the guard lapsed; web variable changes while the GitHub source is set) and collects the owner decisions up front.
3. **N2 is a narrow hotfix.** IL-01 plus a targeted prod index repair, and nothing else. It ships first if the N1.6 census shows prod hiding new cars.
4. **N3 (security wave 1) and N4 (config-as-code) run in parallel from the start.** Their files are disjoint from the scanner work. The XFF spoofing and CSS injection holes are live today, and config-as-code has a hard date.
5. **N5 (Phase 4 migrations) moves onto the critical path.** The judges showed that ownership evidence must be a persisted rooftop tier, not the stored `source_url`. Live prod retirement also needs per-VIN provenance (V027) and two-strike (V028). Those three migrations need `migrate --target` and V026. N5 is 3S 5M and sits on the critical path: N7, N8 and N9 start after it merges (P4.2 `--target`, P4.5 strict mode in `database.py`). N2, N3, N4 and N6 run in parallel with it.
6. **N6 lands the write-path quick wins before any rehearsal.** The verifier PHEV bug distorts every verdict-based metric. The dict-repr trims, the index rebuild and the new-arrival "pending" class change what the first prod fleet shows. N6 also pulls P8A.2 (pure precedence module) and P8A.6 (read-time drivetrain follows vPIC) forward, so v1.6.0 displays the vPIC drivetrain from fleet #1 on (owner rule 4); the write path and the heal stay in N15-N17.
7. **N7 pulls the Postgres harness core forward** (D-PLAN1). Every retirement and guard test is SQLite-only today (`backend/tests/conftest.py:82-92`), and N8/N9 rewrite exactly the Postgres-specific SQL: the guard backstop at `upsert/sql.py:150-155` compares `scraped_at` as text.
8. **N10 lands the scan controls a cron and a second host need:** run window, fleet lock, leases, per-dealer locks (P11A.1-P11A.4 pulled forward), kill switches, recipe hygiene, the pre-fleet check and the audit tool.
9. **N11 rehearses the exact release candidate:**
   - an empty-DB pass for counts and accounting;
   - a reversed-order pass on the same commit for determinism;
   - a convergence pass on a copy of the populated clean room, which mirrors prod's lapsed-guard first run;
   - a fourth pass at least 20 h later, so two-strike retirement really retires and can be live-checked.
   Then v1.6.0 is tagged.
10. **N12 refreshes prod without wiping it:**
    - migrate V027-V029 and deploy idle;
    - repair prod recipes from the home IP;
    - run a home verification scan and a 20-dealer smoke;
    - fleet #1 writes first strikes only, and the strike list is live-checked;
    - fleet #2 retires;
    - residue repair, then the nightly cron, watched for three nights.
11. **After prod is fresh:** recipe-store CAS before the permanent home lane, then precedence on the write path (drivetrain vs vPIC; the read-time display ships in N6), fetch policy and prefetch coverage, recipe validation, the remaining incomplete-listings work, dictionary, EPA, and finally consolidation and hygiene.

### 3.2 Parallel lanes and critical path

```
Day:    0     3     6     9     12    15    18    21    24    27    30    33    36
N1      ██████
N2          ████                                       (hotfix; web deploy before N9 merges)
N3            ██████████                               (web deploy before N9 merges)
N4            ██████████                               (proven by 11-20)
N5            ████████                                 (prod V026 owner apply)
N6              ██████████
N7                    ████████
N9                    ██████████████                   (incl. N9.14 targeted re-run)
N8                    ████████······██████████         (N8.6 after N9 release and N7)
N10                   ████████················██████   (wave 3 after N8 release)
N11                                                 ██████████    (incl. a ≥20 h wait)
N12                                                           ████████████████ (fleet #1, #2, 3 nights)
```

**Critical path:** N1 → N5 (P4.2 `--target`, P4.5) → N9 (release with V027/V028, after the N9.14 targeted re-run) → N8.6 (V029, evidence-aware guard; also needs N7's P14A.2 and N7.9) → N8 release → N10 wave 3 (P11A.3, P6A.3', N10.8) → N11 → N12.

**Rough estimate:** prod fleet #1 around day 29-31; the cron watched for three nights around day 33-35. This is an estimate, not a commitment. Owner availability for decisions and prod steps is the main variable.

### 3.3 File ownership in the first window (N2-N10)

A unit that touches a file owned by a concurrent phase waits until that phase's unit has merged to main. Releases are serialized: each phase rebases on `origin/main`, bumps VERSION and fast-forwards.

| File or area | Order of ownership |
|---|---|
| `backend/utils/listing_completeness.py` | N2.1 → N6.5 / N6.7 / N6.8 (serial, Wave A) → N6.4 |
| car and dealer templates (mileage line, "last checked" line, "details coming" line) | N2.1 → N3.11 → N6.4 |
| `backend/scripts/fleet_scan.py` | N6.1 (post-step block `:354-362`) → N9.12 (summary `:383`) → N9.15 (resume) → P6A.2 (entry, window, lock) → N10.8 (shard order) |
| `backend/scripts/dealer_pipeline.py`, `backend/scanner/pipeline/run.py` (roster filter for resume) | N9.15 → P11A.3 (N10) |
| `backend/scanner/database.py` | P4.5 (N5) → P5A.5 (N9; `drop_unattributable_vehicles` `:335` returns the VINs it dropped) → N8.6 (guard `:83-101`, the `:43-46` comment) → P6A.3' (N10) |
| `backend/vehicle_facts/precedence.py` (new), `backend/enrichment/verified_specs/drivetrain.py` | P8A.2 → P8A.6 (both N6) → P8A.3 and later (N15) |
| `backend/scanner/upsert/{guard,sql,write}.py` | N8.6 → N8.14 (`sql.py:60-61,91`, shared-host registry id) (P8B.1 later, in N16) |
| `backend/scanner/upsert/rows.py` | N6.7 |
| `backend/scanner/recipes.py` | P5A.2 → P5B.1/P5B.2 (N9) → P6A.6 (N10) |
| `backend/scanner/inventory_reconcile.py`, `phases/dealer_run_steps/after_write.py`, `delta_scan.py` | N9 (P5A.4 → P5A.5 → P5A.6 → P5B.3 → P5B.4) → N8.14 (`delta_scan.py:181,197-199`; N8 already waits for N9 on main) → P11A.3 (N10, `delta_scan.py`) |
| `backend/scanner/pipeline/{assess,triage,dealer_logs}.py` | N9.12 → P5A.7 (`assess`, `triage`) → P11A.3 (N10) |
| `backend/scanner/pipeline/{lifecycle,runner,reconcile}.py` | N9.13 → P5A.7 (both N9) → P11A.3 (N10) |
| `backend/scanner/pipeline/constants.py` | N6.9 → P5A.7 |
| `backend/scanner/phases/dealer_run.py` | N8.7 → P11A.3 |
| `backend/scanner/vdp/prefetch.py` | N8.7 (later N18) |
| `backend/scanner/utils/vdp_extras_parse.py` | N6.5 |
| `backend/scripts/scan_lab_report.py` | N6.6 |
| `backend/parsers/base.py`, `backend/parsers/dealer_eprocess.py` | N6.8, N6.9 → N8.11 (`dealer_eprocess.py:170-195`, conditional) |
| `backend/dealer/admin/incomplete_listings_api.py`, `backend/db/repositories/cars_repo.py` (admin delete paths) | P5B.3 (N9) |
| `backend/parsers/{__init__,jazel,chapman,motive_ridemotive,oneaudi,generic_json}.py`, `scanner/scrapers/dealer_on.py`, `scanner/synth/platforms/{motive,carscommerce_scope}.py`, `backend/attribution/{match,rooftop,place,gate}.py` | N8 |
| `backend/attribution/disown.py` | P5A.4 (switch only, N9) → N8.6 (`:33`, only if `third_rooftop` joins `EVIDENCE_BACKED_REJECTS`) |
| `backend/db/incomplete_listings_db.py`, `backend/db/repositories/data_quality_repo.py` | N6.1 → N6.4 → P5A.4 (index delete on retire) |
| `backend/db/repositories/dealers_repo.py` (summary whitelist) | P5A.2 → N9.12 |
| `migrations/`, `backend/scripts/migrate.py`, `backend/db/{schema_version,inventory_pg}.py`, `backend/scanner/recipe_store.py`, `backend/db/repositories/schema_repo.py` | N5 → P5B.3/P5B.4 (V027/V028) → N8.6 (V029) |
| `backend/tests/conftest.py`, `.github/workflows/ci.yml` | N7 |
| `backend/web/*`, `users_db`, OAuth modules, `client_ip.py`, `outbound_url.py`, templates, `Dockerfile.web`, `requirements.txt`, `backend/dealer/admin/dealers_hub.py`, `deploy/railway/sync-vault-to-railway.sh`, `deploy/railway/README.md` | N3 |
| root `railway.toml`, `deploy/railway/deploy_*.sh`, `railway.scanner-nightly.json`, RELEASING.md deploy sections, worker/scheduler Dockerfiles, `docker-compose.yml` | N4 |

### 3.4 Fatal flaws named by the judges and how this plan handles them

| Flaw | Handling here |
|---|---|
| Ownership judged from the stored `source_url` host (value-first guard v1) | Ownership evidence is a persisted rooftop tier (V029, N8.6), set only from stamped per-car rooftop data. The VDP host is used only to **refuse** a row whose host belongs to another roster store (`vdp_host_other_store`, N8.5) |
| Tie-break "VDP host = dealer host" favours the wrong owner | Not used. Evidence-less contests follow the owner's pool policy (D-N3, a tracked `group_pools.json`) |
| Two-strike makes the pass-2 retirement proof vacuous | N11 runs four passes, so pass D on the populated copy produces real second-strike retirements; the pass-C strike list is live-checked too. In prod, P6B.3 and P6B.4 run with `SCANNER_RECONCILE=dry`, so no strike predates fleet #1, and fleet #1's strike list is live-checked before fleet #2 retires |
| 32% turnover used as an expected retirement count; 10% per-dealer alarm vs legitimate turnover after a long gap | The 32% is labelled an upper bound and never a target. The first live prod retirement uses a gap-aware alarm (D-N21); nightly runs keep 10% |
| Live prod retirement without per-VIN provenance | V027 `listing_retirements` and V028 two-strike are applied to prod (N12.2) before fleet #1. No retirement reaches prod without a provenance row |
| "Web source blocks every push to main" | Corrected. The gate applies only to web variable changes; pushes and `deploy_web.sh` uploads are not gated |
| Home lane before P11A.3 per-dealer locks | P11A.1-P11A.4 land in N10, before the first prod write and before the N12 home-IP bridge; the permanent lane (N14) comes after |
| Determinism checked across code versions, or before the evidence guard exists | N11.4 runs forward and reversed passes (`SCAN_SHARD_ORDER=reverse`, N10.8) on the same commit, after N8.6 has merged. N8.10 reports determinism as information only |
| Wrong dependency reference (`N4.9`) | Every dependency in this plan names an existing unit |
| One owner session bundling about 9 prod changes | Every prod step is its own ordered unit with its own verification before the next |
| Manual bulk reset of prod ownership on estimated classes | No bulk reset (D-N22). The evidence rules re-file ownership during the fleet, with snapshots before and after and every move logged in `car_move_log` |
| Phase 7 deferred while two hosts write recipes | Phase 7 stays (N13) and lands before the permanent home lane (N14). The interim bridge runs outside the Railway window, under per-dealer locks |
| Mutating the `cars_cleanroom` baseline | `cars_cleanroom` is frozen at V025 (read-only baseline). Re-runs use new DBs or `createdb -T` copies. The clean-room run files (`cleanroom.env:41-42,53-57,95`, `preflight.sh:19,43-50`, `isolation_snapshot.sh:6`) hard-code `cars_cleanroom` and its folders, so N1.3 replaces them with parameterized `scripts/cleanroom/{run.env.template,preflight.sh,isolation_snapshot.sh}` that refuse `cars_cleanroom` and `cars` as write targets and put every re-run under `workspace/cleanroom_runs/<db>/` |
| The SCAN_FLEET disarm accepted with a "no-touch rule" fallback | N1.5 requires a verified `SCAN_FLEET=0`; no fallback |
| A 20 h wait inside one workflow | N11 ends its workflow before every ≥20 h wait and resumes in a new session from RESUME.md |
| Config-as-code coupled to the V026 release | Config-as-code is its own phase (N4), with a carve-out date of 2026-11-20 |
| "MIGRATE_ON_BOOT does not exist" (judge claim) | Rejected after checking: `scripts/docker-entrypoint-web.sh:21` uses it, default on. The plan keeps "dump and migrate before the web deploy" |

## 4. Phase overview

Sizes: S = up to half a day of agent work, M = 1-3 days, L = 3-5 days. "Touches prod" means the phase contains an owner-run prod step.

| ID | Title | Goal | Depends | Parallel with | Owner input | Touches prod | Size |
|---|---|---|---|---|---|---|---|
| N1 | Close the clean-room RCA, disarm prod, acceptance tool, decisions | RCA doc with the corrected premise; per-group and per-platform evidence; one read-only acceptance tool; SCAN_FLEET=0 verified; decisions logged | none | N2, N3, N4 (after N1.8 gating minimum) | Hazard pack, read-only census, ruleset, decision brief | Yes (variable change with `--skip-deploys`, reads) | M |
| N2 | Prod visibility hotfix | Stop hiding new cars with no odometer (21,152 in a fresh scan) | N1.6, D-N12 | N3, N4, N5 | Docker smoke, deploy, targeted index repair | Yes (web deploy, index repair) | S |
| N3 | Security wave 1 and web stopgaps | Close XFF spoofing, CSS injection, esm.sh, CSRF, SSRF, latent OAuth; Chromium out of web; lxml advisory | N1 | N2, N4, N5 | D-SEC6, Docker smoke, env steps after web-source-clear | Yes (web deploy, env) | M |
| N4 | Railway config-as-code and ops loose ends | Deploy and cron path survives 2026-12-01; worker files deleted; mini settled | N1.5 | N2, N3, N5-N7 | Config mechanism, service settings, mini | Yes (service settings) | M |
| N5 | Migration foundation (Phase 4) | `migrate --target`, advisory lock, V026, strict mode | N1 | N2-N4, N6 (N7, N8 and N9 start after it merges) | D-DB5/6/7, D-PR8, prod V026 apply | Yes (V026, web deploy) | M |
| N6 | Listing write-path and completeness fixes | Index rebuild, new-arrival "pending", verifier PHEV, dict/token junk, price floor, placeholders, VIN length gate, read-time vPIC drivetrain (P8A.2, P8A.6) | N1, N2.1 | N5, N7, N8, N9 | D-N13..D-N17, D-PR1/4/6/7 | No (its web-facing changes ship with the next web deploy from a later tag, Section 7.0) | M |
| N7 | Postgres harness core | Real-Postgres tests for guard, retirement and concurrency SQL | N5 | N6, N8, N9 | D-TC3/6/9, D-PLAN1, ruleset contexts | No | M |
| N8 | Attribution fix wave | Per-car rooftop identity, persisted evidence tier (V029), evidence-aware VIN ownership, recipe identity, repair set | N1, N5; N9 release and N7 (P14A.2, N7.9) before N8.6 | N6, N7, N9, N10 wave 1 | D-N3..D-N8, D-N34, repair-set OK | No | L |
| N9 | Retirement single writer, provenance, two-strike, accounting, pause safety | One fail-closed policy; V027/V028; triage counts captured/refused/written; pause-proof batches; fleet resume; targeted retirement re-run | N1, N5, N6.9 | N6, N7, N8, N10 wave 1 | D-RC1/2/3/7, D-SR4, D-N10/11/21 | No | L |
| N10 | Scan controls and pre-fleet tooling | Run window, leases, per-dealer locks, kill switches, recipe hygiene, gap cover, shard-order knob, pre-fleet check, audit tool | N5; N8, N9 for wave 3 | N8, N9 (wave 1) | D-IF5, D-IF10, D-SR2 | No | M |
| N11 | Release-candidate rehearsal, v1.6.0 | Prove counts, determinism, convergence and retirement on the exact candidate | N6, N8, N9, N10 | none on the scanner | Fleet runs, restore, live checks, sign-off | No | M |
| N12 | Prod refresh (6B) | Fresh prod: V027-V029, idle deploy, recipe repair, smoke, fleet #1 (strikes), fleet #2 (retire), residue repair, cron | N11, N4, N5 | N3 release, N13 code | Every prod step, go/no-go gates | Yes | L |
| N13 | Recipe store authority (Phase 7) | CAS so the home lane and Railway cannot clobber recipes | N12 | N15, N17-N20 code | D-RS1..D-RS4, V030 | Yes | L |
| N14 | Home-IP lane (Phase 15A) | Permanent nightly lane for the Railway-blocked dealers; D-IF6 by desk review | N12, N13 | N15-N18 | D-IF1/3/6, `home_lane.env` | Yes | M |
| N15 | Fact precedence foundation (8A) | vPIC decides drivetrain and electrification on every write (read-time display shipped in N6); Gasoline fill | N12, N6 (P8A.2, P8A.6) | N13, N14, N18, N20 | none new (D-PR1/4/6/7 answered for N6) | Yes (deploy) | M |
| N16 | Every fact writer through precedence (8B) | No writer reverts a higher-ranked fact; body style and HD tables | N15 | N13, N14, N18 | D-PR2/3/5, D-N32 | Yes (deploy) | L |
| N17 | Precedence close-out and prod heal (8C) | Legacy writers retired; prod facts healed | N16 | N18, N20 | prod heal window | Yes | M |
| N18 | One fetch policy and VDP prefetch coverage (12A+) | Unblock VDP enrichment; gap-first; stop guessed URLs | N12, N13 | N15-N17 | D-OD3/7/8/13, burst approval | Yes (deploy) | L |
| N19 | One recipe validation rule (12B) | Recipes judged by the replay walker; rescan under-collected dealers | N18, N10 | N21-N25 | D-OD1/2/4/5/6 | Yes | L |
| N20 | Dictionary lookups in prod (Phase 9) | 54 of 300 groups and 128 wrong-model YMMs fixed | N5 | any phase not deploying web the same day | D-HY1, D-HY9 | Yes (web deploy) | M |
| N21 | Incomplete-listings remainder and discovery batch | Price meaning, pseudo-VINs, condition labels, galleries, Gardena, outliers | N16, N18 | N22-N25 | D-N17b, D-N18, D-N31, D-N33 | Yes | L |
| N22 | EPA catalog code (10A) | Induction vocabulary, permanent key, V031, importer | N9 release, N17 | N21 | D-EPA1, D-ET2/3/5, D-ER2 | Yes (V031) | L |
| N23 | EPA builder, backfill, re-resolve, local (10B) | Clean local catalog with provenance | N22 | N21 | D-ER1, D-ER3 | No | M |
| N24 | Forced-induction resolution (10C) | FI labels from evidence through precedence | N23, N16 | N21 | D-ET2, D-ET4 | Yes (deploy) | L |
| N25 | EPA data repairs and prod rollout (10D) | Patched CSVs; staged prod rollout; V032 | N24 | N26-N28 | D-ER4/5/6 | Yes | L |
| N26 | Scanner runtime remainder and upsert cost (11A rest, 11B gated) | Upsert telemetry, retries; 11B only if the trigger fires; V033 | N25 | N27, N31 | D-SR6 | Yes (deploy, V033) | M |
| N27 | DB layer (13A) | Loud adapter, DDL guard, strict mode everywhere | N12 | N26, N31 | D-DB3/4/8 | Yes (env) | M |
| N28 | Legacy DDL deletion, backup drops, scratch DB retirement (13B) | Chain is the only DDL source | N25, N27 (strict ≥7 days) | N31, N32 | D-DB1 | Yes | M |
| N29 | Dependencies and images (15B rest) | Per-image requirements, CPU torch, pip-audit in CI | N3 | N26-N28 | D-SEC7, Docker OK | Yes (deploy) | M |
| N30 | Security wave 2 (Phase 16) | Session revocation, non-root web, scanner SSRF, CSP styles | N29 | N31, N32 | D-SEC2/3, Resend key | Yes | L |
| N31 | Postgres test breadth (14B, 14C) | Remaining PG coverage, corpus check, skip budget | N7 + 30 days green | N26-N30 | D-TC4, D-TC8 | No | L |
| N32 | Shims and dead functions (17A) | Delete alias shims and unreferenced code | N1.7, N19 | N30, N31 | none | No | M |
| N33 | Browser path retirement (17B) | No browser code on scan paths | N32, N18 | N31 | D-DC1/5/6/7/9 | Yes (deploy) | L |
| N34 | Module splits and knob registry (17C) | Registry, ScanConfig | N33 | N35 | D-OD9/10/11 | Yes (env cleanup) | L |
| N35 | Repo hygiene (18A) | Junk, nightly, k8s, ruff | N14, N34 | none | D-HY3/4, D-IF9, D-TC7 | No | M |
| N36 | Docs close-out (18B) and plan closure | Docs current; every unit DONE, DROPPED or DEFERRED | N35 | none | D-HY6/7, CLAUDE.md wording | No | S |

## 5. Owner decisions

How to answer: reply "accept all recommendations except …" for a phase group. A phase does not start a unit whose blocking decision is unanswered; that unit is marked BLOCKED and the rest continue.

**Gating minimum before N8, N9 and N11 can start:** D-N1, D-N2, D-N3, D-N4, D-N25, D-RC1, D-RC2. Everything else can be answered as its phase approaches.

**Already answered (not re-asked; see `OWNER_DECISIONS_LOG.md`):** D-REL1-D-REL10, D-IF7, D-RS5 (keep writing; P1B.5 dropped), D-SR7, D-TC5, D-SEC1 (a), D-SEC4 (a), "Web GitHub source: owner disconnects", "Mini SSH: not now", "P1C.4 overlapping recipes (7 dealers): keep both now; clean up in Phase 7" (10-08; D-N34 asks whether to move that clean-up earlier). **Answered 2026-10-16 (gating minimum):** D-N25, D-N9 (C+D), D-N1, D-N2 (b), D-N3, D-RC1 (A), D-RC2 (two-strike ≥20 h) as recommended; **D-N4 changed by the owner** (profile and scan missing rooftops; see its row).

**Answered 2026-10-18 (owner reply "okacceptall"):** every decision still open in `docs/remediation/DECISION_BRIEF_2026_10.md` is accepted as recommended there, D-N6 in the brief's changed form (keep mbbeverlyhills-com on CarsCommerce `listings/6074385`; drop fletcherjones-com as a store after N8.9 re-files its rows). Each answer has its own row in `OWNER_DECISIONS_LOG.md`.

**No answer needed:** D-RC4 (policy default "nothing"), D-DC10 (keep facades), D-OD12 (deferred enrichment follow-up), D-HY8 (status quo). D-HY2 is an owner action only (Section 6).

| ID | Question | Options | Recommendation | Needed by |
|---|---|---|---|---|
| **N1** | | | | |
| D-N25 | Approvals bundle through the end of N12 (about 5 weeks) | per item | Approve: (1) paced read-only live fetches from the home IP (≤3 pages per dealer for group dossiers, ≤30 requests per host for platform field maps, ≥2 s apart, no browser); (2) these local scans, each into the named scratch DB only: N1.3's 2-dealer smoke (`cars_cr_test`), N6.2's 3-dealer scan (`cars_cr_n6`), the N8 per-unit mini re-runs of 3-10 dealers (`cars_cr_n8_<unit>`), N8.10 (`cars_cr_n8`), N9.14 (`cars_cr_n9`), N11 passes A-D (`cars_cr_rc`, `cars_cr_rc_b`, `cars_cr_conv`), N11.13 (the 103 prod-only dealers into `cars_cr_rc`), the P5A.8 shadow (`cars_rehearsal`, `SCANNER_RECONCILE=dry`); (3) the P5B.8 rescans into `cars`; (4) one multi-GB restore of `cars` into `cars_rehearsal`. Later scans (the N15 re-run of the 142 dealers, the N18 20-dealer sample, others) are approved per phase in the Section 10 prompt's approvals line. Docker builds stay a per-run ask (the web and scanner-nightly smoke of the exact stage for each release), unless the owner explicitly accepts a blanket Docker approval here | N1.2 (N1.3's smoke for item 2) |
| D-N9 | What happens to the 263 dealers with no analysis record | (A) finish all, about 120 agents; (B) non-ok only, about 54; (C) signal-driven; (D) fix first, then analyse the re-run | C+D: group dossiers now (N1.2), mechanical audit of all 458 now (N1.3), the 46 signal dealers on rehearsal data (N11.10); the 8 pause-killed dealers rescanned, not analysed | N1.2 |
| D-N1 | Which local DBs serve as baseline, rehearsal and repair DBs (NEW-3) | keep or drop `cars_cleanroom`; which DB P5B.8 targets | `cars_cleanroom` frozen at V025 as the read-only baseline (entered in the schema ledger as "frozen, never migrated, never scanned into"); new DBs `cars_cr_<phase>[_x]` for re-runs (built by `build_db.sh` or `createdb -T cars_cleanroom`, then migrated); `cars` stays the prod-shaped repair rehearsal DB (P5B.8, P8A.7, P8C.6, 10B, 10D); scratch DBs dropped with owner OK in N28 | N5, N8 |
| D-N2 | The mini's role (LE-7) | (a) approve SSH and repair it (P0A.5, P0A.2, P1C.1, P1C.4, hooksPath); (b) no scanning role for now | (b). The MBP is the home host and the canonical log host. This implies D-DB5 (b), D-IF10 = MBP and the D-IF1 host = MBP. The mini returns only through N4.4 after SSH approval and the P1C repairs | N4, N5 |
| D-N19 | Show "Inventory last checked <date>" while prod is stale? | yes / no | **ANSWERED 2026-10-18 (accept all, as recommended):** Yes: one plain text line per dealer and car page, hidden once data is under 48 h old; no card or box container | N3 |
| D-N29 | Accept the conditional tail units (P11B.2-P11B.4, P15B.8, P16.3, 14B/14C, 17C, ruff P18A.9-P18A.11) with revival triggers | accept / keep unconditional | **ANSWERED 2026-10-18 (accept all, as recommended):** Accept (Section 8 lists each trigger) | N26 |
| **N2** | | | | |
| D-N12 | IL-01: a New, non-CPO car with no odometer value | complete or missing; page wording | **ANSWERED 2026-10-18 (accept all, as recommended):** Complete. Show no mileage line on New cars; keep "Mileage not listed" for used cars | N2.1 |
| **N3** | | | | |
| D-SEC6 | Direct-to-origin XFF spoofing | (a) `TRUSTED_PROXY_CIDRS=cloudflare`; (b) remove the `*.up.railway.app` domain; (c) both | **ANSWERED 2026-10-18 (accept all, as recommended):** (c) | N3.13 |
| D-SEC5 | Count as applied? | applied / open | **ANSWERED 2026-10-18 (accept all, as recommended):** Applied in effect (pass-through deleted 10-08). `DEV_IP_ALLOWLIST` stays unset until P3.2 ships and the CIDRs are live | N3.13 |
| D-DEP1 | Remove orphan dependencies, lxml ≥6.1 | (a) / no | **ANSWERED 2026-10-18 (accept all, as recommended):** (a) | P15B.1 |
| **N4** | | | | |
| D-N24 | Replacement for Railway config-as-code | Railway's supported post-12-01 mechanism / service settings via GraphQL | **ANSWERED 2026-10-18 (accept all, as recommended):** Whichever Railway documents as supported after 12-01; N4.1 verifies before building | N4.1 |
| **N5** | | | | |
| D-DB5 | Is the mini's Postgres a live inventory DB? | (a) live / (b) retired | **SETTLED 2026-10-18 by D-N2 (b):** (b) (follows D-N2) | P4.8 |
| D-DB6 | How migrations reach prod | (a) manual, before the deploy / (b) at web boot | **ANSWERED 2026-10-18 (accept all, as recommended):** (a) | P4.8 and every migration |
| D-DB7 | Out-of-chain objects | (a) adopt / (b) drop; NULLS NOT DISTINCT | **ANSWERED 2026-10-18 (accept all, as recommended):** (a) adopt; NULLS NOT DISTINCT is fine on PG 17/18 | P4.3 |
| D-PR8 | Haiku fallback and `haiku_spec_cache` | (a)-(d) | **ANSWERED 2026-10-18 (accept all, as recommended):** (a): never writes fact columns, opt-in, keep the table | P4.3 |
| D-N30 | The 4 untracked log tables (`image_summary_restore_log`, `make_canon_log`, `model_canon_log`, `package_value_purge_log`) | adopt in V026 / drop now | **ANSWERED 2026-10-18 (accept all, as recommended):** Adopt in V026; export and drop in P13B.4 | P4.3 |
| D-PLAN1 | Pull the Postgres harness core forward | yes / keep the order | **ANSWERED 2026-10-18 (accept all, as recommended):** Yes (N7, right after N5) | N7 |
| **N6** | | | | |
| D-N13 | New-arrival window | N days; display; hide or show | **ANSWERED 2026-10-18 (accept all, as recommended):** 3 days, for price and photo gaps only; show a plain "details coming" line; never hide a car inside the window. A car is inside the window when (its latest `scraped_at` − the gap's `first_seen_at`) ≤ N days, measured against the row's latest scrape, not the wall clock | N6.4 |
| D-N14 | Price floor | value; repeated-value rule | **ANSWERED 2026-10-18 (accept all, as recommended):** $1,000. A value repeated across ≥5 models at one dealer is withheld (NULL plus a provenance note) | N6.7 |
| D-N15 | Do placeholder-only images count as photos? | yes / no | **ANSWERED 2026-10-18 (accept all, as recommended):** No: placeholder-only counts as "no photo". "No photo" counts as incomplete only outside the D-N13 window | N6.8 |
| D-N16 | F4: MY2026-27 cars filed as used at 0 mi (497 fresh) | exempt / relabel / flag | **ANSWERED 2026-10-18 (accept all, as recommended):** Report as a data question; do not count as incomplete | N6.9 |
| D-N17 | Identifier gate at write time | refuse length ≠ 17; what to do with 17-char I/O/Q pre-assignment IDs (1,251 rows) | **ANSWERED 2026-10-18 (accept all, as recommended):** Refuse length ≠ 17 now (order numbers, the Koons 8-character codes). I/O/Q rows are decided in N21 (D-N17b) | N6.9 |
| D-PR1 | Sticker vs vPIC | (a) / (b) / (c) | **ANSWERED 2026-10-18 (accept all, as recommended):** (a) | P8A.2 (N6) |
| D-PR4 | Drivetrain catalog tiebreak scope | (a) / (b) / (c) | **ANSWERED 2026-10-18 (accept all, as recommended):** (a) | P8A.2 (N6) |
| D-PR6 | Several listing signals contradict vPIC on drive | vPIC wins + cannot_assess / silent / dealer wins | **ANSWERED 2026-10-18 (accept all, as recommended):** vPIC wins, row marked cannot_assess | P8A.2 (N6) |
| D-PR7 | Write-time vPIC cylinders | only when not contradicting / unconditional / both | **ANSWERED 2026-10-18 (accept all, as recommended):** Only when not contradicting | P8A.2 (N6) |
| **N7** | | | | |
| D-TC3 | CI Postgres image | match prod / pg17 / postgres:16 | **ANSWERED 2026-10-18 (accept all, as recommended):** Match prod: pg18 with pgvector 0.8.x | P14A.3 |
| D-TC6 | `integration` marker and job | replace with `pytest-pg` / keep / fold | **ANSWERED 2026-10-18 (accept all, as recommended):** Replace the job, keep the marker | P14A.3 |
| D-TC9 | May tests create `pytest_*` DBs on the local server that holds `cars`? | guarded / separate instance / CI only | **ANSWERED 2026-10-18 (accept all, as recommended):** Yes, guarded (refuses `cars`, `cars_cleanroom`, non-local hosts, port 15432) | P14A.1 |
| **N8** | | | | |
| D-N3 | Ownership of shared group inventory (CF-6, WI-5, W02) | VDP-host store / designated primary / refuse pool writes when an evidenced sister claims / multi-store listing schema | (1) A store whose own feed stamps the car at a strong tier owns it. (2) Otherwise the designated primary in a tracked `backend/config/group_pools.json` owns it, with `dealer_feed_scope='group'` so the site shows "listed by N group stores; location unconfirmed". (3) Pairs not in the file keep their current owner and get flagged for the file. The multi-store schema is deferred | N8.6 |
| D-N4 | Rooftops missing from the roster (CF-12): Mtn View Dalton/Bristol, Bentley Chevrolet of Florence, Sonic Honda/Mercedes stores, Chapman Hyundai/VW, Sam Pack Carrollton/Lewisville/Grapevine, autosavvy's other rooftops (22 rooftops in 9 states filed under San Antonio), the non-INFINITI makes in infinitiofcharlotte's multi-brand pool | add to roster / refuse their cars | **ANSWERED 2026-10-16, owner override:** "we should build a profile for these and scan them when we see cases like this." Each missing rooftop becomes its own dealer (full NETWORK_SCAN_PROCESS loop, five dealer_logs files, roster entry) so its cars are scanned and filed under it: first batch N1.11, standing detector and queue N8.13. Until a rooftop's profile exists its cars are held (refused at the gate as `third_rooftop`, rooftop recorded), never filed under a sister store. **Scope ANSWERED 2026-10-18:** "yes they should be added": D-N4 also covers rooftops outside the 22 roster states (UT, NM, ID, LA, KY, WV so far), which get profiles and roster entries the same way (N1.11 batch 3; N8.13 queues them too); site readiness for the new states is checked in batch 3 | N1.11, N8.1, N8.9, N8.13 |
| D-N5 | Do Hendrick `RHendrickUsed` cars at Terry Labonte's street address count as Terry's (RET-9)? | yes / no | **ANSWERED 2026-10-18 (accept all, as recommended):** Yes | N8.8 |
| D-N6 | Scope of `mbbeverlyhills-com` and `fletcherjones-com` | keep / drop | **ANSWERED 2026-10-18 (accept all), brief's changed recommendation:** keep mbbeverlyhills-com, recipe pointed at CarsCommerce `listings/6074385` shipped with the parser fix that reads the store label; drop fletcherjones-com as a store: N8.9 first moves its rows to the stores they name (Henderson now, the Las Vegas stores once N1.11 has profiled them), then it leaves the scan roster. Plan's original: Keep both: re-discover the real MB Beverly Hills endpoint; scope fletcherjones to its own rooftop (no Henderson cars) | N8.9 |
| D-N7 | May `vdp_host_other_store` also un-list cars? | refuse the write only / also un-list | **ANSWERED 2026-10-18 (accept all, as recommended):** Refuse the write only, until N11 proves it | N8.5 |
| D-N8 | Role of `dealer_feed_scope` (CF-15) | scanner evidence / display only | **ANSWERED 2026-10-18 (accept all, as recommended):** Display only; refreshed from the `group_feed_unstamped` flag with owner OK | N9.12 |
| D-N34 | Move the LE-14 clean-up of the 7 overlapping-recipe dealers from Phase 7 (N13, as answered on 10-08) to N8.9, before the first prod fleet, keeping one recipe set per dealer and archiving the other in the repair file? | yes / no | **ANSWERED 2026-10-18 (accept all, as recommended):** Yes. If declined, the bullet moves back to N13 | N8.9 |
| **N9** | | | | |
| D-RC1 | Which layer writes retirements | A scanner / B pipeline / C both | A | P5A.4 |
| D-RC2 | One miss or two strikes | single / two-strike / three | Two-strike, ≥20 h apart | P5B.4 |
| D-RC3 | How far a replay walk goes | 40 and flag / to the payload total, cap 150 / no cap | **ANSWERED 2026-10-18 (accept all, as recommended):** To the total, cap 150 | P5B.2 |
| D-RC7 | Add `listing_retirements` | yes / summary_json only | **ANSWERED 2026-10-18 (accept all, as recommended):** Yes | P5B.3 |
| D-SR4 | Verdict for a truncated replay | (a) thin with `replay_truncated:` / (b) new verdict / (c) note | **ANSWERED 2026-10-18 (accept all, as recommended):** (a) | P5A.7 |
| D-N10 | Does a first pass with verdict `no_rows` write its rows? | write / don't write | **ANSWERED 2026-10-18 (accept all, as recommended):** Write; the prior-window rule and two-strike protect them | P5A.1 |
| D-N11 | Accept that failing closed keeps sold cars listed for one or two more runs (Terry's 5 sold cars would have waited) | yes / no | **ANSWERED 2026-10-18 (accept all, as recommended):** Yes | P5A.1 |
| D-N21 | Alarm threshold for the first live prod retirement | fixed 10% / gap-aware | **ANSWERED 2026-10-18 (accept all, as recommended):** Gap-aware for N12.8 (and the N11.7 rehearsal): alarm when a dealer retires more than min(60%, 10% + 1.5% × days) of its prior active rows, where days = run start − the median `scraped_at` of the rows being retired (on prod this equals the days since the dealer's last scrape before fleet #1, 09-29). Nightly runs keep 10% | P5A.7, N11.7, N12.8 |
| **N10** | | | | |
| D-IF5 | How the VIN guard survives scan gaps | (a) auto-extend / (b) env / (c) 72 h / (d) refuse known pairs | **ANSWERED 2026-10-18 (accept all, as recommended):** Superseded: keep the 48 h default; gap cover applies only to owners with a persisted strong tier; no blanket 72 h; known pairs are handled by D-N3's `group_pools.json` | P6A.3' |
| D-IF10 | Canonical `dealer_logs` tree | mini / MBP / both | **SETTLED 2026-10-18 by D-N2 (b):** MBP (follows D-N2) | P6A.5 |
| D-SR2 | Cross-host exclusion of a dealer | (a) / (b) / (c) | **ANSWERED 2026-10-18 (accept all, as recommended):** (c): per-dealer Postgres advisory lock, plus the file lock as hygiene | P11A.3 |
| **N11** | | | | |
| D-RC5 | Wrong-retirement candidates that cannot be verified live | leave / reactivate all / rescan first | **ANSWERED 2026-10-18 (accept all, as recommended):** Rescan first; restore only verified-live residue | P5B.8 |
| D-RC6 | Repair order | local then prod / prod only / local only | **ANSWERED 2026-10-18 (accept all, as recommended):** Local, then prod | P5B.8 |
| **N12** | | | | |
| D-IF4 | Nightly parameters | 8×6 at `0 9 * * *`, 14 GB alarm / batch 4 / investigate | **ANSWERED 2026-10-18 (accept all, as recommended):** 8×6 at `0 9 * * *`, 14 GB alarm | N12.7, N12.11 |
| D-N22 | Prod ownership before scanning resumes (CF-14) | manual reset of classified pairs / let the evidence rules re-file | **ANSWERED 2026-10-18 (accept all, as recommended):** Let the rules re-file during fleet #1, with snapshots and `car_move_log`. A manual reset only for pairs verified by DB checks, if any remain after fleet #2 | N12.7 |
| D-N23 | Guard hours for fleet #1 | default 48 h / extended | **ANSWERED 2026-10-18 (accept all, as recommended):** Default 48 h. The lapsed guard makes fleet #1 behave like the N11 convergence pass, which is what was proven | N12.7 |
| D-IF1 | How the 31 dealers that answer only from a home IP are scanned | (a) home lane / (b) paid proxy / (c) accept loss | **ANSWERED 2026-10-18 (accept all, as recommended):** Interim (N12): the owner runs a bridge from the MBP every ≤3 days. Final (N14): (a), on the MBP | N12.10, N14 |
| **N13** | | | | |
| D-RS1 | Canonical recipe store | (a) prod for all / (b) two stores plus reconcile / (c) independent | **ANSWERED 2026-10-18 (accept all, as recommended):** (b) now | P7.9 |
| D-RS2 | What re-synthesis does to old recipes | replace / replace-with-archive / merge | **ANSWERED 2026-10-18 (accept all, as recommended):** Replace-with-archive | P7.5 |
| D-RS3 | How far versioning goes | phase 0 / DB-authoritative CAS / DB only | **ANSWERED 2026-10-18 (accept all, as recommended):** DB-authoritative with CAS | P7.1 |
| D-RS4 | Blocked status per host? | shared / per host | **ANSWERED 2026-10-18 (accept all, as recommended):** Per host | P7.6 |
| **N14** | | | | |
| D-IF3 | Who runs admin onboard jobs | (a) retire queue onboarding / (b) onboard-only worker / (c) Railway service | **ANSWERED 2026-10-18 (accept all, as recommended):** (a): hand off to dealer-discovery (the mini is not a host). P11A.5 is then dropped | P15A.3 |
| D-IF6 | Capabilities of the retired laptop nightly and k8s crons | port per step / home lane / drop | **ANSWERED 2026-10-18 (accept all, as recommended):** Per step, from the N14.1 desk review | P15A.4 |
| D-N28 | Replace P0B.5's multi-GB scratch restore with a desk review of code and git history | yes / no | **ANSWERED 2026-10-18 (accept all, as recommended):** Yes; a restore only if the review is inconclusive for a step | N14.1 |
| D-SR1 | Worker exit 75 | (a) / (b) / (c) | **MOOT 2026-10-18:** D-IF3 = (a). Only if D-IF3 = (b); otherwise no answer needed | P11A.5 |
| **N15** | D-PR1, D-PR4, D-PR6, D-PR7 moved to N6 with P8A.2 | | | |
| **N16** | | | | |
| D-PR2 | May a rescan overwrite a higher-ranked value? | (a) no / (b) yes / (c) after N days | **ANSWERED 2026-10-18 (accept all, as recommended):** (a) | P8B.1 |
| D-PR3 | Dictionary engine correction | (a)-(d) | **ANSWERED 2026-10-18 (accept all, as recommended):** (b) + (d) | P8B.5 |
| D-PR5 | How every scan path gets a decode | (a) inline / (b) post-scan / (c) status quo | **ANSWERED 2026-10-18 (accept all, as recommended):** (a) | P8B.4 |
| D-N32 | Does a noncanonical body style (24,817 fresh) count as missing? | yes / normalize only | **ANSWERED 2026-10-18 (accept all, as recommended):** Normalize only | P8B.6 |
| **N17** | prod heal window (owner picks a date) | | | P8C.7 |
| **N18** | | | | |
| D-OD3 | Validation through the scan's replay | (A) / (B) / (C) | **ANSWERED 2026-10-18 (accept all, as recommended):** (A) | P12A.2 |
| D-OD7 | One fetch policy | (A) / (B) / (C) | **ANSWERED 2026-10-18 (accept all, as recommended):** (A), confirmed by N18.1 | P12A.2 |
| D-OD8 | Retry transients in replay | one retry / none / synth-style | **ANSWERED 2026-10-18 (accept all, as recommended):** One retry with 2 s backoff, only if N18.1 or P6B.4 sees transient 429/5xx | P12A.2 |
| D-OD13 | DDG spec search through the proxy | (a) no / (b) yes | **ANSWERED 2026-10-18 (accept all, as recommended):** (a) | P12A.3 |
| **N19** | | | | |
| D-OD1 | Minimum-VIN rule | revised / status quo | **ANSWERED 2026-10-18 (accept all, as recommended):** Revised (plan §7) | P12B.5 |
| D-OD2 | Which count decides pass | (A) / (B) / (C) | **ANSWERED 2026-10-18 (accept all, as recommended):** (A) | P12B.2 |
| D-OD4 | `validate_recipe` | thin wrapper / delete / separate | **ANSWERED 2026-10-18 (accept all, as recommended):** Thin wrapper | P12B.3 |
| D-OD5 | Cascade through the gate | yes / no / retire | **ANSWERED 2026-10-18 (accept all, as recommended):** Yes | P12B.6 |
| D-OD6 | GET recipe with non-JSON `post_template` | mirror / error / repair | **ANSWERED 2026-10-18 (accept all, as recommended):** Mirror the scan | P12B.3 |
| **N20** | | | | |
| D-HY1 | Where the dictionary lives | keep tracked / LFS / ... | **ANSWERED 2026-10-18 (accept all, as recommended):** Keep tracked | P9.7 |
| D-HY9 | Serve the trim-ladder content in prod | (a) yes after the 20-car diff / (b) no | **ANSWERED 2026-10-18 (accept all, as recommended):** (a) | P9.8 |
| **N21** | | | | |
| D-N18 | Which price is "the price" (W08) | advertised final / internet price / fee-inclusive | **ANSWERED 2026-10-18 (accept all, as recommended):** The advertised price shown to buyers before taxes and government fees; MSRP only in `msrp`; dealer add-on fees not included unless the site includes them in its advertised price | N21.1 |
| D-N17b | 17-character I/O/Q pre-assignment IDs (1,251 rows at 69 dealers) | refuse / keep flagged | **ANSWERED 2026-10-18 (accept all, as recommended):** Keep the rows, skip the vPIC decode, mark VIN-derived facts cannot_assess, exclude from the VIN-owner guard | N21.2 |
| D-N31 | May a single-valued vPIC trim fill feeds that carry no trim (Gardena, about 620 rows)? | yes / no | **ANSWERED 2026-10-18 (accept all, as recommended):** Yes, with provenance `vpic`, only when vPIC returns exactly one trim | N21.4 |
| D-N33 | Dealer-relative interior color rule (4,573 blanks at 258 dealers) | adopt / no | **ANSWERED 2026-10-18 (accept all, as recommended):** Not now; revisit after N21 | N21.7 |
| **N22-N25** | | | | |
| D-EPA1 | How the EPA CSVs are corrected | (a) / ... | **ANSWERED 2026-10-18 (accept all, as recommended):** (a) in-place patch from a pinned `vehicles.csv`; owner approves the pinned file | P10A.5 |
| D-ET2 | Naturally aspirated representation | (a)-(d) | **ANSWERED 2026-10-18 (accept all, as recommended):** (a); revisit after the P10C.9 dry run | P10A.1, P10C.6 |
| D-ET3 | Rows flagged both turbo and supercharged (229) | Turbocharged / combined | **ANSWERED 2026-10-18 (accept all, as recommended):** Turbocharged | P10A.1 |
| D-ET4 | Role of the make/model heuristic | last resort / display only / remove | **ANSWERED 2026-10-18 (accept all, as recommended):** Last resort | P10C.2 |
| D-ET5 | Twin-turbo labels | keep via one helper / collapse | **ANSWERED 2026-10-18 (accept all, as recommended):** Keep, one helper | P10A.1 |
| D-ER1 | One ingestion path | (a) / (b) / (c) | **ANSWERED 2026-10-18 (accept all, as recommended):** (a) | P10B.3 |
| D-ER2 | Permanent catalog key | revised | **ANSWERED 2026-10-18 (accept all, as recommended):** `epaId` when present, else a sticky content key | P10A.2 |
| D-ER3 | 20,006 legacy rows | (b) / (c) | **ANSWERED 2026-10-18 (accept all, as recommended):** Re-resolve | P10B.6 |
| D-ER4 | FK policy | RESTRICT / SET NULL / none | **ANSWERED 2026-10-18 (accept all, as recommended):** RESTRICT, after the 0-dangling check | P10D.7 |
| D-ER5 | Root `DICTIONARY/` | git rm after audit / move / keep | **ANSWERED 2026-10-18 (accept all, as recommended):** git rm after the prod provenance backfill | P10D.8 |
| D-ER6 | How EPA work reaches prod | agent with per-step approval / owner / guards | **ANSWERED 2026-10-18 (accept all, as recommended):** Agent prepares each step, owner approves and runs it | P10D.6 |
| **N26-N31** | | | | |
| D-SR6 | Where the upsert measurement runs | MBP / mini / skip | **ANSWERED 2026-10-18 (accept all, as recommended):** MBP | P11B.1 |
| D-DB3 | Once every DB is strict | (a) / (b) / (c) | **ANSWERED 2026-10-18 (accept all, as recommended):** (a), with `INVENTORY_AUTO_MIGRATE=1` kept for dev | P13B.1 |
| D-DB4 | Where boot-time DB init runs | (a) / (b) / (c) | **ANSWERED 2026-10-18 (accept all, as recommended):** (a) | P13A.5 |
| D-DB8 | `cars.zip_code` | (a) drop / (b) keep | **ANSWERED 2026-10-18 (accept all, as recommended):** (b) keep; no migration | P13A.8 |
| D-DB1 | Backup tables | (a) export and drop / (b) backups schema / (c) keep | **ANSWERED 2026-10-18 (accept all, as recommended):** (a), after N25; list extended with prod `cars_owner_backup_20260929` (6,961 rows), the 4 log tables and the N12 backup tables | P13B.4 |
| D-SEC7 | Lock files per image | (a) / (b) | **ANSWERED 2026-10-18 (accept all, as recommended):** (a), run as P15B.8 only if confirmed | P15B.8 |
| D-SEC2 | Dead TOTP/phone storage | (a) / (b) / (c) | **ANSWERED 2026-10-18 (accept all, as recommended):** (a) | P16.2 |
| D-SEC3 | Users data isolation on Postgres | (a) / (b) / (c) | **ANSWERED 2026-10-18 (accept all, as recommended):** (a), sized by P16.10 | P16.10 |
| D-TC4 | Breadth of the PG tier | targeted / whole suite / write path only | **ANSWERED 2026-10-18 (accept all, as recommended):** Targeted | P14B.6 |
| D-TC8 | pytest-xdist | yes if over 12 min and safe / no | **ANSWERED 2026-10-18 (accept all, as recommended):** Decide from P14C.5 | P14C.5 |
| **N33-N36** | | | | |
| D-DC1 | Confirm the HTTP-only plan decisions | confirm / reopen | **ANSWERED 2026-10-18 (accept all, as recommended):** Confirm (D2 now reads "MBP primary" per D-N2) | P17B.11 |
| D-DC5 | Retire `--allow-browser` and the Playwright fallbacks | (a) now / (b) per platform / (c) keep | **ANSWERED 2026-10-18 (accept all, as recommended):** (a); precondition met (`SCANNER_ALLOW_BROWSER` unset on web and scanner-nightly) | P17B.3 |
| D-DC6 | Dealer-group LLM adjudication | (a) / (b) / (c) | **ANSWERED 2026-10-18 (accept all, as recommended):** (b) | P17B.9 |
| D-DC7 | `backend/oem/vehicle_reference` | (a) / (b) / (c) | **ANSWERED 2026-10-18 (accept all, as recommended):** (b) | P17B.10 |
| D-DC9 | `requires_browser` hint | (a) / (b) / (c) | **ANSWERED 2026-10-18 (accept all, as recommended):** (c) | P17B.7 |
| D-OD9 | Brochure CLI | recommended set / status quo | **ANSWERED 2026-10-18 (accept all, as recommended):** Recommended set | P17C.2 |
| D-OD10 | vPIC getter to module level | yes / no | **ANSWERED 2026-10-18 (accept all, as recommended):** Yes | P17C.1 |
| D-OD11 | Env knobs | registry + lint first / ... | **ANSWERED 2026-10-18 (accept all, as recommended):** The first option | P17C.6 |
| D-HY3 | `dealer_logs` in git | (a) curated `_learning` only / (b) all / (c) none | **ANSWERED 2026-10-18 (accept all, as recommended):** (a) | P18A.4 |
| D-HY4 | The 47 force-added `workspace/` files | (a) / (b) / (c) | **ANSWERED 2026-10-18 (accept all, as recommended):** (a) | P18A.3 |
| D-IF9 | Deploy assets to delete | per plan | **ANSWERED 2026-10-18 (accept all, as recommended):** Delete k8s, car-scanner, postgres manifests and the build.sh k8s block; keep launchd plists until replaced | P18A.7 |
| D-TC7 | How far ruff goes | the first option / gate only / more | **ANSWERED 2026-10-18 (accept all, as recommended):** The first option; waves F401 and later conditional (D-N29) | P18A.8 |
| D-HY6 | Single entry document | (a) / (b) / (c) | **ANSWERED 2026-10-18 (accept all, as recommended):** (a) | P18B.1 |
| D-HY7 | Location-logging contract | (a) / (b) | **ANSWERED 2026-10-18 (accept all, as recommended):** (a); the owner approves the CLAUDE.md wording | P18B.7 |

## 6. Owner actions outside workflows

Run every command in a plain terminal from `/Users/asarrafi/Projects/DealershipScanner`: no `!` prefix, one command per line. Run `railway status` before any railway command and always pass `-s`. The main checkout is linked to scanner-nightly, so an unflagged command acts on the fleet service.

**Do today (N1.5 records the results, but they need not wait for the workflow):**

1. **Disarm scanner-nightly.**
   ```
   railway status
   railway variable list -s scanner-nightly --kv | grep -E '^SCAN_(FLEET|DEALERS)='
   railway variable set SCAN_FLEET=0 -s scanner-nightly --skip-deploys
   railway variable list -s scanner-nightly --kv | grep -E '^SCAN_(FLEET|DEALERS)='
   railway deployment list -s scanner-nightly
   ```
   Expect `SCAN_FLEET=0` and no `SCAN_DEALERS` line. The deployment list should be unchanged: newest `152fb03c` FAILED, then `90a3d2a0` SUCCESS. The staged value applies only on the next new deployment. A rollback restores the target deployment's own variables (`SCAN_FLEET=1`), and a restart relaunches the same deployment with its old variables (`RELEASING.md:579-580`): a restart is a full fleet on the 1.4.1 image with the VIN guard lapsed. So, until N12.2 deploys v1.6.0:
   - never roll back scanner-nightly;
   - never restart or redeploy it;
   - stop mid-fleet trouble with `railway down -s scanner-nightly`.
   Optional: `railway down -s scanner-nightly -y` now, so no deployment is left to restart. Its effect on a Completed deployment is unverified; check `railway deployment list -s scanner-nightly --limit 3` afterwards.
2. **Remove the unflagged-command foot-gun.** Preferred: keep the "always pass `-s`" rule (N1.9 adds it to RELEASING.md) and leave the link as it is. Relinking the main checkout to web (`railway service`, pick `web`) moves the bare-`railway up` hazard onto prod web: an upload of the working tree would replace the site. If you relink anyway, `railway status` stays the first line of every pack (already the rule).
3. **Disconnect web's GitHub source.** Dashboard: web → Settings → Source → Disconnect. Then paste and run `web_source_check` from RELEASING.md ("Web's GitHub source") and expect `web-source-clear`. Then check:
   - `railway deployment list -s web --limit 3`: `848895c1` still newest and SUCCESS;
   - `curl -fsS https://sarraficars.com/api/health`: still 1.5.4.
   Also open github.com/settings/installations and confirm the Railway app has no access to `asarrafi47/DealershipScanner`.
4. **Railway failure notifications:** turn them on for web, scanner-nightly and Postgres in the dashboard.
5. **PG18 client:** `brew install postgresql@18`, then `/opt/homebrew/opt/postgresql@18/bin/pg_dump --version` should print 18.x. No Docker needed.
6. **Standing "never" list:**
   - never run `deploy/railway/sync-vault-to-railway.sh` before P3.7 ships (N3);
   - never a bare `railway up`;
   - never `redeploy --from-source`;
   - never change a web variable while `web_source_check` is not clear;
   - never deploy from an old tag's scripts;
   - never run more than one scanner process per lock file.

**Owner-only items no phase waits on (do when convenient):**

7. **Tell ksarrafi** that `feat/inventory-consolidation-0.4.0` and `feature/crawler-llm-standardization` will be archived as `archive/*` tags and deleted (D-REL4). N1.7's archive step is BLOCKED until this is done.
8. **www DNS.** In Cloudflare, add a proxied CNAME `www` pointing to `sarraficars.com`, plus a Redirect Rule `www` → apex (301). Verify: `dig +short www.sarraficars.com` is non-empty, and `curl -sI https://www.sarraficars.com` shows 301.
9. **D-HY2.** Rotate or disable the 7 accounts whose emails and bcrypt hashes are in public history (commits `c40bed8fe..9e8324620`), or confirm they are dead test accounts.
10. **Admin password (after item 3 prints clear).** Change the password in the site profile, then:
    ```
    read -rs VALUE
    railway variable set "ADMIN_PASSWORD=$VALUE" -s web --skip-deploys
    railway redeploy -s web -y
    railway variable list -s web --kv | grep -c '^BOOTSTRAP_FORCE_ADMIN_PASSWORD='
    ```
    The last command must print 0.
11. **Car chat on 1.5.4.** Send one car-chat request with a test account. Then check the web logs for errors (`railway logs -s web`).
12. **GitHub Actions failure email.** Confirm notifications are on (github.com/settings/notifications → Actions).
13. **Resend.** Create the key and verify the sending domain, in time for N30. Do not set the variables before P16.1 ships.
14. **Disk:** `docker image prune` (owed since 08-18; only when Docker is running anyway).
15. **Mini (only if you change D-N2 to (a)):** approve SSH, or run the P0A.5 pack yourself (`SCANNING_OPS_LOG.md:1212-1227`, plus print the DSN's `host` parameter). The N4.4 (a) pack also joins the mini to Tailscale and prefixes every mini command with the explicit DSN. The mini does not scan until N4.4 has repaired its recipe cache.
16. **Before every multi-hour scan unit (N8.10, N9.14, N11 passes, P5B.8, the N12 bridge):** lid open; `pgrep -fl caffeinate` shows `caffeinate -dims` (start it in its own terminal if not); AC recommended for runs over about 2 h (battery is not a blocker, owner 10-09); `pmset -g batt` recorded in the run dir.

## 7. Phases

### 7.0 Conventions every phase uses

- **Branches.** Phase branch `phase/<NID>` cut from `origin/main`. Unit branches are `unit/<unit id>`, in worktrees under `.claude/worktrees/<unit id>`. Units marked **MAIN** touch `workspace/`, local Postgres, Railway or git refs, and run serially in the main checkout after their code dependencies merge.
- **Code under a MAIN scan.** `workspace/` is gitignored and exists only in the main checkout, so a worktree cannot replace it, and the main checkout sits on `feature/http-only-scans` (v1.5.4) until told otherwise. Before every MAIN scan unit (N1.3's 2-dealer smoke, N6.2, the N8 mini re-runs, N8.10, N9.14, N11 passes, N11.13, P5A.8, P5B.8), the owner approves `git switch phase/<NID>` in the main checkout (`git switch --detach <RC_SHA>` in N11). The parameterized preflight (N1.3) refuses unless `git rev-parse HEAD` equals that phase HEAD or `RC_SHA`. The SHA is recorded in the run dir and in the unit checkpoint.
- **Scratch-DB scan runs.** Every re-run uses N1.3's `scripts/cleanroom/build_db.sh`, `run.env.template`, `preflight.sh` and `isolation_snapshot.sh` with `--db <name> --art workspace/cleanroom_runs/<db> --roster <file>`, never the files under `workspace/cleanroom_2026_10/`. After `build_db.sh`, `migrate --apply` runs on the scratch DB (its schema is copied from `cars`, and the run uses `INVENTORY_SCHEMA_CHECK=strict`). Fleet passes run with `fleet_scan --resume` (N9.15) once it has merged. `caffeinate -dims` verified; AC recommended for runs over about 2 h (battery is not a blocker, owner 10-09).
- **Checkpoints.** `workspace/phase_runs/<NID>/<unit id>.json` per unit, plus `workspace/phase_runs/<NID>/RESUME.md`. The schema and the re-run rule are in Section 10. Before any named wait (an owner step, a ≥20 h gap, nightly runs, a soak), the workflow writes RESUME.md with the earliest restart time and ends; each phase's workflow shape names its waits.
- **Plan status.** Status edits to this plan (unit states, measured numbers) are committed on `phase/<NID>` and ride that phase's release.
- **Reviews.** One adversarial reviewer per unit, at most 2 rounds. The reviewer checks every acceptance item and the SQLite vs Postgres behaviour, applies the audit-deletions rule (grep every consumer before deleting), and confirms the diff stays inside the unit's files.
- **Tests.** Only the unit's targeted tests, in chunks under 120 s per tool call. No call may exceed 150 s; wrap slow commands in `timeout 140`.
- **Read-only SQL helper used in exit gates:**
  ```
  roq() { PGOPTIONS='-c default_transaction_read_only=on -c statement_timeout=120000' psql -X -h /tmp -p 5432 -d "$1" -Atc "$2"; }
  ```
- **Data applies.** Every local data apply is a backup, then a dry run, then the owner's OK on the counts, then the apply, then a verify. Prod steps are prepared by agents as one-line command packs in `workspace/phase_runs/<NID>/<unit>.md` and run by the owner. The owner drops a done-file (`<unit>.done`) that the next unit verifies.
- **Dealer logs.** Every dealer touched gets dated blocks in all five CLAUDE.md files under `workspace/dealer_logs/<dealer_id>/`: discovery.md, location.md, scan_runs.md, summary.md, scan_instructions.md. Lessons go to `_learning/`. A scratch-DB re-run writes its logs under its run root (`workspace/cleanroom_runs/<db>/dealer_logs/`); the unit then appends one dated summary block per dealer from the run root to the canonical `scan_runs.md` and `summary.md`. P18B.7 (D-HY7) may change the contract later.
- **Data rules.** vPIC outranks the dealer feed for drivetrain and electrification. The EPA catalog never outranks dealer engine text or the VIN decode.
- **Release step** (every phase that releases). Follow `docs/RELEASING.md` steps 1-14 (15 for scanner-nightly). The list below is a summary; RELEASING.md wins on release mechanics. Where a phase says "no deploy", steps 12-13 are skipped and the release-log row says so.
  1. CHANGELOG `## [<VERSION>]` section;
  2. `scripts/bump_version.sh <level>`;
  3. secrets and large-file scan over `git ls-files -co`;
  4. rebase on `origin/main`; ruff and the phase's targeted tests; push the rebased `phase/<NID>` and wait for green CI on that exact SHA; open the release PR so `release-guard` runs before main moves (steps 5-7);
  5. Railway snapshot before the push (step 8: `railway_snapshot` over web, scanner-nightly and Postgres, plus variable **names** only for web and scanner-nightly), then `git push origin phase/<NID>:main` (fast-forward);
  6. annotated tag `v<VERSION>`; CI green on main (steps 9-10);
  7. the same snapshot again within 15 minutes and a diff: no new deployment (step 11);
  8. RELEASING.md release-log row with: both snapshots, the deployment ids, the last-known-good web deployment id, whether rolling back below this release is schema-safe (additive migrations only: yes/no), every web variable changed since the previous row, and the Docker smoke result.
  Every release before N11 is a 1.5.x patch; N11 cuts v1.6.0, the tag N12 deploys. After that, a phase that deploys scanner or schema changes uses minor; docs-only and test-only phases use patch. Deploys happen only through `deploy/railway/deploy_web.sh` or `deploy_scanner_nightly.sh`, from the tag, `--dry-run` first, after an owner-approved Docker smoke of the exact stage, asked per run (web: `deploy_web.sh --dry-run --keep-stage <dir>`, `Dockerfile.web`; scanner-nightly: `deploy_scanner_nightly.sh --dry-run --keep-stage <dir>`, `Dockerfile.scanner`, run against local PG; with `SCAN_FLEET` unset it must print idle). The web-source gate applies only to web **variable** changes; pushes and uploads are not gated.
- **Migration check before every web deploy.** Every web deploy ships all of main at its tag: code and migrations merged by earlier no-deploy releases (N5 V026, N9 V027/V028, N8 V029) and the web-facing changes of N6/N8/N9 (pending class, completeness rules), even before v1.6.0. Web boot applies every pending migration (`scripts/docker-entrypoint-web.sh:21`, `MIGRATE_ON_BOOT` default 1), and RELEASING.md step 1 compares only against `origin/main`. So before every `deploy_web.sh`, the owner runs the RELEASING.md "Targeting prod" `migrate --dry-run` and requires `database is up to date`. If anything is pending, the D-DB6 protocol runs first (PG18 `pg_dump -Fc`, `pg_restore -l`, `migrate --apply --target <max>`), or the web deploy is postponed. The N2, N3 and N4.5 web deploys are sequenced before N9's release reaches main; a later web deploy before N12.2 either applies V027-V029 to prod first as owner steps with a dump (they are additive), or waits. The release-log row lists the web-facing changes a deploy carries. N1.9 changes RELEASING.md step 1 to compare against prod's applied max version.
- **Web rollback.** RELEASING.md "Rollback → web" steps 1-5: `web_source_check` clear; GraphQL `deploymentRollback` with `User-Agent: railway-cli/<ver>` (the CLI has no rollback command); re-apply every web variable changed since the target deployment (each release-log row lists them) with `--skip-deploys`; `railway redeploy -s web -y`; check `/api/health`. A rollback never undoes a migration: roll back below a release only when its release-log row says schema-safe; otherwise fix forward or restore the pre-deploy dump. scanner-nightly is never rolled back.
- **Migration numbers** (registry update, recorded by N1.9; numbers follow merge order, and only unmerged files renumber):

  | Number | Content | Unit |
  |---|---|---|
  | V026 | chain completeness | P4.3 (N5) |
  | V027 | `listing_retirements` | P5B.3 (N9) |
  | V028 | `listing_miss_strikes` | P5B.4 (N9) |
  | V029 | attribution evidence (rooftop tier) | N8.6 |
  | V030 | dealer_recipes rev / CAS (plan's V029) | P7.1 (N13) |
  | V031 | epa_master flags (plan's V030) | P10A.3 (N22) |
  | V032 | unique key + RESTRICT NOT VALID | P10D.7 (N25) |
  | V033 | VALIDATE | N26 |
  | next | backup-table drops, `users.auth_epoch`, dead MFA columns | N28, N30 |

---

### N1 Close the clean-room RCA, disarm prod, build the acceptance tool, collect decisions

**Goal.**
- Close the interrupted root-cause work with the corrected premise: 21 retirements; the 9,610/9,638 VIN-owner refusals are the "8.8k".
- Turn it into fix inputs per conflict group and per platform.
- Build one read-only acceptance tool every later phase proves against.
- Pin the failure shapes as strict xfail tests.
- Remove the live prod hazards, finish the repo governance items, and collect the decisions.

**Entry conditions.**
- Today. HEAD `486f9012d`.
- No fleet running anywhere.
- The 10-15 session's scratchpad `il/` scripts (`recompute.py`, `summarize.py`, `age_old.py`, `paired.py`, `paired2.py`, `arr2.py`, `arrivals.py`, `body.py`, `dealers.py`, `idxcmp.py`, `split.py`, `transit.py`, `headline.sql`) are copied to `workspace/cleanroom_2026_10/analysis/il_scripts/` before that session ends (the scratchpad is cleared on reboot and N1 runs in a new session; `arr.pkl` stays out). N1 checks that the copy exists; if it is missing, N1.3 rebuilds the recompute.

**Units.**

| ID | Source | What | Kind | Files / area | Depends | Acceptance |
|---|---|---|---|---|---|---|
| N1.1 | W01, RET-1, WI-1, RET-13, NEW-4, CF-10 (premise) | Committed RCA doc with every number re-derived; premise corrected in RESUME.md and memory | docs | `docs/remediation/CLEANROOM_RCA_2026_10.md` (new), `workspace/cleanroom_2026_10/analysis/RESUME.md`, memory | N1.3 | figures equal N1.3 output, 0 difference |
| N1.2 | W10, CF-13, CF-11, RET-12, IL-10 (payload), lean N1.2 | 19 conflict-group dossiers + 8 platform field maps from paced live fetches | investigation, MAIN | `workspace/cleanroom_2026_10/analysis/groups/*.json`, `analysis/platform_evidence/<platform>/`, dealer_logs | D-N25 | 19 dossiers + 8 maps with checker verdicts |
| N1.3 | value-first N1.3, lean N1.3, risk-first N1.4 | `cleanroom_acceptance.py` (read-only) + 458-row fleet signals + parameterized clean-room run scripts | code | `backend/scripts/cleanroom_acceptance.py` (new), `backend/scripts/cleanroom/` (ported recompute), `scripts/cleanroom/{build_db.sh,run.env.template,preflight.sh,isolation_snapshot.sh}` (new), `backend/tests/fixtures/cleanroom_baseline_expected.json` (new), tests | none | exact match with the hand-built expected-baseline fixture; preflight refuses the frozen DBs |
| N1.4 | risk-first N4.1, WI-14 | Clean-room failure shapes as `xfail(strict=True)` tests (SQLite) | code (tests) | `backend/tests/test_cleanroom_shapes_2026_10.py` (new) + fixtures | none | all collected, all xfailed |
| N1.5 | LE-1, LE-2, LE-12, OPS-1/2/3/7/9 | Owner hazard pack | prod-owner | `docs/ops_log/N1.5.md` | none | `SCAN_FLEET=0` verified; web-source state recorded; PG18 client present |
| N1.6 | IL-02, CF-14 census, value-first N1.7 | Read-only prod census with a decision rule for N2; prod roster and conflict-pair exports for N11.13 | prod-owner | `docs/ops_log/N1.6.md`, `workspace/phase_runs/N1/` | N1.5 | results recorded without secrets |
| N1.7 | LE-3, LE-4, P2B.5, P2B.8 | Ruleset, ref bundle, archive tags, branch prune | prod-owner + MAIN | GitHub settings, `workspace/backups/` | owner told ksarrafi (for the archive part) | `rules/branches/main` non-empty; bundle verifies |
| N1.8 | value-first N1.8, lean N1.4 | Decision brief, "accept all recs except …" | owner-decision | `docs/remediation/DECISION_BRIEF_2026_10.md` (new), `OWNER_DECISIONS_LOG.md` | N1.2 (frames D-N3/D-N4) | gating minimum answered |
| N1.9 | LE-6, LE-10, LE-13, NEW-6, OPS-10, OPS-11 | Plan and repo upkeep | docs | `REMEDIATION_PLAN_2026_10.md`, `RELEASING.md`, `SCHEMA_LEDGER_2026_10.md`, memory, INCOMPLETE_LISTINGS doc, this plan, the decision brief | N1.1, N1.8 | statuses and numbers updated; this plan and the brief committed; secrets scan clean |
| N1.10 | release | v1.5.5 | docs | VERSION, CHANGELOG | N1.1, N1.3, N1.4, N1.9 | `release_guard` and CI green; tag on origin; `git ls-tree origin/main docs/NEXT_PHASES_PLAN_2026_10.md` prints one line |
| N1.11 | D-N4 (owner 10-16), CF-12, FP01 | Missing-rooftop profiles, batch 1: every rooftop the N1.2 dossiers confirm as off-roster gets a full dealer profile and a test scan | investigation + scan, MAIN | `workspace/dealer_logs/<new_id>/` (all five files), `workspace/cleanroom_2026_10/analysis/new_rooftops/<new_id>.json`, `workspace/recipes/repairs_2026_10/roster_add_<new_id>.json` (staged for N8.9), scratch DB `cars_cr_rooftops` | N1.2, N1.3 (run scripts) | one profile per confirmed rooftop with verified address and a recipe whose test scan returns only that rooftop's cars, vPIC-verified; rooftops that cannot be verified marked cannot_assess with the reason |

**Unit status (updated 2026-10-18 by N1.9, then by the N1 integrator at commit time; checkpoints in `workspace/phase_runs/N1/`, summary in `workspace/phase_runs/N1/RESUME.md`).**
- N1.1: DONE. `docs/remediation/CLEANROOM_RCA_2026_10.md` written 2026-10-18 and committed with this status edit. The integrator re-ran 17 of its figures (Q1-Q8, Q10-Q13, Q20, Q23 read-only on `cars_cleanroom`, S1 over the run's triage files): 0 differences. Figures equal the N1.3 tool output (0 differences, 374 leaves).
- N1.2: DONE (`N1.2.json`). 20 group dossiers (`analysis/groups/group_{1a,1b,2..19}.json`; group 1 split into 1a/1b) plus `group_1a_rows.json` and `retired21_recheck.json`; 8 platform field maps. Every file carries a checker verdict: dossiers 17 confirmed, 3 revise (1b, 2, 19; corrections applied in the RCA doc Sections 4.5 and 7.3); maps 6 confirmed, 2 revise (jazel, motive). Consolidated off-roster list for N1.11: 60 confirmed rooftops (4,446 `cars_cleanroom` rows), 15 likely (260 rows) in `workspace/phase_runs/N1/N1.11_missing_rooftops.json`.
- N1.3: DONE, merged `28c3bf4c7` (review 2 rounds, pass). On `cars_cleanroom`: EXPECT OK, 67 of 67 values; on `cars`: 11,719 incomplete; preflight FAIL on `cars_cleanroom`, OK on `cars_cr_test`; the 2-dealer `cars_cr_test` smoke (289 rows) left `cars_cleanroom`'s write counters at 0/0/0; `analysis/baseline_r1.json` written.
- N1.4: DONE, merged `078a0c211`.
- N1.5: DONE by the owner on 2026-10-16 (`docs/SCANNING_OPS_LOG.md`, "N1.5 hazard pack" block): `SCAN_FLEET=0` staged with `--skip-deploys`, deployment list unchanged; `web_source_check` printed `web-source-clear`; failure notifications already on; PG18 client 18.6 installed (not on PATH). The always-`-s` rule is in RELEASING.md (N1.9, uncommitted). The block went straight into SCANNING_OPS_LOG.md (no `docs/ops_log/N1.5.md`, no `N1.5.json`).
- N1.6: BLOCKED on the owner running `workspace/phase_runs/N1/N1.6.md`.
- N1.7: BLOCKED. Part A waits for the owner (ruleset POST, `delete_branch_on_merge`); part D waits for the owner's confirmation that ksarrafi was told.
- N1.8: DONE per its checkpoint (`N1.8.json`, 2026-10-18T14:42:36Z): `docs/remediation/DECISION_BRIEF_2026_10.md` written (untracked), 105 open decisions listed by blocking phase; it changes one recommendation (D-N6). The gating minimum was already answered (`OWNER_DECISIONS_LOG.md`, 2026-10-16).
- N1.9: DONE 2026-10-18: `REMEDIATION_PLAN_2026_10.md` (statuses, answers, P0B.1 numbers, D-DB1 list, P1B.5/P4.4, C.1 renumbering, Appendix E drift table and differences, Section 8 ticks, OPS-11 checklist in its Section 2), `RELEASING.md`, `docs/db_layer/SCHEMA_LEDGER_2026_10.md`, the `INCOMPLETE_LISTINGS_FINDINGS_2026_10.md` header, these status lines and the N1.8 brief, committed on `phase/N1` by the integrator after a secrets scan (clean). Open: the memory refresh (agents may not edit memory; Section 8 item 9 stays with the owner or an interactive session). `arr.pkl` is not at the repo root (checked 2026-10-18).
- N1.10: DONE 2026-10-18. Released as v1.5.6 (`164096b33`, annotated tag on origin), not v1.5.5: 1.5.5 went red in CI on a test-isolation leak and a RELEASING.md path, was fixed forward in `381ae4811` and never released. Main CI green (lint, pytest, pytest-integration, release-guard). No deploy; the Railway snapshot diff is empty.
- N1.11: DONE 2026-10-18 (`N1.11.json`; summary `workspace/phase_runs/N1/N1.11/N1.11_summary.md`). All 75 queued rooftops (60 confirmed + 15 likely) have a profile that validates (`validate_profiles.py`: 75/75): 63 DONE; 2 REPAIR_EXISTING (mercedesofcalabasas-com id 599, fjimports-com id 420); 10 BLOCKED_SCANNER; 0 CANNOT_ASSESS; 0 REFUTED. The 10 blocked are Chapman 3 (N8.3), 6 DEP rooftops (N8.11, which must also cover the results-API path `backend/scanner/scrapers/dealer_eprocess.py:705/794` or fall back to P17B.4), and tedrussellford-net-parkside (shared-host identity, owned by N8.14 since 2026-10-22). Every DONE / REPAIR_EXISTING test scan stored 0 rows naming another store (18,374 rows in scratch `cars_cr_rooftops`; vPIC year+make agree on 18,348). Expected move for N8.9: 3,666 `cars_cleanroom` rows, plus 1,051 held for the blocked rooftops. `cars` and `cars_cleanroom` write counters were 0/0/0 before and after, and pacing kept every host at 2 s or more across agents (1,412 requests). 16 last-round checker findings stay open for N8.9; none changes a status. Batch-2 queue (AutoSavvy 21, infinitiofcharlotte 33 plus held rows, group 17 2): `workspace/phase_runs/N1/N1.11_batch2_queue.json`. The owner answered on 2026-10-18 that D-N4 also covers rooftops outside the 22 roster states (OWNER_DECISIONS_LOG). Batches 2 and 3 DONE 2026-10-22 after the 10-19 pause (`N1.11b.json`, `N1.11c.json`; summaries `N1.11/N1.11_batch2_summary.md`, `N1.11/N1.11_batch3_summary.md`): `validate_profiles.py` 133/133. Batch 2, the 44 in-state entries: 42 DONE, 2 REPAIR_EXISTING (`autosavvy-com` id 198, `infinitiofcharlotte-com` id 17), 8,981 test-scan rows, 0 naming another store, expected move 4,285 `cars_cleanroom` rows. Batch 3, 14 rooftops in UT/NM/ID/LA/KY/WV: 13 DONE, 1 REFUTED, expected move 1,813 rows. Two batch-3 outcomes are contested and must be settled before N8.9 applies them: drop dead feed 9090509 from `landrovershreveport-com` and rescan, and profile Volvo Shreveport as its own store (Suite B). The autosavvy-com repair must be applied together with its 21 sister roster_adds; applied alone it retires 3,761 active sister rows. Shared-host identity is now owned by N8.14, which gates every `shared_host` roster_add. Found without an owning unit (owner to assign): dealer.com relative car-page links joined onto the manifest host (`backend/parsers/dealer_dot_com.py:413`, proposed N8.2); foreign car-page hosts that N8.5's `vdp_host_other_store` must accept when the stamped store field matches; VINs failing the check digit are stored. Owner decisions listed in `N1.11b.json` owner_decisions. `cars` and `cars_cleanroom` were never written.

**N-unit specs.**

**N1.1 RCA doc.**
- **What:** write `docs/remediation/CLEANROOM_RCA_2026_10.md` from the conflicts-RCA, retirements-RCA and code-map reports. Contents:
  - the accounting identity;
  - the 21-retirement table (writer, run id, mechanism, correct or wrong);
  - conflict classes with method;
  - the six root causes with file:line;
  - pause artifacts labelled as such;
  - a fix map from CF/RET/WI/W/IL items to N-units.
  Correct `analysis/RESUME.md` (mark the 8 shard workflows and `wf_ef900653-f48` closed: resume cannot cross sessions), memory `project_cleanroom_scan_2026_10.md`, and the MEMORY.md index line.
- **Why:** the "~8.8k in-run retirements" figure is in memory, RESUME.md and the task brief. It is wrong: 8,776 = 9,610 refusals − 834 error-dealer rows; only 21 rows are inactive.
- **Files:** as in the table.
- **Acceptance:** every figure carries SQL or a log path:line. A reviewer re-runs ≥10 of them and gets identical results. `grep -rn "8.8k\|8,800 in-run" ~/.claude/projects/-Users-asarrafi-Projects-DealershipScanner/memory workspace/cleanroom_2026_10/analysis/RESUME.md` returns only lines marked corrected.
- **Tests:** none (docs).

**N1.2 Group dossiers and platform field maps.**
- **What:**
  - One agent per conflict group with ≥20 refusals: 19 groups, 70 dealers, 44 of them unanalysed.
  - One agent per platform family: cosmos, dealer.com ws-inv-data, Motive, Chapman, Jazel, OneAudi/omnigraph, DEP/typesense pools (whether a per-car store or lot field exists decides N8.11), CarsCommerce.
  - 4 adversarial checkers.
  - Each **dossier** records:
    - which payload field names the true store;
    - confirmation or refutation of the photo-account mapping (don:21776/18411/19779/20573/20574/20624/24529/877, cc:11000429);
    - roster gaps;
    - the pool classification;
    - the expected owner counts after the fix;
    - the proposed rule.
  - Each **platform map** records: field name, % of sampled cars carrying it, store-id → dealer_id map, and whether the parser can stamp `_rooftop`.
  - Also closes:
    - CF-11: why the CarsCommerce gate stayed silent on listing 2309549 and the East Tennessee listings;
    - RET-12: re-check of the 21 retired VINs;
    - the 98 Lexus VINs missing from the live feed;
    - the real MB Beverly Hills endpoint;
    - the coastline dead account 6061839;
    - the Motive retry payload shape (IL-10).
- **Why:** the fix acts per parser and per group, not per dealer. Photo-account mappings rest on a 24-card sample. 15 of 18 top pair dealers logged no rooftop-gate line.
- **Files:** `workspace/cleanroom_2026_10/analysis/groups/<group>.json`, `analysis/platform_evidence/<platform>/{payload_*.json,field_map.json}`, one `discovery.md` block per dealer touched.
- **Acceptance:**
  - 19 dossiers and 8 maps, each with a checker verdict.
  - Every rule cites a saved payload sample.
  - Anything unconfirmable is marked cannot_assess.
  - The request log shows ≤3 pages per dealer for dossiers, ≤30 per host for maps, ≥2 s apart, home IP, no browser.
  - One summary table: rows per class per group, plus the proposed rule.
- **Tests:** a JSON schema check over the outputs (re-runs skip valid files).

**N1.3 Acceptance tool.**
- **What:** first, port the recompute scripts from `workspace/cleanroom_2026_10/analysis/il_scripts/` (the copy of the scratchpad `il/` set listed in the entry conditions) into `backend/scripts/cleanroom/`. If they are gone, rebuild the recompute from `merge_verified_specs(include_extended_specs=False)` + `listing_missing_field_codes(include_non_actionable=True)` over every active car.
  - `backend/scripts/cleanroom_acceptance.py --db <name> [--run-dir <dir>] [--ref-db cars] --json <out>` computes:
    - conflicts (rows, VINs, pairs) and the CF-7 class-(b) DB checks;
    - the triage identity: captured, refused, written, active, inactive;
    - in-run retirements by writer;
    - dict-trim and token-color rows;
    - `vpic_fuel` PHEV false positives;
    - AWD/4WD rows where vPIC decodes 2WD;
    - page-cap walks;
    - VDP wanted vs fetched;
    - verdict counts;
    - coverage against a reference DB;
    - junk prices; non-17 and I/O/Q VINs; placeholder images;
    - the incomplete recompute, split into first seen ≤3 d, ≤7 d and older (via `first_seen_at` and a VIN join to the reference DB).
  - `--signals` writes `analysis/fleet_signals.json` (one row per dealer) and flags ≤46 signal dealers for N11.10. Signals include feed-level HTTP 403/429 from the home IP (63 and 9 in the clean room).
  - `scripts/cleanroom/build_db.sh <name>` is a parameterized copy of `workspace/cleanroom_2026_10/build_db.sh` (which hard-codes `cars_cleanroom` at `:19-26`). It refuses an existing DB, has a `--from-template <db>` mode (`createdb -T`) and stamps `git rev-parse HEAD` into the run dir.
  - The other clean-room run files hard-code the frozen DB and its folders too: `cleanroom.env:41-42` (both DSNs), `:53-57` (`RECIPES_CACHE_DIR`, `DEALER_LOGS_ROOT`, `SCAN_OUT_ROOT` under `workspace/cleanroom_2026_10`), `:95` (`SCAN_DEALERS` from `roster.txt`); `preflight.sh:19,43-50` (passes only for `cars_cleanroom`); `isolation_snapshot.sh:6` (`ART`). So N1.3 adds `scripts/cleanroom/run.env.template`, `scripts/cleanroom/preflight.sh` and `scripts/cleanroom/isolation_snapshot.sh`, each taking `--db <name> --art <dir> --roster <file>`. The per-run root is `workspace/cleanroom_runs/<db>/{recipes_cache,dealer_logs,runs}`. Preflight refuses when the write target is `cars_cleanroom` or `cars`, a non-`/tmp` host or port 15432; it checks that `RECIPES_CACHE_DIR`, `DEALER_LOGS_ROOT`, `SCAN_OUT_ROOT` and `SCANNER_LOCK_PATH` sit under the run root, and that `git rev-parse HEAD` equals the expected SHA (`--sha`).
  - `backend/tests/fixtures/cleanroom_baseline_expected.json` is hand-built from the spec numbers below (not written by the tool).
- **Why:** later phases need one committed tool to prove "the counts drop". The recompute exists only in a temporary scratchpad. The clean-room run's commit was never stamped. Re-running with the old run files would either fail preflight or write into the frozen baseline and its recipe cache, logs and runs.
- **Files:** as in the table; tests in `backend/tests/test_cleanroom_acceptance.py` (new).
- **Acceptance:** on `cars_cleanroom` with `runs/fleet_20261013T140256Z`, its output matches `cleanroom_baseline_expected.json` exactly:
  - conflicts 9,638 / 7,393 / 92;
  - `mbbeverlyhills-com` rows with host `www.mbontario.com`: 1,284 total, 1,278 of them conflict rows with claimant `mbontario-com`;
  - identity 208,233 / 9,610 / 198,623 / 199,457 / 21;
  - dict trim 1,228 / 52; token color 882 / 60;
  - `vpic_fuel` 3,585 / 262;
  - AWD-vs-2WD 5,306 / 142;
  - 13 page-cap walks;
  - VDP 92,282 / 188,597;
  - incomplete 34,023 (14,345 without the new-NULL mileage code); stale index flags 4,581.
  On `cars` it reproduces 11,719 incomplete. Every query chunk finishes in under 140 s. It then writes `analysis/baseline_r1.json`, the baseline later phases compare against.
  Run scripts: `scripts/cleanroom/preflight.sh --db cars_cleanroom ...` prints FAIL; `--db cars_cr_test` prints OK; a 2-dealer smoke into `cars_cr_test` leaves `pg_stat_database` `tup_inserted` and `tup_updated` for `cars_cleanroom` unchanged.
- **Tests:** fixture DB tests on SQLite for each metric function; a read-only guard test (refuses to run without `default_transaction_read_only`); preflight refusal tests.

**N1.4 Failure-shape tests.**
- **What:** strict xfail tests that encode the defects:
  - Terry retry: 16 prior rows, 0 re-seen, 134 new → retire 0;
  - Coastline 8/11 → retire 3; Napleton 16/17 → 1; South County Lexus 14/15 → 1;
  - a lifecycle retry whose baseline is known=0;
  - guard scan-order dependence: two orders must give the same owner;
  - an unstamped group feed misattributed (keep `test_single_store_feed_is_untouched` unchanged);
  - the triage identity replay from a fixture: `rows_written` = active.
- **Why:** later phase exits must flip them. This proves the fix targets the observed failure, not a proxy.
- **Files:** `backend/tests/test_cleanroom_shapes_2026_10.py` (new), `backend/tests/fixtures/cleanroom_shapes/` (new).
- **Acceptance:** `pytest backend/tests/test_cleanroom_shapes_2026_10.py -q` reports every test as xfailed, none as xpassed.
- **Tests:** the file itself.

**N1.5 Hazard pack.** Section 6 items 1-6. The agent writes the command pack; the owner runs it.
- **Acceptance:**
  - the grep prints `SCAN_FLEET=0` and no `SCAN_DEALERS`;
  - no new scanner-nightly deployment;
  - the always-`-s` rule is in RELEASING.md (relinking to web is optional and carries the web hazard in Section 6 item 2);
  - `web_source_check` prints `web-source-clear`, or the block records "pending" (that blocks only web variable and web settings steps: N3.13 steps 5-6, N4.1's web part, Section 6 item 10);
  - PG18 `pg_dump --version` prints 18.x;
  - a dated block in `docs/ops_log/N1.5.md`.
  **No fallback is accepted for the disarm.** Optional note, not a gate: if the owner has done Section 6 item 11, the block records whether car chat answered without errors in the web logs.

**N1.6 Read-only prod census.** The agent prepares the queries; the owner runs them with a read-only session (`PGOPTIONS='-c default_transaction_read_only=on'` and the PG18 psql). Checks:
- the values of `LISTINGS_INCLUDE_INCOMPLETE_CARS` and `FLASK_ENV` on web (not secrets): `railway variable list -s web --kv | grep -E '^(LISTINGS_INCLUDE_INCOMPLETE_CARS|FLASK_ENV)='`;
- the count of prod `incomplete_listings` rows coded `mileage` on active New or blank-condition cars with NULL mileage (IL-02);
- `vin_owner_conflicts` grouped by pair (expected 12,903 rows / 212 pairs), exported to `workspace/phase_runs/N1/prod_conflict_pairs.csv` for N11.13;
- the prod scan roster (dealer ids, 561 expected), exported to `workspace/phase_runs/N1/prod_roster.txt` for N11.13;
- whether `mbbeverlyhills-com`'s recipe equals `mbontario-com`'s;
- the coastlinecdjr recipe account;
- auth-staled recipe counts (expected 31 dealers; 235 rows with `max_saved_at=0`);
- whether `CAR_CHAT_WEB_RESEARCH` is present.
- **Decision rule:** prod hides incomplete cars unless the value is `1`, `true`, `yes` or `on` (`data_quality_repo.py:117-122`: unset in production, or `0/false/no/off` anywhere, hides them). If prod hides them and IL-02 > 0, N2 releases before anything else.
- **Acceptance:** numbers recorded in `docs/ops_log/N1.6.md`, no values of secrets.

**N1.7 Repo governance.**
- **What:**
  - The agent writes `workspace/phase_runs/N1/ruleset.json`: no deletion, `non_fast_forward`, required checks `lint`, `pytest`, `pytest-integration`, `release-guard` with `integration_id` 15368, owner bypass, optional `v*` tag rule.
  - The owner runs `gh api -X POST repos/asarrafi47/DealershipScanner/rulesets --input workspace/phase_runs/N1/ruleset.json` and sets `delete_branch_on_merge=true`.
  - The agent runs `git bundle create workspace/backups/repo_all_refs_<stamp>.bundle --all` and `git bundle verify`.
  - After the owner confirms ksarrafi was told: annotated tags `archive/inventory-consolidation-0.4.0` (87bb1b689) and `archive/crawler-llm-standardization` (7006ed7ff). Then, after `merge-base --is-ancestor` checks, delete the remote branches `Dev`, `feature/admin-scanner-ops-hub`, `feature/data-architecture-scan-reliability` and `feature/scanner-website-destructuring-layers`, plus the 9 stale local branches. The owner runs the deletions.
- **Why:** P2B.5 and P2B.8 are unfinished; N32 (17A) is gated on the archive.
- **Acceptance:**
  - `gh api repos/asarrafi47/DealershipScanner/rules/branches/main` lists `non_fast_forward`, `deletion` and the 4 checks;
  - the bundle verifies;
  - `git ls-remote origin` shows only `main`, `feature/http-only-scans`, active `phase/*`, tags and `archive/*`.
  If ksarrafi has not been told, the archive part is BLOCKED and becomes N32's entry gate.

**N1.8 Decision brief.**
- **What:** one brief listing every decision in Section 5 with its recommendation and evidence, answered as "accept all except …", logged row by row in `OWNER_DECISIONS_LOG.md`.
- **Acceptance:** the gating minimum (D-N1, D-N2, D-N3, D-N4, D-N25, D-RC1, D-RC2) is answered before N8 or N9 start. Every other decision is answered or marked with the phase it blocks.

**N1.9 Plan and repo upkeep.** In `REMEDIATION_PLAN_2026_10.md`:
- statuses for 0A-2B (DONE or PARTIAL);
- P1B.5 DROPPED, also in C.2;
- D-RS5 = Yes;
- the D-OD1/2/3/7/8 numbers pasted from `validation_paths_2026_10.md` §10;
- the D-DB1 list extended with prod `cars_owner_backup_20260929` (6,961 rows) and the 4 log tables;
- P4.4 marked no-op;
- the C.1 migration renumbering from Section 7.0;
- a line-ref drift table (`recipes.py` promote_from_ledger `:889`, union log `:1695`, total stop `:1665`; `recipe_store._ensure_table` `:142-170`; RAILWAY_SCANNING.md 20-dealer table about `:308-345`, idle-in-transaction `:399`);
- Section 8 items ticked;
- a pointer to this plan.
Elsewhere:
- RELEASING.md: release-log row `1.5.4 / 486f9012d / web 848895c1 / Docker smoke not recorded`; the always-`-s` rule; step 1 compares the release's migrations against prod's applied max version (the "Targeting prod" `migrate --dry-run`), not only against `origin/main`;
- `SCHEMA_LEDGER_2026_10.md`: `cars_cleanroom` as a frozen V025 baseline;
- commit `docs/INCOMPLETE_LISTINGS_FINDINGS_2026_10.md` with a header mapping every finding to its N-unit;
- commit `docs/NEXT_PHASES_PLAN_2026_10.md` (untracked today, and Section 10.1 tells agents to read it inside worktrees cut from `origin/main`) and `docs/remediation/DECISION_BRIEF_2026_10.md` (N1.8);
- a long-run hygiene checklist (OPS-11);
- memory: remediation progress at 1.5.4, "TRUST_PROXY_HEADERS OPEN" resolved, the web-source note;
- with owner OK, move the untracked repo-root `arr.pkl` out of the checkout.
- **Acceptance:** the diff touches only docs and memory; the secrets scan is clean; `git status` shows no `arr.pkl` at the root.

**N1.11 Missing-rooftop profiles, batch 1 (D-N4 as answered 2026-10-16).**
- **What:** the owner's rule is that a rooftop whose cars show up inside another store's feed gets its own profile and is scanned as its own dealer. For every rooftop the N1.2 dossiers confirm as off-roster (starting list in the D-N4 row: Mtn View Dalton/Bristol, Bentley Chevrolet of Florence, the Sonic Honda/Mercedes stores, Chapman Hyundai/VW, Sam Pack Carrollton/Lewisville/Grapevine, autosavvy's other rooftops, the non-INFINITI stores in infinitiofcharlotte's pool), run the `docs/NETWORK_SCAN_PROCESS.md` loop:
  - discovery: the rooftop's own website (or the group site's per-store page), platform, and the payload field and value that name this rooftop (from the N1.2 platform map);
  - location: street address and city verified against a second source (the feed's own location field plus the store's website or Google Places with `websiteUri` host = dealer host);
  - recipe: a store-scoped recipe (the group feed filtered to this rooftop's store id or lot code), never the unfiltered group pool;
  - test scan into scratch DB `cars_cr_rooftops` (built with N1.3's `scripts/cleanroom/build_db.sh`, run through its preflight), then vPIC verification;
  - all five dealer_logs files under `workspace/dealer_logs/<new_id>/`, lessons to `_learning/`.
  - The roster entry is staged as `workspace/recipes/repairs_2026_10/roster_add_<new_id>.json` and applied to `cars` by N8.9's tool with the owner's OK on the diff; prod gets it with the N12 roster.
- **Why:** filing these cars under a sister store shows them at the wrong address; refusing them forever loses real inventory the owner wants listed.
- **Acceptance:**
  - one `analysis/new_rooftops/<new_id>.json` per confirmed rooftop with: verified address and sources, platform, store-id field and value, recipe path, test-scan counts, the number of `cars_cleanroom` rows of that rooftop currently filed under the sister store (the expected move count);
  - every test scan stores only cars whose payload names this rooftop (0 rows naming another store), and vPIC verification is recorded in `scan_runs.md`;
  - a rooftop that cannot be verified (no per-car store field, dead site, address unconfirmed) is marked cannot_assess with the reason and stays held;
  - live fetches stay inside the D-N25 pacing (≥2 s apart per host, no browser).
- **Tests:** none (data); the profile JSON passes a schema check so a re-run skips finished rooftops.

**Workflow shape.**
- Wave A (parallel): N1.3 (builder + reviewer), N1.4 (builder + reviewer), N1.5 and N1.6 packs (preparer; owner runs).
- Wave B: N1.2 (8 platform agents, then 19 group agents in chunks of 5, plus 4 checkers); N1.1 after N1.3 lands.
- Wave C: N1.8, N1.7 (owner), then N1.9 (it commits the brief N1.8 writes). N1.11 runs after N1.2 and N1.3 (it needs the confirmed rooftop list and the parameterized run scripts) and does not block N1.10; unfinished profiles carry into N8.9.
- Then N1.10.
- About 36 agents.
- Checkpoints: `workspace/phase_runs/N1/<unit>.json`. Dossiers and maps are one file each, so a re-run skips files that pass the schema check. Owner packs are skipped once their done-file exists.

**Exit gate.**
- `railway variable list -s scanner-nightly --kv` (owner) shows `SCAN_FLEET=0`.
- `python backend/scripts/cleanroom_acceptance.py --db cars_cleanroom --run-dir workspace/cleanroom_2026_10/runs/fleet_20261013T140256Z --json /dev/stdout` matches the hand-built `backend/tests/fixtures/cleanroom_baseline_expected.json` exactly.
- `scripts/cleanroom/preflight.sh --db cars_cleanroom ...` prints FAIL; the 2-dealer `cars_cr_test` smoke left `cars_cleanroom` unchanged.
- `git ls-tree origin/main docs/NEXT_PHASES_PLAN_2026_10.md` prints one line.
- `pytest backend/tests/test_cleanroom_shapes_2026_10.py -q`: all xfailed.
- `ls workspace/cleanroom_2026_10/analysis/groups/*.json | wc -l` returns 19.
- `ls workspace/cleanroom_2026_10/analysis/platform_evidence | wc -l` returns 8.
- `gh api repos/asarrafi47/DealershipScanner/rules/branches/main` is non-empty.
- The gating-minimum decisions are logged.
- N1.11: `ls workspace/cleanroom_2026_10/analysis/new_rooftops/*.json` lists one file per rooftop the dossiers confirmed (each DONE or cannot_assess).
- v1.5.5 is on main.

**Release step.** Patch v1.5.5: docs (this plan included), the read-only tool, the clean-room run scripts, tests. No deploy. Not gated on web-source-clear.

**Prod steps and who runs them.** Owner: N1.5 (variable set with `--skip-deploys`; dashboard source disconnect), N1.6 (read-only), N1.7 (ruleset POST, branch deletions). No agent connects to prod.

**Owner decisions / actions for this phase.**
- Decisions: D-N25, D-N9, D-N1, D-N2, D-N19, D-N29.
- Actions: Section 6 items 1-7.

---

### N2 Prod visibility hotfix (IL-01 plus a targeted index repair)

**Goal.** Stop counting a New car with no odometer value as incomplete. Repair only the affected prod index rows. Nothing else ships in this release.

**Entry conditions.**
- D-N12 answered.
- N1.6 result known.
- If prod hides incomplete cars and IL-02 > 0, N2 releases first. Otherwise N2 still merges early (it must be live before the first prod fleet) and may ride N3's deploy.

**Units.**

| ID | Source | What | Kind | Files / area | Depends | Acceptance |
|---|---|---|---|---|---|---|
| N2.1 | IL-01 | NULL odometer on a New, non-CPO row is not missing | code | `backend/utils/listing_completeness.py:126-141`, `backend/tests/test_mileage_zero_f12.py:38`, the car template mileage line | D-N12 | `mileage` code count on `cars_cleanroom` falls 22,947 → ≤1,795 |
| N2.2 | IL-02 | Targeted prod index repair script | code | `backend/scripts/repair_new_null_mileage_index.py` (new) + test | N2.1 | dry-run count = IL-02 |
| N2.3 | release | Docker smoke, `deploy_web.sh`, owner runs the repair | prod-owner | `docs/ops_log/N2.3.md` | N2.1, N2.2 | prod count 0 |

**N-unit specs.**

**N2.1.**
- **What:** `_mileage_blank` returns False for NULL mileage when the condition is New and not CPO. The test at `test_mileage_zero_f12.py:38` changes to match. The display follows D-N12: no mileage line on New cars. Used rows with 0 or NULL keep the code.
- **Why:** 21,152 active New or blank-condition cars (10.6%) carry a false `mileage` code in a fresh scan. Production hides indexed incomplete cars (`data_quality_repo.py:105-122`).
- **Files:** as in the table.
- **Acceptance:** the N1.3 recompute on `cars_cleanroom` shows the `mileage` code on ≤1,795 cars (1,374 used at 0 mi + 421 used NULL); incomplete falls from 34,023 to 14,345 ±1%; `cars` stays at 11,719.
- **Tests:** updated F12 test; new cases for New/NULL, CPO/NULL, Used/0, Used/NULL, blank condition/NULL.

**N2.2.**
- **What:** a script that recomputes index codes only for active New or blank-condition cars with NULL mileage, in batches of 1,000. `--dry-run` is the default and prints the count. `--apply` requires a named backup file. It uses the same rule code as N2.1.
- **Why:** a full prod rebuild under the old rules elsewhere would mix changes. This repair touches only the F12 class.
- **Files:** as in the table; test `backend/tests/test_repair_new_null_mileage_index.py` (new).
- **Acceptance:** on a `cars_cleanroom` copy, the dry run equals the 21,152 class count; apply then dry run gives 0; each batch runs under 30 s.
- **Tests:** SQLite fixture test for selection, batching and the backup requirement.

**N2.3.**
- **What, in order:**
  1. The owner approves the Docker smoke of the exact stage (`deploy_web.sh --keep-stage`, then build and run against local PG).
  2. Migration check (Section 7.0): the RELEASING.md "Targeting prod" `migrate --dry-run` prints `database is up to date`. N2 deploys before N9's release reaches main; if anything is pending, stop and run the D-DB6 protocol first.
  3. `deploy_web.sh --dry-run`, then the real deploy from the tag.
  4. `curl -fsS https://sarraficars.com/api/health` shows the tag.
  5. The owner backs up the affected rows with `\copy (SELECT ...) TO 'workspace/backups/incomplete_newnull_<stamp>.csv'`.
  6. The owner runs N2.2 `--dry-run` against prod: count = IL-02.
  7. The owner runs `--apply`.
  8. A read-only recount.
- **Acceptance:** prod `mileage` codes on active New NULL-mileage cars = 0; the visible-car count before and after is recorded; a release-log row and an ops-log block exist.

**Workflow shape.**
- Wave A: N2.1, N2.2 with 1 reviewer each.
- Wave B: N2.3 command pack, then the owner runs it.
- About 5 agents.
- Checkpoints: `workspace/phase_runs/N2/`; before the N2.3 owner steps the workflow writes RESUME.md and ends.

**Exit gate.**
- `pytest backend/tests/test_mileage_zero_f12.py backend/tests/test_repair_new_null_mileage_index.py -q` passes.
- The acceptance tool on `cars_cleanroom` shows `mileage` ≤1,795.
- Prod health shows the N2 tag; the owner's read-only count = 0.

**Release step.** Patch (v1.5.6 if N1 released first). Web deploy only; scanner-nightly is not touched.

**Prod steps and who runs them.** Owner: Docker smoke, `deploy_web.sh`, `\copy` backup, repair dry run and apply, recount.

**Owner decisions / actions.** D-N12; Docker smoke OK; the N2.3 steps.

---

### N3 Security wave 1 and web stopgaps (Phase 3 plus lxml)

**Goal.** Close the web holes live on 1.5.4, take Chromium out of the web image, clear the lxml advisory, and stop admin onboard jobs queuing forever.

**Entry conditions.**
- N1 merged.
- D-SEC6, D-SEC5, D-DEP1, D-N19 answered.
- Docker OK for the smoke.
- `web-source-clear` is needed only for N3.13 steps 5-6 (the env step and the domain removal).

**Units.** P-unit specs: REMEDIATION_PLAN "Phase 3: Security wave 1" (line 1072) and "Phase 15B" (line 3499).

| ID | Source | What | Kind | Files / area | Depends | Acceptance |
|---|---|---|---|---|---|---|
| P3.1 | plan Phase 3 | OAuth: email-only lookup, verified provider email, link only after a password login (D-SEC4 (a)) | code | `google_oauth.py:147,165,267`, `apple_oauth.py:185,207`, `users_db/auth.py:89` (plan said :98) | none | per plan |
| P3.2 | plan | `client_ip` trusts XFF only from `TRUSTED_PROXY_CIDRS` | code | `backend/utils/client_ip.py` | none | per plan |
| P3.3 | plan | SSRF guard blocks every non-global address | code | `outbound_url.py:12,:35` | none | per plan |
| P3.4 | plan | esm.sh out of CSP; CSRF on `api_admin_operator_*` DELETE; url_map test | code | `security.py:86-92,159-162,178-179` | none | per plan |
| P3.5 | plan | `css_url` filter | code | templates, filter module | none | per plan |
| P3.6 | plan | Prod config warnings, reset cooldown. Stale: the pass-through warning is now a guard only | code | per plan | none | per plan |
| P3.7 | plan | README service list; vault-sync refuses unset `TRUSTED_PROXY_HOPS` (`:98`) | code+docs | `deploy/railway/sync-vault-to-railway.sh`, `deploy/railway/README.md` | none | per plan |
| P3.8 | plan | WebResearcher HTTP-only by default; Chromium out of `Dockerfile.web:7,30`. Stale refs: `_fetch_with_playwright` now `:574` | code | `web_researcher.py`, `Dockerfile.web` | none | per plan; image has no chromium |
| P15B.1 | plan Phase 15B | Orphan deps out (crawl4ai has no importers), lxml ≥6.1, pip-audit without ignores | code | `requirements.txt:61` | D-DEP1 | `pip-audit` clean |
| N3.10 | NEW-5 | Disable the admin onboard action (no consumer since 10-08) | code | `backend/dealer/admin/dealers_hub.py:40`, admin template | none | POST returns 409, nothing enqueued |
| N3.11 | risk-first N2.3, D-N19 | "Inventory last checked <date>" line while stale | code | dealer/car templates, a repo query | D-N19 | line shows max(`scraped_at`) and hides under 48 h |
| P3.9 | plan | SECURITY_MASTER_TODO wave-1 step. Stale: drop the "prod exception under SEC-083" note | docs | `SECURITY_MASTER_TODO.md` | P3.1-P3.8 | per plan |
| N3.13 | release | Docker smoke, deploy, owner env follow-ups | prod-owner | `docs/ops_log/N3.13.md` | all | exit gate |

**N-unit specs.**

**N3.10 Onboard stopgap.**
- **What:** the admin "onboard dealer" POST returns 409 with one line, "Onboarding is paused; use dealer-discovery (see P15A.3)", and does not call `enqueue_job`. The ops log records how many jobs are already queued (read-only). No jobs are deleted.
- **Why:** `dealers_hub.py:40` enqueues `dealer_jobs`, but no worker has consumed them since the worker and scheduler were deleted on 10-08 (`SCANNING_OPS_LOG.md:1264`).
- **Acceptance:** the test shows a 409 and zero rows enqueued.
- **Tests:** `backend/tests/test_admin_onboard_paused.py` (new).

**N3.11 Last-checked line.**
- **What:** one plain text line on dealer and car pages: "Inventory last checked <date>", from max(`scraped_at`) per dealer. Hidden when that value is under 48 h old. No card or box container.
- **Why:** prod is 16 days stale, and the line is honest until N12.
- **Acceptance:** template tests for both states; markup contains no container element.
- **Tests:** `backend/tests/test_last_checked_line.py` (new).

**N3.13 Release and env (ordered; each step verified before the next).**
1. Docker smoke of the exact stage: health 200 with the tag, CSP without esm.sh, no chromium binary, image size recorded, admin and listing pages load.
2. Migration check (Section 7.0): the prod `migrate --dry-run` prints `database is up to date`. N3 deploys before N9's release reaches main; if anything is pending (for example V026 from N5 not yet applied by its owner pack), stop and run the D-DB6 protocol first, or wait.
3. `deploy_web.sh --dry-run`, then the real deploy.
4. Live CSP check (`curl -sD - https://sarraficars.com/ -o /dev/null`).
5. After `web-source-clear` only: `railway variable set TRUSTED_PROXY_CIDRS=cloudflare -s web --skip-deploys`, then `railway redeploy -s web -y`.
6. After `web-source-clear` only (a domain removal is a service settings write; whether it creates a deployment is unverified): remove the `*.up.railway.app` domain in the dashboard, with `railway deployment list -s web --limit 3` before and after, and record whether a deployment was created.
7. Check that `curl -s -o /dev/null -w '%{http_code}' https://web-production-26b11.up.railway.app/api/health` no longer returns 200.
8. `DEV_IP_ALLOWLIST` per D-SEC5.

**Workflow shape.**
- Wave A (parallel worktrees): P3.1-P3.7, P15B.1, N3.10, N3.11, each with a security-lens reviewer.
- Wave B: P3.8 (its Docker build runs MAIN after the owner's OK).
- Wave C: P3.9, integration, N3.13.
- About 24 agents.
- Checkpoints: `workspace/phase_runs/N3/`; before the N3.13 owner steps the workflow writes RESUME.md and ends.

**Exit gate.**
- The unit tests pass in CI on `phase/N3`.
- `pip-audit -r requirements.txt` reports 0 findings with no ignores.
- `grep -n esm.sh backend/web/security.py` is empty.
- The prod CSP header has no esm.sh.
- The railway.app origin is not serving (once step 6 is done), or D-SEC6 records the wait.

**Release step.** Patch (every release before N11 is a 1.5.x patch, so N11 can cut v1.6.0). Web deploy.

**Prod steps and who runs them.** Owner: smoke approval, migration check, deploy, `TRUSTED_PROXY_CIDRS` and domain removal (both gated on web-source-clear).

**Owner decisions / actions.** D-SEC6, D-SEC5, D-DEP1, D-N19; Docker OK; N3.13 steps.

---

### N4 Railway config-as-code replacement and ops loose ends

**Goal.** The deploy scripts and the future cron work after 2026-12-01. The dead worker/scheduler files leave the repo. Branch naming matches the docs. The mini decision is carried out.

**Entry conditions.**
- N1.5 shows `SCAN_FLEET=0` verified. Even so, N4 creates no scanner-nightly deployment before N12.2 (Section 6 item 1; D-REL8 (b): scanner-nightly deploys first at P6B.1).
- Web settings writes only after `web_source_check` prints `web-source-clear` (a web settings change can create a deployment, and the 09-11 incident was a web change rebuilding from GitHub main); otherwise N4.1 prepares both packs and web waits.
- D-N24 answered.
- **Carve-out date:** if N4.1 is not proven by 2026-11-20, it releases alone, ahead of any other train.

**Units.**

| ID | Source | What | Kind | Files / area | Depends | Acceptance |
|---|---|---|---|---|---|---|
| N4.1 | NEW-2, OPS-8 | Move web and scanner-nightly settings off config-as-code; deploy scripts check them | code + ops | root `railway.toml`, `railway.scanner-nightly.json`, `deploy/railway/deploy_web.sh`, `deploy_scanner_nightly.sh`, RELEASING.md | D-N24 | read-only query equals old files; dry runs pass with no deprecation warning |
| P15B.6 | plan Phase 15B | Delete worker/scheduler Dockerfiles, tomls, compose entries (`docker-compose.yml:81,108`); compose Postgres uses a pgvector image | deletion | per plan | none | `grep` clean; `docker compose config` validates |
| N4.3 | LE-5, D-REL2 | Runner template and RELEASING.md agree on `phase/<NID>` / `unit/<id>`; retire `feature/http-only-scans` after the first phase release (and after N4.4's mini check or waiver) | docs + MAIN | RELEASING.md, Section 10 template | N1.10 | branch archived or deleted per owner |
| N4.4 | LE-7, OPS-6 | Mini per D-N2: repair pack if SSH approved, else a re-plan note | prod-owner / docs | `docs/ops_log/N4.4.md`, `OWNER_DECISIONS_LOG.md` | D-N2 | either repaired and on the release commit, or recorded as excluded |
| N4.5 | release | Prove the new web deploy path: owner-approved Docker smoke of the exact stage, migration check (Section 7.0; before N9's release reaches main), then `deploy_web.sh` | prod-owner | ops log | N4.1 | health shows the tag; no config-as-code warning; prod `migrate --dry-run` printed `database is up to date` before the deploy |

**N-unit specs.**

**N4.1.**
- **What:**
  - Read the current settings: web (`railway.toml`: Dockerfile.web build, healthcheck, restart policy) and scanner-nightly (`railway.scanner-nightly.json`: Dockerfile.scanner, restart NEVER, 16 vCPU / 16 GB, no cron).
  - Write web's settings into service settings with the mechanism D-N24 picks, only after `web-source-clear`. If that is GraphQL `serviceInstanceUpdate`, the owner runs it from a prepared pack with the `User-Agent: railway-cli/...` header, with `railway deployment list -s web --limit 3` before and after.
  - Prepare the scanner-nightly settings pack, but create **no scanner-nightly deployment before N12.2**: a deployment of the 1.4.1 image risks a fleet with the guard lapsed, and the redeploy/restart variable semantics are not safe (a rollback restores `SCAN_FLEET=1`). The pack is applied either (a) only through a mechanism first proven on web to create no deployment, or (b) inside N12.2, immediately before `deploy_scanner_nightly.sh`.
  - Change both deploy scripts to stop copying config files and to read back the service settings before deploying (a refusal on mismatch).
  - Document the cron path P6B.7 (N12.11) will use.
- **Why:** every `railway deployment list` prints the 2026-12-01 deprecation; P6B.8 was 5 phases out on the serial order.
- **Acceptance:**
  - a read-only GraphQL query shows web's settings equal to the old file, and the scanner-nightly pack equal to the old file (applied now only under option (a));
  - `deploy_web.sh --dry-run` and `deploy_scanner_nightly.sh --dry-run` pass with no deprecation warning;
  - no new scanner-nightly deployment until N12.2 (`railway deployment list -s scanner-nightly`);
  - RELEASING.md is updated.
- **Tests:** bash `-n` on both scripts; a stubbed-railway test of the settings check.

**N4.4.**
- **(b) mini excluded:** a note in OWNER_DECISIONS_LOG listing the re-planned units: D-DB5 (b), D-IF10 = MBP, D-IF1 host = MBP, P6A.7 with two hosts, P15A.6 on the MBP, the Phase 7 rollout without the mini.
- **(a) SSH approved:** the owner runs the P0A.5 pack, the mini parts of P0A.2 and of P1C.1/P1C.4, `git config core.hooksPath scripts/git-hooks`, `git status; git log @{u}..`, and pulls the release. The pack also installs Tailscale on the mini, logs it in and enables Remote Login (OPS-6; the mini is not on the tailnet today). Every mini command prefixes the explicit DSN (`INVENTORY_DATABASE_URL=postgresql://localhost:15432/cars`), because the mini's `.env` points at its own Postgres (668 rows went to the wrong DB on 09-26); the pack prints the DSN's `host` parameter before any write.
- **Acceptance:** one of the two records exists. The rule "the mini does not scan until P1C is done" holds.

**Workflow shape.**
- Wave A: N4.1, P15B.6 (1 reviewer each).
- Wave B: N4.3, N4.4 (owner).
- Wave C: N4.5. It can ride the same owner deploy session as N3.13 or N5's web deploy, as an ordered step.
- About 8 agents.
- Checkpoints: `workspace/phase_runs/N4/`; before the owner's service-settings pack and the N4.5 deploy the workflow writes RESUME.md with the earliest restart time and ends.

**Exit gate.**
- The dry runs pass with no deprecation warning.
- A web deploy through the new path is healthy.
- The scanner-nightly settings pack is prepared (applied only under N4.1 option (a), or in N12.2), with no fleet started and no new scanner-nightly deployment.
- `grep -rn "scanner-worker\|scanner-scheduler" deploy Dockerfile* docker-compose.yml` returns only history docs.
- Done by 2026-11-20.

**Release step.** Patch; the web deploy proves the path.

**Prod steps and who runs them.** Owner: web service settings (after web-source-clear), the scanner-nightly pack only under option (a), the Docker smoke, the migration check and the web deploy.

**Owner decisions / actions.** D-N24, D-N2; the service-settings pack; the mini pack if approved.

---

### N5 Migration foundation (Phase 4)

**Goal.** A migrator that can target, record-only, lock and time out; a complete V026 chain; strict mode that propagates. This unblocks V027-V029 (N8, N9) before the first prod fleet.

**Entry conditions.**
- N1 merged.
- D-DB5, D-DB6, D-DB7, D-PR8, D-N30, D-N1 answered.
- PG18 client present (N1.5).

**Units.** P-unit specs: REMEDIATION_PLAN "Phase 4: Migration foundation" (line 1182).

| ID | Source | What | Kind | Files / area | Depends | Acceptance |
|---|---|---|---|---|---|---|
| P4.1 | plan Phase 4 | Schema drift report: chain-built scratch DB vs a live DB; read-only prod mode for the owner | code | `backend/scripts/schema_drift_report.py` (new) | none | 0 unexplained drift on `cars` and `cars_cleanroom` |
| P4.2 | plan | `migrate.py`: conditional record-only V019, `--record-only`, `--target`, advisory lock, `lock_timeout` | code | `backend/scripts/migrate.py` | none | per plan |
| P4.3 | plan | V026 chain completeness (D-DB7 adopt, D-PR8 keep `haiku_spec_cache`, adopt the 4 log tables per D-N30). Stale: no `*_embeddings` tables exist; drop that part | code | `migrations/V026__*.sql` | P4.2 | fresh chain = `cars` in P4.1 |
| P4.5 | plan | Strict mode propagates; fix "prod at V018" docstrings (`schema_version.py:19`, `inventory_pg.py:302`) | code | per plan, incl. `backend/scanner/database.py` (`_ensure_schema`) | P4.2 | per plan |
| P4.6 | plan | Gate hot-path runtime DDL (`recipe_store._ensure_table` now `:142-170`; `inventory_pg.py:338`); fix the `SET LOCAL lock_timeout` leak | code | per plan | P4.3, P4.5 | per plan |
| P4.7 | plan | Comment-aware placeholder rewrite (`inventory_pg.py:117`) | code | per plan | none | per plan |
| P4.8 | plan | Apply V026: `cars` locally (backup first); prod by the owner; **not** `cars_cleanroom` (frozen per D-N1). Step 2.3 (P4.4) skipped | prod-owner + MAIN | `docs/ops_log/P4.8.md` | P4.3, P4.6 | `schema_migrations` top = 26 on `cars` and prod |

P4.4 is dropped: P0A.2 found 0 duplicate groups.

**Workflow shape.**
- Wave A: P4.1, P4.2, P4.7.
- Wave B: P4.3, P4.5.
- Wave C: P4.6.
- Wave D (MAIN): P4.8 local, then the owner's prod pack.
- Reviewer lens: idempotence, SQLite vs Postgres, no runtime DDL.
- About 14 agents.
- Checkpoints: `workspace/phase_runs/N5/`; before the owner's P4.8 prod pack the workflow writes RESUME.md and ends.

**Exit gate.**
- `python backend/scripts/migrate.py --dry-run --target 26` against a scratch copy lists only V026.
- A second concurrent `migrate` waits or exits (test).
- `roq cars "SELECT max(version) FROM schema_migrations"` returns 26.
- `INVENTORY_SCHEMA_CHECK=strict` refuses a DB missing V026 (test).
- `grep -n "V018" backend/db/schema_version.py backend/db/inventory_pg.py` is empty.
- The owner's prod record shows 26.

**Release step.** Patch. After the prod apply, a web deploy (V026 is already applied, so the `MIGRATE_ON_BOOT` boot step at `scripts/docker-entrypoint-web.sh:21` is a no-op).

**Prod steps and who runs them.** Owner, in order, each verified:
1. PG18 `pg_dump -Fc` of prod to `workspace/backups/prod_<stamp>_preV026.dump`.
2. `pg_restore -l` TOC check.
3. `migrate --dry-run`.
4. `migrate --apply --target 26`.
5. A second prod `migrate --dry-run` prints `database is up to date` (Section 7.0 migration check; if N9 has released by then, V027/V028 are pending: apply them under the same protocol or postpone the web deploy).
6. Owner-approved Docker smoke of the exact web stage.
7. `deploy_web.sh` from the tag.

**Owner decisions / actions.** D-DB5, D-DB6, D-DB7, D-PR8, D-N30; the P4.8 prod pack.

---

### N6 Listing write-path and completeness fixes (before any rehearsal)

**Goal.** Fix the verified write-path and metric defects that would otherwise distort the rehearsal or show on the first prod fleet. These are:
- the stale incomplete index;
- the new-arrival gray area;
- the verifier PHEV bug;
- dict-repr trims and template-token colors;
- junk prices;
- placeholder photos;
- order-number "VINs" and model year 0;
- used-car mileage coverage;
- the displayed drivetrain, which follows vPIC from v1.6.0 on (P8A.2 and P8A.6 pulled forward from N15; owner rule 4).

**Entry conditions.**
- N2.1 merged (it owns `listing_completeness.py` first).
- D-N13, D-N14, D-N15, D-N16, D-N17 answered; D-PR1, D-PR4, D-PR6, D-PR7 answered for P8A.2 (otherwise P8A.2 and P8A.6 are BLOCKED and the rest continue).

**Units.**

| ID | Source | What | Kind | Files / area | Depends | Acceptance |
|---|---|---|---|---|---|---|
| N6.1 | IL-03, X2, X5 | Index rebuild once per fleet after the vPIC heal; `rules_version` + `last_rebuilt_at` in `incomplete_listings_meta` (key/value, V001:1034); chunked, advisory lock | code | `backend/scripts/fleet_scan.py:354-362` (post block only), `backend/db/incomplete_listings_db.py` | N2.1 | `cars_cleanroom` copy: 4,581 stale flags → 0 |
| N6.2 | IL-04 | Re-sync cars touched by backfill, catalog link and vPIC heal | code | `post_write.py:62-63`, vPIC heal path | N6.1 | 3-dealer scratch scan leaves 0 stale flags without a rebuild |
| N6.3 | IL-05 (part 1) | Admin incomplete counts active-only | code | `data_quality_repo.py`, admin repos | none | admin count = active-only recompute |
| N6.4 | IL-13, D-N13 | New-arrival "pending" class and "details coming" display; `incomplete_report --window N` | code | `listing_completeness.py`, index module, templates | N6.1, D-N13 | about 1,280 (±15%) cars reclassified on `cars` with N=3, window measured per D-N13 |
| N6.5 | W04, lean N2.7, FP16 | `_clean()` unwraps `{_type,en_US}`, treats `[[...]]`, single-bracket `[...]` tokens (e.g. `[extra_cost_color]`), option-code colors (alphanumeric codes of ≤3 characters such as `X9`, `21`), `null`, `''` as missing; upsert treats those stored values as overwritable; invariant; heal script (dry run) | code | `backend/scanner/utils/vdp_extras_parse.py:94-96,143,146`, a new heal script, invariant | none | heal dry run targets 1,228 + 882 rows, plus the single-bracket and option-code rows counted separately |
| N6.6 | W03, FP05 | Verifier: PHEV bucket `phev`; MINI door counts; vPIC decode errors not discrepancies | code | `backend/scripts/scan_lab_report.py:184-199` | none | `vpic_fuel` false positives → only the 1 non-PHEV dealer's rows |
| N6.7 | IL-06, D-N14 | Price floor in `coerce_price`, low-side plausibility, completeness judges the displayed price, repair script (dry run) | code | `backend/scanner/upsert/rows.py:92-100`, price plausibility module | D-N14 | dry run lists 234 + 205 + 1 rows |
| N6.8 | IL-07, D-N15 | One shared placeholder-image test used by parsers, completeness and a per-dealer photo alarm | code | `backend/parsers/base.py:381-389`, completeness | D-N15 | about 2,970 placeholder-only cars detected |
| N6.9 | IL-09, M2, IL-14, FP17, D-N16, D-N17 | Length ≠ 17 gate at parse and upsert; model year 0 or NULL refused or flagged; mileage joins the pipeline coverage gate (the valid-VIN reconcile baseline moved to P5A.1/P5A.4/P5A.7, which own the reconcile files) | code | `backend/parsers/dealer_eprocess.py:171-172` and other length gates, `backend/scanner/pipeline/constants.py:25-27` | D-N16, D-N17 | 0 non-17 writes and 0 model-year-0 writes in tests |
| P8A.2 | plan Phase 8A (pulled from N15) | `backend/vehicle_facts/precedence.py` (pure) | code | `backend/vehicle_facts/precedence.py` (new), `backend/vehicle_facts/__init__.py`, tests | D-PR1/4/6/7 | rank-pair table tests; 4x2 never maps to AWD |
| P8A.6 | plan + W09 (pulled from N15) | Read-time drivetrain follows the vPIC rule | code | `backend/enrichment/verified_specs/drivetrain.py`, tests | P8A.2 | the 15 live-confirmed cars display the vPIC value |

**N-unit specs.**

**N6.1 Index rebuild after each fleet.**
- **What:**
  - Add `fast_rebuild_incomplete_listings_index` to the `fleet_scan` post steps. It runs once per fleet (not per shard), after the pipeline vPIC heal and reconcile.
  - Store `rules_version` and `last_rebuilt_at` as keys in `incomplete_listings_meta`; rebuild whenever the code's rules version differs.
  - Rebuild in chunks of ≤20k cars under a Postgres advisory lock, writing to a staging table and swapping in one transaction. This avoids the DELETE+INSERT race (X5).
  - One-off rebuild of local `cars` with owner OK.
- **Why:** one scan's per-car syncs left 4,581 complete cars flagged, because the sync runs before backfill (`post_write.py:62-63`) and `fleet_scan` never rebuilds. Locally, 8,738 retired cars sit in the index.
- **Acceptance:**
  - On a `createdb -T cars_cleanroom` copy, stale flags fall 4,581 → 0.
  - On `cars` (owner OK), retired index rows fall 8,738 → 0.
  - A 199k-car rebuild takes ≤3 min of wall time, in chunks under 140 s.
  - A `rules_version` bump forces a rebuild.
- **Tests:** `backend/tests/test_incomplete_index_rebuild.py` (new): chunking, version trigger, lock contention.

**N6.2 Re-sync after late writers.**
- **What:** after `apply_structured_spec_backfill_for_car`, the model_specs fill, the catalog link and the pipeline vPIC heal (`vpic_facts.heal_rows`), re-sync the cars they touched. Keep the empty-decode memo (`knowledge_engine.py:1284-1295`).
- **Why:** this closes the X1 window between rebuilds.
- **Acceptance:** a 3-dealer scratch scan (`cars_cr_n6`) leaves 0 stale flags with no rebuild.
- **Tests:** unit test with a stubbed heal that changes fuel and drive.

**N6.3 Admin counts.**
- **What:** `get_incomplete_listings_count`, `get_dealership_issue_stats` and the `get_cars_by_ids` callers count only active rows. The index delete on retirement lands in P5A.4.
- **Acceptance:** on `cars`, the admin count equals the active-only recompute.
- **Tests:** repo tests.

**N6.4 New-arrival window.**
- **What:**
  - Index rows carry the gap's first-seen date and the number of scans it has survived.
  - The window is measured per D-N13: age = (the row's latest `scraped_at` − the gap's `first_seen_at`) ≤ N days, against the row's latest scrape, never the wall clock (local `cars` was last scraped 09-28, so a wall-clock window would put no car inside it). `incomplete_report --window` uses the same definition.
  - A car inside the D-N13 window whose only gaps are price or photos is classified `pending`, not incomplete. The grid shows it, with a plain "details coming" line (no card or box container).
  - `incomplete_report --window N` prints the inside-window and outside-window rates separately. Every later phase quotes the outside-window number.
  - The per-run new-arrival rate goes into `summary_json` in N9.12.
- **Why:** owner rule 3. Cars ≤3 days old are 11.5% incomplete vs about 4.8% baseline. About 25% of price gaps are timing: 77% of the 09-28 cohort's price gaps filled later.
- **Acceptance:**
  - With N=3 on `cars`, about 1,280 cars (±15%) move from incomplete to pending.
  - 0 pending cars are older than N days.
  - A template test shows no box container.
- **Tests:** day-boundary tests; template test.

**N6.5 Junk values from the dealer.com VDP parser.**
- **What:**
  - `_clean()` unwraps the localized object (`{'_type':'s','en_US':...}` gives the `en_US` value or None) and treats `[[...]]`, single-bracket `[...]` tokens (`[extra_cost_color]`), option-code colors (alphanumeric codes of ≤3 characters such as `X9` or `21`), `null` and the empty string as missing (FP16).
  - The upsert treats a stored dict-repr or `[[...]]` value as overwritable, so a rescan heals it.
  - A completeness invariant refuses to count those values as filled.
  - `backend/scripts/heal_vdp_junk_values.py` (dry run by default) sets them to NULL with a provenance note. Local apply needs owner OK; the prod apply is N12.10.
- **Why:** 1,228 trim rows at 52 dealers and 882 color rows at 60 dealers are junk counted as complete. A live check reproduced the dict on 10 cars.
- **Acceptance:** the heal dry run on `cars_cleanroom` targets exactly 1,228 trim and 882 color rows. A fixture of the 10 reproduced cars parses to a real trim or None.
- **Tests:** parser fixtures; upsert overwrite test; invariant test.

**N6.6 Verifier fixes.**
- **What:**
  - `_vpic_fuel_bucket` returns `phev` for plug-in hybrids, matching `catalog/resolver.py:57-72`.
  - MINI "2 Door"/"4 Door" is not a `vpic_model` discrepancy.
  - A vPIC decode error (ErrorCode not 0) is not a discrepancy.
- **Why:** 262 dealers / 3,585 rows flagged; 261 of those dealers' examples are all correct PHEVs; 18 inaccurate verdicts rest on this alone.
- **Acceptance:** re-assessing the stored clean-room data drops the `vpic_fuel` flags to the 1 non-PHEV dealer's rows. The 18 PHEV-only verdicts are re-graded. creviermini is clean.
- **Tests:** table tests per bucket.

**N6.7 Prices.**
- **What:**
  - `coerce_price` refuses values under the D-N14 floor and over $1M, with a provenance note.
  - A low-side plausibility check withholds a value repeated across ≥5 models at one dealer.
  - Completeness judges the price that is displayed.
  - `backend/scripts/repair_junk_prices.py` (dry run by default).
- **Why:** 234 active prices at $10-$999 (harperacura 89 at $699-799, ourismanhondaoftysonscorner 36 at $999, and others), 9 repeated-value pairs (205 cars), and one at $6,001,323.
- **Acceptance:** the dry run on `cars_cleanroom` lists 234 + 205 + 1. A 100-car sample of prices ≥$1,000 shows 0 nulled.
- **Tests:** unit tests for the floor, the cap and the repeated-value rule.

**N6.8 Placeholders.**
- **What:** one helper `is_placeholder_image(url, dealer_context)` covering:
  - the tokens `photo_unavailable`, `unavailable_stockphoto`, `coming_soon`, `comingsoon`, `default.jpg`, `/static/placeholder.svg`;
  - the same image on many cars at one dealer.
  It is used by `parsers/base.py:381`, by completeness (per D-N15) and by a per-dealer photo-coverage alarm.
- **Acceptance:** about 2,970 placeholder-only fresh cars detected (`unavailable_stockphoto` 984 at 52 dealers; Chapman `default.jpg` 416); 0 false positives in a 50-image hand sample; nelsonmazdamurfreesboro trips the alarm.
- **Tests:** a parser test per token; the shared-image heuristic.

**N6.9 Identifier gate and mileage coverage.**
- **What:**
  - Refuse identifiers whose length is not 17 at parse (`dealer_eprocess.py:171-172` and the other length-only gates) and at upsert.
  - Refuse or flag model year 0 or NULL at upsert (FP17: 1,019 rows at 9 dealers in the clean room).
  - Mileage joins the pipeline coverage gate (`pipeline/constants.py:25-27`); used rows at 0 mi are reported, and F4 rows are labelled per D-N16.
  - The "reconcile baselines count valid VINs only" change moves to P5A.1/P5A.4 (scanner) and P5A.7 (pipeline), which own `inventory_reconcile.py` and `pipeline/reconcile.py` (Section 3.3).
- **Why:** 410 Metro Honda order-number rows in `cars` and prod; 187 non-17 identifiers in the fresh scan (koonstoyotatysonscorner 152); 1,374 used rows at 0 mi; 1,019 rows with model year 0 or NULL.
- **Acceptance:** tests refuse the order-number and Koons shapes and model year 0; coverage reports 1,374 used-at-0 rows (497 MY2026+) on `cars_cleanroom`; the model-year-0/NULL count (1,019) is re-measured and the target for new writes is 0.
- **Tests:** parser and upsert tests.

**Workflow shape.**
- Wave A (parallel worktrees): N6.3, N6.5, N6.6, N6.7, N6.8, N6.9, P8A.2. N6.5, N6.7 and N6.8 merge serially (all touch `listing_completeness.py`).
- Wave B: N6.1, then N6.2 (its 3-dealer `cars_cr_n6` scan is MAIN, through the N1.3 scripts) and N6.4; P8A.6.
- 1 reviewer per unit.
- MAIN at the end: the `cars` rebuild (owner OK); the N6 re-baseline (exit gate).
- About 20 agents.
- Checkpoints: `workspace/phase_runs/N6/`; before the owner's OK on the local rebuild the workflow writes RESUME.md and ends.

**Exit gate.**
- The acceptance tool on a `cars_cleanroom` copy after rebuild shows stale flags 0, junk-value heal targets 1,228/882, and `vpic_fuel` PHEV false positives 0 on re-assessment.
- Re-baseline: the N1.3 tool runs on `cars_cleanroom` (read-only) under the N6 rules and writes `analysis/baseline_r1_rules_<rules_version>.json` with per-code counts. N6 makes junk honest (N6.5 turns 1,228 dict trims and 882 token colors into gaps, N6.7 nulls about 440 prices, D-N15/N6.8 count about 2,970 placeholder-only cars as photo-less outside the window), so later incomplete gates (N11.3, P6B.8, N21.12, N36.8) compare only within one `rules_version`.
- The 15 live-confirmed drivetrain cars display the vPIC value (P8A.6).
- `incomplete_report --window 3` prints both rates.
- Targeted pytest files pass.

**Release step.** Patch, tagged; no deploy. Its web-facing changes reach prod with the next web deploy from a later tag (Section 7.0); the scanner changes ship with v1.6.0 in N12.

**Prod steps and who runs them.** None here. The prod repairs run in N12.10.

**Owner decisions / actions.** D-N13-D-N17, D-PR1, D-PR4, D-PR6, D-PR7; OK on the local rebuild.

---

### N7 Postgres harness core and failure shapes on Postgres (14A core, D-PLAN1)

**Goal.** Put the guard, retirement and upsert SQL under real Postgres tests before N8 and N9 change it.

**Entry conditions.**
- N5 merged (the chain builds V001-V026).
- D-TC3, D-TC6, D-TC9, D-PLAN1 answered.
- N1.7 ruleset exists, because its contexts change here.

**Units.** P-unit specs: REMEDIATION_PLAN "Phase 14A" (line 3232).

| ID | Source | What | Kind | Files / area | Depends | Acceptance |
|---|---|---|---|---|---|---|
| P14A.1 | plan Phase 14A | pg marker, migration-built template, per-test clone, guards | code (L) | `backend/tests/pg_harness*`, `backend/tests/conftest.py` | none | refuses `cars`, `cars_cleanroom`, non-local hosts |
| P14A.2 | plan | Shared seed helpers | test | `pg_harness` | P14A.1 | per plan |
| P14A.3 | plan | CI `pytest-pg` job, pg18 + pgvector 0.8.x (replaces `pytest-integration`); owner updates ruleset contexts | ops | `.github/workflows/ci.yml` | P14A.2 | CI green; `rules/branches/main` lists `pytest-pg` |
| P14A.4 | plan | Move live-PG tests onto the harness; prove the chain on a real server | test | per plan | P14A.2 | template = V026 |
| P14A.5 | plan | PG test: adapted SQL | test | per plan | P14A.2 | per plan |
| P14A.6 | plan | PG test: upsert golden parity | test | per plan | P14A.2 | SQLite = PG rows |
| P14A.7 | plan | PG test: concurrent upserts, incl. the VIN-owner backstop (`upsert/sql.py:150-155` compares `scraped_at` as text) | test | per plan | P14A.2 | one owner per VIN |
| P14A.8 | plan | PG test: pin W1, W2, W3 and the portal retire path | test | per plan | P14A.2 | rowcounts asserted |
| N7.9 | risk-first N4.1, WI-14 | PG versions of the N1.4 shapes, xfail strict | test | `backend/tests/test_cleanroom_shapes_pg_2026_10.py` (new) | P14A.2, N1.4 | collected, xfailed on PG |

P14A.9 (recipe CAS) moves to N13, because it needs P7.1.

**N-unit spec, N7.9.**
- **What:** the same shapes as N1.4, run on Postgres through the harness, plus one test of the guard backstop under two concurrent connections.
- **Why:** every guard and retirement test is SQLite-only (`conftest.py:82-92`). The backstop's text comparison of `scraped_at` behaves differently on PG.
- **Acceptance:** collected and xfailed on PG. N8 and N9 exits flip them.
- **Tests:** the file itself.

**Workflow shape.**
- Wave A: P14A.1.
- Wave B: P14A.2, P14A.3.
- Wave C (parallel): P14A.4-P14A.8, N7.9.
- Reviewer lens: guard strength, no flakiness, no non-disposable DSN.
- About 14 agents.
- Checkpoints: `workspace/phase_runs/N7/`; before the owner's ruleset-context PUT the workflow writes RESUME.md with the earliest restart time and ends.

**Exit gate.**
- CI `pytest-pg` green on `phase/N7`.
- `gh api repos/asarrafi47/DealershipScanner/rules/branches/main` lists `pytest-pg`.
- A test proves the harness refuses `dbname=cars`.

**Release step.** Patch; no deploy.

**Prod steps.** None.

**Owner decisions / actions.** D-TC3, D-TC6, D-TC9, D-PLAN1; update the ruleset contexts (`gh api` PUT).

---

### N8 Attribution fix wave: per-car rooftop identity, persisted evidence tier, evidence-aware VIN ownership

**Goal.**
- Group feeds name the true store per car.
- Sister and third-rooftop cars are refused at the gate.
- First-writer-wins is replaced by a persisted evidence tier plus the owner's pool policy.
- Cross-wired recipes are repaired.
- VDP budget is no longer spent on refused cars.
- Each fix is proven by a targeted clean-room re-run of the affected dealers.

**Entry conditions.**
- N1 exit (dossiers and maps; D-N3-D-N8).
- N5 merged (`migrate --target`; P4.5 done in `database.py`).
- N8.6 also waits until N9 is on main (V027/V028 for migration order; P5A.5 in `database.py`) and until N7's P14A.2 and N7.9 have merged (its PG acceptance runs on the harness).

**Units.**

| ID | Source | What | Kind | Files / area | Depends | Acceptance |
|---|---|---|---|---|---|---|
| N8.1 | CF-1, WI-4 | Cosmos mapper stamps `_rooftop` from DealerLocatedAtId / VehicleLocation / DealerLocatedAtCity; learned own-site-id hint | code | `backend/scanner/scrapers/dealer_on.py:196-252`, `backend/attribution/place.py` | N1.2 | fixture and targeted re-run checks |
| N8.2 | CF-2, W12 (routing) | `/api/widget/ws-inv-data/` payloads go to `dealer_dot_com` whatever `provider_hint` says; provider-label check at load | code | `backend/parsers/__init__.py:177-193`, `generic_json.py:74-86` | N1.2 | 24/24 rows with `_rooftop` (was 0/24) |
| N8.3 | CF-5a, W16 | Jazel accountName before manifest name (`jazel.py:183`) and ext/int color swap fix (`:172-173`); Chapman lot code → `_rooftop` (`chapman.py:175-216`); OneAudi dealer field (`oneaudi.py:92-138`) | code | those parsers | N1.2 | fixture checks |
| N8.4 | CF-5b, IL-10 | Motive: filter on the primary `dealer_id` (`synth/platforms/motive.py:64`) or gate on the hit's `dealer_id`; fix retry mapping (`motive_ridemotive.py:147-158`); find why the primary dealer.com recipe returned 0 rows | code | Motive files | N1.2 | Lexus split within 5% of live; trim ≥95% at 4 dealers |
| N8.5 | WI-2, CF-3, CF-8, IL-18 (part) | `vdp_host_other_store` refusal (write only); `recipe_identity_check.py`; guessed-URL flag; no KEEP on a weak tier without a roster street | code | `attribution/{match,rooftop,place}.py`, `backend/scripts/recipe_identity_check.py` (new), `vdp_urls.py:17-70` | N1.2 | 12 shared endpoints listed; corpus parity green |
| N8.6 | CF-4, WI-6, WI-12, WI-13, CF-12 (comment) | V029 persisted rooftop tier; evidence-aware guard and backstop; pool policy; evidence-backed refusals struck; moves logged; manual moves honoured | code + migration | `migrations/V029__*.sql`, `database.py:43-46,83-101`, `upsert/{guard,sql,write}.py`, `attribution/rooftop.py:967-968`, `attribution/disown.py:33` (only if `third_rooftop` joins the set), `backend/config/group_pools.json` (new), `apply_attribution_moves.py:376`, `resolve_photo_attribution.py:317` | N8.1-N8.5, N8.11 (or its cannot_assess record), N9 on main, N7 (P14A.2, N7.9), D-N3 | flips N1.4/N7.9 scan-order and unstamped-feed shapes |
| N8.7 | CF-9, W05a, IL-12 (part) | Ownership pre-check before prefetch; exhausted re-check inside the per-host lock; 403/429 counted; gap-first candidate order | code | `phases/dealer_run.py:112`, `vdp/prefetch.py:600,628-636` | N8.6 | 0 VDP fetches on refused VINs |
| N8.8 | RET-9, CF-11, FP01 | CarsCommerce store scope unions a replay-verified Location/`dealer.location` filter over the group pool; fix the silent gate on 2309549 per N1.2; the FP01 group-feed dealers with no conflict rows | code | `synth/platforms/carscommerce_scope.py:182`, `parsers/carscommerce.py` | N1.2, D-N5 | Terry captures the 13 Greensboro cars; gate line appears; FP01 targets in the spec |
| N8.9 | CF-3, CF-12, W19, IL-19, LE-14, D-N4, D-N6, D-N34 | Recipe and roster repair set with one dry-run/apply tool | data-local, MAIN | `workspace/recipes/repairs_2026_10/*.json`, `backend/scripts/apply_recipe_repairs.py` (new), dealer_logs | N1.2; N8.14 for every roster_add with `shared_host` (the 22 AutoSavvy stores) | owner-approved diff; idempotent apply; all five log files per dealer |
| N8.10 | CF-7 | Targeted clean-room re-run of the affected dealers | scan, MAIN | `cars_cr_n8` via the N1.3 scripts | N8.1-N8.9, N8.11, N9.15 | counts in the spec |
| N8.11 | WI-4 (DEP), CF-1 | Conditional: DealerEProcess rooftop stamping, if the N1.2 DEP map finds a per-car store or lot field; otherwise a cannot_assess record and every DEP pair in `group_pools.json` | code (conditional) | `backend/parsers/dealer_eprocess.py:170-195`, `backend/config/group_pools.json` | N1.2, N6.9 | DEP checks in N8.10, or every DEP pair listed before N8.6 starts |
| N8.12 | rule 5 (local applies) | Local V029 apply on `cars`: `pg_dump`, `migrate --dry-run`, owner OK, `--apply --target 29`, verify | data-local, MAIN | `cars`, `workspace/backups/` | N8.6 | `roq cars "SELECT max(version) FROM schema_migrations"` returns 29 |
| N8.13 | D-N4 (owner 10-16) | Unrostered-rooftop detector and profiling queue: every gate refusal that names a rooftop not on the roster is recorded as a candidate; fleet summary and `prefleet_check` list new candidates; the dealer-discovery workflow consumes the queue | code | `backend/attribution/rooftop.py`/`match.py` (refusal record), `backend/scripts/rooftop_candidates.py` (new), `workspace/rooftop_candidates.json` or a table in V029, fleet summary | N8.6 | a fixture feed with an unknown rooftop yields one candidate row (name, address, store id, host, car count, first/last seen) and 0 rows filed under the sister; the candidate list appears in the fleet summary |
| N8.14 | D-N4; N1.11 readiness audit findings 1-4 and N1-N4 (`workspace/phase_runs/N1/N1.11/new_states_readiness.md`), assigned 2026-10-22 | Shared-host dealer identity: stores that share one website host (all 22 AutoSavvy stores on `www.autosavvy.com`; Ted Russell Ford Parkside, `tedrussellford-net-parkside`, on `www.tedrussellford.net`) are told apart by `dealer_id` and their own `dealership_registry_id`, never by URL host. The manifest/roster row carries the registry id and `shared_host`; the scanner uses it and never resolves a registry id from a shared host; the gate's own-store place comes from that registry row; the upsert never replaces a registry id or dealer_url with a host-derived value; the text-search radius and market-price comps place a car by its registry row before its dealer_url host; `/dealership/<id>` resolves per store; host-to-dealer_id helpers skip or refuse shared hosts; the N8.9 apply tool matches registry rows by host plus store id/street, never host alone | code | `backend/scanner/phases/dealer_run_steps/enrich.py:248-263`, `backend/listings/dealer_registry_match.py:13-45`, `backend/scanner/manifest.py:131-205`, `backend/attribution/place.py:36-75`, `backend/scanner/upsert/sql.py:60-61,91`, `backend/scanner/delta_scan.py:181,197-199`, `backend/db/repositories/search/ranking.py:28-70`, `backend/utils/market_price.py:215-232`, `backend/routes/dealership_page.py:66-122`, `backend/scripts/apply_attribution_moves.py:66-71`, N8.9's `apply_recipe_repairs.py` | N8.6 (shares `upsert/sql.py`) | a fixture with 3 stores on one host gives 3 registry rows; each store's scan stamps its own registry id and keeps it on re-scan; the gate place is each store's own street; a ZIP search near store B ranks B's cars by B's coordinates; each `/dealership/<id>` shows its own address and inventory; in N8.10 the 22 AutoSavvy dealer_ids sit on 22 registry rows with 0 cars on a sister's row. N8.9 must not apply any `shared_host` roster_add before N8.14 is merged |

**N-unit specs.**

**N8.1 Cosmos rooftop stamping.**
- **What:**
  - `_map_vehicle_card` sets `row['_rooftop']` from `DealerLocatedAtId`, `VehicleLocation` and `DealerLocatedAtCity/State`.
  - Each store's own site id is learned as a place hint from the N1.2 map. Without it, `rooftop.py:836-844` (`target_rooftop_unidentified`) would refuse a whole multi-store feed and empty the dealer. A missing hint falls back to keep plus a log line.
  - `test_single_store_feed_is_untouched` stays as it is.
- **Why:** cosmos owners hold 3,968 conflict rows. The 10-14 live fetch of cleveland-nissan's feed showed 956 cars over 4 rooftops (Chattanooga 314, Cleveland 287, Dalton 259, Bristol 96). Bentley Cadillac's page 1 held 0 of its own cars among 24.
- **Acceptance:**
  - The cleveland-nissan fixture keeps only Cleveland (25124) cards. The bentleycadillac page-1 fixture keeps 0 of 24.
  - Targeted re-run (N8.10):
    - cleveland-nissan rows with stock suffix CH/DA/BR = 0; mtnviewnissan CH rows ≥300;
    - bentleycadillac rows with photo account don:18411/21776/19779 = 0;
    - Landers rows with don:20573/20574/20624/24529 under the wrong store = 0;
    - no dealer below 80% of its N1.2 expected own-row count.
- **Tests:** fixture tests from saved payloads; `test_rooftop_corpus.py` parity.

**N8.2 ws-inv-data routing.**
- **What:** URL-shape routing sends `/api/widget/ws-inv-data/` to the `dealer_dot_com` parser. `generic_json` claims a payload only when no platform shape matches. Recipes with `provider_hint='unknown'` and a recognized URL shape are retagged at load, with a log line.
- **Why:** 6 recipes (bmwofmonrovia-net, camelbacksubaru, camelbacktoyota, crownlexus, showcasehonda, toyotacarlsbad) lose `accountId`: 1,082 owner rows and 874 claimant rows.
- **Acceptance:** the saved Sonic payload gives 24/24 rows with `_rooftop`. Re-run: bmwofmonrovia rows whose dealer.com photo account is not `sonicbmwmonrovia` = 0.
- **Tests:** routing tests; parser fixture.

**N8.3 Jazel, Chapman, OneAudi.**
- **What:**
  - Jazel uses the per-row `accountName` (then the VDP city) before the manifest name, and swaps `extColor`/`intColor` back.
  - Chapman stamps from the lot code in the photo path (CAE Dodge, CAU Ford, CAY/CAV other stores).
  - OneAudi stamps the dealer field the N1.2 map names; if omnigraph returns none, it records cannot_assess.
- **Why:** 5starford holds 90 third-store and 31 claimant-true rows of 130; chapmanfordaz/chapmandodge 444 rows; Jazel paint words in `interior_color` on 1,613 rows at 33 dealers.
- **Acceptance:** re-run checks:
  - 5starford rows with a VDP city other than North_Richland_Hills = 0;
  - paint words in `interior_color` at the 6 Jazel dealers = 0;
  - chapmanfordaz rows with lot code CAE/CAY/CAV = 0.
- **Tests:** fixtures per parser.

**N8.4 Motive.**
- **What:** filter on the primary `dealer_id` (or gate on `doc.dealer_id`/`dealership`). Fix the trim, body, stock and gallery mapping on the retry recipe shape. Find out why the primary dealer.com recipe returned 0 rows at lexusescondido, lexuscarlsbad, darcarshondatenafly and coastlinecdjr.
- **Why:** the Lexus pair has 624 conflicts; the live split was 305 claimant-true / 221 owner-true / 98 missing. The retry recipe gave 0% trim/body/stock/gallery (about 1,133 blank trims) where 09-28 had 99.7% trim.
- **Acceptance:** the Lexus split matches the live dossier within 5%; trim ≥95% and body/stock above 0% at the 4 dealers.
- **Tests:** fixtures.

**N8.5 VDP-host refusal, recipe identity, weak tier.**
- **What:**
  - An alias-aware host → dealer_id index in `place.py`.
  - When a row carries no store evidence and its non-guessed detail-page host belongs to another roster dealer, the gate refuses the write with reason `vdp_host_other_store`, with parity in the legacy ladder. It is not added to `EVIDENCE_BACKED_REJECTS`, so it never un-lists (D-N7).
  - `vdp_urls.py` marks invented URLs `_source_url_guessed`, and guessed URLs are never evidence.
  - `recipe_identity_check.py` flags identical URL+body across dealers and recipes whose rows' VDP host is not the dealer host or an alias.
  - No KEEP on a weak tier (`name_token_subset`, `rooftop.py:476`) when the roster row has no street.
  - The VDP host is **never** ownership evidence: group sites serve sister cars on their own domain (all 2,706 conflicted VINs, 4,201 conflict rows, at the 4 largest wrong owners).
- **Why:** mbbeverlyhills-com's 1,284 rows come from `www.mbontario.com` (1,278 of them conflict rows with claimant mbontario-com); fletcherjones kept 106 Henderson cars on a weak name match.
- **Acceptance:**
  - The identity check lists the 12 shared endpoints, including mbbeverlyhills = mbontario.
  - Re-run: mbbeverlyhills rows with host `mbontario.com` = 0; mbontario active ≥1,200 (was 11); fletcherjones rows naming "Mercedes-Benz of Henderson" = 0.
  - Corpus parity green; golden changes limited to the new reason.
- **Tests:** `test_rooftop_match.py`, `test_dealer_attribution.py` (MB shape; own-host and group-site-hosted feeds untouched), identity-check tests.

**N8.6 Persisted evidence tier and evidence-aware ownership.**
- **What:**
  - **V029** persists the rooftop tier and a short evidence record per car. These are computed at `rooftop.py:967-968` and discarded today. The column shape is decided in the unit; a side table is acceptable only if the `sql.py:150-155` backstop can read it in the same statement. Legacy rows count as tier `none`.
  - **Guard rule** (prefetch `guard.py:36-67` and backstop `sql.py:150-155`):
    - (1) a claimant with strong in-run stamped evidence (store-scoped feed, rooftop name or site-id match) takes a VIN from an owner whose stored tier is none or weak, whatever the window;
    - (2) a claimant without strong evidence never takes from an owner with stored strong evidence;
    - (3) evidence-less contests follow `backend/config/group_pools.json` (D-N3 primary);
    - (4) pairs not in the file keep the current owner and are reported as `group_feed_unstamped` (N9.12);
    - (5) alias IDs of one store never conflict.
    - Rules (1)-(5) are evaluated whatever the window, so a lapsed guard (prod's state at fleet #1) never falls back to scan order. The 48 h window governs only evidence-less contests between dealers not covered by `group_pools.json`.
  - **Evidence-backed refusals.** P5A.5's protected-VIN set (WI-8) excludes VINs refused for `third_rooftop` or `sibling_rooftop`: those are struck under the two-strike policy, so a class-(d) row already stored under a sibling (on `cars_cr_conv` and on prod) does not stay listed under the wrong store indefinitely. If the unit prefers it, `third_rooftop` joins `EVIDENCE_BACKED_REJECTS` (`disown.py:33`, today `{sibling_rooftop, single_rooftop_is_not_this_store}`) behind the P5A.4 no-retire switch. `vdp_host_other_store` stays outside both (D-N7).
  - Every allowed move writes a `car_move_log` row (`evidence` column exists, V019:79-91).
  - Manual moves are honoured: the guard respects non-reverted `car_move_log` rows, and the move scripts refresh `scraped_at`.
  - The comment at `database.py:43-46` that labels mtnviewnissan-com as CA is corrected (CF-12; it is the Chattanooga, TN store). N8.6 owns the file, so the fix lands here rather than in N8.9.
  - The 48 h default (`database.py:53`) is unchanged.
- **Why:** first-writer-wins in an empty DB came down to alphabetical shard placement. On a populated DB with nightly rescans, a wrong owner is permanent: its row is always under 48 h old.
- **Acceptance:**
  - SQLite and PG tests: a strong claimant beats a none/weak owner; a weak claimant never beats a strong owner; two scan orders give the same owner on mixed fixtures.
  - A stale-owner test (owner row older than 48 h): rules (1)-(5) still decide; nothing falls back to scan order.
  - A stored class-(d) row refused as `third_rooftop` in two runs ≥20 h apart is retired, with a `listing_retirements` row.
  - The backstop agrees with the prefetch on PG (N7 harness).
  - `car_move_log` rows equal the allowed moves.
  - The N1.4 and N7.9 scan-order and unstamped-feed shapes flip to pass.
- **Tests:** `test_upsert_vin_owner_guard.py` new cases; PG concurrency case.

**N8.7 Prefetch quick fixes.**
- **What:**
  - A read-only ownership pre-check before `prefetch_details` (`dealer_run.py:112` runs before persist at `:119`).
  - `exhausted` re-checked inside the per-host lock (`prefetch.py:628-636`).
  - 403/429 counted in `stats['statuses']`.
  - Candidates ordered gap first: price, exterior color, trim, photos (`prefetch.py:600`).
  - TLS profile rotation is left to N18.
- **Why:** VDP budget went to refused cars (bentleygmc 468, mbhsv 234, mtnviewnissan 848); 65 blocked dealers report `statuses={}`.
- **Acceptance:** re-run shows 0 VDP fetches on guard-refused VINs; every blocked dealer has non-empty statuses.
- **Tests:** prefetch unit tests.

**N8.8 CarsCommerce scope and silent gate.**
- **What:**
  - `_carscommerce_store_filter` also unions the group used pool filtered by the store's Location facet or `dealer.location`, verified by replay (per D-N5).
  - Fix whatever N1.2 finds for the silent gate on listing 2309549 and the East Tennessee listings.
  - The FP01 group-feed dealers that have no conflict rows, so no other unit covers them: autosavvy (22 rooftops in 9 states filed under San Antonio; 3,947 active rows in `cars_cleanroom`), infinitiofcharlotte (a multi-brand pool of 2,545 active rows on one INFINITI rooftop), tedrussellford (the gate picked the used lot and refused 666 rows, so 0 of about 549 new Fords were captured; 54 rows stored), kearnymesasubaru (`source_id` = page oem_code returns 42 of 213 new cars and drops the 553-car group used pool; 773 → 90), hendricksubaruhoover (`no_rows`; 99 rows refused by the gate).
- **Acceptance:**
  - terrylabontechevy captures the 13 "Greensboro, NC" RHendrickUsed cars (if D-N5 = yes); a rooftop-gate line appears for 2309549.
  - In the N8.10 re-run: tedrussellford new Fords ≥90% of the site total; kearnymesasubaru ≥90% of the owner-approved scope; hendricksubaruhoover not `no_rows`; autosavvy rows limited to its own San Antonio lot, its other rooftops filed under their own N1.11 profiles (or held where cannot_assess); infinitiofcharlotte non-INFINITI rows filed under their own profiled stores, or held (per D-N4 as answered 10-16).
- **Tests:** fixtures from saved payloads.

**N8.9 Repair set.** One JSON per change, applied by `apply_recipe_repairs.py` (dry run, backup, apply, verify; idempotent). Contents:
- the re-discovered mbbeverlyhills endpoint (dealer-discovery skill);
- coastlinecdjr moved to Algolia `dealer_ids` 2927;
- the 6 ws-inv-data retags if still needed;
- Old Orchard (12 of about 941) and Hendrick Lexus Charlotte (used only);
- registry name fixes: audihuntsville, fjmercedes, 5starford, and the 31 dealers with "Parts"/"Service" names (13,811 rows); the mislabelled mtnviewnissan-com comment in `database.py:43-46` is fixed in N8.6, which owns the file;
- dead roster URLs: mckennasubaru (hijacked), lagunahyundai (dead DNS);
- the N1.11 roster additions (`roster_add_<new_id>.json`, D-N4 as answered 10-16: profile and scan missing rooftops); fletcherjones per D-N6;
- only if D-N34 = yes: the 7 overlapping-recipe dealers (LE-14: andymohrford, dchparamushonda, stevenscreektoyota, huntersvilleford, bmwofmtlaurel, kunilexusofgreenwoodvillage, tuttleclickmazda): one recipe set kept per dealer, the other archived in the repair file. If D-N34 = no, this bullet moves to N13, as answered on 10-08.
- **Acceptance:** the owner approves the dry-run diff; a second apply changes 0 rows; all five log files (`discovery.md`, `location.md`, `scan_runs.md`, `summary.md`, `scan_instructions.md`) exist for every dealer touched.

**N8.10 Targeted re-run.**
- **What:**
  - The owner approves `git switch phase/N8` in the main checkout (Section 7.0).
  - Build `cars_cr_n8` with `scripts/cleanroom/build_db.sh cars_cr_n8`, run `migrate --apply` on it (to 29), apply the repair set.
  - Environment from `scripts/cleanroom/run.env.template --db cars_cr_n8 --art workspace/cleanroom_runs/cars_cr_n8 --roster <roster>`, then `scripts/cleanroom/isolation_snapshot.sh` and `scripts/cleanroom/preflight.sh` with the same arguments and `--sha <phase/N8 HEAD>`; both print OK.
  - Roster: the 109 conflict-group dealers, the 4 retirement dealers, the IL-19 dealers, the 4 Motive dealers, the 5 FP01 dealers (autosavvy, infinitiofcharlotte, tedrussellford, kearnymesasubaru, hendricksubaruhoover) and, if N8.11 stamps DEP, fordofmurfreesboro, nissanofcookeville, parksidekia and hyundaiofcookeville.
  - Scan with `fleet_scan --resume` (N9.15) so a sleep does not restart the pass. Measure with the N1.3 tool against the matching `baseline_r1` subset.
  - Append one dated summary block per dealer from the run root to the canonical `workspace/dealer_logs/<id>/scan_runs.md` and `summary.md`.
- **Acceptance:**
  - conflicts among these dealers fall ≥75%;
  - every CF-7 class-(b) DB check = 0;
  - third-rooftop rows are refused, not filed under a sibling;
  - residual pairs are pool pairs in `group_pools.json` or n=1;
  - no dealer is emptied;
  - the N8.8 FP01 targets and, if N8.11 stamps DEP, the DEP checks are met;
  - `pg_stat_database` writes on `cars_cleanroom` unchanged;
  - all five dealer log files exist for every dealer, and the canonical `scan_runs.md`/`summary.md` carry the run's block.
  Determinism is reported for information only; it is gated in N11.4.

**N8.11 DealerEProcess stamping (conditional).**
- **What:** DEP sits on both sides of the Cookeville and Murfreesboro group conflicts: 647 conflict rows on the owner side and 2,050 on the claimant side (2,185 distinct rows; fordofmurfreesboro → nissanofcookeville 543, → parksidekia 522, → hyundaiofcookeville 222). If the N1.2 DEP/typesense map finds a per-car store or lot field, `dealer_eprocess.py:170-195` stamps `_rooftop` from it with a learned own-store hint, as N8.1 does, and N8.10 adds the four DEP checks. Otherwise the unit records cannot_assess with the payload evidence and lists every DEP pair in `group_pools.json` before N8.6 starts.
- **Acceptance:** either the fixture from a saved DEP payload keeps only the dealer's own cars and N8.10's DEP pairs fall ≥75%, or the cannot_assess record and the `group_pools.json` entries exist.
- **Tests:** parser fixture (stamping case) or a JSON check that every DEP pair is listed (fallback).

**N8.12 Local V029 apply.** MAIN, after N8.6 merges: `pg_dump -Fc` of `cars` to `workspace/backups/`, `migrate --dry-run`, owner OK on the output, `migrate --apply --target 29`, then `roq` verifies 29.

**Workflow shape.**
- Wave A (parallel worktrees): N8.1, N8.2, N8.3, N8.4, N8.8, N8.11, and N8.9 (dealer-discovery with paced fetches, MAIN).
- Wave B: N8.5.
- Wave C, after N9 is on main and N7's P14A.2/N7.9 have merged: N8.6, then N8.7 and N8.12 (MAIN).
- Wave D: N8.10.
- 1 adversarial reviewer per code unit, plus a guard-domain skeptic on N8.6.
- Each code unit also runs a mini re-run of 3-10 dealers into `cars_cr_n8_<unit>` (MAIN, through the N1.3 scripts, on the unit's merged code) and records the numbers in its checkpoint.
- About 30 agents.
- Checkpoints: `workspace/phase_runs/N8/`; re-run state in its run dir (`fleet_scan --resume`, N9.15). Before the wait for N9's release, before the owner's repair-set and N8.12 OKs, and before N8.10 if the owner has not approved the `git switch`, the workflow writes RESUME.md with the earliest restart time and ends.

**Exit gate.**
- N8.10 targets met; reviewers passed.
- `pytest backend/tests/test_cleanroom_shapes_2026_10.py -q` shows the ownership shapes passing.
- The owner approved the repair-set diff.
- `roq cars "SELECT max(version) FROM schema_migrations"` returns 29 after N8.12.

**Release step.** Patch, tagged; no deploy (deploys in N12 as part of v1.6.0).

**Prod steps.** None (prod recipe repairs in P6B.2, N12).

**Owner decisions / actions.** D-N3-D-N8, D-N34; approve the repair-set diff, the dealer-discovery fetches, the scratch DBs, the `git switch` before MAIN scans and the N8.12 apply.

---

### N9 Retirement single writer, provenance, two-strike, honest accounting, pause safety

**Goal.**
- One fail-closed retirement policy, written by the scanner only. It measures prior rows by VIN, never retires on an incomplete walk, and never retires on a first miss.
- Every retirement leaves a provenance row (V027).
- Triage and logs tell captured, refused and written apart.
- Batches survive a pause.

**Entry conditions.**
- N5 merged.
- N6.9 merged before P5A.7.
- D-RC1, D-RC2, D-RC3, D-RC7, D-SR4, D-N10, D-N11, D-N21 answered.

**Units.** P-unit specs: REMEDIATION_PLAN "Phase 5A" (line 1305) and "Phase 5B" (line 1457).

| ID | Source | What | Kind | Files / area | Depends | Acceptance |
|---|---|---|---|---|---|---|
| P5A.1 | plan + RET-2 | Pure `plan_retirement()`; add the Terry/Coastline/Napleton/SCL shapes; baselines count only valid 17-character VINs (moved from N6.9); a 35-day-gap test | code | `backend/scanner/retirement_policy.py` (new) | none | per plan + shapes; 35-day gap with 30% turnover strikes on run 1 and retires on run 2 (rule 4's 90-day fallback branch) |
| P5A.3 | plan | One-condition allow-list as a tracked file, one loader | code | per plan | none | per plan |
| P5A.2 | plan + RET-5, WI-7 | Walk report: stop reason, completeness, `in_union`, recipe-scope hash; truncation logged. Drift: union log now `recipes.py:1695` | code (L) | `recipes.py`, `feed.py`, `dealers_repo.py` | none | 13 cap walks tagged `page_cap` |
| P5B.1 | plan + W06, FP02 | Walk ends on no new RAW VIN (total stop now `recipes.py:1665`) | code | per plan | P5A.2 | group walk passes an all-refused page; in N9.14's scratch DB a re-run of harehonda, jimcolemantoyota, mcdavidhondairving, mercedesdallas and shottenkirkhondarome captures the pages past the first all-sibling page (shottenkirk's 18 in-transit cars on pages 5-6) |
| P5B.2 | plan + RET-10 | One site-total reader; cap from the total, max 150 (D-RC3); `_MAX_REPLAY_PAGES=40` (`:1026`) goes | code | per plan | P5B.1 | 5starford reaches 1,338; autosavvy 5,406 |
| P5A.4 | plan + WI-9, IL-05 (part 2) | Scanner reconcile adopts the policy; guarded UPDATE (`:323-330`); baselines count valid VINs only; dry mode; one no-retire switch covering W1, W2, lifecycle, delta and disown; index rows deleted on retire | code | `inventory_reconcile.py`, `attribution/disown.py` (switch), index module | P5A.1, P5A.3 | switch matrix test |
| P5A.5 | plan + RET-3, WI-8 | Evidence wiring: `run_started_iso` into `after_write.py:124-136`; protected VINs; `drop_unattributable_vehicles` (`database.py:335`) returns the VINs it dropped (moved from N8.6); evidence-backed refusals (`third_rooftop`, `sibling_rooftop`) excluded from protected VINs; recovery source | code | `after_write.py`, `database.py`, `enrich.py` | P5A.2, P5A.4, P4.5 | Terry/Coastline shapes flip; a VIN dropped by `drop_unattributable_vehicles` is protected; a `third_rooftop` VIN is not |
| P5A.6 | plan | Delta scan applies the same policy | code | `delta_scan.py` | P5A.5 | parity test |
| P5A.7 | plan + RET-4, FP02 | Pipeline and lifecycle reconcile report only (`pipeline/reconcile.py:94,153`; `lifecycle.py:335-338`); valid-VIN baselines in the report; `replay_truncated:` verdict; triage alarm per D-N21 | code | `pipeline/{reconcile,lifecycle,assess,triage}.py`, `constants.py` | P5A.5, N6.9 | no UPDATE under `pipeline/`; invalid-VIN and backlog rows (W2's job today) still retire through P5A.1 rule 8 (test); downeyhyundai `replay_truncated` test: after a mid-walk 403 (about 130 new cars missing) the verdict is not ok |
| P5B.3 | plan (V027) | `listing_retirements`, written by every writer, including the admin delete paths | code + migration | `migrations/V027__*.sql`, writers, `schema_repo.py`, `backend/dealer/admin/incomplete_listings_api.py:182`, `backend/db/repositories/cars_repo.py:73` | P5A.4 | rows = UPDATE rowcount per writer; the admin delete paths write `writer='admin_delete'` rows, or are documented as exempt |
| P5B.4 | plan (V028) | Two-strike in `listing_miss_strikes`, ≥20 h apart | code + migration | `migrations/V028__*.sql`, reconcile | P5B.3 | first miss retires 0; `SCANNER_RECONCILE=dry` writes 0 strikes and 0 retirements (would-strike list only) |
| P5B.7 | plan | Replay→persist gap telemetry and parity pin | code + test | per plan | P5A.5 | captured − refused − dropped = written |
| N9.12 | CF-10, RET-1, W01, W14, IL-22, WI-3, CF-15, IL-13 (summary), B4 | Honest accounting, verdicts and logs; `handover_pending` report | code | `pipeline/{assess,triage,dealer_logs}.py`, `fleet_scan.py:383`, `dealers_repo.py` | P5A.2 | identity replay exact; all five log files written |
| N9.13 | RET-8, W07 | Pause-safe batch timeout; killed dealers re-queued; `no scan_runs row` → retry; scan_run for every upsert | code | `pipeline/runner.py:119-122`, `lifecycle.py:73-117` | none | injected SIGSTOP leaves 0 error dealers |
| N9.14 | owner rule 1, RET-1, P5A.8 (shape part, moved here) | Targeted retirement re-run in the clean room | scan, MAIN | `cars_cr_n9` via the N1.3 scripts | P5A.1-P5A.7, P5B.1-P5B.4 | counts in the spec |
| N9.15 | owner rule 7, RET-8 | Fleet resume: `fleet_scan --resume <run_dir>` / `SCAN_RESUME_DIR` | code | `fleet_scan.py`, `dealer_pipeline.py` / `pipeline/run.py` (roster filter) | N9.12 | killed 3-shard run resumes only the remaining dealers |
| N9.16 | rule 5 (local applies) | Local V027/V028 apply on `cars`: `pg_dump`, `migrate --dry-run`, owner OK, `--apply --target 28`, verify | data-local, MAIN | `cars`, `workspace/backups/` | P5B.3, P5B.4 | `roq cars "SELECT max(version) FROM schema_migrations"` returns 28 |

WI-10 (interim provenance in `summary_json`) is dropped: V027 lands in this phase and reaches prod before any live retirement.

**N-unit specs.**

**N9.12 Accounting, verdicts and logs.**
- **What:**
  - Triage and the FLEET SUMMARY report `rows_captured`, `rows_refused_vin_owner`, `rows_refused_rooftop`, `rows_written`, `active`, `inactive` (`fleet_scan.py:383`, `assess.py:104-108`). Error dealers without a scan_run are counted, not null.
  - `assess.py:131` names the VIN-owner guard instead of "rooftop gate refused the rest".
  - `dealer_logs.py:66/68` print rows written and both refusal counts, with event timestamps. The 300-character truncation at `:70` goes.
  - A guard-only `no_rows` never triggers a lifecycle re-synth.
  - "ok" is blocked by a coverage drop over 30% against the previous scan, by a missing site-total check, or by 100% placeholder photos.
  - Thin verdicts still run NHTSA verification.
  - All five dealer log files (discovery.md, location.md, scan_runs.md, summary.md, scan_instructions.md) are written or appended for every run, thin and inaccurate runs included.
  - Flag `group_feed_unstamped` (≥25 conflicts and ≥20% against one sister) and print the pair. `dealer_feed_scope='group'` is set only with owner OK (D-N8).
  - Report `handover_pending` when a retired VIN has a fresh `vin_owner_conflicts` row naming the dealer as owner (B4).
  - The per-run new-arrival incomplete rate goes into `summary_json`.
- **Why:** the "8.8k retirements" came from triage counting the feed before the guard. 43 of 434 `http_first` log lines are unparseable. kearnymesasubaru dropped 773 → 90 and still got "ok".
- **Acceptance:**
  - An offline replay of the 10-13 triage gives 208,233 / 9,610 / 198,623 written, and 199,457 active including the 834 error-dealer rows.
  - 0 unparseable `http_first` lines.
  - kearnymesasubaru, groovetoyota and mymetrohonda are no longer "ok".
  - mtnviewnissan and overdrive-usa are not routed to re-synth.
  - A dealer_logs test shows all five files written for an ok, a thin and an inaccurate run.
  - A `handover_pending` fixture is reported.
- **Tests:** `test_assess_stamped_rows.py` extended; `test_triage_group_feed_flag.py` (new); dealer_logs tests.

**N9.13 Pause-safe batches.**
- **What:**
  - The batch timeout uses active time (monotonic clock with suspend detection) instead of wall time (`runner.py:119-122`).
  - Killed dealers are re-queued once with reason `killed_by_timeout`.
  - `route_verdict` (`lifecycle.py:73-117`) sends `no scan_runs row` to a retry.
  - Dealers upserted without a scan_run get one, plus logs (righthonda/selmanchevy shape).
- **Why:** the 21 h pause killed 8 batches (rc=-9 after 78,209-78,778 s) and left 21 dealers unscanned with a misleading reason.
- **Acceptance:** a test injects a SIGSTOP longer than `--scan-timeout` into a 3-dealer run: 0 dealers end as `error / no scan_runs row`, and killed dealers re-run once.
- **Tests:** `backend/tests/test_runner_pause_safe.py` (new).

**N9.14 Targeted retirement re-run.**
- **What:** the owner's proof for the in-run retirements ("prove each fix by re-running the affected dealers in the clean room"), before N9 releases.
  1. The owner approves `git switch phase/N9` (Section 7.0). Build `cars_cr_n9` empty with the N1.3 scripts and `migrate --apply` it to 28; preflight OK with `--sha <phase/N9 HEAD>`.
  2. Run terrylabontechevy, coastlinecdjr, napletonhondamortongrove and southcountylexus with their 10-13 first-pass recipes, then with the re-synthesized retry recipes, in the same DB (the first-pass/retry shape, moved here from P5A.8).
  3. On the scratch DB only, age the strike rows by 21 h (`UPDATE listing_miss_strikes ...`, logged in the run dir) and run the retry recipes again.
  4. Re-run the 5 FP02 dealers for P5B.1 (harehonda, jimcolemantoyota, mcdavidhondairving, mercedesdallas, shottenkirkhondarome).
  5. Append the dated summary blocks to the canonical dealer logs.
- **Acceptance:**
  - in-run retirements at the 4 dealers: 21 → 0 after the retry;
  - strikes after the retry: Coastline 3, Napleton 1, South County Lexus 1, Terry 0;
  - after the aged second pass exactly 3/1/1 retire, Terry 0;
  - `listing_retirements` rows equal the inactive delta;
  - the P5B.1 FP02 targets are met;
  - `pg_stat_database` writes on `cars_cleanroom` unchanged.
- **Tests:** none beyond the unit tests; the numbers go into the checkpoint.

**N9.15 Fleet resume.**
- **What:** `fleet_scan --resume <run_dir>` (or `SCAN_RESUME_DIR`). It reuses the run dir and its shard roster files, skips dealers that already have a triage entry or a `scan_runs` row with `started_at` ≥ the run start, and appends to the existing `triage.json`. Today `fleet_scan` makes a fresh run dir every time (`fleet_scan.py:313`) and neither it nor `dealer_pipeline.py` has a resume path, so a Mac sleep mid-pass forces a full re-run of a 458-dealer pass.
- **Acceptance:** a test kills a 3-shard run midway; the resume scans only the remaining dealers and the merged triage has one entry per dealer.
- **Tests:** `backend/tests/test_fleet_scan_resume.py` (new). N8.10 and the N11 passes depend on this unit.

**N9.16 Local V027/V028 apply.** MAIN, after P5B.3 and P5B.4 merge: `pg_dump -Fc` of `cars` to `workspace/backups/`, `migrate --dry-run`, owner OK on the output, `migrate --apply --target 28`, then `roq` verifies 28.

**Alarm (P5A.7, D-N21).** Nightly runs alarm when a dealer retires more than 10% of its prior active rows. The first live prod retirement (N12.8) and its rehearsal (N11.7 pass D) use min(60%, 10% + 1.5% × days), where days = run start − the median `scraped_at` of the rows being retired. A P5A.7 test: rows last scraped 35 days ago, struck in run 1 and retired in run 2, give a threshold of min(60%, 62.5%) = 60%.

**Workflow shape.**
- Wave A: P5A.1, P5A.3, P5A.2, N9.13.
- Wave B: P5B.1 → P5B.2 (same file as P5A.2); P5A.4; N9.12.
- Wave C: P5A.5, P5B.3, N9.15.
- Wave D: P5B.4, P5A.6, P5A.7, P5B.7.
- Wave E (MAIN): N9.16, then N9.14.
- 1 reviewer per unit plus a retirement-domain reviewer at merge.
- About 30 agents.
- Checkpoints: `workspace/phase_runs/N9/`; before the owner's N9.16 OK and before N9.14's `git switch` the workflow writes RESUME.md and ends.

**Exit gate.**
- `pytest backend/tests/test_cleanroom_shapes_2026_10.py -k "retire or retry or identity" -q`: retirement and identity shapes pass.
- The PG versions pass in CI `pytest-pg` once N7 is merged.
- `grep -rn "UPDATE cars SET listing_active" backend/scanner/pipeline` is empty.
- `roq cars "SELECT max(version) FROM schema_migrations"` returns 28 after N9.16.
- The identity replay is exact.
- N9.14 targets met (21 → 0 in-run retirements; 3/1/1/0 strikes, then 3/1/1/0 retirements).

**Release step.** Patch, tagged; no deploy. This must be on main before N8.6 starts. From this release on, a web deploy before N12.2 first applies V027/V028 to prod with a dump, or waits (Section 7.0).

**Prod steps.** None (V027/V028 go to prod in N12.2).

**Owner decisions / actions.** D-RC1, D-RC2, D-RC3, D-RC7, D-SR4, D-N10, D-N11, D-N21; the N9.16 apply OK; the `git switch` and `cars_cr_n9` for N9.14.

---

### N10 Scan controls and pre-fleet tooling (6A, per-dealer locks, audit tool)

**Goal.** Make scanner-nightly safe to deploy and run:
- a run window, a fleet lock and leases;
- per-dealer locks, so a second host can never scan the same dealer concurrently;
- a kill-switch matrix;
- recipe hygiene;
- gap cover limited to evidenced owners;
- a pre-fleet check;
- the retirement audit tool;
- ownership snapshots.

**Entry conditions.**
- N5 merged.
- Wave 3 needs N8 and N9 on main.
- D-IF5, D-IF10, D-SR2 answered.

**Units.** P-unit specs: REMEDIATION_PLAN "Phase 6A" (line 1589), "Phase 11A" (line 2814), "Phase 5B" P5B.5/P5B.6 (line 1457).

| ID | Source | What | Kind | Files / area | Depends | Acceptance |
|---|---|---|---|---|---|---|
| P6A.1 | plan Phase 6A | Advisory scan leases (`try_lock_dealers`) | code | `backend/scanner/scan_leases.py` (new) | none | PG test: second holder refused; released on session end |
| P6A.5 | plan | Pull Railway volume logs; merge into the MBP canonical `dealer_logs` tree (D-IF10 = MBP) | code | new script | none | idempotent merge, no duplicate blocks |
| P11A.1 | plan Phase 11A (pulled forward) | `ScanLockHeld`, exit 75 | code | per plan | none | per plan |
| P11A.4 | plan (pulled) | Atomic lock with `fcntl.flock` | code | per plan | P11A.1 | race test |
| P5B.5 | plan Phase 5B | `audit_retirements.py` classify and report (read-only); exclude file seeded with lexusofknoxville 09-27 (1,417 rows) | code | per plan | P5B.3 (N9) | classifies the 09-23..09-29 retirements in `cars` |
| P5B.6 | plan | Audit verify (replay/VDP, host match), apply and rollback | code | per plan | P5B.5 | rollback diff = 0 on a copy |
| N10.7 | CF-14 tooling, value-first N5.12 | `\copy`-based ownership snapshot and diff; read-only prod census pack | code | `scripts/ownership_snapshot.sh` (new), `backend/scripts/ownership_diff.py` (new) | none | reproduces the RCA's old-vs-clean split |
| P6A.2 | plan | Run window `SCAN_FLEET_WINDOW_UTC`, fleet lock (exit 3), idle-hold cap, post-run hold | code | `fleet_scan.py` (after N6.1, N9.12 and N9.15), `scripts/railway_scan_fleet.sh` | P6A.1 | start outside the window idles |
| N10.8 | judges (same-commit determinism) | Shard-order knob `SCAN_SHARD_ORDER` (`reverse` or `seed:<n>`) | code | `fleet_scan.py` (`make_shards`, `:139-149`) | P6A.2 | test: two orders give different first-writer sequences; the order is recorded in `fleet_summary.json` |
| P11A.2 | plan (pulled) | Pipeline launcher retries on 75; `scan_skipped` through triage | code | per plan | P11A.1, N9 | per plan |
| P11A.3 | plan (pulled; D-SR2 (c)) | Per-dealer advisory lock in `dealer_run` and `delta_scan` | code | `dealer_run.py`, `delta_scan.py`, `cli.py`, `pipeline/{run,assess,lifecycle,triage}.py` | P11A.1, P11A.2, P6A.1, N8, N9 | two hosts on one dealer: second skips |
| P6A.3' | plan P6A.3 reworked | Gap cover only for owners with a persisted strong tier; default stays 48 h | code | `database.py` guard section | N8.6 | 16-day gap test |
| P6A.6 | plan + W12 | Refuse VIN-list, widget and single-VIN recipes at capture (`promote_from_ledger` now `recipes.py:889`); stale the 13 existing ones locally; list one-condition and certified-only recipes for N19 | code + data | `recipes.py`, recipe store | N9 (recipes.py) | 13 recipes staled with backup |
| P6A.4 | plan | Kill-switch matrix tests and doc (window, lock, lease, `SCAN_FLEET`, `SCANNER_RECONCILE=dry`, the no-retire switch, guard hours, per-dealer lock) | test + docs | `test_fleet_scan_safety.py`, RAILWAY_SCANNING.md | P6A.2, P6A.3', P11A.3 | every switch has a passing row |
| P6A.7 | plan, slimmed | `scripts/prefleet_check.sh` + checklist | ops | per plan | P6A.2, P6A.4 | prints PREFLEET OK or lists failures |

**N-unit spec, N10.7 ownership snapshots.**
- **What:** `scripts/ownership_snapshot.sh <dsn-env> <label>` writes `\copy (SELECT vin, dealer_id, listing_active, scraped_at FROM cars) TO workspace/backups/ownership_<label>_<stamp>.csv` through a read-only session. `ownership_diff.py <before> <after>` reports moves per (from, to) pair, cross-referenced with `car_move_log`. A read-only census pack for prod `vin_owner_conflicts` by pair.
- **Why:** N12 re-files prod ownership under the new rules (D-N22). Every move must be visible and reversible.
- **Acceptance:** the diff of `cars` (09-28) against `cars_cleanroom` VIN sets reproduces the RCA split: claimant 3,941 + 59, owner 1,344 + 26, third dealer 931 + 289, absent 3,048 (± the classification method).
- **Tests:** diff unit tests on fixtures.

**N10.8 Shard-order knob.** `make_shards` (`fleet_scan.py:139-149`) sorts by (−weight, dealer_id), and in an empty DB every weight is the default 50, so the order is always alphabetical and N11.4's reversed pass cannot be run without a knob. `SCAN_SHARD_ORDER=reverse` reverses the per-shard dealer order and the shard assignment; `seed:<n>` shuffles both deterministically. The value is written to `fleet_summary.json`. **Acceptance:** a test shows two orders give different first-writer sequences on the same roster. N11.4 depends on it.

**P6A.3' rework.** Guard hours are extended to cover a scan gap only for owners whose stored tier is strong. An owner with no or weak evidence gets no gap cover, so the evidence rules can re-file its rows. The blanket 72 h default from the plan is not adopted; `database.py:53` stays at 48.0 unless D-IF5 is answered differently. Tests: after a 16-day outage, a pool VIN with no evidence follows `group_pools.json`; a strong owner keeps its VIN against a weak claimant; a strong claimant still takes over from a weak owner.

**P6A.7 slimmed.** Checks:
- host parity: the MBP and Railway (the mini only if N4.4 revived it) on the same commit;
- `SCANNER_EGRESS_TAG=railway` on Railway; `SCANNER_ALLOW_BROWSER` unset;
- `SCAN_FLEET` empty or 0, except that `SCAN_FLEET=1` is allowed only with a valid `SCAN_FLEET_WINDOW_UTC` and, from N12.11 on, the cron set (before N12.11 a run is started by an explicit owner step, N12.7/N12.8);
- no locks held; no idle-in-transaction sessions;
- `recipe_identity_check.py` clean;
- guard config as expected;
- `schema_migrations` top = 29 on the target DB.

**Workflow shape.**
- Wave 1 (any time after N5): P6A.1, P6A.5, P11A.1, P11A.4, P5B.5 (after P5B.3 merges), N10.7.
- Wave 2: P6A.2, P5B.6, then N10.8 (same file as P6A.2).
- Wave 3 (after N8 and N9 releases): P11A.2, P11A.3, P6A.3', P6A.6.
- Wave 4: P6A.4, P6A.7 (both edit RAILWAY_SCANNING.md, so serial).
- 1 reviewer per unit.
- About 22 agents.
- Checkpoints: `workspace/phase_runs/N10/`; before the wait for the N8 and N9 releases and before the owner's recipe-hygiene OK the workflow writes RESUME.md and ends.

**Exit gate.**
- The kill-switch matrix is green.
- The N10.8 shard-order test passes.
- `scripts/prefleet_check.sh --target local` prints `PREFLEET OK`.
- A PG test shows two processes cannot hold the same dealer lock.
- The 13 bad recipes are staled locally, with a backup named in the ops log.
- The audit-tool rollback diff on a copy is 0.

**Release step.** Patch, tagged; no deploy.

**Prod steps.** None.

**Owner decisions / actions.** D-IF5, D-IF10, D-SR2; OK on the recipe-hygiene local apply counts.

---

### N11 Release-candidate rehearsal in the clean room, then v1.6.0

**Goal.** Prove on the exact release candidate, before any prod write, that:
- attribution is fixed fleet-wide;
- ownership does not depend on scan order;
- wrong owners converge from a populated DB with a lapsed guard (prod's real state);
- two-strike retirement retires the right cars.
Rehearse the prod first run and the local repair. Then tag v1.6.0.

**Entry conditions.**
- N6, N8, N9, N10 released to main (N9.15 resume and N10.8 shard order included); no unreleased scanner code.
- The rehearsed commit is recorded as `RC_SHA`. From `RC_SHA` until v1.6.0 is tagged, main is frozen for `backend/`, `scripts/`, `migrations/` and `deploy/`: other phases (N3, N4, N7, N20) queue their releases, so the N11.12 diff check holds.
- Lid open, `caffeinate -dims` verified; AC recommended for the passes (battery is not a blocker, owner 10-09).
- D-N25 approvals; D-RC5, D-RC6 answered.

**Units.**

| ID | Source | What | Kind | Files / area | Depends | Acceptance |
|---|---|---|---|---|---|---|
| N11.1 | lean N7.1, judges | Build the rehearsal DBs at `RC_SHA` | data-local, MAIN | `cars_cr_rc`, `cars_cr_rc_b`, `cars_cr_conv` | none | BUILD, ISOLATION and PREFLIGHT OK; commit stamped |
| N11.2 | CF-7 | Pass A: all 458 dealers, forward order, empty DB `cars_cr_rc` | scan, MAIN | run dir | N11.1 | finishes with a FLEET SUMMARY |
| N11.3 | CF-7, RET-11, IL-25 | Measure pass A | measurement | N1.3 tool | N11.2 | counts in the spec |
| N11.4 | judges (same-commit determinism) | Pass B: the 109 conflict-group dealers, reversed shard order (`SCAN_SHARD_ORDER=reverse`), empty `cars_cr_rc_b`, same commit | scan, MAIN | run dir | N11.2, N10.8 | ≥99% same dealer_id |
| N11.5 | lean N7.1 (convergence) | Pass C: all 458 dealers on `cars_cr_conv` (a copy of the 10-13/14 DB, rows shifted >30 days old) | scan, MAIN | run dir | N11.1 | ≥90% class-(b) VINs moved |
| N11.6 | judges (two-strike proof) | Live check of the pass-C first-strike list | investigation | `analysis_rc/strike_check.json` | N11.5 | wrong ≤2%, each wrong strike root-caused; cannot_assess ≤10% |
| N11.7 | RET-11 | Pass D: all 458 dealers on `cars_cr_conv`, ≥20 h after pass C; second strikes retire; live check | scan + investigation | run dir | N11.5 + 20 h | wrong 0, accounting closes |
| P5A.8 | plan Phase 5A, value-first + risk-first grafts | Dry shadow on a prod-shaped restore of `cars` (`cars_rehearsal`); the first-pass/retry shape moved to N9.14 | scan, MAIN | `cars_rehearsal` | N11.1 | owner sign-off |
| P5B.8 | plan Phase 5B | Local repair of wrongly retired rows in `cars`, rescan first (D-RC5) | data-local, MAIN | `cars`, backups | P5B.6 (N10) | owner OK; rollback tested |
| N11.10 | W11 (option C step 2) | Analyse the ≤46 signal dealers whose N1.3 signals persist in pass A | investigation | `analysis_rc/dealers/*.json` | N11.3 | records with checker verdicts |
| N11.11 | docs | Dealer logs (run-root blocks appended to the canonical logs), `_learning`, P5B.9 retirement docs, RC report | docs | `docs/remediation/RC_REHEARSAL_<date>.md` (new), `workspace/dealer_logs/` | all | owner signs the report |
| N11.12 | release | v1.6.0 | release | VERSION, CHANGELOG | N11.11 | no code diff vs `RC_SHA` |
| N11.13 | 9.1 risk "458 vs 561", CF-14 | Prod-only dealers and prod-only conflict pairs | scan + investigation, MAIN | `cars_cr_rc`, `analysis_rc/prod_pairs.json`, `analysis_rc/group_pools_additions.json` | N11.3, N1.6 | every prod pair classified; spec |

**N-unit specs.**

**N11.1 Build.**
- The owner approves `git switch --detach <RC_SHA>` in the main checkout (Section 7.0).
- `scripts/cleanroom/build_db.sh cars_cr_rc` and `cars_cr_rc_b`: empty `cars`; reference tables and the repaired recipes from `cars`; then `migrate --apply` on each (to 29).
- `scripts/cleanroom/build_db.sh --from-template cars_cleanroom cars_cr_conv`, then `migrate --apply` to 29. `cars_cleanroom` itself stays frozen.
- On `cars_cr_conv`, replace `dealer_recipes` and the other `build_db.sh` `COPY_TABLES` reference tables with the current rows from `cars` (N8.9 repairs and P6A.6 staling applied), using the same copy step as `build_db.sh`. The template's 10-13 recipes still hold mbbeverlyhills-com's byte copy of mbontario's listing and the 13 VIN-list recipes, which prod will not have after P6B.2.
- On `cars_cr_conv` only, shift `scraped_at` back so the gap exceeds 30 days (`UPDATE cars SET scraped_at = scraped_at - interval '20 days'`, logged in the run dir), so passes C/D take P5A.1 rule 4's 90-day fallback branch, as prod fleet #1 will (09-29 rows, gap over 30 days).
- Then `scripts/cleanroom/isolation_snapshot.sh` and `scripts/cleanroom/preflight.sh` with `--db`, `--art workspace/cleanroom_runs/<db>`, `--roster` and `--sha <RC_SHA>` for each DB.
- **Acceptance:** all print OK; `RC_SHA` is in each run dir; `recipe_identity_check.py` is clean on `cars_cr_conv`; the `scraped_at` shift is logged with its row count.

**N11.3 Pass A acceptance** (N1.3 tool against `baseline_r1`, and against `baseline_r1_rules_<rules_version>` for incomplete counts):
- `vin_owner_conflicts` ≤2,100 rows (from 9,638); residual pairs are only `group_pools.json` pairs or n=1.
- Every CF-7 class-(b) check = 0:
  - mbbeverlyhills rows on host `mbontario.com`; cleveland-nissan CH/DA/BR suffixes; chapmanfordaz CAE/CAY/CAV; bentleycadillac photo don:18411/21776/19779; Landers don:20573/20574/20624/24529 misplacements; bmwofmonrovia non-`sonicbmwmonrovia` accounts; 5starford non-NRH VDP city; fletcherjones "Henderson" rows.
  - mbontario active ≥1,200; mtnviewnissan CH rows ≥300.
- Identity: Σ captured − refusals = Σ written = active.
- 0 dealers in `error / no scan_runs row` without a logged site cause (the 21 pause-killed dealers are scanned).
- Dict-trim and token rows 0; junk prices 0; non-17 VINs written 0.
- `vpic_fuel` PHEV false positives 0.
- The 15 live-confirmed drivetrain cars display the vPIC value (P8A.6).
- Inaccurate verdicts down by ≥25 from 139, each remaining one with a cause code.
- 0 page-cap truncations without a stop reason; toyotacarlsbad (errored in the clean room) has a `scan_runs` row and its walk did not stop at `page_cap` (quick win 12).
- 0 VDP fetches on refused VINs.
- Incomplete, compared like for like: pass A runs on an empty DB, so every car's `first_seen_at` is the pass date and a window split would be vacuous. The gate is: incomplete per code, with no window, ≤ the value in `analysis/baseline_r1_rules_<rules_version>.json` (the N6 re-baseline, same definition and the same `rules_version`), and the trim code down by at least the N8.4 Motive fix (about 1,133). The window split is reported only on populated-DB passes (N11.5 pass C, N11.7 pass D) and on prod (N12.7, N21.12).
- Stale index flags 0 after the post-step rebuild.
- Strike and retirement counts 0 (empty DB).
- All five dealer log files for every dealer; `cars_cleanroom` writes unchanged.

**N11.4 Determinism.** Same `RC_SHA`. The 109 dealers with `SCAN_SHARD_ORDER=reverse` (N10.8; recorded in `fleet_summary.json`). **Accept:** ≥99% of VINs land on the same dealer_id as in pass A; each difference is listed and belongs to a pair flagged `group_feed_unstamped` but not yet in `group_pools.json`.

**N11.5 Convergence.** `cars_cr_conv` holds the 10-13/14 rows with `scraped_at` shifted back 20 days (N11.1): wrong owners included, every row more than 30 days old, so the 48 h guard has lapsed and P5A.1 rule 4 takes its fallback branch, as on prod. **Accept:**
- ≥90% of the class-(b) VINs listed in N1.1 now sit at the true store, and every such move has a `car_move_log` row;
- class-(d) VINs are refused and struck by evidence (N8.6/P5A.5), not re-filed under another sibling;
- 0 first strikes on walks whose stop reason is `page_cap`, `http_*` or `fetch_failed`;
- the strike list is stored;
- the incomplete rate is reported inside and outside the D-N13 window.

**N11.6 Strike-list live check.** Check 100% of strikes if there are fewer than 100; otherwise a stratified sample of ≥100 (by dealer and platform). Paced, home IP, ≤3 pages per dealer. "Wrong" means the car is still listed at that dealer. A strike is not a retirement, and scan misses (FP02 caps, FP03 blocks) make occasional wrong strikes likely: at a true 2% rate, 0 wrong in 100 happens only about 13% of the time. **Accept:** wrong ≤2%, every wrong strike root-caused and fixed or explained (or a second sample after the fix passes); cannot_assess ≤10%. A wrong strike with no explanation goes back to N9 for a fix and a targeted re-run; the phase does not continue.

**N11.7 Pass D.** At least 20 h after pass C. Second strikes retire. **Accept:**
- inactive delta = `listing_retirements` rows = Σ scanner-retired, and the pipeline wrote 0;
- 0 retirements on incomplete walks;
- live check of 100% of retirements if fewer than 100, else ≥100 sampled: wrong 0, cannot_assess ≤10% (clean-room baseline: 11 of 21 wrong);
- no dealer above the gap-aware D-N21 threshold (days computed from the retired rows' `scraped_at`, so about 60% here) without a written explanation. Pass D retires about three weeks of turnover plus the shift, so the 10% nightly alarm does not apply;
- the incomplete rate is reported inside and outside the D-N13 window.

**P5A.8 Shadow.** `pg_restore` or `createdb -T cars cars_rehearsal` (multi-GB, owner OK). Run, with `SCANNER_RECONCILE=dry`, the plan's 5 named dealers plus terrylabontechevy, coastlinecdjr, napletonhondamortongrove, southcountylexus and 10 group dealers. This rehearses N12's 16+-day-gap first run. (The first-pass/retry shape of the 4 retiring dealers moved to N9.14.) **Accept:**
- the would-strike lists are reviewed;
- the ownership diff per pair matches the N1.2 classes;
- 0 strikes written (dry) and 0 would-strikes on incomplete walks;
- owner sign-off in `docs/ops_log/P5A.8.md`.

**P5B.8 Local repair.** The owner approves `git switch --detach <RC_SHA>` if the checkout moved. Rescan the affected dealers into `cars` from the home IP (`caffeinate -dims` verified; AC recommended for runs over about 2 h). Then audit, dry run, owner OK, apply on a template copy, roll back, then apply on `cars` with owner OK. Cross-check the 1,679 VINs retired in `cars` but active in the clean room. **Accept:** backups named; every one of the 1,679 re-listed or explained; lexusofknoxville excluded.

**N11.10 Signal dealers.** Dealers from `fleet_signals.json` whose signal persists in pass A: one-condition, drops over 30%, small claimants, page cap, gains over 30%, the unsampled platforms (Motive, Chapman, DealerOn, autoWALL, OneAudi), no_rows, feed-level HTTP 403/429 from the home IP (63 and 9 in the clean room; neither scan lane may reach them). Chunks of 5, each with a checker. **Accept:** each finding is routed to a later N-unit (N19, N21) or closed; dealers with a persistent feed-level 403/429 are routed to dealer-discovery (N21.11) with cannot_assess logged in their dealer logs.

**N11.13 Prod-only dealers and pairs.** Prod's roster is 561 dealers; the clean room scanned 458, and prod has 212 conflict pairs of which 92 were rehearsed.
- **What:** from the N1.6 exports (`prod_roster.txt`, `prod_conflict_pairs.csv`), list the 103 prod-only dealers and every prod pair not in the clean room. After N11.3 has measured pass A, scan the 103 dealers into `cars_cr_rc` (recipes from `cars`; a dealer with no local recipe is logged cannot_assess). Classify every unrehearsed prod pair through one N11.10 chunk. Proposed `group_pools.json` entries go to `analysis_rc/group_pools_additions.json` for owner review; they ship in the first release after v1.6.0 (the main freeze forbids a `backend/` change before the tag). Until then those pairs follow N8.6 rule (4): keep the current owner and flag.
- **Acceptance:** every prod pair has a class or cannot_assess; per-pair counts of new conflicts in `cars_cr_rc` are recorded for the N12.7 cap; dealer logs written; home-IP 403/429 counts compared against the 63/9 baseline.

**N11.12 Release.**
- VERSION minor (v1.6.0); CHANGELOG `[1.6.0]` carries the N11 numbers.
- `git diff --stat <RC_SHA>..v1.6.0 -- backend scripts migrations deploy` must be empty: only VERSION, CHANGELOG and docs differ.
- Push to main and tag. No deploy (N12 deploys).

**Workflow shape.**
- **Session 1:** N11.1 → N11.2 (background, anomaly-only monitor) → N11.3, plus N11.10 (about 20 agents) → N11.13 → N11.4 → N11.5 → N11.6. P5A.8 runs during idle CPU, with a separate `SCANNER_LOCK_PATH` and a disjoint DB.
- **The workflow then ends and writes RESUME.md** with the earliest restart time (workflow resume does not cross sessions; pass D needs ≥20 h).
- **Session 2** (≥20 h after pass C, from RESUME.md): N11.7 → P5B.8 → N11.11 → N11.12.
- Space the passes and keep pass B to the 109 dealers; compare each pass's home-IP 403/429 counts against the 63/9 baseline and stop if either rises by more than 50% (Section 9.1).
- About 36 agents.
- Checkpoints: `workspace/phase_runs/N11/`; each pass resumes with `fleet_scan --resume <run_dir>` (N9.15): dealers with a triage entry or a `scan_runs` row started at or after the run start are skipped.

**Exit gate.** N11.3, N11.4, N11.5, N11.6, N11.7 and N11.13 met; P5A.8 signed; RC report signed; v1.6.0 tagged.

**Release step.** v1.6.0 (minor), tagged, not deployed.

**Prod steps.** None.

**Owner decisions / actions.** D-N25 (runs, restore, live checks), D-RC5, D-RC6; the `git switch --detach <RC_SHA>`; sign P5A.8 and the RC report; caffeinate verified (AC recommended).

---

### N12 Prod refresh (Phase 6B): fresh, correct data without wiping prod

**Goal.**
- Deploy the rehearsed tag idle, and apply V027-V029.
- Repair prod recipes.
- Verify from home, smoke on Railway.
- Fleet #1 writes first strikes only; its strike list is live-checked.
- Fleet #2 retires.
- Repair the residue, then turn the nightly cron on and watch three nights.

**Entry conditions.**
- v1.6.0 tagged and N11 signed.
- N4 deploy path proven; N5 prod at V026.
- PG18 client present.
- `SCAN_FLEET=0` verified; cron OFF; all local fleets paused.
- D-IF4, D-N22, D-N23, D-IF1 interim answered.

**Units.** P-unit specs: REMEDIATION_PLAN "Phase 6B" (line 1676). Every unit is a separate owner step, verified before the next.

| ID | Source | What | Kind | Depends | Acceptance |
|---|---|---|---|---|---|
| N12.1 | P6B.1 (part) | PG18 `pg_dump -Fc` of prod, verified with `pg_restore -l`; `\copy` ownership snapshot; re-run the N1.6 census | prod-owner | entry | dump size and TOC count logged; snapshot rows = prod `cars` count |
| N12.2 | P6B.1 | Step 0: owner-approved Docker smoke of both v1.6.0 stages; `migrate --dry-run`, then `--apply --target 29`; the N4.1 scanner-nightly settings pack if not applied earlier; `deploy_scanner_nightly.sh --dry-run`, then the real deploy from v1.6.0; then `deploy_web.sh` v1.6.0 | prod-owner | N12.1 | smoke passed; top = 29; scanner logs print idle; health 1.6.0 |
| N12.3 | P6A.7 | `prefleet_check.sh --target prod`, plus one verification that a redeploy with `SCAN_FLEET=1` outside the window idles | prod-owner | N12.2 | PREFLEET OK; "outside run window" verified (LE-1 class closed) |
| P6B.2 | plan + N8.9 | Home-IP prod recipe repairs: `saved_at` backfill (235 rows), un-stale the 31, stale the 13 hygiene recipes, apply the N8.9 repair set, coastline account check, after a `dealer_recipes` backup | prod-owner | N12.3 | backup file named; 0 rows with `max_saved_at=0`; repair-set re-apply is a no-op |
| P6B.3 | plan | Home verification scan of the un-staled dealers, the 6 contested VIN owners first, after a `\copy` backup, with `SCANNER_RECONCILE=dry` exported in the command | scan (owner) | P6B.2 | every dealer has a verdict and logs; strikes written = 0 |
| P6B.4 | plan + LE-9 (D-OD8) | 20-dealer Railway smoke + one-shard 403 measurement + transient 429/5xx counts, with `SCANNER_RECONCILE=dry` | prod-owner | P6B.3 | 0 recipes staled by Railway 403s; strikes written = 0 |
| N12.7 | P6B.5, value-first fleet #1 | Supervised fleet #1 with snapshots | prod-owner | P6B.4 | spec |
| N12.8 | value-first N7.7, judges | Live check of fleet #1's strike list, then fleet #2 (≥20 h later) with retirement live and ownership snapshots before and after, then a post-run live check | prod-owner + investigation | N12.7 | strikes wrong ≤2% (each root-caused); retirements wrong 0 |
| P6B.6 | plan | Prod retirement audit and residue repair (09-23..09-29 damage) with `audit_retirements`; lexusofknoxville excluded | prod-owner | N12.8 | dry-run counts approved; post-audit residue 0 |
| N12.10 | N6 repair packs, IL-09 | Prod write-path repairs: junk prices, 410 Metro order-number rows (owner confirms on the site), dict/token leftovers, index rebuild at the current `rules_version` | prod-owner | N12.8 | targets 0 |
| N12.11 | P6B.7 | Nightly cron `0 9 * * *` through the N4.1 path, with the run window; 3 nights watched; home-IP bridge for the blocked dealers | prod-owner | P6B.6, N12.10 | 3 clean nights |
| P6B.8 | plan | Docs and configs aligned; IL-25 prod re-measure with the new-arrival split, compared only within one `rules_version`; release-log rows; memory | docs | N12.11 | docs match live settings |

**N-unit specs.**

**N12.1 / N12.2 / N12.3 (ordered).**
- Backups go to `workspace/backups/prod_<stamp>_preV029.dump` and `ownership_prod_before_<stamp>.csv`.
- **Step 0 (N12.2), owner-approved Docker smoke of both stages from v1.6.0.** This is the first scanner image deployed since 1.4.1. `deploy_scanner_nightly.sh --dry-run --keep-stage <dir>` (`Dockerfile.scanner`), built and run against local PG at V029: with `SCAN_FLEET` unset it prints idle; with `SCAN_FLEET=1` and a closed window it prints "outside run window"; with `SCAN_DEALERS=<1 dealer>` against a scratch DB it completes. `deploy_web.sh --dry-run --keep-stage <dir>`: health 200 with the tag.
- Set `SCAN_FLEET_WINDOW_UTC`, `SCANNER_RECONCILE=dry` and `SCANNER_EGRESS_TAG=railway` on scanner-nightly with `--skip-deploys` (dry until N12.7, so the P6B.4 smoke writes no strike), apply the N4.1 scanner-nightly settings pack if it was not applied earlier, confirm `SCAN_FLEET=0` and no `SCAN_DEALERS`, then run `deploy_scanner_nightly.sh` (dry run, then real). `INVENTORY_DATABASE_URL` stays on the private network.
- Confirm `BUILD_COMMIT` in the idle hold (`railway ssh`), or from the build log if SSH is blocked.
- **N12.3 redeploy test.** Stage `SCAN_FLEET=1` and `SCAN_FLEET_WINDOW_UTC` at least 2 h from now, both with `--skip-deploys`. The owner stands ready with `railway down -s scanner-nightly -y` (a misconfigured window would start a full fleet before the P6B.2 recipe repairs). `railway redeploy -s scanner-nightly -y`, then expect "fleet outside run window" in the log within 2 min. Afterwards `SCAN_FLEET=0` with `--skip-deploys`. (With `SCAN_FLEET=0` a redeploy idles whatever the window, so that would prove nothing.)
- **Acceptance:** Docker smoke passed; schema top 29; scanner log "idle (window)"; `/api/health` shows 1.6.0; PREFLEET OK; the N12.3 redeploy printed "outside run window". That closes the LE-1 hazard for good.

**P6B.2 backup.** Before any write: `\copy (SELECT * FROM dealer_recipes) TO 'workspace/backups/dealer_recipes_prod_<stamp>.csv'`, row count logged.

**N12.7 Fleet #1.**
- Trigger: `fleet_scan` runs once per container start, so the owner sets `SCANNER_RECONCILE=1` and `SCAN_FLEET=1` with `--skip-deploys`, then runs `railway redeploy -s scanner-nightly -y` inside the window; afterwards `SCAN_FLEET=0` with `--skip-deploys`.
- Snapshot before and after. Post steps run the incomplete-index rebuild and the grid cards with `FLASK_ENV=production`.
- With two-strike, an empty strike table and P6B.3/P6B.4 run dry, fleet #1 can only write first strikes; no row is retired.
- **Acceptance:**
  - for ok dealers, ≥99% of active rows either carry this run's scrape date or hold a first strike from this run (`listing_miss_strikes`); and `rows_written` ≥ 90% of (`rows_captured` − refusals). (Fleet #1 retires nothing by design, so unseen prior rows stay active with 09-29 dates; a "≥90% fresh" gate would fail on correct behaviour at about 25-32% turnover.)
  - ownership moves judged per VIN: ≥90% of moves land on the store that the N11 pass-A/N11.13 result or the `group_pools.json` primary names as owner of that VIN, counted only over DB-checkable or live-verified VINs (prod's 09-29 owners are often the reverse of the clean-room classes: local `cars` had the clean-room claimant as owner for 3,941 conflict rows and the clean-room owner for 1,344); moves in pairs N11.13 could not rehearse are capped at 2× that pair's N11.13 count and listed for review; every move has a `car_move_log` row;
  - new `vin_owner_conflicts` rows (`seen_at` ≥ fleet start) per pair ≤2× the N11 pass-A (or N11.13) count, scaled by 561/458, and none in a pair N11 proved clean;
  - `SELECT count(*) FROM listing_retirements WHERE created_at >= <fleet #1 start>` = 0;
  - peak memory <14 GB; duration ≤180 min;
  - index stale flags 0;
  - prod `mileage` codes on New NULL-mileage cars 0;
  - the strike list is stored; the incomplete rate is reported inside and outside the D-N13 window.
- No manual bulk ownership reset (D-N22).

**N12.8 Strike check and fleet #2.**
1. Live-check ≥100 sampled first-strike VINs (stratified by dealer and platform; paced, home IP). **Gate:** wrong ≤2%, every wrong strike root-caused and fixed or explained (or a second sample after the fix passes); cannot_assess ≤10%. If the gate fails, set `SCANNER_RECONCILE=dry` with `--skip-deploys` before fleet #2 and return to N9.
2. `scripts/ownership_snapshot.sh` before fleet #2. With owner go, run fleet #2 ≥20 h after fleet #1 (same trigger as N12.7: `SCAN_FLEET=1` with `--skip-deploys`, `railway redeploy -s scanner-nightly -y` inside the window, then `SCAN_FLEET=0` with `--skip-deploys`), retirement live, under the gap-aware alarm (D-N21): min(60%, 10% + 1.5% × days), days = run start − the median `scraped_at` of the rows being retired (the days since the dealer's last scrape before fleet #1, 09-29), not since fleet #1. `scripts/ownership_snapshot.sh` after it.
3. Post-run: live-check ≥100 sampled retirements.
- **Acceptance:** retirements wrong 0 and cannot_assess ≤10%; the strike sample per step 1; `listing_retirements` rows = the inactive delta; no alarm left unexplained; before/after snapshots of fleet #2 reconcile with `car_move_log`. The 32% local turnover is not a target; it is an upper bound that includes scan misses.

**N12.10 Write-path repairs.** Each one: backup table, dry-run count, owner OK, apply, recount.
- junk prices set to NULL (N6.7 script; target 0 at $10-$999 except whitelisted dealers);
- the 410 Metro Honda order-number rows retired, after the owner confirms on mymetrohonda's site that they are not cars;
- dict and token values not healed by the rescans (N6.5 script; target 0);
- incomplete index rebuilt at the current `rules_version`.

**N12.11 Cron and bridge.**
- Cron `0 9 * * *` with the window (D-IF4), set through the N4.1 mechanism.
- Watch 3 nights with anomaly-only monitoring.
- Bridge (D-IF1 interim): until N14, the owner runs the 31 Railway-blocked dealers from the MBP every ≤3 days, outside the Railway window. Per-dealer locks (P11A.3) are live, and the runs use `PROJECT_DOTENV_DISABLE=1`, `SCANNER_EGRESS_TAG` exported empty, and a scratch `RECIPES_CACHE_DIR`. `caffeinate -dims` verified; AC recommended for runs over about 2 h (battery is not a blocker, owner 10-09).
- From here on `SCAN_FLEET=1` stays set with the cron and a valid window (P6A.7).
- **Acceptance:** 3 consecutive nights each start inside the window, exit 0 within 180 min and finish before the 12:30 UTC no-change horizon, with memory <14 GB, daily retirements within the 10% per-dealer alarm, 0 recipes staled by egress, the index rebuilt and 0 retirements on incomplete walks. Bridge runs logged in dealer_logs.

**Workflow shape.**
- Strictly serial.
- For each unit an agent writes the one-line command pack to `workspace/phase_runs/N12/<unit>.md`. The owner runs it and drops `<unit>.done`. A verifier agent reads the results, writes `docs/ops_log/<unit>.md` and checks the acceptance before the next unit starts.
- Live checks use investigator agents.
- The workflow may end after any unit and resume from `workspace/phase_runs/N12/RESUME.md` in a new session. Checkpoints: `workspace/phase_runs/N12/`; before fleet #2's ≥20 h gap and before each N12.11 night the workflow writes RESUME.md with the earliest restart time and ends.
- About 14 agents.

**Exit gate.**
- Three clean nightly runs.
- The last prod scrape is under 24 h old; ≥95% of active rows scraped in the last run or retired with a `listing_retirements` row.
- Residue and write-path repairs applied.
- The before/after ownership snapshots reconcile with `car_move_log`.
- Docs current.

**Release step.** No new code: deploys v1.6.0. P6B.8 docs ship as a patch afterwards. A mid-fleet hotfix uses `ALLOW_UNRELEASED_DEPLOY=1`, then becomes a patch release.

**Prod steps and who runs them.** All owner-run, in the unit order above. Never roll back scanner-nightly. Stop trouble with `railway down -s scanner-nightly`.

**Owner decisions / actions.** D-IF4, D-N22, D-N23, D-IF1 interim, the fleet #2 go/no-go, the cron go; every prod command; the bridge runs.

---

### N13 Recipe store authority (Phase 7), before the permanent home lane

**Goal.** The Postgres recipe store is authoritative with compare-and-set, so the MBP lane and Railway cannot overwrite each other's recipes. Local and prod stores are reconciled.

**Entry conditions.**
- N12 done (live scanning; a paused window is available).
- D-RS1-D-RS4 answered.
- Hosts: MBP and Railway (the mini only if N4.4 revived it).

**Units.** P-unit specs: REMEDIATION_PLAN "Phase 7" (line 1798). Staleness: the plan assumed three hosts and older `recipes.py` line numbers (the file grew from 1,167 to 1,839 lines); LE-14 moves to N8.9 if D-N34 = yes.

| ID | Source | What | Kind | Files / area | Depends | Acceptance |
|---|---|---|---|---|---|---|
| P7.1 | plan | V030 (renumbered from V029): rev, `recipes_written_at`, `superseded_json`; CAS primitives; `mutate_scan_hints` | code + migration | `recipe_store.py`, `migrations/V030__*.sql` | none | CAS conflict test |
| P7.2 | plan | Store-authoritative load; refresh only on change | code | per plan | P7.1 | per plan |
| P7.3 | plan | Key-scoped mutation API with an offline op-log | code | per plan | P7.2 | per plan |
| P7.4 | plan | Replay and promotion use the API | code | `recipes.py` | P7.3 | grep: no direct writes |
| P7.5 | plan | Replace-with-archive into `superseded_json` | code | per plan | P7.4 | per plan |
| P7.6 | plan | Per-host blocked status and lifecycle guard | code | per plan | P7.5 | a Railway 403 never stales the lane's recipe |
| P7.7 | plan | Back-compat test and rollback runbook | test + docs | per plan | P7.6 | per plan |
| P7.8 | plan | Authority docs, CHANGELOG | docs | per plan | P7.6, P7.7 | per plan |
| P14A.9 | plan Phase 14A | recipe_store CAS PG test | test | harness | P7.1, N7 | green in `pytest-pg` |
| P7.9 | plan | DB-to-DB reconcile local vs prod plus the Railway volume cache | prod-owner | per plan | P7.6 | 0 unexplained diffs |
| N13.11 | rule 5 (local applies) | Local V030 apply on `cars`: `pg_dump`, `migrate --dry-run`, owner OK, `--apply --target 30`, verify | data-local, MAIN | `cars`, `workspace/backups/` | P7.1 | `roq cars "SELECT max(version) FROM schema_migrations"` returns 30 |

If D-N34 = no, the LE-14 overlap clean-up (7 dealers) runs here, through the P7.3 mutation API.

**Workflow shape.**
- Serial chain P7.1 → P7.6; N13.11 (MAIN) after P7.1 merges.
- Then in parallel: P7.7, P7.8, P14A.9.
- Then P7.9 (owner) during a paused window.
- About 16 agents.
- Checkpoints: `workspace/phase_runs/N13/`; before P7.9's paused window and before the 3 nightly runs the workflow writes RESUME.md with the earliest restart time and ends.

**Exit gate.**
- `roq cars "SELECT max(version) FROM schema_migrations"` returns 30; prod returns 30.
- The CAS PG test is green in CI.
- The P7.9 report shows 0 unexplained diffs.
- 3 nightly runs with 0 recipe clobbers (op-log conflicts reported, none lost).

**Release step.** Minor. Prod order: dump, `migrate --target 30`, an owner-approved Docker smoke of the scanner-nightly and web stages, coordinated deploy of scanner-nightly (outside the window) and web.

**Prod steps.** Owner: dump, migrate, deploys, P7.9.

**Owner decisions / actions.** D-RS1-D-RS4.

---

### N14 Home-IP lane (Phase 15A) and the D-IF6 desk review

**Goal.** A permanent, guarded nightly lane for the dealers Railway cannot reach. This replaces the N12 bridge. Settle admin onboarding and the retired post steps.

**Entry conditions.**
- N12 and N13 done; P11A.3 live (N10).
- P6B.3/P6B.4 403 evidence.
- D-IF1, D-IF3, D-IF6 (after N14.1), D-N28 answered.
- Departure from the plan's 15A gate ("Phase 14 done"): only the 14A core (N7) is required. The lock and retirement SQL it was meant to protect is tested there.

**Units.** P-unit specs: REMEDIATION_PLAN "Phase 15A" (line 3426). Staleness: P15A.6 assumed the mini; under D-N2 it runs on the MBP.

| ID | Source | What | Kind | Files / area | Depends | Acceptance |
|---|---|---|---|---|---|---|
| N14.1 | LE-8, lean N11.1 | P0B.5 as a desk review: capability table of every retired nightly and k8s post step from code and git history; keep, replace or drop | investigation | `docs/NIGHTLY_CAPABILITIES_2026_10.md` (new) | D-N28 | every step mapped; D-IF6 asked |
| P15A.1 | plan | Egress lanes: `home_lane_dealers.txt`, Railway exclusion, alarms | code | per plan | none | rosters disjoint; stale alarm |
| P15A.2 | plan | Home-lane runner: dedicated clone, prod-DSN guards, launchd template (MBP) | code | per plan | P15A.1 | refuses without guards |
| P15A.3 | plan | Fix or retire the admin onboarding path (D-IF3; replaces N3.10) | code | per plan | none | works end to end or removed |
| P15A.4 | plan + IL-21 | Configurable post steps (those D-IF6 keeps); a recover phase and auto-heal aimed at index codes | code | `fleet_scan.py` | N14.1, P15A.1 | targeted heal acts only on rows with open codes |
| P15A.5 | plan | Kill-switch matrix for lanes and dealer locks | test + docs | per plan | P15A.1 | rows pass |
| P15A.6 | plan | Stand up the lane on the MBP; watch 3 nights; retire the bridge | prod-owner | `home_lane.env`, launchd | P15A.2, P15A.5 | 3 nights fresh |

P11A.5 (worker loop) runs only if D-IF3 = (b); otherwise it is dropped (Section 8).

**N-unit spec, N14.1.**
- **What:** read the retired laptop nightly scripts, the k8s CronJobs and their git history. For each post step write: what it did, what consumes its output today, its replacement (or "drop"), and evidence. A multi-GB scratch restore happens only for a step the review cannot settle.
- **Why:** P0B.5 never ran, because its host was the mini, so D-IF6 is unanswerable. Battery is no longer a blocker, but a multi-GB restore still needs an OK. The review costs no restore.
- **Acceptance:** every step has a row with evidence. D-IF6 is answered from the table. The index rebuild (N6.1) appears as the replacement for the old index step.
- **Tests:** none.

**Workflow shape.**
- Wave A: N14.1, P15A.1, P15A.3.
- Wave B: P15A.2, P15A.4, P15A.5.
- Wave C: P15A.6 (owner).
- About 14 agents.
- Checkpoints: `workspace/phase_runs/N14/`; before D-IF6 is answered from the N14.1 table and before P15A.6's 3 nights the workflow writes RESUME.md with the earliest restart time and ends.

**Exit gate.**
- Three nights of lane runs, each dealer with logs; the Railway nightly excludes the lane dealers.
- The stale-lane alarm is tested.
- D-IF6 is logged.

**Release step.** Minor; web deploy for P15A.3; scanner-nightly redeployed outside the window if fleet code changed.

**Prod steps.** Owner: `home_lane.env`, the launchd load, deploys.

**Owner decisions / actions.** D-IF1, D-IF3, D-IF6, D-N28.

---

### N15 Fact precedence foundation (8A), with the drivetrain and fuel fixes

**Goal.** One precedence module, with vPIC deciding drivetrain and electrification on every write (the read side shipped in N6). Stop new writes in the class of 5,306 rows that store AWD/4WD where vPIC decodes 2WD.

**Entry conditions.** N12 done; P8A.2 and P8A.6 merged in N6 (D-PR1, D-PR4, D-PR6, D-PR7 answered there).

**Units.** P-unit specs: REMEDIATION_PLAN "Phase 8A" (line 1950). P8A.2 (pure precedence module) and P8A.6 (read-time drivetrain) moved to N6.

| ID | Source | What | Kind | Files / area | Depends | Acceptance |
|---|---|---|---|---|---|---|
| P8A.1 | plan + W09 | Read-only precedence invariants; baselines 5,306 rows / 142 dealers, blank fuel 599, "F" 30 | code | per plan | none | reproduces baselines |
| P8A.3 | plan | `apply_fact_proposals` | code | per plan | P8A.2 (N6) | lower rank never overwrites higher |
| P8A.4 | plan | vpic_facts through precedence; provenance with value; chunked heal | code | `vpic_facts.py` | P8A.3 | provenance on every vPIC fact |
| N15.5 | IL-08 | Fill plain Gasoline from vPIC (`vpic_facts.py:76-85` fills only electrified and diesel); map single-letter fuels; repair stored injection values | code | `vpic_facts.py` | P8A.4 | blank fuel ≤50 (undecodable only); 0 "F" |
| P8A.5 | plan | Decode refresh; `heal_from_vpic --dry-run` writes nothing (INCOMPLETE §2.12, §2.17) | code | per plan | P8A.4 | dry run leaves write counters unchanged |
| P8A.7 | plan + M5, FP08 | Local heal: 1,409 fuel-only PHEV labels + the drivetrain class + N15.5 repairs + the 32 BEV rows stored as Gasoline, on `cars` | data-local | `cars` | P8A.4, P8A.5, N15.5 | invariants 0 except cannot_assess; no "Hybrid" label where vPIC says MHEV; EV engine displacement NULL (no "0.0L" or "5.7L"); the 32 BEV rows healed or marked cannot_assess (the known vPIC filing error on 2027 BMW 550e marked cannot_assess, not healed to BEV) |

**N-unit spec, N15.5.**
- **What:** through `apply_fact_proposals`, fill `fuel_type='Gasoline'` when vPIC says so and the stored value is blank. Map single-letter fuels ("F" and similar) to the vocabulary. A repair script rewrites stored "Direct Injection" values (381 in `cars` and prod) from the decode.
- **Why:** 599 blank and 30 "F" in the fresh scan. F13 stops new injection junk, but stored values remain.
- **Acceptance:** a targeted re-run shows blank fuel ≤50 (undecodable VINs only) and 0 "F"; `cars` has 0 injection values after the repair (owner OK).
- **Tests:** precedence tests for fuel; repair dry run.

**Workflow shape.**
- Wave A: P8A.1, P8A.3.
- Wave B: P8A.4, then N15.5.
- Wave C: P8A.5.
- Wave D (MAIN): P8A.7.
- About 14 agents.
- Checkpoints: `workspace/phase_runs/N15/`; before the owner's P8A.7 apply OK, the 142-dealer re-run approval and the deploy the workflow writes RESUME.md with the earliest restart time and ends.

**Exit gate.**
- Invariants green on `cars` after P8A.7, except listed cannot_assess rows.
- A targeted re-run of the 142 affected dealers (approved in the phase prompt) writes 0 new AWD/4WD where vPIC says 2WD.

**Release step.** Minor. The owner approves the Docker smoke of the scanner-nightly and web stages, then deploys scanner-nightly outside the window, and web. The prod heal is in N17.

**Owner decisions / actions.** P8A.7 apply OK; the 142-dealer re-run (D-PR1, D-PR4, D-PR6, D-PR7 were answered for N6).

---

### N16 Every fact writer through precedence (8B), plus body style and the HD table

**Goal.** No writer reverts a higher-ranked fact. Body style uses a canonical vocabulary. Heavy-duty drivetrain and transmission are mapped.

**Entry conditions.** N15 released; D-PR2, D-PR3, D-PR5, D-N32 answered.

**Units.** P-unit specs: REMEDIATION_PLAN "Phase 8B" (line 2069).

| ID | Source | What | Kind | Depends | Acceptance |
|---|---|---|---|---|---|
| P8B.1 | plan | Scan upsert stops reverting higher-ranked stored facts | code | P8A.3 | a rescan never downgrades a vPIC drivetrain |
| P8B.2 | plan | Window-sticker columns through precedence | code | P8A.3 | per plan |
| P8B.3 | plan | Post-scan: decode first, heal last | code | P8A.4 | order asserted |
| P8B.4 | plan + IL-20 | Inline vPIC decode on every scan path (D-PR5 (a)); HD/commercial drivetrain and transmission table gated on GVWR/body class, 4x2 unmapped by default | code | P8A.4 | decode coverage ≥99% on new rows; drivetrain gaps 393 and transmission gaps 713 halved |
| P8B.5 | plan | EPA dictionary writer: corroboration-only engine corrections | code | P8B.3 | the catalog never outranks dealer engine text or the decode |
| P8B.6 | plan + IL-15, W21 | Structured backfill (VIN before catalog); canonical body style from vPIC BodyClass or the catalog when the stored value is noncanonical (`field_clean.py:286-333`) | code | P8A.6 | noncanonical body styles ≤1% of active (24,817 fresh); body filter tested end to end |
| P8B.7 | plan + INCOMPLETE §2.19 | Storage repair and scripts through precedence, including the recovery scripts (≤273 recovery writes without provenance) | code | P8B.5, P8B.6 | grep: no raw fact UPDATE outside `apply_fact_proposals`; 0 raw fact UPDATEs in recovery scripts |
| P8B.8 | plan | model_specs and InventoryEnricher at the lowest rank (D-PR8) | code | P8B.1, P8B.3, P8B.4 | model_specs never overwrites vPIC 4x2 |
| P8B.9 | plan | gap_fill through precedence; DDG values tagged `web_search` | code | P8A.3 | per plan |

The body-style and HD-table additions are specified in the table: what, files, acceptance; tests are table tests in the touched module.

**Workflow shape.**
- Wave A: P8B.1, P8B.2, P8B.4, P8B.6, P8B.9.
- Wave B: P8B.3, P8B.5.
- Wave C: P8B.7, P8B.8.
- About 18 agents.
- Checkpoints: `workspace/phase_runs/N16/`; before the owner's deploy the workflow writes RESUME.md with the earliest restart time and ends.

**Exit gate.**
- The fact-precedence cycle test (from N17's P8C.3 if early, otherwise a targeted re-run) shows no regressions.
- The body and HD re-measure targets are met.
- No writer stores a "Hybrid" label where vPIC says MHEV, or an engine displacement on an EV (FP08, M5); P8B.7's grep also shows 0 raw fact UPDATEs in the recovery scripts (INCOMPLETE §2.19).

**Release step.** Minor; the owner approves the Docker smoke of the scanner-nightly and web stages, then deploys outside the window.

**Owner decisions / actions.** D-PR2, D-PR3, D-PR5, D-N32.

---

### N17 Precedence close-out and prod heal (8C)

**Goal.** Retire the legacy writers, prove the cycle, and heal prod facts in an owner window.

**Entry conditions.** N16 released; owner picks the prod window.

**Units.** P-unit specs: REMEDIATION_PLAN "Phase 8C" (line 2214).

| ID | Source | What | Kind | Depends | Acceptance |
|---|---|---|---|---|---|
| P8C.1 | plan | Retire or route `spec_backfill` and `persist_enrichment` | deletion + code | none | no callers (grep) |
| P8C.2 | plan | `heal_cylinders_from_vpic` Phase A = decode refresh + `heal_rows` | code | P8A.5 | per plan |
| P8C.3 | plan | Fact-precedence cycle test with a snapshot after every step | test | none | stable after 2 iterations |
| P8C.4 | plan | `VEHICLE_FACT_PRECEDENCE.md` | docs | none | matches `precedence.py` |
| P8C.5 | plan | Relabel legacy provenance (tool + local run) | code + data | none | 0 unlabeled legacy facts on `cars` |
| P8C.6 | plan | Full local heal, then a catalog relink dry run | data-local | P8C.1-P8C.5 | invariants green |
| P8C.7 | plan | Prod heal, relabel, relink | prod-owner | P8C.6 | prod AWD-vs-vPIC-2WD = 0 except cannot_assess |

**Workflow shape.** Wave A: P8C.1-P8C.5. Wave B: P8C.6 (MAIN), then P8C.7 (owner). About 12 agents. Checkpoints: `workspace/phase_runs/N17/`; before the owner's prod heal window the workflow writes RESUME.md with the earliest restart time and ends.

**Exit gate.** The prod invariant query (owner, read-only) returns 0 for AWD/4WD stored where vPIC says 2WD, except listed cannot_assess rows.

**Release step.** Patch.

**Prod steps.** Owner: dump, heal, relabel, relink, verify.

**Owner decisions / actions.** The prod window.

---

### N18 One fetch policy and VDP prefetch coverage (12A extended), guessed URLs

**Goal.** Every fetch goes through one policy. Prefetch spends its budget on gaps, rotates TLS profiles, counts blocks, and stops trusting guessed URLs.

**Entry conditions.**
- N12 done (Railway egress data from P6B.4).
- N13 merged (serializes `recipes.py`).
- D-OD3, D-OD7, D-OD8, D-OD13 answered after N18.1.

**Units.** P-unit specs: REMEDIATION_PLAN "Phase 12A" (line 2933).

| ID | Source | What | Kind | Files / area | Depends | Acceptance |
|---|---|---|---|---|---|---|
| N18.1 | LE-9, P0B.1 | Dealer Inspire burst (6 workers × 150 ms, rotation vs serialization, about 30 requests per host, owner-approved) + D-OD8 numbers from P6B.4 | measurement | `docs/monolith_audit_2026_10_01/measurements/` | owner OK | numbers pasted into D-OD7/D-OD8 |
| P12A.1 | plan | FetchPolicy values; HostProfileMemory | code | `net/client` | none | per plan |
| P12A.2 | plan | Feed replay on the decided policy; HTML-edge parity; challenge outcome | code | `recipes.py` replay | P12A.1, N18.1 | a mid-walk 403 records a challenge outcome |
| P12A.3 | plan | Stray scanner HTTP sites onto `net/client` | code | per plan | P12A.1 | grep clean |
| P12A.4 | plan | Chain requests path uses the proxy | code | `chain.py` | P12A.2, P12A.3 | per plan |
| P12A.5 | plan + W05b, IL-12 | VDP prefetch on the policy; TLS profile rotation; per-VIN attempt ledger; wall-clock budget spent on gaps; Carfax not required on new cars; `candidates_with_gap` in `summary_json` | code | `vdp/prefetch.py` | P12A.4 | spec |
| P12A.6 | plan | Script `curl_cffi` sites onto `net/client` | code | scripts | P12A.1 | grep clean |
| N18.8 | IL-18, W13, F5 | Resolver reads `_source_url`; VIN-on-page check before applying fields (`prefetch.py:404`); guessed URLs on non-dealer.com platforms not fetched; F5 check | code | `vdp_urls.py:17-70,172-200`, `prefetch.py` | P12A.5 | spec |

**N-unit specs.**
- **N18.1:** run from the home IP with owner approval; write rotation vs serialization results and transient-status counts (home and Railway). **Acceptance:** D-OD7 confirmed or revised with numbers.
- **P12A.5 extension:**
  - **What:** rotate impersonation profiles per D-OD7 (`safari17_0` passed where `chrome` got 403 at 7 dealers); a per-VIN attempt ledger, so a car is not re-tried forever; the wall-clock budget goes to gap rows first; drop the Carfax requirement on new cars (`_extras_missing`); record `candidates_with_gap` and 403/429.
  - **Why:** 140 blocked dealers (59,523 wanted, 365 fetched); 85,480 of 170,135 candidates unfetched; the wall clock, not the cap, binds.
  - **Acceptance:** on a 20-dealer home sample of the blocked dealers, the fetched share rises from <1% to ≥30% wherever any profile passes; price gaps outside the new-arrival window fall ≥25% on the next re-measure (fresh baseline 6,281).
  - **Tests:** profile-rotation unit tests; ledger tests.
- **N18.8:**
  - **What:** use the payload's `_source_url` when present. Apply page fields only when the VIN appears on the page. Mark `/xxx-inventory/vin-<VIN>.htm` guesses and do not fetch them on non-dealer.com platforms. Classify F5 (old high-mileage unpriced cars, 523 locally).
  - **Why:** 19,332 rows at 96 dealers carry guessed URLs; 2,728 fetches returned 404; all-404 at the Motive stores.
  - **Acceptance:** guessed-URL 404s in a re-run fall below 100; 0 page fields applied without a VIN match; F5 verdict recorded.
  - **Tests:** resolver tests.

**Workflow shape.**
- N18.1 first.
- Then P12A.1 → (P12A.2, P12A.3, P12A.6) → P12A.4 → P12A.5 → N18.8.
- About 18 agents.
- Checkpoints: `workspace/phase_runs/N18/`; before the N18.1 burst approval and before the post-deploy nightly the workflow writes RESUME.md with the earliest restart time and ends.

**Exit gate.**
- grep: no direct `requests`/`curl_cffi` calls in `backend/scanner` outside `net/client`.
- The 20-dealer sample targets are met.
- The prod nightly after deploy shows the price-gap drop outside the window.

**Release step.** Minor; the owner approves the Docker smoke of the scanner-nightly stage, then deploys scanner-nightly outside the window.

**Owner decisions / actions.** D-OD3, D-OD7, D-OD8, D-OD13; approve the burst.

---

### N19 One recipe validation rule (12B) and under-collection rescans

**Goal.** Recipes are judged by the walker that replays them. Every live recipe is re-judged. Under-collected dealers are rescanned.

**Entry conditions.** N18 and P6A.6 (N10) merged; D-OD1, D-OD2, D-OD4, D-OD5, D-OD6 answered.

**Units.** P-unit specs: REMEDIATION_PLAN "Phase 12B" (line 3010).

| ID | Source | What | Kind | Depends | Acceptance |
|---|---|---|---|---|---|
| P12B.1 | plan | Validation uses the scan's store place | code | none | per plan |
| P12B.2 | plan | check_all/judge split; raw-VIN termination; `short_page` | code | P12B.1 | per plan |
| P12B.3 | plan | `validate_recipe` as a paced count over the replay walker | code | P12B.2, P12A.2 | per plan |
| P12B.4 | plan | Save paths do one full walk | code | P12B.3 | one walk per save |
| P12B.5 | plan + CF-3 | One minimum-VIN rule; refuse at save a recipe whose URL+body duplicates another dealer's | code | P12B.4 | duplicate refused |
| P12B.6 | plan | Cascade through the gate with fall-through and dealer logs | code | P12B.5 | per plan |
| P12B.7 | plan | Re-judge every live recipe, local then prod | data | P12B.5, P12B.6 | report includes the one-condition dealers (≥14), northparklexus, carvision, hendricklexuscharlotte |
| P12B.8 | plan | Docs and workflow text | docs | P12B.6 | per plan |
| P12B.9 | plan | Release, local stage check, canary shard | prod-owner | all | canary clean |
| P12B.10 | plan | Rescan under-collected dealers; audit prod cascade recipes | scan (owner) | P12B.9 | carvision ≥90% of site total (was 7.6%); one-condition dealers capture both conditions |

**Workflow shape.** Serial chain P12B.1 → P12B.6; then P12B.7 (MAIN), P12B.8; then the owner's prod steps. About 18 agents. Checkpoints: `workspace/phase_runs/N19/`; before the canary night and the prod re-judge the workflow writes RESUME.md with the earliest restart time and ends.

**Exit gate.** The canary shard shows rows written within ±5% of the previous night. The rescans meet their targets.

**Release step.** Minor; after an owner-approved Docker smoke of the scanner-nightly stage, deploys scanner-nightly.

**Owner decisions / actions.** D-OD1, D-OD2, D-OD4, D-OD5, D-OD6; the canary and prod re-judge.

---

### N20 Dictionary Complete_Options lookups in prod (Phase 9)

**Goal.** Prod resolves options for the groups it misses today: 54 of the top 300 groups resolve, which leaves 246 missing. Also fix PF-1, the 128 wrong-model YMMs on the no-catalog fallback.

**Entry conditions.**
- N5 merged (no shared files with N6-N19).
- The default start is after N12, earlier if capacity allows. An earlier P9.8 deploy runs the Section 7.0 migration check first (V027-V029 applied to prod with a dump, or the deploy waits), and it may not land on main between `RC_SHA` and v1.6.0 (N11 freeze).
- No other web deploy on the same day.
- D-HY1, D-HY9 answered (D-HY9 after the 20-car diff).

**Units.** P-unit specs: REMEDIATION_PLAN "Phase 9" (line 2279). The entry-gate fact is satisfied: `DICTIONARY_CATALOG_DB_PATH` is unset on web.

| ID | Source | What | Kind | Depends | Acceptance |
|---|---|---|---|---|---|
| P9.1 | plan | Merge the 97 root-only Brochure Summary rows | data (tracked files) | none | diff of 97 rows, none lost |
| P9.2 | plan + PF-1 | Normalized in-memory options index; PF-1 in Accept (`tests.md:446`) | code | P9.1 | 128 YMMs resolve; 20-car diff ready |
| P9.3 | plan | Delete the 614 root duplicates after a content pre-flight | deletion | P9.1, P9.2 | 0 unique content lost |
| P9.4 | plan | Delete the 2,004 header-only stubs after a pre-flight | deletion | P9.3 | 0 unique content lost |
| P9.5 | plan | Rebuild the local index or record it as not needed | data | P9.2 | recorded |
| P9.6 | plan | In-image catalog decision (expected: close as not needed) | investigation | none | logged |
| P9.7 | plan | Fence script-only trees out of the images | code | none | trees absent from the image |
| P9.8 | plan | Release and verify in prod: Docker smoke, migration check (Section 7.0), `deploy_web.sh` | prod-owner | all | 20-car diff signed; prod `migrate --dry-run` printed `database is up to date` before the deploy |

**Workflow shape.** Wave A: P9.1, P9.2, P9.6, P9.7. Wave B: P9.3 → P9.4, P9.5. Wave C: P9.8. About 10 agents. Checkpoints: `workspace/phase_runs/N20/`; before the owner signs the 20-car diff and before the P9.8 deploy the workflow writes RESUME.md with the earliest restart time and ends.

**Exit gate.** The owner signs the 20-car before/after diff; the PF-1 test passes.

**Release step.** Patch or minor; web deploy.

**Owner decisions / actions.** D-HY1, D-HY9; sign the diff.

---

### N21 Incomplete-listings remainder and dealer-discovery batch

**Goal.** Close the remaining clean-room data-quality patterns (FP07, FP09, FP14, FP17, FP18) and the incomplete-listings findings not covered elsewhere. Every metric keeps the new-arrival window separate.

**Entry conditions.**
- N16 (body/text vocabulary) and N18 (prefetch) merged.
- D-N18, D-N17b, D-N31, D-N33 answered.

**Units.**

| ID | Source | What | Kind | Files / area | Depends | Acceptance |
|---|---|---|---|---|---|---|
| N21.1 | W08, FP07 | Store the D-N18 price; fix the cosmos MSRP fallback, dealer.com pre-offer `internetPrice`, fee-inclusive totals | code | `scrapers/dealer_on.py`, `parsers/dealer_dot_com.py` | D-N18 | live re-check matches the advertised price |
| N21.2 | W17, D-N17b | 17-char I/O/Q pre-assignment IDs: keep, skip decode, cannot_assess, excluded from the VIN guard | code | parsers, `vpic_facts.py`, guard | D-N17b | 1,251 rows handled per decision |
| N21.3 | W18, FP14 | Condition labels: CarBravo/CPO stored as Used, CTP/demo as New, Used shown as New; new cars over 1,000 mi flagged | code | parsers, field_clean | none | fixtures correct |
| N21.4 | IL-11, D-N31 | Gardena Honda via dealer-discovery; vPIC single-valued trim for no-trim feeds | data + code | dealer_logs, `dealermasters.py` | D-N31 | Gardena incomplete ≤10% outside the window |
| N21.5 | IL-17 | One hand VDP check each at kingfordsales, koonstoyotaarlington, raypearman, centurybmwhuntsville (owner-run); fleet alarm for dealers >25% incomplete with no open item | investigation + code | triage | none | each classified; alarm live |
| N21.6 | W15, FP09 | Gallery harvest: DEP extension-less cloudflareimages, FordDirect/Team Velocity `photoUrls`, malformed CSS URLs | code | parsers | none | photo-less cars fall ≥30% (fresh 9,270) |
| N21.7 | IL-16, D-N33 | Dealer-relative interior-color rule, if adopted | code | completeness | D-N33 | live or declined |
| N21.8 | IL-23, IL-24, F3, M1, 2.24, 2.18 | Mercedes designation (113 rows); trimless list; non-road field sets; model fill from a clean decode; delete the dead recovery script and columns after a consumer audit | code + deletion | per finding | none | counts re-measured; consumers grepped |
| N21.9 | FP18 | Text normalization: model carrying the trim, make casing, HTML entities, description = SEO meta | code | field_clean | none | 200-row sample clean |
| N21.10 | W19 (rest) | `dealership_registry_id` for cosmos dealers that lack it | data | registry | none | every cosmos dealer has an id |
| N21.11 | N11.10 follow-ups | Close or route each signal-dealer finding no other unit covers, including the dealers with a persistent feed-level HTTP 403/429 from the home IP (dealer-discovery, cannot_assess logged) | investigation | dealer_logs | N11.10 | each closed or routed |
| N21.12 | IL-25, INCOMPLETE §2.2 | Re-measure on prod (owner, read-only) with the new-arrival split; the residual trim gap after N8.4, N21.4, N21.8 and P8B.6 | measurement | N1.3 tool | all | outside-window incomplete ≥30% below the N11 value, compared only within one `rules_version` (re-baseline both sides if the rules changed) |

**N-unit specs.**
- **N21.1:**
  - **What:** per D-N18, store the advertised price; MSRP goes only to `msrp`.
  - **Why:** 28 of 33 cosmos dealers store price = MSRP on ≥80% of new rows. Live checks found price wrong in 50 of 218 comparisons (a sample biased toward gaps), e.g. bentleygmc $67,445 MSRP vs $65,194 advertised; cdodge $70,390 vs $59,330.
  - **Acceptance:** fixtures match the live values (bentleygmc $65,194, clevelandford $63,009, jackyjonessweetwater $94,424, cdodge $59,330, secretcitycdjr $52,085); a stratified 100-car live sample shows price mismatch ≤5%.
  - **Tests:** parser fixtures.
- **N21.2:** per D-N17b. **Acceptance:** each I/O/Q row is decoded 0 times and its VIN-derived facts are cannot_assess; counts re-measured. **Tests:** decode-skip tests.
- **N21.3:** **Acceptance:** fixtures for welbornchevrolet, toyotasb, normreeveshondairvine, machaikchevy and northparklexus label correctly. **Tests:** fixtures.
- **N21.4:** dealer-discovery loop per `docs/NETWORK_SCAN_PROCESS.md`; vPIC trim fill with provenance `vpic` only when vPIC returns exactly one trim. **Acceptance:** Gardena (646/646 incomplete) ≤10% outside the window, or a documented source limit.
- **N21.5:** **Acceptance:** each dealer classed as hidden at the source or a parse gap; parse gaps fixed; alarm test.
- **N21.6:** **Acceptance:** the DEP fixture yields about 20 photos (was 1); no malformed URLs stored; photo-less cars fall ≥30%.
- **N21.7-N21.10:** acceptance as in the table; tests are unit tests in the touched module.
- **N21.11:** each N11.10 finding gets a dealer_logs block and a closing or routing line.
- **N21.12:** owner-run read-only recompute with the N1.3 tool.

**Workflow shape.**
- Code units in parallel worktrees with reviewers.
- N21.4 and N21.11 through the dealer-discovery skill (MAIN for recipe writes, through the N13 CAS API).
- N21.5 owner fetches.
- About 22 agents.
- Checkpoints: `workspace/phase_runs/N21/`; before the N21.5 owner fetches, the repair OKs and the N21.12 owner re-measure the workflow writes RESUME.md with the earliest restart time and ends.

**Exit gate.** The units' acceptance is met; the N21.12 re-measure is recorded.

**Release step.** Minor; deploys. Prod repairs follow the backup, dry run and OK rule.

**Owner decisions / actions.** D-N18, D-N17b, D-N31, D-N33; N21.5 fetches; repair OKs.

---

### N22 EPA catalog code foundation (10A)

**Goal.** Induction vocabulary, a permanent catalog key, V031 (renumbered from V030), and the importer.

**Entry conditions.**
- N17 released (precedence exists).
- D-EPA1, D-ET2, D-ET3, D-ET5, D-ER2 answered.
- The owner approves the pinned `vehicles.csv.zip`.

**Units.** P-unit specs: REMEDIATION_PLAN "Phase 10A" (line 2347).

| ID | What | Kind | Depends | Acceptance |
|---|---|---|---|---|
| P10A.1 | Induction vocabulary, EPA-flag mapper, shared twin helper | code | none | per plan |
| P10A.2 | Permanent catalog key (`epaId`, else a sticky content key) | code | none | key stable across two imports |
| P10A.3 | V031: epa_master flags, provenance, archive tables, indexes | code + migration | none | chain builds; drift report clean |
| P10A.4 | epa_engine honours the catalog row's label | code | P10A.1 | per plan |
| P10A.5 | Importer: flags, new columns, `--years`, `--patch-induction-only`, `--verify` (L) | code | P10A.1, P10A.4 | `--verify` clean on the pinned file |
| P10A.6 | Provenance backfill script with positive legacy classification | code | P10A.2, P10A.3 | dry run classifies 100% of rows |
| P10A.7 | Relink modes | code | none | each mode tested |
| P10A.8 | `--refresh-induction`, `--restore` | code | P10A.1, P10A.3 | restore round trip checksum-equal |

**Workflow shape.** Wave A: P10A.1, P10A.2, P10A.3, P10A.7. Wave B: P10A.4, P10A.6, P10A.8. Wave C: P10A.5. About 14 agents. Checkpoints: `workspace/phase_runs/N22/`; before the quiet-window V031 apply the workflow writes RESUME.md with the earliest restart time and ends.

**Exit gate.** V031 applied locally and on prod (owner, quiet window); `--verify` clean.

**Release step.** Minor. Owner: dump, `migrate --target 31` outside the cron window.

**Owner decisions / actions.** D-EPA1, D-ET2, D-ET3, D-ET5, D-ER2; the pinned zip.

---

### N23 EPA builder, provenance backfill, legacy re-resolve, local (10B)

**Goal.** A clean local catalog with provenance; legacy links re-resolved with undo.

**Entry conditions.** N22 released; D-ER1, D-ER3 answered.

**Units.** P-unit specs: REMEDIATION_PLAN "Phase 10B" (line 2488).

| ID | What | Kind | Depends | Acceptance |
|---|---|---|---|---|
| P10B.1 | Builder loader and diff report | code | none | diff reviewed |
| P10B.2 | Builder `--apply`, `--archive-missing` (lifts P1A.2's refusal) | code | P10B.1, P10B.4 | apply on scratch = diff |
| P10B.3 | Import follows the single ingestion path (D-ER1) | code | P10A.8 | one code path |
| P10B.4 | Local backups, provenance backfill, verify (MAIN) | data-local | P10A.6 | owner OK on counts |
| P10B.5 | Relink cars on the 461 appended rows | code | P10B.6 | relinked or archived |
| P10B.6 | Re-resolve legacy-linked cars, `--undo` (D-ER3) | code | P10A.7 | undo round trip exact |
| P10B.7 | Catalog invariants | code | P10B.4 | green locally |
| P10B.8 | Local builder check, re-resolve, stratified sample, invariants (MAIN) | data-local | P10B.2, P10B.5, P10B.6, P10B.7 | sample ≥98% correct by VIN decode |

**Workflow shape.** Waves by dependency; MAIN units last. About 12 agents. Checkpoints: `workspace/phase_runs/N23/`; before the local apply OKs the workflow writes RESUME.md with the earliest restart time and ends.

**Exit gate.** P10B.8 sample signed; invariants green.

**Release step.** Patch; no deploy.

**Owner decisions / actions.** D-ER1, D-ER3; local apply OKs.

---

### N24 Forced-induction resolution (10C)

**Goal.** Turbo and supercharger labels come from evidence through precedence; the heuristic never overwrites evidence.

**Entry conditions.** N23 released; P8B.1 live (N16); D-ET2, D-ET4 answered.

**Units.** P-unit specs: REMEDIATION_PLAN "Phase 10C" (line 2587).

| ID | What | Depends | Acceptance |
|---|---|---|---|
| P10C.1 | vPIC boosted key | none | tests |
| P10C.5 | Tighten the 2016+ heuristic (false negatives, PHEV miss, Raptor R) | none | fixtures |
| P10C.2 | `resolve_car_forced_induction` as precedence's FI rule | P10C.1, P10C.5 | evidence beats heuristic |
| P10C.3 | Linker reconciles FI after linking | P10C.2 | tests |
| P10C.4 | `enrich_car` uses the resolver | P10C.2 | tests |
| P10C.6 | Consumers treat Naturally Aspirated as not boosted | P10C.2 | facet counts |
| P10C.7 | Upsert never lets the heuristic overwrite evidence | P10C.2, P8B.1 | cycle test |
| P10C.8 | Induction invariants; fix latent Postgres `\b` patterns | none | green on PG |
| P10C.9 | Heal tool with restore and a conflict sample; dry run re-measures D-ET2 | P10C.2, P10C.3 | dry-run numbers logged |

**Workflow shape.** Waves: (P10C.1, P10C.5, P10C.8) → P10C.2 → (P10C.3, P10C.4, P10C.6, P10C.7) → P10C.9. About 14 agents. Checkpoints: `workspace/phase_runs/N24/`; before the owner reviews the heal dry run the workflow writes RESUME.md with the earliest restart time and ends.

**Exit gate.** Invariants green; the heal dry run is reviewed by the owner.

**Release step.** Minor; the owner approves the Docker smoke of the scanner-nightly and web stages, then deploys outside the window.

**Owner decisions / actions.** D-ET2, D-ET4.

---

### N25 EPA data repairs and prod rollout (10D)

**Goal.** Patch the tracked EPA data, roll the catalog repairs out to prod in stages, add constraints, delete `DICTIONARY/`.

**Entry conditions.** N24 released; D-EPA1, D-ER4, D-ER5, D-ER6 answered.

**Units.** P-unit specs: REMEDIATION_PLAN "Phase 10D" (line 2684).

| ID | What | Kind | Depends | Acceptance |
|---|---|---|---|---|
| P10D.1 | Patch the 12,126 EPA CSVs from the pinned source | data (tracked) | none | `--verify` clean |
| P10D.2 | Patch the 10,614 Complete_Options CSVs | data | P10D.1 | diff scoped to FI fields |
| P10D.3 | Regenerate or retire `curated/trim_ladders_epa.json` | data | P10D.1, P10D.4 | consumers checked |
| P10D.4 | epa_master label refresh, local | data-local | P10D.1 | owner OK |
| P10D.5 | Cars label heal from evidence, local | data-local | P10D.4 | owner OK |
| P10D.6 | Staged prod rollout (agent prepares each step, owner approves and runs) | prod-owner | P10D.5 | each stage verified |
| P10D.7 | V032: unique key and RESTRICT FKs NOT VALID; V033 VALIDATE ships in N26 after a 0-dangling prod check | code + migration | P10D.6 | 0 dangling on prod |
| P10D.8 | Delete root `DICTIONARY/` after the content audit | deletion | P10D.6 | 0 unique content lost |
| P10D.9 | Docs: induction precedence, catalog id contract, importer usage | docs | P10D.6, P10D.8 | done |

**Workflow shape.** Wave A: P10D.1, P10D.2, P10D.7 code. Wave B (MAIN): P10D.3-P10D.5. Wave C (owner): P10D.6. Wave D: P10D.8, P10D.9. About 14 agents. Checkpoints: `workspace/phase_runs/N25/`; before each P10D.6 stage approval the workflow writes RESUME.md with the earliest restart time and ends.

**Exit gate.** Prod catalog invariants match local; V032 applied; `DICTIONARY/` removed.

**Release step.** Minor; deploys.

**Owner decisions / actions.** D-EPA1, D-ER4, D-ER5, D-ER6; stage approvals.

---

### N26 Scanner runtime remainder and upsert cost (11A rest, 11B gated)

**Goal.** Upsert telemetry and transient retries; V033 VALIDATE; upsert cost work only if measured as a bottleneck.

**Entry conditions.** N25 released (V033 rides along); D-SR6 answered.

**Units.** P-unit specs: REMEDIATION_PLAN "Phase 11A" (line 2814), "Phase 11B" (line 2899). P11A.1-P11A.4 already shipped in N10.

| ID | What | Kind | Depends | Acceptance |
|---|---|---|---|---|
| P11A.6 | Replace `datetime.utcnow()` with a byte-identical stored format | code | none | golden format test |
| P11A.7 | Upsert timing breakdown (wall and thread CPU) | code | P11A.6 | fields in `summary_json` |
| P11A.8 | Coordinator retries transient connection errors; retry counts | code | P11A.7 | tests |
| P11B.1 | Measure the breakdown on 3 dealers at batch 1 and 6 (MBP) | measurement | P11A.7 | numbers logged |
| P11B.2 | **Only if** upsert is >40% of dealer wall time, or the fleet exceeds its window: post-write enrichment borrows one read connection (covers X4) | code | P11B.1 | upsert time down ≥30% on the 3 dealers |
| P11B.3 | Same trigger: previous-price read moves into the prefetch | code | P11B.1 | same |
| P11B.4 | Same trigger: batched upsert behind a parity gate | code | P11B.2, P11B.3 | parity green on a clean-room subset |
| N26.8 | V033 VALIDATE on prod after the 0-dangling check | prod-owner | P10D.7 | validated |

**Workflow shape.** Wave A: P11A.6 → P11A.7 → P11A.8. Then P11B.1 (MAIN). The trigger decision is written into the checkpoint, then P11B.2-P11B.4 run or are marked SKIPPED. About 10-16 agents. Checkpoints: `workspace/phase_runs/N26/`; before the owner's migrate and deploy the workflow writes RESUME.md with the earliest restart time and ends.

**Exit gate.** Telemetry in `summary_json`; V033 validated; the P11B trigger outcome recorded.

**Release step.** Minor; owner migrates, approves the Docker smoke of the scanner-nightly stage, and deploys outside the window.

**Owner decisions / actions.** D-SR6.

---

### N27 DB layer (13A)

**Goal.** Portable SQL, a loud adapter, strict schema mode everywhere, no import-time init.

**Entry conditions.** N12 done; D-DB3, D-DB4, D-DB8 answered; `web-source-clear` (strict env on web).

**Units.** P-unit specs: REMEDIATION_PLAN "Phase 13A" (line 3112).

| ID | What | Kind | Depends | Acceptance |
|---|---|---|---|---|
| P13A.1 | Portable ON CONFLICT at every INSERT OR site; delete `seed_cars` | code | none | grep: no INSERT OR in inventory paths |
| P13A.2 | Adapter raises `UnportableSqlError` | code | P13A.1 | tests |
| P13A.3 | Static guard against new runtime DDL | test | none | fails on injected DDL |
| P13A.4 | Strict mode on every service and host | prod-owner | P13A.1, P13A.2 | every service boots strict |
| P13A.5 | `db_bootstrap` in every launcher (D-DB4) | code | none | grep test |
| P13A.6 | No import-time init; lazy users.db | code | P13A.5 | import issues no SQL |
| P13A.7 | Snapshot scripts write into a `backups` schema | code | none | tests |
| P13A.8 | `cars.zip_code` per D-DB8 (rec keep: no migration) | code | none | done |

**Workflow shape.** Wave A: P13A.1, P13A.3, P13A.5, P13A.7, P13A.8. Wave B: P13A.2, P13A.6. Wave C: P13A.4. About 12 agents. Checkpoints: `workspace/phase_runs/N27/`; before the strict-mode env step the workflow writes RESUME.md and ends, and the date strict mode went live is recorded as the start of N28's 7-day clock.

**Exit gate.** Strict mode live on prod, starting the 7-day clock for N28.

**Release step.** Patch; deploys.

**Owner decisions / actions.** D-DB3, D-DB4, D-DB8; the strict env.

---

### N28 Legacy DDL deletion, backup drops, scratch DB retirement (13B)

**Goal.** The migration chain is the only DDL source. Backup tables are exported and dropped. Scratch DBs are retired.

**Entry conditions.** N25 done (EPA recovery tables); N27 strict mode live ≥7 days; D-DB1 answered.

**Units.** P-unit specs: REMEDIATION_PLAN "Phase 13B" (line 3190). Stale: D-DB1's list missed prod `cars_owner_backup_20260929` (6,961 rows) and the 4 log tables. Added here, plus the N12 backup tables.

| ID | What | Kind | Depends | Acceptance |
|---|---|---|---|---|
| P13B.1 | Delete `_legacy_postgres_inventory_ddl`; init check-only | deletion | none | fresh build via the chain only |
| P13B.2 | Drop the Postgres DDL branches in `backend/db/*` | deletion | P13B.1 | P13A.3 guard green |
| P13B.3 | Drop the Postgres DDL branches in scanner, reviews, catalog, intelligence, enrichment, vector | deletion | P13B.2 | grep: no CREATE TABLE outside migrations |
| P13B.4 | Export, then drop the backup tables (extended list) | prod-owner | none | exports verified before each drop |
| N28.5 | Retire the `cars_cr_*` scratch DBs; decide whether `cars_cleanroom` stays as an archive or is dumped and dropped | owner-decision + MAIN | none | dump exists before any drop; decision logged |

**N-unit spec, N28.5.** List the local scratch DBs and their sizes. Dump each to `workspace/backups/` before any `dropdb`, with owner OK per DB. Record the decision on `cars_cleanroom` in OWNER_DECISIONS_LOG.md. **Acceptance:** every dropped DB has a verified dump.

**Workflow shape.** Serial chain P13B.1 → P13B.3; owner P13B.4, N28.5. About 6 agents. Checkpoints: `workspace/phase_runs/N28/`; before each drop OK the workflow writes RESUME.md with the earliest restart time and ends.

**Exit gate.** No legacy DDL; backups exported and dropped.

**Release step.** Patch.

**Owner decisions / actions.** D-DB1; drop OKs.

---

### N29 Dependencies and images (15B rest)

**Goal.** Per-image requirements, a pinned embedding model, CPU-only torch, a slim discovery image, pip-audit in CI.

**Entry conditions.** N3 merged (P3.8, P15B.1); D-SEC7 answered; Docker OK.

**Units.** P-unit specs: REMEDIATION_PLAN "Phase 15B" (line 3499).

| ID | What | Depends | Acceptance |
|---|---|---|---|
| P15B.2 | Requirements split per image | P15B.1 | each image builds |
| P15B.3 | Pin and bake the embedding model revision | P15B.2 | no network fetch at start |
| P15B.4 | CPU-only torch in web | P15B.2, P15B.3 | size drop recorded |
| P15B.5 | Discovery image on slim requirements | P15B.4 | smoke passes |
| P15B.7 | pip-audit in CI | P15B.2 | required job green |
| P15B.8 | Lock files with hashes plus a drift check, **only if** D-SEC7 = (a) is confirmed | P15B.4, P15B.5, P15B.7 | drift check fails on an unlocked change |

**Workflow shape.** Serial along the `Dockerfile.web` chain; P15B.7 in parallel. About 8 agents. Checkpoints: `workspace/phase_runs/N29/`; before each Docker OK the workflow writes RESUME.md with the earliest restart time and ends.

**Exit gate.** Images build and smoke; pip-audit required in CI.

**Release step.** Minor; web deploy.

**Owner decisions / actions.** D-SEC7; Docker OK.

---

### N30 Security wave 2 (Phase 16)

**Goal.** Session revocation, non-root web, scanner SSRF, CSP style hardening, a DSN privilege map, password reset enabled.

**Entry conditions.** N29 released (P15B.4 before P16.6); D-SEC2, D-SEC3 answered; Resend key ready.

**Units.** P-unit specs: REMEDIATION_PLAN "Phase 16" (line 3561).

| ID | What | Depends | Acceptance |
|---|---|---|---|
| P16.1 | Session revocation via `users.auth_epoch` (users migration on prod) | none | logout-all test |
| P16.2 | Retire dead TOTP/phone helpers; drop dead columns (D-SEC2) | P16.1 | tests |
| P16.3 | Optional owner scrub of dead seeds and phones | none | done or declined |
| P16.4 | Scanner SSRF guard at URL entry points | none | private-range recipe URL refused |
| P16.5 | Inventory and gate browser launch sites outside `backend/scanner` | none | inventory complete |
| P16.6 | Non-root web container | P16.1, P16.2, P15B.4 | `id -u` ≠ 0; volume writable |
| P16.7 | Static style attributes to classes | none | grep |
| P16.8 | Dynamic styles to data-* + CSSOM | P16.7 | grep |
| P16.9 | `style-src-attr 'none'` after a ≥7-day report-only soak | P16.8 | 0 reports, then enforce |
| P16.10 | DSN privilege matrix (D-SEC3) | none | written |
| P16.11 | Security docs; then the owner sets the Resend variables and `PASSWORD_RESET_ENABLED=1`, then `EMAIL_VERIFICATION_ENABLED=1` | all | `/forgot-password` returns 200 (was 503) |

**Workflow shape.** Wave A: P16.1, P16.4, P16.5, P16.7, P16.10. Wave B: P16.2, P16.6, P16.8. Wave C: P16.9 after the soak, P16.11. About 18 agents. Checkpoints: `workspace/phase_runs/N30/`; before P16.9's ≥7-day report-only soak and before the owner's Resend env step the workflow writes RESUME.md with the earliest restart time and ends.

**Exit gate.** Non-root web live; inline style attributes no longer allowed; revocation live; password reset on.

**Release step.** Minor; deploys.

**Owner decisions / actions.** D-SEC2, D-SEC3; users migration; Resend env (needs `web-source-clear`).

---

### N31 Postgres test breadth (14B, 14C)

**Goal.** PG coverage for the queue, grid, search, stores and admin; a SQL corpus check, a skip budget, a coverage map, `TESTING.md`.

**Entry conditions.** `pytest-pg` green in CI for 30 days since N7; D-TC4, D-TC8 answered. Individual units may be pulled into an earlier phase that touches the same area.

**Units.** P-unit specs: REMEDIATION_PLAN "Phase 14B" (line 3332), "Phase 14C" (line 3378).

| ID | What | Depends | Acceptance |
|---|---|---|---|
| P14B.1-P14B.3 | PG tests: job queue, grid cards, search golden support | N7 | green |
| P14B.4 | Search parity | P14B.3 | parity identical |
| P14B.5-P14B.8 | PG-only features, web stores, admin routes, scanner stores (D-TC4 targeted) | N7 | green |
| P14C.1 | SQL corpus check from a committed snapshot (L) | none | runs in CI |
| P14C.2 | Web boot on PG issues no DDL | none | test |
| P14C.3 | Gated-skip budget (baseline 9 asset-gated skips; 6 PYTHONHASHSEED shards) | P14C.1 | enforced |
| P14C.4-P14C.5 | Coverage map; runtime and xdist decision (D-TC8) | P14C.3 | recorded |
| P14C.6 | `docs/TESTING.md` | P14C.5 | reviewed |

**Workflow shape.** Parallel waves; about 14 agents. Checkpoints: `workspace/phase_runs/N31/`; RESUME.md records the date `pytest-pg` reaches 30 days green as the earliest start, and the workflow ends before any wait for CI history.

**Exit gate.** CI enforces the corpus check and the skip budget.

**Release step.** Patch.

**Owner decisions / actions.** D-TC4, D-TC8.

---

### N32 Shims and dead functions (17A)

**Goal.** Delete the alias shims and code referenced nowhere.

**Entry conditions.** N1.7 archive done (ksarrafi told); N19 merged (for P17A.4).

**Units.** P-unit specs: REMEDIATION_PLAN "Phase 17A" (line 3674). Every deletion is preceded by a consumer grep.

| ID | What | Depends | Acceptance |
|---|---|---|---|
| P17A.1-P17A.2 | Move importers off the 7 small shims and the window_sticker / dealer_sticker_provider / listing_gap_fill shims | none | grep |
| P17A.3 | Delete the 10 shims; AST guard test | P17A.1, P17A.2 | guard green |
| P17A.4 | Delete `synth/validate.py` after re-checking zero importers | N19 | grep |
| P17A.5 | Delete the `vdp/core.py` re-export; repoint 4 consumers | P17A.1 | tests |
| P17A.6 | `dealer_run` dead timeout helpers and stale Warmup log | P17A.1 | golden fixture edits only |
| P17A.7 | `nav.py` dead failure-HAR/warmup chain, `truncate_url` | none | tests |
| P17A.8 | Token-scan sweep of unreferenced functions | P17A.1, P17A.2 | evidence per deletion |
| P17A.9-P17A.11 | Empty providers package; one source for recovery names; sister.tv TODO | P17A.1 | tests |

**Workflow shape.** Wave A: P17A.1, P17A.2, P17A.7, P17A.9, P17A.11. Wave B: P17A.3, P17A.5, P17A.6, P17A.8, P17A.10. Wave C: P17A.4. About 12 agents. Checkpoints: `workspace/phase_runs/N32/`; before the wait for N19's merge (P17A.4) the workflow writes RESUME.md with the earliest restart time and ends.

**Exit gate.** No shims; the AST guard is in CI.

**Release step.** Patch.

**Owner decisions / actions.** None.

---

### N33 Browser path retirement (17B)

**Goal.** Remove the browser-era scan paths; scanning is HTTP-only.

**Entry conditions.** N32 merged; P12A.4 (N18) merged; D-DC1, D-DC5, D-DC6, D-DC7, D-DC9 answered.

**Units.** P-unit specs: REMEDIATION_PLAN "Phase 17B" (line 3767).

| ID | What | Depends | Acceptance |
|---|---|---|---|
| P17B.1 | Node/Puppeteer leftovers | none | grep |
| P17B.2 | `platform_registry` reuses synth; seed drift guard | none | guard green |
| P17B.3 | Retire `--allow-browser` full-scan path (L) | none | 5-dealer browser-free scan within ±2% of rows |
| P17B.4 | HTTP-only recovery chain | P17B.3 | tests without Playwright installed |
| P17B.5 | Remove scrapers' page halves | P17B.4 | parser tests |
| P17B.6 | Remove Playwright from `chain.py` and gap_fill | P17B.3, P17B.4, P12A.4 | grep: no playwright under scanner |
| P17B.7 | `requires_browser`: drop the skip branch (D-DC9 (c)) | none | tests |
| P17B.8 | Clear cached winning strategies that no longer exist (optional) | P17B.4 | 0 dangling names |
| P17B.9 | Delete the LLM adjudication branch (D-DC6 (b)) | none | grep |
| P17B.10 | Move the EPA client; delete the rest of `backend/oem` (incl. the crawl4ai comment) | none | consumer audit |
| P17B.11 | HTTP_ONLY_SCANS_PLAN decisions (D-DC1) | all | reviewed |

**Workflow shape.** Chain P17B.3 → P17B.4 → P17B.5 → P17B.6; others in parallel; P17B.11 last. About 16 agents. Checkpoints: `workspace/phase_runs/N33/`; before the deploy and the next-nightly comparison the workflow writes RESUME.md with the earliest restart time and ends.

**Exit gate.** No browser code on scan paths; the next nightly is within ±3% of rows.

**Release step.** Minor; the owner approves the Docker smoke of the scanner-nightly stage, then deploys scanner-nightly outside the window.

**Owner decisions / actions.** D-DC1, D-DC5, D-DC6, D-DC7, D-DC9.

---

### N34 Module splits and the knob registry (17C)

**Goal.** Split the oversized modules; one registry of scanner knobs; one frozen ScanConfig.

**Entry conditions.** N33 merged; D-OD9, D-OD10, D-OD11 answered; D-N29 accepted.

**Units.** P-unit specs: REMEDIATION_PLAN "Phase 17C" (line 3883).

| ID | What | Depends | Acceptance |
|---|---|---|---|
| P17C.1 | vPIC HTTP getter at module level (D-OD10) | none | offline stub patches it |
| P17C.2 | Brochure acquisition CLI (D-OD9) | none | tests |
| P17C.3 | `brochure_extract` split step 1 | none | surface golden unchanged |
| P17C.4 | `brochure_extract` split step 2 | P17C.3 | tests |
| P17C.5 | `comments_db` split | none | tests |
| P17C.6 | Env-knob registry plus AST lint (D-OD11) | none | lint fails on an unregistered knob |
| P17C.7 | Retire browser-era and unread knobs (batches ≤10) | P17C.6 | owner removes unread env vars |
| P17C.8 | ScanConfig (L) | P17C.7 | golden scan output unchanged |

**Workflow shape.** Wave A: P17C.1, P17C.2, P17C.3, P17C.5, P17C.6. Wave B: P17C.4, P17C.7. Wave C: P17C.8. About 12 agents. Checkpoints: `workspace/phase_runs/N34/`; before the owner's env cleanup the workflow writes RESUME.md with the earliest restart time and ends.

**Exit gate.** Registry enforced; ScanConfig in use.

**Release step.** Minor; owner env cleanup on Railway (web variables only after `web-source-clear`).

**Owner decisions / actions.** D-OD9, D-OD10, D-OD11.

---

### N35 Repo hygiene (18A)

**Goal.**
- Tracked junk gone and runtime state off tracked paths.
- The laptop nightly retired and the k8s manifests deleted.
- Ruff tightened as far as D-TC7 says.

**Entry conditions.** N14 done (D-IF6 answered via N14.1); N34 merged; D-HY3, D-HY4, D-IF9, D-TC7 answered.

**Units.** P-unit specs: REMEDIATION_PLAN "Phase 18A" (line 3935).

| ID | What | Depends | Acceptance |
|---|---|---|---|
| P18A.1 | Remove dead tracked junk; ignore rules (owner decides the 5+1 dealer ids) | none | done |
| P18A.2 | Runtime state off tracked paths; race-safe counter | P18A.1 | `git status` clean after a scan |
| P18A.3 | Untrack force-added workspace scratch; move the 92694 fixture (D-HY4) | P18A.2 | done |
| P18A.4 | Track only curated `_learning` files (D-HY3) | P18A.3 | done |
| P18A.5 | Retire the laptop nightly; NIGHTLY_OPERATIONS.md from the N14.1 table | N14.1 | every step mapped |
| P18A.6 | Portable report wrappers with an explicit DB target (D-IF7 answered) | P18A.5 | refuse without a target |
| P18A.7 | Delete the k8s, car-scanner, postgres manifests, build.sh k8s block, `deploy/always-on` (D-IF9) | P18A.5 | consumer audit |
| P18A.8 | Ruff wave A: exclude vendored skills, F541, facade `__all__` | none | green |
| P18A.9-P18A.11 | Ruff waves F401, F841, B023/PLE/E7/B904, **only if** accepted under D-N29 (P18A.9 touches about 60 files and runs alone) | P18A.8 | green per wave |

**Workflow shape.** Serial waves: P18A.1-P18A.4 → P18A.5-P18A.7 → each ruff wave alone. About 12 agents. Checkpoints: `workspace/phase_runs/N35/`; before the owner's dealer-id decision and the post-scan repo-clean check the workflow writes RESUME.md with the earliest restart time and ends.

**Exit gate.** Repo clean after a scan; ruff green at the configured level.

**Release step.** Patch.

**Owner decisions / actions.** D-HY3, D-HY4, D-IF9, D-TC7; the 5+1 dealer ids.

---

### N36 Docs close-out (18B) and plan closure

**Goal.** Docs match reality. Every remediation unit, every N-unit and every finding is marked DONE, DROPPED or DEFERRED.

**Entry conditions.** N35 merged; D-HY6, D-HY7 answered; the owner approves the CLAUDE.md wording.

**Units.** P-unit specs: REMEDIATION_PLAN "Phase 18B" (line 4004).

| ID | What | Depends | Acceptance |
|---|---|---|---|
| P18B.1 | RESUME_HERE as a pointer page; master-todo into EXECUTION_PLAN (D-HY6) | none | done |
| P18B.2 | README cleanup; delete `docs/SCANNER_NODE.md`; fix `debug/README.md` | P18B.1 | link check |
| P18B.3 | CLOUD_RESTRUCTURE banner and decision outcomes | P18B.1 | done |
| P18B.4 | Historical banners (can run any time) | none | done |
| P18B.5 | Re-scope SECURITY_MASTER_TODO | none | every path exists |
| P18B.6 | Docstrings: discovery tiers, hybrid_search | none | reviewed |
| P18B.7 | Location-logging contract (D-HY7); owner-approved CLAUDE.md wording | none | approved |
| N36.8 | Close the remediation plan and this plan with a status per unit; final acceptance re-measure on prod (N1.3 tool, owner read-only); memory index refresh | all | 0 TODO rows without an owner and a date |

**N-unit spec, N36.8.** Walk every row of both plans and the Appendix A index below and set a status with a commit or a reason. Run the N1.3 tool against prod (owner, read-only) and record the final numbers next to `baseline_r1` and the `baseline_r1_rules_<rules_version>` file of the same `rules_version` (numbers from different rule sets are never compared). **Acceptance:** 0 TODO rows without an owner and a date.

**Workflow shape.** Wave A: P18B.1, P18B.4, P18B.5, P18B.6. Wave B: P18B.2, P18B.3, P18B.7. Wave C: N36.8. About 8 agents. Checkpoints: `workspace/phase_runs/N36/`; before the owner approves the CLAUDE.md wording and runs the prod re-measure the workflow writes RESUME.md with the earliest restart time and ends.

**Exit gate.** Plan closed.

**Release step.** Patch.

**Owner decisions / actions.** D-HY6, D-HY7; approve the CLAUDE.md wording.

## 8. Deferred and dropped items

Every remaining remediation unit not placed unconditionally in a phase is listed here, with its reason and, where it applies, its revival trigger.

| Item | Status | Reason / trigger |
|---|---|---|
| P4.4 option_rejections dedupe (and P4.8 step 2.3) | Dropped | P0A.2 found 0 duplicate groups on prod and local; V026 enforces the index |
| P1B.5 `persist=False` for audit scripts | Dropped (earlier) | D-RS5 = keep writing (owner, 10-08) |
| P11A.5 worker loop lock-held exit | Conditional (N14) | No worker service since 10-08. Runs only if D-IF3 = (b); dropped under the recommended (a) |
| P11B.2-P11B.4 upsert cost | Conditional (N26) | Run only if P11B.1 shows upsert >40% of dealer wall time, or the fleet exceeds its run window. Fleets take about 159 min on Railway today; throughput comes from shards |
| P15B.8 lock files per image | Conditional (N29) | Only if D-SEC7 = (a) is confirmed; pip-audit in CI (P15B.7) carries the vulnerability signal |
| P16.3 scrub dead seeds and phones | Optional (N30) | Owner-only data action; P16.2 already drops the dead columns |
| P17B.8 clear cached strategy names | Optional (N33) | Cosmetic data cleanup |
| P18A.9-P18A.11 ruff waves F401, F841, B023/PLE/E7/B904 | Conditional (N35, D-N29) | Lint churn across about 60 files with no correctness payoff; wave A (P18A.8) still ships |
| 14B / 14C (N31) | Time-gated | Starts after `pytest-pg` has been green in CI for 30 days; any unit may be pulled into a phase that touches its area |
| 17C (N34) | Placed, lowest priority | No user-visible effect; runs after N33 |
| P5A.8 placement in Phase 5A | Moved to N11 | Run on a prod-shaped restore of `cars`, so it rehearses N12's 16+-day-gap first run. Its first-pass/retry shape replay moved to N9.14, so N9 is proven by a clean-room re-run before it releases |
| P8A.2, P8A.6 placement in 8A | Moved to N6 | v1.6.0 displays the vPIC drivetrain from fleet #1 on (owner rule 4); the write path and the heal stay in N15-N17 |
| P5B.1, P5B.2 placement in 5B | Moved to N9 | The 13 capped walks are an under-collection and retirement risk for the first prod refresh; `recipes.py` is open there for P5A.2 |
| P5B.5, P5B.6 | Moved to N10 | Needed for the N12 residue repair |
| P5B.8, P5B.9 | Moved to N11 | Local repair rehearsal and docs travel with the rehearsal |
| P6A.1-P6A.7 (6A as a phase) | Moved to N10 | The N12 cron and bridge need them |
| P6A.3 as written (default 72 h, gap cover from the oldest owner) | Replaced by P6A.3' | A longer window entrenches first-writer misattribution under nightly rescans. Gap cover applies only to owners with a persisted strong tier; the default stays 48 h |
| P6B.1 as one unit | Split | N12.1/N12.2 apply V027-V029 and deploy v1.6.0 idle |
| P11A.1-P11A.4 placement in 11A | Moved to N10 | Per-dealer locks must exist before a second host writes prod (Phase 15A gate; N12 bridge) |
| P14A.9 recipe CAS PG test | Moved to N13 | Needs P7.1 |
| P15B.1, P15B.6 | Moved to N3 and N4 | Security advisory (lxml PYSEC-2026-87); dead deploy files |
| P0B.5 multi-GB scratch restore | Replaced by N14.1 | A desk review answers D-IF6; a restore happens only for a step the review cannot settle |
| P0B.1 Dealer Inspire burst | Moved to N18.1 | Needed before P12A.2/P12A.5, not before the refresh |
| Mini parts of P0A.2, P0A.5, P1C.1, P1C.4; mini hooksPath; the mini as home lane or log host | Conditional (N4.4) | SSH not approved (D-N2 (b)). The units re-plan around the MBP |
| LE-14 overlap clean-up "in Phase 7" | Moved to N8.9 if D-N34 = yes | Done before the first prod fleet; if the owner declines D-N34, it stays in N13 as answered on 10-08 |
| Finish all 263 per-dealer analyses (option A, about 120 agents) or non-ok only (option B, about 54) | Dropped | Every top pattern is already seen at 20-110 dealers. N1.3 measures all 458 mechanically. Live checks were blocked at 47% of analysed dealers, and 7% of cars sold within a day. Replaced by N1.2, N1.3 and N11.10 |
| Resuming the 8 shard analysis workflows and `wf_ef900653-f48` | Dropped | Workflow resume cannot cross sessions; their tool results were salvaged and re-verified by the 10-15 gather reports |
| Analysis of the 8 pause-killed error dealers | Merged | A pause artifact; rescanned in N11 pass A; N9.13 stops the class |
| The "~8,800 in-run retirements" fix track | Merged | It does not exist (21 retirements). The work moves to N8 (conflicts) and N9 (the in-run mechanism) |
| WI-10 interim retirement provenance in `summary_json` | Dropped | V027 lands in N9 and reaches prod (N12.2) before any live retirement |
| F-4 / C.4.11 attribution "owner" deferral | Merged | Group-feed ownership is its own phase (N8), ahead of any prod fleet |
| Multi-store listing schema (one car under several stores) | Deferred | Schema is one row per VIN (`upsert/sql.py:38`). D-N3 (primary + caveat) covers the about 1,995 pool rows. Revive if the owner rejects D-N3 or residual pool conflicts exceed about 2,600 after N12 |
| Interim prod refresh on today's code | Dropped | Today's code would re-file about 55% of contested VINs by scan order. Replaced by the "last checked" line (N3.11) and the N2 hotfix |
| Prod DB wipe for a clean start | Dropped | Owner rule 2: the prod DB is never wiped |
| FP19 packages/stickers missing in the clean room | Dropped | Paid steps were deliberately off (RUNBOOK) |
| M3 CTP outside the mileage rule | Dropped | Fixed by F14 (0 CTP conditions fresh); stale rows heal on the N12 refresh |
| INCOMPLETE 2.10 Metro Honda as a standalone item | Merged | Prevention in N6.9; the 410 stale rows in N12.10 |
| X4 per-car index sync cost | Conditional | Covered by P11B.2 (N26, gated) |
| D-HY8 live EIA gas prices | Deferred | Status quo fallback anchors |
| D-OD12 brochure grid "P" marker | Deferred | Belongs to an enrichment follow-up |
| `docker image prune` | Owner action | Section 6 item 14 |

## 9. Risks and open questions

### 9.1 Risks

| Risk | Mitigation |
|---|---|
| Live prod retirement before the policy has run at scale. In the clean room, 11 of 21 in-run retirements were wrong | Two-strike (V028) and `listing_retirements` (V027) are on prod before fleet #1. N11 pass D proves retirement with a live check. Fleet #1 only strikes, and its strike list is live-checked. A single no-retire switch exists (P5A.4) |
| The first prod fleet re-files many VINs at once (the guard has lapsed since 09-29) | N11.5 rehearses exactly this on a copy of the populated clean room. Snapshots before and after, every move in `car_move_log`, N11.4 determinism ≥99%, no manual bulk reset |
| Cosmos stamping without a learned own-site id refuses a whole feed (`rooftop.py:836-844`) and empties a dealer | Hints come from the N1.2 maps; a missing hint falls back to keep plus a log line; acceptance floor of 80% of expected own rows per dealer |
| The pool policy (D-N3) is a business rule, not a fact: about 1,995 pool rows stay single-owner | The site shows "listed by N group stores; location unconfirmed"; the multi-store schema has a revival trigger |
| Owner bandwidth: about 100 decisions and about 30 owner-run prod steps | "Accept all except" briefs; a named gating minimum; pre-written one-line packs with done-files; phases continue around BLOCKED units |
| `SCAN_FLEET=0` staged with `--skip-deploys` applies only to the next new deployment; a rollback restores `SCAN_FLEET=1`; a restart relaunches the same deployment with its old variables (`RELEASING.md:579-580`), so a restart is a full fleet on 1.4.1 | Never roll back, restart or redeploy scanner-nightly until N12.2; no N4 settings change creates a deployment; stop trouble with `railway down -s scanner-nightly` |
| Web's GitHub source may still be connected | It blocks only web variable steps (`TRUSTED_PROXY_CIDRS`, `ADMIN_PASSWORD`, strict env, Resend). Those steps wait; nothing else does |
| Config-as-code ends 2026-12-01; the replacement mechanism is unverified | N4 runs in parallel from the start, with a carve-out release by 2026-11-20 |
| Merge contention across parallel phases; N8.6 waits on N9's release (critical-path coupling) | The Section 3.3 file-ownership table; serialized releases. The only migration order is V027/V028 = N9, V029 = N8.6 (P5A.5 in N9 supplies the `drop_unattributable_vehicles` VIN return N8.6 relies on) |
| No-deploy releases carry migrations and web-facing code that the next web deploy ships, and web boot applies pending migrations | Section 7.0 migration check before every web deploy; N2, N3 and N4.5 web deploys sequenced before N9's release; later ones apply V027-V029 with a dump first or wait |
| Mac sleep or SIGSTOP killed agents three times and 8 batches once | Per-unit checkpoints in `workspace/phase_runs/`; `fleet_scan --resume` (N9.15); N9.13 pause-safe timeout; lid open, caffeinate checked (AC recommended for runs over about 2 h); commands under 150 s |
| Home-IP rehearsal vs Railway egress (63 HTTP 403 even from home; 31 auth-staled dealers on prod) | P6B.4 measures Railway first; the blocked dealers stay on the bridge, then on the N14 lane |
| Home-IP reputation: about 6 full or partial local fleets (N8.10, N9.14, N11 passes A-D, N11.13, P5B.8) plus the N1.2/N8.9 live fetches all come from the home IP, which N12 then relies on for the 31 Railway-blocked dealers (bridge, P6B.2/P6B.3) | Space the passes; keep pass B to the 109 dealers; compare each pass's 403/429 counts against the clean-room 63/9 baseline and stop if either rises by more than 50%; live fetches stay paced (≥2 s, ≤3 pages per dealer) |
| The clean-room roster is 458 dealers; prod has 561. 103 dealers and about 120 prod-only conflict pairs (212 on prod, 92 rehearsed) are unrehearsed | N11.13 scans the 103 dealers into `cars_cr_rc` and classifies every prod-only pair; P6B.4 smoke; N12.7 caps moves in unrehearsed pairs at 2× the N11.13 count and lists them |
| Prod stays stale for about 3-4 more weeks (about 30% turnover in 16 days, an upper bound) | The N2 hotfix and the N3.11 "last checked" line ship early; the critical path is kept minimal |
| Live evidence decays (7% of cars sold within a day) and is blocked for 47% of dealers | Paced checks from the home IP; acceptance allows cannot_assess ≤10%; DB-checkable class-(b) tests carry the main proof |
| Fresh clean-room numbers overstate VDP-dependent gaps (no carry-forward; 4,293 paired cars lost photos) | Compare like with like (pass A against `baseline_r1`, incomplete counts against `baseline_r1_rules_<rules_version>` with no window); steady-state targets come from N12 prod measurements |
| Local PG is 17.10; prod is 18.6 | The CI harness uses pg18 (D-TC3); the PG18 client is used for prod dumps |
| Two-strike plus fail-closed keeps sold cars listed for at least one more run (≥20 h) | Accepted under D-N11; the gap-aware alarm keeps the first live run honest |
| Public repo | Secrets and large-file scan before every push; `arr.pkl` moved out (N1.9) |
| Conflict classes beyond the DB checks are estimates ("about 55% wrong owners" is likely, not verified) | Acceptance uses only DB-checkable class-(b) tests and live samples |

### 9.2 Open questions (unverified as of 2026-10-15)

- Is web's GitHub source disconnected? The last read-only check, on 10-09, still showed it set. (N1.5)
- Is `SCAN_FLEET` still 1 on scanner-nightly, and is `CAR_CHAT_WEB_RESEARCH=0` still on web `848895c1`? Variables were last read 10-07/08. (N1.5, N1.6)
- Does prod hide new cars with NULL mileage today, i.e. what is IL-02? (N1.6)
- Do DealerOn cosmos payloads carry a stable per-store id beyond the photo path? Are the photo-account mappings right? (N1.2)
- Why did the CarsCommerce gate refuse 1,519 of 1,521 rows of listing 2309549 on 09-28 but stay silent on 10-13? (N1.2, N8.8)
- What is the real MB Beverly Hills endpoint, and is that store in scope? (N1.2, D-N6)
- Are Hendrick `RHendrickUsed` cars at Terry's address for sale at Terry? (D-N5)
- Which config mechanism does Railway support after 2026-12-01? (N4.1)
- Does a Railway restart re-read variables? Answered: no. `RELEASING.md:579-580` records that a restart relaunches the same deployment with its old variables, so a scanner-nightly restart before N12.2 is a full fleet on 1.4.1. Still open: what `railway down` does to a Completed deployment (Section 6 item 1).
- Did the P2B.6 Docker smoke run before `848895c1`? (recorded as "not recorded" in N1.9)
- Which code commit did the clean room run? Likely v1.5.4; not stamped (N1.3 stamps from now on).
- Are the 13 lots whose replay hit the 40-page cap really truncated? (P5B.2, N11.3)
- 9,638 vs 9,610 conflicts: most likely retried first-pass claims kept in the table but replaced in triage. Unverified.
- Does car chat work on 1.5.4 now that the anthropic SDK ships? (Section 6 item 11)
- How many of prod's 09-29 owners are wrong for the same pairs? Only the read-only census is planned (N1.6); fleet #1 re-files by evidence.

## 10. How to run a phase

### 10.1 Prompt template

Paste into a new Claude Code session and edit the brackets.

```
Run Phase <NID> of docs/NEXT_PHASES_PLAN_2026_10.md as a Workflow.

Answered owner decisions for this phase (from OWNER_DECISIONS_LOG.md):
  <D-xxx: chosen option>   (one line each; the phase section lists the ones it needs)
Owner approvals for this run:
  <e.g. "Docker smoke OK", "cars_cr_n8 scratch DB OK", "paced live fetches per D-N25 OK",
   "git switch phase/<NID> before MAIN scans OK">
  (A scan, scratch DB or Docker build that is neither in D-N25 nor on this line is BLOCKED.)

Rules:
1. Read, in this order: the phase section in NEXT_PHASES_PLAN_2026_10.md (Section 7.0
   conventions included); every REMEDIATION_PLAN_2026_10.md section it points to for
   P-units, plus Appendix C there; CLAUDE.md; docs/NETWORK_SCAN_PROCESS.md;
   docs/RELEASING.md if the phase releases or deploys.
2. Checkpoints: before starting, read workspace/phase_runs/<NID>/RESUME.md and every
   workspace/phase_runs/<NID>/<unit>.json. Skip units per Section 10.3. Write each
   unit's JSON the moment its state changes, and rewrite RESUME.md after every unit.
3. Branch: phase/<NID> cut from origin/main (create it only if absent). One git
   worktree per unit at .claude/worktrees/<unit id> on branch unit/<unit id>. Units
   marked MAIN run serially in the main checkout after their dependencies merge.
4. Fan out by wave. Do not start a unit whose blocking decision or approval is missing:
   mark it BLOCKED with the reason and continue with the rest.
5. Each implementing agent does exactly its unit: runs only its tests, in chunks under
   120 s per tool call (wrap slow commands in `timeout 140`; nothing over 150 s);
   never touches prod; never bumps VERSION; never commits .env; writes dealer_logs for
   every dealer it touches; uses read-only psql (PGOPTIONS default_transaction_read_only)
   for every query that does not belong to an approved local apply.
6. One adversarial reviewer per unit: checks every acceptance item with a command or a
   query, the SQLite-vs-Postgres behaviour, the audit-deletions rule, and that the diff
   stays inside the unit's files and the Section 3.3 ownership table. At most 2 rounds.
7. Local data applies: backup, then dry run, then STOP and report the counts. Apply only
   after my OK.
8. Prod steps: write the one-line command pack to workspace/phase_runs/<NID>/<unit>.md
   (plain terminal, no "!", always -s, railway status first) and wait for
   <unit>.done. Then verify from the owner's pasted outputs and write
   docs/ops_log/<unit>.md. An agent read of prod needs a per-unit owner grant recorded in
   OWNER_DECISIONS_LOG.md (the 10-07 grant covered Phase 0A only).
9. Before any wait of more than about 1 hour (a 20 h pass gap, an owner step), write
   RESUME.md with the exact next action and the earliest restart time, and end the
   workflow.
10. Integrate in the phase's order onto phase/<NID>, run the exit gate, add CHANGELOG
    lines. Stop before the release step and report: units DONE / BLOCKED / DROPPED /
    SKIPPED, exit-gate results with numbers, decisions needed, and the exact release
    (and deploy) commands for me to run.
11. Review and test agents never run the test suite or any check as a background
    command: they wait for the result (backgrounded suites ended review agents early).
12. Never send a message to a workflow agent while it runs: a message can resume a
    second copy that edits the same files. Put corrections into the next workflow run.
13. On resume, never edit a prompt constant shared by finished agents: the cache keys
    on the exact prompt, so an edit re-runs every finished agent that embedded it. Add
    new rules in a separate constant appended only to the prompts still to run.
```

**Resume prompt** for a new session: `Resume Phase <NID> of docs/NEXT_PHASES_PLAN_2026_10.md from workspace/phase_runs/<NID>/RESUME.md. Same rules as the phase prompt.`

### 10.2 Checkpoint file

`workspace/phase_runs/<NID>/<unit id>.json`:

```json
{
  "phase": "N8",
  "unit": "N8.1",
  "status": "TODO | RUNNING | REVIEW | DONE | BLOCKED | DROPPED | SKIPPED",
  "branch": "unit/N8.1",
  "worktree": ".claude/worktrees/N8.1",
  "base_sha": "<sha the worktree was cut from>",
  "merged_sha": "<sha on phase/N8 after merge, or null>",
  "tests": [{"chunk": "backend/tests/test_rooftop_match.py", "result": "pass", "seconds": 41}],
  "review": {"rounds": 1, "verdict": "pass", "notes": "..."},
  "acceptance": {"cleveland_ch_da_br_rows": 0, "mtnview_ch_rows": 311},
  "blocked_on": null,
  "artifacts": ["workspace/cleanroom_2026_10/analysis/groups/mtnview.json"],
  "updated_at": "2026-10-20T14:02:00Z"
}
```

`RESUME.md` holds:
- a table of units with status and merged SHA;
- the next action, as one sentence;
- any running background process (pid, log path, how to check it);
- open owner asks.

### 10.3 Re-run rule

- **DONE** units whose `merged_sha` is an ancestor of `origin/phase/<NID>` (or of the local phase branch) are skipped.
- **RUNNING or REVIEW** units restart in a fresh worktree. First check the stale worktree for unpushed commits (`git -C <worktree> log @{u}..` or against `base_sha`) and salvage them, then remove it.
- **BLOCKED** units re-check their blocker and start if it is cleared.
- **Investigation units** write one file per group, dealer or platform; a re-run skips files that pass the schema check.
- **Scan units** resume with `fleet_scan --resume <run_dir>` (N9.15): dealers with a triage entry or a `scan_runs` row started at or after the run start are skipped, and the shard rosters and `triage.json` are reused. The run dir holds `RESUME.md` and per-shard `triage.json`. Before N9.15 merges, only short scans run (N1.3's smoke, N6.2, the N8 mini re-runs); a killed one restarts into a fresh scratch DB.
- **Owner-step units** are skipped once `<unit>.done` exists and the verifier's ops-log block is written.
- **Waits:** a unit that needs a ≥20 h gap records the earliest start time in RESUME.md. The resumed session checks the clock before starting it.

## Appendix A. Index of work items to units

| Items | Units |
|---|---|
| LE-1 LE-2 LE-12 | Section 6 items 1-6, N1.5, N12.2, N12.3 |
| LE-3 LE-4 | N1.7 |
| LE-5 | N4.3 |
| LE-6 LE-10 LE-13 | N1.9 (LE-13 also N1.1) |
| LE-7 | D-N2, N4.4 |
| LE-8 | N14.1 |
| LE-9 | P6B.4 (N12), N18.1 |
| LE-11 | Section 6 items 8-13 |
| LE-14 | N8.9 if D-N34 = yes, else N13 |
| NEW-1 | N8 |
| NEW-2 | N4.1 |
| NEW-3 | D-N1 |
| NEW-4 | N1.1, N1.2 |
| NEW-5 | N3.10, P15A.3 |
| NEW-6 | N1.9 |
| OPS-1 OPS-2 OPS-3 OPS-7 OPS-9 | Section 6, N1.5 |
| OPS-4 | N1.7, N1.9, N4.3 |
| OPS-5 | Section 6 items 4, 8-10, 13 |
| OPS-6 | N4.4 |
| OPS-8 | N4.1 |
| OPS-10 OPS-11 | N1.9, Section 6 item 16 |
| DEC-1 | Section 5, N1.8 |
| SEC-1 | N3 |
| SCAN-1 | N8-N12 |
| CF-1 CF-2 | N8.1, N8.2 |
| CF-3 | N8.5, N8.9, P12B.5 |
| CF-4 | N8.6 |
| CF-5 | N8.3, N8.4 |
| CF-6 | D-N3, N8.6 |
| CF-7 | N8.10, N11.3 |
| CF-8 | N8.5 |
| CF-9 | N8.7 |
| CF-10 | N1.1, N9.12 |
| CF-11 | N1.2, N8.8 |
| CF-12 | D-N4, N8.9, N8.6 (the `database.py:43-46` comment) |
| CF-13 | N1.2 |
| CF-14 | D-N22, N1.6, N10.7, N11.13, N12.7 |
| CF-15 | D-N8, N9.12 |
| RET-1 | N1.1, N9.12 |
| RET-2 RET-3 RET-4 RET-5 | P5A.1, P5A.5, P5A.7, P5A.2 |
| RET-6 RET-7 | P5B.3, P5B.4 |
| RET-8 | N9.13 |
| RET-9 | D-N5, N8.8 |
| RET-10 | P5B.2 |
| RET-11 | N9.14, N11.5-N11.7 |
| RET-12 | N1.2 |
| RET-13 | N1.1, N8 |
| W01 | N1.1, N9.12 |
| W02 | D-N3, N8.6 |
| W03 | N6.6 |
| W04 | N6.5, N12.10 |
| W05 | N8.7, P12A.5 |
| W06 | P5B.1, P5B.2 |
| W07 | N9.13 |
| W08 | D-N18, N21.1 |
| W09 | P8A.1, P8A.6, P8A.7, P8B.8 |
| W10 | N1.2 |
| W11 | N11.10, N21.11 |
| W12 | N8.2, N8.9, P6A.6 |
| W13 | N18.8 |
| W14 | N9.12 |
| W15 | N21.6 |
| W16 | N8.3 |
| W17 | D-N17b, N21.2 |
| W18 | N21.3 |
| W19 | N8.9, N21.10 |
| W20 | P11B (N26, gated) |
| W21 | P8B.6 |
| IL-01 IL-02 | N2.1, N1.6, N2.2 |
| IL-03 IL-04 | N6.1, N6.2 |
| IL-05 | N6.3, P5A.4 |
| IL-06 | N6.7, N12.10 |
| IL-07 | N6.8 |
| IL-08 | N15.5 |
| IL-09 | N6.9, N12.10 |
| IL-10 | N1.2, N8.4 |
| IL-11 | N21.4 |
| IL-12 | N8.7, P12A.5 |
| IL-13 | N6.4, N9.12 |
| IL-14 | N6.9 |
| IL-15 | P8B.6 |
| IL-16 | N21.7 |
| IL-17 | N21.5 |
| IL-18 | N8.5, N18.8 |
| IL-19 | N8.9 |
| IL-20 | P8B.4 |
| IL-21 | P15A.4, P18A.5 |
| IL-22 | N9.12 |
| IL-23 IL-24 | N21.8 |
| IL-25 | N6 re-baseline, N11.3, P6B.8, N21.12, N36.8 (compared within one `rules_version`) |
| WI-1 | N1.1 |
| WI-2 | N8.5 |
| WI-3 | N9.12 |
| WI-4 | N8.1, N8.11 (DEP) |
| WI-5 | D-N3 |
| WI-6 WI-12 WI-13 | N8.6 |
| WI-7 | P5A.2 |
| WI-8 | P5A.5 |
| WI-9 | P5A.4 |
| WI-10 | dropped (Section 8) |
| WI-11 | P5B.3, plus moves in `car_move_log` (N8.6) |
| WI-14 | N1.4, N7.9, N8.6 |
| Code-map residuals: W2 replacement, admin delete paths, B4 `handover_pending` | P5A.7 (rule 8 test), P5B.3, N9.12 |
| FP01 group feeds | N8 (N8.1-N8.6, N8.8 for autosavvy, infinitiofcharlotte, tedrussellford, kearnymesasubaru, hendricksubaruhoover; N8.11 for DEP) |
| FP02 | P5A.2, P5B.1, P5B.2, P12A.2 |
| FP03 | N8.7, P12A.5 |
| FP04 | P12A.5 |
| FP05 | N6.6, N9.12 |
| FP06 | N6.5, N18.8 |
| FP07 | N6.7, N21.1 |
| FP08 | N15, N16 (P8A.7 acceptance; N16 exit gate) |
| FP09 | N6.8, N21.6 |
| FP10 | P6A.6, N8.2, N8.9, N19 |
| FP11 | N9.13 |
| FP12 | N9.12 |
| FP13 | N8.9, N21.10 |
| FP14 | N21.3 |
| FP15 | N26 |
| FP16 | N8.3, N6.5 (single-bracket tokens, option-code colors) |
| FP17 | N6.9, N21.2 |
| FP18 | N21.9 |
| FP19 | dropped (Section 8) |
| INCOMPLETE X1, X2, X5 | N6.1, N6.2 |
| INCOMPLETE X3 | N6.1, N6.3, P5A.4 |
| INCOMPLETE X4 | P11B.2 (N26, gated) |
| INCOMPLETE F1, F3 | N21.8 |
| INCOMPLETE F2 | N6.1 |
| INCOMPLETE F4 | D-N16, N6.9 |
| INCOMPLETE F5 | N18.8 |
| INCOMPLETE M1 | N21.8 |
| INCOMPLETE M2 | N6.9, D-N17b, N21.2 |
| INCOMPLETE M3 | dropped (Section 8) |
| INCOMPLETE M4 | P8B.6 (D-N32) |
| INCOMPLETE M5 | P8A.7 (1,409 PHEV labels; the 32 BEV rows healed or cannot_assess) |
| INCOMPLETE M6, M7 | N6.8, N21.6; N6.7, N21.7 |
| INCOMPLETE §2.1 price | N6.4 (window), N6.7, N8.7, P12A.5, N21.1 |
| INCOMPLETE §2.2 trim (4,293 fresh) | N8.4, N21.4, N21.8, P8B.6; the residual measured in N21.12 |
| INCOMPLETE §2.3 body style | P8B.6 |
| INCOMPLETE §2.4 photos | N6.8, N21.6 |
| INCOMPLETE §2.5 exterior color | N6.5, N8.7, P12A.5 |
| INCOMPLETE §2.6 prefetch | N8.7, P12A.5 |
| INCOMPLETE §2.7 used at 0 mi | N6.9, D-N16 |
| INCOMPLETE §2.8 guessed URLs | N8.5, N18.8 |
| INCOMPLETE §2.9 Gardena | N21.4 |
| INCOMPLETE §2.10 Metro Honda | N6.9, N12.10 |
| INCOMPLETE §2.11 stale rows kept by guards | P5A.1, P5A.4, P5B.4, N12.8 |
| INCOMPLETE §2.12-§2.14, §2.17 engine, fuel, drive, decode | P8A.5, N15.5, P8B.4 |
| INCOMPLETE §2.15, §2.16 recovery machinery | P15A.4 |
| INCOMPLETE §2.18 dead recovery script | N21.8 |
| INCOMPLETE §2.19 recovery writes without provenance (≤273) | P8B.7, P8B.9; acceptance "0 raw fact UPDATEs in recovery scripts" |
| INCOMPLETE §2.20 coverage reporting | N9.12 (log truncation, honest coverage lines) |
| INCOMPLETE §2.21 junk prices | N6.7, N12.10 |
| INCOMPLETE §2.22 interior color | D-N33, N21.7 |
| INCOMPLETE §2.23 first-scan arrivals | D-N13, N6.4 |
| INCOMPLETE §2.24 smaller gaps | N21.8 |
| INCOMPLETE §2.25 high-rate dealers | N21.5 |
| Quick wins 1, 2 | N6.1; N6.3, P5A.4 |
| Quick wins 3, 11 | N21.8 (the CTP part of 11 is dropped with M3) |
| Quick wins 4, 5, 6 | N6.7; N15.5; N6.9, N12.10 |
| Quick wins 7, 8, 9, 10 | P8A.5; N8.7, P12A.5; N18.8; N6.8 |
| Quick win 12 (toyotacarlsbad `pages_needed`) | N11.3: toyotacarlsbad has a `scan_runs` row and its walk did not stop at `page_cap` |
