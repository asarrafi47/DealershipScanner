# Clean-room RCA, October 2026

Root-cause record for the clean-room fleet scan of 2026-10-13/14 (DB `cars_cleanroom`, run `workspace/cleanroom_2026_10/runs/fleet_20261013T140256Z`). Plan unit N1.1 of `docs/NEXT_PHASES_PLAN_2026_10.md`.

- **Written:** 2026-10-18, on branch `phase/N1` at `078a0c211`. Every figure below was re-run on that day. All SQL ran in read-only sessions on local Postgres (`-h /tmp -p 5432`). No live HTTP requests were made for this document.
- **Scan code:** the commit the scan ran on was never stamped. It was most likely v1.5.4 (no commits after 2026-10-09). Code line references below are at `078a0c211`. N1.3 and N1.4 only added new files, so lines in existing files are the same as at v1.5.4.
- **`cars_cleanroom` is frozen** (owner decision D-N1). It is read-only for good. Re-runs go into new `cars_cr_*` databases.
- **How to read the references.** `[Qn]` is a SQL query in Appendix A.1. `[Sn]` is a script in Appendix A.2. `[T]` is the N1.3 acceptance tool: its saved output is `workspace/cleanroom_2026_10/analysis/baseline_r1.json`, and Appendix A.3 says how it was re-run. A path like `s5/scanner.log:1973` is a line in the run directory. `[G1a]`…`[G19]` are the group dossiers `workspace/cleanroom_2026_10/analysis/groups/group_<key>.json`.
- **Sources this document replaces.** The 10-15 gather reports under `workspace/phase_runs/N1/gather/` (conflicts RCA, retirements RCA, code map). Where this document and those reports disagree, this document wins. Section 11 lists every correction.

---

## 0. Bottom line

1. **The "~8,800 in-run retirements" never happened** (corrected premise). The run retired **21 rows** [Q1]. The 8,776 gap between the fleet summary's 208,233 rows and the 199,457 active rows is accounting: 9,610 VIN-owner guard refusals minus 834 rows written by two dealers whose batch was killed (Section 2).
2. **All 21 retirements came from one writer**, the scanner's `inventory_reconcile`, inside lifecycle retries on 2026-10-14 between 13:29:30Z and 13:45:50Z. 10 were correct and 11 were wrong (all 11 at Terry Labonte Chevrolet). A live re-check on 10-16 confirmed the 11 wrong ones are still for sale at Terry (Section 3).
3. **The real loss is VIN-owner conflicts:** 9,638 rows, 7,393 VINs, 92 owner/claimant pairs [Q2]. In nearly every row both dealers list the same car with the same stock number. Group sites share one inventory, and the guard gives the car to whichever site wrote first.
4. **19 conflict groups were classified car by car from paced live feeds (N1.2).** They hold 9,615 of the 9,638 rows. Result: correct refusals 2,179; wrong winners 3,417; third rooftop on the roster 1,288; third rooftop off the roster 2,259; pool with no evidence 312; unknown 160 (Section 4.5).
5. **Every one of the 19 groups has a per-car store field in its feed.** The 10-15 estimate of 1,995 rows in "pools with no evidence" was wrong: only 312 rows remain without evidence, all in one group, most likely cars sold or moved after the scan. The parsers drop the store field, so the rooftop gate keeps every row (root cause 1).
6. **Six root causes**, detailed in Section 5: parsers drop the store field; the guard has no evidence tie-break; one recipe is cross-wired; DealerEProcess pools are unparsed; a weak name match keeps a portal's sister cars; rooftops missing from the roster land under sisters with no conflict row.
7. **The 21-hour pause caused no retirement and no conflict.** It did kill 8 batches and left 21 dealers with an `error` verdict (Section 6).
8. **The owner changed D-N4 on 10-16:** rooftops missing from the roster get their own dealer profile and are scanned as their own dealer. The dossiers found 60 confirmed off-roster rooftops (2 of them already exist as entities and need repair) and 15 likely ones. 4,446 `cars_cleanroom` rows of the confirmed ones are filed under a sister store today (Section 7).

---

## 1. The corrected premise

| Claim (memory, RESUME.md, task brief before 10-15) | What the data says | Reference |
|---|---|---|
| About 8,800 rows were retired inside the run | 21 rows are inactive; 199,457 are active | [Q1] |
| The fleet summary's 208,233 rows are rows written | They are rows captured before the VIN-owner guard. `assess.py:104-108` sets `rows` from `capture_coverage.n`, which `persist.py:18` computes before the guard drops refused VINs (`persist.py:25-39`) | code |
| The gap is retirement | 208,233 − 199,457 = 8,776 = 9,610 guard refusals − 834 rows of two killed dealers | [S1], [Q13] |

The memory note `project_cleanroom_scan_2026_10.md` and its MEMORY.md index line already carry the corrected premise (checked 2026-10-18; this unit did not edit memory). `workspace/cleanroom_2026_10/analysis/RESUME.md` was corrected by this unit.

---

## 2. Accounting identity

| Quantity | Value | Reference |
|---|---|---|
| Dealers in the roster / with a triage `rows` value / with `rows = null` | 458 / 437 / 21 | [S1]; [T] `identity` |
| Captured feed rows (sum of triage `rows`) | 208,233 | [S1]; `fleet_summary.json` `rows`; [T] |
| VIN-owner guard refusals (sum of triage `vin_owner_conflicts`) | 9,610 | [S1]; `fleet_summary.json`; [T] |
| Captured − refused = stamped (sum of `rows_stamped`) | 198,623 | [S1]; [T] |
| Dealers where `rows − vin_owner_conflicts = rows_stamped` | 437 of 437 | [S1]; [T] `dealers_rows_minus_active_eq_refused` |
| Rows of the 2 `error` dealers with `rows = null` (righthonda-com 553, written 15:34:01Z; selmanchevy-com 281, written 15:34:35Z; no scan_runs row for either) | 834 | [Q13]; [T] |
| Stamped + 834 = active (equals the sum of triage `active_after`) | 199,457 | [Q1]; [S1]; [T] |
| Inactive rows (all with `listing_removed_at` set) | 21 | [Q1]; [T] |
| Total rows in `cars` = distinct VINs | 199,478 | [Q1] |
| `car_move_log` rows | 0 | [Q4] |

**Identity:** 208,233 captured − 9,610 refused = 198,623 stamped; + 834 = 199,457 active; + 21 inactive = 199,478 rows.

**Three conflict counts, one table.** `vin_owner_conflicts` holds 9,638 rows, keyed by (vin, owner, claimant) across first passes and retries [Q2]. Triage sums each dealer's latest run: 9,610. `scan_runs` sums every run: 10,684 = 9,610 + 1,074 from the first passes of 18 dealers that ran twice [Q12]. Of those 18, 16 were lifecycle retries (`s0.out:75`, `s2.out:72`, `s4.out:73`, `s5.out:74`, `s6.out:75`, `s7.out:73`). The other 2 (gmcchampion-com, milehighhonda-com) ran in the smoke run and again in the fleet.

---

## 3. The 21 retirements

### 3.1 Which writer retired them

| Writer | File:line | Rows retired | Reference |
|---|---|---|---|
| Scanner `inventory_reconcile` (after every full scan, including lifecycle retries) | SELECT `backend/scanner/inventory_reconcile.py:240-244`, UPDATE `:323-330`; called from `phases/dealer_run_steps/after_write.py:124-136` via `phases/dealer_run.py:120` | **21** | `s5/scanner.log:1973`, `s2/scanner.log:1892`, `s0/scanner.log:1764`, `s7/scanner.log:1729`; sum of `scan_runs.summary_json.reconcile.marked_inactive` over 455 runs = 21 [Q11]; [T] `retirements.by_writer` |
| Pipeline `reconcile_dealer` | `backend/scanner/pipeline/reconcile.py:94`, `:152-155` | 0 | [T] `retirements.by_writer.pipeline.reconcile_dealer` |
| Lifecycle retry reconcile | `backend/scanner/pipeline/lifecycle.py:335-338` | 0 (the scanner had already retired the rows) | retirements RCA §3 |
| Rooftop disown | `backend/attribution/disown.py:72-81` | 0 | `rooftop_disowned` 0 on every scan_run (retirements RCA §3) |
| VIN-owner guard | `backend/scanner/database.py:83-101` | 0 (it skips the write; it never retires) | code |
| Manual and delta moves | `scripts/apply_attribution_moves.py`, `delta_scan.py` | 0 | `car_move_log` = 0 [Q4] |

All 21 have `listing_removed_at` inside the post-resume lifecycle retry window: 13:29:30Z (17 rows), 13:32:25Z (1), 13:45:50Z (3) [Q5]. None of the 21 VINs is in `vin_owner_conflicts` [Q6].

### 3.2 Per dealer

| Dealer | Rows | First pass: scan_runs id, finished (Z), upserted, verdict | Retry: scan_runs id, retired at (Z), upserted | Recipe scope, first pass → retry | First-pass rows re-seen by the retry | Verdict |
|---|---|---|---|---|---|---|
| terrylabontechevy-com | 16 Used | 385, 10-13 15:31:48, 16, no_rows (`s5.out:80`) | 442, 10-14 13:29:30, 585 | CarsCommerce listing 9542 (Hendrick group pool), no source filter; the walk stopped at the 40-page cap (`s5/scanner.log:1664`) → same listing with `facetFilters.source_id = ["114069"]` (Terry's own DMS feed) | 0 of 16 | 5 correct, 11 wrong |
| coastlinecdjr-com | 3 New | 92, 10-13 14:22:47, 11, no_rows (`s2.out:75`) | 455, 10-14 13:45:50, 249 | CarsCommerce listing 6061839 with `source_id = ["27175"]` (an abandoned account) → Motive Algolia `dealer_ids:"2927"` | 8 of 11 | 3 correct |
| napletonhondamortongrove-com | 1 New | 357, 10-13 15:28:03, 17, no_rows (`s0.out:78`) | 450, 10-14 13:32:25, 404 | CarsCommerce listing 6000784 with `source_id = ["207385"]` (a 13-car OEM feed) → same listing, no source filter | 16 of 17 | 1 correct |
| southcountylexus-com | 1 Certified | 392, 10-13 15:32:21, 15, no_rows (`s7.out:78`) | 441, 10-14 13:29:30, 193 | dealer.com `pageAlias SITEBUILDER_L_CERTIFIED_INVENTORY_1` → `INVENTORY_LISTING_DEFAULT_AUTO_CERTIFIED_USED` | 14 of 15 | 1 correct |

References: run ids, times and upserted counts [Q9]; re-seen counts [Q10]; recipes [Q22] (the first-pass recipe is the one in `cars`, which the clean room copied at build time; the retry recipe is the re-synthesized one now in `cars_cleanroom`).

**Total: 10 correct, 11 wrong.** The 11 wrong are all Terry used cars.

### 3.3 Mechanism

A lifecycle retry re-synthesizes the recipe with a different scope and scans again inside the same pipeline run. The scanner reconcile then measures coverage **after** the retry's upsert (SELECT at `inventory_reconcile.py:240-244` runs after `persist`), so the denominator includes the retry's own new rows. Terry's used bucket was 134/150 = 89% (134 used of 585 in the retry, `s5.out:136`; 150 = 134 + 16) and its lot 585/601 = 97% [Q10], both above `SCANNER_RECONCILE_MIN_COVERAGE` 0.5. The 16 first-pass rows had come from the unscoped group pool. The store-scoped retry did not see them, so all 16 were retired.

- The 16 Terry rows are first-pass rows of an unscoped group feed [Q10: 601 rows, 585 first seen on 10-14].
- Planned P5A.1 rule 4 (measure coverage against prior rows by VIN, share ≥ 0.6, matched > 0) would retire Coastline 3 (8/11 = 73%), Napleton 1 (16/17) and South County Lexus 1 (14/15), and keep all 16 at Terry (0/16). Two-strike (D-RC2, P5B.4) would defer all 21.

### 3.4 The 21 VINs, with the 10-16 live re-check

Source: `workspace/cleanroom_2026_10/analysis/groups/retired21_recheck.json` (agent N1.2-ret12, 8 paced requests logged in `analysis/request_log/N1.2-ret12.jsonl`). The 10-15 verdicts came from investigator checks on 10-14.

| Dealer | VIN | Stock | 10-15 verdict | Listed on 10-16 | Final verdict |
|---|---|---|---|---|---|
| coastlinecdjr-com | 1C6SRFU93VN551440 | | correct (phantom from the dead feed) | no | correct |
| coastlinecdjr-com | 3C63R5CL1TG167870 | 260044 | correct (phantom) | no (active at Pedder CDJR, Algolia dealer 1770) | correct |
| coastlinecdjr-com | 3C6MRVSG1VE552787 | | correct (phantom) | no | correct |
| napletonhondamortongrove-com | 5FNYG1H91TB070314 | | correct (gone) | no (`total 0` under any status) | correct |
| southcountylexus-com | 2T2ADCAZXRC014932 | RC014932 | correct (gone) | no (VDP HTTP 410) | correct |
| terrylabontechevy-com | 1G1ZD5ST5SF134253 | P9559 | correct (gone) | yes: relisted in Terry's own feed 114069 since | correct when retired; relisted later |
| terrylabontechevy-com | 1G1ZD5ST7RF182654 | P9568 | correct (gone) | no | correct |
| terrylabontechevy-com | 1GB4KSEY2SF287495 | DCT9763A | correct (gone) | no | correct |
| terrylabontechevy-com | 1GNERLKS5TJ151868 | X9588 | correct (gone) | no | correct |
| terrylabontechevy-com | 3GCPDCEK5RG371145 | P9554 | correct (gone) | no: moved to a sister Hendrick store in Franklin TN (new stock XH5713) | correct |
| terrylabontechevy-com | 1C4RJFAG5FC117065 | P9421A | wrong | yes | wrong |
| terrylabontechevy-com | 1FTEW1CP3JKF33226 | T9631A | wrong | yes | wrong |
| terrylabontechevy-com | 1GD07RF72N1260272 | CS8690A | wrong | yes | wrong |
| terrylabontechevy-com | 1GNSKDKD4PR387630 | PS9510 | wrong | yes | wrong |
| terrylabontechevy-com | 1GNSKNKD5NR346913 | T9808A | wrong | yes | wrong |
| terrylabontechevy-com | 1GNSKPKD0NR286356 | T9609B | wrong | yes | wrong |
| terrylabontechevy-com | 2C4RC1BG9SR531611 | P9486 | wrong | yes | wrong |
| terrylabontechevy-com | 3KPF44AC8PE681945 | CS8289C | wrong | yes | wrong |
| terrylabontechevy-com | 5TFSZ5AN9LX223882 | DT9947A | wrong | yes | wrong |
| terrylabontechevy-com | KL79MPSL0TB074150 | PS9497 | wrong | yes | wrong |
| terrylabontechevy-com | KL79MRSL5SB036942 | P9567 | wrong | yes | wrong |

The 11 wrong ones are `status publish` in Hendrick's group pool under `source_id RHendrickUsed`, with `dealer.location` = Terry's roster street (1401 Bridford Pkwy, Greensboro NC 27407) and Terry's own stock numbers. On 10-16, 12 of the 21 were listed (the 11 wrong ones plus P9559) and 9 were not.

**Coastline's dead account.** CarsCommerce listing 6061839 still answers HTTP 200 with 318 cars, the newest `date_in_stock` 2026-09-30, but the live site runs Motive Algolia dealer 2927 (252 cars on 10-16). All 3 retired VINs are rows of 6061839 `source_id 27175`, and none is in the 2927 pool (`retired21_recheck.json` `coastline_account`). The first pass should never have written them; they were visible for about 23.4 h.

---

## 4. VIN-owner conflicts

### 4.1 Headline counts

| Quantity | Value | Reference |
|---|---|---|
| Rows / distinct VINs | 9,638 / 7,393 | [Q2]; [T] `conflicts` |
| Owners / claimants / (owner, claimant) pairs | 60 / 65 / 92 | [Q2]; [T] |
| `seen_at` range (latest sighting per row) | 2026-10-13T14:05:35Z to 2026-10-14T13:33:21Z | [Q2] |
| VINs with 1 / 2 / 3 / 4 claims | 5,416 / 1,715 / 256 / 6 | [Q3] |
| Conflict rows whose owner row is active | 9,638 of 9,638 | [Q7] |
| Pairs with exactly 1 row | 38 | [S3] |
| Connected components of the pair graph / dealers in them | 35 / 109 | [T] `conflicts.groups_ge_min_rows` |
| Groups with ≥ 20 rows / their dealers / their rows | 19 / 70 / 9,615 | [T]; [S3]; `workspace/phase_runs/N1/conflict_groups.json` |
| Rows outside those 19 groups | 23, each a 1-row pair (cross-listings or dealer trades, not classified) | [S3] |
| Rows seen before the pause (seen_at < 15:34Z) / claimants | 7,842 / 59 | [Q8] |
| Rows seen after the resume / claimants | 1,796 / 9 | [Q8] |

### 4.2 A conflict is the same car on two group sites

- **Same stock number** on 9,574 of 9,638 rows; **same price** on 6,464, different on 3,174 [S2]. Rule: the owner's vehicle record in the 10-13/14 scan JSONL vs the claimant's latest record. 1,046 rows have two claimant records (first pass and retry); using the claimant's first record instead gives 9,578 same stock and 6,540 same price.
- **The stored `source_url` names the site that served the car, not the store.** By host of the owner row's `source_url`: owner's own host 7,998 rows, claimant's host 1,278 (all mbbeverlyhills-com rows of mbontario-com cars), another host 340, none 22 [Q18]. All 4,201 conflict rows (2,706 VINs) owned by cleveland-nissan-com, bmwofmonrovia-net, fordofmurfreesboro-com and bentleycadillac-com carry the owner's own host [Q19], even where the car belongs to a sister store.
- **The old last-writer database mostly had the opposite owner.** For the same 9,638 rows, the 09-28 local `cars` held the VIN under: the claimant, active 3,941; the owner, active 1,344; another dealer, active 931; another dealer, retired 289; the claimant, retired 59; the owner, retired 26; not present 3,048 [S5].

### 4.3 Platforms

Platform = the dealer's latest `scan_runs.provider` [Q16], [Q17]. CarsCommerce recipes run under the label `dealer_dot_com`.

| Side | Rows by platform |
|---|---|
| Owner | dealer_on_cosmos 3,968; dealer_dot_com 2,400; unknown 1,082; dealer_eprocess 647; motive_ridemotive 624; chapman 446; oneaudi 231; jazel 130; typesense 108; team_velocity 2 |
| Claimant | dealer_dot_com 3,572; dealer_eprocess 2,050; dealer_on_cosmos 1,928; unknown 874; motive_ridemotive 626; team_velocity 407; jazel 131; typesense 47; chapman 2; wp_vehicles_index 1 |

`unknown` is six dealer.com `ws-inv-data` recipes whose `provider_hint` is `unknown`: bmwofmonrovia-net, camelbacksubaru-com, camelbacktoyota-com, crownlexus-com, showcasehonda-com, toyotacarlsbad-com [Q21].

**The gate was silent for most conflict dealers.** 48 of the 60 owners (8,844 rows, 91.8%) and 44 of the 65 claimants (8,828 rows) logged no `rooftop attribution [<dealer>]` line in any shard log [S4].

### 4.4 Who wrote first

- `backend/scripts/fleet_scan.py:139-149` (`make_shards`) places dealers longest-first by weight. In the empty DB nearly every dealer weighed the default 50, so placement was effectively alphabetical round-robin over 8 shards (conflicts RCA §4).
- Each dealer writes only at the end of its run: `phases/dealer_run.py:111-119` runs the sister filter, the VDP prefetch (`:112`) and the rest before `_persist` (`:119`). The first sister to finish capture and prefetch owns the VIN.
- Example: mbbeverlyhills-com fetched listing 2309549 at 11:12:15 EDT, 1,499 VINs (`s7/scanner.log:1069`); mbontario-com's guard line at 11:27:08 EDT refused 1,496 VINs, 1,278 of them owned by mbbeverlyhills-com (`s0/scanner.log:1347`).

### 4.5 Classes, group by group

**Classes.** (a) correct refusal: the owner is the true store. (b) wrong winner: the claimant is the true store. (c) the same store under two dealer ids. (d) a third rooftop, split into roster and off-roster. Pool: no per-car evidence anywhere. Unknown: no evidence for this row.

**Method (N1.2, 2026-10-16 to 10-18).** One agent per group with ≥ 20 rows took the true store per row from a per-car store field in the dealer's own feed, fetched live under D-N25 pacing (≤ 3 pages per dealer, ≥ 2 s apart per host, home IP, no browser). Where the car had left the feed by 10-16, it used scan-time evidence: the photo account in the stored image path, the stock-number series, or the scan-time gate stamp. Each class carries a confidence (verified / likely / cannot_assess) inside its dossier. An adversarial checker re-derived each dossier from the saved payloads and read-only SQL; the verdict is in the dossier's `checker` key. Platform field maps for 8 platform families sit in `analysis/platform_evidence/<platform>/field_map.json`.

**Re-check for this document.** Group row, VIN and pair totals were recomputed from `vin_owner_conflicts` and match every dossier [S3]. The sum of classes equals the row total in every dossier. Per-row classes come from the dossiers and checkers; they rest on saved live payloads and were not re-derived here.

| Group | Dealers (owner side first) | Rows / VINs | Store field that names the true store | (a) | (b) | (c) | (d) roster | (d) off-roster | Pool | Unknown | Checker |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1a | mbbeverlyhills, mbontario, audiofcostamesa, fjmercedes, bmwsantamonica, mbofstevenscreek, mbsd (Fletcher Jones, CarsCommerce listing 2309549) | 1,714 / 1,497 | `extra_fields.location_name` + `dealer.address`; `source_id`/`feed_ccid` 1:1 in this group | 326 | 894 | 0 | 326 | 139 | 0 | 29 | confirmed |
| 1b | bmwofmonrovia, crownlexus, wisimonson, bmwofbeverlyhills, longbeachbmw, cadillaclasvegaswest, plus 1-row edges with bmwnorthscottsdale, chapmanbmwchandler, crevierbmw, bmwcarlsbad, bmwofsandiego (Sonic dealer.com pool) | 1,176 / 844 * | dealer.com `inventory[].accountId` → `accounts{}`; photo account; Chapman `uniqueArkona` | 156 | 357 | 0 | 288 * | 317 | 0 | 58 | revise * |
| 2 | fordofmurfreesboro, nissan/hyundai of Cookeville, parksidekia, fordlincolnofcookeville, chevroletbuickgmcofmurfreesboro, tedrussellford, eddrogersvalleyford, jackyjonessweetwater (AMSI Tennessee) | 1,881 / 893 | CarsCommerce `dealer.location` (+ extra_fields), dealer.com `accountId`, cosmos `DealerLocatedAtId`, DEP `location_id`, photo account | 247 | 78 | 0 | 129 | 1,115 † | 312 | 0 | revise † |
| 3 | bentleycadillac, bentleyhyundai, bentleygmc | 972 / 509 | cosmos `DealerLocatedAtId` | 0 | 388 | 0 | 352 | 222 | 0 | 10 | confirmed |
| 4 | cleveland-nissan, mtnviewnissan | 867 / 867 | cosmos `DealerLocatedAtId`; stock suffix CH/CL/DA/BR | 294 | 317 | 0 | 0 | 256 | 0 | 0 | confirmed |
| 5 | lexusescondido, lexuscarlsbad, southcountylexus | 626 / 625 | Motive Algolia `dealer_id` + `dealership` | 290 | 274 | 0 | 1 | 0 | 0 | 61 | confirmed |
| 6 | landersmclartychevrolet, landersmclartyfordfortpayne, landersmclartytoyota | 448 / 344 | cosmos `DealerLocatedAtId`; CarsCommerce brand token; Team Velocity `dealerName`; photo account | 158 | 183 | 0 | 106 | 0 | 0 | 1 | confirmed |
| 7 | chapmanfordaz, chapmandodge | 444 / 444 | Chapman `uniqueArkona`; photo lot code | 149 | 175 | 0 | 0 | 120 | 0 | 0 | confirmed |
| 8 | cumberlandchryslercenter, overdrive-usa, cumberlandkia, cumberlandtoyota | 261 / 162 | typesense `dealerName`; DEP `location_id` | 168 | 47 | 0 | 46 | 0 | 0 | 0 | confirmed |
| 9 | landersmclartynissanhuntsville, mbhsv | 234 / 234 | cosmos photo account + `ImageProvider`; MB stock series | 150 | 84 | 0 | 0 | 0 | 0 | 0 | confirmed |
| 10 | audihuntsville, hileyvwhuntsville, hileymazdahuntsville, raypearman | 231 / 190 | Team Velocity `dealerName` (Mazda vs non-Mazda only) | 0 | 190 ‡ | 0 | 40 | 0 | 0 | 1 | confirmed ‡ |
| 11 | davekirk, easttennesseedodge, easttennesseeford | 157 / 157 | CarsCommerce `extra_fields.meta_location`; `dealer.location`; `feed_ccid` | 94 | 63 | 0 | 0 | 0 | 0 | 0 | confirmed |
| 12 | 5starford, fivestarforddallas | 130 / 130 | Jazel `accountId` / `accountName` / `accountAddress` | 9 | 31 | 0 | 0 | 90 | 0 | 0 | confirmed |
| 13 | fletcherjones, mbofhenderson | 106 / 106 | CarsCommerce `dealer.location`, `location_name`, `dealer.address`, `source_id` | 0 | 106 | 0 | 0 | 0 | 0 | 0 | confirmed |
| 14 | landersmclartysubaru, landersmclartydcjal | 90 / 90 | cosmos `DealerLocatedAtId`; CarsCommerce `dealer.location` / `feed_ccid` | 0 | 90 | 0 | 0 | 0 | 0 | 0 | confirmed |
| 15 | harperacura, harpervw | 69 / 69 | DEP `location_id`; Team Velocity `dealerName` | 0 | 69 | 0 | 0 | 0 | 0 | 0 | confirmed |
| 16 | covertfordaustin, covertbuickgmc | 68 / 68 | cosmos `DealerLocatedAtId`; CarsCommerce `location_sm` + `source_id` | 0 | 68 | 0 | 0 | 0 | 0 | 0 | confirmed |
| 17 | drivelenoircity, lexusofhuntsville | 62 / 62 | cosmos `DealerLocatedAtId`; CarsCommerce `location_filter` | 62 | 0 | 0 | 0 | 0 | 0 | 0 | confirmed |
| 18 | gunnhonda, gunnnissan, ingramparknissan | 58 / 58 | cosmos `DealerLocatedAtId`; CarsCommerce `source_id` / `feed_ccid` | 55 | 3 § | 0 | 0 | 0 | 0 | 0 | confirmed |
| 19 | basswoodcdjr, cdjrofalbertville | 21 / 21 | CarsCommerce `feed_ccid` + `used_location_oct2026` | 21 | 0 | 0 | 0 | 0 | 0 | 0 | revise ¶ |
| **Total** | 70 dealers (group 1 has 18; 1a and 1b share wisimonson-net's 1-row edge) | **9,615** | | **2,179** | **3,417** | **0** | **1,288** | **2,259** | **312** | **160** | |

Notes on the table:
- `*` Group 1b's dossier lists 1,177 rows, 845 VINs and (d) roster 289. The checker found that row `4JGFD6BB8TB541527` (mbbeverlyhills-com ← wisimonson-net) is also counted in 1a, as unknown. Counted once (as 1a's unknown), 1b is 1,176 rows and (d) roster 288. Group 1 as a whole is 2,890 rows and 2,341 VINs [S3].
- `†` Group 2's off-roster 1,115 = 717 at 11 named Tennessee rooftops + 398 in the "AMSI Kentucky Market" pool (cosmos lot id 17861, photo account don:7775). That pool label names no store. The checker asked for the Kentucky pool and "Louisville KY (unnamed)" to move out of the missing-rooftop list into a held unnamed-pool list, and for Ted Russell Ford Parkside Drive to be marked conditional (Section 7.3). The class counts do not change.
- `‡` Group 10's 190 (b) include 59 rows on A/P/9-prefixed stock that the feed cannot assign to Audi Huntsville or Hiley VW. They are (b) only under D-N3 rule 2 with `hileyvwhuntsville-com` as the designated primary; if A/P prefixes go to Audi instead, (a) 59 / (b) 131. The checker also noted the Ray Pearman VIN `1C4RJXFGXSW574607` is "unknown" in the dossier but "d" in its per-VIN file; the totals use "unknown".
- `§` Group 18's 3 (b) rows are a dealer trade between groups (Gunn Nissan → Ingram Park Nissan). Both feeds stamp the car strongly; the tie-break ("later in-stock date wins") is still for the owner to decide.
- `¶` Group 19's classes stand. The checker found that "CDJR Of Fort Payne" is the roster dealer basswoodcdjr-com under a new name and site, so it is a roster repair, not a missing rooftop (Section 7.3).

**Compared with the 10-15 estimate** (rule-based over all 9,638 rows, conflicts RCA §3): (a) 1,874 → 2,179; (b) 3,334 → 3,417; (c) 0 → 0; (d) 1,422 → 3,547 (1,288 roster + 2,259 off-roster); pool 1,995 → 312; unknown 1,013 → 160 (plus 23 single-row pairs outside the groups, not classified). The 10-15 per-VIN figures ("about 4,060 of 7,393 VINs stored under the wrong dealer") were not recomputed: the dossiers do not share one per-VIN format. Treat them as superseded (cannot_assess).

**Largest pair corrected.** The 10-15 report called all 1,278 rows of mbbeverlyhills-com ← mbontario-com class (b). Group 1a found that listing 2309549 carries six stores: (b) 817, (a) 45, (d) roster 251, (d) off-roster 139, unknown 26 [G1a].

**How D-N3 decides each group.** Rule (1), the strong stamped tier, decides every car in groups 1a, 1b, 3, 4, 5, 7, 8, 11, 12, 13, 14, 15, 16, 17, 18 and 19. The exceptions:
- Group 9: both sites stamp every pool car as their own. The dossier proposes a `group_pools.json` entry (`mirror_campus_pool`) with per-member source keys (photo account, stock series, franchise CPO) and `landersmclartynissanhuntsville-com` as the designated primary.
- Group 10: the non-Mazda used pool goes to the designated primary `hileyvwhuntsville-com` (D-N3 rule 2).
- Group 6: about 10 untagged used-pool rows go to the designated primary `landersmclartychevrolet-com`.
- Group 2: the 312 rows (184 VINs) without a store field are held (designated primary: none).

`backend/config/group_pools.json` does not exist yet (checked 2026-10-18); N8.6 creates it.

---

## 5. The six root causes

Numbered as in the 10-15 conflicts RCA (the plan cites them by these numbers). Sizes are from Section 4 unless stated; RC1 and RC2 are by far the largest.

### RC1. Parsers drop the per-car store field, so the gate keeps every row

- **What:** every one of the 19 groups carries a per-car store field (Section 4.5). Only 4 parsers stamp `_rooftop`: `backend/parsers/team_velocity.py:246`, `dealer_dot_com.py:781`, `typesense.py:347`, `carscommerce.py:380` (grep of `["_rooftop"] =`).
- **Parsers that ignore the field:**
  - DealerOn cosmos `backend/scanner/scrapers/dealer_on.py:196-252` (`_map_vehicle_card`): drops `DealerLocatedAtId`, `VehicleLocation`, `DealerLocatedAtCity/State`. The cosmos map found the field on 425 of 425 sampled cards and 98 distinct lot ids (`platform_evidence/cosmos/field_map.json`).
  - Chapman `backend/parsers/chapman.py:175` (`_map`): drops `uniqueArkona` / `location`.
  - Jazel `backend/parsers/jazel.py:183`: uses the manifest dealer name before the car's `accountName`.
  - Motive `backend/parsers/motive_ridemotive.py:135-172` (`_map`): ignores the hit's `dealer_id` / `dealership`; the recipe filter `dealer_ids:"<id>"` (`backend/scanner/synth/platforms/motive.py:64`) returns every car shared to a store.
  - DealerEProcess `backend/parsers/dealer_eprocess.py:170` (`_map`): never reads `filter_vehicle_data[<id>].location_id` (see RC4).
  - dealer.com payloads tagged `unknown` go to `parse_generic_json` (`backend/parsers/__init__.py:177-193`), which drops `accountId` (6 dealers [Q21]).
  - CarsCommerce `rooftop_of` reads only `dealer.location` / `dealer.name` (`backend/parsers/carscommerce.py:301-303`). It misses the store labels in `extra_fields` (`location_name`, `meta_location`, `location_sm`, `location_filter`, `used_location_oct2026`). This is why the gate was silent on listing 2309549 and on East Tennessee Dodge's account 150600 (CF-11, answered in `platform_evidence/carscommerce/field_map.json` `cf_11`).
  - OneAudi `backend/parsers/oneaudi.py:92-138`: its only dealer field (`stockCar.dealer`) is the publishing store on every car, so it must **not** be stamped as a rooftop (`platform_evidence/oneaudi_omnigraph/field_map.json`).
- **The gate keeps everything when no row names a store:** `backend/attribution/match.py:339-343` returns keep / `no_rooftop_evidence` for every row, and the legacy ladder does the same at `backend/attribution/rooftop.py:804-805`.
- **Size:** owner-side conflict rows on the non-stamping platforms: cosmos 3,968 + unknown 1,082 + DEP 647 + Motive 624 + Chapman 446 + OneAudi 231 + Jazel 130 = 7,128 [Q16]. 48 of 60 owners (8,844 rows) logged no gate line [S4].
- **Fix:** N8.1 (cosmos), N8.2 (ws-inv-data routing), N8.3 (Jazel, Chapman), N8.4 (Motive), N8.8 (CarsCommerce label chain), N8.11 (DEP).

### RC2. The VIN-owner guard is first-writer-wins with no evidence tie-break

- **What:** `backend/scanner/database.py:83-101` (`_vin_owned_elsewhere`) refuses a write when the stored row is active, scraped within the window and owned by another dealer. The window defaults to 48 h (`database.py:53`). Prefetch: `backend/scanner/upsert/guard.py:36-67`. SQL backstop for concurrent shards: `backend/scanner/upsert/sql.py:150-155`. Refused VINs are dropped before reconcile at `phases/dealer_run_steps/persist.py:25-39`.
- **Effect:** no evidence is compared, and the rooftop tier is never stored. Write order decides ownership (Section 4.4). On a populated DB the owner re-scans nightly, inside 48 h, so a wrong owner is never displaced. Raising the window to 72 h (P6A.3) would make this stickier.
- **Size:** turns RC1 into 3,417 wrong winners in the 19 groups (Section 4.5).
- **Fix:** N8.6 (persisted tier V029, evidence-aware guard and backstop, `group_pools.json`, moves logged), D-N3.

### RC3. A cross-wired recipe: mbbeverlyhills-com scans mbontario-com's listing

- **What:** both dealers' only recipe is a POST to CarsCommerce listing 2309549 with the same URL, method, `post_template`, auth headers, content type and pagination. Only `dealer_id`, timestamps and coverage differ [Q22]. The listing is the Fletcher Jones group pool (six roster stores plus three off-roster) [G1a].
- **Size:** 1,284 active mbbeverlyhills-com rows, all on host `www.mbontario.com`; 1,278 of them are conflict rows with claimant mbontario-com; mbontario-com keeps 11 active rows [T] `cf7_class_b.mbbeverlyhills_mbontario_host`, `mbontario_active`.
- **Fix:** N8.9 points mbbeverlyhills-com at its own listing 6074385 after a replay check (`www.mbbeverlyhills.com` VDP host on 200 of 200 rows [G1a]); N8.5 adds a recipe-identity check (same request across dealers, VDP host ≠ dealer host).

### RC4. DealerEProcess pools are unparsed (formerly "pools with no evidence")

- **What the 10-15 report said:** about 1,995 rows sat in pools with no per-car evidence (Cookeville DEP sites, Cumberland, AMSI Tennessee Market).
- **What the dossiers found:** each of those pools carries a store field. DEP pages carry `setPageData.filter_vehicle_data[<id>].location_id` on 716 of 716 sampled cards, and the site's own ids are in `site_locations` (`platform_evidence/dep_typesense_pools/field_map.json`). Cumberland's typesense feed stamps `dealerName` on 392 of 392 docs [G8]. Cosmos labels the AMSI pools with lot ids 17864 / 17861, and CarsCommerce names the store [G2]. The DEP parser reads none of it (`dealer_eprocess.py:170`).
- **Size:** 647 owner-side and 2,050 claimant-side DEP conflict rows [Q16], [Q17]; 312 rows (group 2, 184 VINs) still have no store field in any 10-16 payload and are most likely sold or moved since 10-13.
- **Fix:** N8.11 (the condition "a per-car store or lot field exists" is met: stamp DEP rows from `location_id`); D-N3 rule (2)/(3) for the residue.

### RC5. A weak name match keeps a portal's sister cars

- **What:** fletcherjones-com is a group portal whose roster row has no street. The gate kept the Henderson rooftop on `name_token_subset` (`backend/attribution/rooftop.py:476`). Weak tiers (`_WEAK_NAME_TIERS`, `rooftop.py:88`) only limit un-listing, not keeping.
- **Size:** 106 class (b) rows to mbofhenderson-com [G13]. 122 of fletcherjones-com's 131 active rows name "Mercedes-Benz of Henderson" in the description [T] `cf7_class_b.fletcherjones_henderson`; the dossier places all 131 at Henderson (106 conflict + 25 without a conflict row).
- **Fix:** N8.5 (no KEEP on a weak tier when the stamped rooftop carries another roster dealer's street or exact name); N8.9 (fletcherjones-com roster question, D-N6).

### RC6. Rooftops missing from the roster land under a sister with no conflict row

- **What:** the scan roster keeps dealers that already have active inventory, plus revived recipe-only dealers (`backend/scanner/manifest.py:131` `_load_scannable_dealers`, `:269` `apply_scannable_roster_rule`). A rooftop that was never scanned cannot get rostered. Its cars arrive in a sister's group feed, and since no second roster dealer claims them, the guard records nothing.
- **Examples that never show in `vin_owner_conflicts`** (each verified by SQL on stored lot codes or photo accounts, plus the dossier):
  - 90 Mtn View Nissan of Bristol cars (stock suffix BR) under cleveland-nissan-com [T] `cf7_class_b.cleveland_nissan_ch_da_br.by_suffix.BR`; [G4].
  - 651 Chapman BMW on Camelback cars (photo lot CAC) under chapmanbmwchandler-com [Q23]; [G1b].
  - 246 Chapman Ford Pro Elite cars (photo lot CA5) under chapmanfordaz-com and 287 Chapman CDJR Yuma cars under chapmandodge-com (154 by photo lot CYD [Q23], the rest by live store code) [G7].
  - 996 cars of 8 off-roster Covert rooftops under covertfordaustin-com [G16].
  - 583 cars of 10 off-roster Five Star lots under lexusofhuntsville-com [G17].
  - 46 Subaru of Gallatin cars under landersmclartysubaru-net [G14].
- **Size:** 2,259 conflict rows are class (d) off-roster (Section 4.5), plus the hidden rows above. Section 7 counts rows per rooftop.
- **Fix:** D-N4 as changed by the owner: N1.11 (profiles, batch 1), N8.13 (detector and queue), N8.9 (roster entries).

---

## 6. Pause artifacts (labelled; not defects of the scan logic)

The fleet was paused with SIGSTOP. The last pre-pause log line is 2026-10-13 15:34:38Z (`s1/scanner.log`, local 11:34:38 EDT); the first post-resume line is 2026-10-14 13:16:24Z on 7 shards and 13:24:37Z on s4 (logs are in EDT, UTC−4). The pause lasted about 21.7 h.

| Artifact | Detail | Reference |
|---|---|---|
| All 8 in-flight batches killed | `rc=-9` after 78,209 to 78,778 s, because the 3,600 s batch timeout (`--scan-timeout`, `backend/scripts/dealer_pipeline.py:149`) is wall-clock and counted the pause; `subprocess.run(timeout=…)` returns −9 on timeout (`backend/scanner/pipeline/runner.py:119-122`) | `s0.out:67`, `s1.out:67`, `s2.out:66`, `s3.out:68`, `s4.out:68`, `s5.out:68`, `s6.out:67`, `s7.out:67` |
| 21 dealers with verdict `error`, reason `no scan_runs row` | 19 with 0 rows; righthonda-com (553) and selmanchevy-com (281) wrote rows with no scan_runs row. In the 09-28 local `cars` these 21 dealers hold 12,977 active rows | [S1]; [Q13]; [Q14] |
| Lifecycle retries delayed | every retry ran about 21.7 h after its first pass (Section 3.2 times) | [Q9] |
| A dealer cut from group 1 | toyotacarson-com is one of the 21; group 1a needs it in the re-run | [G1a] |
| Retirements caused by the pause | 0. The killed batches never reached reconcile, and the 21 retirements were made by retries that only ran after the resume | Section 3 |
| Conflicts caused by the pause | 0. The 1,796 post-resume conflict rows refused owner rows at most 23.2 h old, inside the 48 h window. This held only because the pause was shorter than the window | [Q15] |

---

## 7. D-N4 and the missing rooftops

### 7.1 The owner's decision (2026-10-16)

From `docs/remediation/OWNER_DECISIONS_LOG.md`, row D-N4, "Changed by owner: profile and scan them": "we should build a profile for these and scan them when we see cases like this." Each missing rooftop becomes its own dealer: the full `docs/NETWORK_SCAN_PROCESS.md` loop (discovery, location verify, recipe, test scan, vPIC verify, five dealer_logs files) and a roster entry. Standing rule: whenever a scan sees cars from a rooftop not on the roster, that rooftop is queued for profiling (N8.13). **Until its profile exists, its cars are held** (refused at the gate with the rooftop recorded), never filed under a sister store. First batch: N1.11.

D-N3 (same date, as recommended): (1) a store whose own feed stamps the car at a strong tier owns it; (2) otherwise the designated primary in `backend/config/group_pools.json`, `dealer_feed_scope='group'`, and the site shows "listed by N group stores; location unconfirmed"; (3) pairs not in the file keep their owner and are flagged.

### 7.2 Off-roster rooftops the dossiers found

"Rows" = `cars_cleanroom` rows of that rooftop filed under a roster dealer today, as counted by the dossier (conflict and non-conflict rows). These are the expected move counts for N1.11. The 3 Fletcher Jones California stores appear in both [G1a] and [G13]; the table uses [G1a]'s live-payload counts. [G13]'s description-text counts (Audi Beverly Hills 70+2, Audi Long Beach 39+1, Porsche Long Beach 40, [Q25]) are an upper bound.

**Confirmed (60 rooftops, 4,446 rows; 2 of them are existing entities to repair):**

| Group | Rooftop | Address (as found) | Payload field and value | Filed under | Rows |
|---|---|---|---|---|---|
| 1a | Audi Beverly Hills | 8850 Wilshire Blvd, Beverly Hills CA 90211 | CarsCommerce `source_id` FJAudiBeverlyHills, `location_name` 'Audi Beverly Hills', feed_ccid 209129 | mbbeverlyhills-com 70, mbontario-com 2 | 72 |
| 1a | Porsche Long Beach | 1919 N Lakewood Blvd, Long Beach CA 90815 (portal says 1850 Outer Traffic Cir) | `source_id` FJPorscheLongBeach, feed_ccid 6060859 | mbbeverlyhills-com | 36 |
| 1a | Audi Long Beach | 1919 N Lakewood Blvd, Long Beach CA 90815 (portal says 2998 Cherry Ave, Signal Hill) | `source_id` FJAudiLongBeach, feed_ccid 6060860 | mbbeverlyhills-com 33, mbontario-com 1 | 34 |
| 1b | Mercedes-Benz of Calabasas | 24181 Calabasas Road, Calabasas CA 91302 | dealer.com accountId `sonicbenzcalabasas` | bmwofmonrovia-net 91, crownlexus-com 19 | 110 ** |
| 1b | Honda West | 7615 West Sahara Ave, Las Vegas NV 89117 | accountId `sonichondawest` | bmwofmonrovia-net 51, crownlexus-com 7 | 58 |
| 1b | Buena Park Honda | 6411 Beach Blvd, Buena Park CA 90621 | accountId `sonicbuenaparkhonda` | bmwofmonrovia-net | 49 |
| 1b | Carson Honda | 1435 E 223rd St, Carson CA 90745 | accountId `soniccarsonhonda` | bmwofmonrovia-net | 45 |
| 1b | Poway Honda | 13747 Poway Road, Poway CA 92064 | accountId `sonicpowayhonda` | bmwofmonrovia-net | 35 |
| 1b | MINI of Monrovia | 1451 S Mountain Ave, Monrovia CA 91016 | accountId `miniofmonrov1451southmountainavenue` | bmwofmonrovia-net | 4 |
| 1b | Chapman BMW on Camelback | 830 E Camelback Rd, Phoenix AZ 85014 | Chapman `uniqueArkona` CAC | chapmanbmwchandler-com | 651 |
| 2 | Toyota of Knoxville | cannot_assess | CarsCommerce 196712 `dealer.location`, source MP5755, feed_ccid 12933; DEP 13180 | parksidekia 44, hyundaiofcookeville 27, fordofmurfreesboro 20 | 91 |
| 2 | Downtown Nashville Nissan | cannot_assess | `dealer.location`, MP4106, feed_ccid 2269378; DEP 13150 | fordofmurfreesboro 47, parksidekia 1 | 48 |
| 2 | Toyota of Cool Springs | cannot_assess | `dealer.location`, feed_ccid 205468; DEP 13179 | parksidekia 28, hyundaiofcookeville 11 | 39 |
| 2 | Chrysler Dodge Jeep Ram of Franklin | cannot_assess | `dealer.location`, MP5912, feed_ccid 6061739; DEP 13149 | fordofmurfreesboro 34, parksidekia 1 | 35 |
| 2 | Toyota of Murfreesboro | 3434 Bill Smith Dr, Murfreesboro TN 37129 (discovery file; unverified) | `dealer.location`, MP5926, feed_ccid 12682; DEP 13182 | fordofmurfreesboro | 33 |
| 2 | Hyundai of Cool Springs | cannot_assess | `dealer.location`, MP15431, feed_ccid 6001210 | fordofmurfreesboro | 33 |
| 2 | Ford Lincoln of Franklin | cannot_assess | `dealer.location` 'Ford Lincoln of Frankin' (sic), MP6763; DEP 13152 | fordofmurfreesboro | 26 |
| 2 | Nissan of Cool Springs | cannot_assess | `dealer.location`, MP12837, feed_ccid 176850 | fordofmurfreesboro | 20 |
| 2 | Infiniti of Cool Springs | cannot_assess | `dealer.location`, MP7754; DEP 13161 | fordofmurfreesboro | 18 |
| 2 | Lance Cunningham Ford | Knoxville TN (street unverified) | `dealer.location`, MP3893, feed_ccid 150629; DEP 13164 | parksidekia 3, hyundaiofcookeville 2 | 5 |
| 3 | Bentley Chevrolet of Florence | 1950 Florence Blvd, Florence AL 35630 | cosmos `DealerLocatedAtId` 27896; photo don:19779 | bentleycadillac-com | 111 |
| 4 | Mtn View Nissan of Dalton | 1706 E Walnut Ave, Dalton GA 30721 | cosmos 25123; stock suffix DA; photo don:16068 | cleveland-nissan-com 256, mtnviewnissan-com 8 | 264 |
| 4 | Mtn View Nissan of Bristol | 2536 W State St, Bristol TN 37620 | cosmos 32790; stock suffix BR; photo don:24562 | cleveland-nissan-com | 90 |
| 7 | Chapman Dodge Chrysler Jeep Ram Yuma | 349 E 32nd St, Yuma AZ 85364 | Chapman `uniqueArkona` CYD | chapmandodge-com | 287 |
| 7 | Chapman Ford Pro Elite | 3400 N 89th St, Scottsdale AZ 85251 | `uniqueArkona` CA5 | chapmanfordaz-com | 246 †† |
| 7 | Chapman Hyundai Scottsdale | 3600 N 89th St, Scottsdale AZ 85251 | `uniqueArkona` CAY | chapmanfordaz-com | 62 |
| 7 | Chapman Volkswagen Scottsdale | 3700 N 89th St, Scottsdale AZ 85251 | `uniqueArkona` CAV | chapmanfordaz-com | 58 |
| 11 | Victory Toyota Dyersburg | Dyersburg TN (street cannot_assess) | CarsCommerce `dealer.location`, feed_ccid 6061854, source MP88828 | none (refused at davekirk-com's gate) | 0 |
| 11 | Victory Nissan of Dickson | Dickson TN (street cannot_assess) | `dealer.location`, feed_ccid 193930, MP4330 | none | 0 |
| 11 | Victory Honda of Jackson | Jackson TN (street cannot_assess) | `dealer.location`, feed_ccid 13025, MP4328 | none | 0 |
| 12 | Five Star Ford Carrollton | 1635 S Interstate 35E, Carrollton TX 75006 | Jazel accountId 6 | 5starford-com 26, fivestarforddallas-com 1 | 27 ‡‡ |
| 12 | Five Star Chevrolet | 1735 S Interstate 35E, Carrollton TX 75006 | Jazel accountId 108 | 5starford-com 23, fivestarforddallas-com 1 | 24 ‡‡ |
| 12 | Five Star Ford Lewisville | 1144 N Stemmons Fwy, Lewisville TX 75067 | Jazel accountId 10 | 5starford-com 21, fivestarforddallas-com 2 | 23 |
| 12 | Subaru Grapevine | 2651 William D Tate Ave, Grapevine TX 76051 | Jazel accountId 190 | 5starford-com | 13 |
| 14 | Subaru of Gallatin | 1395 Nashville Pike, Gallatin TN 37066 | cosmos 32824; CarsCommerce feed_ccid 5392820 | landersmclartysubaru-net | 46 |
| 15 | Harper Chrysler Dodge Jeep RAM FIAT | Alcoa TN 37701 (street cannot_assess) | DEP `location_id` 16095; TV `dealerName` | harperacura-net | 83 |
| 15 | Harper INFINITI | Knoxville TN 37922 (street cannot_assess) | DEP 16092; TV `dealerName` | harperacura-net | 44 |
| 15 | Harper Jaguar (also Maserati, Alfa Romeo) | Knoxville TN 37922 (street cannot_assess) | TV `dealerName`; DEP 16089 (by stock family) | harperacura-net | 25 |
| 16 | Covert Chevrolet Hutto | 1200B Hwy 79 E, Hutto TX 78634 | cosmos 29392; CarsCommerce `location_sm` 'Covert Hutto' | covertfordaustin-com | 187 |
| 16 | Covert Chevrolet Bastrop | 702 Highway 71 W, Bastrop TX 78602 | cosmos 28341; `location_sm` 'Covert Bastrop' | covertfordaustin-com | 186 |
| 16 | Covert Buick GMC Bee Cave | cannot_assess (site 403) | cosmos 29390 | covertfordaustin-com | 157 |
| 16 | Covert Cadillac | cannot_assess (site 403) | cosmos 22619 | covertfordaustin-com | 130 |
| 16 | Covert Cadillac Bee Cave | cannot_assess (site 403) | cosmos 23431 | covertfordaustin-com | 101 |
| 16 | Covert CDJR Bee Cave | cannot_assess (site 403) | cosmos 25256 | covertfordaustin-com | 115 |
| 16 | Covert Chrysler Dodge Jeep Ram (Austin) | cannot_assess (site 403) | cosmos 29391 | covertfordaustin-com | 119 |
| 17 | BMW of Florence | 2199 David H McLeod Blvd Suite B, Florence SC 29501 | cosmos 12223; photo don:2157 | lexusofhuntsville-com | 119 |
| 17 | Five Star Chevrolet Cadillac Buick GMC Warner Robins | Warner Robins GA (street cannot_assess) | cosmos 12220 | lexusofhuntsville-com | 91 |
| 17 | Five Star Toyota of Milledgeville | 2816 N Columbia St, Milledgeville GA 31061 | cosmos 12228 | lexusofhuntsville-com | 69 |
| 17 | Five Star Ford Lincoln Warner Robins | Warner Robins GA (street cannot_assess) | cosmos 12206 | lexusofhuntsville-com | 49 |
| 17 | Five Star Nissan of Florence | 311 N Cashua Dr, Florence SC 29501 | cosmos 12233 | lexusofhuntsville-com | 45 |
| 17 | Five Star Nissan (Warner Robins) | 839 Warren Dr, Warner Robins GA 31088 | cosmos 12230 | lexusofhuntsville-com | 40 |
| 17 | Five Star Ford Stone Mountain | Snellville GA (street cannot_assess) | cosmos 12169 | lexusofhuntsville-com | 39 |
| 17 | Milledgeville Chrysler Dodge Jeep Ram Fiat | Milledgeville GA (street cannot_assess) | cosmos 12168 | lexusofhuntsville-com | 32 |
| 17 | Five Star CDJR Fiat Warner Robins | Warner Robins GA (street cannot_assess) | cosmos 12225 | lexusofhuntsville-com | 31 |
| 13 | Fletcher Jones Imports | 7300 W Sahara Ave, Las Vegas NV 89117 | CarsCommerce `source_id` FJMercedesLasVegas / 13427, feed_ccid 24067, `location_name` 'Fletcher Jones Imports' | none (refused by mbofhenderson-com and fletcherjones-com) | 0 §§ |
| 18 | Gunn Acura | 11911 IH 10 West, San Antonio TX 78230 | cosmos 14414; photo don:4573 | gunnnissan-com | 33 |
| 18 | Gunn Buick GMC | Selma TX (street cannot_assess) | cosmos 15398; photo don:5312 | gunnnissan-com | 58 |
| 18 | Gunn Chevrolet | 16550 IH-35 North, Selma TX 78154 | cosmos 28093 | none in `cars_cleanroom` (171 rows under gunnhonda-com in the 09-28 `cars`) | 0 |
| 18 | Ingram Park Chrysler Jeep Dodge | cannot_assess | CarsCommerce `source_id` MP9596, feed_ccid 22562 | none | 0 |
| 18 | Ingram Park Mazda | cannot_assess | `source_id` MP19508, feed_ccid 109573 | none | 0 |

`**` Mercedes-Benz of Calabasas already has a `dealerships` row (id 599) whose only recipe is BMW of Monrovia's pooled endpoint; N1.11 repairs that entity instead of creating a new one [G1b].
`§§` Fletcher Jones Imports already has `dealerships` id 420 and dealer id fjimports-com, which `manifest/roster_rule.json` excluded as recipe-only; N1.11 reuses them [G13].
`††` Whether Chapman Ford Pro Elite is a separate rooftop or Chapman Ford's commercial department is an N1.11 check [G7].
`‡‡` Plus 11 Carrollton rows that are certainly account 6 or 108 but not resolved between them [G12].

**Likely (15 rooftops, 260 rows; N1.11 must verify before profiling):**

| Group | Rooftop | Why only likely | Filed under | Rows |
|---|---|---|---|---|
| 2 | Ted Russell Ford Parkside Drive (9925 Parkside Dr, Knoxville TN 37922) | off-roster only if tedrussellford-net is the Kingston Pike store; its manifest name says "Parkside" (N8.9 identity decision) | tedrussellford-net 54, parksidekia 11, hyundaiofcookeville 6 | 71 |
| 2 | BMW of Louisville | DEP `---Shared` label 13194 only, no CarsCommerce cross-check | fordofmurfreesboro | 15 |
| 2 | Frankfort Ford Lincoln | DEP label 13201 only | hyundaiofcookeville 2, parksidekia 2 | 4 |
| 2 | Volkswagen of Clarksville | DEP label 13210 only | hyundaiofcookeville | 4 |
| 2 | Courtesy Cadillac (city unknown) | DEP label 13200 only | nissanofcookeville (non-conflict) | 3 |
| 13 | FJ Used Car & Truck Center (7100 W Sahara Ave, Las Vegas) | may be FJ Imports' used lot | none | 0 |
| 13 | Fletcher Jones Motorcars Vans Center (375 Bristol St, Costa Mesa) | may be a department of roster dealer fjmercedes-com | none | 0 |
| 15 | Harper Audi store (name not confirmed) | DEP `location_id` 16098 only; name in no payload | harperacura-net | 81 |
| 15 | Harper Porsche | description text plus stock family only (DEP 16088) | harperacura-net | 13 |
| 16 | Covert Ford Hutto (1200A Hwy 79 E, Hutto TX) | rooftop confirmed; its single row is only likely | covertfordaustin-com | 1 |
| 17 | Five Star Albany CDJR / Hyundai (cosmos 15240) | one rooftop or two | lexusofhuntsville-com | 68 |
| 17 | Tuscaloosa Ford | CarsCommerce label only | none | 0 |
| 17 | Lafayette Ford | CarsCommerce label only; not on the Five Star list | none | 0 |
| 17 | Lafayette Buick GMC | CarsCommerce label only | none | 0 |
| 18 | IPAC Pre-Owned Outlet | may be an outlet lot rather than a rooftop | none | 0 |

### 7.3 Corrections to the dossier lists (checker verdicts and cross-checks)

| Entry | Correct treatment | Rows | Reference |
|---|---|---|---|
| "CDJR Of Fort Payne" (group 19) | Not a missing rooftop. It is roster dealer basswoodcdjr-com under a new name and site (same street 1151 Gault Ave S, postal code 35967-4973; basswood's dealer.com account url is www.cdjroffortpayne.com). N8.9 repairs basswoodcdjr-com; off the N1.11 queue. Until the repair, its cars are refused at Albertville as a sibling store | 52 under cdjrofalbertville-com | [G19] checker |
| "AMSI Kentucky Market pool" and "Louisville KY (unnamed)" (group 2) | Not rooftops: pool labels that name no store. Held as an unnamed pool; N1.11 cannot profile them until a store is named. They hold 398 class (d) conflict rows (Section 4.5) | 232 + 9 (dossier `rows_in_cleanroom`; the checker counted 325 + 26 by another rule) | [G2] checker |
| Mercedes-Benz of Calabasas (group 1b) | Confirmed rooftop, listed in 7.2. Repair existing `dealerships` id 599 instead of creating a dealer (the 2026-09-26 "not a store, delete the entity" verdict is refuted) | 110 | [G1b] |
| Fletcher Jones Imports (group 13) | Confirmed rooftop, listed in 7.2. Reuse `dealerships` id 420 and dealer id fjimports-com, which `manifest/roster_rule.json` excluded as recipe-only | 0 | [G13] checker |
| Bentley Supercenter (group 3) | cannot_assess: 1 live card, looks like an intake lot | 0 | [G3] |
| Gunn store behind photo account don:15181 (group 18) | cannot_assess: no live lot id seen | 0 | [G18] |
| North Park Lexus "VTO Inventory", Motive id 2148 (Motive map) | Likely the same store under a second Motive id (class c), not a missing rooftop; off the N1.11 queue pending an owner/N8 decision | 62 cars in the live pool | `platform_evidence/motive/field_map.json` checker |
| Ted Russell Ford Parkside Drive (group 2) | Conditional on the N8.9 identity decision for tedrussellford-net | 71 | [G2] checker |

Group totals for 7.2 and 7.3: 60 confirmed rooftops, 4,446 rows (58 new, 4,336 rows; 2 existing entities to repair, 110 rows); 15 likely, 260 rows; 1 roster repair, 52 rows; 2 unnamed pools, 241 rows; 2 cannot_assess, 0 rows. Sum: 4,999 rows [S6].

### 7.4 Beyond the 19 groups

The platform maps saw more off-roster stores in feeds outside the 19 groups: 39 off-roster cosmos lots in all (`platform_evidence/cosmos/field_map.json` `off_roster_rooftops_for_N1_11`), 14 Motive stores (11 Doggett stores, CarVision of Trooper, Mile High Acura), Camelback Volkswagen on dealer.com, and the 26 + 9 store labels in two CarsCommerce pools (`platform_evidence/carscommerce/field_map.json`). They are leads for N8.13's detector and later N1.11 batches. They were not checked for this document.

---

## 8. Other clean-room figures (acceptance tool)

All values are from `analysis/baseline_r1.json` [T] and were reproduced on 2026-10-18 (Appendix A.3).

| Figure | Value | Key in baseline_r1.json |
|---|---|---|
| Verdicts | ok 240, inaccurate 139, thin 51, error 21, no_rows 7 | `verdicts` |
| Feed-level HTTP 403 / 429 from the home IP (dealers) | 63 / 9 (43) | `http_403_429` |
| Replays stopped at `_MAX_REPLAY_PAGES = 40` (`backend/scanner/recipes.py:1026`), with nothing logged as truncation | 13 walks | `page_cap` (grep `from 40 page(s)` in `s*/scanner.log`) |
| VDP prefetch wanted / fetched | 188,597 / 92,282 (48.9%); 140 dealers blocked (59,523 wanted, 365 fetched; 65 with `statuses={}`) | `vdp` |
| `vpic_fuel` verifier code (false positive on correct PHEVs) | 3,585 rows at 262 dealers; at 261 of them every example is a PHEV | `vpic_fuel` |
| dealer.com dict repr stored as trim / literal `[[exteriorColor]]` | 1,228 rows at 52 dealers / 882 rows at 60 dealers | `parser_junk` |
| Active rows stored AWD/4WD where vPIC decodes 2WD | 5,306 at 142 dealers | `drivetrain_vs_vpic` |
| Prices $10-$999 / above $1M | 234 (24 dealers) / 5 | `junk_prices` |
| Non-17-character VINs / 17-character with I, O or Q | 187 / 1,240 | `vin_quality` |
| Incomplete listings (recomputed under today's rules) | 34,023 of 199,457 (17.06%); 14,345 (7.19%) without the new-car NULL-mileage code | `incomplete` |
| New or blank-condition active cars with NULL mileage flagged `mileage` | 21,152 | `mileage_columns.new_or_blank_condition_null` |
| Complete cars carrying stale index flags | 4,581 | `stale_index_flags.stale_cars` |
| Per-dealer analysis records | 195 of 458 | `ls workspace/cleanroom_2026_10/analysis/dealers/*.json` |

---

## 9. Fix map

Item ids are from the 10-15 gather reports: CF (conflicts RCA), RET (retirements RCA), WI (code map), W (dealer records), IL (incomplete listings). Units are from `docs/NEXT_PHASES_PLAN_2026_10.md` Appendix A. Status is as of 2026-10-18; units not marked are planned.

### 9.1 Attribution and conflicts

| Item | What | Unit(s) | Status |
|---|---|---|---|
| CF-1 | Cosmos mapper stamps `_rooftop` | N8.1 | |
| CF-2 | ws-inv-data payloads go to dealer_dot_com whatever the hint | N8.2 | |
| CF-3 | Repair mbbeverlyhills-com's recipe; recipe-identity check | N8.5, N8.9, P12B.5 | real endpoint found: listing 6074385 [G1a] |
| CF-4 | Evidence-aware VIN-owner guard | N8.6 | |
| CF-5 | Jazel, Chapman, Motive, OneAudi parsers | N8.3, N8.4 | OneAudi: do not stamp (map) |
| CF-6 | Pool policy | D-N3, N8.6 | D-N3 answered 10-16 |
| CF-7 | Clean-room re-run with acceptance metrics | N8.10, N11.3 | baseline in [T] |
| CF-8 | Weak-tier keep at fletcherjones-com | N8.5 | |
| CF-9 | Ownership pre-check before VDP prefetch | N8.7 | |
| CF-10 | Reporting that hides guard refusals; premise | N1.1, N9.12 | premise: this document |
| CF-11 | Silent CarsCommerce gate on 2309549 and East Tennessee | N1.2, N8.8 | answered (RC1): labels sit in `extra_fields`; not a regression |
| CF-12 | Roster and registry fixes; missing rooftops | D-N4, N8.9, N8.6, N1.11, N8.13 | D-N4 changed by owner; list in Section 7 |
| CF-13 | Live attribution checks | N1.2 | done (19 dossiers, 8 maps) |
| CF-14 | Prod ownership reset for known pairs | D-N22, N1.6, N10.7, N11.13, N12.7 | |
| CF-15 | `dealer_feed_scope` role (27 rows, 2 `group` [Q20]) | D-N8, N9.12 | |
| WI-2 | `vdp_host_other_store` refusal | N8.5 | |
| WI-3 | Flag unstamped group feeds in triage | N9.12 | |
| WI-4 | Store identity per platform | N8.1, N8.11 | N8.11 condition met (DEP `location_id`) |
| WI-5 | Owner decision on shared inventory | D-N3 | answered |
| WI-6, WI-12, WI-13 | Ownership by evidence; log guard-allowed moves; durable manual moves | N8.6 | |
| W02 | Ownership rule for shared inventory | D-N3, N8.6 | answered |
| W10 | Targeted analysis per conflict group | N1.2 | done |
| W12 | Recipe hygiene and provider labels | N8.2, N8.9, P6A.6 | |
| W16 | Jazel color swap | N8.3 | |
| W19 | Department names, missing registry ids | N8.9, N21.10 | |
| RET-13 | Hand the 9,610 refusals to the conflicts work | N1.1, N8 | this document |
| new (dossiers) | CarsCommerce v3 label chain, `_feed_ccid` on every row, per-account brand tokens | N8.8 | |
| new (dossiers) | `group_pools.json` entries for groups 2, 6, 8, 9, 10, 14, 15 | N8.6 | file does not exist yet |
| new (dossiers) | Roster repairs: mbbeverlyhills recipe, basswoodcdjr-com, fjmercedes / 5starford / audihuntsville names, provider_hint retags (bmwofmonrovia-net, crownlexus-com), tedrussellford-net identity, MB Calabasas entity 599, FJ Imports id 420 | N8.9 | |
| new (dossiers) | Cross-group trade tie-break (Gunn/Ingram, Lexus Escondido/South County) | owner decision (open) | |

### 9.2 Retirements

| Item | What | Unit(s) | Status |
|---|---|---|---|
| RET-1, WI-1, W01 | Correct the premise; honest fleet accounting | N1.1, N9.12 | premise: this document; `fleet_scan.py:383` still sums captured rows |
| RET-2 | Terry / Coastline / Napleton / South County Lexus shapes as policy tests | P5A.1 (N9) | strict xfail tests in N1.4 |
| RET-3, WI-8 | `run_started_iso` into after_write; protected VINs | P5A.5 | |
| RET-4 | Pipeline reconcile report-only (D-RC1 = A) | P5A.7 | D-RC1 answered |
| RET-5, WI-7 | Walk report with scope and stop reason | P5A.2 | |
| RET-6 | `listing_retirements` provenance (V027) | P5B.3 | |
| RET-7 | Two-strike, ≥ 20 h (D-RC2) | P5B.4 | D-RC2 answered |
| RET-8, W07 | Pause-safe batch timeout; re-queue killed dealers | N9.13 | |
| RET-9 | CarsCommerce store scope misses store cars in the group used pool (Terry's 11) | D-N5, N8.8 | |
| RET-10 | Walk to the payload total, cap 150 | P5B.2 | Jazel NRH filter needs 51 pages [G12] |
| RET-11 | Retirement re-run acceptance, second pass | N9.14, N11.5-N11.7 | |
| RET-12 | Live re-check of the 21 VINs | N1.2 | done (Section 3.4) |
| WI-9 | Guarded reconcile UPDATE; recovery-replaced runs retire nothing | P5A.4 | |
| WI-10 | Interim provenance in summary_json | dropped (plan Section 8) | |
| WI-11 | `listing_retirements` plus moves in `car_move_log` | P5B.3, N8.6 | |
| WI-14 | Test gaps | N1.4, N7.9, N8.6 | N1.4 done |
| W06 | Page cap and walk termination (`recipes.py:1661-1662` stops on a page with no new kept VIN) | P5B.1, P5B.2 | |

### 9.3 Other findings from the clean room

| Item | What | Unit(s) |
|---|---|---|
| W03 | `vpic_fuel` PHEV false positive | N6.6 |
| W04 | dict repr and template tokens; heal | N6.5, N12.10 |
| W05 | VDP prefetch host exhaustion, 403/429 counts | N8.7, P12A.5 |
| W08 | Price choice, MSRP fallback, junk prices | D-N18, N21.1 |
| W09 | vPIC precedence for drivetrain | P8A.1, P8A.6, P8A.7, P8B.8 |
| W11 | 46 signal dealers | N11.10, N21.11 |
| W13 | Guessed dealer.com `source_url` | N18.8 |
| W14 | Verdict and log blind spots | N9.12 |
| W15 | Placeholders and gallery harvest | N21.6 |
| W17 | Pseudo-VINs | D-N17b, N21.2 |
| W18 | Condition / CPO labels, used at 0 mi | N21.3 |
| W20 | Post-write enrichment cost | P11B (N26, gated) |
| W21 | Body-style vocabulary | P8B.6 |
| IL-01, IL-02 | NULL odometer on new cars flagged `mileage`; prod check | N2.1, N1.6, N2.2 |
| IL-03, IL-04 | Rebuild the index after runs; sync after backfill | N6.1, N6.2 |
| IL-05 | Admin counts active-only; writers delete index rows | N6.3, P5A.4 |
| IL-06 | Price floor and plausibility | N6.7, N12.10 |
| IL-07 | One placeholder-image test | N6.8 |
| IL-08 | Gasoline from vPIC; single-letter fuels | N15.5 |
| IL-09 | VIN-shape gate | N6.9, N12.10 |
| IL-10 | Motive retry payload shape (answered: rows parsed by `dealer_dot_com`; `platform_evidence/motive/field_map.json`) | N1.2, N8.4 |
| IL-11 | Gardena Honda | N21.4 |
| IL-12 | Gap-first VDP prefetch | N8.7, P12A.5 |
| IL-13 | New-arrival grace window | N6.4, N9.12 |
| IL-14 | Used-row mileage coverage gate | N6.9 |
| IL-15 | Canonical body style | P8B.6 |
| IL-16 | Interior color rule | N21.7 |
| IL-17 | High-rate dealers | N21.5 |
| IL-18 | Guessed VDP URLs | N8.5, N18.8 |
| IL-19 | Old Orchard, Hendrick Lexus Charlotte coverage | N8.9 |
| IL-20 | HD/commercial drivetrain table | P8B.4 |
| IL-21 | Recover phase and auto-heal | P15A.4, P18A.5 |
| IL-22 | Stop truncating http_first JSON in logs | N9.12 |
| IL-23, IL-24 | Mercedes designation; rule refinements | N21.8 |
| IL-25 | Acceptance re-measure | N6 re-baseline, N11.3, P6B.8, N21.12, N36.8 |

---

## 10. Open questions and cannot_assess

- **Transfer pools.** Are Hendrick `RHendrickUsed` cars stamped with Terry's address for sale at Terry? The 10-16 re-check says they are listed at Terry; whether D-N5 should treat them as Terry's stock is the owner's call.
- **Cross-group trades.** D-N3 ties when two groups' feeds both stamp a car strongly (Gunn → Ingram, 3 VINs; Lexus Escondido vs South County Lexus, 1 VIN). Proposed rule "later in-stock date wins" is not ruled on.
- **Per-VIN misattribution share.** Not recomputed (Section 4.5).
- **Scan code commit.** Not stamped in the run artifacts (cannot_assess; most likely v1.5.4).
- **Live state is from 10-16 to 10-18,** three to five days after the scan. Cars that moved stores in between can be misclassed; the dossiers name each case they found.
- **Addresses** of many off-roster rooftops are cannot_assess (Cloudflare 403 to curl_cffi, no browser allowed). N1.11 must verify each from a second source.
- **Prod.** None of this was checked on prod (no prod access in this unit). Prod's 12,903 conflict rows from 09-29 are N1.6's job.

---

## 11. Corrections to earlier reports

| Earlier statement | Corrected | Reference |
|---|---|---|
| "~8,800 rows retired inside the run" (memory, RESUME.md, task brief before 10-15) | 21 rows; 8,776 = 9,610 refusals − 834 | Section 2 |
| Identity "holds for all 456 dealers that have a scan_runs row" (retirements RCA) | 437 dealers have a triage `rows` value; the identity holds for all 437 | [S1] |
| Same stock 9,577 / different prices 3,170 (conflicts RCA, plan §2.3) | 9,574 same stock / 3,174 different prices with the claimant's latest record; 9,578 / 3,098 with its first record | [S2] |
| Source host "362 other" | 340 another host + 22 no URL | [Q18] |
| mbbeverlyhills ← mbontario: all 1,278 rows class (b) | (b) 817, (a) 45, (d) 390, unknown 26 | [G1a] |
| Pool with no evidence about 1,995 rows | 312 rows; every group has a per-car store field | Section 4.5, RC4 |
| "15 of 18 top pair dealers logged no gate line" | Not reproduced: taking the first 18 distinct dealers of the top 30 pairs gives 18 of 18 with no gate line, and the report's definition is not recorded. Use instead: 48 of 60 owners (8,844 rows) and 44 of 65 claimants (8,828 rows) logged none | [S4] |
| Class counts (a) 1,874, (b) 3,334, (d) 1,422, unknown 1,013 | Section 4.5 table | dossiers |
| `database.py:45` comment calls mtnviewnissan-com "(CA)" | It is Chattanooga TN (group 4); N8.6 fixes the comment | [G4] |
| 6 dealers with ws-inv-data recipes tagged unknown | Confirmed: 3 at the dealer level, 3 more inside `recipes_json` | [Q21] |

---

## Appendix A. How to reproduce every figure

### A.1 SQL (read-only)

```
roq() { PGOPTIONS='-c default_transaction_read_only=on -c statement_timeout=120000' psql -X -h /tmp -p 5432 -d "$1" -Atc "$2"; }
```

All queries run on `cars_cleanroom` unless the line says `cars`. Results as of 2026-10-18.

| Id | Query | Result |
|---|---|---|
| Q1 | `select count(*), sum(case when listing_active=1 then 1 else 0 end), sum(case when coalesce(listing_active,1)=0 then 1 else 0 end), sum(case when listing_removed_at is not null then 1 else 0 end), count(distinct vin) from cars` | 199478, 199457, 21, 21, 199478 |
| Q2 | `select count(*), count(distinct vin), count(distinct owner_dealer_id), count(distinct claimant_dealer_id), count(distinct (owner_dealer_id, claimant_dealer_id)), min(seen_at), max(seen_at) from vin_owner_conflicts` | 9638, 7393, 60, 65, 92, 2026-10-13T14:05:35.357509Z, 2026-10-14T13:33:21.585785Z |
| Q3 | `select n, count(*) from (select vin, count(*) n from vin_owner_conflicts group by vin) x group by n order by n` | 1: 5416; 2: 1715; 3: 256; 4: 6 |
| Q4 | `select count(*) from car_move_log` | 0 |
| Q5 | `select dealer_id, listing_removed_at, count(*) from cars where coalesce(listing_active,1)=0 group by 1,2 order by 2,1` | southcountylexus 13:29:30 1; terrylabontechevy 13:29:30 16; napletonhondamortongrove 13:32:25 1; coastlinecdjr 13:45:50 3 |
| Q6 | `select count(*) from cars c where coalesce(c.listing_active,1)=0 and exists (select 1 from vin_owner_conflicts v where v.vin=c.vin)` | 0 |
| Q7 | `select count(*) from vin_owner_conflicts v join cars c on c.vin=v.vin and c.dealer_id=v.owner_dealer_id and c.listing_active=1` | 9638 |
| Q8 | `select sum(case when seen_at < '2026-10-13T15:34' then 1 else 0 end), count(distinct case when seen_at < '2026-10-13T15:34' then claimant_dealer_id end), sum(case when seen_at >= '2026-10-13T15:34' then 1 else 0 end), count(distinct case when seen_at >= '2026-10-13T15:34' then claimant_dealer_id end) from vin_owner_conflicts` | 7842, 59, 1796, 9 |
| Q9 | `select id, dealer_id, finished_at, upserted, provider, summary_json::json->'reconcile'->>'marked_inactive' from scan_runs where dealer_id in ('terrylabontechevy-com','southcountylexus-com','napletonhondamortongrove-com','coastlinecdjr-com') order by dealer_id, id` | as Section 3.2 |
| Q10 | `select dealer_id, count(*) filter (where first_seen_at < '2026-10-14'), count(*) filter (where first_seen_at < '2026-10-14' and listing_active=1), count(*) filter (where first_seen_at >= '2026-10-14'), count(*) from cars where dealer_id in (<the 4 dealers>) group by 1` | coastline 11/8/241/252; napleton 17/16/388/405; south county lexus 15/14/179/194; terry 16/0/585/601 |
| Q11 | `select count(*), sum((summary_json::json->'reconcile'->>'marked_inactive')::int) from scan_runs` | 455, 21 |
| Q12 | `with r as (select dealer_id, coalesce((summary_json::json->>'vin_owner_conflicts')::int,0) n, row_number() over (partition by dealer_id order by id desc) rn from scan_runs) select sum(n) filter (where rn=1), sum(n) filter (where rn>1), count(distinct dealer_id) filter (where rn>1) from r` | 9610, 1074, 18 (all runs: 10684) |
| Q13 | `select dealer_id, count(*), min(scraped_at), max(scraped_at) from cars where dealer_id in ('righthonda-com','selmanchevy-com') group by 1`; and `select count(*) from scan_runs where dealer_id in (…)` | righthonda 553 at 15:34:01Z; selmanchevy 281 at 15:34:35Z; 0 scan_runs |
| Q14 | on `cars`: `select count(*) from cars where listing_active=1 and dealer_id in (<the 21 error dealers from triage>)` | 12977 |
| Q15 | `select v.claimant_dealer_id, count(*), min(…), max(extract(epoch from (v.seen_at::timestamptz - c.scraped_at::timestamptz))/3600) from vin_owner_conflicts v join cars c on c.vin=v.vin and c.dealer_id=v.owner_dealer_id where v.seen_at >= '2026-10-13T15:34' group by 1` | 9 claimants, 1,796 rows, max owner age 23.2 h |
| Q16 | `with p as (select distinct on (dealer_id) dealer_id, provider from scan_runs order by dealer_id, id desc) select coalesce(p.provider,'(none)'), count(*) from vin_owner_conflicts v left join p on p.dealer_id=v.owner_dealer_id group by 1 order by 2 desc` | Section 4.3 |
| Q17 | Q16 with `p.dealer_id=v.claimant_dealer_id` | Section 4.3 |
| Q18 | owner row `source_url` host, `www.` stripped and dots replaced by dashes, compared with owner and claimant ids, over `vin_owner_conflicts v join cars c on c.vin=v.vin and c.dealer_id=v.owner_dealer_id` | owner 7998; claimant 1278; other 340; none 22 |
| Q19 | Q18 restricted to owners cleveland-nissan-com, bmwofmonrovia-net, fordofmurfreesboro-com, bentleycadillac-com | 4201 rows, 2706 VINs, 4201 on the owner's host |
| Q20 | `select count(*), count(*) filter (where scope='group') from dealer_feed_scope` | 27, 2 |
| Q21 | `select d.dealer_id, d.provider_hint, r->>'provider_hint' from dealer_recipes d, jsonb_array_elements(d.recipes_json::jsonb) r where r->>'url' like '%/api/widget/ws-inv-data/%' and coalesce(r->>'provider_hint','unknown') in ('unknown','')` | 6 dealers (listed in 4.3) |
| Q22 | `select dealer_id, recipes_json from dealer_recipes where dealer_id in (…)`, compared key by key in Python (both DBs for the retry dealers) | MB pair: same `url`, `method`, `post_template`, `auth_headers`, `content_type`, `pagination`. Retry dealers: scopes as Section 3.2 |
| Q23 | `select substring(image_url from '/vehicles/([A-Z0-9]{2,4})/'), count(*) from cars where dealer_id='<dealer>' and listing_active=1 group by 1` | chapmanbmwchandler CAW 701, CAC 651, none 9; chapmandodge CAE 295, none 269, CYD 154; chapmanfordaz CAU 663, CA5 246, CAE 175, CAY 62, CAV 58 |
| Q25 | `select dealer_id, count(*) from cars where listing_active=1 and description ilike '%<store>%' group by 1` for 'Audi Beverly Hills', 'Audi Long Beach', 'Porsche Long Beach' | 70+2; 39+1; 40 (all mbbeverlyhills-com rows are conflict rows) |

### A.2 Scripts

- **S1 (triage identity).** Over `workspace/cleanroom_2026_10/runs/fleet_20261013T140256Z/s*/triage.json`: sum `rows` (where not null), `rows_stamped`, `active_after` and `vin_owner_conflicts`; count dealers with `rows = null`; check `rows − vin_owner_conflicts == rows_stamped` per dealer. Result: 458 dealers, 437 with rows, 21 null; 208,233 / 198,623 / 199,457 / 9,610; 0 mismatches; the null dealers with rows are righthonda-com 553 and selmanchevy-com 281; all 21 have reason `no scan_runs row`.
- **S2 (same car on both sides).** Read every `type: vehicle` record in `workspace/cleanroom_2026_10/workspace/scanlogs/scan_2026101*.jsonl` (82 files) for the (dealer, VIN) keys in `vin_owner_conflicts`. For each conflict row compare the owner's record with the claimant's latest record (by `scan_ts`) on upper-cased, trimmed `stock_number` and on `price`. Result: stock same 9,574 / different 64; price same 6,464 / different 3,174. With the claimant's earliest record: 9,578 / 6,540.
- **S3 (group totals).** Load `workspace/phase_runs/N1/conflict_groups.json`; for each group count `vin_owner_conflicts` rows whose owner or claimant is a group member, their VINs and pairs. Result: the 19 group sizes in Section 4.5 (2,890 / 1,881 / 972 / 867 / 626 / 448 / 444 / 261 / 234 / 231 / 157 / 130 / 106 / 90 / 69 / 68 / 62 / 58 / 21), total 9,615; 23 rows outside, each a 1-row pair; 38 one-row pairs overall. Group 1a's 10 pairs give 1,714 rows / 1,497 VINs; 1b's 14 pairs give 1,177 / 845.
- **S4 (gate lines).** `grep -ho "rooftop attribution \[[^]]*\]" s*/scanner.log | sort -u`, then count conflict owners and claimants with no line. Result: 48 of 60 owners (8,844 rows, 91.8%); 44 of 65 claimants (8,828 rows).
- **S5 (old owner).** For every conflict row, look up the VIN in the 09-28 local `cars` (`select vin, dealer_id, coalesce(listing_active,1) from cars where vin in (…)`) and compare with owner and claimant. Result: Section 4.2.
- **S6 (rooftop tallies).** Over the `missing_rooftops` arrays of the 20 dossier files, drop the 3 group-13 duplicates and sum `rows_in_cleanroom` by the treatment in Sections 7.2-7.3.

### A.3 Acceptance tool runs (N1.3, read-only)

```
python3 backend/scripts/cleanroom_acceptance.py --db cars_cleanroom --pg-host /tmp --pg-port 5432 \
  --run-dir workspace/cleanroom_2026_10/runs/fleet_20261013T140256Z \
  --sections conflicts,cf7,identity,retirements,run --json <out>
python3 backend/scripts/cleanroom_acceptance.py --db cars_cleanroom --pg-host /tmp --pg-port 5432 \
  --sections columns,drivetrain,body_style --json <out>
python3 backend/scripts/cleanroom_acceptance.py --db cars_cleanroom --pg-host /tmp --pg-port 5432 \
  --sections incomplete,stale_index --work-dir <dir> --jobs 8 --max-new-chunks 8 --json <out>   # run twice: 16 chunks
```

Run on 2026-10-18 at HEAD `078a0c211` and compared leaf by leaf with `analysis/baseline_r1.json` (generated 2026-10-18T14:16:29Z, which itself matches the hand-built fixture `backend/tests/fixtures/cleanroom_baseline_expected.json`: 67 of 67 leaves). Result: 204 leaves equal in the first run, 75 in the second, 95 in the third, **0 different**. The only differences in the third run were the first-seen window split, which needs `--ref-db cars` and is not cited here.
